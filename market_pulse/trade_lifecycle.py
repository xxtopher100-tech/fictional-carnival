"""
Market Pulse — single clean trade lifecycle (replaces dual monitor/shadow truth).

States:
  OPEN → ENTRY_SEEN → TP1_HIT | TP2_HIT | STOP_HIT | EXPIRED | AMBIGUOUS

Rules (fixed):
  - Prefer post-signal candle path via setup_engine.evaluate_path
  - Durable peak high / trough low across polls (no lost wicks)
  - Stop ends the trade (never upgrade to TP after STOP)
  - TP1 may progress to TP2 while still open
  - Same-window stop+TP without order → AMBIGUOUS
  - No AI inventing outcomes

Shadow is not a second public truth here. Optional internal double-check can be
added later; this module is the authoritative closer.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Optional

from market_pulse.config_runtime import ADMIN_IDS, logger
from market_pulse.db import get_db
from market_pulse.helpers import format_price, wat_now
from market_pulse.price_fetchers import get_best_price
from market_pulse.setup_engine import (
    evaluate_path,
    resolve_horizon,
    compute_valid_until,
    _candles_after_timestamp,
)
from market_pulse.trade_lifecycle_rules import is_long as _is_long, resolve_from_extremes

try:
    from market_pulse.telegram_api import send
except Exception:  # pragma: no cover
    send = None  # type: ignore

LIFECYCLE_ENABLED = os.environ.get("TRADE_LIFECYCLE_ENABLED", "1").strip().lower() not in (
    "0", "false", "no", "off",
)
# When 1, handlers can still run legacy outcome_monitor; this module is primary closer
LIFECYCLE_NOTIFY = os.environ.get("TRADE_LIFECYCLE_NOTIFY", "1").strip().lower() not in (
    "0", "false", "no", "off",
)

_TERMINAL = frozenset({"TP1_HIT", "TP2_HIT", "STOP_HIT", "EXPIRED", "AMBIGUOUS", "BE_EXIT"})


def _parse_f(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except Exception:
        return None


def _load_extremes(detail_raw) -> tuple[Optional[float], Optional[float]]:
    try:
        if not detail_raw:
            return None, None
        d = json.loads(detail_raw) if isinstance(detail_raw, str) else dict(detail_raw or {})
        if not isinstance(d, dict):
            return None, None
        hi = d.get("peak_high")
        lo = d.get("trough_low")
        return (_parse_f(hi), _parse_f(lo))
    except Exception:
        return None, None


def _save_detail(c, idea_id: int, detail: dict) -> None:
    try:
        c.execute(
            "UPDATE trade_ideas SET outcome_detail=%s WHERE id=%s",
            (json.dumps(detail), idea_id),
        )
    except Exception:
        pass


def _notify_admins(text: str) -> None:
    if not LIFECYCLE_NOTIFY or not send:
        return
    for aid in ADMIN_IDS or []:
        try:
            send(aid, text[:3500])
        except Exception:
            pass


def process_open_trade(c, row: tuple, now: datetime, now_s: str) -> Optional[str]:
    """
    Process one open trade_ideas row.
    row columns: id, coin, direction, entry, stop, target1, target2, created_at,
                 valid_until, timeframe, tier, status, result, last_notified_state,
                 outcome_detail, publication_status
    Returns new terminal state or None if still open.
    """
    (
        idea_id, coin, direction, entry_s, stop_s, t1_s, t2_s, created_at,
        valid_until, timeframe, tier, status, result, last_notified,
        outcome_detail, publication_status,
    ) = row

    if (status or "").lower() != "open":
        return None
    if (result or "").upper() in _TERMINAL:
        return None

    entry = _parse_f(entry_s)
    stop = _parse_f(stop_s)
    t1 = _parse_f(t1_s)
    t2 = _parse_f(t2_s)
    if not entry or not stop:
        return None

    # Price
    price = None
    try:
        price, _ = get_best_price(coin)
    except Exception:
        price = None
    if (not price or price <= 0) and coin and "/" in str(coin):
        try:
            from market_pulse.forex_trade_engine import get_forex_rate
            rate, _, _, _ = get_forex_rate(str(coin).upper())
            if rate:
                price = float(rate)
        except Exception:
            pass

    # Candles
    after = []
    try:
        from market_pulse.candle_engine import get_candles
        candles = get_candles(coin) or []
        after = _candles_after_timestamp(candles, created_at or "") or []
    except Exception:
        after = []

    targets = [t for t in (t1, t2) if t]
    path = evaluate_path(
        direction, float(entry), float(stop), targets, after, be_trigger_r=1.0,
    ) or {}

    new_state = None
    if path.get("ambiguous"):
        new_state = "AMBIGUOUS"
    else:
        outcome = (path.get("outcome") or "").upper()
        if outcome in ("TP2_HIT", "WIN_T2") or path.get("hit_t2"):
            new_state = "TP2_HIT"
        elif outcome in ("TP1_HIT", "TARGET_HIT", "WIN_T1") or path.get("hit_t1"):
            new_state = "TP1_HIT"
        elif outcome in ("STOP_HIT", "BE_EXIT") or path.get("hit_stop"):
            new_state = "STOP_HIT" if outcome != "BE_EXIT" else "BE_EXIT"

    # Extremes (candles + live + durable)
    prev_hi, prev_lo = _load_extremes(outcome_detail)
    hi = lo = None
    if after:
        try:
            highs = [float(x.get("high") or 0) for x in after if x.get("high") is not None]
            lows = [float(x.get("low") or 0) for x in after if x.get("low") is not None]
            if highs:
                hi = max(highs)
            if lows:
                lo = min(lows)
        except Exception:
            pass
    if price:
        hi = max(hi, float(price)) if hi is not None else float(price)
        lo = min(lo, float(price)) if lo is not None else float(price)
    if prev_hi is not None:
        hi = max(hi, prev_hi) if hi is not None else prev_hi
    if prev_lo is not None:
        lo = min(lo, prev_lo) if lo is not None else prev_lo

    detail = {
        "peak_high": hi,
        "trough_low": lo,
        "updated_at": now_s,
        "source": "trade_lifecycle_v1",
    }
    # preserve opportunity block if present
    try:
        old = json.loads(outcome_detail) if isinstance(outcome_detail, str) and outcome_detail else {}
        if isinstance(old, dict) and "opportunity" in old:
            detail["opportunity"] = old["opportunity"]
    except Exception:
        pass
    _save_detail(c, idea_id, detail)

    long = _is_long(direction or "")
    entry_seen = False
    if long:
        if lo is not None and lo <= entry:
            entry_seen = True
        if price is not None and price <= entry:
            entry_seen = True
        if (last_notified or "").upper() in ("ACTIVE", "TP1_HIT", "ENTRY_SEEN"):
            entry_seen = True
    else:
        if hi is not None and hi >= entry:
            entry_seen = True
        if price is not None and price >= entry:
            entry_seen = True
        if (last_notified or "").upper() in ("ACTIVE", "TP1_HIT", "ENTRY_SEEN"):
            entry_seen = True

    if new_state is None or new_state in ("", "ACTIVE", "STILL_OPEN", "ENTRY_NOT_REACHED"):
        ext = resolve_from_extremes(direction, entry, stop, t1, t2, hi, lo, entry_seen)
        if ext:
            new_state = ext
        elif entry_seen:
            new_state = "ENTRY_SEEN"
        else:
            new_state = "OPEN"

    # Never demote TP to STOP
    prev = (last_notified or result or "").upper()
    if prev in ("TP1_HIT", "TP2_HIT") and new_state in ("STOP_HIT", "BE_EXIT"):
        new_state = prev

    # Expiry
    vu = (valid_until or "").strip()
    if not vu and created_at:
        try:
            hz = resolve_horizon(timeframe, tier)
            vu = compute_valid_until(created_at, hz["valid_hours"])
        except Exception:
            vu = ""
    expired = False
    if vu:
        try:
            expired = now > datetime.strptime(vu[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    if expired and new_state in ("OPEN", "ENTRY_SEEN", "ACTIVE", "ENTRY_NOT_REACHED", None):
        new_state = "EXPIRED"

    # Persist transitions
    if new_state in _TERMINAL:
        # TP1 with t2 stays open for runner
        if new_state == "TP1_HIT" and t2:
            c.execute(
                """
                UPDATE trade_ideas SET
                  lifecycle_status=%s, result=%s, tp1_hit_at=COALESCE(tp1_hit_at,%s),
                  last_notified_state=%s
                WHERE id=%s AND status='open'
                """,
                (new_state, new_state, now_s, new_state, idea_id),
            )
        else:
            c.execute(
                """
                UPDATE trade_ideas SET
                  status='closed', lifecycle_status=%s, result=%s, closed_at=%s,
                  last_notified_state=%s,
                  tp1_hit_at=CASE WHEN %s='TP1_HIT' OR %s='TP2_HIT' THEN COALESCE(tp1_hit_at,%s) ELSE tp1_hit_at END,
                  tp2_hit_at=CASE WHEN %s='TP2_HIT' THEN COALESCE(tp2_hit_at,%s) ELSE tp2_hit_at END,
                  stop_hit_at=CASE WHEN %s IN ('STOP_HIT','BE_EXIT') THEN COALESCE(stop_hit_at,%s) ELSE stop_hit_at END,
                  expired_at=CASE WHEN %s='EXPIRED' THEN COALESCE(expired_at,%s) ELSE expired_at END
                WHERE id=%s AND status='open'
                """,
                (
                    new_state, new_state, now_s, new_state,
                    new_state, new_state, now_s,
                    new_state, now_s,
                    new_state, now_s,
                    new_state, now_s,
                    idea_id,
                ),
            )
        if (last_notified or "") != new_state:
            emoji = {
                "TP1_HIT": "🟢", "TP2_HIT": "🟢", "STOP_HIT": "🔴",
                "BE_EXIT": "🟡", "EXPIRED": "⚪", "AMBIGUOUS": "⚪",
            }.get(new_state, "•")
            _notify_admins(
                f"{emoji} LIFECYCLE #{idea_id} — {new_state}\n"
                f"{coin} · {(direction or '').upper()}\n"
                f"Entry {format_price(entry)} · SL {format_price(stop)}\n"
                f"TP1 {format_price(t1) if t1 else '—'} · TP2 {format_price(t2) if t2 else '—'}\n"
                f"Single lifecycle engine · NFA"
            )
        return new_state

    if new_state == "ENTRY_SEEN" and (last_notified or "") not in (
        "ENTRY_SEEN", "TP1_HIT", "TP2_HIT", "ACTIVE",
    ):
        try:
            c.execute(
                """
                UPDATE trade_ideas SET lifecycle_status=%s, last_notified_state=%s
                WHERE id=%s AND status='open'
                """,
                (new_state, new_state, idea_id),
            )
        except Exception:
            pass
    return None


def run_lifecycle_cycle(limit: int = 80) -> dict:
    """Scan open trades and apply lifecycle. Returns stats."""
    stats = {"processed": 0, "closed": 0, "errors": 0}
    if not LIFECYCLE_ENABLED:
        return stats
    db = None
    try:
        db = get_db()
        c = db.cursor()
        # ensure lifecycle_status column
        try:
            c.execute(
                "ALTER TABLE trade_ideas ADD COLUMN IF NOT EXISTS lifecycle_status TEXT"
            )
        except Exception:
            pass
        c.execute(
            """
            SELECT id, coin, direction, entry, stop, target1, target2, created_at,
                   COALESCE(valid_until,''), COALESCE(timeframe,''), COALESCE(tier,''),
                   status, COALESCE(result,''), COALESCE(last_notified_state,''),
                   COALESCE(outcome_detail,''), COALESCE(publication_status,'')
            FROM trade_ideas
            WHERE status='open'
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = c.fetchall() or []
        now = wat_now()
        now_s = now.strftime("%Y-%m-%d %H:%M:%S")
        for row in rows:
            stats["processed"] += 1
            try:
                closed = process_open_trade(c, row, now, now_s)
                if closed:
                    stats["closed"] += 1
            except Exception as e:
                stats["errors"] += 1
                logger.warning("[LIFECYCLE] #%s error: %s", row[0] if row else "?", e)
        db.commit()
        logger.info(
            "[LIFECYCLE] cycle processed=%s closed=%s errors=%s",
            stats["processed"], stats["closed"], stats["errors"],
        )
    except Exception as e:
        logger.warning("[LIFECYCLE] cycle failed: %s", e)
        if db:
            try:
                db.rollback()
            except Exception:
                pass
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass
    return stats
