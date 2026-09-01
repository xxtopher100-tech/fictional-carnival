"""Real-time trade & key-level follow-up + private weekly report.

Uses existing trade_ideas ledger + setup_engine.evaluate_path.
Does NOT invent outcomes via AI.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Optional

from market_pulse.config_runtime import ADMIN_IDS, logger
from market_pulse.db import get_db
from market_pulse.helpers import format_price, wat_now
from market_pulse.price_fetchers import get_best_price
from market_pulse.setup_engine import (
    evaluate_path,
    resolve_horizon,
    compute_valid_until,
    _parse_level,
    _candles_after_timestamp,
)
from market_pulse.telegram_api import send, post_to_pro_channel

# ── schema helpers ──────────────────────────────────────────────────────────

_TRADE_COLS = [
    ("tp1_hit_at", "TEXT"),
    ("tp2_hit_at", "TEXT"),
    ("stop_hit_at", "TEXT"),
    ("expired_at", "TEXT"),
    ("mfe", "TEXT"),
    ("mae", "TEXT"),
    ("last_notified_state", "TEXT"),
    ("outcome_detail", "TEXT"),
    ("publication_status", "TEXT"),
    ("publication_reason", "TEXT"),
]


def _ensure_schema():
    db = None
    try:
        db = get_db()
        c = db.cursor()
        for col, typ in _TRADE_COLS:
            try:
                c.execute(f"ALTER TABLE trade_ideas ADD COLUMN IF NOT EXISTS {col} {typ}")
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
                db = get_db()
                c = db.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS key_level_watches (
                id SERIAL PRIMARY KEY,
                coin TEXT NOT NULL,
                level DOUBLE PRECISION NOT NULL,
                event_label TEXT NOT NULL,
                phase TEXT,
                status TEXT DEFAULT 'WATCHING',
                generated_at TEXT NOT NULL,
                expiry_at TEXT,
                confirmed_at TEXT,
                confirmation TEXT,
                last_notified_state TEXT,
                notes TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS weekly_report_log (
                id SERIAL PRIMARY KEY,
                week_key TEXT UNIQUE NOT NULL,
                generated_at TEXT NOT NULL,
                report_text TEXT
            )
            """
        )
        db.commit()
    except Exception as e:
        logger.warning("[OUTCOME] schema: %s", e)
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


MONITOR_ACTIVATION_KEY = "outcome_monitor_activated_at"


def get_or_set_monitor_activation_cutoff():
    """First run stores activation WAT time; never overwrites. Survives restarts."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT value FROM admin_settings WHERE key=%s", (MONITOR_ACTIVATION_KEY,))
        row = c.fetchone()
        if row and row[0]:
            return str(row[0])[:19]
        now_s = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT (key) DO NOTHING",
            (MONITOR_ACTIVATION_KEY, now_s, now_s),
        )
        db.commit()
        c.execute("SELECT value FROM admin_settings WHERE key=%s", (MONITOR_ACTIVATION_KEY,))
        row = c.fetchone()
        val = str(row[0])[:19] if row and row[0] else now_s
        logger.info("[OUTCOME] Monitor activation cutoff: %s WAT", val)
        return val
    except Exception as e:
        logger.warning("[OUTCOME] activation cutoff: %s", e)
        if db:
            try:
                db.rollback()
            except Exception:
                pass
        return wat_now().strftime("%Y-%m-%d %H:%M:%S")
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def is_historical_trade(created_at, cutoff_str) -> bool:
    """True only when we can prove the trade predates the monitor activation cutoff.

    Missing cutoff / bad timestamps must NOT suppress Telegram (previous bug:
    empty cutoff → every trade treated as historical → zero outcome DMs).
    """
    if not cutoff_str:
        return False
    if not created_at:
        return False
    try:
        c = datetime.strptime(str(created_at)[:19], "%Y-%m-%d %H:%M:%S")
        k = datetime.strptime(str(cutoff_str)[:19], "%Y-%m-%d %H:%M:%S")
        return c < k
    except Exception:
        return False


def _notify_admins(text: str):
    if not ADMIN_IDS:
        logger.warning("[OUTCOME] ADMIN_IDS empty — private notify skipped")
        return
    for aid in list(ADMIN_IDS):
        try:
            send(aid, text)
            logger.info("[OUTCOME] private notify ok → %s", aid)
        except Exception as e:
            logger.warning("[OUTCOME] notify %s: %s", aid, e)


def _fmt_when_wat(s) -> str:
    if not s:
        return "n/a"
    try:
        dt = datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d %b %Y · %H:%M") + " WAT"
    except Exception:
        return f"{s} WAT"


def _fmt_px(v) -> str:

    try:
        return format_price(float(v))
    except Exception:
        return str(v)


# ── trade monitoring ────────────────────────────────────────────────────────

def monitor_open_trades(limit: int = 40) -> list:
    """Evaluate open trades; notify admin once per state transition."""
    _ensure_schema()
    if not ADMIN_IDS:
        logger.warning(
            "[OUTCOME] ADMIN_IDS is empty — outcomes will update the ledger "
            "but NO private Telegram DMs will be sent. Set ADMIN_IDS on Railway."
        )
    cutoff = get_or_set_monitor_activation_cutoff()
    db = None
    events = []
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            """
            SELECT id, coin, direction, entry, stop, target1, target2, created_at, tier,
                   COALESCE(valid_until, ''), COALESCE(timeframe, '1H'),
                   COALESCE(last_notified_state, ''), COALESCE(status, 'open'),
                   COALESCE(lifecycle_status, ''),
                   COALESCE(publication_status, 'PUBLISHED'),
                   COALESCE(result, '')
            FROM trade_ideas
            WHERE status = 'open'
               OR (
                    status = 'closed'
                    AND COALESCE(last_notified_state, '') = ''
                    AND COALESCE(result, '') IN (
                        'TP1_HIT','TP2_HIT','STOP_HIT','BE_EXIT','EXPIRED',
                        'TARGET_HIT','SETUP_EXPIRED','AMBIGUOUS'
                    )
               )
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = c.fetchall() or []
        now = wat_now()
        now_s = now.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(
            "[OUTCOME] cycle: %s trade row(s) to evaluate (open + un-notified closed)",
            len(rows),
        )
        if not rows:
            logger.info("[OUTCOME] cycle: no open/un-notified trades in ledger")

        for row in rows:
            try:
                row = list(row)
                while len(row) < 16:
                    row.append("")
                (
                    idea_id, coin, direction, entry_s, stop_s, t1_s, t2_s,
                    created_at, tier, valid_until, timeframe,
                    last_notified, status, lifecycle, publication_status, prior_result,
                ) = row[:16]
            except Exception:
                continue

            try:
                _process_one_trade(
                    c, idea_id, coin, direction, entry_s, stop_s, t1_s, t2_s,
                    created_at, tier, valid_until, timeframe,
                    last_notified or "", now, now_s, events, cutoff,
                    publication_status=publication_status or "PUBLISHED",
                    row_status=status or "open",
                    prior_result=prior_result or "",
                )
            except Exception as e:
                logger.error("[OUTCOME] trade #%s error (isolated): %s", idea_id, e)
                try:
                    db.rollback()
                except Exception:
                    pass
                continue

        try:
            db.commit()
        except Exception:
            pass
        if events:
            logger.info(
                "[OUTCOME] cycle events: %s",
                ", ".join(f"#{e.get('id')}:{e.get('state')}" for e in events[:20]),
            )
        else:
            logger.info("[OUTCOME] cycle: no terminal transitions this run")
    except Exception as e:
        logger.error("[OUTCOME] monitor: %s", e)
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass
    return events


def _process_one_trade(
    c, idea_id, coin, direction, entry_s, stop_s, t1_s, t2_s,
    created_at, tier, valid_until, timeframe, last_notified, now, now_s, events,
    cutoff=None,
    publication_status: str = "PUBLISHED",
    row_status: str = "open",
    prior_result: str = "",
):
    entry = _parse_level(entry_s)
    stop = _parse_level(stop_s)
    t1 = _parse_level(t1_s)
    t2 = _parse_level(t2_s)
    if not entry or not stop:
        return

    # Backfill: already closed in ledger but never DMed admin
    if (
        (row_status or "").lower() == "closed"
        and not (last_notified or "").strip()
        and (prior_result or "").strip()
    ):
        mapped = {
            "TARGET_HIT": "TP1_HIT",
            "SETUP_EXPIRED": "EXPIRED",
            "TP1_HIT": "TP1_HIT",
            "TP2_HIT": "TP2_HIT",
            "STOP_HIT": "STOP_HIT",
            "BE_EXIT": "BE_EXIT",
            "EXPIRED": "EXPIRED",
            "AMBIGUOUS": "AMBIGUOUS",
        }.get(prior_result.strip().upper(), prior_result.strip().upper())
        pub = (publication_status or "PUBLISHED").upper().strip()
        historical = is_historical_trade(created_at, cutoff)
        if historical:
            logger.info(
                "[OUTCOME] #%s %s backfill %s HISTORICAL (no Telegram)",
                idea_id, coin, mapped,
            )
            try:
                c.execute(
                    "UPDATE trade_ideas SET last_notified_state=%s WHERE id=%s",
                    (mapped, idea_id),
                )
            except Exception:
                pass
            return
        if pub != "PUBLISHED":
            logger.info(
                "[OUTCOME] #%s %s backfill %s INTERNAL/%s (no Telegram)",
                idea_id, coin, mapped, pub,
            )
            try:
                c.execute(
                    "UPDATE trade_ideas SET last_notified_state=%s WHERE id=%s",
                    (mapped, idea_id),
                )
            except Exception:
                pass
            return
        try:
            c.execute(
                """
                UPDATE trade_ideas SET last_notified_state=%s
                WHERE id=%s AND COALESCE(last_notified_state,'') = ''
                RETURNING id
                """,
                (mapped, idea_id),
            )
            if c.fetchone():
                msg = _format_trade_outcome_msg(
                    idea_id, coin, direction, timeframe, tier,
                    entry, stop, t1, t2, mapped, created_at,
                    historical=False,
                )
                _notify_admins(msg)
                logger.info(
                    "[OUTCOME] #%s %s → %s BACKFILL notified (was closed, never DMed)",
                    idea_id, coin, mapped,
                )
                events.append({"id": idea_id, "state": mapped, "backfill": True})
        except Exception as e:
            logger.warning("[OUTCOME] backfill #%s: %s", idea_id, e)
        return

    # Stale price guard
    price, _ = get_best_price(coin)
    if not price or price <= 0:
        logger.debug("[OUTCOME] #%s skip — no live price for %s", idea_id, coin)
        return

    vu = (valid_until or "").strip()
    if not vu and created_at:
        hz = resolve_horizon(timeframe, tier)
        vu = compute_valid_until(created_at, hz["valid_hours"])
        try:
            c.execute("UPDATE trade_ideas SET valid_until=%s WHERE id=%s", (vu, idea_id))
        except Exception:
            pass

    expired = False
    if vu:
        try:
            vu_dt = datetime.strptime(vu[:19], "%Y-%m-%d %H:%M:%S")
            if now > vu_dt:
                expired = True
        except Exception:
            pass

    candles = []
    try:
        from market_pulse.candle_engine import get_candles
        candles = get_candles(coin) or []
    except Exception:
        candles = []

    after = _candles_after_timestamp(candles, created_at or "")
    if not after:
        logger.debug(
            "[OUTCOME] #%s %s no post-signal 1H candles (n=%s) — live price fallback",
            idea_id, coin, len(candles or []),
        )
    targets = [t for t in (t1, t2) if t]
    path = evaluate_path(
        direction, float(entry), float(stop), targets, after, be_trigger_r=1.0,
    ) or {}

    # No post-signal candles and no terminal from path: do not invent a win/loss
    # (live tick may still resolve later; expiry still handled below)
    outcome = path.get("outcome") or ""
    mfe = path.get("mfe")
    mae = path.get("mae")
    hit_t1 = path.get("hit_t1")
    hit_t2 = path.get("hit_t2")
    hit_stop = path.get("hit_stop")

    # Ambiguity: same-candle both stop and t1 without order
    if path.get("ambiguous"):
        new_state = "AMBIGUOUS"
    elif outcome in ("TP2_HIT", "win_t2") or (hit_t2 and (not hit_stop or str(hit_t2) <= str(hit_stop))):
        new_state = "TP2_HIT"
    elif outcome in ("TP1_HIT", "TARGET_HIT", "win_t1") or (
        hit_t1 and (not hit_stop or str(hit_t1) <= str(hit_stop))
    ):
        new_state = "TP1_HIT"
    elif outcome in ("STOP_HIT", "BE_EXIT") or (
        hit_stop and (not hit_t1 or str(hit_stop) < str(hit_t1 or "9999"))
    ):
        new_state = "STOP_HIT" if outcome != "BE_EXIT" else "BE_EXIT"
    elif expired and outcome in ("STILL_OPEN", "ENTRY_NOT_REACHED", "", None):
        new_state = "EXPIRED"
    elif outcome == "ENTRY_NOT_REACHED":
        new_state = "ENTRY_NOT_REACHED"
    elif outcome == "STILL_OPEN":
        new_state = "ACTIVE"
    else:
        new_state = outcome or "ACTIVE"

    # Live price assist — critical when 1H candles are empty (e.g. Binance geo-block).
    # Crypto is continuous: adverse price beyond stop ⇒ STOP (must have crossed entry).
    # Favourable price beyond TP1/TP2 ⇒ TP (must have crossed entry to reach target).
    # Do NOT require prior ACTIVE from candles — empty post-signal candles were
    # leaving trades stuck in ENTRY_NOT_REACHED with no admin outcome DM.
    if price and new_state in (
        "ACTIVE", "STILL_OPEN", "ENTRY_NOT_REACHED", "", "None"
    ):
        d = (direction or "long").lower()
        is_long = d.startswith("long") or d in ("buy", "l")
        try:
            px = float(price)
            en = float(entry)
            st = float(stop) if stop else None
            tp1 = float(t1) if t1 else None
            tp2 = float(t2) if t2 else None
        except Exception:
            px = en = st = tp1 = tp2 = None

        if px and en:
            if is_long:
                if st is not None and px <= st:
                    new_state = "STOP_HIT"
                elif tp2 is not None and px >= tp2:
                    new_state = "TP2_HIT"
                elif tp1 is not None and px >= tp1:
                    new_state = "TP1_HIT"
                elif px >= en and new_state == "ENTRY_NOT_REACHED":
                    new_state = "ACTIVE"
            else:
                if st is not None and px >= st:
                    new_state = "STOP_HIT"
                elif tp2 is not None and px <= tp2:
                    new_state = "TP2_HIT"
                elif tp1 is not None and px <= tp1:
                    new_state = "TP1_HIT"
                elif px <= en and new_state == "ENTRY_NOT_REACHED":
                    new_state = "ACTIVE"

    if expired and new_state in ("ACTIVE", "ENTRY_NOT_REACHED", "STILL_OPEN"):
        new_state = "EXPIRED"

    # Never demote an achieved TP1/TP2 to STOP in the ledger
    if last_notified in ("TP1_HIT", "TP2_HIT") and new_state in ("STOP_HIT", "BE_EXIT"):
        new_state = last_notified

    # Notification progression (never re-send same or earlier)
    order = {
        "": 0,
        "ENTRY_NOT_REACHED": 1,
        "ACTIVE": 2,
        "TP1_HIT": 3,
        "TP2_HIT": 4,
        "STOP_HIT": 5,
        "BE_EXIT": 5,
        "EXPIRED": 6,
        "AMBIGUOUS": 6,
    }
    prev = last_notified or ""
    # Allow TP1 -> TP2 progression; do not re-notify TP1
    should_notify = False
    if new_state in ("TP1_HIT", "TP2_HIT", "STOP_HIT", "BE_EXIT", "EXPIRED", "AMBIGUOUS"):
        if new_state != prev:
            if new_state == "TP2_HIT" and prev == "TP1_HIT":
                should_notify = True
            elif new_state == "TP1_HIT" and prev not in ("TP1_HIT", "TP2_HIT"):
                should_notify = True
            elif new_state in ("STOP_HIT", "BE_EXIT") and prev not in (
                "STOP_HIT", "BE_EXIT", "TP2_HIT", "TP1_HIT"
            ):
                # Preserve TP1 history — do not rewrite a TP1 trade as a pure stop loss
                should_notify = True
            elif new_state == "EXPIRED" and prev not in ("EXPIRED", "TP2_HIT", "STOP_HIT", "BE_EXIT"):
                should_notify = True
            elif new_state == "AMBIGUOUS" and prev != "AMBIGUOUS":
                should_notify = True
            elif prev == "" or order.get(prev, 0) < order.get(new_state, 0):
                if new_state not in ("ACTIVE", "ENTRY_NOT_REACHED"):
                    should_notify = True

    # Persist milestones (do not erase earlier hits)
    updates = {"lifecycle_status": new_state}
    if mfe is not None:
        updates["mfe"] = str(round(float(mfe), 8))
    if mae is not None:
        updates["mae"] = str(round(float(mae), 8))
    if new_state == "TP1_HIT":
        updates["tp1_hit_at"] = now_s
        updates["result"] = "TP1_HIT"
        # keep open for possible TP2 unless no t2
        if not t2:
            updates["status"] = "closed"
            updates["closed_at"] = now_s
    elif new_state == "TP2_HIT":
        updates["tp2_hit_at"] = now_s
        updates["result"] = "TP2_HIT"
        updates["status"] = "closed"
        updates["closed_at"] = now_s
    elif new_state in ("STOP_HIT", "BE_EXIT"):
        updates["stop_hit_at"] = now_s
        updates["result"] = new_state
        updates["status"] = "closed"
        updates["closed_at"] = now_s
    elif new_state == "EXPIRED":
        updates["expired_at"] = now_s
        updates["result"] = "EXPIRED"
        updates["status"] = "closed"
        updates["closed_at"] = now_s
    elif new_state == "AMBIGUOUS":
        updates["result"] = "AMBIGUOUS"
        updates["status"] = "closed"
        updates["closed_at"] = now_s

    # Official vs research (suppressed / pending / publish_failed)
    pub = (publication_status or "PUBLISHED").upper().strip()
    is_official = pub == "PUBLISHED"

    historical = is_historical_trade(created_at, cutoff)

    if should_notify:
        claimed = False
        try:
            c.execute(
                """
                UPDATE trade_ideas
                SET last_notified_state = %s
                WHERE id = %s
                  AND COALESCE(last_notified_state, '') IS DISTINCT FROM %s
                RETURNING id
                """,
                (new_state, idea_id, new_state),
            )
            claimed = c.fetchone() is not None
        except Exception as e:
            logger.debug("[OUTCOME] claim #%s: %s", idea_id, e)

        if claimed:
            updates["last_notified_state"] = new_state
            if historical:
                events.append({"id": idea_id, "state": new_state, "historical": True})
                logger.info(
                    "[OUTCOME] #%s %s → %s HISTORICAL reconcile (no Telegram)",
                    idea_id, coin, new_state,
                )
            else:
                if is_official:
                    msg = _format_trade_outcome_msg(
                        idea_id, coin, direction, timeframe, tier,
                        entry, stop, t1, t2, new_state, created_at,
                        historical=False,
                    )
                    _notify_admins(msg)
                    if new_state in ("TP1_HIT", "TP2_HIT", "STOP_HIT", "EXPIRED"):
                        try:
                            post_to_pro_channel(
                                f"📋 <b>Trade #{idea_id} — {new_state.replace('_', ' ')}</b>\n"
                                f"{coin} · {(direction or '').upper()}\n"
                                f"<i>Live outcome · detail sent to admin</i>"
                            )
                        except Exception:
                            pass
                    events.append({"id": idea_id, "state": new_state, "historical": False, "official": True})
                    logger.info(
                        "[OUTCOME] #%s %s → %s LIVE notified (OFFICIAL)",
                        idea_id, coin, new_state,
                    )
                else:
                    # Internal / suppressed research — ledger only, no subscriber noise
                    events.append({
                        "id": idea_id, "state": new_state, "historical": False,
                        "official": False, "publication_status": pub,
                    })
                    logger.info(
                        "[OUTCOME] #%s %s → %s INTERNAL/%s (no Telegram)",
                        idea_id, coin, new_state, pub,
                    )
        else:
            logger.debug("[OUTCOME] #%s already claimed %s", idea_id, new_state)

    elif new_state != prev and new_state in ("ACTIVE", "ENTRY_NOT_REACHED"):
        updates["lifecycle_status"] = new_state

    if updates:
        sets = ", ".join(f"{k}=%s" for k in updates)
        vals = list(updates.values()) + [idea_id]
        try:
            c.execute(f"UPDATE trade_ideas SET {sets} WHERE id=%s", vals)
        except Exception as e:
            logger.debug("[OUTCOME] update #%s: %s", idea_id, e)


def _format_trade_outcome_msg(
    idea_id, coin, direction, timeframe, tier, entry, stop, t1, t2, state, created_at,
    historical=False,
):
    emoji = {
        "TP1_HIT": "🟢",
        "TP2_HIT": "🟢",
        "STOP_HIT": "🔴",
        "BE_EXIT": "🟡",
        "EXPIRED": "⚪",
        "AMBIGUOUS": "⚠️",
    }.get(state, "📋")
    when = _fmt_when_wat(created_at)
    title = f"{emoji} <b>TRADE #{idea_id} — {state.replace('_', ' ')}</b>"
    title += f"\n📅 {when}"
    if historical:
        title += "\n<i>HISTORICAL RECONCILIATION (pre-monitor) · private only</i>"
    lines = [
        title,
        "",
        f"{coin}/USDT · {(direction or '').upper()} · {timeframe or '1H'}",
        f"Tier: {(tier or '').upper()}",
        f"Entry: {_fmt_px(entry)}",
    ]
    if stop:
        lines.append(f"Stop: {_fmt_px(stop)}")
    if t1:
        lines.append(f"TP1: {_fmt_px(t1)}")
    if t2:
        lines.append(f"TP2: {_fmt_px(t2)}")
    lines += [
        f"📅 Generated: {_fmt_when_wat(created_at)}",
        f"Status: <b>{state}</b>",
        "",
        "<i>Deterministic monitor · NFA</i>",
    ]
    return "\n".join(lines)



# ── key level watches ───────────────────────────────────────────────────────

# Private follow-ups flooded admins (#26–#45 style). Outcomes stay real;
# we only raise the bar and cluster notifications.
_KEY_WATCH_LEVEL_FRAC = 0.01      # 1% = same zone (ETH 2505 vs 2509)
_KEY_WATCH_NOTIFY_COOLDOWN_H = 2  # same coin + BULLISH/BEARISH family
_KEY_WATCH_MAX_DIGEST = 6
_KEY_WATCH_MIN_AGE_MIN = 15       # no hold/reject until alert has aged
_KEY_WATCH_MOVE_FRAC = 0.0035     # must leave level by ~0.35% before calling outcome


def _key_levels_near(a, b, frac: float = _KEY_WATCH_LEVEL_FRAC) -> bool:
    try:
        a, b = float(a), float(b)
        if a <= 0 or b <= 0:
            return False
        return abs(a - b) / max(a, b) <= frac
    except Exception:
        return False


def _key_watch_family(outcome: str) -> str:
    u = (outcome or "").upper()
    if "BULLISH" in u:
        return "BULLISH"
    if "BEARISH" in u:
        return "BEARISH"
    if "EXPIRED" in u:
        return "EXPIRED"
    return u or "OTHER"


def _key_watch_notify_blocked(coin: str, outcome: str) -> bool:
    db = None
    try:
        db = get_db()
        c = db.cursor()
        fam = _key_watch_family(outcome)
        key = f"key_watch_priv_{coin}_{fam}"
        c.execute("SELECT updated_at FROM admin_settings WHERE key=%s", (key,))
        row = c.fetchone()
        if not row:
            return False
        from datetime import datetime as _dt
        ts = _dt.strptime(str(row[0])[:19], "%Y-%m-%d %H:%M:%S")
        age_h = (wat_now() - ts).total_seconds() / 3600.0
        return age_h < _KEY_WATCH_NOTIFY_COOLDOWN_H
    except Exception:
        return False
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _stamp_key_watch_notify(coin: str, outcome: str) -> None:
    db = None
    try:
        db = get_db()
        c = db.cursor()
        fam = _key_watch_family(outcome)
        key = f"key_watch_priv_{coin}_{fam}"
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, outcome, now),
        )
        db.commit()
    except Exception:
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


def _confirm_key_outcome_15m(coin, price, level, kind: str) -> bool:
    """Fail-CLOSED 15m confirmation. No candles → do not invent an outcome."""
    try:
        from market_pulse.candle_engine import get_candles_15m, candles_15m_ready
        if not candles_15m_ready(coin, min_candles=3):
            return False
        candles = get_candles_15m(coin)
        if not candles or len(candles) < 2:
            return False
        last = candles[-1]
        prev = candles[-2]
        close = last.get("close")
        high = last.get("high")
        low = last.get("low")
        prev_close = prev.get("close")
        if not close:
            return False
        lv = float(level)
        buf = abs(lv) * 0.0008
        kind_u = (kind or "").upper()
        # Breakout above resistance: last close clearly above + not only a wick
        if kind_u == "BREAKOUT":
            return close > lv + buf and (high is None or high >= close)
        # Breakdown below support
        if kind_u in ("BREAK", "TRADING BELOW", "BELOW"):
            return close < lv - buf
        # Hold support: traded to/through level (low), closed back above
        if kind_u in ("HOLD", "TESTING SUPPORT"):
            tested = low is not None and float(low) <= lv + buf
            held = close > lv + buf
            return bool(tested and held)
        # Reject resistance: traded to/through level (high), closed back below
        if kind_u in ("REJECTION", "TESTING RESISTANCE"):
            tested = high is not None and float(high) >= lv - buf
            rejected = close < lv - buf
            return bool(tested and rejected)
        return False
    except Exception as e:
        logger.debug("[KEY WATCH 15m] %s %s: %s", coin, kind, e)
        return False


def _watch_age_minutes(generated_at, now) -> float:
    try:
        g = datetime.strptime(str(generated_at)[:19], "%Y-%m-%d %H:%M:%S")
        return max(0.0, (now - g).total_seconds() / 60.0)
    except Exception:
        return 999.0


def register_key_level_watch(coin, level, event_label, hours_valid: int = 12) -> int:
    """Register a watch. Skips near-duplicate open watches for the same coin."""
    _ensure_schema()
    db = None
    try:
        now = wat_now()
        expiry = (now + timedelta(hours=hours_valid)).strftime("%Y-%m-%d %H:%M:%S")
        db = get_db()
        c = db.cursor()
        c.execute(
            """
            SELECT id, level FROM key_level_watches
            WHERE coin=%s AND status='WATCHING'
            ORDER BY id DESC LIMIT 30
            """,
            (coin,),
        )
        for row in c.fetchall() or []:
            if _key_levels_near(row[1], level):
                logger.info(
                    "[KEY WATCH] skip duplicate %s @ %s (near existing #%s)",
                    coin, level, row[0],
                )
                return int(row[0])

        c.execute(
            """
            INSERT INTO key_level_watches
            (coin, level, event_label, phase, status, generated_at, expiry_at, last_notified_state)
            VALUES (%s,%s,%s,%s,'WATCHING',%s,%s,'WATCHING')
            RETURNING id
            """,
            (
                coin,
                float(level),
                event_label,
                "TESTING",
                now.strftime("%Y-%m-%d %H:%M:%S"),
                expiry,
            ),
        )
        kid = c.fetchone()[0]
        db.commit()
        logger.info("[KEY WATCH] #%s %s %s @ %s", kid, coin, event_label, level)
        return kid
    except Exception as e:
        logger.warning("[KEY WATCH] register: %s", e)
        if db:
            try:
                db.rollback()
            except Exception:
                pass
        return 0
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _close_nearby_watches(c, coin, level, new_state, conf, now_s, keep_id) -> int:
    """Silently close other open watches in the same price zone (ledger only)."""
    closed = 0
    try:
        c.execute(
            """
            SELECT id, level FROM key_level_watches
            WHERE coin=%s AND status='WATCHING' AND id<>%s
            """,
            (coin, keep_id),
        )
        for rid, lv in c.fetchall() or []:
            if not _key_levels_near(lv, level):
                continue
            c.execute(
                """
                UPDATE key_level_watches
                SET status='CLOSED', confirmation=%s, confirmed_at=%s,
                    last_notified_state=%s, notes=%s
                WHERE id=%s
                """,
                (
                    conf or new_state,
                    now_s,
                    new_state,
                    f"clustered with #{keep_id}",
                    rid,
                ),
            )
            closed += 1
    except Exception as e:
        logger.debug("[KEY WATCH] cluster close: %s", e)
    return closed


def _key_outcome_display(event_label, new_state: str) -> str:
    """Admin labels — only three public outcomes:

    🟢 FOLLOW-THROUGH HELD
    🔴 INVALIDATED
    ⚪ EXPIRED / NO CLEAR OUTCOME
    """
    lab = (event_label or "").upper()
    st = (new_state or "").upper()
    if st == "EXPIRED":
        return "EXPIRED / NO CLEAR OUTCOME"
    if "RESISTANCE" in lab:
        if st == "BEARISH_REJECTION":
            return "FOLLOW-THROUGH HELD"
        if st == "BULLISH_BREAKOUT":
            return "INVALIDATED"
    if "SUPPORT" in lab:
        if st == "BULLISH_HOLD":
            return "FOLLOW-THROUGH HELD"
        if st == "BEARISH_BREAK":
            return "INVALIDATED"
    # Standalone break/breakout watches: holding the break = HELD
    if st in ("BULLISH_BREAKOUT", "BEARISH_BREAK"):
        return "FOLLOW-THROUGH HELD"
    return "EXPIRED / NO CLEAR OUTCOME"


def monitor_key_level_watches(limit: int = 40) -> list:

    """Resolve watches with stricter rules; one private digest max per cycle."""
    _ensure_schema()
    db = None
    events = []
    digest_lines = []
    try:
        from market_pulse.alerts import _level_label

        db = get_db()
        c = db.cursor()
        c.execute(
            """
            SELECT id, coin, level, event_label, status, generated_at, expiry_at,
                   COALESCE(last_notified_state, '')
            FROM key_level_watches
            WHERE status = 'WATCHING'
            ORDER BY id ASC
            LIMIT %s
            """,
            (limit,),
        )
        rows = c.fetchall() or []
        now = wat_now()
        now_s = now.strftime("%Y-%m-%d %H:%M:%S")
        cycle_seen = {}  # (coin, family) -> True

        for row in rows:
            try:
                kid, coin, level, event_label, status, generated_at, expiry_at, last_n = row
                price, _ = get_best_price(coin)
                if not price:
                    continue

                expired = False
                if expiry_at:
                    try:
                        if now > datetime.strptime(str(expiry_at)[:19], "%Y-%m-%d %H:%M:%S"):
                            expired = True
                    except Exception:
                        pass

                label = event_label or _level_label(price, level)[0]
                new_state = None
                conf = None
                age_m = _watch_age_minutes(generated_at, now)

                if expired:
                    new_state = "EXPIRED"
                elif age_m < _KEY_WATCH_MIN_AGE_MIN:
                    # Too fresh — wait for structure to develop
                    continue
                else:
                    buf = abs(float(level)) * _KEY_WATCH_MOVE_FRAC
                    lab = (label or "").upper()
                    if "SUPPORT" in lab:
                        if price < float(level) - buf:
                            if _confirm_key_outcome_15m(coin, price, float(level), "BREAK"):
                                new_state, conf = "BEARISH_BREAK", "bearish"
                        elif price > float(level) + buf:
                            if _confirm_key_outcome_15m(coin, price, float(level), "HOLD"):
                                new_state, conf = "BULLISH_HOLD", "bullish"
                    elif "RESISTANCE" in lab:
                        if price > float(level) + buf:
                            if _confirm_key_outcome_15m(coin, price, float(level), "BREAKOUT"):
                                new_state, conf = "BULLISH_BREAKOUT", "bullish"
                        elif price < float(level) - buf:
                            if _confirm_key_outcome_15m(coin, price, float(level), "REJECTION"):
                                new_state, conf = "BEARISH_REJECTION", "bearish"

                if not new_state:
                    continue
                if new_state == last_n:
                    continue

                c.execute(
                    """
                    UPDATE key_level_watches
                    SET status=%s, confirmation=%s, confirmed_at=%s, last_notified_state=%s
                    WHERE id=%s
                    """,
                    (
                        "CLOSED",
                        conf or new_state,
                        now_s,
                        new_state,
                        kid,
                    ),
                )
                clustered = _close_nearby_watches(
                    c, coin, level, new_state, conf, now_s, kid
                )
                events.append(
                    {"id": kid, "coin": coin, "level": level, "state": new_state, "clustered": clustered}
                )
                logger.info(
                    "[KEY WATCH] #%s %s → %s (clustered %s nearby)",
                    kid, coin, new_state, clustered,
                )

                # EXPIRED: ledger only, no private spam
                fam = _key_watch_family(new_state)
                ckey = (coin, fam)
                if ckey in cycle_seen:
                    logger.debug("[KEY WATCH] #%s skip private (same cycle family)", kid)
                    continue
                if _key_watch_notify_blocked(coin, new_state):
                    logger.info(
                        "[KEY WATCH] #%s private cooldown active for %s %s",
                        kid, coin, fam,
                    )
                    continue

                cycle_seen[ckey] = True
                _stamp_key_watch_notify(coin, new_state)

                disp = _key_outcome_display(event_label, new_state)
                if "HELD" in disp:
                    emoji = "🟢"
                elif "INVALID" in disp:
                    emoji = "🔴"
                else:
                    emoji = "⚪"  # EXPIRED / NO CLEAR OUTCOME

                elapsed_s = "n/a"
                try:
                    from datetime import datetime as _dt
                    g = _dt.strptime(str(generated_at)[:19], "%Y-%m-%d %H:%M:%S")
                    mins = max(0, int((now - g).total_seconds() // 60))
                    elapsed_s = f"{mins}m" if mins < 60 else f"{mins // 60}h {mins % 60}m"
                except Exception:
                    pass
                pub_s = _fmt_when_wat(generated_at) if generated_at else "n/a"
                out_s = _fmt_when_wat(now_s)
                extra = f"\nNear-duplicates closed: {clustered}" if clustered else ""

                msg = (
                    f"{emoji} <b>KEY ALERT #{kid} — {disp}</b>\n\n"
                    f"{coin}/USDT\n"
                    f"Level: {_fmt_px(level)}\n"
                    f"Original: {event_label or label}\n"
                    f"Published: <b>{pub_s}</b>\n"
                    f"Outcome: <b>{out_s}</b>\n"
                    f"Elapsed: <b>{elapsed_s}</b>\n"
                    f"Internal: {new_state.replace('_', ' ')}"
                    f"{extra}\n\n"
                    f"<i>Same ID as channel post · private admin · NFA</i>"
                )
                digest_lines.append(msg)
                try:
                    _notify_admins(msg)
                    logger.info(
                        "[KEY WATCH] private DM sent for #%s %s → %s",
                        kid, coin, disp,
                    )
                except Exception as ne:
                    logger.warning("[KEY WATCH] private DM failed #%s: %s", kid, ne)

            except Exception as e:
                logger.error("[KEY WATCH] row error: %s", e)
                continue

        db.commit()

        if digest_lines:
            logger.info(
                "[KEY WATCH] cycle done — %s private follow-up(s) attempted",
                len(digest_lines),
            )

    except Exception as e:
        logger.error("[KEY WATCH] monitor: %s", e)
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
    return events


# ── weekly private report ───────────────────────────────────────────────────

def build_weekly_performance_report(days: int = 7) -> str:
    """Aggregate ledger only — LIVE (post activation) vs HISTORICAL split.

    Does not claim profitability. Counts are outcomes, not expectancy proof.
    """
    _ensure_schema()
    cutoff = get_or_set_monitor_activation_cutoff()
    db = None
    try:
        db = get_db()
        c = db.cursor()
        since = (wat_now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """
            SELECT id, coin, tier, direction, timeframe, result, status,
                   created_at, closed_at, rr, lifecycle_status
            FROM trade_ideas
            WHERE created_at >= %s
              AND COALESCE(publication_status, 'PUBLISHED') = 'PUBLISHED'
            ORDER BY id
            """,
            (since,),
        )
        trades = c.fetchall() or []
        c.execute(
            """
            SELECT id, coin, event_label, status, confirmation, generated_at
            FROM key_level_watches
            WHERE generated_at >= %s
            """,
            (since,),
        )
        keys = c.fetchall() or []
    except Exception as e:
        return f"Weekly report error: {e}"
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass

    def _bucket(row):
        created = row[7] if len(row) > 7 else ""
        return "HISTORICAL" if is_historical_trade(created, cutoff) else "LIVE"

    def _res(row):
        return (row[5] or row[10] or "").upper()

    def _stats(subset):
        total = len(subset)
        open_n = sum(1 for t in subset if (t[6] or "") == "open")
        results = [_res(t) for t in subset]
        tp1 = sum(1 for r in results if "TP1" in r or r == "TARGET_HIT")
        tp2 = sum(1 for r in results if "TP2" in r)
        stops = sum(1 for r in results if "STOP" in r or r == "BE_EXIT")
        expired = sum(1 for r in results if "EXPIRED" in r)
        amb = sum(1 for r in results if "AMBIGUOUS" in r or "DATA_UNAVAILABLE" in r)
        decisive = tp1 + tp2 + stops
        hit_pct = (100.0 * (tp1 + tp2) / decisive) if decisive else 0.0
        return {
            "total": total, "open": open_n, "tp1": tp1, "tp2": tp2,
            "stops": stops, "expired": expired, "amb": amb,
            "decisive": decisive, "hit_pct": hit_pct,
        }

    live = [t for t in trades if _bucket(t) == "LIVE"]
    hist = [t for t in trades if _bucket(t) == "HISTORICAL"]
    sl, sh = _stats(live), _stats(hist)

    def _tier_line(subset, keys):
        part = [t for t in subset if (t[2] or "").lower() in keys]
        if not part:
            return "n=0"
        st = _stats(part)
        return (
            f"n={st['total']} TP1={st['tp1']} TP2={st['tp2']} "
            f"SL={st['stops']} EXP={st['expired']} AMB={st['amb']} "
            f"TP-vs-SL≈{st['hit_pct']:.0f}% (dec={st['decisive']})"
        )

    by_coin = {}
    for t in live:
        by_coin.setdefault(t[1] or "?", []).append(t)
    by_tf = {}
    for t in live:
        by_tf.setdefault((t[4] or "1H").upper(), []).append(t)

    lines = [
        "📊 <b>WEEKLY MEASUREMENT REPORT</b> (private)",
        f"<i>{wat_now().strftime('%A %Y-%m-%d %H:%M')} WAT · window last {days}d</i>",
        f"<i>Monitor activation cutoff: {cutoff} WAT</i>",
        "",
        "⚠️ Counts are outcomes, not proof of profitability.",
        "",
        "━━ LIVE (created on/after activation) ━━",
        f"Setups: <b>{sl['total']}</b> · Open: <b>{sl['open']}</b>",
        f"TP1: <b>{sl['tp1']}</b> · TP2: <b>{sl['tp2']}</b> · STOP: <b>{sl['stops']}</b>",
        f"EXPIRED: <b>{sl['expired']}</b> · AMBIGUOUS/DATA_UNAVAILABLE: <b>{sl['amb']}</b>",
        f"TP vs SL (among decisive): <b>{sl['hit_pct']:.1f}%</b> (n={sl['decisive']})",
        "",
        "By tier (LIVE only):",
        f"🟢 SAFE/steady: {_tier_line(live, ['steady', 'safe'])}",
        f"🟡 NORMAL/momentum: {_tier_line(live, ['momentum', 'normal'])}",
        f"🔴 EDGE/aggressive: {_tier_line(live, ['edge', 'aggressive'])}",
        "",
        "By asset (LIVE):",
    ]
    if not by_coin:
        lines.append("No live setups in window.")
    else:
        for coin, subset in sorted(by_coin.items(), key=lambda x: -len(x[1])):
            st = _stats(subset)
            if st["total"] < 2:
                lines.append(f"{coin}: n={st['total']} — small sample")
            else:
                lines.append(
                    f"{coin}: n={st['total']} TP={st['tp1']+st['tp2']} "
                    f"SL={st['stops']} EXP={st['expired']}"
                )
    lines += ["", "By timeframe (LIVE):"]
    if not by_tf:
        lines.append("—")
    else:
        for tf, subset in sorted(by_tf.items()):
            st = _stats(subset)
            lines.append(f"{tf}: n={st['total']} TP={st['tp1']+st['tp2']} SL={st['stops']}")

    lines += [
        "",
        "━━ HISTORICAL / RECONCILED (created before activation) ━━",
        f"Rows in window: <b>{sh['total']}</b> (not used as live performance)",
        f"TP1={sh['tp1']} TP2={sh['tp2']} STOP={sh['stops']} EXP={sh['expired']} AMB={sh['amb']}",
        "<i>These were ledger backfill — do not treat as post-deploy edge.</i>",
        "",
        "━━ KEY LEVEL WATCHES ━━",
    ]
    if not keys:
        lines.append("No key-level watches in window.")
    else:
        lines.append(f"Watches: {len(keys)}")
    lines += [
        "",
        "Source of truth = trade_ideas ledger.",
        "Measurement phase: focus on LIVE section only.",
        "",
        "<i>NFA — small samples mislead</i>",
    ]
    return "\n".join(lines)



def send_weekly_report_private(force: bool = False) -> bool:
    """Sunday private report to ADMIN_IDS only. Idempotent per week_key."""
    _ensure_schema()
    now = wat_now()
    # ISO week key
    week_key = f"{now.isocalendar()[0]}-W{now.isocalendar()[1]:02d}"
    db = None
    try:
        db = get_db()
        c = db.cursor()
        if not force:
            c.execute("SELECT id FROM weekly_report_log WHERE week_key=%s", (week_key,))
            if c.fetchone():
                logger.info("[WEEKLY REPORT] already sent for %s", week_key)
                return False
        text = build_weekly_performance_report(7)
        _notify_admins(text)
        c.execute(
            """
            INSERT INTO weekly_report_log (week_key, generated_at, report_text)
            VALUES (%s,%s,%s)
            ON CONFLICT (week_key) DO UPDATE SET generated_at=excluded.generated_at, report_text=excluded.report_text
            """,
            (week_key, now.strftime("%Y-%m-%d %H:%M:%S"), text[:8000]),
        )
        db.commit()
        logger.info("[WEEKLY REPORT] sent privately for %s", week_key)
        return True
    except Exception as e:
        logger.error("[WEEKLY REPORT] %s", e)
        if db:
            try:
                db.rollback()
            except Exception:
                pass
        return False
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def run_outcome_cycle():
    """Single safe cycle: trades + key levels. Never raises to caller."""
    try:
        monitor_open_trades()
    except Exception as e:
        logger.error("[OUTCOME CYCLE] trades: %s", e)
    try:
        monitor_key_level_watches()
    except Exception as e:
        logger.error("[OUTCOME CYCLE] keys: %s", e)
