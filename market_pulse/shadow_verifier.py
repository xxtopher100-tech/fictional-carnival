"""Independent LIVE shadow verifier (System B).

Does NOT use outcome_monitor results to decide market outcomes.
Reads monitor result ONLY after independent completion, for comparison.
Private ADMIN only. Enable: SHADOW_VERIFY_ENABLED=true
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from market_pulse.config_runtime import (
    ADMIN_IDS,
    SHADOW_VERIFY_ENABLED,
    SHADOW_VERIFY_PRIVATE_ONLY,
    WAT_OFFSET,
    logger,
)
from market_pulse.db import get_db
from market_pulse.helpers import format_price, wat_now
from market_pulse.price_fetchers import get_best_price
from market_pulse.telegram_api import send

try:
    from market_pulse.price_engine import _ws_get_cached, WS_STALE_SECONDS
except Exception:
    _ws_get_cached = None
    WS_STALE_SECONDS = 60

SHADOW_ACTIVATION_KEY = "shadow_verifier_activated_at"
# Reject REST fallback older than this (seconds) when we have no freshness signal
REST_MAX_AGE_HINT = 120


def _notify_admins(text: str) -> None:
    if not ADMIN_IDS:
        logger.warning("[SHADOW] ADMIN_IDS empty — notify skipped")
        return
    for aid in list(ADMIN_IDS):
        try:
            send(aid, text)
        except Exception as e:
            logger.warning("[SHADOW] notify %s: %s", aid, e)


def _ensure_shadow_schema() -> None:
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS shadow_verifications (
                id SERIAL PRIMARY KEY,
                trade_id INTEGER UNIQUE NOT NULL,
                asset TEXT,
                direction TEXT,
                timeframe TEXT,
                tier TEXT,
                generated_at TEXT,
                entry TEXT,
                stop_loss TEXT,
                target_1 TEXT,
                target_2 TEXT,
                valid_until TEXT,
                shadow_status TEXT DEFAULT 'WATCHING',
                shadow_result TEXT,
                monitor_result TEXT,
                comparison TEXT,
                shadow_entry_activated TEXT,
                shadow_first_event TEXT,
                shadow_outcome_timestamp TEXT,
                data_source TEXT,
                data_resolution TEXT DEFAULT 'poll_5m',
                last_price TEXT,
                comparison_notified INTEGER DEFAULT 0,
                created_at TEXT NOT NULL,
                completed_at TEXT
            )
            """
        )
        db.commit()
    except Exception as e:
        logger.warning("[SHADOW] schema: %s", e)
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


def get_or_set_shadow_activation_cutoff() -> str:
    """Persisted WAT wall time when shadow verifier first became active.

    Only trades with created_at >= cutoff are enrolled. Never overwrites.
    """
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            "SELECT value FROM admin_settings WHERE key=%s",
            (SHADOW_ACTIVATION_KEY,),
        )
        row = c.fetchone()
        if row and row[0]:
            return str(row[0])[:19]
        now_s = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT (key) DO NOTHING",
            (SHADOW_ACTIVATION_KEY, now_s, now_s),
        )
        db.commit()
        c.execute(
            "SELECT value FROM admin_settings WHERE key=%s",
            (SHADOW_ACTIVATION_KEY,),
        )
        row = c.fetchone()
        val = str(row[0])[:19] if row and row[0] else now_s
        logger.info("[SHADOW] activation cutoff set/loaded: %s WAT", val)
        return val
    except Exception as e:
        logger.warning("[SHADOW] activation cutoff: %s", e)
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


def _parse_f(x) -> Optional[float]:
    if x is None:
        return None
    try:
        s = str(x).replace("$", "").replace(",", "").strip()
        if not s or s.lower() in ("none", "n/a", "-"):
            return None
        return float(s)
    except Exception:
        return None


def _wat_str_to_utc(s: str):
    if not s:
        return None
    try:
        wat = datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")
        utc = wat - timedelta(hours=int(WAT_OFFSET))
        return utc.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _live_price(coin: str) -> Tuple[Optional[float], str, Optional[float]]:
    """(price, source, age_seconds_or_None). Never uses outcome_monitor state.

    Prefers fresh WebSocket cache (price_engine already enforces WS_STALE_SECONDS).
    Falls back to get_best_price; age unknown for REST → caller may still use.
    """
    if _ws_get_cached:
        try:
            # Returns (price, change) — NOT a dict
            pair = _ws_get_cached(coin)
            if pair and pair[0] is not None and float(pair[0]) > 0:
                return float(pair[0]), "ws_cache", float(WS_STALE_SECONDS)
        except Exception as e:
            logger.debug("[SHADOW] ws cache: %s", e)
    try:
        p, _ = get_best_price(coin)
        if p and float(p) > 0:
            return float(p), "get_best_price", None
    except Exception as e:
        logger.debug("[SHADOW] get_best_price: %s", e)
    return None, "none", None


def independent_step(
    direction: str,
    entry: float,
    stop: float,
    t1: Optional[float],
    t2: Optional[float],
    price: float,
    entry_activated: bool,
) -> Tuple[bool, Optional[str]]:
    """Pure tick. TP/SL only after a *prior* entry activation observation.

    CRITICAL: The first observation that would both activate entry AND hit
    TP/SL does NOT count as entry+outcome. Without tick history after the
    signal we cannot prove entry was crossed post-signal vs a gap through
    levels. We wait for a clean entry observation (price in entry region
    without simultaneous TP/SL), then allow TP/SL on later polls.

    Simultaneous TP+SL after entry is still AMBIGUOUS.
    """
    d = (direction or "long").lower()
    is_long = d.startswith("long") or d == "buy"

    def _hits_terminal(px: float) -> bool:
        if is_long:
            if stop is not None and px <= stop:
                return True
            if t1 is not None and px >= t1:
                return True
            if t2 is not None and px >= t2:
                return True
        else:
            if stop is not None and px >= stop:
                return True
            if t1 is not None and px <= t1:
                return True
            if t2 is not None and px <= t2:
                return True
        return False

    def _would_activate(px: float) -> bool:
        if is_long:
            return px >= entry
        return px <= entry

    if not entry_activated:
        if not _would_activate(price):
            return False, None
        # Price is at/through entry. If it is also already at TP or SL on
        # this same observation, refuse activation — cannot prove post-signal
        # entry cross (gap / first-poll-beyond-levels).
        if _hits_terminal(price):
            return False, None
        return True, None

    # Entry was activated on a *previous* observation — evaluate outcomes
    hit_tp1 = hit_tp2 = hit_sl = False
    if is_long:
        if stop is not None and price <= stop:
            hit_sl = True
        if t1 is not None and price >= t1:
            hit_tp1 = True
        if t2 is not None and price >= t2:
            hit_tp2 = True
    else:
        if stop is not None and price >= stop:
            hit_sl = True
        if t1 is not None and price <= t1:
            hit_tp1 = True
        if t2 is not None and price <= t2:
            hit_tp2 = True

    if hit_sl and (hit_tp1 or hit_tp2):
        return True, "AMBIGUOUS"
    if hit_tp2:
        return True, "TP2_HIT"
    if hit_tp1:
        return True, "TP1_HIT"
    if hit_sl:
        return True, "STOP_HIT"
    return True, None


def enroll_new_trades(limit: int = 30) -> int:
    """Enroll only trades with created_at >= shadow activation cutoff."""
    if not SHADOW_VERIFY_ENABLED:
        return 0
    _ensure_shadow_schema()
    cutoff = get_or_set_shadow_activation_cutoff()
    db = None
    n = 0
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            """
            SELECT id, coin, direction, timeframe, tier, created_at,
                   entry, stop, target1, target2, COALESCE(valid_until, '')
            FROM trade_ideas
            WHERE created_at >= %s
              AND COALESCE(publication_status, 'PUBLISHED') = 'PUBLISHED'
              AND id NOT IN (SELECT trade_id FROM shadow_verifications)
            ORDER BY id DESC
            LIMIT %s
            """,
            (cutoff, limit),
        )
        rows = c.fetchall() or []
        now_s = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        for row in rows:
            tid, coin, direction, tf, tier, created_at, entry, stop, t1, t2, vu = row
            # Defense in depth: string compare WAT wall clocks (same format)
            if str(created_at or "")[:19] < cutoff:
                continue
            c.execute(
                """
                INSERT INTO shadow_verifications
                (trade_id, asset, direction, timeframe, tier, generated_at,
                 entry, stop_loss, target_1, target_2, valid_until,
                 shadow_status, created_at, data_resolution)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'WATCHING',%s,'poll_5m')
                ON CONFLICT (trade_id) DO NOTHING
                """,
                (
                    tid, coin, direction, tf, tier, created_at,
                    entry, stop, t1, t2, vu, now_s,
                ),
            )
            if c.rowcount:
                n += 1
        db.commit()
        if n:
            logger.info("[SHADOW] enrolled %s trade(s) after cutoff %s", n, cutoff)
    except Exception as e:
        logger.warning("[SHADOW] enroll: %s", e)
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
    return n


def _read_monitor_result(c, trade_id: int) -> str:
    """Comparison only — never feeds independent_step."""
    try:
        c.execute(
            "SELECT COALESCE(result, ''), COALESCE(lifecycle_status, ''), "
            "COALESCE(status, '') FROM trade_ideas WHERE id=%s",
            (trade_id,),
        )
        row = c.fetchone()
        if not row:
            return ""
        result, life, status = row
        r = (result or life or "").upper()
        if r:
            return r
        if status == "open":
            return "OPEN"
        return status or ""
    except Exception:
        return ""


def _normalize_result(r: str) -> str:
    r = (r or "").upper().replace(" ", "_")
    if "TP2" in r:
        return "TP2_HIT"
    if "TP1" in r or r == "TARGET_HIT":
        return "TP1_HIT"
    if "STOP" in r or r == "BE_EXIT":
        return "STOP_HIT"
    if "EXPIRED" in r or "SETUP_EXPIRED" in r:
        return "EXPIRED"
    if "AMBIGUOUS" in r:
        return "AMBIGUOUS"
    return r


def tick_shadow_verifications(limit: int = 40) -> None:
    if not SHADOW_VERIFY_ENABLED:
        return
    _ensure_shadow_schema()
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            """
            SELECT trade_id, asset, direction, entry, stop_loss, target_1, target_2,
                   generated_at, valid_until, COALESCE(shadow_entry_activated, ''),
                   COALESCE(shadow_first_event, ''), COALESCE(comparison_notified, 0)
            FROM shadow_verifications
            WHERE shadow_status = 'WATCHING'
            ORDER BY trade_id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = c.fetchall() or []
        now = wat_now()
        now_s = now.strftime("%Y-%m-%d %H:%M:%S")
        now_utc = datetime.now(timezone.utc)

        for row in rows:
            try:
                (
                    tid, coin, direction, entry_s, stop_s, t1_s, t2_s,
                    generated_at, valid_until, entry_act_s, first_event, comp_notified,
                ) = row
                entry = _parse_f(entry_s)
                stop = _parse_f(stop_s)
                t1 = _parse_f(t1_s)
                t2 = _parse_f(t2_s)
                if not entry or not stop:
                    continue

                sig_utc = _wat_str_to_utc(generated_at or "")
                if sig_utc and now_utc < (sig_utc - timedelta(seconds=2)):
                    continue

                price, src, _age = _live_price(coin)
                if not price:
                    logger.debug("[SHADOW] #%s missing price — skip tick", tid)
                    continue

                entry_on = bool(entry_act_s) or bool(first_event)
                entry_on, event = independent_step(
                    direction, entry, stop, t1, t2, price, entry_on,
                )

                if entry_on and not entry_act_s:
                    c.execute(
                        "UPDATE shadow_verifications SET shadow_entry_activated=%s, "
                        "last_price=%s, data_source=%s "
                        "WHERE trade_id=%s AND shadow_status='WATCHING'",
                        (now_s, str(price), src, tid),
                    )

                # Expiry only if no terminal first_event yet
                if valid_until and not first_event and not event:
                    try:
                        vu = datetime.strptime(str(valid_until)[:19], "%Y-%m-%d %H:%M:%S")
                        if now > vu:
                            event = "EXPIRED"
                    except Exception:
                        pass

                final = None
                if event == "AMBIGUOUS":
                    final = "AMBIGUOUS"
                elif event == "EXPIRED":
                    final = "EXPIRED"
                elif event == "TP2_HIT":
                    final = "TP2_HIT"
                elif event == "TP1_HIT":
                    if first_event == "TP1_HIT":
                        final = None  # keep watching for TP2
                    else:
                        final = "TP1_HIT"
                elif event == "STOP_HIT":
                    if first_event in ("TP1_HIT", "TP2_HIT"):
                        final = None  # never demote TP1/TP2 to STOP
                    else:
                        final = "STOP_HIT"

                if final == "TP1_HIT" and not first_event:
                    c.execute(
                        "UPDATE shadow_verifications SET shadow_first_event=%s, "
                        "last_price=%s, data_source=%s WHERE trade_id=%s "
                        "AND shadow_status='WATCHING'",
                        (final, str(price), src, tid),
                    )
                    continue

                if final in ("TP2_HIT", "STOP_HIT", "EXPIRED", "AMBIGUOUS"):
                    c.execute(
                        """
                        UPDATE shadow_verifications
                        SET shadow_result=%s,
                            shadow_first_event=COALESCE(NULLIF(shadow_first_event,''), %s),
                            shadow_outcome_timestamp=%s,
                            shadow_status='DONE',
                            completed_at=%s,
                            last_price=%s,
                            data_source=%s
                        WHERE trade_id=%s AND shadow_status='WATCHING'
                        RETURNING trade_id
                        """,
                        (final, final, now_s, now_s, str(price), src, tid),
                    )
                    if not c.fetchone():
                        continue  # race lost

                    # Independent calc done — now read monitor for comparison only
                    mon = _read_monitor_result(c, tid)
                    mon_norm = _normalize_result(mon)
                    sh_norm = _normalize_result(final)
                    if mon_norm and mon_norm not in ("OPEN", ""):
                        comparison = "MATCH" if mon_norm == sh_norm else "MISMATCH"
                    else:
                        comparison = "PENDING_MONITOR"

                    c.execute(
                        "UPDATE shadow_verifications SET monitor_result=%s, comparison=%s "
                        "WHERE trade_id=%s",
                        (mon_norm or mon, comparison, tid),
                    )

                    # Atomic notify claim (never Pro channel)
                    c.execute(
                        """
                        UPDATE shadow_verifications
                        SET comparison_notified=1
                        WHERE trade_id=%s AND comparison_notified=0
                        RETURNING trade_id
                        """,
                        (tid,),
                    )
                    if c.fetchone():
                        # Always private admin; SHADOW_VERIFY_PRIVATE_ONLY means never Pro
                        msg = _format_comparison(
                            tid, coin, direction, mon_norm or mon or "OPEN",
                            sh_norm, comparison, entry, stop, t1, t2,
                            generated_at, now_s, src,
                        )
                        _notify_admins(msg)
                        if comparison == "MISMATCH":
                            _notify_admins(
                                "⚠️ <b>MARKET PULSE VERIFICATION MISMATCH</b>\n\n"
                                f"Trade #{tid}\n{coin} · {(direction or '').upper()}\n"
                                f"Monitor: <b>{mon_norm or mon}</b>\n"
                                f"Shadow: <b>{sh_norm}</b>\n"
                                f"Entry {_fmt(entry)} SL {_fmt(stop)} "
                                f"TP1 {_fmt(t1)} TP2 {_fmt(t2)}\n"
                                f"Signal: {generated_at} WAT\n"
                                f"Shadow outcome: {now_s} WAT\n"
                                f"Data: {src}"
                            )
                    logger.info(
                        "[SHADOW] #%s done shadow=%s monitor=%s cmp=%s",
                        tid, final, mon_norm, comparison,
                    )
                else:
                    c.execute(
                        "UPDATE shadow_verifications SET last_price=%s, data_source=%s "
                        "WHERE trade_id=%s",
                        (str(price), src, tid),
                    )
            except Exception as e:
                logger.error("[SHADOW] trade tick isolated: %s", e)
                continue
        db.commit()
    except Exception as e:
        logger.error("[SHADOW] tick: %s", e)
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


def _fmt(v):
    try:
        return format_price(float(v)) if v is not None else "—"
    except Exception:
        return str(v)


def _fmt_when(s) -> str:
    if not s:
        return "n/a"
    try:
        dt = datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%d %b %Y · %H:%M") + " WAT"
    except Exception:
        return f"{s} WAT"


def _format_comparison(
    tid, coin, direction, mon, shadow, comparison, entry, stop, t1, t2, gen, out_ts, src,
):
    icon = "✅" if comparison == "MATCH" else ("❌" if comparison == "MISMATCH" else "⏳")
    return (
        f"🔎 <b>SHADOW VERIFY — TRADE #{tid}</b>\n\n"
        f"{coin}/USDT · {(direction or '').upper()}\n"
        f"Existing Monitor: <b>{mon}</b>\n"
        f"Independent Shadow: <b>{shadow}</b>\n"
        f"Comparison: <b>{comparison}</b> {icon}\n\n"
        f"Entry {_fmt(entry)} · SL {_fmt(stop)}\n"
        f"TP1 {_fmt(t1)} · TP2 {_fmt(t2)}\n"
        f"Generated: {gen} WAT\n"
        f"Shadow outcome: {out_ts} WAT\n"
        f"Data: {src} · resolution: poll (~5m)\n\n"
        f"<i>Private verification · does not change trade_ideas outcome</i>"
    )


def run_shadow_cycle() -> None:
    if not SHADOW_VERIFY_ENABLED:
        return
    try:
        enroll_new_trades()
    except Exception as e:
        logger.error("[SHADOW] enroll cycle: %s", e)
    try:
        tick_shadow_verifications()
    except Exception as e:
        logger.error("[SHADOW] tick cycle: %s", e)
