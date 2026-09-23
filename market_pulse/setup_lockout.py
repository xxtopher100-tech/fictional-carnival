"""
Post-SL setup lockout — prevent immediate re-buy of the same thesis.

After STOP_HIT on a direction:
  - Block same symbol + same direction for LOCKOUT_HOURS
  - Require price to reclaim past the failed entry (long: price > entry;
    short: price < entry) before allowing a new setup
  - New setup must recalculate levels (always true if generator runs fresh)

Does not change R:R math. Used by forex + crypto generators + lifecycle.
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional, Tuple

from market_pulse.config_runtime import logger
from market_pulse.db import get_db

LOCKOUT_HOURS = float(os.environ.get("SETUP_LOCKOUT_HOURS", "6"))
RECLAIM_BUFFER_PCT = float(os.environ.get("SETUP_RECLAIM_BUFFER_PCT", "0.05"))  # 0.05% past entry


def _norm_symbol(sym: str) -> str:
    return (sym or "").upper().replace(" ", "").strip()


def _norm_dir(direction: str) -> str:
    d = (direction or "").lower()
    if "sell" in d or "short" in d:
        return "short"
    if "buy" in d or "long" in d:
        return "long"
    return d.strip() or "long"


def _ensure_table(c) -> None:
    try:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS setup_lockouts (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                failed_entry DOUBLE PRECISION,
                failed_stop DOUBLE PRECISION,
                idea_id INTEGER,
                locked_at DOUBLE PRECISION NOT NULL,
                unlock_after DOUBLE PRECISION NOT NULL,
                reason TEXT,
                UNIQUE(symbol, direction)
            )
            """
        )
    except Exception:
        pass


def record_stop_lockout(
    *,
    symbol: str,
    direction: str,
    entry: Any = None,
    stop: Any = None,
    idea_id: Optional[int] = None,
    hours: Optional[float] = None,
) -> None:
    """Call when a trade hits STOP — blocks same-direction re-entry."""
    sym = _norm_symbol(symbol)
    direction = _norm_dir(direction)
    if not sym:
        return
    try:
        entry_f = float(entry) if entry is not None else None
    except Exception:
        entry_f = None
    try:
        stop_f = float(stop) if stop is not None else None
    except Exception:
        stop_f = None

    now = time.time()
    hrs = float(hours if hours is not None else LOCKOUT_HOURS)
    unlock = now + max(0.5, hrs) * 3600.0

    db = None
    try:
        db = get_db()
        c = db.cursor()
        _ensure_table(c)
        c.execute(
            """
            INSERT INTO setup_lockouts
                (symbol, direction, failed_entry, failed_stop, idea_id, locked_at, unlock_after, reason)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (symbol, direction) DO UPDATE SET
                failed_entry = EXCLUDED.failed_entry,
                failed_stop = EXCLUDED.failed_stop,
                idea_id = EXCLUDED.idea_id,
                locked_at = EXCLUDED.locked_at,
                unlock_after = EXCLUDED.unlock_after,
                reason = EXCLUDED.reason
            """,
            (
                sym,
                direction,
                entry_f,
                stop_f,
                int(idea_id) if idea_id else None,
                now,
                unlock,
                "STOP_HIT",
            ),
        )
        db.commit()
        logger.info(
            "[LOCKOUT] %s %s after STOP entry=%s unlock_in=%.1fh",
            sym, direction, entry_f, hrs,
        )
    except Exception as e:
        logger.warning("[LOCKOUT] record failed %s %s: %s", sym, direction, e)
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


def clear_lockout(symbol: str, direction: str) -> None:
    sym = _norm_symbol(symbol)
    direction = _norm_dir(direction)
    db = None
    try:
        db = get_db()
        c = db.cursor()
        _ensure_table(c)
        c.execute(
            "DELETE FROM setup_lockouts WHERE symbol=%s AND direction=%s",
            (sym, direction),
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


def _live_price(symbol: str) -> Optional[float]:
    sym = _norm_symbol(symbol)
    # Forex pairs
    if "/" in sym:
        try:
            from market_pulse.forex_trade_engine import get_forex_rate
            rate, _, _, _ = get_forex_rate(sym)
            if rate:
                return float(rate)
        except Exception:
            pass
    try:
        from market_pulse.price_fetchers import get_best_price
        p = get_best_price(sym.split("/")[0] if "/" in sym else sym)
        if p is not None:
            return float(p)
    except Exception:
        pass
    return None


def is_setup_blocked(
    symbol: str,
    direction: str,
    *,
    live_price: Optional[float] = None,
) -> Tuple[bool, str]:
    """
    Returns (blocked, reason).
    Unblocks only when time elapsed AND price reclaimed past failed entry.
    """
    sym = _norm_symbol(symbol)
    direction = _norm_dir(direction)
    if not sym:
        return False, ""

    db = None
    try:
        db = get_db()
        c = db.cursor()
        _ensure_table(c)
        c.execute(
            """
            SELECT failed_entry, failed_stop, unlock_after, locked_at, idea_id
            FROM setup_lockouts
            WHERE symbol=%s AND direction=%s
            LIMIT 1
            """,
            (sym, direction),
        )
        row = c.fetchone()
        if not row:
            return False, ""

        failed_entry, failed_stop, unlock_after, locked_at, idea_id = row
        now = time.time()
        unlock_after = float(unlock_after or 0)

        if now < unlock_after:
            left = (unlock_after - now) / 3600.0
            return True, f"POST_SL_COOLDOWN:{left:.1f}h"

        # Time passed — require reclaim
        try:
            fe = float(failed_entry) if failed_entry is not None else None
        except Exception:
            fe = None

        if fe is None or fe <= 0:
            clear_lockout(sym, direction)
            return False, ""

        price = live_price if live_price is not None else _live_price(sym)
        if price is None:
            return True, "POST_SL_WAIT_PRICE"

        buf = abs(fe) * (RECLAIM_BUFFER_PCT / 100.0)
        if direction == "long":
            # Must reclaim above failed entry
            if price < fe + buf:
                return True, f"POST_SL_NEED_RECLAIM_ABOVE:{fe}"
        else:
            if price > fe - buf:
                return True, f"POST_SL_NEED_RECLAIM_BELOW:{fe}"

        # Reclaimed + time ok → clear and allow
        clear_lockout(sym, direction)
        logger.info(
            "[LOCKOUT] cleared %s %s — reclaimed price=%s entry_was=%s",
            sym, direction, price, fe,
        )
        return False, ""
    except Exception as e:
        logger.debug("[LOCKOUT] check %s: %s", sym, e)
        return False, ""
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def has_open_same_direction(symbol: str, direction: str) -> bool:
    """True if an open trade_ideas row exists for symbol+direction."""
    sym = _norm_symbol(symbol)
    direction = _norm_dir(direction)
    db = None
    try:
        db = get_db()
        c = db.cursor()
        if direction == "long":
            d_like = "%buy%"
            d_like2 = "%long%"
        else:
            d_like = "%sell%"
            d_like2 = "%short%"
        c.execute(
            """
            SELECT id FROM trade_ideas
            WHERE status='open' AND UPPER(REPLACE(coin,' ','')) = %s
              AND (direction ILIKE %s OR direction ILIKE %s)
            LIMIT 1
            """,
            (sym, d_like, d_like2),
        )
        return c.fetchone() is not None
    except Exception:
        return False
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass
