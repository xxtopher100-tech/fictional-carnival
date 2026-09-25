"""
Post-SL setup lockout — smart re-entry, not a dumb timer.

After STOP_HIT on symbol+direction:
  1) Minimum pause (default 1h) — no instant re-entry of the same idea
  2) After that: allow only if price RECLAIMS past failed entry
     - long stop → price must trade back above failed entry
     - short stop → price must trade back below failed entry
  3) Max age (default 24h) — lockout expires so the bot is not frozen forever
  4) Unpublished open zombies do not block new setups (only PUBLISHED opens)

Used by forex + crypto generators + lifecycle.
"""

from __future__ import annotations

import os
import time
from typing import Any, Optional, Tuple

from market_pulse.config_runtime import logger
from market_pulse.db import get_db

# Min pause after SL before any re-check (hours)
MIN_WAIT_HOURS = float(os.environ.get("SETUP_LOCKOUT_MIN_HOURS", "1"))
# Max time a lockout can block without reclaim (hours)
MAX_LOCKOUT_HOURS = float(os.environ.get("SETUP_LOCKOUT_HOURS", "24"))
RECLAIM_BUFFER_PCT = float(os.environ.get("SETUP_RECLAIM_BUFFER_PCT", "0.05"))


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
    """Call when a trade hits STOP — blocks same-direction re-entry until confirm."""
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
    # unlock_after = earliest time we even look at reclaim (min wait)
    min_h = float(hours if hours is not None else MIN_WAIT_HOURS)
    unlock = now + max(0.25, min_h) * 3600.0

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
            "[LOCKOUT] %s %s after STOP entry=%s min_wait=%.1fh max=%.1fh",
            sym, direction, entry_f, min_h, MAX_LOCKOUT_HOURS,
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
        coin = sym.split("/")[0] if "/" in sym else sym
        p = get_best_price(coin)
        if isinstance(p, (list, tuple)):
            p = p[0]
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

    Logic:
      - Before min wait → blocked
      - After max lockout age → clear, allow
      - Else require price reclaim past failed entry
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
        locked_at = float(locked_at or 0)
        unlock_after = float(unlock_after or 0)

        # 1) Minimum pause
        if now < unlock_after:
            left = (unlock_after - now) / 3600.0
            return True, f"POST_SL_MIN_WAIT:{left:.1f}h"

        # 2) Max age — do not freeze forever
        if locked_at and (now - locked_at) >= MAX_LOCKOUT_HOURS * 3600.0:
            clear_lockout(sym, direction)
            logger.info("[LOCKOUT] expired max age %s %s", sym, direction)
            return False, ""

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
            if price < fe + buf:
                return True, f"POST_SL_NEED_RECLAIM_ABOVE:{fe}"
        else:
            if price > fe - buf:
                return True, f"POST_SL_NEED_RECLAIM_BELOW:{fe}"

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
    """
    Only PUBLISHED open trades block a new setup.
    Zombie / research opens must not freeze the scanner.
    """
    sym = _norm_symbol(symbol)
    direction = _norm_dir(direction)
    db = None
    try:
        db = get_db()
        c = db.cursor()
        if direction == "long":
            d_like, d_like2 = "%buy%", "%long%"
        else:
            d_like, d_like2 = "%sell%", "%short%"
        try:
            c.execute(
                """
                SELECT id FROM trade_ideas
                WHERE status='open'
                  AND UPPER(REPLACE(coin,' ','')) = %s
                  AND (direction ILIKE %s OR direction ILIKE %s)
                  AND UPPER(COALESCE(publication_status,'')) = 'PUBLISHED'
                LIMIT 1
                """,
                (sym, d_like, d_like2),
            )
            if c.fetchone():
                return True
        except Exception:
            pass
        c.execute(
            """
            SELECT id FROM trade_ideas
            WHERE status='open'
              AND UPPER(REPLACE(coin,' ','')) = %s
              AND (direction ILIKE %s OR direction ILIKE %s)
              AND UPPER(COALESCE(result,'')) NOT IN
                  ('STOP_HIT','TP1_HIT','TP2_HIT','EXPIRED','AMBIGUOUS','BE_EXIT')
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
