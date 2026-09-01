"""Message integrity helpers — presentation only, no strategy math."""
from __future__ import annotations

import os
import re
from typing import Optional, Tuple

try:
    from market_pulse.config_runtime import logger
except Exception:
    import logging
    logger = logging.getLogger("message_integrity")

# BTC/ETH/etc technical levels must never show ₦ unless quote is NGN.
_NAIRA_BEFORE_USD_LEVEL = re.compile(
    r"(?i)(₦|NGN\s*)(\s*)(\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+\.\d+)"
)


def strip_spurious_naira_from_crypto_text(text: str) -> str:
    """Remove ₦ attached to values that are clearly USD-style crypto levels.

    Does NOT touch legitimate P2P lines like Buy ₦1,500 (handled by leaving
    small-integer naira alone when context is USDT/NGN — conservative: only
    strip ₦ before numbers >= 1000 with decimals OR numbers looking like BTC).
    """
    if not text:
        return text

    def repl(m):
        num = m.group(3).replace(",", "")
        try:
            v = float(num)
        except Exception:
            return m.group(0)
        # Typical USDT/NGN rates 1000-3000 integer: keep
        if v < 5000 and "." not in m.group(3) and v == int(v):
            return m.group(0)
        # Crypto USD levels (BTC ~70k, ETH ~2k with decimals, etc.)
        return m.group(3) if "." in m.group(3) or v >= 5000 else m.group(0)

    out = _NAIRA_BEFORE_USD_LEVEL.sub(repl, text)
    # Also fix patterns like "Entry: ₦77265.79"
    out = re.sub(
        r"(?i)((?:Entry|Stop|Target|TP\s*1|TP\s*2|Price|Level)\s*:?\s*)₦(\d[\d,]*\.\d+)",
        r"\1$\2",
        out,
    )
    out = re.sub(
        r"(?i)((?:Entry|Stop|Target|TP\s*1|TP\s*2|Price|Level)\s*:?\s*)₦(\d{5,})",
        r"\1$\2",
        out,
    )
    return out


def count_standard_footers(text: str) -> int:
    if not text:
        return 0
    return len(re.findall(r"(?i)Illustrative only", text))


def similar_setup_env_hours() -> float:
    try:
        return float(os.environ.get("SIMILAR_SETUP_HOURS", "6"))
    except Exception:
        return 6.0


def similar_entry_pct() -> float:
    try:
        return float(os.environ.get("SIMILAR_ENTRY_PCT", "1.5"))
    except Exception:
        return 1.5


def classify_vs_active_open(
    coin: str,
    direction: str,
    timeframe: str,
    entry: float,
    *,
    hours: Optional[float] = None,
    entry_pct: Optional[float] = None,
) -> Tuple[str, Optional[int]]:
    """Compare candidate to open trade_ideas.

    Returns (NEW_SETUP | SIMILAR_ACTIVE_SETUP | REFRESH_EXISTING, existing_id|None)
    Uses only existing open ledger rows — no new thresholds beyond env config.
    """
    hours = similar_setup_env_hours() if hours is None else hours
    entry_pct = similar_entry_pct() if entry_pct is None else entry_pct
    if not coin or not entry:
        return "NEW_SETUP", None
    try:
        from market_pulse.db import get_db
        from market_pulse.helpers import wat_now
        from datetime import timedelta
    except Exception:
        return "NEW_SETUP", None

    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            """
            SELECT id, direction, timeframe, entry, created_at
            FROM trade_ideas
            WHERE status = 'open' AND UPPER(coin) = UPPER(%s)
            ORDER BY id DESC
            LIMIT 20
            """,
            (coin,),
        )
        rows = c.fetchall() or []
        now = wat_now()
        d_new = (direction or "").lower()
        tf_new = (timeframe or "").upper()
        for row in rows:
            eid, d_old, tf_old, entry_s, created = row
            try:
                e_old = float(str(entry_s).replace(",", "").replace("$", "").replace("₦", ""))
            except Exception:
                continue
            d_old_l = (d_old or "").lower()
            same_dir = (
                ("long" in d_new or "buy" in d_new) and ("long" in d_old_l or "buy" in d_old_l)
            ) or (
                ("short" in d_new or "sell" in d_new) and ("short" in d_old_l or "sell" in d_old_l)
            )
            if not same_dir:
                continue
            tf_old_u = (tf_old or "").upper()
            same_tf = (not tf_new) or (not tf_old_u) or (tf_new == tf_old_u)
            if not same_tf:
                continue
            if e_old <= 0:
                continue
            dist = abs(float(entry) - e_old) / e_old * 100.0
            age_ok = True
            if created:
                try:
                    from datetime import datetime
                    cdt = datetime.strptime(str(created)[:19], "%Y-%m-%d %H:%M:%S")
                    age_ok = (now - cdt) <= timedelta(hours=hours)
                except Exception:
                    age_ok = True
            if dist <= entry_pct and age_ok:
                logger.info(
                    "[SIMILAR SETUP] %s %s ~ #%s entry_dist=%.2f%% within %sh",
                    coin, direction, eid, dist, hours,
                )
                return "SIMILAR_ACTIVE_SETUP", int(eid)
        return "NEW_SETUP", None
    except Exception as e:
        logger.debug("[SIMILAR SETUP] %s", e)
        return "NEW_SETUP", None
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass
