"""MarketPulse v3.1 blueprint helpers — data quality, final price check, version tag.

Does NOT change ATR/R:R math. Aligns discovery/publish gates with the Final Blueprint.
"""
from __future__ import annotations

import time
from typing import Any, Optional, Tuple

try:
    from market_pulse.config_runtime import logger
except Exception:
    import logging
    logger = logging.getLogger("blueprint_v31")

BLUEPRINT_VERSION = "3.1"

# Status codes for audit ledger (blueprint §6)
STATUS_SKIPPED_DATA_QUALITY = "SKIPPED_DATA_QUALITY"
STATUS_EXPIRED_BEFORE_PUBLISH = "EXPIRED_BEFORE_PUBLISH"
STATUS_REJECTED = "REJECTED"
STATUS_SUPPRESSED = "SUPPRESSED"
STATUS_PUBLISHED = "PUBLISHED"

# Soft freshness (seconds) — WS tick age if available
_MAX_PRICE_AGE_SEC = 180
_MAX_ENTRY_DRIFT_PCT = 0.75  # final price check vs setup entry


def assess_crypto_price_quality(coin: str, price: Optional[float]) -> Tuple[str, str]:
    """Return (OK|DEGRADED|BLOCKED, reason).

    BLOCKED → skip discovery for this asset this cycle.
    DEGRADED → allow but log (soft).
    OK → normal path.
    """
    if not price or price <= 0:
        return "BLOCKED", "PRICE_UNAVAILABLE"
    # Optional WS age from price engine cache metadata
    age = None
    try:
        from market_pulse.price_engine import _ws_price_cache, _ws_lock
        with _ws_lock:
            meta = _ws_price_cache.get(coin) or _ws_price_cache.get(str(coin).upper())
        if isinstance(meta, dict):
            ts = meta.get("ts") or meta.get("time") or meta.get("updated")
            if ts is not None:
                try:
                    age = time.time() - float(ts)
                except Exception:
                    age = None
        elif isinstance(meta, (list, tuple)) and len(meta) >= 2:
            # (price, change) — no age
            pass
    except Exception:
        age = None

    if age is not None and age > _MAX_PRICE_AGE_SEC * 3:
        return "BLOCKED", f"STALE_PRICE age={age:.0f}s"
    if age is not None and age > _MAX_PRICE_AGE_SEC:
        return "DEGRADED", f"PRICE_AGING age={age:.0f}s"
    return "OK", "PRICE_OK"


def final_price_check(
    coin: str,
    entry: float,
    direction: str,
    *,
    max_drift_pct: float = _MAX_ENTRY_DRIFT_PCT,
) -> Tuple[bool, str, Optional[float]]:
    """Blueprint §3 final price check before publish.

    If live price has moved too far from planned entry, expire-before-publish.
    """
    try:
        from market_pulse.price_fetchers import get_best_price
        live, _ = get_best_price(coin)
    except Exception as e:
        return False, f"PRICE_CHECK_FAILED:{e}", None
    if not live or live <= 0:
        return False, "PRICE_UNAVAILABLE", None
    if not entry or entry <= 0:
        return True, "NO_ENTRY_TO_COMPARE", live
    drift = abs(float(live) - float(entry)) / float(entry) * 100.0
    if drift > max_drift_pct:
        return False, f"EXPIRED_BEFORE_PUBLISH drift={drift:.2f}%", live
    return True, f"PRICE_STILL_VALID drift={drift:.2f}%", live


def build_immutable_snapshot(
    *,
    coin: str,
    tier: str,
    direction: str,
    entry: Any,
    stop: Any,
    target1: Any,
    target2: Any = None,
    timeframe: str = "",
    score: float = 0.0,
    blueprint: str = BLUEPRINT_VERSION,
) -> dict:
    """Minimal immutable setup snapshot for audit (numbers frozen at decision time)."""
    return {
        "blueprint": blueprint,
        "coin": coin,
        "tier": tier,
        "direction": direction,
        "timeframe": timeframe,
        "entry": entry,
        "stop": stop,
        "target1": target1,
        "target2": target2,
        "score": score,
        "frozen_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
