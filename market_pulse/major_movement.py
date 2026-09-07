"""
Major-movement detector — early intelligence, NOT a trade signal.

Evidence model: price displacement, volatility expansion, structure break,
liquidations, funding — multiple categories strengthen the signal.
Does not invent entry/stop/tp.
"""

from __future__ import annotations

from market_pulse.config_runtime import logger
from market_pulse.opportunity_engine import assess_opportunity, CORE_MAJORS, PRIORITY_COINS

# Cooldown memory process-local
_last_alert: dict[str, float] = {}
_COOLDOWN_SEC = 45 * 60  # 45 minutes per coin


def detect_major_movement(coin: str) -> dict:
    """
    Returns {
      active: bool,
      direction: bullish|bearish|uncertain,
      confidence: 0-100,
      evidence: [str],
      size: SMALL|NORMAL|BIG,
      opportunity: dict,
    }
    """
    coin = (coin or "").upper().split("/")[0]
    opp = assess_opportunity(coin)
    evidence = list(opp.get("reasons") or [])
    size = (opp.get("size") or "NONE").upper()
    trend = opp.get("trend") or "neutral"
    score = 0
    if size == "BIG":
        score += 40
    elif size == "NORMAL":
        score += 20
    if trend in ("bullish", "bearish"):
        score += 15
    if any("vol_expand" in e or "vol_expansion" in e for e in evidence):
        score += 15
    if any("multi_day" in e for e in evidence):
        score += 20
    if any(e.startswith("liq_") for e in evidence):
        score += 10
    if opp.get("liquidations") == "heavy":
        score += 10
    if coin in CORE_MAJORS:
        score += 5

    # Extreme greed + only bullish big → cap confidence (no chase)
    fg = opp.get("fg")
    if fg is not None and fg >= 80 and trend == "bullish":
        score = min(score, 55)
        evidence.append("greed_cap")

    direction = "uncertain"
    if trend == "bullish":
        direction = "bullish"
    elif trend == "bearish":
        direction = "bearish"

    active = score >= 50 and size in ("NORMAL", "BIG")
    return {
        "coin": coin,
        "active": active,
        "direction": direction,
        "confidence": min(100, score),
        "evidence": evidence[:10],
        "size": size,
        "opportunity": opp,
    }


def format_major_move_alert(det: dict) -> str:
    coin = det.get("coin")
    direction = det.get("direction")
    conf = det.get("confidence")
    size = det.get("size")
    ev = det.get("evidence") or []
    lines = [
        f"⚡ MAJOR MOVE DEVELOPING — {coin}",
        f"Bias: {direction} · Size: {size} · Confidence: {conf}/100",
        "",
        "Evidence (facts only):",
    ]
    for e in ev[:6]:
        lines.append(f"· {e}")
    lines += [
        "",
        "This is NOT a trade entry.",
        "Wait for a qualified setup with entry, stop, and targets.",
        "NFA — DYOR",
    ]
    return "\n".join(lines)


def should_emit_alert(coin: str, det: dict) -> bool:
    import time
    if not det.get("active"):
        return False
    now = time.time()
    last = _last_alert.get(coin, 0)
    if now - last < _COOLDOWN_SEC:
        return False
    _last_alert[coin] = now
    return True


def scan_majors(coins: list[str] | None = None) -> list[dict]:
    coins = coins or sorted(PRIORITY_COINS)
    out = []
    for coin in coins:
        try:
            det = detect_major_movement(coin)
            if det.get("active"):
                out.append(det)
                logger.info(
                    "[MAJOR MOVE] %s active conf=%s size=%s dir=%s",
                    coin, det.get("confidence"), det.get("size"), det.get("direction"),
                )
        except Exception as e:
            logger.debug("[MAJOR MOVE] %s: %s", coin, e)
    return out
