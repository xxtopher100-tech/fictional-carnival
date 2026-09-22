"""
Major-movement detector — early intelligence + setup bridge.

Flow (product):
  1) Detect strong move (confidence / size)
  2) Optional quiet public "DEVELOPING" (anti-spam)
  3) Register pending if conf >= SETUP_TRIGGER_CONF
  4) Wait CONFIRM_WAIT_SEC, re-check move still valid
  5) Run setup logic once — publish trade only if it passes

Does not invent entry/stop/tp on the move alert itself.
"""

from __future__ import annotations

import time
from typing import Any

from market_pulse.config_runtime import logger
from market_pulse.opportunity_engine import assess_opportunity, CORE_MAJORS, PRIORITY_COINS

# ── Public alert anti-spam ──────────────────────────────────────────────────
_last_alert: dict[str, float] = {}
_COOLDOWN_SEC = 3 * 60 * 60  # 3h per coin for public DEVELOPING posts
_PUBLIC_MIN_CONF = 80  # only post DEVELOPING if confidence high
_PUBLIC_REQUIRE_BIG = True  # public posts: BIG size only
_global_alert_times: list[float] = []
_MAX_PUBLIC_PER_HOUR = 2

# ── Move → setup bridge ─────────────────────────────────────────────────────
SETUP_TRIGGER_CONF = 70  # attempt setup path at/above this
CONFIRM_WAIT_SEC = 20 * 60  # wait ~20m so spike can prove itself
SETUP_ATTEMPT_COOLDOWN_SEC = 4 * 60 * 60  # per coin after a setup attempt
_pending: dict[str, dict[str, Any]] = {}
_last_setup_attempt: dict[str, float] = {}


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
        "Bot will attempt a qualified setup after confirmation — only if levels pass.",
        "NFA — DYOR",
    ]
    return "\n".join(lines)


def _trim_global_alerts(now: float) -> None:
    global _global_alert_times
    _global_alert_times = [t for t in _global_alert_times if now - t < 3600]


def should_emit_alert(coin: str, det: dict) -> bool:
    """Strict public DEVELOPING gate — cuts channel spam."""
    if not det.get("active"):
        return False
    conf = int(det.get("confidence") or 0)
    size = (det.get("size") or "").upper()
    if conf < _PUBLIC_MIN_CONF:
        return False
    if _PUBLIC_REQUIRE_BIG and size != "BIG":
        return False

    now = time.time()
    last = _last_alert.get(coin, 0)
    if now - last < _COOLDOWN_SEC:
        return False

    _trim_global_alerts(now)
    if len(_global_alert_times) >= _MAX_PUBLIC_PER_HOUR:
        logger.info("[MAJOR MOVE] public cap (%s/h) — skip %s", _MAX_PUBLIC_PER_HOUR, coin)
        return False

    _last_alert[coin] = now
    _global_alert_times.append(now)
    return True


def register_pending_move(det: dict) -> bool:
    """
    Queue a move for later setup attempt when conf >= SETUP_TRIGGER_CONF.
    Does not publish a trade.
    """
    if not det.get("active"):
        return False
    conf = int(det.get("confidence") or 0)
    if conf < SETUP_TRIGGER_CONF:
        return False
    direction = (det.get("direction") or "uncertain").lower()
    if direction not in ("bullish", "bearish"):
        return False

    coin = (det.get("coin") or "").upper()
    if not coin:
        return False

    now = time.time()
    last_try = _last_setup_attempt.get(coin, 0)
    if now - last_try < SETUP_ATTEMPT_COOLDOWN_SEC:
        logger.debug("[MOVE BRIDGE] %s setup cooldown active", coin)
        return False

    existing = _pending.get(coin)
    if existing:
        existing["confidence"] = max(int(existing.get("confidence") or 0), conf)
        existing["size"] = det.get("size") or existing.get("size")
        existing["direction"] = direction
        logger.debug("[MOVE BRIDGE] refreshed pending %s conf=%s", coin, existing["confidence"])
        return True

    _pending[coin] = {
        "registered_at": now,
        "attempt_after": now + CONFIRM_WAIT_SEC,
        "direction": direction,
        "confidence": conf,
        "size": (det.get("size") or "NORMAL").upper(),
    }
    logger.info(
        "[MOVE BRIDGE] pending %s conf=%s size=%s dir=%s wait=%sm",
        coin, conf, det.get("size"), direction, CONFIRM_WAIT_SEC // 60,
    )
    return True


def _tier_for_move(size: str, conf: int) -> str:
    size = (size or "").upper()
    if size == "BIG" and conf >= 85:
        return "edge"
    if size == "BIG" or conf >= 80:
        return "momentum"
    return "steady"


def process_pending_setups() -> dict:
    """
    After CONFIRM_WAIT_SEC: re-detect move; if still valid, try one setup.
    Publish only via publication_gate when setup passes.
    """
    stats = {"checked": 0, "expired": 0, "setup_ok": 0, "setup_fail": 0, "published": 0}
    now = time.time()
    due = [c for c, p in list(_pending.items()) if now >= float(p.get("attempt_after") or 0)]
    if not due:
        return stats

    try:
        from market_pulse.trade_setup_engine import generate_qualified_setup
    except Exception as e:
        logger.warning("[MOVE BRIDGE] setup import: %s", e)
        return stats

    try:
        from market_pulse.publication_gate import publish_canonical_trade
    except Exception as e:
        logger.warning("[MOVE BRIDGE] publish import: %s", e)
        publish_canonical_trade = None  # type: ignore

    for coin in due:
        stats["checked"] += 1
        meta = _pending.pop(coin, None) or {}
        _last_setup_attempt[coin] = now

        try:
            det = detect_major_movement(coin)
        except Exception as e:
            stats["setup_fail"] += 1
            logger.warning("[MOVE BRIDGE] redetect %s: %s", coin, e)
            continue

        still = bool(det.get("active"))
        conf = int(det.get("confidence") or 0)
        direction = (det.get("direction") or "").lower()
        orig_dir = (meta.get("direction") or "").lower()

        if not still or conf < SETUP_TRIGGER_CONF:
            stats["expired"] += 1
            logger.info(
                "[MOVE BRIDGE] %s expired/weak after wait active=%s conf=%s",
                coin, still, conf,
            )
            continue
        if direction != orig_dir or direction not in ("bullish", "bearish"):
            stats["expired"] += 1
            logger.info(
                "[MOVE BRIDGE] %s direction changed %s → %s — skip setup",
                coin, orig_dir, direction,
            )
            continue

        tier = _tier_for_move(det.get("size") or meta.get("size"), conf)
        try:
            result = generate_qualified_setup(coin, tier)
        except Exception as e:
            stats["setup_fail"] += 1
            logger.warning("[MOVE BRIDGE] setup %s: %s", coin, e)
            continue

        if not result.get("ok"):
            stats["setup_fail"] += 1
            logger.info(
                "[MOVE BRIDGE] %s no setup after confirm reason=%s",
                coin, result.get("reason"),
            )
            continue

        stats["setup_ok"] += 1
        msg = result.get("msg")
        trade = result.get("trade") or {}
        idea_id = result.get("idea_id")
        if not msg or not idea_id or not publish_canonical_trade:
            stats["setup_fail"] += 1
            continue

        try:
            ok, reason = publish_canonical_trade(
                msg=msg,
                idea_id=int(idea_id),
                symbol=coin,
                direction=trade.get("direction") or (
                    "long" if direction == "bullish" else "short"
                ),
                timeframe=trade.get("timeframe") or "1H",
                entry=trade.get("entry"),
                stop=trade.get("stop"),
                target1=trade.get("target1"),
                market_type="crypto",
                tier=tier,
                source="move_bridge",
            )
            if ok:
                stats["published"] += 1
                logger.info(
                    "[MOVE BRIDGE] PUBLISHED %s tier=%s idea=#%s conf=%s",
                    coin, tier, idea_id, conf,
                )
            else:
                logger.info(
                    "[MOVE BRIDGE] publish suppressed %s reason=%s",
                    coin, reason,
                )
        except Exception as e:
            stats["setup_fail"] += 1
            logger.warning("[MOVE BRIDGE] publish %s: %s", coin, e)

    return stats


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
                try:
                    register_pending_move(det)
                except Exception as re:
                    logger.debug("[MOVE BRIDGE] register %s: %s", coin, re)
        except Exception as e:
            logger.debug("[MAJOR MOVE] %s: %s", coin, e)
    return out
