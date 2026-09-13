"""
Trade setup engine v2 — orchestration layer.

Uses existing setup/edge level math; adds opportunity + major-move context
and hard risk rejection. Does not invent Entry/SL/TP via AI.
"""

from __future__ import annotations

from market_pulse.config_runtime import logger
from market_pulse.opportunity_engine import assess_opportunity
from market_pulse.major_movement import detect_major_movement

# Reuse proven generators
from market_pulse.edge_trade_engine import generate_trade_idea
from market_pulse.trade_scanner import (
    _risk_is_tradeable,
    MIN_PUBLISH_SCORE,
)


def generate_qualified_setup(coin: str, tier: str = "momentum") -> dict:
    """
    Returns {
      ok: bool,
      reason: str,
      msg, trade, idea_id,
      opportunity, major_move,
    }
    """
    coin = (coin or "").upper().split("/")[0]
    tier = (tier or "momentum").lower()
    opp = assess_opportunity(coin)
    major = detect_major_movement(coin)

    try:
        msg, trade, idea_id = generate_trade_idea(coin, tier)
    except Exception as e:
        return {"ok": False, "reason": f"generate_error:{e}", "opportunity": opp, "major_move": major}

    if not trade or not idea_id:
        return {
            "ok": False,
            "reason": "NO_SETUP",
            "opportunity": opp,
            "major_move": major,
        }

    ok_risk, risk_reason = _risk_is_tradeable(
        trade.get("entry"),
        trade.get("stop"),
        trade.get("direction") or "long",
        asset_type="crypto",
    )
    if not ok_risk:
        return {
            "ok": False,
            "reason": f"RISK_REJECT:{risk_reason}",
            "trade": trade,
            "idea_id": idea_id,
            "opportunity": opp,
            "major_move": major,
        }

    trade["_opportunity_size"] = opp.get("size")
    trade["_major_move"] = major.get("active")
    trade["_major_confidence"] = major.get("confidence")

    return {
        "ok": True,
        "reason": "OK",
        "msg": msg,
        "trade": trade,
        "idea_id": idea_id,
        "opportunity": opp,
        "major_move": major,
    }


def run_setup_pass(coins: list[str], tiers: list[str] | None = None) -> list[dict]:
    """Generate candidates for a coin list (no publish — caller publishes)."""
    tiers = tiers or ["steady", "momentum", "edge"]
    results = []
    for coin in coins:
        for tier in tiers:
            try:
                r = generate_qualified_setup(coin, tier)
                results.append({"coin": coin, "tier": tier, **r})
                if r.get("ok"):
                    logger.info(
                        "[SETUP V2] %s %s ok size=%s major=%s",
                        coin, tier, (r.get("opportunity") or {}).get("size"),
                        (r.get("major_move") or {}).get("active"),
                    )
                    break  # one tier per coin per pass
            except Exception as e:
                logger.debug("[SETUP V2] %s %s: %s", coin, tier, e)
    return results
