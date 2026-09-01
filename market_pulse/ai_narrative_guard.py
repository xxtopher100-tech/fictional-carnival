"""AI narrative guard — prevent fabricated causality in MarketPulse text.

Does NOT change entry/stop/TP/direction/R:R or strategy math.
Layer 1 is prompt rules (ai_engine.AI_SYSTEM_PROMPT).
Layer 2 is this sanitizer applied to AI narrative before posting.
"""
from __future__ import annotations

import re
from typing import Optional

try:
    from market_pulse.config_runtime import logger
except Exception:
    import logging
    logger = logging.getLogger("ai_narrative_guard")

# Causal / speculative phrases that invent WHY the market moved without evidence.
# Applied at sentence level. Technical observation language is allowed.
_UNSAFE_SENTENCE = re.compile(
    r"(?is)("
    r"\b(because|due to|owing to|driven by|caused by|as a result of)\b.{0,80}"
    r"\b(naira|nigerian|p2p|dollar scarcity|cbn|whale|whales|institution|institutional|"
    r"retail demand|local demand|profit[- ]taking)\b"
    r"|"
    r"\b(naira|nigerian|p2p|dollar scarcity)\b.{0,80}"
    r"\b(causing|causes|will (push|tighten|cap|force)|caps? (the )?rall|"
    r"tightens?|forces?|drives?|driving)\b"
    r"|"
    r"\b(whales?|institutions?|institutional|smart money)\b.{0,40}\b(accumulat\w*|buying|selling|dumping|driving)\b"
    r"|"
    r"\bpsychological barrier\b"
    r"|"
    r"\boften caps?\b.{0,30}\brall"
    r"|"
    r"\b(naira volatility|dollar scarcity)\b.{0,60}\b(will|could|may|causing|drives?)\b"
    r"|"
    r"\bnigerian (p2p )?traders?\b.{0,50}\b(are buying|are selling|profit[- ]taking|dumping)\b"
    r")"
)

# Allowed technical stems — if sentence is ONLY technical, keep even if "because" of price level
_TECHNICAL_OK = re.compile(
    r"(?is)^(price|the (pair|level|market|candle|trend|setup)|a (close|rejection|breakout|breakdown)|"
    r"rsi|atr|ema|volume|support|resistance|entry|stop|target)"
)

NARRATIVE_RULES_FOR_PROMPTS = (
    "NARRATIVE RULES (mandatory):\n"
    "- Use ONLY facts supplied in the prompt (prices, levels, indicators, labeled P2P status).\n"
    "- Do NOT invent causal links between crypto moves and naira/P2P/CBN/dollar scarcity.\n"
    "- Do NOT invent whale, institutional, or retail trader behavior.\n"
    "- Do NOT invent news or macro events not listed in the prompt.\n"
    "- Technical observation is OK (testing level, rejection, range).\n"
    "- Scenarios must be labeled as scenarios (could / would / if), not as facts.\n"
    "- If P2P is Estimated or Unavailable, say so — never call it live demand.\n"
    "- If you lack evidence for a Nigerian angle, omit it or say data unavailable.\n"
    "- Never change Entry, Stop, Target, direction, or R:R numbers.\n"
)


def sanitize_ai_narrative(
    text: Optional[str],
    *,
    fallback: Optional[str] = None,
) -> str:
    """Remove unsupported causal sentences. Keep technical observations.

    If everything is stripped, return fallback or a short neutral line.
    """
    if not text or not str(text).strip():
        return (fallback or "").strip()

    raw = str(text).strip()
    # Split on sentence boundaries while keeping content
    parts = re.split(r"(?<=[.!?])\s+", raw)
    kept = []
    removed = 0
    for part in parts:
        s = part.strip()
        if not s:
            continue
        if _UNSAFE_SENTENCE.search(s):
            removed += 1
            logger.info("[AI GUARD] stripped unsupported narrative: %s", s[:120])
            continue
        kept.append(s)

    out = " ".join(kept).strip()
    if not out:
        out = (fallback or "Technical levels are shown below. No additional narrative.").strip()
    if removed:
        logger.info("[AI GUARD] removed %s sentence(s)", removed)
    return out


def append_narrative_rules(prompt: str) -> str:
    """Append mandatory narrative rules to a user prompt."""
    p = (prompt or "").rstrip()
    if "NARRATIVE RULES" in p:
        return p
    return p + "\n\n" + NARRATIVE_RULES_FOR_PROMPTS
