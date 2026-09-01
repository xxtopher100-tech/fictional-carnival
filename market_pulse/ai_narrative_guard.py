"""AI narrative guard — prevent fabricated causality in MarketPulse text."""
from __future__ import annotations

import re
from typing import Optional

try:
    from market_pulse.config_runtime import logger
except Exception:
    import logging
    logger = logging.getLogger("ai_narrative_guard")

_UNSAFE_SENTENCE = re.compile(
    r"(?is)("
    r"\b(because|due to|owing to|driven by|caused by|as a result of)\b.{0,80}"
    r"\b(naira|nigerian|p2p|dollar scarcity|cbn|whale|whales|institution|institutional|"
    r"retail demand|local demand|profit[- ]taking)\b"
    r"|"
    r"\b(naira|nigerian|p2p|dollar scarcity)\b.{0,80}"
    r"\b(causing|causes|will (push|tighten|cap|force)|caps? (the )?rall|"
    r"tightens?|forces?|drives?|driving|ease|eases|easing)\b"
    r"|"
    r"\b(whales?|institutions?|institutional|smart money)\b.{0,40}"
    r"\b(accumulat\w*|buying|selling|dumping|driving)\b"
    r"|"
    r"\bpsychological barrier\b"
    r"|"
    r"\boften caps?\b.{0,30}\brall"
    r"|"
    r"\b(naira volatility|dollar scarcity)\b.{0,60}\b(will|could|may|causing|drives?)\b"
    r"|"
    r"\bnigerian (p2p )?traders?\b.{0,50}\b(are buying|are selling|profit[- ]taking|dumping)\b"
    r"|"
    r"\b(naira pressure|naira liquidity)\b"
    r"|"
    r"\b(could|may|will)\b.{0,50}\b(ease|tighten|increase|reduce)\b.{0,40}\b(naira|p2p|liquidity)\b"
    r"|"
    r"\bincurs?\b.{0,50}\bloss\b"
    r"|"
    r"\bnot optimal for entry\b"
    r"|"
    r"\b\d+(?:\.\d+)?\s*%\b.{0,40}\b(loss|cost)\b"
    r"|"
    r"\b(worth converting|makes it worth)\b.{0,30}\b(naira|spread)\b"
    r")"
)

NARRATIVE_RULES_FOR_PROMPTS = (
    "NARRATIVE RULES (mandatory):\n"
    "- Use ONLY facts supplied in the prompt.\n"
    "- Do NOT invent causal links between crypto/FX and naira/P2P/CBN.\n"
    "- Do NOT invent whale/institutional behavior or news not provided.\n"
    "- Do NOT invent % losses from spreads or naira pressure/liquidity claims.\n"
    "- Technical observation is OK. Scenarios use could/would/if.\n"
    "- CONTEXT may only restate provided P2P numbers/status.\n"
    "- Never change Entry, Stop, Target, direction, Confidence, or R:R.\n"
    "- Do NOT invent Confidence different from the prompt.\n"
)


def sanitize_ai_narrative(text: Optional[str], *, fallback: Optional[str] = None) -> str:
    if not text or not str(text).strip():
        return (fallback or "").strip()
    raw = str(text).strip()
    # Also split on em-dash fragments
    parts = re.split(r"(?<=[.!?])\s+|\s+[—–]\s+", raw)
    kept, removed = [], 0
    for part in parts:
        s = part.strip(" .")
        if not s:
            continue
        if _UNSAFE_SENTENCE.search(s):
            removed += 1
            logger.info("[AI GUARD] stripped: %s", s[:120])
            continue
        kept.append(s if s.endswith((".", "!", "?")) else s)
    out = " ".join(kept).strip()
    if not out:
        out = (fallback or "Technical levels are shown below. No additional narrative.").strip()
    if removed:
        logger.info("[AI GUARD] removed %s sentence(s)", removed)
    return out


def append_narrative_rules(prompt: str) -> str:
    p = (prompt or "").rstrip()
    if "NARRATIVE RULES" in p:
        return p
    return p + "\n\n" + NARRATIVE_RULES_FOR_PROMPTS


def format_level_for_prompt(value, decimals: int = 4) -> str:
    try:
        v = float(value)
    except Exception:
        return str(value)
    if abs(v) >= 1000:
        return f"{v:,.2f}"
    if abs(v) >= 1:
        s = f"{v:.{min(decimals, 4)}f}".rstrip("0").rstrip(".")
        return s
    return f"{v:.{max(decimals, 4)}f}".rstrip("0").rstrip(".")


def lock_levels_and_confidence_in_text(
    text: Optional[str],
    *,
    entry=None,
    stop=None,
    target=None,
    confidence: Optional[str] = None,
    decimals: int = 4,
) -> str:
    if not text:
        return ""
    out = str(text)

    def _repl(label: str, val) -> None:
        nonlocal out
        if val is None:
            return
        formatted = format_level_for_prompt(val, decimals=decimals)
        pat = re.compile(rf"(?i)({label}\s*:\s*)\$?[0-9][0-9,]*\.?[0-9]*")
        out = pat.sub(rf"\g<1>${formatted}", out, count=3)

    _repl("Entry", entry)
    _repl("Stop Loss", stop)
    _repl("Stop", stop)
    _repl("Target 1", target)
    _repl("Target", target)
    if confidence:
        conf = str(confidence).strip()
        out = re.sub(
            r"(?i)(Confidence\s*:\s*)(High|Moderate|Low|Uncertain|Medium)\b",
            rf"\1{conf}",
            out,
        )
    return out


def neutral_p2p_context(p2p_status: str = "Unavailable", detail: str = "") -> str:
    st = (p2p_status or "Unavailable").strip()
    d = (detail or "").strip()
    if d:
        return (
            f"CONTEXT: P2P data ({st}): {d}. "
            f"Shown separately from the technical setup — no causal link is assumed."
        )
    return (
        f"CONTEXT: P2P data is currently {st}. "
        f"No causal relationship with this technical setup is assumed."
    )
