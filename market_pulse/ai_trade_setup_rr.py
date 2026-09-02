"""AI Trade Setup — programmatic R:R (authoritative).

Extracted from handlers.py. Entry-zone rule: midpoint of range for R:R.
"""
from __future__ import annotations

import re
from typing import Any, Optional

from market_pulse.helpers import format_price

def _ts_parse_price_token(raw):
    """Parse a single price token like '$76,800' or '76800.5' -> float or None."""
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "").replace("$", "").replace("\u20a6", "")
    s = re.sub(r"[^\d.\-]", "", s)
    if not s or s in (".", "-", "-."):
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _ts_extract_trade_levels(ai_text):
    """
    Extract entry/stop/TP1/TP2/TP3 from free-form AI trade-setup text.
    Returns dict with floats (and entry_low/entry_high if a zone was given).
    Missing keys are None.

    AI R:R claim lines are stripped first so phrases like "TP1 3:1" cannot
    be mistaken for price levels.
    """
    if not ai_text:
        return {}
    text = _ts_strip_ai_rr_claims(ai_text)

    def _first(patterns, group=1):
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                return _ts_parse_price_token(m.group(group))
        return None

    # Entry zone: $76,800–$77,000 or 76800-77000
    entry_low = entry_high = entry = None
    m = re.search(
        r"Entry(?:\s*(?:zone|price|range))?[:\s]+\$?([\d,\.]+)\s*[-–—to]+\s*\$?([\d,\.]+)",
        text, re.IGNORECASE,
    )
    if m:
        entry_low = _ts_parse_price_token(m.group(1))
        entry_high = _ts_parse_price_token(m.group(2))
        if entry_low and entry_high:
            if entry_low > entry_high:
                entry_low, entry_high = entry_high, entry_low
            entry = (entry_low + entry_high) / 2.0  # midpoint rule
    if entry is None:
        entry = _first([
            r"Entry(?:\s*(?:zone|price))?[:\s]+\$?([\d,\.]+)",
            r"Entry\s+\$?([\d,\.]+)",
        ])

    stop = _first([
        r"Stop(?:\s*Loss)?[:\s]+\$?([\d,\.]+)",
        r"SL[:\s]+\$?([\d,\.]+)",
    ])
    # Prefer explicit "Target N" / "Take Profit N" labels; require a price-like
    # token (optional $). Order matters — never match "TP1 3:1" R:R claims.
    tp1 = _first([
        r"Target\s*1[:\s]+\$?([\d,\.]+)",
        r"Take\s*Profit\s*1[:\s]+\$?([\d,\.]+)",
        r"\bTP\s*1[:\s]+\$([\d,\.]+)",  # require $ so "TP1 3:1" is ignored
    ])
    tp2 = _first([
        r"Target\s*2[:\s]+\$?([\d,\.]+)",
        r"Take\s*Profit\s*2[:\s]+\$?([\d,\.]+)",
        r"\bTP\s*2[:\s]+\$([\d,\.]+)",
    ])
    tp3 = _first([
        r"Target\s*3[:\s]+\$?([\d,\.]+)",
        r"Take\s*Profit\s*3[:\s]+\$?([\d,\.]+)",
        r"\bTP\s*3[:\s]+\$([\d,\.]+)",
    ])
    # Single "Target: $X" fallback when only one target is given
    if tp1 is None:
        tp1 = _first([r"(?<!\d\s)Target[:\s]+\$?([\d,\.]+)"])

    direction = None
    dm = re.search(r"\b(Long|Short|Buy|Sell)\b", text, re.IGNORECASE)
    if dm:
        d = dm.group(1).lower()
        direction = "short" if d in ("short", "sell") else "long"
    # Infer from levels if not stated
    if direction is None and entry and stop:
        direction = "short" if stop > entry else "long"

    return {
        "entry": entry,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "direction": direction,
    }


def _ts_calc_rr(entry, stop, target, direction):
    """
    Direction-aware R:R. Returns float or None if invalid.
    SHORT: risk = stop - entry; reward = entry - target
    LONG:  risk = entry - stop; reward = target - entry
    """
    try:
        e, s, t = float(entry), float(stop), float(target)
    except (TypeError, ValueError):
        return None
    if e <= 0 or s <= 0 or t <= 0:
        return None
    if direction == "short":
        if not (s > e and t < e):
            return None
        risk = s - e
        reward = e - t
    else:  # long
        if not (s < e and t > e):
            return None
        risk = e - s
        reward = t - e
    if risk <= 0:
        return None
    return reward / risk


def _ts_format_rr(rr):
    """Consistent display: 0.85:1, 1.55:1, 2.45:1"""
    if rr is None:
        return "n/a"
    return f"{rr:.2f}:1"


def _ts_strip_ai_rr_claims(ai_text):
    """Remove lines that claim authoritative R:R so they cannot contradict code."""
    if not ai_text:
        return ai_text
    cleaned = []
    for line in ai_text.splitlines():
        if re.search(
            r"(risk\s*[:/]\s*reward|r\s*:\s*r|risk-to-reward|risk\s+to\s+reward)\s*[:=]?",
            line, re.IGNORECASE,
        ) and re.search(r"\d", line):
            continue  # drop AI R:R claim lines
        cleaned.append(line)
    return "\n".join(cleaned).strip()


def _ts_build_rr_section(levels):
    """
    Build the authoritative LEVELS + R:R block from parsed numbers.
    Returns (html_section_str, ok: bool). ok=False when levels invalid.
    """
    entry = levels.get("entry")
    stop = levels.get("stop")
    direction = levels.get("direction") or "long"
    tps = [("TP1", levels.get("tp1")), ("TP2", levels.get("tp2")), ("TP3", levels.get("tp3"))]
    tps = [(n, v) for n, v in tps if v is not None]

    if not entry or not stop:
        return (
            "📐 <b>LEVELS / R:R</b>\n"
            "<i>Could not extract a clear Entry and Stop from the AI response. "
            "Treat any R:R in the narrative as unverified.</i>",
            False,
        )

    # Direction / placement validation
    if direction == "short":
        if stop <= entry:
            return (
                "📐 <b>LEVELS / R:R</b>\n"
                f"⚠️ Invalid SHORT levels: stop ({format_price(stop)}) must be above entry ({format_price(entry)}). "
                "R:R not calculated.",
                False,
            )
    else:
        if stop >= entry:
            return (
                "📐 <b>LEVELS / R:R</b>\n"
                f"⚠️ Invalid LONG levels: stop ({format_price(stop)}) must be below entry ({format_price(entry)}). "
                "R:R not calculated.",
                False,
            )

    lines = ["📐 <b>LEVELS + R:R (calculated)</b>"]
    if levels.get("entry_low") and levels.get("entry_high"):
        lines.append(
            f"Entry zone:  <b>{format_price(levels['entry_low'])} – {format_price(levels['entry_high'])}</b>"
        )
        lines.append(
            f"Entry used:  <b>{format_price(entry)}</b>  <i>(midpoint of zone for R:R)</i>"
        )
    else:
        lines.append(f"Entry:       <b>{format_price(entry)}</b>")
    lines.append(f"Stop Loss:   <b>{format_price(stop)}</b>")
    lines.append(f"Direction:   <b>{direction.upper()}</b>")

    any_valid_tp = False
    for name, tp in tps:
        rr = _ts_calc_rr(entry, stop, tp, direction)
        if rr is None:
            lines.append(
                f"{name}:        <b>{format_price(tp)}</b>  — <i>invalid vs entry/stop, R:R skipped</i>"
            )
        else:
            any_valid_tp = True
            lines.append(
                f"{name}:        <b>{format_price(tp)}</b>  ·  R:R <b>{_ts_format_rr(rr)}</b>"
            )

    if not tps:
        lines.append("<i>No take-profit levels could be parsed.</i>")
    elif not any_valid_tp:
        lines.append("<i>No valid take-profit levels for R:R calculation.</i>")

    lines.append("<i>R:R is computed in code from the prices above — not taken from the AI narrative.</i>")
    return "\n".join(lines), True




