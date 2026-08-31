"""
Market Pulse — Setup Engine (Phase 1 + Phase 2)

Phase 1: Outcome evaluation without look-ahead (from signal time forward only).
Phase 2: Steady-tier programmatic Entry/SL/TP from structure + ATR (AI explains only).

Design rules:
- Python owns numerical levels for Steady when candles are available.
- AI is optional narrative, not the authority on prices.
- "No trade" is a valid and preferred outcome when conditions are weak.
- Outcomes never use future knowledge relative to signal time.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from market_pulse.candle_engine import candles_ready, get_candles
from market_pulse.config_runtime import logger
from market_pulse.db import get_db
from market_pulse.helpers import format_price, wat_now
from market_pulse.config_runtime import WAT_OFFSET
from market_pulse.indicators_ext import atr, ema
from market_pulse.market_structure import nearest_levels, support_resistance_levels
from market_pulse.news import get_crypto_news


# ─── News caution (veto / context — not a price generator) ───────────────────

_HIGH_IMPACT_KEYWORDS = (
    "fomc", "fed rate", "interest rate decision", "cpi ", "nonfarm", "nfp",
    "sec ", "etf approval", "etf reject", "hack", "exploit", "ban ",
    "emergency", "liquidation cascade", "exchange insolvent",
)


def news_market_flag(coin: str | None = None) -> dict:
    """
    Returns {flag: clear|caution|blackout, headlines: [...]}.
    Used to block new Edge/Momentum or annotate Steady — never to invent Entry/SL.
    """
    articles = get_crypto_news() or []
    hits = []
    coin_l = (coin or "").lower()
    for a in articles[:10]:
        title = (a.get("title") or "").lower()
        if any(k in title for k in _HIGH_IMPACT_KEYWORDS):
            hits.append(a.get("title") or "")
        elif coin_l and coin_l in title and any(
            w in title for w in ("surge", "crash", "halt", "sec", "etf", "hack")
        ):
            hits.append(a.get("title") or "")

    if not hits:
        return {"flag": "clear", "headlines": []}
    # Multiple severe hits → blackout; single → caution
    severe = sum(
        1
        for h in hits
        if any(k in h.lower() for k in ("fomc", "hack", "exploit", "ban", "insolvent"))
    )
    flag = "blackout" if severe >= 1 or len(hits) >= 3 else "caution"
    return {"flag": flag, "headlines": hits[:3]}


# ─── Phase 2: Steady programmatic setup ─────────────────────────────────────

def _candle_closes(candles):
    return [float(c["close"]) for c in candles if c.get("close") is not None]


def _last_atr(candles, period=14):
    series = atr(candles, period=period)
    if not series:
        return None
    for v in reversed(series):
        if v is not None and v > 0:
            return float(v)
    return None


# Tier parameters for programmatic setups (ATR + structure)
_TIER_SPEC = {
    # steady = SAFE — tight risk, must have structure + trend alignment
    "steady": {
        "max_stop_pct": 0.05,
        "stop_atr": 0.9,
        "t1_r": 1.8,
        "t2_r": 2.8,
        "min_rr_t1": 1.5,
        "need_structure": True,
        "require_trend": True,
        "require_level": True,
        "allow_counter_level": False,  # no LONG into resistance without breakout
        "extension_min": 0.0,
        "extension_max": 1.2,  # not chasing stretched moves
        "trail_pct": 0.004,
        "be_trigger_r": 1.0,
        "display": "SAFE",
    },
    # momentum = NORMAL — trend continuation, structure preferred not mandatory
    "momentum": {
        "max_stop_pct": 0.08,
        "stop_atr": 1.2,
        "t1_r": 1.6,
        "t2_r": 2.6,
        "min_rr_t1": 1.4,
        "need_structure": False,
        "require_trend": True,
        "require_level": False,
        "allow_counter_level": False,
        "extension_min": 0.0,
        "extension_max": 2.0,
        "trail_pct": 0.005,
        "be_trigger_r": 1.0,
        "display": "NORMAL",
    },
    # edge = AGGRESSIVE — earlier, needs catalyst, still capped risk
    "edge": {
        "max_stop_pct": 0.12,
        "stop_atr": 1.4,
        "t1_r": 2.0,
        "t2_r": 3.2,
        "min_rr_t1": 1.6,
        "need_structure": False,
        "require_trend": False,  # can anticipate if catalyst + invalidation exist
        "require_level": False,
        "allow_counter_level": True,  # breakout/rejection style allowed with thesis
        "extension_min": 0.8,
        "extension_max": 3.5,
        "trail_pct": 0.006,
        "be_trigger_r": 1.0,
        "display": "AGGRESSIVE",
    },
}


def _fmt_px(v: float) -> str:
    if v >= 1000:
        return f"${v:,.0f}"
    if v >= 1:
        return f"${v:,.2f}"
    return f"${v:,.4f}"


def _resolve_structure(candles, price):
    support = resistance = None
    try:
        near = nearest_levels(candles, price)
        if isinstance(near, dict):
            support = near.get("support")
            resistance = near.get("resistance")
            if isinstance(support, dict):
                support = support.get("price")
            if isinstance(resistance, dict):
                resistance = resistance.get("price")
    except Exception:
        pass
    if support is None or resistance is None:
        sr = support_resistance_levels(candles)
        supports = [s["price"] for s in sr.get("support", []) if s["price"] < price]
        resists = [r["price"] for r in sr.get("resistance", []) if r["price"] > price]
        if support is None and supports:
            support = max(supports)
        if resistance is None and resists:
            resistance = min(resists)
    return support, resistance


def _structural_target(direction, entry, risk, atr_val, support, resistance, candles, t_r):
    """
    Prefer previous swing / S/R within a realistic ATR window over pure R-multiple.
    Cap target distance to ~3–6 ATR so we never ask for a macro move on local vol.
    """
    max_dist = max(3.0 * atr_val, risk * t_r)
    min_dist = max(0.6 * atr_val, risk * 0.8)
    pure_r = entry + risk * t_r if direction == "long" else entry - risk * t_r

    candidates = []
    if direction == "long":
        if resistance and resistance > entry:
            candidates.append(resistance)
        # recent swing highs from last 30 candles
        for c in candles[-30:]:
            h = float(c.get("high") or 0)
            if h > entry:
                candidates.append(h)
        # keep levels between min_dist and max_dist above entry
        structural = [x for x in candidates if min_dist <= (x - entry) <= max_dist]
        if structural:
            # nearest achievable structure (highest probability TP1)
            return min(structural), "structure"
        # clamp pure R to max_dist
        return entry + min(risk * t_r, max_dist), "atr_cap"
    else:
        if support and support < entry:
            candidates.append(support)
        for c in candles[-30:]:
            low = float(c.get("low") or 0)
            if 0 < low < entry:
                candidates.append(low)
        structural = [x for x in candidates if min_dist <= (entry - x) <= max_dist]
        if structural:
            return max(structural), "structure"
        return entry - min(risk * t_r, max_dist), "atr_cap"



# ── Trade time horizon (expectation, not a promise) ─────────────────────────
# Maps setup timeframe → expected holding language + max validity hours.
_HORIZON_BY_TF = {
    "5M":  {"label": "Intraday (typically a few hours)", "valid_hours": 18},
    "15M": {"label": "Intraday (typically several hours)", "valid_hours": 24},
    "1H":  {"label": "Typically hours to ~1-2 days", "valid_hours": 60},
    "4H":  {"label": "Typically hours to several days", "valid_hours": 168},
    "DAILY": {"label": "Typically several days or longer", "valid_hours": 336},
    "D":   {"label": "Typically several days or longer", "valid_hours": 336},
    "WEEKLY": {"label": "Multi-day to multi-week swing", "valid_hours": 504},
}

# Tier default timeframe when not specified
_TIER_DEFAULT_TF = {
    "steady": "4H",      # SAFE — slower, higher quality
    "momentum": "1H",    # NORMAL — default
    "edge": "1H",         # AGGRESSIVE — faster context
}


def resolve_horizon(timeframe: str | None = None, tier: str | None = None) -> dict:
    """Return expected_horizon label, valid_hours, timeframe used."""
    tf = (timeframe or "").strip().upper().replace(" ", "")
    if not tf or tf in ("NONE", "N/A"):
        t = (tier or "momentum").lower()
        t = {"safe": "steady", "normal": "momentum", "aggressive": "edge"}.get(t, t)
        tf = _TIER_DEFAULT_TF.get(t, "1H")
    # normalize aliases
    aliases = {"1HR": "1H", "4HR": "4H", "1D": "DAILY", "DAY": "DAILY", "1DAY": "DAILY"}
    tf = aliases.get(tf, tf)
    spec = _HORIZON_BY_TF.get(tf) or _HORIZON_BY_TF["1H"]
    return {
        "timeframe": tf,
        "expected_horizon": spec["label"],
        "valid_hours": int(spec["valid_hours"]),
    }


def compute_valid_until(signal_dt, valid_hours: int) -> str:
    """WAT wall-time string for validity end."""
    if signal_dt is None:
        signal_dt = wat_now()
    if isinstance(signal_dt, str):
        try:
            signal_dt = datetime.strptime(signal_dt[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            signal_dt = wat_now()
    end = signal_dt + timedelta(hours=int(valid_hours or 48))
    return end.strftime("%Y-%m-%d %H:%M:%S")


def build_programmatic_setup(coin: str, price: float, tier: str = "steady") -> dict | None:
    """
    Python-owned Entry/SL/TP for steady | momentum | edge.

    - Stops from structure + ATR (not round numbers alone)
    - Targets prefer swing/S/R inside an ATR-realistic window
    - Includes trade-management plan text (BE at 1R, suggested trail)
    - Does NOT place exchange orders — Market Pulse is a signal bot
    """
    tier = (tier or "steady").lower()
    tier = {"safe": "steady", "normal": "momentum", "aggressive": "edge"}.get(tier, tier)
    if tier not in _TIER_SPEC:
        tier = "steady"
    spec = _TIER_SPEC[tier]

    if not price or price <= 0:
        return None

    news = news_market_flag(coin)
    if news["flag"] == "blackout":
        logger.info(f"[SETUP ENGINE] {coin} {tier} blocked — news blackout")
        return None

    if not candles_ready(coin, min_candles=60):
        logger.info(f"[SETUP ENGINE] {coin} {tier} skipped — candles not ready")
        return None

    candles = get_candles(coin) or []
    if len(candles) < 60:
        return None

    closes = _candle_closes(candles)
    if len(closes) < 50:
        return None

    atr_val = _last_atr(candles, 14)
    if not atr_val or atr_val <= 0:
        return None

    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)
    if not ema20 or not ema50 or ema20[-1] is None or ema50[-1] is None:
        return None
    e20, e50 = float(ema20[-1]), float(ema50[-1])

    support, resistance = _resolve_structure(candles, price)
    reasons = []

    bullish_trend = e20 > e50 and price >= e20 * 0.992
    bearish_trend = e20 < e50 and price <= e20 * 1.008
    near_support = support and abs(price - support) <= 1.5 * atr_val and price >= support * 0.998
    near_resist = resistance and abs(resistance - price) <= 1.5 * atr_val and price <= resistance * 1.002

    # RSI-ish extension from price vs EMA for edge
    extension = abs(price - e20) / atr_val if atr_val else 0

    # Normalize tier aliases
    if tier in ("safe",):
        tier = "steady"
    elif tier in ("normal",):
        tier = "momentum"
    elif tier in ("aggressive",):
        tier = "edge"
    if tier not in _TIER_SPEC:
        tier = "steady"
    spec = _TIER_SPEC[tier]

    # Structural contradictions (SAFE/NORMAL): do not LONG into resistance or SHORT into support
    # without explicit breakout/breakdown thesis (AGGRESSIVE may attempt with catalyst).
    direction = None
    display = spec.get("display", tier.upper())

    if extension > spec.get("extension_max", 99):
        logger.info(f"[SETUP ENGINE] {coin} {display} — extension {extension:.1f}×ATR too stretched")
        return None

    if tier == "steady":
        # SAFE: trend + level required. No counter-level.
        if bullish_trend and near_support and not near_resist:
            direction = "long"
            reasons += ["SAFE: EMA20>EMA50", f"Validated support {_fmt_px(support)}"]
        elif bearish_trend and near_resist and not near_support:
            direction = "short"
            reasons += ["SAFE: EMA20<EMA50", f"Validated resistance {_fmt_px(resistance)}"]
        elif bullish_trend and near_resist:
            logger.info(f"[SETUP ENGINE] {coin} SAFE — LONG blocked at resistance (need breakout confirmation)")
            return None
        elif bearish_trend and near_support:
            logger.info(f"[SETUP ENGINE] {coin} SAFE — SHORT blocked at support (need breakdown confirmation)")
            return None

    elif tier == "momentum":
        # NORMAL: trend required; level preferred.
        if bullish_trend and not near_resist:
            direction = "long"
            reasons += ["NORMAL: bullish EMA stack", "Continuation bias"]
            if near_support:
                reasons.append(f"Support {_fmt_px(support)}")
        elif bearish_trend and not near_support:
            direction = "short"
            reasons += ["NORMAL: bearish EMA stack", "Continuation bias"]
            if near_resist:
                reasons.append(f"Resistance {_fmt_px(resistance)}")
        elif bullish_trend and near_resist:
            logger.info(f"[SETUP ENGINE] {coin} NORMAL — no LONG into resistance without breakout")
            return None
        elif bearish_trend and near_support:
            logger.info(f"[SETUP ENGINE] {coin} NORMAL — no SHORT into support without breakdown")
            return None

    else:
        # AGGRESSIVE: catalyst (extension) + hard invalidation later. Higher uncertainty labeled.
        min_ext = spec.get("extension_min", 0.8)
        if extension >= min_ext and bullish_trend:
            direction = "long"
            reasons += [f"AGGRESSIVE: extension {extension:.1f}×ATR", "Early long thesis"]
            if near_support:
                reasons.append(f"Demand zone {_fmt_px(support)}")
        elif extension >= min_ext and bearish_trend:
            direction = "short"
            reasons += [f"AGGRESSIVE: extension {extension:.1f}×ATR", "Early short thesis"]
            if near_resist:
                reasons.append(f"Supply zone {_fmt_px(resistance)}")
        elif near_support and extension >= min_ext * 0.75:
            direction = "long"
            reasons += ["AGGRESSIVE: early long near support", f"Ext {extension:.1f}×ATR"]
        elif near_resist and extension >= min_ext * 0.75:
            direction = "short"
            reasons += ["AGGRESSIVE: early short near resistance", f"Ext {extension:.1f}×ATR"]

    if not direction:
        logger.info(f"[SETUP ENGINE] {coin} {display} — NO VALID {display} SETUP")
        return None

    entry = float(price)
    if direction == "long":
        struct_stop = (support - 0.35 * atr_val) if support else (entry - spec["stop_atr"] * atr_val)
        atr_stop = entry - spec["stop_atr"] * atr_val
        stop = min(struct_stop, atr_stop)
        risk = entry - stop
        if risk <= 0 or risk / entry > spec["max_stop_pct"]:
            stop = entry - min(spec["stop_atr"] * atr_val, entry * spec["max_stop_pct"] * 0.9)
            risk = entry - stop
        if risk <= 0 or risk / entry > spec["max_stop_pct"]:
            return None
        t1, t1_src = _structural_target("long", entry, risk, atr_val, support, resistance, candles, spec["t1_r"])
        t2, t2_src = _structural_target("long", entry, risk, atr_val, support, resistance, candles, spec["t2_r"])
        if t2 <= t1:
            t2 = entry + risk * spec["t2_r"]
            t2_src = "r_multiple"
    else:
        struct_stop = (resistance + 0.35 * atr_val) if resistance else (entry + spec["stop_atr"] * atr_val)
        atr_stop = entry + spec["stop_atr"] * atr_val
        stop = max(struct_stop, atr_stop)
        risk = stop - entry
        if risk <= 0 or risk / entry > spec["max_stop_pct"]:
            stop = entry + min(spec["stop_atr"] * atr_val, entry * spec["max_stop_pct"] * 0.9)
            risk = stop - entry
        if risk <= 0 or risk / entry > spec["max_stop_pct"]:
            return None
        t1, t1_src = _structural_target("short", entry, risk, atr_val, support, resistance, candles, spec["t1_r"])
        t2, t2_src = _structural_target("short", entry, risk, atr_val, support, resistance, candles, spec["t2_r"])
        if t2 >= t1:
            t2 = entry - risk * spec["t2_r"]
            t2_src = "r_multiple"

    rr1 = abs(t1 - entry) / risk
    if rr1 < spec["min_rr_t1"] * 0.85:  # slight tolerance after structure snap
        logger.info(f"[SETUP ENGINE] {coin} {tier} — T1 R:R {rr1:.2f} too low")
        return None

    reasons.append(f"ATR(14)≈{_fmt_px(atr_val) if atr_val >= 1 else f'{atr_val:.4g}'}")
    reasons.append(f"T1 via {t1_src}, T2 via {t2_src}")
    reasons.append(f"T1 R:R ≈ {rr1:.2f}:1 (code)")
    if news["flag"] == "caution":
        reasons.append("News caution: " + (news["headlines"][0][:70] if news["headlines"] else "active"))

    be_price = entry  # break-even = entry
    trail_pct = spec["trail_pct"]
    management = (
        f"MANAGEMENT (manual / your exchange — bot does not place orders): "
        f"(1) At +{spec['be_trigger_r']:.0f}R profit, move stop to break-even {_fmt_px(be_price)}. "
        f"(2) Optional trail ≈ {trail_pct*100:.1f}% behind price once BE is on. "
        f"(3) TP1 is the high-probability exit; TP2 is runner only."
    )

    hz = resolve_horizon(None, tier=tier)
    signal_wat = wat_now()
    signal_str = signal_wat.strftime("%Y-%m-%d %H:%M:%S")
    valid_until = compute_valid_until(signal_wat, hz["valid_hours"])

    return {
        "timeframe": hz["timeframe"],
        "expected_horizon": hz["expected_horizon"],
        "valid_hours": hz["valid_hours"],
        "valid_until": valid_until,
        "lifecycle_status": "ENTRY_NOT_REACHED",
        "direction": direction.capitalize(),
        "entry": _fmt_px(entry),
        "stop": _fmt_px(stop),
        "target1": _fmt_px(t1),
        "target2": _fmt_px(t2),
        "invalidation": _fmt_px(stop),
        "bias": "Bullish" if direction == "long" else "Bearish",
        "confidence": ("Low" if tier == "edge" else ("High" if tier == "steady" and news["flag"] == "clear" else "Moderate")),
        "rationale": " | ".join(reasons),
        "ng_angle": "Size per tier limits; protect naira capital — follow management plan.",
        "management": management,
        "source": f"setup_engine_{tier}",
        "display_tier": _TIER_SPEC.get(tier, {}).get("display", tier.upper()),
        "reasons": reasons,
        "news_flag": news["flag"],
        "entry_raw": entry,
        "stop_raw": stop,
        "target1_raw": t1,
        "target2_raw": t2,
        "atr": atr_val,
        "be_trigger_r": spec["be_trigger_r"],
        "trail_pct": trail_pct,
        "signal_price": price,
        "signal_time_wat": signal_str,
    }


def build_steady_setup(coin: str, price: float) -> dict | None:
    """Backward-compatible alias."""
    return build_programmatic_setup(coin, price, tier="steady")


# ─── Phase 1: Outcome evaluation (no look-ahead) ────────────────────────────

def _parse_level(raw):
    if raw is None:
        return None
    s = str(raw).replace("$", "").replace(",", "").strip()
    try:
        v = float(s)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def evaluate_path(direction: str, entry: float, stop: float, targets: list,
                  candles_after: list, be_trigger_r: float | None = 1.0) -> dict:
    """
    Walk candles strictly AFTER the signal. First touch wins.

    If be_trigger_r is set, once favorable excursion >= be_trigger_r * risk,
    the working stop is moved to entry (break-even). That models the
    recommended management rule without placing exchange orders.
    """
    direction = (direction or "long").lower()
    targets = [t for t in targets if t is not None]
    entered = False
    entry_time = None
    hit_stop = hit_t1 = hit_t2 = None
    mfe = 0.0
    mae = 0.0
    fill = entry
    working_stop = stop
    be_on = False
    risk = abs(entry - stop)

    for c in candles_after:
        o = float(c.get("open") or 0)
        h = float(c.get("high") or 0)
        l = float(c.get("low") or 0)
        ts = c.get("open_time") or c.get("t") or c.get("time")

        if not entered:
            # Zone: treat entry as a level — long fills if low<=entry<=high
            if l <= entry <= h or (direction == "long" and l <= entry) or (direction == "short" and h >= entry):
                # Prefer conservative fill: long at min(open, entry) capped to entry
                if direction == "long":
                    if o <= entry:
                        fill = o
                    else:
                        fill = entry
                else:
                    if o >= entry:
                        fill = o
                    else:
                        fill = entry
                entered = True
                entry_time = ts
                mfe = 0.0
                mae = 0.0
            else:
                continue

        if entered:
            if direction == "long":
                mfe = max(mfe, h - fill)
                mae = max(mae, fill - l)
                if (
                    be_trigger_r
                    and risk > 0
                    and not be_on
                    and (h - fill) >= be_trigger_r * risk
                ):
                    working_stop = fill  # break-even
                    be_on = True
                if hit_stop is None and l <= working_stop:
                    hit_stop = ts
                if targets and hit_t1 is None and h >= targets[0]:
                    hit_t1 = ts
                if len(targets) > 1 and hit_t2 is None and h >= targets[1]:
                    hit_t2 = ts
            else:
                mfe = max(mfe, fill - l)
                mae = max(mae, h - fill)
                if (
                    be_trigger_r
                    and risk > 0
                    and not be_on
                    and (fill - l) >= be_trigger_r * risk
                ):
                    working_stop = fill
                    be_on = True
                if hit_stop is None and h >= working_stop:
                    hit_stop = ts
                if targets and hit_t1 is None and l <= targets[0]:
                    hit_t1 = ts
                if len(targets) > 1 and hit_t2 is None and l <= targets[1]:
                    hit_t2 = ts

    if not entered:
        return {
            "outcome": "ENTRY_NOT_REACHED",
            "fill": None,
            "mfe": 0.0,
            "mae": 0.0,
            "hit_stop": None,
            "hit_t1": None,
            "hit_t2": None,
        }

    # First event among stop / t1 / t2
    events = []
    if hit_stop is not None:
        events.append(("STOP_HIT", hit_stop))
    if hit_t1 is not None:
        events.append(("TP1_HIT", hit_t1))
    if hit_t2 is not None:
        events.append(("TP2_HIT", hit_t2))

    def _key(ev):
        t = ev[1]
        if isinstance(t, (int, float)):
            return t
        return str(t)

    events.sort(key=_key)
    ambiguous = False
    if not events:
        outcome = "STILL_OPEN"
    else:
        outcome = events[0][0]
        # Same timestamp for STOP and TP → cannot order reliably on OHLC alone
        if len(events) >= 2 and _key(events[0]) == _key(events[1]):
            kinds = {events[0][0], events[1][0]}
            if "STOP_HIT" in kinds and ("TP1_HIT" in kinds or "TP2_HIT" in kinds):
                ambiguous = True
                outcome = "AMBIGUOUS"
        if outcome == "STOP_HIT" and be_on and not ambiguous:
            outcome = "BE_EXIT"  # stopped at break-even after 1R trigger

    return {
        "outcome": outcome,
        "fill": fill,
        "mfe": mfe,
        "mae": mae,
        "hit_stop": hit_stop,
        "hit_t1": hit_t1,
        "hit_t2": hit_t2,
        "be_on": be_on,
        "ambiguous": ambiguous,
        "first_event": events[0][0] if events else None,
    }


def _wat_ledger_str_to_utc(created_at_str):
    """Ledger created_at / valid_until are WAT wall-clock naive strings from wat_now().

    Convert to timezone-aware UTC for market-data comparisons.
    """
    if not created_at_str:
        return None
    try:
        wat_naive = datetime.strptime(str(created_at_str)[:19], "%Y-%m-%d %H:%M:%S")
        # wat_now = utcnow + WAT_OFFSET → invert
        utc_naive = wat_naive - timedelta(hours=int(WAT_OFFSET))
        return utc_naive.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _candle_open_utc(ts):
    """Normalize candle open timestamp to timezone-aware UTC.

    candle_engine stores open_time as Unix seconds (UTC epoch).
    """
    if ts is None:
        return None
    try:
        if isinstance(ts, (int, float)):
            tsec = float(ts)
            if tsec > 1e12:  # ms
                tsec = tsec / 1000.0
            return datetime.fromtimestamp(tsec, tz=timezone.utc)
        s = str(ts)[:19]
        # ISO with Z
        if str(ts).endswith("Z"):
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        # Naive string: treat as UTC (exchange convention), not WAT
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _candles_after_timestamp(candles, created_at_str):
    """Keep only candles at/after signal time. Comparisons in UTC.

    created_at is a WAT wall-clock string from the trade ledger.
    candle open_time is UTC epoch seconds from the exchange feed.

    Fail closed: missing/unparseable timestamps are excluded (no look-ahead).
    Allow at most 2 seconds clock skew, not hours.
    """
    if not candles:
        return []
    sig_utc = _wat_ledger_str_to_utc(created_at_str)
    if sig_utc is None:
        return []

    skew = timedelta(seconds=2)
    out = []
    for c in candles:
        ts = c.get("open_time") if isinstance(c, dict) else None
        if ts is None and isinstance(c, dict):
            ts = c.get("t") or c.get("time")
        ct = _candle_open_utc(ts)
        if ct is None:
            continue  # fail closed — do not include undated candles
        if ct >= (sig_utc - skew):
            out.append(c)
    return out


def score_open_trade_ideas(limit=20) -> list:
    """
    Evaluate open trade_ideas against subsequent 1h candles (no look-ahead).

    Outcomes:
      TARGET_HIT | STOP_HIT | ENTRY_NOT_REACHED (still open)
      ACTIVE (entered, neither hit yet — keep open)
      SETUP_EXPIRED (past valid_until without terminal hit)
      THESIS_INVALIDATED (reserved for explicit invalidation closes)

    A long-duration setup within validity is NOT a failure.
    """
    db = None
    results = []
    try:
        db = get_db()
        c = db.cursor()
        # Ensure optional columns exist (safe on Postgres)
        for col, typ in (
            ("valid_until", "TEXT"),
            ("expected_horizon", "TEXT"),
            ("lifecycle_status", "TEXT"),
        ):
            try:
                c.execute(f"ALTER TABLE trade_ideas ADD COLUMN IF NOT EXISTS {col} {typ}")
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
                db = get_db()
                c = db.cursor()
        try:
            db.commit()
        except Exception:
            pass

        c.execute(
            """
            SELECT id, coin, direction, entry, stop, target1, target2, created_at, tier,
                   COALESCE(valid_until, ''), COALESCE(timeframe, '1H')
            FROM trade_ideas
            WHERE status = 'open'
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = c.fetchall() or []
        now = wat_now()
        now_s = now.strftime("%Y-%m-%d %H:%M:%S")

        for row in rows:
            try:
                idea_id, coin, direction, entry_s, stop_s, t1_s, t2_s, created_at, tier = row[:9]
                valid_until = row[9] if len(row) > 9 else ""
                timeframe = row[10] if len(row) > 10 else "1H"
            except Exception:
                continue

            entry = _parse_level(entry_s)
            stop = _parse_level(stop_s)
            t1 = _parse_level(t1_s)
            t2 = _parse_level(t2_s)
            if not entry or not stop:
                continue

            # Expire by validity clock first
            vu = (valid_until or "").strip()
            if not vu and created_at:
                hz = resolve_horizon(timeframe, tier)
                vu = compute_valid_until(created_at, hz["valid_hours"])
                try:
                    c.execute(
                        "UPDATE trade_ideas SET valid_until=%s, expected_horizon=%s WHERE id=%s AND (valid_until IS NULL OR valid_until='')",
                        (vu, hz["expected_horizon"], idea_id),
                    )
                except Exception:
                    pass

            expired = False
            if vu:
                try:
                    vu_dt = datetime.strptime(vu[:19], "%Y-%m-%d %H:%M:%S")
                    if now > vu_dt:
                        expired = True
                except Exception:
                    pass

            candles = []
            try:
                from market_pulse.candle_engine import get_candles
                candles = get_candles(coin) or []
            except Exception:
                candles = []

            after = _candles_after_timestamp(candles, created_at or "")
            path = evaluate_path(
                direction, float(entry), float(stop),
                [float(t1) if t1 else None, float(t2) if t2 else None],
                after,
                be_trigger_r=1.0,
            )

            outcome = (path or {}).get("outcome") or (path or {}).get("result") or ""
            entered = outcome not in ("ENTRY_NOT_REACHED", "", None) and outcome != "ENTRY_NOT_REACHED"
            if outcome == "ENTRY_NOT_REACHED":
                entered = False
            elif outcome in ("STILL_OPEN", "TP1_HIT", "TP2_HIT", "STOP_HIT", "BE_EXIT"):
                entered = True

            # Map path outcome → lifecycle
            lifecycle = "ENTRY_NOT_REACHED"
            terminal = None
            if outcome in ("TP1_HIT", "TP2_HIT", "TARGET_HIT", "target1", "tp1", "win_t1", "win_t2"):
                lifecycle, terminal = "TARGET_HIT", "TARGET_HIT"
            elif outcome in ("STOP_HIT", "stop", "loss"):
                lifecycle, terminal = "STOP_HIT", "STOP_HIT"
            elif outcome == "BE_EXIT":
                lifecycle, terminal = "STOP_HIT", "BE_EXIT"
            elif outcome == "STILL_OPEN" or (entered and not terminal):
                lifecycle = "ACTIVE"
            elif outcome == "ENTRY_NOT_REACHED":
                lifecycle = "ENTRY_NOT_REACHED"

            if terminal:
                # Do NOT close here — outcome_monitor owns close + admin notify.
                # Only stamp lifecycle so the monitor can resolve and DM.
                try:
                    c.execute(
                        "UPDATE trade_ideas SET lifecycle_status=%s, result=%s WHERE id=%s AND status='open'",
                        (lifecycle, terminal, idea_id),
                    )
                except Exception as e:
                    logger.debug("[SCORE] lifecycle stamp #%s: %s", idea_id, e)
                results.append({"id": idea_id, "coin": coin, "tier": tier, "lifecycle": lifecycle, **(path or {})})
                continue

            if expired and not terminal:
                try:
                    c.execute(
                        """UPDATE trade_ideas
                           SET status='closed', closed_at=%s, result=%s, lifecycle_status=%s
                           WHERE id=%s AND status='open'""",
                        (now_s, "SETUP_EXPIRED", "SETUP_EXPIRED", idea_id),
                    )
                except Exception:
                    c.execute(
                        "UPDATE trade_ideas SET status='closed', closed_at=%s, result=%s WHERE id=%s AND status='open'",
                        (now_s, "SETUP_EXPIRED", idea_id),
                    )
                results.append({"id": idea_id, "coin": coin, "tier": tier, "lifecycle": "SETUP_EXPIRED", **(path or {})})
                continue

            # Still open — refresh lifecycle label
            try:
                c.execute(
                    "UPDATE trade_ideas SET lifecycle_status=%s WHERE id=%s AND status='open'",
                    (lifecycle, idea_id),
                )
            except Exception:
                pass
            results.append({"id": idea_id, "coin": coin, "tier": tier, "lifecycle": lifecycle, **(path or {})})

        db.commit()
    except Exception as e:
        logger.error(f"[SETUP ENGINE] score_open_trade_ideas: {e}")
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
    return results



def outcome_summary(limit=50) -> str:
    """Human-readable stats for admin."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            """
            SELECT result, COUNT(*) FROM trade_ideas
            WHERE status='closed' AND result IS NOT NULL
            GROUP BY result
            """
        )
        rows = c.fetchall() or []
        open_c = c.execute("SELECT COUNT(*) FROM trade_ideas WHERE status='open'")
        open_n = c.fetchone()[0]
        lines = ["📊 <b>Trade idea outcomes</b> (closed)", ""]
        total = 0
        for r, n in rows:
            lines.append(f"• {r}: <b>{n}</b>")
            total += n
        lines.append(f"\nClosed: <b>{total}</b> · Still open: <b>{open_n}</b>")
        lines.append("<i>Not a promise of profit — path evaluation only.</i>")
        return "\n".join(lines)
    except Exception as e:
        return f"Outcome summary unavailable: {e}"
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass
