"""
Market Pulse Bot — multi-confirmation signal engine.
====================================================
Deterministic, rule-based, fully explainable trade signal generation.
Never generates a signal from a single indicator — every signal is the
sum of independently-checked confirmations across six categories:
trend, momentum, volume, market structure, volatility, and pattern
recognition. No black boxes: every signal carries its `reasons` (why),
`confirmations` (what was checked), and `risks` (what could go wrong).

This is additive to the existing AI-narrative trade engine
(edge_trade_engine.py) — it doesn't replace it. Use this where you want
a deterministic, backtestable signal; use the AI engine where you want
narrative trade ideas. They can run side by side.

Input: `candles` — a list of OHLCV dicts, oldest -> newest, at least
~250 candles for the EMA200 stack to be meaningful (fewer will still
work, just with fewer trend confirmations available).
"""

from market_pulse.indicators_ext import (
    ema_alignment, macd_crossover, stoch_rsi, adx, atr,
    bollinger_bands, obv, volume_spike,
)
from market_pulse.market_structure import (
    detect_breakout, detect_fake_breakout, nearest_levels,
)
from market_pulse.patterns import detect_candlestick_patterns, detect_all_chart_patterns


BULLISH_CANDLES = {"hammer", "bullish_engulfing"}
BEARISH_CANDLES = {"shooting_star", "bearish_engulfing"}
BULLISH_CHART_PATTERNS = {"double_bottom", "inverse_head_and_shoulders"}
BEARISH_CHART_PATTERNS = {"double_top", "head_and_shoulders"}


def _infer_direction(closes, candles):
    """
    Direction comes from trend + momentum agreement, not from any single
    signal. Returns "long", "short", or None (no clear bias -> no trade).
    """
    trend = ema_alignment(closes)
    macd_x = macd_crossover(closes)
    votes = []
    if trend == "bullish":
        votes.append("long")
    elif trend == "bearish":
        votes.append("short")
    if macd_x == "bullish_cross":
        votes.append("long")
    elif macd_x == "bearish_cross":
        votes.append("short")

    candle_hits = detect_candlestick_patterns(candles)
    if any(p in BULLISH_CANDLES for p in candle_hits):
        votes.append("long")
    if any(p in BEARISH_CANDLES for p in candle_hits):
        votes.append("short")

    if not votes:
        return None
    longs = votes.count("long")
    shorts = votes.count("short")
    if longs > shorts:
        return "long"
    if shorts > longs:
        return "short"
    return None


def _confirm(name, ok, detail):
    return {"name": name, "met": bool(ok), "detail": detail}


def analyze(candles, symbol="", min_candles=60):
    """
    Runs full multi-confirmation analysis on `candles`.

    Returns a dict:
        {
          "signal": bool,
          "direction": "long"/"short"/None,
          "category": "safe"/"medium"/"high_risk"/None,
          "confidence": 0-100,
          "current_price": float,
          "entry": float, "stop_loss": float,
          "take_profit": [tp1, tp2, tp3],
          "risk_reward": float,
          "trend": "bullish"/"bearish"/"mixed"/None,
          "reasons": [str, ...],          # human-readable "why"
          "confirmations": [{"name","met","detail"}, ...],
          "risks": [str, ...],
        }
    `signal` is False (with an explanation in `risks`) if there isn't
    enough data or no clear direction emerges — this function never
    forces a trade idea into existence.
    """
    if len(candles) < min_candles:
        return {
            "signal": False, "direction": None, "category": None,
            "confidence": 0, "reasons": [], "confirmations": [],
            "risks": [f"Only {len(candles)} candles available; need at least {min_candles} for a reliable read."],
        }

    closes = [c["close"] for c in candles]
    current_price = closes[-1]

    direction = _infer_direction(closes, candles)
    if direction is None:
        return {
            "signal": False, "direction": None, "category": None,
            "confidence": 0, "current_price": current_price,
            "reasons": [], "confirmations": [],
            "risks": ["Trend and momentum are not in agreement — no clean directional bias right now."],
        }

    confirmations = []
    reasons = []
    risks = []

    # ── Trend ──
    trend = ema_alignment(closes)
    trend_ok = (trend == "bullish" and direction == "long") or (trend == "bearish" and direction == "short")
    confirmations.append(_confirm("trend_alignment", trend_ok, f"EMA20/50/100/200 stack is {trend or 'undetermined'}"))
    if trend_ok:
        reasons.append(f"EMA stack is cleanly {trend}, aligned with the {direction} bias")

    # ── Momentum: MACD ──
    macd_x = macd_crossover(closes)
    macd_ok = (macd_x == "bullish_cross" and direction == "long") or (macd_x == "bearish_cross" and direction == "short")
    confirmations.append(_confirm("macd_crossover", macd_ok, macd_x))
    if macd_ok:
        reasons.append(f"MACD just produced a {macd_x.replace('_', ' ')}")

    # ── Momentum: Stochastic RSI (avoid chasing an already-exhausted move) ──
    k, d = stoch_rsi(closes)
    k_last = k[-1] if k else None
    stoch_ok = False
    stoch_detail = "insufficient data"
    if k_last is not None:
        if direction == "long":
            stoch_ok = k_last < 80  # not already overbought
            stoch_detail = f"StochRSI %K={k_last:.1f}"
            if k_last > 90:
                risks.append("StochRSI is deep overbought (>90) — upside may be limited near-term")
        else:
            stoch_ok = k_last > 20
            stoch_detail = f"StochRSI %K={k_last:.1f}"
            if k_last < 10:
                risks.append("StochRSI is deep oversold (<10) — downside may be limited near-term")
    confirmations.append(_confirm("momentum_not_exhausted", stoch_ok, stoch_detail))
    if stoch_ok:
        reasons.append(f"Momentum has room to run ({stoch_detail})")

    # ── Momentum: ADX (is there a trend worth trading at all) ──
    adx_series = adx(candles)
    adx_last = adx_series[-1] if adx_series else None
    adx_ok = adx_last is not None and adx_last >= 20
    confirmations.append(_confirm("adx_trending", adx_ok, f"ADX={adx_last:.1f}" if adx_last is not None else "insufficient data"))
    if adx_ok:
        reasons.append(f"ADX at {adx_last:.1f} confirms a real trend, not a chop")
    elif adx_last is not None:
        risks.append(f"ADX only {adx_last:.1f} — market may be range-bound rather than trending")

    # ── Volume ──
    vol_spike = volume_spike(candles)
    obv_series = obv(candles)
    obv_rising = len(obv_series) >= 5 and obv_series[-1] > obv_series[-5]
    obv_ok = obv_rising if direction == "long" else (len(obv_series) >= 5 and obv_series[-1] < obv_series[-5])
    volume_ok = bool(vol_spike) or obv_ok
    vol_detail = f"volume_spike={vol_spike}, OBV trend {'up' if obv_rising else 'down'}"
    confirmations.append(_confirm("volume_confirms", volume_ok, vol_detail))
    if vol_spike:
        reasons.append("Volume spiked vs. the recent average — real participation, not a thin move")
    elif obv_ok:
        reasons.append("On-balance volume is trending in the trade's favor")
    else:
        risks.append("No volume confirmation — this move isn't backed by above-average participation yet")

    # ── Market structure: breakout ──
    breakout = detect_breakout(candles)
    breakout_ok = breakout is not None and (
        (direction == "long" and breakout["direction"] == "up") or
        (direction == "short" and breakout["direction"] == "down")
    )
    confirmations.append(_confirm("structure_breakout", breakout_ok, breakout))
    if breakout_ok:
        conf_bits = []
        if breakout["close_confirmed"]:
            conf_bits.append("close-confirmed")
        if breakout["volume_confirmed"]:
            conf_bits.append("volume-confirmed")
        reasons.append(f"Price broke {breakout['direction']} through {breakout['level']:.4g}" +
                        (f" ({', '.join(conf_bits)})" if conf_bits else ""))

    # ── Market structure: fake-breakout risk check (this is a RISK flag, not a confirmation) ──
    fake = detect_fake_breakout(candles)
    if fake:
        contradicts = (fake["direction"] == "bull_trap" and direction == "long") or \
                      (fake["direction"] == "bear_trap" and direction == "short")
        if contradicts:
            risks.append(f"Recent {fake['direction'].replace('_', ' ')} at {fake['level']:.4g} — the last breakout attempt in this direction got rejected")

    # ── Market structure: proximity to S/R (context, not a hard confirmation) ──
    levels = nearest_levels(candles, current_price)
    if direction == "long" and levels.get("resistance"):
        r = levels["resistance"]
        dist_pct = (r["price"] - current_price) / current_price * 100
        if dist_pct < 1.0:
            risks.append(f"Resistance at {r['price']:.4g} is only {dist_pct:.2f}% away — limited room before a likely reaction")
    if direction == "short" and levels.get("support"):
        s = levels["support"]
        dist_pct = (current_price - s["price"]) / current_price * 100
        if dist_pct < 1.0:
            risks.append(f"Support at {s['price']:.4g} is only {dist_pct:.2f}% away — limited room before a likely reaction")

    # ── Pattern recognition (direction-aligned; resolve contradictions) ──
    candle_hits = detect_candlestick_patterns(candles)
    chart_hits = detect_all_chart_patterns(candles)
    chart_names = list(chart_hits.keys()) if isinstance(chart_hits, dict) else list(chart_hits or [])
    bull_charts = [p for p in chart_names if p in BULLISH_CHART_PATTERNS]
    bear_charts = [p for p in chart_names if p in BEARISH_CHART_PATTERNS]
    if bull_charts and bear_charts:
        risks.append(
            "Conflicting chart patterns detected ("
            + ", ".join(bull_charts + bear_charts)
            + ") — not counted as confirmation"
        )
        aligned_charts = []
    elif direction == "long":
        aligned_charts = bull_charts
    else:
        aligned_charts = bear_charts
    if direction == "long":
        aligned_candles = [p for p in candle_hits if p in BULLISH_CANDLES]
    else:
        aligned_candles = [p for p in candle_hits if p in BEARISH_CANDLES]
    pattern_ok = bool(aligned_candles or aligned_charts)
    confirmations.append(_confirm(
        "pattern_confirms", pattern_ok,
        {
            "candles": aligned_candles,
            "chart": aligned_charts,
            "ignored_conflict": bool(bull_charts and bear_charts),
        },
    ))
    if pattern_ok:
        reasons.append("Pattern confirmation: " + ", ".join(aligned_candles + aligned_charts))

    flag = chart_hits.get("flag_or_pennant")
    explosive_setup = bool(flag) or (breakout_ok and not breakout.get("close_confirmed", True))

    # ── Volatility: use ATR to size the stop, and flag if it's unusually wide ──
    atr_series = atr(candles)
    atr_last = atr_series[-1] if atr_series and atr_series[-1] is not None else None
    if atr_last is None or atr_last <= 0:
        return {
            "signal": False, "direction": direction, "category": None,
            "confidence": 0, "current_price": current_price,
            "reasons": reasons, "confirmations": confirmations,
            "risks": risks + ["ATR unavailable — can't size a stop-loss responsibly, so no trade idea generated."],
        }
    atr_pct = atr_last / current_price * 100
    if atr_pct > 8:
        risks.append(f"ATR is {atr_pct:.1f}% of price — unusually volatile, expect wide swings")

    upper_bb, mid_bb, lower_bb = bollinger_bands(closes)
    if upper_bb[-1] is not None and lower_bb[-1] is not None and mid_bb[-1]:
        bb_width_pct = (upper_bb[-1] - lower_bb[-1]) / mid_bb[-1] * 100
        if bb_width_pct < 2.0:
            reasons.append("Bollinger Bands are tightly squeezed — a volatility expansion is likely near")

    # ── Scoring ──
    total_confirmations = len(confirmations)
    met_count = sum(1 for c in confirmations if c["met"])
    confidence = round(met_count / total_confirmations * 100)

    if met_count >= 6 and not any("rejected" in r or "trap" in r.lower() for r in risks):
        category = "safe"
    elif met_count >= 4:
        category = "medium"
    elif explosive_setup and met_count >= 2:
        # Fewer confirmations is acceptable here — the goal is reward
        # potential on an early/aggressive entry, not high accuracy.
        category = "high_risk"
        reasons.append("Classified high-risk/high-reward: early entry on a breakout/flag setup with fewer full confirmations, but strong reward potential")
    else:
        return {
            "signal": False, "direction": direction, "category": None,
            "confidence": confidence, "current_price": current_price,
            "reasons": reasons, "confirmations": confirmations,
            "risks": risks + ["Not enough confirmations for any tier — sitting this one out."],
        }

    # ── Trade levels ──
    stop_mult = {"safe": 1.5, "medium": 2.0, "high_risk": 2.5}[category]
    rr_targets = {"safe": (1.5, 2.5, 4.0), "medium": (1.5, 3.0, 5.0), "high_risk": (2.0, 4.0, 7.0)}[category]

    entry = current_price
    if direction == "long":
        stop_loss = entry - stop_mult * atr_last
        risk = entry - stop_loss
        take_profit = [round(entry + risk * m, 8) for m in rr_targets]
    else:
        stop_loss = entry + stop_mult * atr_last
        risk = stop_loss - entry
        take_profit = [round(entry - risk * m, 8) for m in rr_targets]

    risk_reward = rr_targets[1]  # quoted R:R uses TP2, the "realistic" target

    return {
        "signal": True,
        "direction": direction,
        "category": category,
        "confidence": confidence,
        "current_price": current_price,
        "entry": round(entry, 8),
        "stop_loss": round(stop_loss, 8),
        "take_profit": take_profit,
        "risk_reward": risk_reward,
        "trend": trend,
        "reasons": reasons,
        "confirmations": confirmations,
        "risks": risks if risks else ["Standard market risk — no invalidation signal currently active."],
    }
