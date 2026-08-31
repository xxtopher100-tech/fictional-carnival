"""
Market Pulse Bot — pattern recognition.
====================================================
Candlestick patterns (single/multi-candle, evaluated on the most recent
candles) and chart patterns (built on top of market_pulse.market_structure's
swing points). Pure stdlib, operates on OHLCV candle lists.
"""

from market_pulse.market_structure import swing_highs_lows


# ═══════════════════════════════════════════════════════════════════════════
# 🕯️ CANDLESTICK PATTERNS
# ═══════════════════════════════════════════════════════════════════════════

def _body(c):
    return abs(c["close"] - c["open"])


def _range(c):
    return c["high"] - c["low"]


def _upper_wick(c):
    return c["high"] - max(c["open"], c["close"])


def _lower_wick(c):
    return min(c["open"], c["close"]) - c["low"]


def is_doji(c, body_pct_max=10.0):
    """Body is <= body_pct_max % of the candle's total range."""
    rng = _range(c)
    if rng == 0:
        return False
    return (_body(c) / rng * 100) <= body_pct_max


def is_hammer(c, lower_wick_mult=2.0, upper_wick_max_pct=25.0):
    """
    Small body near the top of the range, long lower wick (>= lower_wick_mult
    x body), small/no upper wick. Bullish reversal signal (context-dependent
    — caller should confirm it appears after a downtrend).
    """
    rng = _range(c)
    body = _body(c)
    if rng == 0 or body == 0:
        return False
    lower = _lower_wick(c)
    upper = _upper_wick(c)
    return (lower >= lower_wick_mult * body) and (upper / rng * 100 <= upper_wick_max_pct)


def is_shooting_star(c, upper_wick_mult=2.0, lower_wick_max_pct=25.0):
    """Mirror of hammer: long upper wick, small body near the bottom. Bearish reversal."""
    rng = _range(c)
    body = _body(c)
    if rng == 0 or body == 0:
        return False
    upper = _upper_wick(c)
    lower = _lower_wick(c)
    return (upper >= upper_wick_mult * body) and (lower / rng * 100 <= lower_wick_max_pct)


def is_bullish_engulfing(prev_c, c):
    """Prior candle is bearish (red), current candle is bullish (green) and
    its body fully engulfs the prior candle's body."""
    prev_bearish = prev_c["close"] < prev_c["open"]
    cur_bullish = c["close"] > c["open"]
    if not (prev_bearish and cur_bullish):
        return False
    return c["open"] <= prev_c["close"] and c["close"] >= prev_c["open"]


def is_bearish_engulfing(prev_c, c):
    """Mirror of bullish engulfing."""
    prev_bullish = prev_c["close"] > prev_c["open"]
    cur_bearish = c["close"] < c["open"]
    if not (prev_bullish and cur_bearish):
        return False
    return c["open"] >= prev_c["close"] and c["close"] <= prev_c["open"]


def detect_candlestick_patterns(candles):
    """
    Evaluates the most recent 1-2 candles for every supported pattern.
    Returns a list of pattern name strings that matched (possibly empty,
    possibly more than one — e.g. a doji that's also a hammer shape).
    """
    if not candles:
        return []
    found = []
    last = candles[-1]

    if is_doji(last):
        found.append("doji")
    if is_hammer(last):
        found.append("hammer")
    if is_shooting_star(last):
        found.append("shooting_star")

    if len(candles) >= 2:
        prev = candles[-2]
        if is_bullish_engulfing(prev, last):
            found.append("bullish_engulfing")
        if is_bearish_engulfing(prev, last):
            found.append("bearish_engulfing")

    return found


# ═══════════════════════════════════════════════════════════════════════════
# 📐 CHART PATTERNS  (built on swing points)
# ═══════════════════════════════════════════════════════════════════════════

def detect_double_top(candles, left=3, right=3, tolerance_pct=1.0, min_separation=5):
    """
    Two swing highs at roughly the same price, separated by at least
    `min_separation` candles, with a swing low (the "neckline") between
    them. Returns None or {"peaks": [(i1,p1),(i2,p2)], "neckline": price}.
    """
    highs, lows = swing_highs_lows(candles, left, right)
    if len(highs) < 2:
        return None
    candidates = []
    for a in range(len(highs) - 1):
        for b in range(a + 1, len(highs)):
            i1, p1 = highs[a]
            i2, p2 = highs[b]
            if i2 - i1 < min_separation:
                continue
            if abs(p1 - p2) / p1 * 100 <= tolerance_pct:
                between_lows = [l for l in lows if i1 < l[0] < i2]
                if between_lows:
                    neckline = min(l[1] for l in between_lows)
                    candidates.append({"peaks": [(i1, p1), (i2, p2)], "neckline": neckline})
    if not candidates:
        return None
    # Prefer the most prominent pair (highest average peak price) — the
    # first-by-index match can pick up minor early swing highs on noisy data.
    return max(candidates, key=lambda d: (d["peaks"][0][1] + d["peaks"][1][1]) / 2)


def detect_double_bottom(candles, left=3, right=3, tolerance_pct=1.0, min_separation=5):
    """Mirror of detect_double_top."""
    highs, lows = swing_highs_lows(candles, left, right)
    if len(lows) < 2:
        return None
    candidates = []
    for a in range(len(lows) - 1):
        for b in range(a + 1, len(lows)):
            i1, p1 = lows[a]
            i2, p2 = lows[b]
            if i2 - i1 < min_separation:
                continue
            if abs(p1 - p2) / p1 * 100 <= tolerance_pct:
                between_highs = [h for h in highs if i1 < h[0] < i2]
                if between_highs:
                    neckline = max(h[1] for h in between_highs)
                    candidates.append({"troughs": [(i1, p1), (i2, p2)], "neckline": neckline})
    if not candidates:
        return None
    # Prefer the most prominent pair (lowest average trough price).
    return min(candidates, key=lambda d: (d["troughs"][0][1] + d["troughs"][1][1]) / 2)


def detect_head_and_shoulders(candles, left=3, right=3, shoulder_tolerance_pct=3.0):
    """
    Three consecutive swing highs where the middle one is clearly the
    tallest (the head) and the two outer ones (shoulders) are roughly
    equal height. Returns None or
    {"left_shoulder":.., "head":.., "right_shoulder":.., "neckline": price}.
    """
    highs, lows = swing_highs_lows(candles, left, right)
    if len(highs) < 3:
        return None
    for i in range(len(highs) - 2):
        ls, head, rs = highs[i], highs[i + 1], highs[i + 2]
        if not (head[1] > ls[1] and head[1] > rs[1]):
            continue
        if abs(ls[1] - rs[1]) / ls[1] * 100 > shoulder_tolerance_pct:
            continue
        between = [l for l in lows if ls[0] < l[0] < rs[0]]
        if len(between) < 2:
            continue
        neckline = sum(l[1] for l in between[:2]) / 2
        return {"left_shoulder": ls, "head": head, "right_shoulder": rs, "neckline": neckline}
    return None


def detect_inverse_head_and_shoulders(candles, left=3, right=3, shoulder_tolerance_pct=3.0):
    """Mirror of detect_head_and_shoulders, built on swing lows."""
    highs, lows = swing_highs_lows(candles, left, right)
    if len(lows) < 3:
        return None
    for i in range(len(lows) - 2):
        ls, head, rs = lows[i], lows[i + 1], lows[i + 2]
        if not (head[1] < ls[1] and head[1] < rs[1]):
            continue
        if abs(ls[1] - rs[1]) / ls[1] * 100 > shoulder_tolerance_pct:
            continue
        between = [h for h in highs if ls[0] < h[0] < rs[0]]
        if len(between) < 2:
            continue
        neckline = sum(h[1] for h in between[:2]) / 2
        return {"left_shoulder": ls, "head": head, "right_shoulder": rs, "neckline": neckline}
    return None


def _trendline_slope(points):
    """Simple least-squares slope over a list of (index, price) points."""
    n = len(points)
    if n < 2:
        return 0.0
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    denom = sum((x - mean_x) ** 2 for x in xs)
    if denom == 0:
        return 0.0
    return sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / denom


def detect_triangle(candles, left=3, right=3, min_points=3, flat_slope_pct=0.05):
    """
    Fits trendlines across recent swing highs and swing lows and classifies
    the pattern by their slopes:
      - both converging (highs falling, lows rising)        -> "symmetrical"
      - highs flat, lows rising                              -> "ascending"
      - highs falling, lows flat                              -> "descending"
    `flat_slope_pct` is the slope magnitude (as % of price per candle)
    below which a line counts as "flat" rather than trending.
    Returns None if there isn't a clean triangle shape.
    """
    highs, lows = swing_highs_lows(candles, left, right)
    if len(highs) < min_points or len(lows) < min_points:
        return None
    highs = highs[-min_points:]
    lows = lows[-min_points:]
    avg_price = (sum(p for _, p in highs) + sum(p for _, p in lows)) / (len(highs) + len(lows))
    if avg_price == 0:
        return None

    high_slope = _trendline_slope(highs)
    low_slope = _trendline_slope(lows)
    high_slope_pct = high_slope / avg_price * 100
    low_slope_pct = low_slope / avg_price * 100

    high_flat = abs(high_slope_pct) <= flat_slope_pct
    low_flat = abs(low_slope_pct) <= flat_slope_pct

    if not high_flat and not low_flat and high_slope_pct < 0 and low_slope_pct > 0:
        return {"type": "symmetrical", "highs": highs, "lows": lows}
    if high_flat and not low_flat and low_slope_pct > 0:
        return {"type": "ascending", "highs": highs, "lows": lows}
    if low_flat and not high_flat and high_slope_pct < 0:
        return {"type": "descending", "highs": highs, "lows": lows}
    return None


def detect_flag_or_pennant(candles, impulse_lookback=10, impulse_pct=5.0,
                            consolidation_len=5, consolidation_max_range_pct=3.0):
    """
    Looks for a sharp directional move (the flagpole) followed by a tight
    sideways consolidation (the flag/pennant). Doesn't distinguish flag vs
    pennant shape (parallel vs converging channel) — reports "flag_or_pennant"
    with the pole direction, which is what matters for the trade engine.

    Returns None or {"direction": "bullish"/"bearish", "pole_pct": float}.
    """
    n = len(candles)
    if n < impulse_lookback + consolidation_len:
        return None

    consolidation = candles[-consolidation_len:]
    pole_window = candles[-(impulse_lookback + consolidation_len):-consolidation_len]
    if not pole_window:
        return None

    pole_move_pct = (pole_window[-1]["close"] - pole_window[0]["open"]) / pole_window[0]["open"] * 100

    cons_high = max(c["high"] for c in consolidation)
    cons_low = min(c["low"] for c in consolidation)
    cons_mid = (cons_high + cons_low) / 2
    if cons_mid == 0:
        return None
    cons_range_pct = (cons_high - cons_low) / cons_mid * 100

    if cons_range_pct > consolidation_max_range_pct:
        return None

    if pole_move_pct >= impulse_pct:
        return {"direction": "bullish", "pole_pct": pole_move_pct}
    if pole_move_pct <= -impulse_pct:
        return {"direction": "bearish", "pole_pct": pole_move_pct}
    return None


def detect_all_chart_patterns(candles, **kwargs):
    """Runs every chart-pattern detector and returns a dict of only the hits."""
    results = {}
    checks = {
        "double_top": detect_double_top,
        "double_bottom": detect_double_bottom,
        "head_and_shoulders": detect_head_and_shoulders,
        "inverse_head_and_shoulders": detect_inverse_head_and_shoulders,
        "triangle": detect_triangle,
        "flag_or_pennant": detect_flag_or_pennant,
    }
    for name, fn in checks.items():
        try:
            result = fn(candles)
        except Exception:
            result = None
        if result:
            results[name] = result
    return results
