"""
Market Pulse Bot — market structure detection.
====================================================
Pure-Python, stdlib-only. Operates on lists of OHLCV dicts:
    {"open": .., "high": .., "low": .., "close": .., "volume": ..}
ordered oldest -> newest, one entry per candle.

Nothing here calls out to an exchange or the DB — it's pure analysis so
it can be unit tested against fixed candle arrays.
"""


# ═══════════════════════════════════════════════════════════════════════════
# SWING POINTS
# ═══════════════════════════════════════════════════════════════════════════

def swing_highs_lows(candles, left=3, right=3):
    """
    Fractal-style swing point detection: a candle is a swing high if its
    high is the max within [i-left, i+right], and a swing low if its low
    is the min within that same window.

    Returns (swing_highs, swing_lows) — each a list of (index, price) tuples.
    Points within `right` candles of the end can't be confirmed yet and are
    excluded (a swing point isn't a swing point until the candles after it
    exist).
    """
    n = len(candles)
    highs, lows = [], []
    for i in range(left, n - right):
        window = candles[i - left: i + right + 1]
        h = candles[i]["high"]
        l = candles[i]["low"]
        if h == max(c["high"] for c in window):
            highs.append((i, h))
        if l == min(c["low"] for c in window):
            lows.append((i, l))
    return highs, lows


# ═══════════════════════════════════════════════════════════════════════════
# SUPPORT / RESISTANCE
# ═══════════════════════════════════════════════════════════════════════════

def support_resistance_levels(candles, left=3, right=3, cluster_pct=0.5, min_touches=2):
    """
    Clusters swing highs/lows into support/resistance zones.

    cluster_pct: swing points within this % of each other are merged into
    one zone (e.g. 0.5 = merge points within 0.5% of the zone's average price).
    min_touches: a zone must be touched by at least this many swing points
    to be reported (filters out noise).

    Returns {"resistance": [...], "support": [...]}, each a list of dicts:
        {"price": avg_price, "touches": n, "first_index": i, "last_index": j}
    sorted by touch count descending.
    """
    highs, lows = swing_highs_lows(candles, left, right)

    def _cluster(points):
        if not points:
            return []
        points = sorted(points, key=lambda p: p[1])
        zones = []
        current = [points[0]]
        for idx, price in points[1:]:
            zone_avg = sum(p[1] for p in current) / len(current)
            if abs(price - zone_avg) / zone_avg * 100 <= cluster_pct:
                current.append((idx, price))
            else:
                zones.append(current)
                current = [(idx, price)]
        zones.append(current)

        out = []
        for z in zones:
            if len(z) < min_touches:
                continue
            avg_price = sum(p[1] for p in z) / len(z)
            out.append({
                "price": avg_price,
                "touches": len(z),
                "first_index": min(p[0] for p in z),
                "last_index": max(p[0] for p in z),
            })
        return sorted(out, key=lambda z: -z["touches"])

    return {"resistance": _cluster(highs), "support": _cluster(lows)}


def nearest_levels(candles, current_price, **kwargs):
    """Convenience: nearest support below and resistance above current_price."""
    levels = support_resistance_levels(candles, **kwargs)
    supports_below = [s for s in levels["support"] if s["price"] < current_price]
    resistances_above = [r for r in levels["resistance"] if r["price"] > current_price]
    nearest_support = max(supports_below, key=lambda s: s["price"]) if supports_below else None
    nearest_resistance = min(resistances_above, key=lambda r: r["price"]) if resistances_above else None
    return {"support": nearest_support, "resistance": nearest_resistance}


# ═══════════════════════════════════════════════════════════════════════════
# BREAKOUTS
# ═══════════════════════════════════════════════════════════════════════════

def detect_breakout(candles, lookback=20, confirm_closes=1, vol_confirm_mult=1.3):
    """
    Checks whether the latest candle broke above the highest high (or below
    the lowest low) of the prior `lookback` candles, with optional close
    confirmation and volume confirmation.

    Returns None if no breakout, or a dict:
        {"direction": "up"/"down", "level": float, "volume_confirmed": bool,
         "close_confirmed": bool}
    """
    n = len(candles)
    confirm_closes = max(confirm_closes, 1)
    if n < lookback + confirm_closes:
        return None

    window = candles[-(lookback + confirm_closes): -confirm_closes]
    last = candles[-1]
    prior_avg_vol = sum((c.get("volume", 0) or 0) for c in window) / len(window)
    vol_confirmed = (last.get("volume", 0) or 0) >= vol_confirm_mult * prior_avg_vol if prior_avg_vol else False

    confirm_slice = candles[-confirm_closes:]
    recent_high = max(c["high"] for c in window)
    recent_low = min(c["low"] for c in window)

    if last["high"] > recent_high:
        close_confirmed = all(c["close"] > recent_high for c in confirm_slice)
        return {"direction": "up", "level": recent_high,
                "volume_confirmed": vol_confirmed, "close_confirmed": close_confirmed}
    if last["low"] < recent_low:
        close_confirmed = all(c["close"] < recent_low for c in confirm_slice)
        return {"direction": "down", "level": recent_low,
                "volume_confirmed": vol_confirmed, "close_confirmed": close_confirmed}
    return None


def detect_fake_breakout(candles, lookback=20, wick_reject_pct=50):
    """
    Looks for a breakout candle that wicked beyond a recent level but closed
    back inside it — classic stop-hunt / fake breakout signature.

    wick_reject_pct: the wick beyond the level must be at least this % of
    the candle's total range for it to count as a rejection (filters out
    breakouts that just barely poked through).

    Returns None, or {"direction": "bull_trap"/"bear_trap", "level": float}.
    ("bull_trap" = price broke above resistance then got rejected back down —
    bearish for continuation. "bear_trap" = the mirror image.)
    """
    n = len(candles)
    if n < lookback + 1:
        return None
    window = candles[-lookback - 1: -1]
    last = candles[-1]
    recent_high = max(c["high"] for c in window)
    recent_low = min(c["low"] for c in window)
    rng = last["high"] - last["low"]
    if rng == 0:
        return None

    if last["high"] > recent_high and last["close"] < recent_high:
        upper_wick = last["high"] - max(last["open"], last["close"])
        if upper_wick / rng * 100 >= wick_reject_pct:
            return {"direction": "bull_trap", "level": recent_high}

    if last["low"] < recent_low and last["close"] > recent_low:
        lower_wick = min(last["open"], last["close"]) - last["low"]
        if lower_wick / rng * 100 >= wick_reject_pct:
            return {"direction": "bear_trap", "level": recent_low}

    return None


# ═══════════════════════════════════════════════════════════════════════════
# LIQUIDITY ZONES / FAIR VALUE GAPS / ORDER BLOCKS  (ICT-style structure)
# ═══════════════════════════════════════════════════════════════════════════

def detect_liquidity_zones(candles, left=3, right=3, equal_tolerance_pct=0.15):
    """
    Finds clusters of roughly-equal swing highs/lows — classic "liquidity
    resting above/below" zones (equal highs = buy-side liquidity, equal
    lows = sell-side liquidity).

    Returns {"buy_side": [...], "sell_side": [...]} — each a list of
    {"price": avg, "count": n} for clusters with >= 2 equal points.
    """
    highs, lows = swing_highs_lows(candles, left, right)

    def _equal_clusters(points):
        if len(points) < 2:
            return []
        points = sorted(points, key=lambda p: p[1])
        clusters = []
        current = [points[0]]
        for idx, price in points[1:]:
            ref = current[-1][1]
            if ref and abs(price - ref) / ref * 100 <= equal_tolerance_pct:
                current.append((idx, price))
            else:
                if len(current) >= 2:
                    clusters.append(current)
                current = [(idx, price)]
        if len(current) >= 2:
            clusters.append(current)
        return [{"price": sum(p[1] for p in c) / len(c), "count": len(c)} for c in clusters]

    return {"buy_side": _equal_clusters(highs), "sell_side": _equal_clusters(lows)}


def detect_fair_value_gaps(candles, min_gap_pct=0.05):
    """
    3-candle imbalance / FVG detection: a bullish FVG exists when
    candle[i-1].high < candle[i+1].low (a gap the price hasn't traded
    through yet); bearish is the mirror.

    Returns a list of {"direction": "bullish"/"bearish", "top": .., "bottom": ..,
    "index": i} for every unfilled gap as of the last candle (gaps that have
    since been fully traded through are excluded).
    """
    gaps = []
    n = len(candles)
    for i in range(1, n - 1):
        prev_c, next_c = candles[i - 1], candles[i + 1]
        if prev_c["high"] < next_c["low"]:
            gap_size_pct = (next_c["low"] - prev_c["high"]) / prev_c["high"] * 100
            if gap_size_pct >= min_gap_pct:
                gaps.append({"direction": "bullish", "top": next_c["low"],
                             "bottom": prev_c["high"], "index": i})
        elif prev_c["low"] > next_c["high"]:
            gap_size_pct = (prev_c["low"] - next_c["high"]) / next_c["high"] * 100
            if gap_size_pct >= min_gap_pct:
                gaps.append({"direction": "bearish", "top": prev_c["low"],
                             "bottom": next_c["high"], "index": i})

    # Drop gaps that later price action has fully closed
    unfilled = []
    for g in gaps:
        filled = False
        for c in candles[g["index"] + 2:]:
            if c["low"] <= g["bottom"] and c["high"] >= g["top"]:
                filled = True
                break
        if not filled:
            unfilled.append(g)
    return unfilled


def detect_order_blocks(candles, impulse_pct=1.0):
    """
    Heuristic order-block detection: the last down-close candle before a
    strong up-impulse (>= impulse_pct % move over the next candle) is a
    bullish order block (institutional buy zone); mirror for bearish.

    Returns a list of {"direction": "bullish"/"bearish", "top": .., "bottom": ..,
    "index": i}, most recent last.
    """
    blocks = []
    n = len(candles)
    for i in range(0, n - 1):
        cur = candles[i]
        nxt = candles[i + 1]
        move_pct = (nxt["close"] - cur["close"]) / cur["close"] * 100 if cur["close"] else 0

        if cur["close"] < cur["open"] and move_pct >= impulse_pct:
            blocks.append({"direction": "bullish", "top": cur["high"],
                           "bottom": cur["low"], "index": i})
        elif cur["close"] > cur["open"] and move_pct <= -impulse_pct:
            blocks.append({"direction": "bearish", "top": cur["high"],
                           "bottom": cur["low"], "index": i})
    return blocks
