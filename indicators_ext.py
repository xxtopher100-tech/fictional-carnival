"""
Market Pulse Bot — extended technical indicators.
====================================================
Pure-Python, stdlib-only implementations (no numpy/pandas), matching the
style of the existing `market_pulse.indicators` module (rsi_wilder, sma).

Every function takes plain lists of floats (or OHLCV dicts where noted)
and returns either a single float, or a list aligned to the input
(padded with None where there isn't enough data yet — never silently
truncated, so callers can zip() against timestamps safely).

Candle format expected by the OHLCV-based functions, unless noted:
    {"open": float, "high": float, "low": float, "close": float, "volume": float}

No indicator here mutates its input.
"""

# ═══════════════════════════════════════════════════════════════════════════
# 📈 TREND
# ═══════════════════════════════════════════════════════════════════════════

def ema(values, period):
    """Exponential moving average. Returns a list same length as `values`,
    with None for indices before the EMA can be computed."""
    if not values or period <= 0 or len(values) < period:
        return [None] * len(values)
    k = 2.0 / (period + 1)
    out = [None] * (period - 1)
    seed = sum(values[:period]) / period
    out.append(seed)
    prev = seed
    for v in values[period:]:
        prev = v * k + prev * (1 - k)
        out.append(prev)
    return out


def ema_last(values, period):
    """Convenience: just the most recent EMA value, or None."""
    series = ema(values, period)
    return series[-1] if series else None


def ema_stack(closes):
    """EMA20/50/100/200 snapshot used for trend-alignment scoring."""
    return {
        "ema20": ema_last(closes, 20),
        "ema50": ema_last(closes, 50),
        "ema100": ema_last(closes, 100),
        "ema200": ema_last(closes, 200),
    }


def ema_alignment(closes):
    """
    Returns "bullish", "bearish", or "mixed" based on EMA20 > EMA50 > EMA100 > EMA200
    (or the reverse). Used as one trend confirmation in the signal engine.
    Returns None if there isn't enough data for all four EMAs yet.
    """
    s = ema_stack(closes)
    if None in s.values():
        return None
    e20, e50, e100, e200 = s["ema20"], s["ema50"], s["ema100"], s["ema200"]
    if e20 > e50 > e100 > e200:
        return "bullish"
    if e20 < e50 < e100 < e200:
        return "bearish"
    return "mixed"


def vwap(candles):
    """
    Session VWAP. `candles` is a list of OHLCV dicts for the current session
    (caller is responsible for windowing — e.g. last 24h of 1h candles).
    Returns None if candles is empty.
    """
    if not candles:
        return None
    cum_pv = 0.0
    cum_vol = 0.0
    for c in candles:
        typical = (c["high"] + c["low"] + c["close"]) / 3.0
        vol = c.get("volume", 0) or 0
        cum_pv += typical * vol
        cum_vol += vol
    if cum_vol == 0:
        return None
    return cum_pv / cum_vol


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MOMENTUM
# ═══════════════════════════════════════════════════════════════════════════

def macd(closes, fast=12, slow=26, signal=9):
    """
    Returns (macd_line, signal_line, histogram) — each a list aligned to
    `closes`, with None padding where undefined.
    """
    if len(closes) < slow + signal:
        pad = [None] * len(closes)
        return pad, pad, pad

    ema_fast = ema(closes, fast)
    ema_slow = ema(closes, slow)
    macd_line = [
        (f - s) if (f is not None and s is not None) else None
        for f, s in zip(ema_fast, ema_slow)
    ]
    # signal = EMA of the macd_line, but only over the non-None tail
    first_valid = next((i for i, v in enumerate(macd_line) if v is not None), None)
    if first_valid is None:
        pad = [None] * len(closes)
        return macd_line, pad, pad
    tail = macd_line[first_valid:]
    signal_tail = ema(tail, signal)
    signal_line = [None] * first_valid + signal_tail
    hist = [
        (m - s) if (m is not None and s is not None) else None
        for m, s in zip(macd_line, signal_line)
    ]
    return macd_line, signal_line, hist


def macd_crossover(closes, fast=12, slow=26, signal=9):
    """
    Returns "bullish_cross" / "bearish_cross" / "none" based on the last
    two histogram values (sign flip = fresh crossover this candle).
    """
    _, _, hist = macd(closes, fast, slow, signal)
    valid = [h for h in hist if h is not None]
    if len(valid) < 2:
        return "none"
    prev, last = valid[-2], valid[-1]
    if prev <= 0 < last:
        return "bullish_cross"
    if prev >= 0 > last:
        return "bearish_cross"
    return "none"


def _rsi_series_wilder(closes, period=14):
    """
    Self-contained Wilder RSI series (deliberately NOT importing the
    existing market_pulse.indicators.rsi_wilder here — that function's
    exact return shape wasn't available to verify against, and silently
    guessing at it would risk feeding wrong values into every downstream
    signal. This is the standard Wilder formula, independently verifiable.
    Returns a list aligned to `closes`, None-padded.
    """
    n = len(closes)
    out = [None] * n
    if n <= period:
        return out
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        delta = closes[i] - closes[i - 1]
        gains[i] = max(delta, 0.0)
        losses[i] = max(-delta, 0.0)

    avg_gain = sum(gains[1: period + 1]) / period
    avg_loss = sum(losses[1: period + 1]) / period
    out[period] = 100.0 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss))

    for i in range(period + 1, n):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        out[i] = 100.0 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss))
    return out


def stoch_rsi(closes, rsi_period=14, stoch_period=14, smooth_k=3, smooth_d=3):
    """
    Stochastic RSI, built on the self-contained Wilder RSI above.
    Returns (k_series, d_series), each aligned to `closes`, 0-100 scale.
    """
    series = _rsi_series_wilder(closes, rsi_period)

    k_raw = [None] * len(series)
    for i in range(len(series)):
        window = [v for v in series[max(0, i - stoch_period + 1): i + 1] if v is not None]
        if len(window) < stoch_period or series[i] is None:
            continue
        lo, hi = min(window), max(window)
        k_raw[i] = 0.0 if hi == lo else (series[i] - lo) / (hi - lo) * 100

    def _sma_series(vals, period):
        out = [None] * len(vals)
        for i in range(len(vals)):
            window = [v for v in vals[max(0, i - period + 1): i + 1] if v is not None]
            if len(window) < period:
                continue
            out[i] = sum(window) / period
        return out

    k = _sma_series(k_raw, smooth_k)
    d = _sma_series(k, smooth_d)
    return k, d


def adx(candles, period=14):
    """
    Average Directional Index (Wilder's smoothing). `candles` is a list of
    OHLC dicts. Returns a list aligned to `candles`, None-padded.
    """
    n = len(candles)
    if n < period * 2:
        return [None] * n

    plus_dm = [0.0] * n
    minus_dm = [0.0] * n
    tr = [0.0] * n

    for i in range(1, n):
        up_move = candles[i]["high"] - candles[i - 1]["high"]
        down_move = candles[i - 1]["low"] - candles[i]["low"]
        plus_dm[i] = up_move if (up_move > down_move and up_move > 0) else 0.0
        minus_dm[i] = down_move if (down_move > up_move and down_move > 0) else 0.0
        tr[i] = max(
            candles[i]["high"] - candles[i]["low"],
            abs(candles[i]["high"] - candles[i - 1]["close"]),
            abs(candles[i]["low"] - candles[i - 1]["close"]),
        )

    def _wilder_smooth(vals):
        out = [None] * n
        seed = sum(vals[1: period + 1])
        out[period] = seed
        for i in range(period + 1, n):
            out[i] = out[i - 1] - (out[i - 1] / period) + vals[i]
        return out

    sm_tr = _wilder_smooth(tr)
    sm_plus = _wilder_smooth(plus_dm)
    sm_minus = _wilder_smooth(minus_dm)

    dx = [None] * n
    for i in range(period, n):
        if not sm_tr[i]:
            continue
        pdi = 100 * sm_plus[i] / sm_tr[i]
        mdi = 100 * sm_minus[i] / sm_tr[i]
        denom = pdi + mdi
        dx[i] = 0.0 if denom == 0 else 100 * abs(pdi - mdi) / denom

    out = [None] * n
    valid_dx = [v for v in dx if v is not None]
    if len(valid_dx) >= period:
        start = next(i for i, v in enumerate(dx) if v is not None) + period - 1
        if start < n:
            out[start] = sum(v for v in dx[start - period + 1: start + 1] if v is not None) / period
            for i in range(start + 1, n):
                if dx[i] is None or out[i - 1] is None:
                    continue
                out[i] = (out[i - 1] * (period - 1) + dx[i]) / period
    return out


def cci(candles, period=20):
    """Commodity Channel Index. Returns a list aligned to `candles`."""
    n = len(candles)
    typical = [(c["high"] + c["low"] + c["close"]) / 3.0 for c in candles]
    out = [None] * n
    for i in range(period - 1, n):
        window = typical[i - period + 1: i + 1]
        mean = sum(window) / period
        mean_dev = sum(abs(t - mean) for t in window) / period
        out[i] = None if mean_dev == 0 else (typical[i] - mean) / (0.015 * mean_dev)
    return out


def momentum_oscillator(closes, period=10):
    """Simple momentum: close[i] - close[i-period], as % of close[i-period]."""
    n = len(closes)
    out = [None] * n
    for i in range(period, n):
        base = closes[i - period]
        if base:
            out[i] = (closes[i] - base) / base * 100
    return out


# ═══════════════════════════════════════════════════════════════════════════
# 🌊 VOLATILITY
# ═══════════════════════════════════════════════════════════════════════════

def atr(candles, period=14):
    """Average True Range (Wilder's smoothing). Returns a list aligned to `candles`."""
    n = len(candles)
    if n < 2:
        return [None] * n
    tr = [None] + [
        max(
            candles[i]["high"] - candles[i]["low"],
            abs(candles[i]["high"] - candles[i - 1]["close"]),
            abs(candles[i]["low"] - candles[i - 1]["close"]),
        )
        for i in range(1, n)
    ]
    out = [None] * n
    valid = [v for v in tr[1: period + 1] if v is not None]
    if len(valid) < period:
        return out
    out[period] = sum(valid) / period
    for i in range(period + 1, n):
        out[i] = (out[i - 1] * (period - 1) + tr[i]) / period
    return out


def bollinger_bands(closes, period=20, num_std=2.0):
    """Returns (upper, mid, lower) lists aligned to `closes`."""
    n = len(closes)
    mid = [None] * n
    upper = [None] * n
    lower = [None] * n
    for i in range(period - 1, n):
        window = closes[i - period + 1: i + 1]
        m = sum(window) / period
        variance = sum((x - m) ** 2 for x in window) / period
        sd = variance ** 0.5
        mid[i] = m
        upper[i] = m + num_std * sd
        lower[i] = m - num_std * sd
    return upper, mid, lower


def keltner_channels(candles, ema_period=20, atr_period=10, mult=2.0):
    """Returns (upper, mid, lower) lists aligned to `candles`."""
    closes = [c["close"] for c in candles]
    mid = ema(closes, ema_period)
    atr_series = atr(candles, atr_period)
    upper = [None] * len(candles)
    lower = [None] * len(candles)
    for i in range(len(candles)):
        if mid[i] is not None and atr_series[i] is not None:
            upper[i] = mid[i] + mult * atr_series[i]
            lower[i] = mid[i] - mult * atr_series[i]
    return upper, mid, lower


# ═══════════════════════════════════════════════════════════════════════════
# 📊 VOLUME
# ═══════════════════════════════════════════════════════════════════════════

def obv(candles):
    """On-Balance Volume. Returns a cumulative list aligned to `candles`."""
    if not candles:
        return []
    out = [0.0]
    for i in range(1, len(candles)):
        vol = candles[i].get("volume", 0) or 0
        if candles[i]["close"] > candles[i - 1]["close"]:
            out.append(out[-1] + vol)
        elif candles[i]["close"] < candles[i - 1]["close"]:
            out.append(out[-1] - vol)
        else:
            out.append(out[-1])
    return out


def volume_profile(candles, num_bins=12):
    """
    Coarse volume-at-price histogram. Returns a list of
    {"price_low": .., "price_high": .., "volume": ..} bins sorted by price,
    plus the single highest-volume bin as "poc" (point of control).
    """
    if not candles:
        return {"bins": [], "poc": None}
    lo = min(c["low"] for c in candles)
    hi = max(c["high"] for c in candles)
    if hi <= lo:
        return {"bins": [], "poc": None}
    width = (hi - lo) / num_bins
    bins = [{"price_low": lo + i * width, "price_high": lo + (i + 1) * width, "volume": 0.0}
            for i in range(num_bins)]
    for c in candles:
        mid_price = (c["high"] + c["low"]) / 2.0
        idx = min(int((mid_price - lo) / width), num_bins - 1)
        bins[idx]["volume"] += c.get("volume", 0) or 0
    poc = max(bins, key=lambda b: b["volume"]) if bins else None
    return {"bins": bins, "poc": poc}


def delta_volume(candles):
    """
    Approximate buy/sell delta per candle using close-position-in-range as
    a proxy for aggressor side (no tick data available from REST candles).
    Returns a list of per-candle deltas (positive = net buying pressure).
    """
    out = []
    for c in candles:
        rng = c["high"] - c["low"]
        vol = c.get("volume", 0) or 0
        if rng == 0:
            out.append(0.0)
            continue
        buy_fraction = (c["close"] - c["low"]) / rng
        out.append(vol * (2 * buy_fraction - 1))
    return out


def volume_spike(candles, lookback=20, threshold=2.0):
    """
    Returns True if the most recent candle's volume is >= `threshold`x the
    average of the prior `lookback` candles. None if not enough data.
    """
    if len(candles) < lookback + 1:
        return None
    prior = [c.get("volume", 0) or 0 for c in candles[-lookback - 1: -1]]
    avg = sum(prior) / len(prior)
    if avg == 0:
        return None
    return candles[-1].get("volume", 0) >= threshold * avg
