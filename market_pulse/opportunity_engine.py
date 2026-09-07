"""
Market Pulse — opportunity layer (small / normal / big).

Fuses candle structure, volatility, history, Fear & Greed, optional funding.
Does NOT invent Entry/SL/TP (setup_engine / edge still own levels).
Does NOT invent news causality.

Used by trade_scanner to rank and prioritize majors so real moves are less
likely to be starved by weak FX / low-priority alts.
"""

from __future__ import annotations

from market_pulse.config_runtime import logger
from market_pulse.candle_engine import (
    get_candles,
    get_candles_15m,
    candles_ready,
    candles_15m_ready,
)
from market_pulse.price_fetchers import get_best_price

try:
    from market_pulse.fear_greed import get_fear_greed
except Exception:  # pragma: no cover
    get_fear_greed = None  # type: ignore

try:
    from market_pulse.derivatives_engine import (
        get_derivatives_snapshot,
        get_recent_liquidations,
    )
except Exception:  # pragma: no cover
    get_derivatives_snapshot = None  # type: ignore
    get_recent_liquidations = None  # type: ignore

try:
    from market_pulse.setup_engine import news_market_flag
except Exception:  # pragma: no cover
    news_market_flag = None  # type: ignore

# Majors get explicit priority for opportunity detection
PRIORITY_COINS = frozenset({"BTC", "ETH", "SOL", "BNB", "XRP"})
CORE_MAJORS = frozenset({"BTC", "ETH"})


def _atr_series(candles: list, period: int = 14) -> list[float]:
    if not candles or len(candles) < period + 1:
        return []
    out: list[float] = []
    for i in range(1, len(candles)):
        try:
            h = float(candles[i].get("high") or 0)
            l = float(candles[i].get("low") or 0)
            pc = float(candles[i - 1].get("close") or 0)
            if h <= 0 or l <= 0 or pc <= 0:
                continue
            tr = max(h - l, abs(h - pc), abs(l - pc))
            out.append(tr)
        except Exception:
            continue
    if len(out) < period:
        return []
    atrs: list[float] = []
    for i in range(period - 1, len(out)):
        window = out[i - period + 1 : i + 1]
        atrs.append(sum(window) / float(period))
    return atrs


def _ema_last(closes: list[float], period: int) -> float | None:
    if len(closes) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / float(period)
    for c in closes[period:]:
        ema = c * k + ema * (1 - k)
    return float(ema)


def _funding_and_oi(coin: str) -> dict:
    """Funding + open interest from derivatives snapshot when engine is live."""
    out = {
        "funding_label": "unavailable",
        "funding_rate": None,
        "oi": None,
        "oi_value": None,
    }
    if not get_derivatives_snapshot:
        return out
    try:
        snap = get_derivatives_snapshot(coin)
        if not snap:
            return out
        fr = snap.get("funding_rate")
        if fr is not None:
            fr = float(fr)
            out["funding_rate"] = fr
            if fr > 0.0005:
                out["funding_label"] = "longs_crowded"
            elif fr < -0.0005:
                out["funding_label"] = "shorts_crowded"
            else:
                out["funding_label"] = "neutral"
        if snap.get("open_interest") is not None:
            out["oi"] = float(snap["open_interest"])
        if snap.get("open_interest_value") is not None:
            out["oi_value"] = float(snap["open_interest_value"])
    except Exception:
        pass
    return out


def _liquidation_pressure(coin: str) -> dict:
    """Count recent liquidations (15m window) — whale/forced-flow proxy."""
    out = {
        "count": 0,
        "buy_liq": 0,
        "sell_liq": 0,
        "notional": 0.0,
        "label": "none",
    }
    if not get_recent_liquidations:
        return out
    try:
        rows = get_recent_liquidations(coin, max_age_sec=900.0, limit=50) or []
        out["count"] = len(rows)
        notional = 0.0
        for r in rows:
            side = str(r.get("side") or r.get("direction") or "").lower()
            # Bybit: S side of liquidated position
            if "buy" in side or "long" in side:
                out["buy_liq"] += 1
            elif "sell" in side or "short" in side:
                out["sell_liq"] += 1
            try:
                px = float(r.get("price") or 0)
                sz = float(r.get("size") or 0)
                if px > 0 and sz > 0:
                    notional += px * sz
            except Exception:
                pass
        out["notional"] = round(notional, 2)
        # Heavy if many events OR large notional (BTC/ETH scale)
        if out["count"] >= 8 or notional >= 2_000_000:
            out["label"] = "heavy"
        elif out["count"] >= 3 or notional >= 500_000:
            out["label"] = "elevated"
    except Exception:
        pass
    return out


def _micro_15m_signal(coin: str, price: float) -> dict:
    """SMALL-opportunity refinement from 15m candles when available."""
    out = {"ready": False, "micro_trend": "neutral", "boost": 0.0, "reasons": []}
    try:
        if not candles_15m_ready(coin, min_candles=12):
            return out
        c15 = get_candles_15m(coin) or []
        if len(c15) < 12:
            return out
        closes = [float(c["close"]) for c in c15 if c.get("close")]
        if len(closes) < 12:
            return out
        out["ready"] = True
        e8 = _ema_last(closes, 8)
        e21 = _ema_last(closes, 21) if len(closes) >= 21 else _ema_last(closes, 12)
        if e8 and e21:
            if e8 > e21 and price >= e8 * 0.998:
                out["micro_trend"] = "bullish"
                out["boost"] = 5.0
                out["reasons"].append("15m_trend_bull")
            elif e8 < e21 and price <= e8 * 1.002:
                out["micro_trend"] = "bearish"
                out["boost"] = 5.0
                out["reasons"].append("15m_trend_bear")
        # Short-term range break on 15m
        highs = [float(c.get("high") or 0) for c in c15[-16:]]
        lows = [float(c.get("low") or 0) for c in c15[-16:]]
        if highs and lows and max(highs) > 0:
            if price >= max(highs) * 0.997:
                out["boost"] += 4.0
                out["reasons"].append("15m_high_test")
            elif price <= min(x for x in lows if x > 0) * 1.003:
                out["boost"] += 4.0
                out["reasons"].append("15m_low_test")
    except Exception:
        pass
    return out


def assess_opportunity(coin: str, price: float | None = None) -> dict:
    """
    Classify opportunity size and produce a rank boost.

    size:
      SMALL  — near level / mild structure (pullback class)
      NORMAL — trend aligned, moderate vol
      BIG    — vol expansion + range break / strong trend extension setup
      NONE   — insufficient data
    """
    coin = (coin or "").upper().split("/")[0].strip()
    result = {
        "coin": coin,
        "size": "NONE",
        "score_boost": 0.0,
        "reasons": [],
        "vol_ratio": None,
        "trend": "neutral",
        "funding": "unavailable",
        "fg": None,
        "priority_major": coin in PRIORITY_COINS,
        "core_major": coin in CORE_MAJORS,
    }

    if not coin:
        return result

    if price is None or price <= 0:
        try:
            price, _ = get_best_price(coin)
        except Exception:
            price = None
    if not price or price <= 0:
        result["reasons"].append("no_price")
        return result

    if not candles_ready(coin, min_candles=40):
        result["reasons"].append("candles_not_ready")
        # Still boost majors slightly so they aren't starved when data is catching up
        if coin in CORE_MAJORS:
            result["score_boost"] = 8.0
            result["size"] = "SMALL"
        return result

    candles = get_candles(coin) or []
    if len(candles) < 40:
        result["reasons"].append("few_candles")
        return result

    closes: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    for c in candles:
        try:
            closes.append(float(c.get("close") or 0))
            highs.append(float(c.get("high") or 0))
            lows.append(float(c.get("low") or 0))
        except Exception:
            continue
    closes = [x for x in closes if x > 0]
    if len(closes) < 40:
        return result

    atrs = _atr_series(candles, 14)
    atr = atrs[-1] if atrs else None
    atr_med = None
    if atrs and len(atrs) >= 20:
        window = sorted(atrs[-30:])
        atr_med = window[len(window) // 2]
    vol_ratio = (atr / atr_med) if (atr and atr_med and atr_med > 0) else 1.0
    result["vol_ratio"] = round(vol_ratio, 3)

    e20 = _ema_last(closes, 20)
    e50 = _ema_last(closes, 50)
    trend = "neutral"
    if e20 and e50:
        if e20 > e50 and price >= e20 * 0.99:
            trend = "bullish"
        elif e20 < e50 and price <= e20 * 1.01:
            trend = "bearish"
    result["trend"] = trend

    lookback = min(48, len(highs) - 1)
    recent_high = max(highs[-lookback:]) if lookback > 5 else max(highs)
    recent_low = min(lows[-lookback:]) if lookback > 5 else min(lows)
    near_high = recent_high > 0 and price >= recent_high * 0.992
    near_low = recent_low > 0 and price <= recent_low * 1.008
    broke_high = recent_high > 0 and price >= recent_high * 0.998 and closes[-1] >= recent_high * 0.995
    broke_low = recent_low > 0 and price <= recent_low * 1.002 and closes[-1] <= recent_low * 1.005

    # Layer 3: multi-day structure from 1H history (~72 bars ≈ 3 days)
    htf_n = min(72, max(len(highs) - 1, 1))
    htf_high = max(highs[-htf_n:]) if htf_n > 10 else recent_high
    htf_low = min(x for x in lows[-htf_n:] if x > 0) if htf_n > 10 else recent_low
    htf_break_up = htf_high > 0 and price >= htf_high * 0.997 and closes[-1] >= htf_high * 0.994
    htf_break_dn = htf_low > 0 and price <= htf_low * 1.003 and closes[-1] <= htf_low * 1.006
    result["htf_high"] = htf_high
    result["htf_low"] = htf_low

    # Historical range context: is today's range large vs median bar range?
    bar_ranges = [h - l for h, l in zip(highs[-40:], lows[-40:]) if h > l > 0]
    med_range = sorted(bar_ranges)[len(bar_ranges) // 2] if bar_ranges else 0
    last_range = (highs[-1] - lows[-1]) if highs and lows else 0
    range_expansion = bool(med_range and last_range >= med_range * 1.6)

    reasons: list[str] = []
    size = "SMALL"
    boost = 0.0

    if trend == "bullish":
        reasons.append("ema_trend_bull")
        boost += 6.0
    elif trend == "bearish":
        reasons.append("ema_trend_bear")
        boost += 6.0
    else:
        reasons.append("ema_neutral")

    if vol_ratio >= 1.35 or range_expansion:
        reasons.append(f"vol_expand:{vol_ratio:.2f}")
        boost += 12.0
        size = "NORMAL"
    if vol_ratio >= 1.7 or (range_expansion and vol_ratio >= 1.4):
        reasons.append("strong_vol_expansion")
        boost += 10.0
        size = "BIG"

    if broke_high and trend != "bearish":
        reasons.append("near_range_high_break")
        boost += 14.0
        size = "BIG" if size == "BIG" or vol_ratio >= 1.25 else "NORMAL"
    elif broke_low and trend != "bullish":
        reasons.append("near_range_low_break")
        boost += 14.0
        size = "BIG" if size == "BIG" or vol_ratio >= 1.25 else "NORMAL"
    elif near_high or near_low:
        reasons.append("testing_range_extreme")
        boost += 5.0
        if size == "SMALL":
            size = "NORMAL"

    # Layer 3: multi-day break → prefer BIG on majors
    if htf_break_up and trend != "bearish":
        reasons.append("multi_day_high_break")
        boost += 16.0
        size = "BIG"
    elif htf_break_dn and trend != "bullish":
        reasons.append("multi_day_low_break")
        boost += 16.0
        size = "BIG"

    # Fear & Greed — filter extremes, don't invent direction alone
    fg_val = None
    try:
        if get_fear_greed:
            fg = get_fear_greed() or {}
            fg_val = fg.get("value")
            if fg_val is not None:
                fg_val = int(fg_val)
                result["fg"] = fg_val
                if fg_val >= 75 and trend == "bullish" and size == "BIG":
                    reasons.append("greed_high_no_chase_penalty")
                    boost -= 8.0  # reduce chase into extreme greed blow-off
                elif fg_val <= 25 and trend == "bearish" and size == "BIG":
                    reasons.append("fear_extreme_short_penalty")
                    boost -= 6.0
                elif 30 <= fg_val <= 70:
                    boost += 3.0
    except Exception:
        pass

    # Layer 2: funding + OI
    der = _funding_and_oi(coin)
    fund_label = der.get("funding_label") or "unavailable"
    result["funding"] = fund_label
    if fund_label == "longs_crowded" and trend == "bullish":
        reasons.append("funding_longs_crowded")
        boost -= 5.0
    elif fund_label == "shorts_crowded" and trend == "bearish":
        reasons.append("funding_shorts_crowded")
        boost -= 5.0
    elif fund_label == "neutral" and der.get("funding_rate") is not None:
        boost += 2.0
    if der.get("oi_value") and der["oi_value"] > 0:
        reasons.append("oi_present")
        boost += 2.0

    # Layer 2: liquidation pressure (whale / forced flow proxy)
    liq = _liquidation_pressure(coin)
    result["liquidations"] = liq.get("label")
    if liq.get("label") == "heavy":
        reasons.append(f"liq_heavy:{liq.get('count')}")
        if liq.get("notional"):
            reasons.append(f"liq_notional:{int(liq['notional'])}")
        boost += 10.0
        if liq.get("notional", 0) >= 2_000_000:
            boost += 6.0
        if size != "BIG":
            size = "BIG" if vol_ratio and vol_ratio >= 1.2 else "NORMAL"
    elif liq.get("label") == "elevated":
        reasons.append(f"liq_elevated:{liq.get('count')}")
        boost += 5.0
        if size == "SMALL":
            size = "NORMAL"

    # Layer 2: 15m micro structure (small / timing)
    micro = _micro_15m_signal(coin, float(price))
    if micro.get("ready"):
        boost += float(micro.get("boost") or 0.0)
        reasons.extend(list(micro.get("reasons") or []))
        if micro.get("micro_trend") == trend and trend != "neutral":
            reasons.append("1h_15m_trend_align")
            boost += 6.0
            if size == "SMALL":
                size = "NORMAL"
        elif (
            micro.get("micro_trend") in ("bullish", "bearish")
            and trend != "neutral"
            and micro.get("micro_trend") != trend
        ):
            reasons.append("1h_15m_trend_conflict")
            boost -= 4.0

    # Layer 2: news caution (never invents direction; soft penalty)
    if news_market_flag:
        try:
            nf = news_market_flag(coin) or {}
            flag = (nf.get("flag") or "clear").lower()
            result["news_flag"] = flag
            if flag == "blackout":
                reasons.append("news_blackout")
                boost -= 25.0  # strong suppress — setup_engine may also block
            elif flag == "caution":
                reasons.append("news_caution")
                boost -= 6.0
        except Exception:
            pass

    # Priority: BTC/ETH must surface when any real opportunity exists
    if coin in CORE_MAJORS:
        boost += 18.0
        reasons.append("core_major_priority")
    elif coin in PRIORITY_COINS:
        boost += 10.0
        reasons.append("priority_coin")

    # Floor size for majors with clear trend
    if coin in CORE_MAJORS and trend in ("bullish", "bearish") and size == "SMALL":
        size = "NORMAL"
        boost += 4.0

    result["size"] = size
    result["score_boost"] = round(max(boost, 0.0), 2)
    result["reasons"] = reasons

    logger.info(
        "[OPPORTUNITY] %s size=%s boost=+%.1f trend=%s vol=%.2f liq=%s news=%s %s",
        coin, size, result["score_boost"], trend, vol_ratio or 0.0,
        result.get("liquidations") or "n/a",
        result.get("news_flag") or "n/a",
        ",".join(reasons[:6]),
    )
    return result


def score_boost_for_symbol(symbol: str) -> float:
    """Convenience for scanner rank."""
    coin = (symbol or "").upper().split("/")[0].strip()
    if not coin:
        return 0.0
    try:
        return float(assess_opportunity(coin).get("score_boost") or 0.0)
    except Exception as e:
        logger.debug("[OPPORTUNITY] score_boost %s: %s", coin, e)
        return 0.0


def format_opportunity_footer(opp: dict | None) -> str:
    """User-facing facts only — no causal stories."""
    if not opp or (opp.get("size") or "NONE") == "NONE":
        return ""
    size = str(opp.get("size") or "SMALL").upper()
    trend = str(opp.get("trend") or "neutral")
    vol = opp.get("vol_ratio")
    fg = opp.get("fg")
    fund = opp.get("funding") or "n/a"
    liq = opp.get("liquidations") or "none"
    news = opp.get("news_flag") or "clear"
    lines = [
        "",
        "· · · · · · · · · · · · · · · · · ·",
        f"📡 OPPORTUNITY  {size}",
        f"Trend: {trend} · Vol vs avg: {vol if vol is not None else 'n/a'}",
        f"Funding: {fund} · Liqs(15m): {liq} · News: {news}",
    ]
    if fg is not None:
        lines.append(f"Fear & Greed: {fg}")
    # Compact factual tags (max 4)
    tags = []
    for r in list(opp.get("reasons") or [])[:8]:
        if r.startswith("multi_day"):
            tags.append("multi-day range break")
        elif r.startswith("near_range"):
            tags.append("range extreme")
        elif r.startswith("vol_expand") or "vol_expansion" in r:
            tags.append("volatility expansion")
        elif r.startswith("15m_"):
            tags.append("15m structure")
        elif r.startswith("liq_"):
            tags.append("liquidation activity")
        elif r == "1h_15m_trend_align":
            tags.append("1H+15m aligned")
        elif r == "core_major_priority":
            tags.append("major pair priority")
    if tags:
        # dedupe preserve order
        seen = set()
        clean = []
        for t in tags:
            if t not in seen:
                seen.add(t)
                clean.append(t)
        lines.append("Signals: " + ", ".join(clean[:4]))
    lines.append("Levels above are from the setup engine — not from sentiment alone.")
    return "\n".join(lines)


def persist_opportunity_snapshot(idea_id: int, opp: dict) -> None:
    """Store opportunity JSON on trade_ideas.outcome_detail (merge if present)."""
    if not idea_id or not opp:
        return
    try:
        import json
        from market_pulse.db import get_db

        db = get_db()
        c = db.cursor()
        c.execute("SELECT outcome_detail FROM trade_ideas WHERE id=%s", (idea_id,))
        row = c.fetchone()
        base = {}
        if row and row[0]:
            try:
                base = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0] or {})
            except Exception:
                base = {}
        if not isinstance(base, dict):
            base = {}
        base["opportunity"] = {
            "size": opp.get("size"),
            "score_boost": opp.get("score_boost"),
            "trend": opp.get("trend"),
            "vol_ratio": opp.get("vol_ratio"),
            "funding": opp.get("funding"),
            "liquidations": opp.get("liquidations"),
            "news_flag": opp.get("news_flag"),
            "fg": opp.get("fg"),
            "reasons": list(opp.get("reasons") or [])[:12],
        }
        c.execute(
            "UPDATE trade_ideas SET outcome_detail=%s WHERE id=%s",
            (json.dumps(base), idea_id),
        )
        db.commit()
        db.close()
    except Exception as e:
        logger.debug("[OPPORTUNITY] persist #%s: %s", idea_id, e)
