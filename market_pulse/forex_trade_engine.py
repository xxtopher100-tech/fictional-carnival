"""Market Pulse Bot — Forex trade engine.

Upgrades:
- Cached rates + explicit source + WAT timestamp
- EUR/NGN, GBP/NGN pairs (via P2P) alongside existing pairs
- Programmatic R:R + validation + realistic stop caps
- News blackout gate
- Management plan text (BE / trail guidance — no exchange orders)
- AI explains only when programmatic levels exist; otherwise validated AI levels
"""

from __future__ import annotations

import re
import threading
import time

from market_pulse.ai_engine import ask_ai
from market_pulse.config_runtime import logger
from market_pulse.db import get_db
from market_pulse.edge_trade_engine import EDGE_DISCLAIMER, STANDARD_DISCLAIMER, TRADE_TIERS, mark_trade_publication
from market_pulse.fear_greed import get_fear_greed
from market_pulse.helpers import format_forex, format_ngn, wat_now
from market_pulse.p2p import get_p2p_rate
from market_pulse.price_fetchers import get_best_price, get_fiat_rates

try:
    from market_pulse.setup_engine import news_market_flag
except Exception:
    def news_market_flag(coin=None):
        return {"flag": "clear", "headlines": []}


FOREX_PAIRS = {
    "USDT/NGN": {
        "description": "Tether vs Nigerian Naira (P2P)",
        "base": "USDT", "quote": "NGN", "symbol": "₦", "source": "p2p",
        "pip_size": 1.0, "typical_spread": 30, "asset": "USDT",
    },
    "EUR/NGN": {
        "description": "Euro vs Nigerian Naira (P2P)",
        "base": "EUR", "quote": "NGN", "symbol": "₦", "source": "p2p",
        "pip_size": 1.0, "typical_spread": 50, "asset": "EUR",
    },
    "GBP/NGN": {
        "description": "British Pound vs Nigerian Naira (P2P)",
        "base": "GBP", "quote": "NGN", "symbol": "₦", "source": "p2p",
        "pip_size": 1.0, "typical_spread": 50, "asset": "GBP",
    },
    "USD/NGN": {
        "description": "US Dollar vs Nigerian Naira",
        "base": "USD", "quote": "NGN", "symbol": "₦", "source": "fiat",
        "pip_size": 1.0, "typical_spread": 50, "asset": None,
    },
    "BTC/NGN": {
        "description": "Bitcoin vs Nigerian Naira",
        "base": "BTC", "quote": "NGN", "symbol": "₦", "source": "derived",
        "pip_size": 1000, "typical_spread": 5000, "asset": None,
    },
    "EUR/USD": {
        "description": "Euro vs US Dollar",
        "base": "EUR", "quote": "USD", "symbol": "$", "source": "fiat",
        "pip_size": 0.0001, "typical_spread": 0.0002, "asset": None,
    },
    "GBP/USD": {
        "description": "British Pound vs US Dollar",
        "base": "GBP", "quote": "USD", "symbol": "$", "source": "fiat",
        "pip_size": 0.0001, "typical_spread": 0.0002, "asset": None,
    },
}

# Liquid pairs preferred for morning package / scanner
MORNING_FOREX_PAIRS = ["USDT/NGN", "EUR/NGN", "GBP/NGN", "EUR/USD", "GBP/USD"]  # rates/context OK

# Pairs that must never become SAFE/NORMAL/EDGE trade setups
NON_TRADEABLE_FOREX_PAIRS = frozenset({"USDT/NGN"})


_rate_cache = {}
_rate_lock = threading.Lock()
_RATE_TTL = 180  # 3 minutes


def get_forex_rate(pair_key, use_cache=True):
    """Returns (rate, bid, ask, source_str) or (None, None, None, None)."""
    pair = FOREX_PAIRS.get(pair_key)
    if not pair:
        return None, None, None, None

    now = time.time()
    if use_cache:
        with _rate_lock:
            hit = _rate_cache.get(pair_key)
            if hit and now - hit["ts"] < _RATE_TTL:
                return hit["rate"], hit["bid"], hit["ask"], hit["source"]

    rate = bid = ask = None
    source = None
    try:
        if pair["source"] == "p2p":
            asset = pair.get("asset") or pair["base"]
            buy, sell, p2p_src = get_p2p_rate(asset, "NGN")
            if buy and sell:
                mid = (buy + sell) / 2
                rate, bid, ask = mid, sell, buy
                source = f"P2P ({p2p_src}) · {wat_now().strftime('%H:%M')} WAT"
            else:
                return None, None, None, None

        elif pair["source"] == "fiat":
            rates = get_fiat_rates() or {}
            if pair_key == "USD/NGN":
                ngn = rates.get("NGN")
                if ngn:
                    spread = pair["typical_spread"]
                    rate = float(ngn)
                    bid, ask = rate - spread / 2, rate + spread / 2
                    source = f"ExchangeRate · {wat_now().strftime('%H:%M')} WAT"
            elif pair_key == "EUR/USD":
                eur = rates.get("EUR")
                if eur and float(eur) > 0:
                    rate = 1 / float(eur)
                    spread = pair["typical_spread"]
                    bid, ask = rate - spread, rate + spread
                    source = f"Frankfurter · {wat_now().strftime('%H:%M')} WAT"
            elif pair_key == "GBP/USD":
                gbp = rates.get("GBP")
                if gbp and float(gbp) > 0:
                    rate = 1 / float(gbp)
                    spread = pair["typical_spread"]
                    bid, ask = rate - spread, rate + spread
                    source = f"Frankfurter · {wat_now().strftime('%H:%M')} WAT"

        elif pair["source"] == "derived":
            btc_usd, _ = get_best_price("BTC")
            rates = get_fiat_rates() or {}
            ngn_rate = rates.get("NGN")
            if btc_usd and ngn_rate:
                rate = float(btc_usd) * float(ngn_rate)
                spread = rate * 0.005
                bid, ask = rate - spread, rate + spread
                source = f"Derived (BTC×NGN) · {wat_now().strftime('%H:%M')} WAT"

    except Exception as e:
        logger.warning("[FOREX RATE] %s: %s", pair_key, e)
        return None, None, None, None

    if rate is None:
        return None, None, None, None

    with _rate_lock:
        _rate_cache[pair_key] = {
            "rate": rate, "bid": bid, "ask": ask, "source": source, "ts": now
        }
    return rate, bid, ask, source


def _programmatic_forex_levels(pair_key, rate, tier):
    """Simple ATR-style levels from typical_spread + tier stop caps (no candle history required)."""
    pair = FOREX_PAIRS[pair_key]
    tier_cfg = TRADE_TIERS.get(tier, TRADE_TIERS["momentum"])
    max_stop_pct = float(tier_cfg.get("max_stop_pct", 10)) / 100.0
    min_rr = float(tier_cfg.get("min_rr", 1.5))

    # Use typical spread * multiplier as volatility proxy
    vol = max(float(pair.get("typical_spread") or 0), rate * 0.002)
    stop_dist = min(vol * 3.0, rate * max_stop_pct * 0.9)
    if stop_dist <= 0:
        return None

    # Direction: for NGN pairs, mild bias from P2P mid vs USD/NGN if available
    direction = "Buy"
    is_buy = True
    try:
        if "NGN" in pair_key and pair_key != "USD/NGN":
            usd_ngn, _, _, _ = get_forex_rate("USD/NGN")
            if usd_ngn and rate > usd_ngn * 1.01:
                direction, is_buy = f"Sell {pair['base']}", False
            else:
                direction, is_buy = f"Buy {pair['base']}", True
        else:
            direction, is_buy = f"Buy {pair['base']}", True
    except Exception:
        direction, is_buy = f"Buy {pair['base']}", True

    entry = float(rate)
    if is_buy:
        stop = entry - stop_dist
        t1 = entry + stop_dist * min_rr
        t2 = entry + stop_dist * (min_rr + 1.0)
    else:
        stop = entry + stop_dist
        t1 = entry - stop_dist * min_rr
        t2 = entry - stop_dist * (min_rr + 1.0)

    if stop <= 0 or t1 <= 0:
        return None

    return {
        "timeframe": "4H",
        "direction": direction,
        "entry": entry,
        "stop": stop,
        "target1": t1,
        "target2": t2,
        "invalidation": stop,
        "bias": f"Bullish {pair['base']}" if is_buy else f"Bearish {pair['base']}",
        "confidence": "Moderate",
        "rationale": (
            f"Programmatic {tier} levels from rate ± volatility proxy "
            f"(spread-based). R:R ≥ {min_rr}:1 by construction."
        ),
        "ng_angle": "Size small; confirm P2P/liquidity before converting naira.",
        "management": (
            f"MANAGEMENT (manual — bot does not place orders): "
            f"(1) At +1R move stop to break-even (entry). "
            f"(2) Optional trail ~0.4% once BE is on. "
            f"(3) TP1 is the high-probability exit."
        ),
        "source": "forex_programmatic",
    }


def _build_forex_ai_prompt(pair_key, rate, bid, ask, tier, fg_val, source):
    pair = FOREX_PAIRS[pair_key]
    tier_cfg = TRADE_TIERS[tier]
    symbol = pair["symbol"]
    tf_guide = {
        "steady": "Daily or Weekly. Prefer range boundaries.",
        "momentum": "4H or Daily. Trend continuation or breakout.",
        "edge": "1H or 4H. High-conviction only.",
    }
    ngn_context = ""
    if "NGN" in pair_key:
        ngn_context = (
            "\nNIGERIAN CONTEXT: Consider naira pressure, parallel market, and P2P liquidity."
        )
    return (
        f"You are a forex analyst for Nigerian traders on Market Pulse Pro.\n\n"
        f"PAIR: {pair_key} — {pair['description']}\n"
        f"CURRENT RATE: {symbol}{rate:,.6f}\n"
        f"BID: {symbol}{bid:,.6f} | ASK: {symbol}{ask:,.6f}\n"
        f"DATA SOURCE: {source}\n"
        f"FEAR & GREED: {fg_val}/100{ngn_context}\n\n"
        f"TIER: {tier_cfg['label']} — {tier_cfg['risk_desc']}\n"
        f"TIMEFRAME: {tf_guide[tier]}\n"
        f"STOP MAX: {tier_cfg['max_stop_pct']}% | MIN R:R: {tier_cfg['min_rr']}:1\n"
        f"Do NOT state R:R ratios — levels only.\n"
        f"Entry/Stop/Target in {pair['quote']} terms.\n\n"
        f"Respond ONLY:\n"
        f"TIMEFRAME: [1H / 4H / Daily / Weekly]\n"
        f"DIRECTION: [Buy {pair['base']} / Sell {pair['base']}]\n"
        f"RATIONALE: [2 sentences]\n"
        f"NIGERIAN ANGLE: [1 sentence]\n"
        f"Market Bias: [Bullish {pair['base']} / Bearish {pair['base']} / Neutral]\n"
        f"Entry: {symbol}[rate]\n"
        f"Stop Loss: {symbol}[rate]\n"
        f"Target 1: {symbol}[rate]\n"
        f"Target 2: {symbol}[rate or none]\n"
        f"Invalidation: {symbol}[rate]\n"
        f"Confidence: [High / Moderate / Low]\n"
        f"If no quality setup: Entry: none"
    )


def _parse_forex_trade(ai_text, rate, symbol):
    if not ai_text:
        return None
    try:
        def _get(pattern, text):
            m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            return m.group(1).strip() if m else None

        def _pf(patterns, text):
            if isinstance(patterns, str):
                patterns = [patterns]
            for pattern in patterns:
                raw = _get(pattern, text)
                if not raw or raw.lower() in ("none", "n/a", "-"):
                    continue
                cleaned = re.sub(r"[₦$£€,]", "", raw).strip().rstrip(".")
                try:
                    v = float(cleaned)
                    if v > 0:
                        return v
                except Exception:
                    continue
            return None

        entry = _pf([r"Entry[:\s]+[₦$£€]?([0-9,\.]+)"], ai_text)
        if not entry:
            return None
        stop = _pf([r"Stop\s*Loss[:\s]+[₦$£€]?([0-9,\.]+)", r"Stop[:\s]+[₦$£€]?([0-9,\.]+)"], ai_text)
        t1 = _pf([r"Target\s*1[:\s]+[₦$£€]?([0-9,\.]+)", r"TP\s*1[:\s]+[₦$£€]?([0-9,\.]+)"], ai_text)
        t2 = _pf([r"Target\s*2[:\s]+[₦$£€]?([0-9,\.]+)", r"TP\s*2[:\s]+[₦$£€]?([0-9,\.]+)"], ai_text)
        inv = _pf([r"Invalidation[:\s]+[₦$£€]?([0-9,\.]+)"], ai_text)

        # Reject levels wildly far from market (>15%)
        def _near(v):
            return v is not None and abs(v - rate) / rate <= 0.15

        if not _near(entry):
            return None
        if stop is not None and not _near(stop):
            stop = None
        if t1 is not None and not _near(t1):
            t1 = None

        return {
            "timeframe": _get(r"TIMEFRAME[:\s]+(\S+)", ai_text) or "4H",
            "direction": _get(r"DIRECTION[:\s]*(.+?)(?=\n|$)", ai_text) or "Buy",
            "rationale": _get(r"RATIONALE[:\s]*(.+?)(?=\nNIGERIAN|\nMarket|\nEntry:|\Z)", ai_text),
            "ng_angle": _get(r"NIGERIAN ANGLE[:\s]*(.+?)(?=\nMarket|\nEntry:|\Z)", ai_text),
            "bias": _get(r"Market Bias[:\s]*(.+?)(?=\n|$)", ai_text) or "Neutral",
            "entry": entry,
            "stop": stop,
            "target1": t1,
            "target2": t2,
            "invalidation": inv,
            "confidence": _get(r"Confidence[:\s]+(\w+)", ai_text) or "Moderate",
        }
    except Exception as e:
        logger.warning("[FOREX PARSE] %s", e)
        return None


def _validate_forex_trade(pair_key, rate, trade, tier="momentum"):
    entry = trade.get("entry")
    stop = trade.get("stop")
    target = trade.get("target1")
    direction = (trade.get("direction") or "Buy").lower()

    if not entry or not stop or not target:
        return False, "Missing entry, stop, or target"
    if entry <= 0 or stop <= 0 or target <= 0:
        return False, "Non-positive levels"

    is_buy = "buy" in direction or "long" in direction
    if is_buy:
        if stop >= entry:
            return False, f"Buy stop {stop} >= entry {entry}"
        if target <= entry:
            return False, f"Buy target {target} <= entry {entry}"
        risk = entry - stop
        reward = target - entry
    else:
        if stop <= entry:
            return False, f"Sell stop {stop} <= entry {entry}"
        if target >= entry:
            return False, f"Sell target {target} >= entry {entry}"
        risk = stop - entry
        reward = entry - target

    if risk <= 0:
        return False, "Zero risk"
    rr = reward / risk
    min_rr = float(TRADE_TIERS.get(tier, {}).get("min_rr", 1.5))
    if rr < min_rr * 0.9:
        return False, f"R:R {rr:.2f} below minimum {min_rr}"

    max_stop_pct = float(TRADE_TIERS.get(tier, {}).get("max_stop_pct", 15))
    if (risk / entry) * 100 > max_stop_pct:
        return False, f"Stop too wide ({(risk/entry)*100:.1f}% > {max_stop_pct}%)"

    return True, "OK"


def build_forex_trade_message(pair_key, rate, tier, trade, idea_id=0, source_str=""):
    pair = FOREX_PAIRS[pair_key]
    tier_cfg = TRADE_TIERS[tier]
    symbol = pair["symbol"]
    entry = trade.get("entry")
    stop = trade.get("stop")
    t1 = trade.get("target1")
    t2 = trade.get("target2")
    inv = trade.get("invalidation")
    conf = trade.get("confidence", "Moderate")
    direction = trade.get("direction", f"Buy {pair['base']}")

    rr_str = "N/A"
    stop_pct = t1_pct = None
    if entry and stop and t1:
        is_buy = "buy" in direction.lower() or "long" in direction.lower()
        try:
            risk = (entry - stop) if is_buy else (stop - entry)
            reward = (t1 - entry) if is_buy else (entry - t1)
            if risk > 0:
                rr_str = f"1 : {reward/risk:.2f}"
                stop_pct = abs(risk / entry * 100)
                t1_pct = abs(reward / entry * 100)
        except Exception:
            pass

    def fmt(v):
        if v is None:
            return "—"
        if symbol == "₦":
            return format_ngn(v)
        return format_forex(v, symbol)

    lines = [
        f"{tier_cfg['emoji']} <b>{tier_cfg['label']} #{idea_id} — FOREX</b>",
        f"<b>{pair_key}</b>  ·  {direction.upper()}  ·  {trade.get('timeframe','4H')}",
        f"<i>{pair['description']} — {tier_cfg['risk_desc']}</i>",
        "",
        f"💱 Current Rate: <b>{fmt(rate)}</b>",
    ]
    if source_str:
        lines.append(f"📡 Source: <i>{source_str}</i>")
    lines += [
        f"📈 Bias: <b>{trade.get('bias','Neutral')}</b>",
        "",
    ]
    if trade.get("rationale"):
        lines += ["📋 <b>SETUP</b>", trade["rationale"], ""]
    if trade.get("ng_angle"):
        lines += ["🇳🇬 <b>NIGERIAN ANGLE</b>", trade["ng_angle"], ""]

    lines += [
        "· · · · · · · · · · · · · · · · · · ·", "",
        "📐 <b>LEVELS</b>",
        f"Entry:        <b>{fmt(entry)}</b>",
        f"Stop Loss:    <b>{fmt(stop)}</b>",
        f"Target 1:     <b>{fmt(t1)}</b>",
    ]
    if t2:
        lines.append(f"Target 2:     <b>{fmt(t2)}</b>  <i>(aggressive)</i>")
    lines += [f"Invalidation: <b>{fmt(inv)}</b>", ""]

    if rr_str != "N/A" and stop_pct is not None:
        lines += [
            "📊 <b>RISK METRICS</b>",
            f"Risk:Reward:  <b>{rr_str}</b>  <i>(calculated)</i>",
            f"Stop Risk:    <b>-{stop_pct:.2f}%</b>",
        ]
        if t1_pct is not None:
            lines.append(f"T1 Reward:    <b>+{t1_pct:.2f}%</b>")
        lines += [
            f"Confidence:   <b>{conf}</b>",
            f"Max Size:     <b>{tier_cfg['max_size']}</b>",
            "",
        ]

    mgmt = trade.get("management") or (
        "MANAGEMENT (manual — bot does not place orders): "
        "At +1R move stop to break-even. Optional small trail after BE. TP1 is primary exit."
    )
    lines += ["🛡️ <b>TRADE MANAGEMENT</b>", mgmt, ""]
    lines += ["· · · · · · · · · · · · · · · · · · ·", ""]
    lines.append(EDGE_DISCLAIMER if tier == "edge" else STANDARD_DISCLAIMER)
    return "\n".join(lines)


def generate_forex_trade_idea(pair_key, tier="momentum"):
    """Generate a forex trade idea. USDT/NGN is context-only — never a trade setup."""
    if pair_key in NON_TRADEABLE_FOREX_PAIRS:
        logger.info("[FOREX] %s is not tradeable (local context only) — skip setup", pair_key)
        return None, None, None
    """Fetch rate → news gate → programmatic levels preferred → validate → save."""
    try:
        if pair_key not in FOREX_PAIRS:
            return None, None, 0

        nf = news_market_flag()
        if nf.get("flag") == "blackout" and tier in ("momentum", "edge"):
            logger.info("[FOREX ENGINE] %s %s blocked — news blackout", pair_key, tier)
            return None, None, 0

        rate, bid, ask, source = get_forex_rate(pair_key)
        if not rate:
            logger.info("[FOREX ENGINE] No rate for %s", pair_key)
            return None, None, 0

        fg_data = get_fear_greed()
        fg_val = fg_data[0]["value"] if fg_data else "50"

        # Prefer programmatic levels
        trade = _programmatic_forex_levels(pair_key, rate, tier)
        ai_raw = ""

        # Optional AI narrative only
        if trade:
            try:
                prompt = (
                    f"{pair_key} {tier} levels already set by rules.\n"
                    f"Entry {trade['entry']} Stop {trade['stop']} TP1 {trade['target1']}\n"
                    f"Direction {trade['direction']}. Source {source}.\n"
                    f"Write 2 short sentences for Nigerian traders. Do NOT change numbers."
                )
                ai_raw, _ = ask_ai(prompt)
                if ai_raw:
                    trade["rationale"] = (ai_raw.strip()[:400] + "\n\n" + trade.get("rationale", "")).strip()
            except Exception:
                pass
        else:
            # Fallback AI levels with validation
            prompt = _build_forex_ai_prompt(
                pair_key, rate, bid or rate, ask or rate, tier, fg_val, source or ""
            )
            ai_raw, _ = ask_ai(prompt)
            trade = _parse_forex_trade(ai_raw, rate, FOREX_PAIRS[pair_key]["symbol"])
            if not trade or not trade.get("entry"):
                return None, None, 0
            trade["management"] = (
                "MANAGEMENT (manual — bot does not place orders): "
                "At +1R move stop to break-even. Optional trail after BE."
            )

        valid, reason = _validate_forex_trade(pair_key, rate, trade, tier=tier)
        if not valid:
            logger.warning("[FOREX ENGINE] %s %s validation failed: %s", pair_key, tier, reason)
            return None, None, 0

        idea_id = 0
        db = None
        try:
            db = get_db()
            c = db.cursor()
            now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
            entry = float(trade["entry"])
            stop = float(trade["stop"])
            t1 = float(trade["target1"])
            is_buy = "buy" in str(trade.get("direction", "")).lower() or "long" in str(trade.get("direction", "")).lower()
            risk = (entry - stop) if is_buy else (stop - entry)
            reward = (t1 - entry) if is_buy else (entry - t1)
            rr = (reward / risk) if risk else 0
            c.execute(
                """INSERT INTO trade_ideas
                   (coin, tier, direction, timeframe, entry, stop, target1, target2,
                    bias, confidence, rr, invalidation, max_size_pct, ai_rationale, status, created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open',%s) RETURNING id""",
                (
                    pair_key, tier,
                    trade.get("direction", "Buy"),
                    trade.get("timeframe", "4H"),
                    str(round(entry, 6)),
                    str(round(stop, 6)),
                    str(round(t1, 6)),
                    str(round(float(trade["target2"]), 6)) if trade.get("target2") else None,
                    trade.get("bias", "Neutral"),
                    trade.get("confidence", "Moderate"),
                    f"1:{rr:.2f}",
                    str(round(float(trade["invalidation"]), 6)) if trade.get("invalidation") else None,
                    TRADE_TIERS[tier]["max_size"],
                    (ai_raw or trade.get("rationale") or "")[:500],
                    now,
                ),
            )
            idea_id = c.fetchone()[0]
            db.commit()
            logger.info("[FOREX ENGINE] #%s saved — %s %s", idea_id, pair_key, tier)
        except Exception as e:
            logger.error("[FOREX ENGINE] Save error: %s", e)
        finally:
            if db:
                try:
                    db.close()
                except Exception:
                    pass

        msg = build_forex_trade_message(pair_key, rate, tier, trade, idea_id, source_str=source or "")
        msg += (
            "\n\n<i>📐 Forex levels validated in code. "
            "Management is guidance only — bot does not place exchange orders.</i>"
        )
        return msg, trade, idea_id

    except Exception as e:
        logger.error("[FOREX ENGINE] %s %s: %s", pair_key, tier, e)
        return None, None, 0
