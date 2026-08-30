"""Market Pulse Bot — forex_trade_engine module (split from the real monolithic bot.py)."""

import os
import ssl
import socket
import base64
import struct
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse
import json
import time
import requests
import xml.etree.ElementTree as ET
import re
import random
from datetime import datetime, timedelta
from collections import defaultdict
import logging
import threading
from logging.handlers import RotatingFileHandler

from market_pulse.ai_engine import ask_ai
from market_pulse.config_runtime import logger
from market_pulse.db import get_db
from market_pulse.edge_trade_engine import EDGE_DISCLAIMER, STANDARD_DISCLAIMER, TRADE_TIERS
from market_pulse.fear_greed import get_fear_greed
from market_pulse.helpers import format_forex, format_ngn, wat_now
from market_pulse.p2p import get_p2p_rate
from market_pulse.price_fetchers import get_best_price, get_fiat_rates


# ─── extracted section ───
# 💱 FOREX TRADE ENGINE
# ═══════════════════════════════════════════════════════════════════════════
# Generates trade setups for currency pairs — same 3 tiers as crypto.
# Data sources: get_p2p_rate() for NGN pairs, get_fiat_rates() for major forex.
# All ideas posted to Pro channel only.
# ═══════════════════════════════════════════════════════════════════════════

FOREX_PAIRS = {
    "USDT/NGN": {
        "description": "Tether vs Nigerian Naira (P2P market)",
        "base": "USDT", "quote": "NGN",
        "symbol": "₦", "source": "p2p",
        "pip_size": 1.0,        # 1 naira pip
        "typical_spread": 30,   # typical buy-sell spread in naira
    },
    "USD/NGN": {
        "description": "US Dollar vs Nigerian Naira",
        "base": "USD", "quote": "NGN",
        "symbol": "₦", "source": "fiat",
        "pip_size": 1.0,
        "typical_spread": 50,
    },
    "BTC/NGN": {
        "description": "Bitcoin vs Nigerian Naira",
        "base": "BTC", "quote": "NGN",
        "symbol": "₦", "source": "derived",
        "pip_size": 1000,
        "typical_spread": 5000,
    },
    "EUR/USD": {
        "description": "Euro vs US Dollar",
        "base": "EUR", "quote": "USD",
        "symbol": "$", "source": "fiat",
        "pip_size": 0.0001,
        "typical_spread": 0.0002,
    },
    "GBP/USD": {
        "description": "British Pound vs US Dollar",
        "base": "GBP", "quote": "USD",
        "symbol": "$", "source": "fiat",
        "pip_size": 0.0001,
        "typical_spread": 0.0002,
    },
}


def get_forex_rate(pair_key):
    """Get current rate for a forex pair.
    Returns (rate, bid, ask, source_str) or (None, None, None, None)."""
    pair = FOREX_PAIRS.get(pair_key)
    if not pair:
        return None, None, None, None

    try:
        if pair["source"] == "p2p":
            buy, sell, source = get_p2p_rate("USDT", "NGN")
            if buy and sell:
                mid = (buy + sell) / 2
                return mid, sell, buy, f"P2P ({source})"
            return None, None, None, None

        elif pair["source"] == "fiat":
            rates = get_fiat_rates()
            if pair_key == "USD/NGN":
                ngn = rates.get("NGN")
                if ngn:
                    spread = pair["typical_spread"]
                    return ngn, ngn - spread/2, ngn + spread/2, "ExchangeRate"
            elif pair_key == "EUR/USD":
                eur = rates.get("EUR")
                if eur:
                    # EUR/USD = 1/EUR rate (EUR rate is how many EUR per USD)
                    rate = 1 / eur if eur else None
                    if rate:
                        spread = pair["typical_spread"]
                        return rate, rate - spread, rate + spread, "Frankfurter"
            elif pair_key == "GBP/USD":
                gbp = rates.get("GBP")
                if gbp:
                    rate = 1 / gbp if gbp else None
                    if rate:
                        spread = pair["typical_spread"]
                        return rate, rate - spread, rate + spread, "Frankfurter"
            return None, None, None, None

        elif pair["source"] == "derived":
            # BTC/NGN = BTC/USD * USD/NGN
            btc_usd, _ = get_best_price("BTC")
            rates = get_fiat_rates()
            ngn_rate = rates.get("NGN")
            if btc_usd and ngn_rate:
                rate = btc_usd * ngn_rate
                spread = rate * 0.005  # 0.5% spread
                return rate, rate - spread, rate + spread, "Derived (BTC*NGN)"
            return None, None, None, None

    except Exception as e:
        logger.warning(f"[FOREX RATE] {pair_key}: {e}")
    return None, None, None, None


def _build_forex_ai_prompt(pair_key, rate, bid, ask, tier, fg_val, source):
    """Build AI prompt for a forex trade idea."""
    pair = FOREX_PAIRS[pair_key]
    tier_cfg = TRADE_TIERS[tier]
    symbol = pair["symbol"]
    tf_guide = {
        "steady":   "Daily or Weekly timeframe. Prefer established range boundaries.",
        "momentum": "4H or Daily timeframe. Trend continuation or breakout from range.",
        "edge":     "1H or 4H timeframe. High-conviction directional move only.",
    }
    ngn_context = ""
    if "NGN" in pair_key:
        ngn_context = (
            f"\nNIGERIAN CONTEXT: This is the most important pair for Nigerian traders. "
            f"Consider naira depreciation trends, CBN policy, parallel market dynamics, "
            f"and import demand pressures in your analysis."
        )

    return (
        f"You are a professional forex analyst generating a {tier_cfg['risk_desc']} trade idea "
        f"for Nigerian traders on Market Pulse Pro.\n\n"
        f"PAIR: {pair_key} — {pair['description']}\n"
        f"CURRENT RATE: {symbol}{rate:,.4f}\n"
        f"BID: {symbol}{bid:,.4f} | ASK: {symbol}{ask:,.4f}\n"
        f"DATA SOURCE: {source}\n"
        f"FEAR & GREED (crypto): {fg_val}/100 (sentiment context){ngn_context}\n\n"
        f"TIER: {tier_cfg['label']} — {tier_cfg['risk_desc']}\n"
        f"TIMEFRAME: {tf_guide[tier]}\n"
        f"STOP MAX: {tier_cfg['max_stop_pct']}% from entry\n"
        f"TARGET MIN: {tier_cfg['min_target_pct']}% from entry\n"
        f"MIN R:R: {tier_cfg['min_rr']}:1\n\n"
        f"IMPORTANT: Entry, Stop, Target must be in {pair['quote']} terms (e.g. {symbol}1,620 not $1,620).\n"
        f"If no quality {tier} setup exists right now, say so clearly.\n\n"
        f"Respond ONLY in this exact format. No asterisks. Plain text:\n"
        f"TIMEFRAME: [1H / 4H / Daily / Weekly]\n"
        f"DIRECTION: [Buy {pair['base']} / Sell {pair['base']}]\n"
        f"RATIONALE: [2 sentences — explain why this setup makes sense now]\n"
        f"NIGERIAN ANGLE: [1 sentence — what this means for naira holders or P2P traders]\n"
        f"Market Bias: [Bullish {pair['base']} / Bearish {pair['base']} / Neutral]\n"
        f"Entry: {symbol}[rate]\n"
        f"Stop Loss: {symbol}[rate]\n"
        f"Target 1: {symbol}[rate]\n"
        f"Target 2: {symbol}[rate or none]\n"
        f"Invalidation: {symbol}[rate]\n"
        f"Confidence: [High / Moderate / Low]\n"
        f"If no quality setup: TIMEFRAME: None\nDIRECTION: None\nEntry: none"
    )


def _parse_forex_trade(ai_text, rate, symbol):
    """Parse AI forex trade response. Returns dict or None."""
    if not ai_text:
        return None
    try:
        def _get(pattern, text):
            m = re.search(pattern, text, re.IGNORECASE)
            return m.group(1).strip() if m else None

        def _pf(pattern, text):
            raw = _get(pattern, text)
            if not raw or raw.lower() in ("none", "n/a", "-"):
                return None
            # Strip any currency symbols and commas
            cleaned = re.sub(r"[₦$£€,]", "", raw).strip()
            try:
                return float(cleaned)
            except Exception:
                return None

        entry = _pf(r"Entry[:\s]+[₦$£€]?([0-9,\.]+)", ai_text)
        if not entry:
            return None
        if str(entry).lower() == "none":
            return None

        return {
            "timeframe":    _get(r"TIMEFRAME[:\s]+(\S+)", ai_text) or "4H",
            "direction":    _get(r"DIRECTION[:\s]*(.+?)(?=\n|$)", ai_text) or "Buy",
            "rationale":    _get(r"RATIONALE[:\s]*(.+?)(?=\nNIGERIAN|\n[A-Z]|$)", ai_text),
            "ng_angle":     _get(r"NIGERIAN ANGLE[:\s]*(.+?)(?=\nMarket|\n[A-Z]|$)", ai_text),
            "bias":         _get(r"Market Bias[:\s]*(.+?)(?=\n|$)", ai_text) or "Neutral",
            "entry":        entry,
            "stop":         _pf(r"Stop Loss[:\s]+[₦$£€]?([0-9,\.]+)", ai_text),
            "target1":      _pf(r"Target 1[:\s]+[₦$£€]?([0-9,\.]+)", ai_text),
            "target2":      _pf(r"Target 2[:\s]+[₦$£€]?([0-9,\.]+)", ai_text),
            "invalidation": _pf(r"Invalidation[:\s]+[₦$£€]?([0-9,\.]+)", ai_text),
            "confidence":   _get(r"Confidence[:\s]+(\w+)", ai_text) or "Moderate",
        }
    except Exception as e:
        logger.warning(f"[FOREX PARSE] {e}")
        return None


def _validate_forex_trade(pair_key, rate, trade):
    """Validate forex trade levels. Returns (valid, reason)."""
    entry  = trade.get("entry")
    stop   = trade.get("stop")
    target = trade.get("target1")
    direction = trade.get("direction","Buy").lower()

    if not entry or not stop or not target:
        return False, "Missing entry, stop, or target"
    if entry <= 0 or stop <= 0 or target <= 0:
        return False, "Negative or zero price levels"

    is_buy = "buy" in direction or "long" in direction

    if is_buy:
        if stop >= entry:
            return False, f"Buy stop {stop} >= entry {entry}"
        if target <= entry:
            return False, f"Buy target {target} <= entry {entry}"
        rr = (target - entry) / (entry - stop) if (entry - stop) > 0 else 0
    else:
        if stop <= entry:
            return False, f"Sell stop {stop} <= entry {entry}"
        if target >= entry:
            return False, f"Sell target {target} >= entry {entry}"
        rr = (entry - target) / (stop - entry) if (stop - entry) > 0 else 0

    tier_cfg = TRADE_TIERS.get("momentum", {})
    min_rr = tier_cfg.get("min_rr", 1.5)
    if rr < min_rr:
        return False, f"R:R {rr:.2f} below minimum {min_rr}"

    return True, "OK"


def build_forex_trade_message(pair_key, rate, tier, trade, idea_id=0):
    """Build Pro channel message for a forex trade idea."""
    pair     = FOREX_PAIRS[pair_key]
    tier_cfg = TRADE_TIERS[tier]
    symbol   = pair["symbol"]
    entry    = trade.get("entry")
    stop     = trade.get("stop")
    t1       = trade.get("target1")
    t2       = trade.get("target2")
    inv      = trade.get("invalidation")
    conf     = trade.get("confidence", "Moderate")
    direction = trade.get("direction", f"Buy {pair['base']}")

    # Calculate R:R
    rr_str = "N/A"
    risk_pct = stop_pct = t1_pct = None
    if entry and stop and t1:
        is_buy = "buy" in direction.lower() or "long" in direction.lower()
        try:
            if is_buy:
                risk   = entry - stop
                reward = t1 - entry
            else:
                risk   = stop - entry
                reward = entry - t1
            if risk > 0:
                rr_str   = f"1 : {reward/risk:.2f}"
                stop_pct = abs(risk / entry * 100)
                t1_pct   = abs(reward / entry * 100)
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

    if rr_str != "N/A" and stop_pct:
        lines += [
            "📊 <b>RISK METRICS</b>",
            f"Risk:Reward:  <b>{rr_str}</b>",
            f"Stop Risk:    <b>-{stop_pct:.2f}%</b>",
        ]
        if t1_pct:
            lines.append(f"T1 Reward:    <b>+{t1_pct:.2f}%</b>")
        lines += [
            f"Confidence:   <b>{conf}</b>",
            f"Max Size:     <b>{tier_cfg['max_size']}</b>",
            "",
        ]

    lines += ["· · · · · · · · · · · · · · · · · · ·", ""]
    lines.append(EDGE_DISCLAIMER if tier == "edge" else STANDARD_DISCLAIMER)
    return "\n".join(lines)


def generate_forex_trade_idea(pair_key, tier="momentum"):
    """Full pipeline for forex trade idea: fetch → AI → parse → validate → save → post."""
    try:
        rate, bid, ask, source = get_forex_rate(pair_key)
        if not rate:
            logger.info(f"[FOREX ENGINE] No rate for {pair_key}")
            return None, None, 0

        pair     = FOREX_PAIRS[pair_key]
        fg_data  = get_fear_greed()
        fg_val   = fg_data[0]["value"] if fg_data else "50"

        prompt   = _build_forex_ai_prompt(pair_key, rate, bid or rate, ask or rate, tier, fg_val, source)
        ai_raw, _ = ask_ai(prompt)
        if not ai_raw:
            return None, None, 0

        trade = _parse_forex_trade(ai_raw, rate, pair["symbol"])
        if not trade or not trade.get("entry"):
            logger.info(f"[FOREX ENGINE] {pair_key} {tier} — no setup from AI")
            return None, None, 0

        valid, reason = _validate_forex_trade(pair_key, rate, trade)
        if not valid:
            logger.warning(f"[FOREX ENGINE] {pair_key} {tier} validation failed: {reason}")
            return None, None, 0

        # Save to trade_ideas table (reuse same table — pair_key as coin)
        db = None
        idea_id = 0
        try:
            db = get_db()
            c  = db.cursor()
            now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
            is_buy = "buy" in trade.get("direction","").lower()
            entry = trade.get("entry", 0)
            stop  = trade.get("stop", 0)
            t1    = trade.get("target1", 0)
            rr    = abs((t1 - entry) / (entry - stop)) if (entry - stop) != 0 else 0
            c.execute(
                """INSERT INTO trade_ideas
                   (coin, tier, direction, timeframe, entry, stop, target1, target2,
                    bias, confidence, rr, invalidation, max_size_pct, ai_rationale, status, created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open',%s) RETURNING id""",
                (
                    pair_key, tier,
                    trade.get("direction", "Buy"),
                    trade.get("timeframe", "4H"),
                    str(round(entry, 4)) if entry else None,
                    str(round(stop, 4)) if stop else None,
                    str(round(t1, 4)) if t1 else None,
                    str(round(trade["target2"], 4)) if trade.get("target2") else None,
                    trade.get("bias", "Neutral"),
                    trade.get("confidence", "Moderate"),
                    f"1:{rr:.2f}",
                    str(round(trade["invalidation"], 4)) if trade.get("invalidation") else None,
                    TRADE_TIERS[tier]["max_size"],
                    ai_raw[:500],
                    now,
                )
            )
            idea_id = c.fetchone()[0]
            db.commit()
            logger.info(f"[FOREX ENGINE] #{idea_id} saved — {pair_key} {tier}")
        except Exception as e:
            logger.error(f"[FOREX ENGINE] Save error: {e}")
        finally:
            if db:
                try: db.close()
                except Exception: pass

        msg = build_forex_trade_message(pair_key, rate, tier, trade, idea_id)
        return msg, trade, idea_id

    except Exception as e:
        logger.error(f"[FOREX ENGINE] {pair_key} {tier}: {e}")
        return None, None, 0


# ═══════════════════════════════════════════════════════════════════════════
