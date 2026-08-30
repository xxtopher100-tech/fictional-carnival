"""Market Pulse Bot — edge_trade_engine module (split from the real monolithic bot.py)."""

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
from market_pulse.alerts import _calc_trade_metrics, _validate_alert
from market_pulse.config_runtime import logger
from market_pulse.db import get_db
from market_pulse.fear_greed import get_fear_greed
from market_pulse.helpers import format_price, wat_now
from market_pulse.p2p import get_p2p_rate
from market_pulse.price_fetchers import get_best_price, get_secondary_coin
from market_pulse.telegram_api import send


# ─── extracted section ───
# ⚡ EDGE TRADE ENGINE — THREE-TIER TRADE SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

TRADE_TIERS = {
    "steady": {
        "label": "STEADY TRADE", "emoji": "🟢",
        "risk_desc": "Low-Medium Risk",
        "max_stop_pct": 5.0, "min_target_pct": 8.0, "min_rr": 1.5,
        "max_size": "3-5% of portfolio",
    },
    "momentum": {
        "label": "MOMENTUM TRADE", "emoji": "🟡",
        "risk_desc": "Medium-High Risk",
        "max_stop_pct": 10.0, "min_target_pct": 15.0, "min_rr": 1.5,
        "max_size": "2-3% of portfolio",
    },
    "edge": {
        "label": "EDGE TRADE", "emoji": "🔴",
        "risk_desc": "HIGH RISK — HIGH REWARD",
        "max_stop_pct": 15.0, "min_target_pct": 30.0, "min_rr": 2.0,
        "max_size": "1-2% of portfolio MAX",
    },
}

EDGE_DISCLAIMER = (
    "\u2501" * 24 + "\n"
    "\u26a0\ufe0f <b>RISK DISCLAIMER</b>\n"
    "This is a HIGH-RISK setup. You can LOSE your entire position. "
    "Only trade money you can afford to lose completely. "
    "Past setups do not guarantee future results. "
    "Market Pulse takes no responsibility for trading outcomes.\n"
    "NFA \u2014 DYOR \u2014 Trade at your own risk.\n"
    "\u2501" * 24
)

STANDARD_DISCLAIMER = (
    "<i>Illustrative only. Not financial advice. "
    "Always use a stop loss. NFA \u2014 DYOR \u2014 manage your risk.</i>\n"
    "\u26a1 Market Pulse Pro"
)


def _gather_trade_analytics(coin, price):
    """Pull rich market data from price history DB for AI context.
    Returns a dict of calculated indicators."""
    analytics = {
        "rsi_14": None,
        "above_ma20": None,
        "pct_from_30d_high": None,
        "pct_from_30d_low": None,
        "volume_trend": None,
        "price_30d_high": None,
        "price_30d_low": None,
    }
    db = None
    try:
        db = get_db()
        c = db.cursor()
        since_30d = (wat_now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "SELECT price FROM history WHERE coin=%s AND timestamp >= %s ORDER BY timestamp ASC",
            (coin, since_30d)
        )
        rows = c.fetchall()
        prices = [float(r[0]) for r in rows if r[0]]

        if len(prices) >= 14:
            # RSI-14 approximation using Wilder smoothing
            gains, losses = [], []
            for i in range(1, len(prices)):
                delta = prices[i] - prices[i-1]
                gains.append(max(delta, 0))
                losses.append(max(-delta, 0))
            avg_gain = sum(gains[-14:]) / 14
            avg_loss = sum(losses[-14:]) / 14
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                analytics["rsi_14"] = round(100 - (100 / (1 + rs)), 1)
            else:
                analytics["rsi_14"] = 100.0

        if len(prices) >= 20:
            ma20 = sum(prices[-20:]) / 20
            analytics["above_ma20"] = price > ma20

        if len(prices) >= 5:
            high_30d = max(prices)
            low_30d  = min(prices)
            analytics["price_30d_high"] = high_30d
            analytics["price_30d_low"]  = low_30d
            analytics["pct_from_30d_high"] = round((price - high_30d) / high_30d * 100, 1)
            analytics["pct_from_30d_low"]  = round((price - low_30d)  / low_30d  * 100, 1)

        # Volume trend: compare recent 7 data points to previous 7
        if len(prices) >= 14:
            recent_vol  = sum(abs(prices[i]-prices[i-1]) for i in range(len(prices)-7, len(prices)))
            prev_vol    = sum(abs(prices[i]-prices[i-1]) for i in range(len(prices)-14, len(prices)-7))
            if prev_vol > 0:
                analytics["volume_trend"] = "rising" if recent_vol > prev_vol * 1.1 else (
                    "falling" if recent_vol < prev_vol * 0.9 else "flat"
                )

    except Exception as e:
        logger.warning(f"[TRADE ANALYTICS] {coin}: {e}")
    finally:
        if db:
            try: db.close()
            except Exception: pass

    return analytics


def _analytics_to_str(a):
    """Format analytics dict into a concise string for the AI prompt."""
    parts = []
    if a["rsi_14"] is not None:
        rsi = a["rsi_14"]
        zone = "oversold" if rsi < 35 else ("overbought" if rsi > 65 else "neutral")
        parts.append(f"RSI-14: {rsi} ({zone})")
    if a["above_ma20"] is not None:
        parts.append(f"Price {'above' if a['above_ma20'] else 'below'} 20-day average")
    if a["pct_from_30d_high"] is not None:
        parts.append(f"{a['pct_from_30d_high']:+.1f}% from 30d high ({format_price(a['price_30d_high'])})")
    if a["pct_from_30d_low"] is not None:
        parts.append(f"{a['pct_from_30d_low']:+.1f}% from 30d low ({format_price(a['price_30d_low'])})")
    if a["volume_trend"]:
        parts.append(f"Volatility trend: {a['volume_trend']}")
    return " | ".join(parts) if parts else "Insufficient history (< 14 data points)"


def _tier_conditions_met(tier, analytics, fg_val):
    """Pre-screen: return (ok, reason) based on market conditions vs tier requirements.
    Prevents the AI from generating a setup when conditions are clearly wrong."""
    rsi = analytics.get("rsi_14")
    above_ma = analytics.get("above_ma20")
    vol_trend = analytics.get("volume_trend")
    pct_high  = analytics.get("pct_from_30d_high")
    fg = int(fg_val) if str(fg_val).isdigit() else 50

    if tier == "steady":
        # Steady needs clear structure — avoid extreme conditions
        if rsi and (rsi > 75 or rsi < 25):
            return False, f"RSI {rsi} is extreme — no steady setup in these conditions"
        if vol_trend == "rising" and fg > 75:
            return False, "Volatility rising + extreme greed — not a steady environment"
        return True, "ok"

    elif tier == "momentum":
        # Momentum needs directional movement
        if vol_trend == "flat" and rsi and 40 < rsi < 60:
            return False, "Market is ranging (flat volatility, neutral RSI) — no momentum"
        return True, "ok"

    elif tier == "edge":
        # Edge needs strong conditions — RSI extended OR near 30d extreme
        has_condition = False
        if rsi and (rsi > 68 or rsi < 32):
            has_condition = True
        if pct_high and abs(pct_high) < 3:
            has_condition = True  # Near 30d high/low
        if fg > 75 or fg < 25:
            has_condition = True
        if not has_condition:
            return False, "No extreme conditions present — save Edge for high-conviction moments"
        return True, "ok"

    return True, "ok"


def _build_trade_ai_prompt(coin, price, tier, sd, fg_val, p2p_str, analytics=None):
    tier_cfg = TRADE_TIERS[tier]
    h24 = sd.get("usd_24h_high") if sd else None
    l24 = sd.get("usd_24h_low") if sd else None
    h_str = format_price(h24) if isinstance(h24, (int, float)) else "N/A"
    l_str = format_price(l24) if isinstance(l24, (int, float)) else "N/A"
    analytics_str = _analytics_to_str(analytics) if analytics else "No history data"
    tf_guide = {
        "steady":   "Daily or Weekly. Prefer established structure.",
        "momentum": "4H or Daily. Breakouts or trend continuations.",
        "edge":     "1H or 4H. High-conviction momentum setups only.",
    }
    return (
        f"You are a professional crypto analyst generating a {tier_cfg['risk_desc']} trade idea "
        f"for Nigerian traders on Market Pulse Pro.\n\n"
        f"COIN: {coin} | PRICE: {format_price(price)} | 24H: {l_str}—{h_str}\n"
        f"FEAR & GREED: {fg_val}/100 | P2P: {p2p_str}\n"
        f"MARKET DATA: {analytics_str}\n\n"
        f"TIER: {tier_cfg['label']} — {tier_cfg['risk_desc']}\n"
        f"TIMEFRAME: {tf_guide[tier]}\n"
        f"STOP MAX: {tier_cfg['max_stop_pct']}% | TARGET MIN: {tier_cfg['min_target_pct']}% | MIN R:R: {tier_cfg['min_rr']}:1\n\n"
        f"Use the MARKET DATA above (RSI, MA, distance from highs/lows) to justify your setup.\n"
        f"If the data does not support a {tier} setup, say so — do not force a trade.\n\n"
        f"Respond ONLY in this exact format. No asterisks. Plain text:\n"
        f"TIMEFRAME: [1H / 4H / Daily / Weekly]\n"
        f"DIRECTION: [Long / Short]\n"
        f"RATIONALE: [2 sentences — must reference the market data above]\n"
        f"NIGERIAN ANGLE: [1 sentence — naira/P2P relevance]\n"
        f"Market Bias: [Strongly Bullish / Bullish / Neutral / Bearish / Strongly Bearish]\n"
        f"Entry: $[price]\n"
        f"Stop Loss: $[price]\n"
        f"Target 1: $[price]\n"
        f"Target 2: $[price or none]\n"
        f"Invalidation: $[price]\n"
        f"Confidence: [High / Moderate / Low]\n"
        f"If no quality setup: TIMEFRAME: None\nDIRECTION: None\nEntry: none"
    )


def _parse_trade_idea(ai_text, price):
    if not ai_text:
        return None
    try:
        def _get(pattern, text):
            m = re.search(pattern, text, re.IGNORECASE)
            return m.group(1).strip() if m else None
        def _pf(pattern, text):
            raw = _get(pattern, text)
            if not raw or raw.lower() in ("none","n/a","-","$none"):
                return None
            try:
                return "$" + f"{float(raw.replace('$','').replace(',','')):,.2f}"
            except Exception:
                return None
        return {
            "timeframe":    _get(r"TIMEFRAME[:\s]+(\S+)", ai_text) or "4H",
            "direction":    _get(r"DIRECTION[:\s]+(\w+)", ai_text) or "Long",
            "rationale":    _get(r"RATIONALE[:\s]*(.+?)(?=\nNIGERIAN|\n[A-Z]|\Z)", ai_text),
            "ng_angle":     _get(r"NIGERIAN ANGLE[:\s]*(.+?)(?=\nMarket|\n[A-Z]|\Z)", ai_text),
            "bias":         _get(r"Market Bias[:\s]*(.+?)(?=\n|\Z)", ai_text) or "Neutral",
            "entry":        _pf(r"Entry[:\s]+\$?([0-9,\.]+)", ai_text),
            "stop":         _pf(r"Stop Loss[:\s]+\$?([0-9,\.]+)", ai_text),
            "target1":      _pf(r"Target 1[:\s]+\$?([0-9,\.]+)", ai_text),
            "target2":      _pf(r"Target 2[:\s]+\$?([0-9,\.]+)", ai_text),
            "invalidation": _pf(r"Invalidation[:\s]+\$?([0-9,\.]+)", ai_text),
            "confidence":   _get(r"Confidence[:\s]+(\w+)", ai_text) or "Moderate",
        }
    except Exception as e:
        logger.warning(f"[TRADE PARSE] {e}")
        return None


def save_trade_idea(coin, tier, trade, ai_raw=""):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        metrics = _calc_trade_metrics(trade.get("entry",""), trade.get("stop",""), trade.get("target1",""))
        rr_str = f"1:{metrics['rr']}" if metrics else "N/A"
        c.execute(
            """INSERT INTO trade_ideas
               (coin, tier, direction, timeframe, entry, stop, target1, target2,
                bias, confidence, rr, invalidation, max_size_pct, ai_rationale, status, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open',%s) RETURNING id""",
            (coin, tier, trade.get("direction","Long"), trade.get("timeframe","4H"),
             trade.get("entry"), trade.get("stop"), trade.get("target1"), trade.get("target2"),
             trade.get("bias","Neutral"), trade.get("confidence","Moderate"), rr_str,
             trade.get("invalidation"), TRADE_TIERS[tier]["max_size"],
             ai_raw[:500] if ai_raw else "", now)
        )
        idea_id = c.fetchone()[0]
        db.commit()
        logger.info(f"[TRADE IDEAS] #{idea_id} saved — {coin} {tier}")
        return idea_id
    except Exception as e:
        logger.error(f"[TRADE IDEAS] Save error: {e}")
        if db:
            try: db.rollback()
            except Exception: pass
        return 0
    finally:
        if db:
            try: db.close()
            except Exception: pass


def build_trade_idea_message(coin, price, tier, trade, idea_id=0):
    tier_cfg = TRADE_TIERS[tier]
    metrics = _calc_trade_metrics(trade.get("entry",""), trade.get("stop",""), trade.get("target1",""))
    lines = [
        f"{tier_cfg['emoji']} <b>{tier_cfg['label']} #{idea_id}</b>",
        f"<b>{coin}/USDT</b>  \u00b7  {trade.get('direction','Long').upper()}  \u00b7  {trade.get('timeframe','4H')}",
        f"<i>{tier_cfg['risk_desc']}</i>",
        "",
        f"\U0001f4b0 Current: <b>{format_price(price)}</b>",
        f"\U0001f4c8 Bias: <b>{trade.get('bias','Neutral')}</b>",
        "",
    ]
    if trade.get("rationale"):
        lines += ["\U0001f4cb <b>SETUP</b>", trade["rationale"], ""]
    if trade.get("ng_angle"):
        lines += ["\U0001f1f3\U0001f1ec <b>NIGERIAN ANGLE</b>", trade["ng_angle"], ""]
    lines += ["\u00b7 " * 18, ""]
    entry = trade.get("entry","\u2014")
    stop  = trade.get("stop","\u2014")
    t1    = trade.get("target1","\u2014")
    t2    = trade.get("target2")
    inv   = trade.get("invalidation","\u2014")
    conf  = trade.get("confidence","Moderate")
    lines += [
        "\U0001f4d0 <b>LEVELS</b>",
        f"Entry:        <b>{entry}</b>",
        f"Stop Loss:    <b>{stop}</b>",
        f"Target 1:     <b>{t1}</b>",
    ]
    if t2:
        lines.append(f"Target 2:     <b>{t2}</b>  <i>(aggressive)</i>")
    lines += [f"Invalidation: <b>{inv}</b>", ""]
    if metrics:
        lines += [
            "\U0001f4ca <b>RISK METRICS</b>",
            f"Risk:Reward:  <b>1 : {metrics['rr']}</b>",
            f"Stop Risk:    <b>-{metrics['risk_pct']:.1f}%</b>  (${metrics['pot_loss']:,.0f} per $1,000)",
            f"T1 Reward:    <b>+{metrics['reward_pct']:.1f}%</b>  (${metrics['pot_profit']:,.0f} per $1,000)",
            f"Confidence:   <b>{conf}</b>",
            f"Max Size:     <b>{tier_cfg['max_size']}</b>",
            "",
        ]
    lines += ["\u00b7 " * 18, ""]
    lines.append(EDGE_DISCLAIMER if tier == "edge" else STANDARD_DISCLAIMER)
    return "\n".join(lines)


def generate_trade_idea(coin, tier="momentum"):
    """Full pipeline: gather analytics → pre-screen → AI → parse → validate → save → return."""
    try:
        price, _ = get_best_price(coin)
        if not price:
            return None, None, 0
        sd      = get_secondary_coin(coin)
        fg_data = get_fear_greed()
        fg_val  = fg_data[0]["value"] if fg_data else "50"
        buy, sell, _ = get_p2p_rate("USDT", "NGN")
        p2p_str = f"USDT/NGN Buy \u20a6{int(buy):,} / Sell \u20a6{int(sell):,}" if buy else "N/A"

        # Gather rich market analytics from price history
        analytics = _gather_trade_analytics(coin, price)

        # Pre-screen: check if market conditions support this tier
        ok, reason = _tier_conditions_met(tier, analytics, fg_val)
        if not ok:
            logger.info(f"[TRADE ENGINE] {coin} {tier} pre-screened out: {reason}")
            return None, None, 0

        prompt  = _build_trade_ai_prompt(coin, price, tier, sd, fg_val, p2p_str, analytics)
        ai_raw, _ = ask_ai(prompt)
        if not ai_raw:
            return None, None, 0
        trade = _parse_trade_idea(ai_raw, price)
        if not trade or not trade.get("entry"):
            return None, None, 0
        if trade["entry"] and trade["entry"].lower() in ("$none","none"):
            logger.info(f"[TRADE ENGINE] {coin} {tier} — AI found no quality setup")
            return None, None, 0
        direction = trade.get("direction","Long").lower()
        valid, reason = _validate_alert(
            coin, price,
            trade.get("entry",""), trade.get("stop",""), trade.get("target1",""),
            tier, direction=direction
        )
        if not valid:
            logger.warning(f"[TRADE ENGINE] {coin} {tier} validation failed: {reason}")
            return None, None, 0
        idea_id = save_trade_idea(coin, tier, trade, ai_raw)
        analytics_str = _analytics_to_str(analytics) if analytics else ""
        msg = build_trade_idea_message(coin, price, tier, trade, idea_id)
        if analytics_str and "Insufficient" not in analytics_str:
            msg += f"\n\n<i>\U0001f4ca Data: {analytics_str}</i>"
        return msg, trade, idea_id
    except Exception as e:
        logger.error(f"[TRADE ENGINE] {coin} {tier}: {e}")
        return None, None, 0


def get_trade_history(limit=10, coin=None, tier=None):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        filters, params = [], []
        if coin:
            filters.append("coin=%s"); params.append(coin)
        if tier:
            filters.append("tier=%s"); params.append(tier)
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        params.append(limit)
        c.execute(
            f"SELECT id, coin, tier, direction, timeframe, entry, target1, confidence, status, created_at "
            f"FROM trade_ideas {where} ORDER BY id DESC LIMIT %s", params
        )
        return c.fetchall()
    except Exception as e:
        logger.error(f"[TRADE HISTORY] {e}")
        return []
    finally:
        if db:
            try: db.close()
            except Exception: pass


def close_trade_idea(idea_id, result):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "UPDATE trade_ideas SET status='closed', closed_at=%s, result=%s WHERE id=%s",
            (now, result, idea_id)
        )
        db.commit()
        return True
    except Exception as e:
        logger.error(f"[CLOSE TRADE] {e}")
        if db:
            try: db.rollback()
            except Exception: pass
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass




def check_user_price_alerts():
    """Check all active user-set price alerts. Batch-deactivates triggered alerts."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT id, chat, coin, condition, target, label FROM alerts WHERE active=1")
        rows = c.fetchall()
    except Exception as e:
        logger.error(f"[PRICE ALERTS LOAD] {e}")
        return
    finally:
        if db:
            try: db.close()
            except Exception: pass

    triggered_ids = []
    for row in rows:
        aid, chat_id, coin, condition, target, label = row
        try:
            price, _ = get_best_price(coin)
            if not price:
                continue
            fired = (condition == "above" and price >= target) or                     (condition == "below" and price <= target)
            if fired:
                lbl = f" ({label})" if label else ""
                arrow = "📈" if condition == "above" else "📉"
                msg = (
                    f"🔔 <b>PRICE ALERT TRIGGERED</b>\n\n"
                    f"{arrow} <b>{coin}</b> is now <b>{condition}</b> your target{lbl}\n"
                    f"💰 Current: <b>{format_price(price)}</b>\n"
                    f"🎯 Target: <b>{format_price(target)}</b>\n\n"
                    f"<i>NFA - DYOR</i>"
                )
                send(int(chat_id), msg)
                triggered_ids.append(aid)
                logger.info(f"[PRICE ALERT] {coin} {condition} {target} triggered for {chat_id}")
        except Exception as e:
            logger.error(f"[PRICE ALERT] {coin} for {chat_id}: {e}")

    # Batch-deactivate all triggered alerts in one query
    if triggered_ids:
        db2 = None
        try:
            db2 = get_db()
            c2 = db2.cursor()
            c2.execute("UPDATE alerts SET active=0 WHERE id = ANY(%s)", (triggered_ids,))
            db2.commit()
        except Exception as e:
            logger.error(f"[PRICE ALERT DEACTIVATE] {e}")
            if db2:
                try: db2.rollback()
                except Exception: pass
        finally:
            if db2:
                try: db2.close()
                except Exception: pass



def check_watchlist_alerts():
    """Single-query watchlist check — no N+1 pattern."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT chat, coin FROM watchlists ORDER BY chat")
        rows = c.fetchall()
    except Exception as e:
        logger.error("[WATCHLIST ALERT ERROR] %s" % e)
        return
    finally:
        if db:
            try: db.close()
            except Exception: pass

    from collections import defaultdict
    watchlists = defaultdict(list)
    for chat_id, coin in rows:
        watchlists[chat_id].append(coin)

    for chat_id, coins in watchlists.items():
        for coin in coins:
            try:
                price, change = get_best_price(coin)
                if price and change and abs(change) > 5:
                    direction = "🚀 UP" if change > 0 else "🔴 DOWN"
                    send(chat_id, (
                        f"🔔 <b>Watchlist Alert</b>\n\n"
                        f"{coin} is {direction} <b>{abs(change):.2f}%</b>\n"
                        f"Current: {format_price(price)}\n\n"
                        f"<i>NFA - DYOR</i>"
                    ))
            except Exception as e:
                logger.error(f"[WATCHLIST ALERT] {coin} for {chat_id}: {e}")

# ═══════════════════════════════════════════════════════════════════════════
