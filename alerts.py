"""Market Pulse Bot — alerts module (split from the real monolithic bot.py)."""

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
from market_pulse.config_runtime import ADMIN_IDS, logger
from market_pulse.db import get_db
from market_pulse.fear_greed import get_fear_greed
from market_pulse.helpers import format_change, format_price, wat_now
from market_pulse.p2p import get_p2p_rate
from market_pulse.price_fetchers import get_best_price, get_secondary_coin
from market_pulse.pro_system import should_show_upsell, FREE_UPSELL_BLOCK
from market_pulse.telegram_api import post_to_channel, post_to_pro_channel, send


# ─── extracted section ───
# 🔔 KEY MARKET ALERT SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

# Key alert coins — kept small by default. Admin can change with /setwatchlist.
KEY_ALERT_COINS = ["BTC", "ETH", "SOL", "BNB", "XRP"]

# Max ONE alert per check cycle — channel stays clean
MAX_ALERTS_PER_CYCLE = 1

# Tolerance: price must be within 1.0% of a level (was 1.8% — too wide)
KEY_LEVEL_TOLERANCE = 0.010

# Cooldown: 6 hours per coin per level, stored in DB to survive restarts
KEY_ALERT_COOLDOWN_HOURS = 6

def _get_key_alert_cooldown(coin):
    """Return True if this coin is still in cooldown (6 hours). DB-backed.
    Keyed on coin only — not level — so slight level fluctuations don't bypass cooldown."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        since = (wat_now() - timedelta(hours=KEY_ALERT_COOLDOWN_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "SELECT updated_at FROM admin_settings WHERE key=%s AND updated_at >= %s",
            (f"key_alert_{coin}", since)
        )
        return c.fetchone() is not None
    except Exception as e:
        logger.warning(f"[KEY ALERT CD] {e}")
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass

def _set_key_alert_cooldown(coin):
    """Record that we just sent an alert for this coin."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (f"key_alert_{coin}", now, now)
        )
        db.commit()
        logger.info(f"[KEY ALERT] Cooldown set for {coin} — next alert in {KEY_ALERT_COOLDOWN_HOURS}h")
    except Exception as e:
        logger.warning(f"[KEY ALERT CD SET] {e}")
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass

# ── Dynamic Key Levels ───────────────────────────────────────────────────
# No hardcoded levels. Levels are calculated on-demand from price history
# in the DB (swing highs, swing lows, round numbers near current price).
# Cache: { coin: (levels_list, calculated_at_timestamp) }
_dynamic_levels_cache = {}
_LEVELS_CACHE_TTL = 3600  # recalculate every hour

def get_dynamic_key_levels(coin, price):
    """Calculate key levels dynamically from stored price history.
    Returns a sorted list of relevant price levels for this coin.
    Falls back to round-number generation if history is insufficient."""
    now = time.time()
    cached = _dynamic_levels_cache.get(coin)
    if cached and (now - cached[1]) < _LEVELS_CACHE_TTL:
        return cached[0]

    levels = set()

    # ── 1. Swing highs and lows from 30-day price history ─────────────────
    db = None
    try:
        db = get_db()
        c = db.cursor()
        since = (wat_now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "SELECT price FROM history WHERE coin=%s AND timestamp >= %s ORDER BY timestamp ASC",
            (coin, since)
        )
        rows = c.fetchall()
        prices = [float(r[0]) for r in rows if r[0]]
    except Exception as e:
        logger.warning(f"[KEY LEVELS] DB read error for {coin}: {e}")
        prices = []
    finally:
        if db:
            try: db.close()
            except Exception: pass

    if len(prices) >= 10:
        # Find swing highs: local max with 3 bars either side
        for i in range(3, len(prices) - 3):
            window = prices[i-3:i+4]
            if prices[i] == max(window):
                levels.add(round(prices[i], _price_decimals(prices[i])))
        # Find swing lows: local min with 3 bars either side
        for i in range(3, len(prices) - 3):
            window = prices[i-3:i+4]
            if prices[i] == min(window):
                levels.add(round(prices[i], _price_decimals(prices[i])))

    # ── 2. Round psychological numbers near current price ──────────────────
    if price:
        levels.update(_round_number_levels(price))

    # ── 3. Filter to levels within 30% of current price ───────────────────
    if price:
        levels = {l for l in levels if l > 0 and abs(l - price) / price <= 0.30}

    result = sorted(levels, reverse=True)
    _dynamic_levels_cache[coin] = (result, now)
    logger.info(f"[KEY LEVELS] {coin}: {len(result)} dynamic levels calculated")
    return result


def _price_decimals(price):
    """Number of decimal places to round to based on price magnitude."""
    if price >= 10000: return 0
    if price >= 1000:  return 0
    if price >= 100:   return 1
    if price >= 10:    return 2
    if price >= 1:     return 3
    return 4


def _round_number_levels(price):
    """Generate psychological round numbers (00, 000, 0000) near a price."""
    levels = set()
    magnitude = 10 ** (len(str(int(price))) - 1)
    for mult in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        levels.add(round(magnitude * mult, _price_decimals(magnitude * mult)))
    for frac in [0.25, 0.50, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0]:
        candidate = round(price * frac / magnitude) * magnitude
        if candidate > 0:
            levels.add(candidate)
    return levels

def _nearest_key_level(price, levels, tolerance=None):
    """Find nearest key level within tolerance. Uses KEY_LEVEL_TOLERANCE by default."""
    if tolerance is None:
        tolerance = KEY_LEVEL_TOLERANCE
    for level in levels:
        if level > 0 and abs(price - level) / level <= tolerance:
            return level
    return None

def _level_label(price, level):
    """Correct terminology based on price vs level."""
    diff_pct = (price - level) / level * 100
    if diff_pct > 1.5:
        return "BREAKOUT", "🚀"
    elif diff_pct > 0:
        return "TESTING RESISTANCE", "🟡"
    elif diff_pct > -1.5:
        return "TESTING SUPPORT", "🟠"
    else:
        return "TRADING BELOW SUPPORT", "🔴"

def _validate_alert(coin, price, entry, stop, target, label, direction="long"):
    """Pre-send validation. Returns (valid, reason). Direction-aware for long/short."""
    # Check for unresolved placeholders
    combined = f"{coin}{price}{entry}{stop}{target}{label}"
    if re.search(r'\\1|\{[a-z_]+\}|%s|None', str(combined)):
        return False, "Unresolved placeholder detected"
    # Price sanity
    if not price or price <= 0:
        return False, "Invalid price"
    # Entry/stop/target logic — direction-aware
    if entry and stop and target:
        try:
            e = float(str(entry).replace("$","").replace(",",""))
            s = float(str(stop).replace("$","").replace(",",""))
            t = float(str(target).replace("$","").replace(",",""))
            if direction == "long":
                if s >= e:
                    return False, f"Long stop {s} >= entry {e}"
                if t <= e:
                    return False, f"Long target {t} <= entry {e}"
                if e <= 0:
                    return False, "Entry price must be positive"
                rr = (t - e) / (e - s)
            else:  # short
                if s <= e:
                    return False, f"Short stop {s} <= entry {e}"
                if t >= e:
                    return False, f"Short target {t} >= entry {e}"
                rr = (e - t) / (s - e)
            if rr < 1.0:
                return False, f"R:R {rr:.2f} below minimum 1:1"
        except Exception as ex:
            logger.warning(f"[VALIDATE ALERT] Parse error: {ex}")
    return True, "OK"

def _calc_trade_metrics(entry, stop, target, size_usd=1000):
    """Calculate R:R, P&L for a given trade."""
    try:
        e = float(str(entry).replace("$","").replace(",",""))
        s = float(str(stop).replace("$","").replace(",",""))
        t = float(str(target).replace("$","").replace(",",""))
        risk_pct  = abs(e - s) / e * 100
        reward_pct = abs(t - e) / e * 100
        rr = reward_pct / risk_pct if risk_pct > 0 else 0
        pot_profit = size_usd * (reward_pct / 100)
        pot_loss   = size_usd * (risk_pct / 100)
        return {
            "rr": round(rr, 2),
            "risk_pct": round(risk_pct, 2),
            "reward_pct": round(reward_pct, 2),
            "pot_profit": round(pot_profit, 2),
            "pot_loss": round(pot_loss, 2),
        }
    except Exception as _e:
        return None

def build_free_key_alert(coin, price, change, level, chat_id=None):
    status_label, status_arrow = _level_label(price, level)
    buy, sell, _ = get_p2p_rate("USDT", "NGN")
    p2p_line = f"💱 USDT/NGN  Buy \u20a6{int(buy):,}  Sell \u20a6{int(sell):,}  Spread \u20a6{int(buy-sell):,}" if buy and sell else ""
    lines = [
        f"⚡ <b>KEY LEVEL ALERT — {coin}</b>",
        f"{status_arrow} <b>{coin}</b> — {status_label}",
        f"💰 Price: <b>{format_price(price)}</b>  {format_change(change)}",
        f"🎯 Key Level: <b>{format_price(level)}</b>",
    ]
    if p2p_line:
        lines += ["", p2p_line]
    lines += ["", "<i>NFA - DYOR  ·  ⚡ Market Pulse</i>"]
    if chat_id and should_show_upsell(chat_id):
        lines += [FREE_UPSELL_BLOCK]
    return "\n".join(lines)

def build_pro_key_alert(coin, price, change, level,
                        entry=None, stop=None, target=None,
                        bias="Neutral", confidence="Uncertain",
                        situation="", context_line="", decision=""):
    """Pro key alert with full Trade Hypothesis section."""
    status_label, status_arrow = _level_label(price, level)
    sd = get_secondary_coin(coin)
    high_24 = sd.get("usd_24h_high") if sd else None
    low_24  = sd.get("usd_24h_low")  if sd else None
    buy, sell, p2p_src = get_p2p_rate("USDT", "NGN")

    # Header
    lines = [
        f"🔔 <b>PRO ALERT — {coin}</b>",
        f"{status_arrow} <b>{status_label}</b>  ·  Key Level: <b>{format_price(level)}</b>",
        f"💰 Price: <b>{format_price(price)}</b>  {format_change(change)}",
    ]
    if high_24 and low_24:
        lines.append(f"📊 24h Range: {format_price(low_24)} — {format_price(high_24)}")

    # Analysis
    lines += ["", "· · · · · · · · · · · · · · · · · · ·", ""]
    if situation:
        lines.append(f"<b>SITUATION:</b> {situation}")
    if context_line:
        lines.append(f"<b>CONTEXT:</b> {context_line}")

    # Trade Hypothesis
    if entry and stop and target:
        valid, reason = _validate_alert(coin, price, entry, stop, target, status_label)
        if valid:
            metrics = _calc_trade_metrics(entry, stop, target)
            lines += [
                "",
                "· · · · · · · · · · · · · · · · · · ·",
                "",
                "📐 <b>TRADE HYPOTHESIS</b>  <i>(Illustrative only)</i>",
                f"Market Bias: <b>{bias}</b>",
                f"Entry Zone: <b>{entry}</b>",
                f"Stop Loss:  <b>{stop}</b>",
                f"Target:     <b>{target}</b>",
            ]
            if metrics:
                lines += [
                    f"Risk:Reward: <b>1 : {metrics['rr']}</b>",
                    f"Pot. Profit: <b>+${metrics['pot_profit']:,.0f} (+{metrics['reward_pct']:.2f}%)</b>  per $1,000",
                    f"Pot. Loss:   <b>-${metrics['pot_loss']:,.0f} (-{metrics['risk_pct']:.2f}%)</b>  per $1,000",
                ]
            lines += [
                f"Confidence: <b>{confidence}</b>",
                "",
                "Conditions: Price must confirm at this level with a candle close.",
                "Assumes normal market liquidity.",
            ]
        else:
            lines += ["", f"⚠️ Trade setup could not be validated: {reason}. Monitor manually."]
    elif decision:
        lines += ["", f"<b>DECISION:</b> {decision}"]

    # P2P Intelligence
    if buy and sell:
        spread = int(buy - sell)
        lines += [
            "",
            "· · · · · · · · · · · · · · · · · · ·",
            "",
            "💱 <b>NIGERIAN P2P INTELLIGENCE</b>",
            f"Buy: <b>₦{int(buy):,}</b>   Sell: <b>₦{int(sell):,}</b>   Spread: <b>₦{spread:,}</b>",
            f"Source: {p2p_src}",
            "Recommendation: " + (
                "Reasonable to convert now — spread is tight." if spread <= 35
                else "Spread is wide — wait for it to compress unless urgent." if spread >= 50
                else "Moderate spread — convert only if needed."
            ),
        ]

    lines += [
        "",
        "· · · · · · · · · · · · · · · · · · ·",
        "",
        "<i>Illustrative example only. Not financial advice. Estimates are model-generated and not guaranteed.</i>",
        "<i>NFA — manage your risk.  ·  ⚡ Market Pulse Pro</i>",
    ]
    return "\n".join(lines)

def _parse_ai_trade(ai_text, price):
    """Extract entry, stop, target from AI response. Returns dict or None."""
    import re as _re
    if not ai_text:
        return None
    try:
        e_m = _re.search(r"Entry[:\s]+\$?([0-9,\.]+)", ai_text, _re.IGNORECASE)
        s_m = _re.search(r"Stop[:\s]+\$?([0-9,\.]+)", ai_text, _re.IGNORECASE)
        t_m = _re.search(r"Target[:\s]+\$?([0-9,\.]+)", ai_text, _re.IGNORECASE)
        bias_m = _re.search(r"(Bullish|Bearish|Neutral)", ai_text, _re.IGNORECASE)
        conf_m = _re.search(r"Confidence[:\s]+(High|Moderate|Low|Uncertain)", ai_text, _re.IGNORECASE)
        sit_m = _re.search(r"SITUATION[:\s]*(.+?)(?:\n|$)", ai_text, _re.IGNORECASE)
        ctx_m = _re.search(r"CONTEXT[:\s]*(.+?)(?:\n|$)", ai_text, _re.IGNORECASE)
        dec_m = _re.search(r"DECISION[:\s]*(.+?)(?:\n|$)", ai_text, _re.IGNORECASE)
        entry  = f"${float(e_m.group(1).replace(',','')):,.0f}" if e_m else None
        stop   = f"${float(s_m.group(1).replace(',','')):,.0f}" if s_m else None
        target = f"${float(t_m.group(1).replace(',','')):,.0f}" if t_m else None
        return {
            "entry":   entry,
            "stop":    stop,
            "target":  target,
            "bias":    bias_m.group(1).capitalize() if bias_m else "Neutral",
            "confidence": conf_m.group(1).capitalize() if conf_m else "Uncertain",
            "situation": sit_m.group(1).strip() if sit_m else "",
            "context":   ctx_m.group(1).strip() if ctx_m else "",
            "decision":  dec_m.group(1).strip() if dec_m else "",
        }
    except Exception as _e:
        return None

def check_key_market_alerts():
    """Smart key level alert engine.
    - Max 1 alert per cycle (MAX_ALERTS_PER_CYCLE)
    - 6-hour per-coin cooldown stored in DB (survives restarts)
    - Tighter 1% proximity tolerance (was 1.8%)
    - Only alerts on confirmed swing levels from price history
    """
    triggered = []

    try:
        for coin in KEY_ALERT_COINS:
            price, change = get_best_price(coin)
            if not price:
                continue
            levels = get_dynamic_key_levels(coin, price)
            if not levels:
                continue
            level = _nearest_key_level(price, levels)
            if not level:
                continue
            # DB-backed cooldown — coin-level, survives restarts
            if _get_key_alert_cooldown(coin):
                logger.debug(f"[KEY ALERT] {coin} in 6h cooldown, skipping")
                continue
            proximity = abs(price - level) / level
            triggered.append((proximity, coin, price, change or 0, level))

        # Sort by proximity — send the coin closest to its level
        triggered.sort(key=lambda x: x[0])

        sent = 0
        for proximity, coin, price, ch, level in triggered:
            if sent >= MAX_ALERTS_PER_CYCLE:
                break
            # Set cooldown BEFORE sending — prevents double-post on error
            _set_key_alert_cooldown(coin)
            logger.info(f"[KEY ALERT] {coin} @ {format_price(price)} — {_level_label(price, level)[0]}")

            # Free channel
            post_to_channel(build_free_key_alert(coin, price, ch, level))

            # Pro channel — structured AI prompt
            sd = get_secondary_coin(coin)
            high_24 = sd.get("usd_24h_high") if sd else None
            low_24  = sd.get("usd_24h_low")  if sd else None
            fg_data = get_fear_greed()
            fg_val  = fg_data[0]["value"] if fg_data else "N/A"
            h_str = format_price(high_24) if isinstance(high_24,(int,float)) else "N/A"
            l_str = format_price(low_24)  if isinstance(low_24,(int,float)) else "N/A"
            status_label, _ = _level_label(price, level)

            ai_prompt = (
                f"{coin} is at {format_price(price)} ({format_change(ch)}). "
                f"Status: {status_label} at {format_price(level)}. "
                f"24h High: {h_str}  Low: {l_str}. Fear & Greed: {fg_val}/100. "
                f"Respond in this EXACT format, plain text, no asterisks:\n"
                f"SITUATION: [one sentence — what is happening at this level right now]\n"
                f"CONTEXT: [one sentence — Nigerian trader angle, P2P or naira impact]\n"
                f"Market Bias: [Bullish / Bearish / Neutral]\n"
                f"Entry: $[price]\n"
                f"Stop: $[price]\n"
                f"Target: $[price]\n"
                f"Confidence: [High / Moderate / Low / Uncertain]\n"
                f"DECISION: [one sentence — what you would do right now or wait with reason]\n"
                f"If no quality setup exists, write: Entry: none  Stop: none  Target: none"
            )
            ai_raw, _ = ask_ai(ai_prompt)
            trade = _parse_ai_trade(ai_raw, price)

            if trade and trade.get("entry") and trade["entry"] != "$none":
                valid, reason = _validate_alert(
                    coin, price,
                    trade.get("entry",""), trade.get("stop",""), trade.get("target",""),
                    status_label
                )
                if not valid:
                    logger.warning(f"[KEY ALERT] Validation failed for {coin}: {reason}")
                    # Send without trade hypothesis
                    post_to_pro_channel(build_pro_key_alert(
                        coin, price, ch, level,
                        situation=trade.get("situation",""),
                        context_line=trade.get("context",""),
                        decision=f"Setup invalidated: {reason}. Monitor manually.",
                        bias=trade.get("bias","Neutral"),
                        confidence="Uncertain"
                    ))
                else:
                    post_to_pro_channel(build_pro_key_alert(
                        coin, price, ch, level,
                        entry=trade.get("entry"),
                        stop=trade.get("stop"),
                        target=trade.get("target"),
                        bias=trade.get("bias","Neutral"),
                        confidence=trade.get("confidence","Uncertain"),
                        situation=trade.get("situation",""),
                        context_line=trade.get("context",""),
                    ))
            else:
                # No trade setup — send analysis only
                post_to_pro_channel(build_pro_key_alert(
                    coin, price, ch, level,
                    situation=trade.get("situation","") if trade else "",
                    context_line=trade.get("context","") if trade else "",
                    decision="No High-Confidence Trade Setup for this alert. Monitor the level.",
                    bias=trade.get("bias","Neutral") if trade else "Neutral",
                    confidence="Uncertain"
                ))
            sent += 1
            time.sleep(1)  # brief pause between alerts

    except Exception as e:
        logger.error(f"[KEY ALERT ERROR] {e}")

def daily_digest():
    db = None
    try:
        db = get_db()
        c = db.cursor()
        today_wat = wat_now().strftime("%Y-%m-%d")
        c.execute("SELECT COUNT(*) FROM events WHERE timestamp LIKE %s", (today_wat + "%",))
        total_events = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE first_seen LIKE %s", (today_wat + "%",))
        new_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE last_seen LIKE %s", (today_wat + "%",))
        active_users = c.fetchone()[0]
    except Exception as e:
        logger.error("[DAILY DIGEST ERROR] %s" % e)
        return
    finally:
        if db:
            try: db.close()
            except Exception: pass

    for admin_id in ADMIN_IDS:
        try:
            send(admin_id, (
                f"📊 <b>Daily Digest</b>\n\n"
                f"📅 {today_wat} (WAT)\n"
                f"👤 New Users: <b>{new_users}</b>\n"
                f"🟢 Active Users: <b>{active_users}</b>\n"
                f"📊 Total Events: <b>{total_events}</b>"
            ))
        except Exception as e:
            logger.error("[DAILY DIGEST SEND] admin %s: %s" % (admin_id, e))

# ═══════════════════════════════════════════════════════════════════════════
