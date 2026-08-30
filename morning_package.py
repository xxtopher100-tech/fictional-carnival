"""Market Pulse Bot — morning_package module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import load_admin_config, logger, save_admin_config
from market_pulse.edge_trade_engine import _gather_trade_analytics, _tier_conditions_met, generate_trade_idea
from market_pulse.fear_greed import fg_emoji, get_fear_greed
from market_pulse.forex_trade_engine import generate_forex_trade_idea, get_forex_rate
from market_pulse.helpers import wat_now
from market_pulse.p2p import get_p2p_rate
from market_pulse.price_fetchers import get_best_price
from market_pulse.telegram_api import post_to_pro_channel


# ─── extracted section ───
# 🌅 MORNING PRO PACKAGE
# ═══════════════════════════════════════════════════════════════════════════
# Fires at 7AM WAT alongside morning brief.
# Pro channel receives: header + 3 crypto setups + 3 forex setups + P2P read
# Each as a separate message. Runs in background thread.
# Skips any tier with no quality setup — never forces a trade.
# ═══════════════════════════════════════════════════════════════════════════

# Best coins to feature in morning package (most liquid, best AI setups)
MORNING_CRYPTO_COINS = ["BTC", "ETH", "SOL", "BNB", "XRP", "AVAX"]
MORNING_FOREX_PAIRS  = ["USDT/NGN", "USD/NGN", "EUR/USD", "GBP/USD", "BTC/NGN"]


def build_morning_p2p_intelligence():
    """Build P2P intelligence section for morning Pro package."""
    try:
        buy, sell, source = get_p2p_rate("USDT", "NGN")
        if not buy or not sell:
            return None
        spread = buy - sell
        spread_pct = (spread / sell) * 100
        fg_data = get_fear_greed()
        fg_val = int(fg_data[0]["value"]) if fg_data else 50

        # Direction read
        if fg_val > 65:
            direction = "Naira under pressure — crypto demand high. Buy USDT now before rates rise further."
            emoji = "📈"
        elif fg_val < 35:
            direction = "Crypto sentiment weak — USDT demand may ease. Consider waiting for better P2P rates."
            emoji = "📉"
        else:
            direction = "Market neutral. P2P rates stable. Standard entry timing."
            emoji = "➡️"

        # Spread health
        if spread_pct < 1.5:
            spread_health = "🟢 Tight spread — good liquidity"
        elif spread_pct < 3.0:
            spread_health = "🟡 Normal spread"
        else:
            spread_health = "🔴 Wide spread — low liquidity, trade carefully"

        return (
            f"💱 <b>P2P INTELLIGENCE — MORNING READ</b>\n"
            f"<i>USDT/NGN · {source}</i>\n\n"
            f"Buy USDT:  <b>₦{int(buy):,}</b>\n"
            f"Sell USDT: <b>₦{int(sell):,}</b>\n"
            f"Spread:    <b>₦{int(spread):,}</b> ({spread_pct:.1f}%)\n"
            f"{spread_health}\n\n"
            f"{emoji} <b>Direction Read</b>\n"
            f"{direction}\n\n"
            f"F&G: <b>{fg_val}/100</b> — {fg_emoji(fg_val)} Sentiment context\n\n"
            f"<i>P2P rates change throughout the day. This is the opening read.\n"
            f"NFA — verify before trading.</i>\n"
            f"⚡ Market Pulse Pro"
        )
    except Exception as e:
        logger.error(f"[MORNING P2P] {e}")
        return None


def run_morning_pro_package():
    """
    Full morning Pro trade package. Called in background thread at 7AM WAT.

    Posts to Pro channel in order:
    1. Header
    2. Crypto Steady (best qualifying coin)
    3. Crypto Momentum (best qualifying coin)
    4. Crypto Edge (only if conditions strongly support it)
    5. Forex Steady
    6. Forex Momentum
    7. Forex Edge (only if conditions strongly support it)
    8. P2P Intelligence

    3-second gap between messages for clean channel flow.
    Skips any tier with no quality setup.
    """
    logger.info("[MORNING PRO PKG] Starting morning trade package...")

    fg_data = get_fear_greed()
    fg_val  = fg_data[0]["value"] if fg_data else "50"

    # ── Header ────────────────────────────────────────────────────────────
    wat_str = wat_now().strftime("%A, %B %d · %I:%M %p WAT")
    header  = (
        f"⚡ <b>MORNING PRO INTELLIGENCE PACKAGE</b>\n"
        f"<i>{wat_str}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Today\'s full trade package follows.\n"
        f"Crypto setups · Forex setups · P2P read\n\n"
        f"<i>Only quality setups posted. Tiers skipped if conditions don\'t support them.</i>"
    )
    post_to_pro_channel(header)
    time.sleep(3)

    # ── Crypto Setups ─────────────────────────────────────────────────────
    crypto_posted = 0
    for tier in ["steady", "momentum", "edge"]:
        posted = False
        for coin in MORNING_CRYPTO_COINS:
            try:
                price, _ = get_best_price(coin)
                if not price:
                    continue
                analytics = _gather_trade_analytics(coin, price)
                ok, reason = _tier_conditions_met(tier, analytics, fg_val)
                if not ok:
                    continue
                msg, trade, idea_id = generate_trade_idea(coin, tier)
                if msg and idea_id:
                    post_to_pro_channel(msg)
                    crypto_posted += 1
                    posted = True
                    logger.info(f"[MORNING PRO PKG] Crypto {tier}: {coin} #{idea_id}")
                    time.sleep(3)
                    break  # One coin per tier
            except Exception as e:
                logger.error(f"[MORNING PRO PKG] Crypto {tier} {coin}: {e}")
                continue
        if not posted:
            logger.info(f"[MORNING PRO PKG] Crypto {tier}: no quality setup found — skipped")

    # ── Forex Setups ──────────────────────────────────────────────────────
    forex_posted = 0
    for tier in ["steady", "momentum", "edge"]:
        posted = False
        for pair_key in MORNING_FOREX_PAIRS:
            try:
                rate, _, _, _ = get_forex_rate(pair_key)
                if not rate:
                    continue
                # Simplified forex pre-screen
                fg = int(fg_val) if str(fg_val).isdigit() else 50
                if tier == "edge" and not (fg > 70 or fg < 30):
                    continue
                msg, trade, idea_id = generate_forex_trade_idea(pair_key, tier)
                if msg and idea_id:
                    post_to_pro_channel(msg)
                    forex_posted += 1
                    posted = True
                    logger.info(f"[MORNING PRO PKG] Forex {tier}: {pair_key} #{idea_id}")
                    time.sleep(3)
                    break  # One pair per tier
            except Exception as e:
                logger.error(f"[MORNING PRO PKG] Forex {tier} {pair_key}: {e}")
                continue
        if not posted:
            logger.info(f"[MORNING PRO PKG] Forex {tier}: no quality setup — skipped")

    # ── P2P Intelligence ──────────────────────────────────────────────────
    try:
        p2p_msg = build_morning_p2p_intelligence()
        if p2p_msg:
            post_to_pro_channel(p2p_msg)
            logger.info("[MORNING PRO PKG] P2P intelligence posted")
            time.sleep(2)
    except Exception as e:
        logger.error(f"[MORNING PRO PKG] P2P: {e}")

    # ── Summary ───────────────────────────────────────────────────────────
    total = crypto_posted + forex_posted
    logger.info(f"[MORNING PRO PKG] Complete — {crypto_posted} crypto + {forex_posted} forex setups posted")

    if total == 0:
        post_to_pro_channel(
            "⚡ <b>MORNING PRO PACKAGE</b>\n\n"
            "No quality setups across all tiers this morning.\n"
            "Market conditions don\'t support a strong entry right now.\n\n"
            "<i>This is the correct call. Protecting capital is part of the strategy.\n"
            "Next package at 7AM tomorrow.</i>\n"
            "⚡ Market Pulse Pro"
        )


def toggle_mirror_mode():
    """Toggle MIRROR_MODE and persist to DB."""
    global MIRROR_MODE
    MIRROR_MODE = not MIRROR_MODE
    try:
        cfg = load_admin_config()
        cfg["MIRROR_MODE"] = MIRROR_MODE
        save_admin_config(cfg)
        logger.info("[ADMIN] Mirror mode toggled to %s" % ("ON" if MIRROR_MODE else "OFF"))
    except Exception as e:
        logger.error("[MIRROR MODE TOGGLE] %s" % e)

def toggle_channel_enabled():
    """Toggle CHANNEL_ENABLED and persist to DB. Safe to call from any scope."""
    global CHANNEL_ENABLED
    CHANNEL_ENABLED = not CHANNEL_ENABLED
    try:
        cfg = load_admin_config()
        cfg["CHANNEL_ENABLED"] = CHANNEL_ENABLED
        save_admin_config(cfg)
        logger.info("[ADMIN] Channel toggled to %s" % ("ON" if CHANNEL_ENABLED else "OFF"))
    except Exception as e:
        logger.error("[TOGGLE CHANNEL] %s" % e)


