"""Market Pulse Bot — trade_scanner module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import logger
from market_pulse.db import get_db
from market_pulse.edge_trade_engine import _gather_trade_analytics, _tier_conditions_met, generate_trade_idea
from market_pulse.fear_greed import get_fear_greed
from market_pulse.forex_trade_engine import generate_forex_trade_idea, get_forex_rate
from market_pulse.helpers import wat_now
from market_pulse.price_fetchers import get_best_price
from market_pulse.telegram_api import post_to_pro_channel


# ─── extracted section ───
# 🤖 AUTOMATED TRADE SCANNER
# ═══════════════════════════════════════════════════════════════════════════
# Runs every 4 hours. Pre-screens all coins + forex pairs.
# Picks the single best setup. Posts to Pro channel.
# Max 1 post per 4-hour window. Max 3 per day. DB-backed cooldown.
# ═══════════════════════════════════════════════════════════════════════════

# Coins the scanner will check (most liquid, best AI setups)
SCANNER_CRYPTO_COINS = ["BTC", "ETH", "SOL", "BNB", "XRP", "AVAX", "LINK", "DOGE"]
SCANNER_FOREX_PAIRS  = ["USDT/NGN", "USD/NGN", "BTC/NGN", "EUR/USD", "GBP/USD"]

# Tier priority — Edge checked first as highest value for Pro subscribers
SCANNER_TIER_ORDER   = ["edge", "momentum", "steady"]

_scanner_daily_count = {"date": None, "count": 0}


def _scanner_get_cooldown():
    """Return True if scanner posted in last 4 hours."""
    db = None
    try:
        db = get_db()
        c  = db.cursor()
        since = (wat_now() - timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "SELECT updated_at FROM admin_settings WHERE key='auto_scanner_last' AND updated_at >= %s",
            (since,)
        )
        return c.fetchone() is not None
    except Exception as e:
        logger.warning(f"[SCANNER CD] {e}")
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass


def _scanner_set_cooldown():
    """Record that scanner just posted."""
    db = None
    try:
        db = get_db()
        c  = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES ('auto_scanner_last',%s,%s) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (now, now)
        )
        db.commit()
    except Exception as e:
        logger.warning(f"[SCANNER CD SET] {e}")
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass


def run_trade_scanner():
    """
    Automated trade scanner. Called every 4 hours by the scheduler.

    Strategy:
    1. Check 4-hour cooldown — skip if already posted recently
    2. Check daily max (3 per day) — skip if hit
    3. Pre-screen all crypto coins for each tier using analytics
    4. Pre-screen all forex pairs
    5. Pick the single best candidate (Edge > Momentum > Steady)
    6. Generate one AI trade idea for that candidate
    7. Post to Pro channel if valid
    8. Set cooldown
    """
    global _scanner_daily_count

    # Daily count reset
    today = wat_now().strftime("%Y-%m-%d")
    if _scanner_daily_count["date"] != today:
        _scanner_daily_count = {"date": today, "count": 0}

    if _scanner_daily_count["count"] >= 3:
        logger.info("[SCANNER] Daily max (3) reached — skipping")
        return

    if _scanner_get_cooldown():
        logger.info("[SCANNER] 4-hour cooldown active — skipping")
        return

    logger.info("[SCANNER] Starting automated trade scan...")

    fg_data = get_fear_greed()
    fg_val  = fg_data[0]["value"] if fg_data else "50"

    # ── Phase 1: Pre-screen crypto coins ──────────────────────────────────
    # No AI calls here — only analytics check. Fast.
    candidates = []  # (priority, type, identifier, tier)

    tier_priority = {"edge": 0, "momentum": 1, "steady": 2}

    for tier in SCANNER_TIER_ORDER:
        for coin in SCANNER_CRYPTO_COINS:
            try:
                price, _ = get_best_price(coin)
                if not price:
                    continue
                analytics = _gather_trade_analytics(coin, price)
                ok, reason = _tier_conditions_met(tier, analytics, fg_val)
                if ok:
                    candidates.append((tier_priority[tier], "crypto", coin, tier))
                    logger.info(f"[SCANNER] {coin} {tier} passed pre-screen")
            except Exception as e:
                logger.warning(f"[SCANNER] {coin} {tier} error: {e}")

    # ── Phase 2: Pre-screen forex pairs ───────────────────────────────────
    for tier in SCANNER_TIER_ORDER:
        for pair_key in SCANNER_FOREX_PAIRS:
            try:
                rate, _, _, _ = get_forex_rate(pair_key)
                if not rate:
                    continue
                # Forex uses simplified pre-screening — just F&G check
                fg = int(fg_val) if str(fg_val).isdigit() else 50
                if tier == "edge" and not (fg > 70 or fg < 30):
                    continue  # Edge needs extreme sentiment
                candidates.append((tier_priority[tier], "forex", pair_key, tier))
                logger.info(f"[SCANNER] {pair_key} {tier} passed pre-screen")
            except Exception as e:
                logger.warning(f"[SCANNER] {pair_key} {tier} error: {e}")

    if not candidates:
        logger.info("[SCANNER] No candidates passed pre-screening — no post today")
        return

    # ── Phase 3: Pick best candidate and generate ONE AI call ─────────────
    # Sort by priority (Edge=0 first), then crypto before forex for reliability
    candidates.sort(key=lambda x: (x[0], 0 if x[1]=="crypto" else 1))

    for priority, asset_type, identifier, tier in candidates:
        try:
            logger.info(f"[SCANNER] Generating {tier} idea for {identifier} ({asset_type})")

            if asset_type == "crypto":
                msg, trade, idea_id = generate_trade_idea(identifier, tier)
            else:
                msg, trade, idea_id = generate_forex_trade_idea(identifier, tier)

            if msg and idea_id:
                post_to_pro_channel(msg)
                _scanner_set_cooldown()
                _scanner_daily_count["count"] += 1
                logger.info(f"[SCANNER] ✅ Posted #{idea_id} — {identifier} {tier} ({asset_type}) | Daily: {_scanner_daily_count['count']}/3")
                return  # One post per scan — stop here
            else:
                logger.info(f"[SCANNER] {identifier} {tier} — AI found no quality setup, trying next candidate")

        except Exception as e:
            logger.error(f"[SCANNER] {identifier} {tier}: {e}")
            continue

    logger.info("[SCANNER] All candidates tried — no valid setup generated this cycle")



# ═══════════════════════════════════════════════════════════════════════════
