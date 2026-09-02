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

from market_pulse.config_runtime import (
    load_admin_config, logger, save_admin_config,
    get_channel_enabled, set_channel_enabled,
    get_mirror_mode, set_mirror_mode,
)
from market_pulse.edge_trade_engine import _gather_trade_analytics, _tier_conditions_met, generate_trade_idea
from market_pulse.fear_greed import fg_emoji, get_fear_greed
from market_pulse.forex_trade_engine import generate_forex_trade_idea, get_forex_rate
from market_pulse.helpers import wat_now
from market_pulse.p2p import get_p2p_rate, format_multi_p2p_intelligence, P2P_ASSETS
from market_pulse.price_fetchers import get_best_price
from market_pulse.telegram_api import post_to_pro_channel
from market_pulse.publication_gate import publish_canonical_trade


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
MORNING_FOREX_PAIRS  = ["EUR/USD", "GBP/USD"]  # tradeable FX only — NGN is P2P context, not trade pairs


def build_morning_p2p_intelligence():
    """Multi-asset P2P morning read (USDT / EUR / GBP vs NGN)."""
    try:
        return format_multi_p2p_intelligence(
            assets=list(P2P_ASSETS),
            title="P2P INTELLIGENCE — MORNING READ",
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
    5. Forex Steady (EUR/USD or GBP/USD)
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
    crypto_tier_status = {}  # tier -> "COIN #id" or "skipped"
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
                    tr = trade or {}
                    ok_pub, code = publish_canonical_trade(
                        msg=msg,
                        idea_id=int(idea_id),
                        symbol=coin,
                        direction=tr.get("direction") or "",
                        timeframe=tr.get("timeframe") or "",
                        entry=tr.get("entry"),
                        stop=tr.get("stop"),
                        target1=tr.get("target1"),
                        market_type="crypto",
                        tier=tier,
                        source="morning_package",
                    )
                    if not ok_pub:
                        logger.info("[MORNING PRO PKG] GATE %s %s %s", code, coin, tier)
                        continue
                    crypto_posted += 1
                    posted = True
                    crypto_tier_status[tier] = f"{coin} #{idea_id}"
                    logger.info(f"[MORNING PRO PKG] Crypto {tier}: {coin} #{idea_id}")
                    time.sleep(3)
                    break  # One coin per tier
            except Exception as e:
                logger.error(f"[MORNING PRO PKG] Crypto {tier} {coin}: {e}")
                continue
        if not posted:
            crypto_tier_status[tier] = "skipped"
            logger.info(f"[MORNING PRO PKG] Crypto {tier}: no quality setup found — skipped")

    # ── Forex Setups ──────────────────────────────────────────────────────
    forex_posted = 0
    forex_tier_status = {}
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
                    tr = trade or {}
                    ok_pub, code = publish_canonical_trade(
                        msg=msg,
                        idea_id=int(idea_id),
                        symbol=pair_key,
                        direction=tr.get("direction") or "",
                        timeframe=tr.get("timeframe") or "",
                        entry=tr.get("entry"),
                        stop=tr.get("stop"),
                        target1=tr.get("target1"),
                        market_type="forex",
                        tier=tier,
                        source="morning_package",
                    )
                    if not ok_pub:
                        logger.info("[MORNING PRO PKG] GATE %s %s %s", code, pair_key, tier)
                        continue
                    forex_posted += 1
                    posted = True
                    forex_tier_status[tier] = f"{pair_key} #{idea_id}"
                    logger.info(f"[MORNING PRO PKG] Forex {tier}: {pair_key} #{idea_id}")
                    time.sleep(3)
                    break  # One pair per tier
            except Exception as e:
                logger.error(f"[MORNING PRO PKG] Forex {tier} {pair_key}: {e}")
                continue
        if not posted:
            forex_tier_status[tier] = "skipped"
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

    # ── Summary (always posted — skipped tiers are visible, not silent) ───
    total = crypto_posted + forex_posted
    logger.info(f"[MORNING PRO PKG] Complete — {crypto_posted} crypto + {forex_posted} forex setups posted")

    def _tier_line(status_map):
        parts = []
        for t in ("steady", "momentum", "edge"):
            st = status_map.get(t, "skipped")
            if st == "skipped":
                parts.append(f"{t.capitalize()}: skipped")
            else:
                parts.append(f"{t.capitalize()}: {st} ✓")
        return " · ".join(parts)

    summary = (
        "📋 <b>MORNING PACKAGE SUMMARY</b>\n\n"
        f"<b>Crypto</b>\n{_tier_line(crypto_tier_status)}\n\n"
        f"<b>Forex</b>\n{_tier_line(forex_tier_status)}\n\n"
    )
    if total == 0:
        summary += (
            "No quality setups across all tiers this morning.\n"
            "Sitting out is the correct call when conditions are weak.\n\n"
        )
    summary += (
        "<i>Next full package at 7AM WAT tomorrow.</i>\n"
        "⚡ Market Pulse Pro"
    )
    try:
        post_to_pro_channel(summary)
    except Exception as e:
        logger.error(f"[MORNING PRO PKG] Summary post failed: {e}")



def toggle_mirror_mode():
    """Toggle MIRROR_MODE and persist to DB."""
    new_value = not get_mirror_mode()
    set_mirror_mode(new_value)
    try:
        cfg = load_admin_config()
        cfg["MIRROR_MODE"] = new_value
        save_admin_config(cfg)
        logger.info("[ADMIN] Mirror mode toggled to %s" % ("ON" if new_value else "OFF"))
    except Exception as e:
        logger.error("[MIRROR MODE TOGGLE] %s" % e)

def toggle_channel_enabled():
    """Toggle CHANNEL_ENABLED and persist to DB. Safe to call from any scope."""
    new_value = not get_channel_enabled()
    set_channel_enabled(new_value)
    try:
        cfg = load_admin_config()
        cfg["CHANNEL_ENABLED"] = new_value
        save_admin_config(cfg)
        logger.info("[ADMIN] Channel toggled to %s" % ("ON" if new_value else "OFF"))
    except Exception as e:
        logger.error("[TOGGLE CHANNEL] %s" % e)
