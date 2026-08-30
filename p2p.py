"""Market Pulse Bot — p2p module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import USER_AGENTS, logger
from market_pulse.price_fetchers import get_best_price, get_fiat_rates


# ─── extracted section ───
# 🇳🇬 P2P SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

def _p2p_median(prices):
    if not prices:
        return None
    prices.sort()
    return prices[len(prices) // 2]

def get_binance_p2p(side, asset, fiat_code):
    try:
        headers = {
            "Content-Type": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json",
            "Referer": "https://p2p.binance.com/",
            "Origin": "https://p2p.binance.com"
        }
        payload = {
            "asset": asset,
            "fiat": fiat_code,
            "merchantCheck": False,
            "page": 1,
            "publisherType": None,
            "rows": 10,
            "tradeType": side
        }
        resp = requests.post(
            "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search",
            json=payload,
            headers=headers,
            timeout=15
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        ads = data.get("data") or []
        prices = []
        for a in ads:
            try:
                adv = a.get("adv", {})
                price = adv.get("price")
                if price:
                    prices.append(float(price))
            except Exception as _e:
                continue
        if not prices:
            return None
        return _p2p_median(prices)
    except Exception as e:
        logger.error("[BINANCE P2P ERROR] %s" % e)
        return None

def get_bybit_p2p(side, asset, fiat_code):
    try:
        bybit_side = "1" if side == "BUY" else "0"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": random.choice(USER_AGENTS)
        }
        resp = requests.post(
            "https://api2.bybit.com/fiat/otc/item/list",
            json={"userId": "", "tokenId": asset, "currencyId": fiat_code,
                  "payment": [], "side": bybit_side, "size": "10", "page": "1",
                  "amount": "", "authMaker": False, "canTrade": False},
            headers=headers,
            timeout=10
        )
        if resp.status_code != 200:
            return None
        items = resp.json().get("result", {}).get("items") or []
        prices = [float(i["price"]) for i in items if i.get("price")]
        return _p2p_median(prices)
    except Exception as e:
        logger.error("[BYBIT P2P ERROR] %s" % e)
        return None

def get_p2p_rate(crypto, fiat):
    try:
        buy = get_binance_p2p("BUY", crypto, fiat)
        sell = get_binance_p2p("SELL", crypto, fiat)
        if buy and sell:
            return buy, sell, "Binance P2P"
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)
    
    try:
        buy = get_bybit_p2p("BUY", crypto, fiat)
        sell = get_bybit_p2p("SELL", crypto, fiat)
        if buy and sell:
            return buy, sell, "Bybit P2P"
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)
    
    try:
        rates = get_fiat_rates()
        price, _ = get_best_price(crypto)
        fiat_per_usd = rates.get(fiat)
        if price and fiat_per_usd:
            val = price * fiat_per_usd
            buy = round(val * 1.015, 2)
            sell = round(val * 0.985, 2)
            return buy, sell, "Estimated ⚠️"
    except Exception as e:
        logger.warning("[P2P ESTIMATE] %s" % e)

    return None, None, None

# ═══════════════════════════════════════════════════════════════════════════
