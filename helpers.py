"""Market Pulse Bot — helpers module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import USER_AGENTS, WAT_OFFSET, logger


# ─── extracted section ───
# 🛠 HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def format_price(v):
    if v is None:
        return "N/A"
    try:
        v = float(v)
    except Exception as _e:
        return "N/A"
    if v >= 1:
        return "$%.2f" % v
    elif v >= 0.01:
        return "$%.4f" % v
    elif v >= 0.0001:
        return "$%.6f" % v
    else:
        return "$%.8f" % v

def format_change(pct):
    if pct is None:
        return "N/A"
    try:
        pct = float(pct)
    except Exception as _e:
        return "N/A"
    sign = "+" if pct >= 0 else ""
    return "%s%.2f%%" % (sign, pct)

def format_ngn(v):
    """Format Nigerian Naira amount with ₦ symbol."""
    if v is None:
        return "N/A"
    try:
        v = float(v)
        return "₦{:,.0f}".format(v) if v >= 1 else "₦{:.4f}".format(v)
    except Exception:
        return str(v)

def format_forex(v, symbol=""):
    """Format a forex pair price with appropriate decimals."""
    if v is None:
        return "N/A"
    try:
        v = float(v)
        if v >= 100:
            return "{}{:,.2f}".format(symbol, v)
        elif v >= 1:
            return "{}{:.4f}".format(symbol, v)
        else:
            return "{}{:.6f}".format(symbol, v)
    except Exception:
        return str(v)

def format_large(v):
    if v is None:
        return "N/A"
    try:
        v = float(v)
    except Exception as _e:
        return "N/A"
    if v >= 1e12:
        return "$%.2fT" % (v / 1e12)
    if v >= 1e9:
        return "$%.1fB" % (v / 1e9)
    if v >= 1e6:
        return "$%.0fM" % (v / 1e6)
    return "$%.0f" % v

def wat_now():
    """Always derive WAT from UTC — server timezone independent."""
    return datetime.utcnow() + timedelta(hours=WAT_OFFSET)

def get_random_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
    }

def request_json(method, url, params=None, json_data=None, timeout=10, retries=3, backoff=1.5):
    last_exc = None
    for attempt in range(retries):
        try:
            if method == "GET":
                r = requests.get(url, params=params, timeout=timeout)
            else:
                r = requests.post(url, json=json_data, timeout=timeout)
            if r.status_code == 429:
                wait = 30 * (attempt + 1)
                logger.warning("[RATE LIMIT] waiting %ds" % wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                time.sleep(backoff * (attempt + 1))
    logger.error("[RETRY FAILED] %s" % last_exc)
    return None

def fetch_with_backoff(url, max_retries=5, timeout=15):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=get_random_headers(), timeout=timeout)
            if response.status_code == 429:
                wait = (2 ** attempt) * 2
                logger.warning("[BACKOFF] Waiting %ds" % wait)
                time.sleep(wait)
                continue
            if response.status_code == 200:
                return response.json()
        except Exception as _e:
            time.sleep(2 ** attempt)
    return None

# ═══════════════════════════════════════════════════════════════════════════
