"""Market Pulse Bot — config_runtime module (split from the real monolithic bot.py)."""

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



# ─── extracted section ───
# 📋 LOGGING CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

LOG_FILE = "bot.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# 🔑 TOKEN CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
DEEPSEEK_KEY = os.environ.get("DEEPSEEK_KEY", "YOUR_DEEPSEEK_KEY_HERE")
MISTRAL_KEY = os.environ.get("MISTRAL_KEY", "YOUR_MISTRAL_KEY_HERE")
QWEN_KEY = os.environ.get("QWEN_KEY", "YOUR_QWEN_KEY_HERE")

# ═══════════════════════════════════════════════════════════════════════════
# 🔐 PRIVACY & CHANNEL CONFIG
# ═══════════════════════════════════════════════════════════════════════════

# Load ADMIN_IDS from env var — comma-separated Telegram user IDs
# e.g. ADMIN_IDS=123456789,987654321
# No hardcoded fallback: if this isn't set, ADMIN_IDS is empty and admin
# commands simply won't work until it's set — safer than silently
# defaulting to someone else's real Telegram ID from earlier testing.
_admin_ids_env = os.environ.get("ADMIN_IDS", "")
ADMIN_IDS = {int(x.strip()) for x in _admin_ids_env.split(",") if x.strip().isdigit()}

ADMIN_CODE = os.environ.get("ADMIN_CODE", "")
CHANNEL_ID = os.environ.get("CHANNEL_ID", "")
PRO_CHANNEL_ID = os.environ.get("PRO_CHANNEL_ID", "")
CHANNEL_ENABLED = True
MIRROR_MODE = False  # When True: Pro channel content also posts to free channel
WAT_OFFSET = 1
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ═══════════════════════════════════════════════════════════════════════════
# 📋 GLOBAL BOT MODE
# ═══════════════════════════════════════════════════════════════════════════

BOT_MODE = "everyone"  # "everyone" or "pro"

# ═══════════════════════════════════════════════════════════════════════════
# 🗄️ ADMIN CONFIG PERSISTENCE
# ═══════════════════════════════════════════════════════════════════════════

def load_admin_config():
    from market_pulse.db import get_db
    """Load admin config from DB (primary) — Railway filesystem is ephemeral so no file storage."""
    defaults = {
        "PRO_CHANNEL_ID": PRO_CHANNEL_ID,
        "CHANNEL_ENABLED": CHANNEL_ENABLED,
        "BOT_MODE": BOT_MODE,
    }
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT key, value FROM admin_settings WHERE key IN ('PRO_CHANNEL_ID','CHANNEL_ENABLED','BOT_MODE')")
        rows = c.fetchall()
        for key, value in rows:
            if key == "CHANNEL_ENABLED":
                defaults[key] = value.lower() in ("true", "1", "yes")
            else:
                defaults[key] = value
        return defaults
    except Exception as e:
        logger.warning("[CONFIG LOAD] DB not ready yet, using defaults: %s" % e)
        return defaults
    finally:
        if db:
            try: db.close()
            except Exception: pass

def save_admin_config(config):
    from market_pulse.helpers import wat_now
    from market_pulse.db import get_db
    """Save admin config to DB — survives Railway restarts and redeploys."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        for key, value in config.items():
            c.execute(
                "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (key, str(value), now)
            )
        db.commit()
        logger.info("[CONFIG] Saved admin config to DB")
    except Exception as e:
        logger.error("[CONFIG ERROR] %s" % e)
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════
# 📋 SCHEDULE
# ═══════════════════════════════════════════════════════════════════════════

SCHEDULE = {
    "morning_hour_wat": 7,
    "midday_hour_wat": 13,
    "evening_hour_wat": 19,
    "weekly_edge_day": 5,
    "weekly_edge_hour": 7,
    "bigmove_pct": 3.0,
    "admin_digest_hour_wat": 8,
    "health_check_interval_minutes": 10,
    "expiry_reminder_days": 7,
}

# ═══════════════════════════════════════════════════════════════════════════
# 🪙 COINS & P2P CONFIG
# ═══════════════════════════════════════════════════════════════════════════

COINS = {
    "BTC": ("XBTUSD", "bitcoin"),
    "ETH": ("ETHUSD", "ethereum"),
    "SOL": ("SOLUSD", "solana"),
    "BNB": (None, "binancecoin"),
    "XRP": ("XRPUSD", "ripple"),
    "DOGE": ("DOGEUSD", "dogecoin"),
    "ADA": ("ADAUSD", "cardano"),
    "TRX": ("TRXUSD", "tron"),
    "AVAX": ("AVAXUSD", "avalanche-2"),
    "LINK": ("LINKUSD", "chainlink"),
    "DOT": ("DOTUSD", "polkadot"),
    "POL": ("POLUSD", "polygon-ecosystem-token"),
    "LTC": ("LTCUSD", "litecoin"),
    "UNI": ("UNIUSD", "uniswap"),
    "ATOM": ("ATOMUSD", "cosmos"),
    "NEAR": ("NEARUSD", "near"),
    "ICP": ("ICPUSD", "internet-computer"),
    "SHIB": (None, "shiba-inu"),
    "APT": (None, "aptos"),
    "ARB": (None, "arbitrum"),
    "OP": (None, "optimism"),
    "SUI": (None, "sui"),
    "INJ": (None, "injective-protocol"),
    "FET": (None, "fetch-ai"),
    "FIL": ("FILUSD", "filecoin"),
    "RENDER": (None, "render-token"),
    "WLD": (None, "worldcoin-wld"),
    "TON": (None, "the-open-network"),
    "USDT": (None, "tether"),
    "USDC": ("USDCUSD", "usd-coin"),
}

def kraken_pair(coin): return COINS[coin][0]
def coin_key(coin): return COINS[coin][1]

P2P_CRYPTOS = ["USDT", "BTC", "ETH", "BNB", "USDC", "SOL", "XRP"]
P2P_FIATS = {
    "NGN": ("Nigerian Naira", "₦"),
    "GHS": ("Ghanaian Cedi", "GHc"),
    "KES": ("Kenyan Shilling", "KSh"),
    "ZAR": ("South African Rand", "R"),
    "UGX": ("Ugandan Shilling", "USh"),
    "TZS": ("Tanzanian Shilling", "TSh"),
    "EGP": ("Egyptian Pound", "E£"),
    "MAD": ("Moroccan Dirham", "MAD"),
    "XOF": ("West African CFA", "CFA"),
    "USD": ("US Dollar", "$"),
    "GBP": ("British Pound", "£"),
    "EUR": ("Euro", "€"),
    "AED": ("UAE Dirham", "AED"),
    "CNY": ("Chinese Yuan", "¥"),
    "INR": ("Indian Rupee", "₹"),
}

TIMEFRAMES = {
    "1H": (1, 12, "hm"),
    "6H": (6, 36, "hm"),
    "1D": (24, 48, "hm"),
    "3D": (72, 36, "dhm"),
    "1W": (168, 42, "dhm"),
    "1M": (720, 30, "date"),
    "3M": (2160, 30, "date"),
    "1Y": (8760, 52, "date"),
}

NEWS_RSS_FEEDS = [
    ("Bitcoin Magazine", "https://bitcoinmagazine.com/.rss/full/"),
    ("CryptoSlate", "https://cryptoslate.com/feed/"),
    ("Decrypt", "https://decrypt.co/feed"),
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("The Block", "https://www.theblock.co/rss.xml"),
    ("CoinTelegraph", "https://cointelegraph.com/rss"),
    ("NewsBTC", "https://www.newsbtc.com/feed/"),
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]

# ═══════════════════════════════════════════════════════════════════════════
