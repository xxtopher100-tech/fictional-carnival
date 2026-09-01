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
# INFO → stdout so Railway does not treat normal logs as errors (stderr).
import sys as _sys
_handlers = [
    RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5),
    logging.StreamHandler(_sys.stdout),
]
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=_handlers,
    force=True,
)
logger = logging.getLogger(__name__)
# Keep WARNING+ on stderr for real problems
_err = logging.StreamHandler(_sys.stderr)
_err.setLevel(logging.WARNING)
logger.addHandler(_err)

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
# Independent live shadow verifier (does not alter outcome_monitor)
SHADOW_VERIFY_ENABLED = os.environ.get("SHADOW_VERIFY_ENABLED", "false").lower() in ("1", "true", "yes")
SHADOW_VERIFY_PRIVATE_ONLY = os.environ.get("SHADOW_VERIFY_PRIVATE_ONLY", "true").lower() in ("1", "true", "yes")

CHANNEL_ID = os.environ.get("CHANNEL_ID", "")
PRO_CHANNEL_ID = os.environ.get("PRO_CHANNEL_ID", "")
CHANNEL_ENABLED = True
MIRROR_MODE = False  # When True: Pro channel content also posts to free channel
WAT_OFFSET = 1
DATABASE_URL = os.environ.get("DATABASE_URL", "")


def validate_critical_config() -> list:
    """Return list of missing/invalid critical env vars. Empty = OK to start."""
    missing = []
    tok = (BOT_TOKEN or "").strip()
    if not tok or tok.startswith("YOUR_") or "TOKEN_HERE" in tok:
        missing.append("BOT_TOKEN")
    db = (DATABASE_URL or "").strip()
    if not db or not (db.startswith("postgres://") or db.startswith("postgresql://")):
        missing.append("DATABASE_URL")
    return missing


def config_status_summary() -> dict:
    """Safe non-secret status for admin diagnostics (no secret values)."""
    return {
        "bot_token_set": bool(BOT_TOKEN) and not str(BOT_TOKEN).startswith("YOUR_"),
        "database_url_set": bool(DATABASE_URL) and (
            DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://")
        ),
        "admin_ids_count": len(ADMIN_IDS),
        "channel_id_set": bool(CHANNEL_ID),
        "pro_channel_id_set": bool(PRO_CHANNEL_ID),
        "shadow_verify": SHADOW_VERIFY_ENABLED,
        "ai_keys_configured": sum(
            1
            for k in (DEEPSEEK_KEY, MISTRAL_KEY, QWEN_KEY)
            if k and not str(k).startswith("YOUR_")
        ),
    }

# ═══════════════════════════════════════════════════════════════════════════
# 🔁 SHARED CHANNEL / MIRROR STATE ACCESSORS
# ═══════════════════════════════════════════════════════════════════════════
# CHANNEL_ENABLED, PRO_CHANNEL_ID, and MIRROR_MODE above are admin-mutable
# at runtime (toggled by /togglechannel, /setprochannel, mirror-mode toggle).
# telegram_api.py, morning_package.py, and handlers.py all read/write them
# through these functions — same get_/set_ pattern already used for BOT_MODE
# in pro_system.py — so there is exactly one copy of this state, not a
# separate disconnected copy per module.

def get_channel_enabled():
    return CHANNEL_ENABLED

def set_channel_enabled(value):
    global CHANNEL_ENABLED
    CHANNEL_ENABLED = value

def get_pro_channel_id():
    return PRO_CHANNEL_ID

def set_pro_channel_id(value):
    global PRO_CHANNEL_ID
    PRO_CHANNEL_ID = value

def get_mirror_mode():
    return MIRROR_MODE

def set_mirror_mode(value):
    global MIRROR_MODE
    MIRROR_MODE = value

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
        "MIRROR_MODE": MIRROR_MODE,
    }
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT key, value FROM admin_settings WHERE key IN ('PRO_CHANNEL_ID','CHANNEL_ENABLED','BOT_MODE','MIRROR_MODE')")
        rows = c.fetchall()
        for key, value in rows:
            if key in ("CHANNEL_ENABLED", "MIRROR_MODE"):
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
