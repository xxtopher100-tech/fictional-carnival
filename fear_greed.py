"""Market Pulse Bot — fear_greed module (split from the real monolithic bot.py)."""

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
from market_pulse.helpers import fetch_with_backoff
from market_pulse.news import get_crypto_news


# ─── extracted section ───
# 🧠 FEAR & GREED
# ═══════════════════════════════════════════════════════════════════════════

_fg_cache = {"data": None, "timestamp": None}

def get_latest_news(limit=5):
    """Alias for get_crypto_news with limit support."""
    news = get_crypto_news()
    if news and limit:
        return news[:limit]
    return news or []


def get_fear_greed():
    global _fg_cache
    now = datetime.now()
    if (_fg_cache["timestamp"] and (now - _fg_cache["timestamp"]).total_seconds() < 21600):  # 6hr cache — F&G only updates once per day
        return _fg_cache["data"]
    try:
        resp = fetch_with_backoff("https://api.alternative.me/fng/?limit=7")
        if resp and resp.get("data"):
            _fg_cache["data"] = resp["data"]
            _fg_cache["timestamp"] = now
            return resp["data"]
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)
    return _fg_cache["data"]

def fg_emoji(value):
    try:
        v = int(value)
    except (ValueError, TypeError):
        return "😐"
    if v <= 24: return "😱"
    elif v <= 44: return "😰"
    elif v <= 54: return "😐"
    elif v <= 74: return "😊"
    else: return "🤑"

# ═══════════════════════════════════════════════════════════════════════════
