"""Market Pulse Bot — channel_lock module (split from the real monolithic bot.py)."""

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
from market_pulse.telegram_api import send, tg


# ─── extracted section ───
# 🔐 CHANNEL LOCK SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

# Cache channel membership — avoids hitting Telegram API on every message/callback
_channel_cache = {}  # chat_id -> (is_member: bool, expires_at: float)
_CHANNEL_CACHE_TTL = 300  # 5 minutes

def is_user_in_channel(chat_id, force=False):
    global CHANNEL_ID
    now_ts = time.time()
    if not force:
        cached = _channel_cache.get(chat_id)
        if cached and now_ts < cached[1]:
            return cached[0]
    try:
        result = tg("getChatMember", {"chat_id": CHANNEL_ID, "user_id": chat_id})
        if result and result.get("ok"):
            status = result.get("result", {}).get("status", "")
            member = status in ["member", "administrator", "creator"]
            _channel_cache[chat_id] = (member, now_ts + _CHANNEL_CACHE_TTL)
            return member
    except Exception as e:
        logger.warning("[CHANNEL CHECK] %s" % e)
    # On error, return cached value if available (fail open), else False
    cached = _channel_cache.get(chat_id)
    return cached[0] if cached else False

def check_channel_membership(chat_id):
    if is_user_in_channel(chat_id):
        return True
    
    send(chat_id,
         "🔒 <b>Channel Membership Required</b>\n\n"
         "To use Market Pulse, you must join our channel first.\n\n"
         "👉 @marketpulseng\n\n"
         "After joining, tap the button below to verify.",
         [[{"text": "✅ I've Joined", "callback_data": "verify_join"}]])
    return False

# ═══════════════════════════════════════════════════════════════════════════
