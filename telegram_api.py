"""Market Pulse Bot — telegram_api module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import BOT_TOKEN, logger
from market_pulse.helpers import request_json


# ─── extracted section ───
# 📨 TELEGRAM HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def tg(method, data):
    return request_json(
        "POST", "https://api.telegram.org/bot%s/%s" % (BOT_TOKEN, method),
        json_data=data, timeout=15, retries=2
    ) or {}

_TG_MAX_LEN = 4096

def _safe_truncate(text, max_len=_TG_MAX_LEN):
    """Truncate text to Telegram safe length, preserving HTML validity best-effort."""
    if len(text) <= max_len:
        return text
    return text[:max_len - 30] + "\n<i>[message truncated]</i>"

def send(chat_id, text, buttons=None):
    data = {"chat_id": chat_id, "text": _safe_truncate(text), "parse_mode": "HTML"}
    if buttons:
        data["reply_markup"] = json.dumps({"inline_keyboard": buttons})
    return tg("sendMessage", data)

def edit(chat_id, message_id, text, buttons=None):
    data = {"chat_id": chat_id, "message_id": message_id,
            "text": _safe_truncate(text), "parse_mode": "HTML"}
    if buttons:
        data["reply_markup"] = json.dumps({"inline_keyboard": buttons})
    return tg("editMessageText", data)

def delete_message(chat_id, message_id):
    tg("deleteMessage", {"chat_id": chat_id, "message_id": message_id})

def answer_cb(cb_id, text=None):
    payload = {"callback_query_id": cb_id}
    if text:
        payload["text"] = text
    tg("answerCallbackQuery", payload)

def post_to_channel(text):
    global CHANNEL_ENABLED, CHANNEL_ID
    if not CHANNEL_ENABLED:
        return
    data = {
        "chat_id": CHANNEL_ID,
        "text": _safe_truncate(text),
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    return tg("sendMessage", data)

def post_to_pro_channel(text):
    """Post to Pro channel. If MIRROR_MODE is on, also posts to free channel."""
    global CHANNEL_ENABLED, PRO_CHANNEL_ID, MIRROR_MODE
    if not CHANNEL_ENABLED or not PRO_CHANNEL_ID:
        return
    if PRO_CHANNEL_ID == "-100XXXXXXXXX":
        return
    data = {
        "chat_id": PRO_CHANNEL_ID,
        "text": _safe_truncate(text),
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    result = tg("sendMessage", data)
    # Mirror mode: same message goes to free channel too
    if MIRROR_MODE and CHANNEL_ENABLED and CHANNEL_ID:
        try:
            mirror_data = {
                "chat_id": CHANNEL_ID,
                "text": _safe_truncate(text),
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            tg("sendMessage", mirror_data)
            logger.info("[MIRROR] Post mirrored to free channel")
        except Exception as e:
            logger.error("[MIRROR] Failed: %s" % e)
    return result

# ═══════════════════════════════════════════════════════════════════════════
