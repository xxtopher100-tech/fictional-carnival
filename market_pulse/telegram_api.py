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
from market_pulse.config_runtime import (
    BOT_TOKEN, CHANNEL_ID, logger,
    get_channel_enabled, get_pro_channel_id, get_mirror_mode,
)
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
    if message_id is None:
        return send(chat_id, text, buttons)
    data = {"chat_id": chat_id, "message_id": message_id,
            "text": _safe_truncate(text), "parse_mode": "HTML"}
    if buttons:
        data["reply_markup"] = json.dumps({"inline_keyboard": buttons})
    result = tg("editMessageText", data)
    # Telegram 400 "message is not modified" is harmless
    if isinstance(result, dict) and not result.get("ok"):
        desc = (result.get("description") or "").lower()
        if "not modified" in desc:
            return result
        if "message to edit not found" in desc or "message can't be edited" in desc:
            return send(chat_id, text, buttons)
    return result
def delete_message(chat_id, message_id):
    tg("deleteMessage", {"chat_id": chat_id, "message_id": message_id})
def answer_cb(cb_id, text=None):
    """Acknowledge inline button. Safe if already answered or id missing."""
    if not cb_id:
        return {}
    payload = {"callback_query_id": cb_id}
    if text:
        payload["text"] = text
    try:
        # Single attempt — double-answer is common and not fatal
        r = requests.post(
            "https://api.telegram.org/bot%s/answerCallbackQuery" % BOT_TOKEN,
            json=payload,
            timeout=10,
        )
        if r.status_code >= 400:
            logger.debug("[ANSWER CB] %s %s", r.status_code, r.text[:120])
        return r.json() if r.content else {}
    except Exception as e:
        logger.debug("[ANSWER CB] %s", e)
        return {}
def post_to_channel(text):
    if not get_channel_enabled():
        logger.info("[FREE CHANNEL] Post skipped — CHANNEL_ENABLED is False")
        return None
    if not CHANNEL_ID:
        logger.warning("[FREE CHANNEL] CHANNEL_ID missing/empty — post skipped")
        return None
    data = {
        "chat_id": CHANNEL_ID,
        "text": _safe_truncate(text),
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    result = tg("sendMessage", data)
    if not result or not result.get("ok"):
        logger.error(
            "[FREE CHANNEL] Telegram rejected post: %s"
            % (result.get("description") if isinstance(result, dict) else result)
        )
    else:
        logger.info("[FREE CHANNEL] Posted ok (message_id=%s)"
                    % result.get("result", {}).get("message_id"))
    return result


def post_to_pro_channel(text):
    """Post to Pro channel. If MIRROR_MODE is on, also posts to free channel.

    Always logs the outcome so Railway shows exactly why a Pro post
    succeeded or was skipped (missing env, channel paused, Telegram error).
    """
    pro_channel_id = get_pro_channel_id()

    if not get_channel_enabled():
        logger.info("[PRO CHANNEL] Post skipped — CHANNEL_ENABLED is False")
        return None

    if not pro_channel_id:
        logger.warning(
            "[PRO CHANNEL] PRO_CHANNEL_ID missing/empty — post skipped. "
            "Set PRO_CHANNEL_ID in Railway Variables (or /setprochannel)."
        )
        return None

    if pro_channel_id in ("-100XXXXXXXXX", "None", "null"):
        logger.warning(
            "[PRO CHANNEL] PRO_CHANNEL_ID is still the placeholder (%s) — post skipped"
            % pro_channel_id
        )
        return None

    data = {
        "chat_id": pro_channel_id,
        "text": _safe_truncate(text),
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    result = tg("sendMessage", data)

    if not result:
        logger.error(
            "[PRO CHANNEL] Telegram API returned empty response for chat_id=%s"
            % pro_channel_id
        )
        return None

    if not result.get("ok"):
        # Common causes: bot not admin in Pro channel, wrong ID, chat not found
        desc = result.get("description") or result
        err_code = result.get("error_code")
        logger.error(
            "[PRO CHANNEL] Telegram rejected post (error_code=%s): %s | chat_id=%s"
            % (err_code, desc, pro_channel_id)
        )
        return result

    msg_id = result.get("result", {}).get("message_id")
    logger.info(
        "[PRO CHANNEL] Posted ok (message_id=%s, chat_id=%s)"
        % (msg_id, pro_channel_id)
    )

    # Mirror mode: same message goes to free channel too
    if get_mirror_mode() and get_channel_enabled() and CHANNEL_ID:
        try:
            mirror_data = {
                "chat_id": CHANNEL_ID,
                "text": _safe_truncate(text),
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            mirror_result = tg("sendMessage", mirror_data)
            if mirror_result and mirror_result.get("ok"):
                logger.info("[MIRROR] Post mirrored to free channel")
            else:
                logger.error(
                    "[MIRROR] Free-channel mirror failed: %s"
                    % (mirror_result.get("description") if isinstance(mirror_result, dict) else mirror_result)
                )
        except Exception as e:
            logger.error("[MIRROR] Failed: %s" % e)
    return result
# ═══════════════════════════════════════════════════════════════════════════
