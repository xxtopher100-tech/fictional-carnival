"""Market Pulse Bot — users module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import ADMIN_IDS, logger
from market_pulse.db import get_db
from market_pulse.helpers import wat_now
from market_pulse.pro_system import get_bot_mode, is_pro, grant_pro_days


# ─── extracted section ───
# 📊 FEATURE TRACKING & USER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def track_feature(chat_id, feature):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO feature_usage (chat, feature, timestamp) VALUES (%s, %s, %s)",
                  (str(chat_id), feature, now))
        db.commit()
    except Exception as e:
        logger.warning("[TRACK FEATURE] %s" % e)
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass

def get_ai_usage_today(chat_id):
    """Return how many AI questions this user has asked today (WAT timezone)."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        today_wat = wat_now().strftime("%Y-%m-%d")
        c.execute(
            "SELECT COUNT(*) FROM feature_usage WHERE chat=%s AND feature='ai_question' AND timestamp LIKE %s",
            (str(chat_id), today_wat + "%")
        )
        return c.fetchone()[0]
    except Exception as _e:
        return 0
    finally:
        if db:
            try: db.close()
            except Exception: pass

FREE_AI_LIMIT = 5
UPGRADE_BTN = [[{"text": "💎 Upgrade to Pro", "callback_data": "upgrade"}, {"text": "🏠 Main Menu", "callback_data": "main_menu"}]]

def check_ai_limit(chat_id):
    """Returns (allowed, used, limit). Admins/Pro/everyone-mode always allowed."""
    if get_bot_mode() == "everyone" or chat_id in ADMIN_IDS or is_pro(chat_id):
        return True, 0, 999
    used = get_ai_usage_today(chat_id)
    return used < FREE_AI_LIMIT, used, FREE_AI_LIMIT

def ai_limit_msg(used, limit):
    return (
        f"⛔ <b>Daily AI Limit Reached</b>\n\n"
        f"You've used <b>{used}/{limit}</b> free AI questions today.\n\n"
        f"✨ Upgrade to Pro for <b>unlimited</b> AI questions, "
        f"market outlooks, trade setups and more.\n\n"
        f"<i>Your limit resets at midnight WAT.</i>"
    )

def upsert_user(chat_id, username=None, first_name=None):
    """Create or refresh user. Brand-new users get a 7-day Pro welcome trial once."""
    db = None
    is_new = False
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("SELECT chat FROM users WHERE chat=%s", (str(chat_id),))
        is_new = c.fetchone() is None
        c.execute(
            """INSERT INTO users (chat, username, first_name, first_seen, last_seen)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT(chat) DO UPDATE SET
                 username=excluded.username,
                 first_name=excluded.first_name,
                 last_seen=excluded.last_seen""",
            (str(chat_id), username or "", first_name or "", now, now)
        )
        db.commit()
    except Exception as e:
        logger.error("[UPSERT USER ERROR] %s" % e)
        if db:
            try: db.rollback()
            except Exception: pass
        is_new = False
    finally:
        if db:
            try: db.close()
            except Exception: pass

    if is_new:
        try:
            ok = grant_pro_days(chat_id, days=7, source="welcome_trial")
            if ok:
                logger.info("[PRO TRIAL] New user %s — 7-day Pro activated", chat_id)
            return True  # signal: new user + trial attempted/applied
        except Exception as e:
            logger.warning("[PRO TRIAL] Failed for %s: %s", chat_id, e)
    return is_new

def log_event(chat_id, action):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO events (chat, action, timestamp) VALUES (%s, %s, %s)",
                  (str(chat_id), action, now))
        db.commit()
    except Exception as e:
        logger.warning("[LOG EVENT] %s" % e)
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass

def set_state(chat_id, state, data=None):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """INSERT INTO user_states (chat, state, data, updated_at)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT(chat) DO UPDATE SET
                 state=excluded.state, data=excluded.data, updated_at=excluded.updated_at""",
            (str(chat_id), state, json.dumps(data or {}), now)
        )
        db.commit()
    except Exception as e:
        logger.error("[SET STATE ERROR] %s" % e)
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass

def get_state(chat_id):
    """Returns (state_string, data_dict). Always a 2-tuple."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT state, data FROM user_states WHERE chat=%s", (str(chat_id),))
        row = c.fetchone()
        if not row:
            return None, {}
        state, raw_data = row
        try:
            data = json.loads(raw_data) if raw_data else {}
        except Exception:
            data = {}
        return state, data
    except Exception as e:
        logger.warning("[GET STATE] %s" % e)
        return None, {}
    finally:
        if db:
            try: db.close()
            except Exception: pass

def clear_state(chat_id):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("DELETE FROM user_states WHERE chat=%s", (str(chat_id),))
        db.commit()
    except Exception as e:
        logger.error("[CLEAR STATE ERROR] %s" % e)
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass

def is_user_banned(chat_id):
    """Check if user is banned"""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT chat FROM banned_users WHERE chat=%s", (str(chat_id),))
        row = c.fetchone()
        return bool(row)
    except Exception as e:
        logger.warning("[IS_USER_BANNED] %s" % e)
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass

def ban_user(chat_id, reason="No reason provided"):
    """Ban a user from using the bot"""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO banned_users (chat, reason, banned_at) VALUES (%s,%s,%s) "
            "ON CONFLICT(chat) DO UPDATE SET reason=excluded.reason, banned_at=excluded.banned_at",
            (str(chat_id), reason, now)
        )
        db.commit()
        logger.info("[BAN] Banned user: %s" % chat_id)
        return True
    except Exception as e:
        logger.error("[BAN ERROR] %s" % e)
        if db:
            try: db.rollback()
            except Exception: pass
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass

def unban_user(chat_id):
    """Unban a user"""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("DELETE FROM banned_users WHERE chat=%s", (str(chat_id),))
        db.commit()
        logger.info("[UNBAN] Unbanned user: %s" % chat_id)
        return True
    except Exception as _e:
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass

def get_banned_users():
    """Get list of banned users"""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT chat, reason, banned_at FROM banned_users ORDER BY banned_at DESC")
        return c.fetchall()
    except Exception as _e:
        return []
    finally:
        if db:
            try: db.close()
            except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════
