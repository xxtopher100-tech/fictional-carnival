"""Market Pulse Bot — pro_system module (split from the real monolithic bot.py)."""

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
from market_pulse.telegram_api import send


# ─── extracted section ───
# 💰 PRO SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

PRO_REFERRAL_REWARDS = {
    3:  "1week",
    5:  "1month",
    10: "3months",
    20: "6months"
}

# ── Upsell nudge (fixes a real bug: build_free_key_alert in alerts.py
#    referenced should_show_upsell/FREE_UPSELL_BLOCK, but neither was ever
#    defined anywhere in the bot — confirmed against the real source file) ──
_upsell_last_shown: dict = {}
UPSELL_COOLDOWN_SECONDS = 6 * 3600  # show at most once every 6h per user

FREE_UPSELL_BLOCK = (
    "\n💎 <i>Get entries, stops, targets, and confidence scores on every alert "
    "— </i><b>Upgrade to Pro</b>"
)


def should_show_upsell(chat_id):
    """True if this is a free-tier user who hasn't seen the upsell block recently."""
    if not chat_id:
        return False
    if is_pro(chat_id):
        return False
    now = time.time()
    last = _upsell_last_shown.get(chat_id, 0)
    if now - last < UPSELL_COOLDOWN_SECONDS:
        return False
    _upsell_last_shown[chat_id] = now
    return True


def get_bot_mode():
    global BOT_MODE
    return BOT_MODE

def set_bot_mode(mode):
    global BOT_MODE
    if mode in ["everyone", "pro"]:
        BOT_MODE = mode
        return True
    return False

def is_pro(chat_id):
    if get_bot_mode() == "everyone" or chat_id in ADMIN_IDS:
        return True
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("SELECT expiry_date FROM pro_subscriptions WHERE chat=%s AND expiry_date > %s",
                  (str(chat_id), now))
        row = c.fetchone()
        return bool(row)
    except Exception as e:
        logger.warning("[IS_PRO] %s" % e)
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass

def get_pro_expiry(chat_id):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT expiry_date FROM pro_subscriptions WHERE chat=%s", (str(chat_id),))
        row = c.fetchone()
        return row[0] if row else None
    except Exception as _e:
        return None
    finally:
        if db:
            try: db.close()
            except Exception: pass

def get_pro_days_left(chat_id):
    expiry = get_pro_expiry(chat_id)
    if not expiry:
        return None
    try:
        expiry_date = datetime.strptime(expiry, "%Y-%m-%d %H:%M:%S")
        days_left = (expiry_date - datetime.now()).days
        return max(0, days_left)
    except Exception as _e:
        return None

def grant_pro(chat_id, months=1, source="payment"):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now()
        expiry = now + timedelta(days=30 * months)
        expiry_str = expiry.strftime("%Y-%m-%d %H:%M:%S")
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        c.execute("SELECT expiry_date FROM pro_subscriptions WHERE chat=%s", (str(chat_id),))
        row = c.fetchone()

        if row:
            try:
                existing_expiry = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                existing_expiry = now  # Malformed date fallback
            if existing_expiry > now:
                new_expiry = existing_expiry + timedelta(days=30 * months)
            else:
                new_expiry = now + timedelta(days=30 * months)
            new_expiry_str = new_expiry.strftime("%Y-%m-%d %H:%M:%S")
            c.execute("UPDATE pro_subscriptions SET expiry_date=%s, source=%s WHERE chat=%s",
                      (new_expiry_str, source, str(chat_id)))
        else:
            c.execute("INSERT INTO pro_subscriptions (chat, expiry_date, source, created_at) VALUES (%s,%s,%s,%s)",
                      (str(chat_id), expiry_str, source, now_str))

        db.commit()
        logger.info("[PRO] Granted Pro to %s for %s months" % (chat_id, months))
        return True
    except Exception as e:
        logger.error("[GRANT PRO ERROR] %s" % e)
        if db:
            try: db.rollback()
            except Exception: pass
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass

def get_pro_referral_count(chat_id):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT COUNT(*) FROM pro_referrals WHERE referrer_chat=%s", (str(chat_id),))
        return c.fetchone()[0]
    except Exception as _e:
        return 0
    finally:
        if db:
            try: db.close()
            except Exception: pass

def get_pro_referral_reward(chat_id):
    """Returns (reward_description, days) for current referral count."""
    count = get_pro_referral_count(chat_id)
    reward = None
    for threshold, reward_type in sorted(PRO_REFERRAL_REWARDS.items(), reverse=True):
        if count >= threshold:
            reward = reward_type
            break
    return reward, count

def record_pro_referral(referrer_chat, referred_chat):
    if str(referrer_chat) == str(referred_chat):
        return
    # Both free and pro users can refer — rewards granted automatically
    try:
        db = get_db()
        c = db.cursor()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("SELECT id FROM pro_referrals WHERE referred_chat=%s", (str(referred_chat),))
        if c.fetchone():
            db.close()
            return
        c.execute("INSERT INTO pro_referrals (referrer_chat, referred_chat, created_at) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                  (str(referrer_chat), str(referred_chat), now))
        db.commit()
        
        new_count = get_pro_referral_count(referrer_chat)
        reward, _ = get_pro_referral_reward(referrer_chat)
        thresholds = {3: ("1 week", 0.25), 5: ("1 month", 1), 10: ("3 months", 3), 20: ("6 months", 6)}
        if reward and new_count in thresholds:
            label, months = thresholds[new_count]
            grant_pro(referrer_chat, months, "referral")
            try:
                send(int(referrer_chat),
                    f"🎉 <b>Referral Reward!</b>\n\n"
                    f"You hit <b>{new_count} referrals</b> — <b>{label} Pro access</b> added!\n\n"
                    f"Keep going:\n"
                    f"{'5 referrals → 1 month free' if new_count < 5 else '10 referrals → 3 months free' if new_count < 10 else '20 referrals → 6 months free' if new_count < 20 else 'You have hit the top tier!'}")
            except Exception as _e:
                logger.debug("[SILENT EXC] %s" % _e)

        db.close()
    except Exception as e:
        logger.error("[PRO REFERRAL ERROR] %s" % e)

# ═══════════════════════════════════════════════════════════════════════════
