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

# Paid referrals only (friend used ref link AND was grant_pro'd as payment).
# Threshold → months of Pro for the referrer.
PRO_PAID_REFERRAL_THRESHOLDS = {
    3:  (1, "1 month"),
    5:  (3, "3 months"),
    10: (6, "6 months"),
}
# Legacy name kept for any old imports
PRO_REFERRAL_REWARDS = {k: v[1] for k, v in PRO_PAID_REFERRAL_THRESHOLDS.items()}

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


BOT_MODE = "pro"

def get_bot_mode():
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

def get_pro_source(chat_id):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT source FROM pro_subscriptions WHERE chat=%s", (str(chat_id),))
        row = c.fetchone()
        return row[0] if row else None
    except Exception:
        return None
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


def grant_pro_days(chat_id, days=7, source="welcome_trial"):
    """Grant Pro for a fixed number of days (does not stack onto active paid time).

    Used for new-user welcome trials. If the user already has a future expiry,
    leave it unchanged and return False (trial not applied).
    """
    db = None
    try:
        days = int(days)
        if days <= 0:
            return False
        db = get_db()
        c = db.cursor()
        now = wat_now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        expiry_str = (now + timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

        c.execute("SELECT expiry_date, source FROM pro_subscriptions WHERE chat=%s", (str(chat_id),))
        row = c.fetchone()
        if row:
            try:
                existing_expiry = datetime.strptime(str(row[0])[:19], "%Y-%m-%d %H:%M:%S")
            except Exception:
                existing_expiry = now
            if existing_expiry > now:
                logger.info("[PRO TRIAL] %s already Pro until %s — trial skipped", chat_id, row[0])
                return False
            c.execute(
                "UPDATE pro_subscriptions SET expiry_date=%s, source=%s WHERE chat=%s",
                (expiry_str, source, str(chat_id)),
            )
        else:
            c.execute(
                "INSERT INTO pro_subscriptions (chat, expiry_date, source, created_at) VALUES (%s,%s,%s,%s)",
                (str(chat_id), expiry_str, source, now_str),
            )
        db.commit()
        logger.info("[PRO TRIAL] Granted %s-day Pro to %s (source=%s)", days, chat_id, source)
        return True
    except Exception as e:
        logger.error("[GRANT PRO DAYS ERROR] %s", e)
        if db:
            try:
                db.rollback()
            except Exception:
                pass
        return False
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass



def _ensure_pro_referral_paid_column():
    """Add paid flag if missing (safe on every call)."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='pro_referrals' AND column_name='paid'"
        )
        if not c.fetchone():
            try:
                c.execute("ALTER TABLE pro_referrals ADD COLUMN paid INTEGER DEFAULT 0")
                db.commit()
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
        c.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='pro_referrals' AND column_name='paid_at'"
        )
        if not c.fetchone():
            try:
                c.execute("ALTER TABLE pro_referrals ADD COLUMN paid_at TEXT")
                db.commit()
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
    except Exception as e:
        logger.debug("[PRO REF SCHEMA] %s", e)
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def get_paid_pro_referral_count(chat_id) -> int:
    """How many referred users later became paying Pro (grant_pro payment)."""
    _ensure_pro_referral_paid_column()
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            "SELECT COUNT(*) FROM pro_referrals WHERE referrer_chat=%s AND COALESCE(paid,0)=1",
            (str(chat_id),),
        )
        row = c.fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _tier_reward_already_claimed(referrer_chat, threshold: int) -> bool:
    db = None
    try:
        db = get_db()
        c = db.cursor()
        key = f"pro_paid_ref_tier_{referrer_chat}_{threshold}"
        c.execute("SELECT value FROM admin_settings WHERE key=%s", (key,))
        return c.fetchone() is not None
    except Exception:
        return False
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _mark_tier_reward_claimed(referrer_chat, threshold: int) -> None:
    db = None
    try:
        db = get_db()
        c = db.cursor()
        key = f"pro_paid_ref_tier_{referrer_chat}_{threshold}"
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, "1", now),
        )
        db.commit()
    except Exception as e:
        logger.debug("[PRO REF TIER MARK] %s", e)
        if db:
            try:
                db.rollback()
            except Exception:
                pass
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _apply_paid_referral_rewards(referrer_chat) -> None:
    """If paid referral count hits 3/5/10, grant that tier once."""
    count = get_paid_pro_referral_count(referrer_chat)
    for threshold in sorted(PRO_PAID_REFERRAL_THRESHOLDS.keys()):
        if count < threshold:
            continue
        if _tier_reward_already_claimed(referrer_chat, threshold):
            continue
        months, label = PRO_PAID_REFERRAL_THRESHOLDS[threshold]
        ok = grant_pro(referrer_chat, months, source="referral")
        if not ok:
            logger.warning("[PRO REF] failed to grant %s mo to %s", months, referrer_chat)
            continue
        _mark_tier_reward_claimed(referrer_chat, threshold)
        try:
            next_hint = {
                3: "Next: 5 paid referrals → 3 months free Pro",
                5: "Next: 10 paid referrals → 6 months free Pro",
                10: "Top tier reached — thank you!",
            }.get(threshold, "")
            send(
                int(referrer_chat),
                (
                    "🎉 <b>Paid referral reward!</b>\n\n"
                    "A friend you referred <b>paid for Pro</b>.\n"
                    f"You now have <b>{count} paid referral(s)</b>.\n\n"
                    f"Reward unlocked: <b>{label} Pro</b> added to your account.\n"
                    f"{next_hint}"
                ),
            )
        except Exception as e:
            logger.debug("[PRO REF NOTIFY] %s", e)


def mark_referral_paid_and_reward(referred_chat) -> None:
    """Call when referred user is grant_pro'd as payment. No link → no-op."""
    _ensure_pro_referral_paid_column()
    db = None
    referrer = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            "SELECT referrer_chat, COALESCE(paid,0) FROM pro_referrals WHERE referred_chat=%s",
            (str(referred_chat),),
        )
        row = c.fetchone()
        if not row:
            # Never used a referral link — not a paid referral credit
            return
        referrer, already = row[0], int(row[1] or 0)
        if already:
            return  # already counted this paying friend once
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "UPDATE pro_referrals SET paid=1, paid_at=%s, claimed=1 WHERE referred_chat=%s",
            (now, str(referred_chat)),
        )
        db.commit()
    except Exception as e:
        logger.error("[PRO REF PAID MARK] %s", e)
        if db:
            try:
                db.rollback()
            except Exception:
                pass
        return
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass
    if referrer:
        try:
            _apply_paid_referral_rewards(referrer)
        except Exception as e:
            logger.error("[PRO REF REWARD] %s", e)



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
        logger.info("[PRO] Granted Pro to %s for %s months (source=%s)" % (chat_id, months, source))
        # Paid activation of a referred user → credit referrer (not trial / not referral-reward grants)
        src = (source or "payment").lower().strip()
        if src in ("payment", "admin", "whatsapp"):
            try:
                mark_referral_paid_and_reward(chat_id)
            except Exception as _re:
                logger.debug("[PRO REF HOOK] %s", _re)
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
    """Returns (reward_label, paid_count) based on paid referrals only."""
    count = get_paid_pro_referral_count(chat_id)
    reward = None
    for threshold, (months, label) in sorted(PRO_PAID_REFERRAL_THRESHOLDS.items(), reverse=True):
        if count >= threshold:
            reward = label
            break
    return reward, count

def record_pro_referral(referrer_chat, referred_chat):
    """Store referral link only. Rewards fire later when referred user pays (grant_pro payment)."""
    if str(referrer_chat) == str(referred_chat):
        return
    _ensure_pro_referral_paid_column()
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("SELECT id FROM pro_referrals WHERE referred_chat=%s", (str(referred_chat),))
        if c.fetchone():
            db.close()
            return
        c.execute(
            "INSERT INTO pro_referrals (referrer_chat, referred_chat, created_at, paid, claimed) "
            "VALUES (%s,%s,%s,0,0) ON CONFLICT DO NOTHING",
            (str(referrer_chat), str(referred_chat), now),
        )
        db.commit()
        db.close()
        logger.info("[PRO REF] Linked %s → referrer %s (awaiting paid activation)", referred_chat, referrer_chat)
    except Exception as e:
        logger.error("[PRO REFERRAL ERROR] %s" % e)



# ═══════════════════════════════════════════════════════════════════════════
