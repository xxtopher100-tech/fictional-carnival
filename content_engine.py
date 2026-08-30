"""Market Pulse Bot — content_engine module (split from the real monolithic bot.py)."""

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

from market_pulse.ai_engine import ask_ai
from market_pulse.config_runtime import ADMIN_IDS, CHANNEL_ENABLED, PRO_CHANNEL_ID, logger
from market_pulse.db import get_db
from market_pulse.helpers import wat_now
from market_pulse.pro_system import get_bot_mode
from market_pulse.telegram_api import send


# ─── extracted section ───
# 📦 CONTENT ENGINE — V2 MULTI-PLATFORM CONTENT GENERATION
# ═══════════════════════════════════════════════════════════════════════════
# One market analysis → 7 native formats (Telegram, X, WhatsApp,
# Instagram caption, Instagram carousel, TikTok script, plus hashtags/CTA).
# All output goes to admin only. Never auto-published.
# ═══════════════════════════════════════════════════════════════════════════

EDUCATIONAL_TOPICS = [
    "Support & Resistance — how levels work and why price respects them",
    "Risk Management — position sizing, stop losses, and why most traders blow their accounts",
    "P2P Explained — how Nigerian P2P trading works, what spreads mean, and how to read them",
    "Market Cycles — accumulation, markup, distribution, markdown and where we are now",
    "Fear & Greed — how sentiment drives price and how to use it as a contrarian signal",
    "Liquidity & Whales — what liquidity really means and how large players move price",
    "Market Psychology — why traders repeat the same mistakes and how to avoid them",
    "Stablecoins — USDT vs USDC, risks, and how to use them in a Nigerian context",
    "What Fake Breakouts Are — how to identify them before you get trapped",
    "Understanding Volatility — how to read market structure during high-volatility moves",
]

def _content_ai(prompt, max_words=None):
    """Single AI call for content generation. Returns text or empty string."""
    if max_words:
        prompt = prompt + f"\n\nKeep under {max_words} words."
    result, _ = ask_ai(prompt)
    return result or ""


def build_content_engine(
    post_type: str,
    telegram_text: str,
    market_context: dict,
) -> dict:
    """
    Core Content Engine.
    Takes the Telegram post (already built) + market context dict and
    produces all other platform formats via individual AI calls.

    post_type: 'morning' | 'midday' | 'evening' | 'weekly' | 'alert'
    market_context: keys like btc_price, btc_change, fg_val, fg_lbl,
                    gainers_str, losers_str, p2p_str, key_insight

    Returns a dict with keys:
      telegram, x_post, x_thread, whatsapp, instagram_caption,
      instagram_carousel, tiktok_script, hashtags, cta, posting_order
    """
    btc_price   = market_context.get("btc_price", "N/A")
    btc_change  = market_context.get("btc_change", "0%")
    fg_val      = market_context.get("fg_val", "50")
    fg_lbl      = market_context.get("fg_lbl", "Neutral")
    gainers_str = market_context.get("gainers_str", "N/A")
    losers_str  = market_context.get("losers_str", "N/A")
    p2p_str     = market_context.get("p2p_str", "")
    key_insight = market_context.get("key_insight", "")

    # Strip HTML tags from telegram text for AI prompts (re already imported at module level)
    clean_tg = re.sub(r"<[^>]+>", "", telegram_text).strip()

    results = {"telegram": telegram_text}

    # ── X POST (single insight, educational, no copy-paste) ──────────────
    x_prompt = (
        f"You are writing a single X (Twitter) post for Market Pulse, a Nigerian crypto "
        f"intelligence brand. Extract ONE valuable insight from this market brief and rewrite it "
        f"natively for X. Educational, not a price alert. No hashtags yet (add later). "
        f"Max 250 characters. Plain text only.\nMarket Brief:\n{clean_tg[:800]}"
    )
    results["x_post"] = _content_ai(x_prompt, max_words=60)

    # ── X THREAD (educational expansion — only for morning/evening/weekly) ─
    if post_type in ("morning", "evening", "weekly", "alert"):
        thread_prompt = (
            f"Write a 4-tweet educational X thread for Market Pulse based on this market brief. "
            f"Each tweet: educational, specific to today's market, no emojis in tweet 1. "
            f"Format: Tweet 1: [text]\nTweet 2: [text]\nTweet 3: [text]\nTweet 4: [text]\n"
            f"Make tweet 1 the hook. Tweet 4 ends with the lesson Nigerian traders should apply now. "
            f"No hashtags in the thread itself.\nMarket Brief:\n{clean_tg[:800]}"
        )
        results["x_thread"] = _content_ai(thread_prompt, max_words=280)
    else:
        results["x_thread"] = ""

    # ── WHATSAPP (short, scannable, max-value snapshot) ───────────────────
    wa_prompt = (
        f"Write a WhatsApp channel post for Market Pulse. Short, easy to scan, maximum value. "
        f"This is a retention tool — existing followers, not new ones. "
        f"Cover only the most important number and the one thing traders should know right now. "
        f"Max 80 words. No HTML tags. Use \n for line breaks.\nContext:\n{clean_tg[:600]}"
    )
    results["whatsapp"] = _content_ai(wa_prompt, max_words=80)

    # ── INSTAGRAM CAPTION ─────────────────────────────────────────────────
    ig_prompt = (
        f"Write an Instagram caption for Market Pulse, a Nigerian crypto intelligence brand. "
        f"Educational, not hype. End with a call-to-action inviting followers to comment or save. "
        f"2–3 short paragraphs, conversational tone. Max 150 words.\nContext:\n{clean_tg[:600]}"
    )
    results["instagram_caption"] = _content_ai(ig_prompt, max_words=150)

    # ── INSTAGRAM CAROUSEL COPY ───────────────────────────────────────────
    carousel_prompt = (
        f"Write copy for a 5-slide Instagram carousel for Market Pulse. "
        f"Topic: today's market insight for Nigerian traders. Educational, visual-first. "
        f"Format each slide: SLIDE [N]: [Title] | [Body — 1-2 short sentences max] "
        f"Slide 1 must be a hook. Slide 5 must be a CTA/lesson. "
        f"Plain text, no HTML.\nContext:\n{clean_tg[:600]}"
    )
    results["instagram_carousel"] = _content_ai(carousel_prompt, max_words=200)

    # ── TIKTOK SCRIPT (30–60 second, faceless, educational) ──────────────
    tiktok_prompt = (
        f"Write a 30–60 second TikTok video script for Market Pulse. "
        f"This is a FACELESS video — no presenter. Use: on-screen text, chart annotations, voice-over. "
        f"Format: \n[VISUAL]: description of what appears on screen\n[TEXT OVERLAY]: what text appears\n[VO]: voice-over line\n"
        f"Educational. Never clickbait. Topic: today's key market insight for Nigerian traders. "
        f"End with one strong lesson. Max 120 words.\nContext:\n{clean_tg[:600]}"
    )
    results["tiktok_script"] = _content_ai(tiktok_prompt, max_words=150)

    # ── HASHTAGS ──────────────────────────────────────────────────────────
    ht_prompt = (
        f"Generate 10 relevant hashtags for a Nigerian crypto intelligence post about today's market. "
        f"Mix broad (#crypto #bitcoin) with Nigerian-specific (#NigerianTraders #CryptoNigeria) "
        f"and educational (#CryptoEducation #TradingLessons). Plain text, one line, space-separated."
    )
    results["hashtags"] = _content_ai(ht_prompt, max_words=30)

    # ── CTA ───────────────────────────────────────────────────────────────
    cta_map = {
        "morning": "🚀 Start your trading day smarter — join @marketpulseng on Telegram for the full brief.",
        "midday":  "📊 Full midday analysis with entry & stop is live in the Pro channel. DM @heisthegeneral.",
        "evening": "🌙 Get tomorrow's exact trade plan before you sleep — Pro channel. DM @heisthegeneral.",
        "weekly":  "🔥 Saturday Weekly Edge is live in the Pro channel — entry, stop, target for next week. DM @heisthegeneral.",
        "alert":   "⚡ Full key level analysis + trade setup is in the Pro channel right now. DM @heisthegeneral.",
    }
    results["cta"] = cta_map.get(post_type, "⚡ Full intelligence in the Pro channel. DM @heisthegeneral.")

    # ── POSTING ORDER ─────────────────────────────────────────────────────
    order_map = {
        "morning": "1. Telegram (full brief) → 2. X Post (insight) → 3. WhatsApp (snapshot) → 4. Instagram Story text",
        "midday":  "1. Telegram (midday) → 2. X Post → 3. WhatsApp (only if significant move)",
        "evening": "1. Telegram (recap) → 2. X Post → 3. WhatsApp → 4. X Thread (if important lesson) → 5. Instagram",
        "weekly":  "1. Telegram (full report) → 2. X Educational Thread → 3. Instagram Carousel → 4. WhatsApp summary → 5. TikTok",
        "alert":   "1. Telegram (alert) → 2. X Post → 3. WhatsApp (only if major event)",
    }
    results["posting_order"] = order_map.get(post_type, "1. Telegram → 2. X → 3. WhatsApp")

    return results


def save_content_package(post_type: str, trigger_source: str, package: dict) -> int:
    """Save generated content package to DB. Returns package ID."""
    db = None
    try:
        db = get_db(); c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """INSERT INTO content_packages
               (package_type, trigger_source, telegram_text, x_post, x_thread,
                whatsapp_text, instagram_caption, instagram_carousel, tiktok_script,
                hashtags, cta, posting_order, status, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending',%s)
               RETURNING id""",
            (
                post_type, trigger_source,
                package.get("telegram",""), package.get("x_post",""),
                package.get("x_thread",""), package.get("whatsapp",""),
                package.get("instagram_caption",""), package.get("instagram_carousel",""),
                package.get("tiktok_script",""), package.get("hashtags",""),
                package.get("cta",""), package.get("posting_order",""), now
            )
        )
        pkg_id = c.fetchone()[0]
        db.commit()
        logger.info(f"[CONTENT ENGINE] Package #{pkg_id} saved ({post_type})")
        return pkg_id
    except Exception as e:
        logger.error(f"[CONTENT ENGINE] Save error: {e}")
        if db:
            try: db.rollback()
            except Exception: pass
        return 0
    finally:
        if db:
            try: db.close()
            except Exception: pass


def format_content_package_for_admin(pkg_id: int, package: dict, post_type: str) -> str:
    """Format content package as admin Telegram message."""
    now_str = wat_now().strftime("%b %d, %I:%M %p WAT")
    sections = [
        f"📦 <b>CONTENT PACKAGE #{pkg_id}</b>  —  {post_type.upper()}",
        f"<i>{now_str}  ·  ⚡ Market Pulse</i>",
        "",
        "⚠️ <b>ADMIN ONLY. Do not publish automatically.</b>",
        "Review each section below and publish manually.",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "📱 <b>TELEGRAM (Full Brief)</b>",
        "<i>→ Already posted to channel if scheduled. No action needed.</i>",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "🐦 <b>X POST</b>",
        package.get("x_post","N/A"),
        "",
    ]

    x_thread = package.get("x_thread","")
    if x_thread:
        sections += [
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "",
            "🧵 <b>X THREAD</b>",
            x_thread,
            "",
        ]

    sections += [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "💬 <b>WHATSAPP CHANNEL</b>",
        package.get("whatsapp","N/A"),
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "📸 <b>INSTAGRAM CAPTION</b>",
        package.get("instagram_caption","N/A"),
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "🎠 <b>INSTAGRAM CAROUSEL COPY</b>",
        package.get("instagram_carousel","N/A"),
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "🎵 <b>TIKTOK SCRIPT (Faceless)</b>",
        package.get("tiktok_script","N/A"),
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "🏷 <b>HASHTAGS</b>",
        package.get("hashtags","N/A"),
        "",
        "📢 <b>SUGGESTED CTA</b>",
        package.get("cta","N/A"),
        "",
        "📋 <b>SUGGESTED POSTING ORDER</b>",
        package.get("posting_order","N/A"),
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"<i>Package #{pkg_id} · Approve or discard · NFA</i>",
    ]
    return "\n".join(sections)


def generate_and_deliver_content_package(
    post_type: str,
    telegram_text: str,
    market_context: dict,
    trigger_source: str = "scheduled",
):
    """
    Full pipeline: build content engine → save to DB → deliver to admin.
    Called after every scheduled channel post.
    """
    try:
        logger.info(f"[CONTENT ENGINE] Building {post_type} package...")
        package = build_content_engine(post_type, telegram_text, market_context)
        pkg_id  = save_content_package(post_type, trigger_source, package)
        admin_msg = format_content_package_for_admin(pkg_id, package, post_type)
        for admin_id in ADMIN_IDS:
            # Split message if too long (Telegram limit 4096)
            if len(admin_msg) <= 4000:
                send(admin_id, admin_msg)
            else:
                # Send in parts at natural break points
                parts = admin_msg.split("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
                current = ""
                for part in parts:
                    if len(current) + len(part) + 40 < 3800:
                        current += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" + part
                    else:
                        if current:
                            send(admin_id, current.strip())
                        current = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n" + part
                        time.sleep(0.5)
                if current:
                    send(admin_id, current.strip())
        logger.info(f"[CONTENT ENGINE] Package #{pkg_id} delivered to admin(s)")
        return pkg_id
    except Exception as e:
        logger.error(f"[CONTENT ENGINE] Pipeline error: {e}")
        return 0


def build_weekly_educational_content() -> str:
    """
    Sunday educational content. Rotates through EDUCATIONAL_TOPICS by week number.
    Sends to admin as a content package, also returns the Telegram version.
    """
    week_num = datetime.now().isocalendar()[1]
    topic = EDUCATIONAL_TOPICS[week_num % len(EDUCATIONAL_TOPICS)]

    tg_prompt = (
        f"Write a Telegram educational post for Nigerian crypto traders on: {topic}. "
        f"Structure: short intro (1 sentence), 3-4 clear educational points, "
        f"real example relevant to Nigerian traders (P2P, naira, or common mistake). "
        f"End with one actionable takeaway. "
        f"No asterisks. Use HTML bold <b>text</b> for key terms. Max 300 words. "
        f"Footer: NFA - DYOR  ·  ⚡ Market Pulse"
    )
    tg_text, _ = ask_ai(tg_prompt)
    if not tg_text:
        tg_text = f"📚 <b>Weekly Education: {topic}</b>\n\n<i>Educational content unavailable — check back next week.</i>"

    # Generate full content package
    mc = {"key_insight": topic, "fg_val": "50", "fg_lbl": "Neutral", "btc_price": "N/A", "btc_change": "0%"}
    generate_and_deliver_content_package("weekly", tg_text, mc, trigger_source="educational")
    return tg_text


def get_pending_content_packages(limit=5) -> list:
    """Return list of pending content packages for admin review."""
    db = None
    try:
        db = get_db(); c = db.cursor()
        c.execute(
            "SELECT id, package_type, trigger_source, created_at FROM content_packages "
            "WHERE status='pending' ORDER BY id DESC LIMIT %s", (limit,)
        )
        return c.fetchall()
    except Exception as _e:
        return []
    finally:
        if db:
            try: db.close()
            except Exception: pass


def get_content_package_by_id(pkg_id: int) -> dict:
    """Retrieve a specific content package by ID."""
    db = None
    try:
        db = get_db(); c = db.cursor()
        c.execute(
            "SELECT id, package_type, trigger_source, telegram_text, x_post, x_thread, "
            "whatsapp_text, instagram_caption, instagram_carousel, tiktok_script, "
            "hashtags, cta, posting_order, status, created_at "
            "FROM content_packages WHERE id=%s", (pkg_id,)
        )
        row = c.fetchone()
        if not row:
            return {}
        keys = ["id","package_type","trigger_source","telegram","x_post","x_thread",
                "whatsapp","instagram_caption","instagram_carousel","tiktok_script",
                "hashtags","cta","posting_order","status","created_at"]
        return dict(zip(keys, row))
    except Exception as _e:
        return {}
    finally:
        if db:
            try: db.close()
            except Exception: pass


def mark_package_status(pkg_id: int, status: str):
    """Mark package as approved/discarded."""
    db = None
    try:
        db = get_db(); c = db.cursor()
        c.execute("UPDATE content_packages SET status=%s WHERE id=%s", (status, pkg_id))
        db.commit()
    except Exception as e:
        logger.error(f"[CONTENT ENGINE] Status update error: {e}")
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass


def build_admin_dashboard() -> str:
    """Comprehensive admin dashboard with all V2 spec metrics."""
    db = None
    try:
        db = get_db(); c = db.cursor()
        now_wat_str = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        today = wat_now().strftime("%Y-%m-%d")
        week_ago = (wat_now() - timedelta(days=7)).strftime("%Y-%m-%d")
        month_ago = (wat_now() - timedelta(days=30)).strftime("%Y-%m-%d")

        c.execute("SELECT COUNT(*) FROM users")
        total_users = c.fetchone()[0]
        # M4 FIX: compare full datetime string against stored YYYY-MM-DD HH:MM:SS
        c.execute("SELECT COUNT(*) FROM pro_subscriptions WHERE expiry_date > %s", (now_wat_str,))
        pro_users = c.fetchone()[0]
        free_users = total_users - pro_users

        c.execute("SELECT COUNT(*) FROM users WHERE first_seen >= %s", (week_ago + " 00:00:00",))
        new_this_week = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE first_seen >= %s", (month_ago + " 00:00:00",))
        new_this_month = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM alerts WHERE active=1")
        active_alerts = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM alerts")
        total_alerts = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM channel_posts WHERE posted_at >= %s", (week_ago + " 00:00:00",))
        posts_this_week = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM content_packages WHERE status='pending'")
        pending_packages = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM content_packages")
        total_packages = c.fetchone()[0]

        # Top features this week
        c.execute(
            "SELECT feature, COUNT(*) as cnt FROM feature_usage WHERE timestamp >= %s "
            "GROUP BY feature ORDER BY cnt DESC LIMIT 5", (week_ago + " 00:00:00",)
        )
        top_features = c.fetchall()

        c.execute("SELECT COUNT(*) FROM banned_users")
        banned_count = c.fetchone()[0]

        lines = [
            "📊 <b>ADMIN DASHBOARD</b>",
            f"<i>{wat_now().strftime('%b %d, %Y  %I:%M %p WAT')}</i>",
            "",
            "👥 <b>USERS</b>",
            f"Total: <b>{total_users:,}</b>",
            f"Pro:   <b>{pro_users:,}</b>  |  Free: <b>{free_users:,}</b>",
            f"New (7d):  <b>{new_this_week:,}</b>  |  New (30d): <b>{new_this_month:,}</b>",
            f"Banned:   <b>{banned_count:,}</b>",
            "",
            "🔔 <b>ALERTS</b>",
            f"Active: <b>{active_alerts:,}</b>  |  Total created: <b>{total_alerts:,}</b>",
            "",
            "📢 <b>CHANNEL</b>",
            f"Posts (7d): <b>{posts_this_week:,}</b>",
            f"Mode: <b>{get_bot_mode().upper()}</b>  |  Channel: <b>{'LIVE' if CHANNEL_ENABLED else 'PAUSED'}</b>",
            "",
            "📦 <b>CONTENT ENGINE</b>",
            f"Packages generated: <b>{total_packages:,}</b>",
            f"Pending approval: <b>{pending_packages:,}</b>",
            "",
        ]

        if top_features:
            lines += ["🏆 <b>TOP FEATURES (7d)</b>"]
            for feature, cnt in top_features:
                lines.append(f"  {feature}: <b>{cnt:,}</b>")
            lines.append("")

        lines += [
            "⚙️ <b>SYSTEM</b>",
            f"Bot Mode: <b>{get_bot_mode().upper()}</b>",
            f"Channel Posting: <b>{'✅ ON' if CHANNEL_ENABLED else '⏸ OFF'}</b>",
            f"Pro Channel: <b>{'✅ SET' if PRO_CHANNEL_ID and PRO_CHANNEL_ID != '-100XXXXXXXXX' else '❌ NOT SET'}</b>",
        ]
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"[DASHBOARD] {e}")
        return f"⚠️ Dashboard error: {e}"
    finally:
        if db:
            try: db.close()
            except Exception: pass
# ═══════════════════════════════════════════════════════════════════════════
