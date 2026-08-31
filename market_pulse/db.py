"""Market Pulse Bot — db module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import DATABASE_URL, logger


# ─── extracted section ───
# 🗄️ DATABASE
# ═══════════════════════════════════════════════════════════════════════════

def get_db():
    """Return a PostgreSQL connection. Falls back gracefully."""
    url = DATABASE_URL
    if not url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    result = urlparse(url)
    conn = psycopg2.connect(
        host=result.hostname,
        port=result.port or 5432,
        database=result.path.lstrip("/"),
        user=result.username,
        password=result.password,
        sslmode="require",
        connect_timeout=10,
    )
    conn.autocommit = False
    return conn

def init_db():
    db = get_db()
    try:
        c = db.cursor()
        tables = """
    CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY ,
        coin TEXT NOT NULL,
        price REAL NOT NULL,
        timestamp TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY ,
        chat TEXT NOT NULL,
        coin TEXT NOT NULL,
        condition TEXT NOT NULL,
        target REAL NOT NULL,
        active INTEGER DEFAULT 1,
        label TEXT DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS user_states (
        chat TEXT PRIMARY KEY,
        state TEXT,
        data TEXT,
        updated_at TEXT
    );
    CREATE TABLE IF NOT EXISTS users (
        chat TEXT PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        first_seen TEXT,
        last_seen TEXT
    );
    CREATE TABLE IF NOT EXISTS watchlists (
        id INTEGER PRIMARY KEY ,
        chat TEXT NOT NULL,
        coin TEXT NOT NULL,
        UNIQUE(chat, coin)
    );
    CREATE TABLE IF NOT EXISTS portfolio (
        id INTEGER PRIMARY KEY ,
        chat TEXT NOT NULL,
        coin TEXT NOT NULL,
        amount REAL NOT NULL,
        buy_price REAL NOT NULL,
        added_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY ,
        chat TEXT NOT NULL,
        action TEXT NOT NULL,
        timestamp TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS p2p_alerts (
        id INTEGER PRIMARY KEY ,
        chat TEXT NOT NULL,
        crypto TEXT NOT NULL,
        fiat TEXT NOT NULL,
        condition TEXT NOT NULL,
        target REAL NOT NULL,
        active INTEGER DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS referrals (
        id INTEGER PRIMARY KEY ,
        referrer_chat TEXT NOT NULL,
        referred_chat TEXT NOT NULL,
        joined_at TEXT NOT NULL,
        UNIQUE(referred_chat)
    );
    CREATE TABLE IF NOT EXISTS pro_subscriptions (
        chat TEXT PRIMARY KEY,
        expiry_date TEXT NOT NULL,
        source TEXT DEFAULT 'payment',
        created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS pro_referrals (
        id INTEGER PRIMARY KEY ,
        referrer_chat TEXT NOT NULL,
        referred_chat TEXT NOT NULL,
        reward_type TEXT,
        claimed INTEGER DEFAULT 0,
        created_at TEXT NOT NULL,
        UNIQUE(referred_chat)
    );
    CREATE TABLE IF NOT EXISTS portfolio_snapshots (
        id INTEGER PRIMARY KEY ,
        chat TEXT NOT NULL,
        value_usd REAL NOT NULL,
        timestamp TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS health_log (
        id INTEGER PRIMARY KEY ,
        service TEXT NOT NULL,
        status TEXT NOT NULL,
        details TEXT,
        timestamp TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS feature_usage (
        id INTEGER PRIMARY KEY ,
        chat TEXT NOT NULL,
        feature TEXT NOT NULL,
        timestamp TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS community_p2p (
        id INTEGER PRIMARY KEY ,
        chat TEXT NOT NULL,
        crypto TEXT NOT NULL,
        fiat TEXT NOT NULL,
        buy_rate REAL NOT NULL,
        sell_rate REAL NOT NULL,
        exchange TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        weight INTEGER DEFAULT 1,
        status TEXT DEFAULT 'pending',
        confirmations INTEGER DEFAULT 0,
        spot_rate REAL,
        expires_at TEXT
    );
    CREATE TABLE IF NOT EXISTS trade_journal (
        id INTEGER PRIMARY KEY ,
        chat TEXT NOT NULL,
        coin TEXT NOT NULL,
        direction TEXT NOT NULL,
        entry_price REAL NOT NULL,
        exit_price REAL,
        size REAL NOT NULL,
        stop_loss REAL,
        take_profit REAL,
        pnl REAL,
        status TEXT DEFAULT 'open',
        opened_at TEXT NOT NULL,
        closed_at TEXT
    );
    CREATE TABLE IF NOT EXISTS rate_submissions (
        chat TEXT PRIMARY KEY,
        submissions_today INTEGER DEFAULT 0,
        strikes_today INTEGER DEFAULT 0,
        blocked_until TEXT,
        last_submission TEXT,
        total_verified INTEGER DEFAULT 0,
        trust_level INTEGER DEFAULT 1,
        p2p_used INTEGER DEFAULT 0,
        onboarded INTEGER DEFAULT 0,
        last_prompted TEXT
    );
    CREATE TABLE IF NOT EXISTS banned_users (
        chat TEXT PRIMARY KEY,
        reason TEXT,
        banned_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS admin_settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS bot_logs (
        id INTEGER PRIMARY KEY ,
        level TEXT NOT NULL,
        message TEXT NOT NULL,
        timestamp TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS system_status (
        id INTEGER PRIMARY KEY ,
        service TEXT NOT NULL,
        status TEXT NOT NULL,
        details TEXT,
        timestamp TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS price_cache (
        coin TEXT NOT NULL,
        price REAL NOT NULL,
        timestamp TEXT NOT NULL,
        PRIMARY KEY (coin, timestamp)
    );
    CREATE TABLE IF NOT EXISTS channel_posts (
        id INTEGER PRIMARY KEY ,
        post_type TEXT NOT NULL,
        posted_at TEXT NOT NULL,
        message_id TEXT
    );
    CREATE TABLE IF NOT EXISTS user_preferences (
        chat TEXT PRIMARY KEY,
        language TEXT DEFAULT 'en',
        notifications INTEGER DEFAULT 1,
        theme TEXT DEFAULT 'dark',
        updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS content_packages (
        id SERIAL PRIMARY KEY,
        package_type TEXT NOT NULL,
        trigger_source TEXT NOT NULL,
        telegram_text TEXT,
        x_post TEXT,
        x_thread TEXT,
        whatsapp_text TEXT,
        instagram_caption TEXT,
        instagram_carousel TEXT,
        tiktok_script TEXT,
        hashtags TEXT,
        cta TEXT,
        posting_order TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS content_performance (
        id SERIAL PRIMARY KEY,
        package_id INTEGER,
        platform TEXT NOT NULL,
        metric TEXT NOT NULL,
        value INTEGER DEFAULT 0,
        recorded_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS trade_ideas (
        id SERIAL PRIMARY KEY,
        coin TEXT NOT NULL,
        tier TEXT NOT NULL,
        direction TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        entry TEXT,
        stop TEXT,
        target1 TEXT,
        target2 TEXT,
        bias TEXT,
        confidence TEXT,
        rr TEXT,
        invalidation TEXT,
        max_size_pct TEXT,
        ai_rationale TEXT,
        status TEXT DEFAULT 'open',
        created_at TEXT NOT NULL,
        closed_at TEXT,
        result TEXT,
        valid_until TEXT,
        expected_horizon TEXT,
        lifecycle_status TEXT
    );
    CREATE TABLE IF NOT EXISTS pro_decisions (
        coin TEXT PRIMARY KEY,
        status TEXT,
        entry TEXT,
        stop TEXT,
        target TEXT,
        updated_at TEXT
    );
    CREATE TABLE IF NOT EXISTS p2p_rate_history (
        id SERIAL PRIMARY KEY,
        asset TEXT NOT NULL,
        fiat TEXT NOT NULL,
        buy_rate REAL,
        sell_rate REAL,
        spread REAL,
        source TEXT,
        recorded_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS p2p_user_alerts (
        id SERIAL PRIMARY KEY,
        chat_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        fiat TEXT DEFAULT 'NGN',
        target_buy REAL NOT NULL,
        active INTEGER DEFAULT 1,
        created_at TEXT NOT NULL
    );
    """
        # Convert SQLite syntax to PostgreSQL
        tables = tables.replace("INTEGER PRIMARY KEY ", "SERIAL PRIMARY KEY")
        tables = tables.replace("INTEGER DEFAULT 1", "INTEGER DEFAULT 1")
        # Execute each CREATE TABLE separately
        for stmt in [s.strip() for s in tables.split(";") if s.strip()]:
            try:
                c.execute(stmt)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logger.warning(f"[DB INIT] {e}")

        # Create indexes on high-frequency query columns (IF NOT EXISTS safe)
        indexes = [
        "CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts(active)",
        "CREATE INDEX IF NOT EXISTS idx_alerts_chat ON alerts(chat)",
        "CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_events_chat ON events(chat)",
        "CREATE INDEX IF NOT EXISTS idx_feature_usage_chat_ts ON feature_usage(chat, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_watchlists_chat ON watchlists(chat)",
        "CREATE INDEX IF NOT EXISTS idx_history_coin_ts ON history(coin, timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_users_first_seen ON users(first_seen)",
        "CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen)",
        "CREATE INDEX IF NOT EXISTS idx_pro_subs_chat ON pro_subscriptions(chat)",
        "CREATE INDEX IF NOT EXISTS idx_pro_subs_expiry ON pro_subscriptions(expiry_date)",
        "CREATE INDEX IF NOT EXISTS idx_content_packages_status ON content_packages(status)",
        "CREATE INDEX IF NOT EXISTS idx_channel_posts_posted_at ON channel_posts(posted_at)",
        "CREATE INDEX IF NOT EXISTS idx_trade_ideas_status ON trade_ideas(status)",
        "CREATE INDEX IF NOT EXISTS idx_trade_ideas_coin ON trade_ideas(coin)",
        "CREATE INDEX IF NOT EXISTS idx_p2p_rate_history_asset_ts ON p2p_rate_history(asset, recorded_at)",
        "CREATE INDEX IF NOT EXISTS idx_p2p_user_alerts_active ON p2p_user_alerts(active)",
        ]
        for idx_sql in indexes:
            try:
                c.execute(idx_sql)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logger.warning(f"[DB INDEX] {e}")
                db.rollback()
                db = get_db()
                c = db.cursor()
        # Add label column if missing
        try:
            c.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS label TEXT DEFAULT ''")
        except Exception as _e:
            logger.debug("[SILENT EXC] %s" % _e)
        db.commit()
        logger.info("Database initialized (PostgreSQL)")
    except Exception as e:
        logger.error("[INIT DB] %s" % e)
        try: db.rollback()
        except Exception: pass
        raise
    finally:
        try: db.close()
        except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════
