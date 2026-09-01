"""Market Pulse Bot — handlers module (split from the real monolithic bot.py)."""
import os
CHANNEL_ID = os.getenv("CHANNEL_ID", "")
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
from market_pulse.alerts import KEY_ALERT_COINS, check_key_market_alerts, daily_digest
from market_pulse.arbitrage import scan_arbitrage
from market_pulse.candle_engine import candle_engine_status, start_candle_engine
from market_pulse.channel_lock import is_user_in_channel
from market_pulse.channel_posts import build_evening_recap, build_evening_recap_pro, build_midday_snapshot, build_midday_snapshot_pro, build_morning_briefing, build_morning_briefing_pro, build_weekly_edge, build_weekly_edge_pro
from market_pulse.config_runtime import (
    ADMIN_IDS, BOT_TOKEN, COINS, LOG_FILE, P2P_FIATS, SCHEDULE, load_admin_config, logger, save_admin_config,
    get_channel_enabled, set_channel_enabled, get_pro_channel_id, set_pro_channel_id, get_mirror_mode, set_mirror_mode,
    validate_critical_config, config_status_summary,
)
from market_pulse.content_engine import build_admin_dashboard, build_weekly_educational_content, format_content_package_for_admin, generate_and_deliver_content_package, get_content_package_by_id, get_pending_content_packages, mark_package_status
from market_pulse.db import get_db, init_db
from market_pulse.derivatives_engine import derivatives_engine_status, start_derivatives_engine
from market_pulse.edge_trade_engine import TRADE_TIERS, check_user_price_alerts, check_watchlist_alerts, close_trade_idea, generate_trade_idea, get_trade_history
from market_pulse.fear_greed import fg_emoji, get_fear_greed
from market_pulse.forex_trade_engine import FOREX_PAIRS, generate_forex_trade_idea
from market_pulse.edge_trade_engine import mark_trade_publication
from market_pulse.helpers import fetch_with_backoff, format_change, format_price, request_json, wat_now
from market_pulse.menus import (
    ACCOUNT_MENU_FREE, ACCOUNT_MENU_PRO, ADMIN_ANALYTICS_MENU, ADMIN_CHANNEL_MENU,
    ADMIN_MENU, ADMIN_SETTINGS_MENU, ADMIN_SYSTEM_MENU, ADMIN_TRADES_MENU, ADMIN_USERS_MENU,
    ALERTS_MENU_FREE, ALERTS_MENU_PRO, BACK_MAIN, INTELLIGENCE_MENU, MARKETS_MENU, P2P_MENU,
    PORTFOLIO_MENU, TOOLS_MENU, TRADES_MENU, FOREX_MENU, CRYPTO_IDEA_MENU, P2P_ALERT_ASSET_MENU,
    forex_tier_menu, crypto_tier_menu, get_user_badge,
)
from market_pulse.morning_package import run_morning_pro_package, toggle_channel_enabled, toggle_mirror_mode
from market_pulse.news import get_crypto_news
from market_pulse.p2p import (
    get_p2p_rate, format_p2p_card, format_multi_p2p_intelligence,
    record_all_p2p_snapshots, check_user_p2p_alerts, set_user_p2p_alert, check_channel_usdt_ngn_pulse,
    P2P_ASSETS,
)
from market_pulse.portfolio import get_portfolio_value
from market_pulse.price_engine import _ws_get_cached, _ws_lock, _ws_price_cache, start_ws_price_engine, ws_engine_status
from market_pulse.price_fetchers import get_best_price, get_gainers_losers, get_kraken_batch, get_secondary_batch, get_secondary_coin, save_price_history
from market_pulse.pro_system import get_bot_mode, get_pro_days_left, get_pro_expiry, get_pro_referral_count, get_pro_referral_reward, get_pro_source, grant_pro, grant_pro_days, is_pro, record_pro_referral, set_bot_mode
from market_pulse.screens import handle_position_calc, show_help, show_main_menu, show_market, show_portfolio, show_position_calculator, show_settings, show_trade_journal, show_upgrade, send_welcome_onboarding
from market_pulse.telegram_api import answer_cb, edit, post_to_channel, post_to_pro_channel, send
from market_pulse.trade_journal import close_trade
from market_pulse.trade_scanner import run_trade_scanner, get_trade_scan_interval_sec, get_trade_scan_interval_sec
from market_pulse.setup_engine import score_open_trade_ideas, outcome_summary
from market_pulse.outcome_monitor import run_outcome_cycle, send_weekly_report_private
from market_pulse.trade_engine_report import send_daily_engine_report
from market_pulse.shadow_verifier import run_shadow_cycle
from market_pulse.users import UPGRADE_BTN, ai_limit_msg, ban_user, check_ai_limit, clear_state, get_banned_users, get_state, is_user_banned, log_event, set_state, track_feature, unban_user, upsert_user
from market_pulse.whale_detection import check_p2p_rate_alerts



# ─── AI Trade Setup: programmatic R:R (authoritative) ───────────────────────
# Entry-zone rule: when the AI gives a range, use the MIDPOINT for R:R.
# That single midpoint is shown in the LEVELS block so the ratio matches
# the price the user sees. (Not optimistic lower/upper bound.)

def _ts_parse_price_token(raw):
    """Parse a single price token like '$76,800' or '76800.5' -> float or None."""
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "").replace("$", "").replace("\u20a6", "")
    s = re.sub(r"[^\d.\-]", "", s)
    if not s or s in (".", "-", "-."):
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _ts_extract_trade_levels(ai_text):
    """
    Extract entry/stop/TP1/TP2/TP3 from free-form AI trade-setup text.
    Returns dict with floats (and entry_low/entry_high if a zone was given).
    Missing keys are None.

    AI R:R claim lines are stripped first so phrases like "TP1 3:1" cannot
    be mistaken for price levels.
    """
    if not ai_text:
        return {}
    text = _ts_strip_ai_rr_claims(ai_text)

    def _first(patterns, group=1):
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                return _ts_parse_price_token(m.group(group))
        return None

    # Entry zone: $76,800–$77,000 or 76800-77000
    entry_low = entry_high = entry = None
    m = re.search(
        r"Entry(?:\s*(?:zone|price|range))?[:\s]+\$?([\d,\.]+)\s*[-–—to]+\s*\$?([\d,\.]+)",
        text, re.IGNORECASE,
    )
    if m:
        entry_low = _ts_parse_price_token(m.group(1))
        entry_high = _ts_parse_price_token(m.group(2))
        if entry_low and entry_high:
            if entry_low > entry_high:
                entry_low, entry_high = entry_high, entry_low
            entry = (entry_low + entry_high) / 2.0  # midpoint rule
    if entry is None:
        entry = _first([
            r"Entry(?:\s*(?:zone|price))?[:\s]+\$?([\d,\.]+)",
            r"Entry\s+\$?([\d,\.]+)",
        ])

    stop = _first([
        r"Stop(?:\s*Loss)?[:\s]+\$?([\d,\.]+)",
        r"SL[:\s]+\$?([\d,\.]+)",
    ])
    # Prefer explicit "Target N" / "Take Profit N" labels; require a price-like
    # token (optional $). Order matters — never match "TP1 3:1" R:R claims.
    tp1 = _first([
        r"Target\s*1[:\s]+\$?([\d,\.]+)",
        r"Take\s*Profit\s*1[:\s]+\$?([\d,\.]+)",
        r"\bTP\s*1[:\s]+\$([\d,\.]+)",  # require $ so "TP1 3:1" is ignored
    ])
    tp2 = _first([
        r"Target\s*2[:\s]+\$?([\d,\.]+)",
        r"Take\s*Profit\s*2[:\s]+\$?([\d,\.]+)",
        r"\bTP\s*2[:\s]+\$([\d,\.]+)",
    ])
    tp3 = _first([
        r"Target\s*3[:\s]+\$?([\d,\.]+)",
        r"Take\s*Profit\s*3[:\s]+\$?([\d,\.]+)",
        r"\bTP\s*3[:\s]+\$([\d,\.]+)",
    ])
    # Single "Target: $X" fallback when only one target is given
    if tp1 is None:
        tp1 = _first([r"(?<!\d\s)Target[:\s]+\$?([\d,\.]+)"])

    direction = None
    dm = re.search(r"\b(Long|Short|Buy|Sell)\b", text, re.IGNORECASE)
    if dm:
        d = dm.group(1).lower()
        direction = "short" if d in ("short", "sell") else "long"
    # Infer from levels if not stated
    if direction is None and entry and stop:
        direction = "short" if stop > entry else "long"

    return {
        "entry": entry,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "direction": direction,
    }


def _ts_calc_rr(entry, stop, target, direction):
    """
    Direction-aware R:R. Returns float or None if invalid.
    SHORT: risk = stop - entry; reward = entry - target
    LONG:  risk = entry - stop; reward = target - entry
    """
    try:
        e, s, t = float(entry), float(stop), float(target)
    except (TypeError, ValueError):
        return None
    if e <= 0 or s <= 0 or t <= 0:
        return None
    if direction == "short":
        if not (s > e and t < e):
            return None
        risk = s - e
        reward = e - t
    else:  # long
        if not (s < e and t > e):
            return None
        risk = e - s
        reward = t - e
    if risk <= 0:
        return None
    return reward / risk


def _ts_format_rr(rr):
    """Consistent display: 0.85:1, 1.55:1, 2.45:1"""
    if rr is None:
        return "n/a"
    return f"{rr:.2f}:1"


def _ts_strip_ai_rr_claims(ai_text):
    """Remove lines that claim authoritative R:R so they cannot contradict code."""
    if not ai_text:
        return ai_text
    cleaned = []
    for line in ai_text.splitlines():
        if re.search(
            r"(risk\s*[:/]\s*reward|r\s*:\s*r|risk-to-reward|risk\s+to\s+reward)\s*[:=]?",
            line, re.IGNORECASE,
        ) and re.search(r"\d", line):
            continue  # drop AI R:R claim lines
        cleaned.append(line)
    return "\n".join(cleaned).strip()


def _ts_build_rr_section(levels):
    """
    Build the authoritative LEVELS + R:R block from parsed numbers.
    Returns (html_section_str, ok: bool). ok=False when levels invalid.
    """
    entry = levels.get("entry")
    stop = levels.get("stop")
    direction = levels.get("direction") or "long"
    tps = [("TP1", levels.get("tp1")), ("TP2", levels.get("tp2")), ("TP3", levels.get("tp3"))]
    tps = [(n, v) for n, v in tps if v is not None]

    if not entry or not stop:
        return (
            "📐 <b>LEVELS / R:R</b>\n"
            "<i>Could not extract a clear Entry and Stop from the AI response. "
            "Treat any R:R in the narrative as unverified.</i>",
            False,
        )

    # Direction / placement validation
    if direction == "short":
        if stop <= entry:
            return (
                "📐 <b>LEVELS / R:R</b>\n"
                f"⚠️ Invalid SHORT levels: stop ({format_price(stop)}) must be above entry ({format_price(entry)}). "
                "R:R not calculated.",
                False,
            )
    else:
        if stop >= entry:
            return (
                "📐 <b>LEVELS / R:R</b>\n"
                f"⚠️ Invalid LONG levels: stop ({format_price(stop)}) must be below entry ({format_price(entry)}). "
                "R:R not calculated.",
                False,
            )

    lines = ["📐 <b>LEVELS + R:R (calculated)</b>"]
    if levels.get("entry_low") and levels.get("entry_high"):
        lines.append(
            f"Entry zone:  <b>{format_price(levels['entry_low'])} – {format_price(levels['entry_high'])}</b>"
        )
        lines.append(
            f"Entry used:  <b>{format_price(entry)}</b>  <i>(midpoint of zone for R:R)</i>"
        )
    else:
        lines.append(f"Entry:       <b>{format_price(entry)}</b>")
    lines.append(f"Stop Loss:   <b>{format_price(stop)}</b>")
    lines.append(f"Direction:   <b>{direction.upper()}</b>")

    any_valid_tp = False
    for name, tp in tps:
        rr = _ts_calc_rr(entry, stop, tp, direction)
        if rr is None:
            lines.append(
                f"{name}:        <b>{format_price(tp)}</b>  — <i>invalid vs entry/stop, R:R skipped</i>"
            )
        else:
            any_valid_tp = True
            lines.append(
                f"{name}:        <b>{format_price(tp)}</b>  ·  R:R <b>{_ts_format_rr(rr)}</b>"
            )

    if not tps:
        lines.append("<i>No take-profit levels could be parsed.</i>")
    elif not any_valid_tp:
        lines.append("<i>No valid take-profit levels for R:R calculation.</i>")

    lines.append("<i>R:R is computed in code from the prices above — not taken from the AI narrative.</i>")
    return "\n".join(lines), True




# ─── Durable daily schedule locks (survive process restart / multi-worker) ──
# In-memory morning_posted/evening_posted flags reset on every Railway
# restart. If the process restarts during the scheduled hour, the same
# evening (or morning) package was sent twice (observed 19:00 + 19:30 WAT).
# These helpers persist a per-day flag in admin_settings so only one send
# wins across restarts and concurrent workers.

def _schedule_lock_key(post_type, wat_date):
    return f"sched_posted_{post_type}_{wat_date.isoformat()}"


def _schedule_already_posted(post_type, wat_date):
    """True if this post_type was already successfully marked for wat_date."""
    key = _schedule_lock_key(post_type, wat_date)
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT value FROM admin_settings WHERE key=%s", (key,))
        row = c.fetchone()
        return bool(row and row[0])
    except Exception as e:
        logger.warning("[SCHEDULER] lock read failed (%s): %s — allowing post" % (key, e))
        return False  # fail-open only if DB down; in-memory flag still helps single process
    finally:
        if db:
            try: db.close()
            except Exception: pass


def _schedule_clear_posted(post_type, wat_date):
    """Remove durable lock so a failed scheduled post can retry same day."""
    key = _schedule_lock_key(post_type, wat_date)
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("DELETE FROM admin_settings WHERE key=%s", (key,))
        db.commit()
        logger.info("[SCHEDULER] Cleared lock %s", key)
    except Exception as e:
        logger.warning("[SCHEDULER] clear lock failed: %s", e)
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass


def _schedule_mark_posted(post_type, wat_date):

    """Persist that post_type was sent for wat_date. Returns True if we acquired the lock (first writer)."""
    key = _schedule_lock_key(post_type, wat_date)
    db = None
    try:
        db = get_db()
        c = db.cursor()
        # Insert-only: if row exists, we are second writer → do not post again
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s, %s, %s) "
            "ON CONFLICT (key) DO NOTHING",
            (key, "1", wat_now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        db.commit()
        # Did we insert? rowcount 1 = we own the lock; 0 = someone else already posted
        return c.rowcount == 1
    except Exception as e:
        logger.warning("[SCHEDULER] lock write failed (%s): %s" % (key, e))
        try:
            if db: db.rollback()
        except Exception:
            pass
        return True  # fail-open for single-process: still allow in-memory path
    finally:
        if db:
            try: db.close()
            except Exception: pass



# ─── extracted section ───
# BTC price at morning briefing — used by midday >2% move gate.
# Module-level so it survives across scheduler loop iterations; reset on new WAT day.
_morning_btc_snapshot = {"price": None, "day": None}
# Admin /refreshprices may clear these; default empty avoids UnboundLocalError.
_kraken_cache = {"data": {}, "timestamp": None}
_secondary_cache = {"data": {}, "timestamp": None}


def run():
    global BOT_MODE, CHANNEL_ID, _kraken_cache, _secondary_cache, _morning_btc_snapshot

    missing = validate_critical_config()
    if missing:
        logger.error(
            "[STARTUP] Critical configuration missing/invalid: %s — "
            "set these Railway Variables and redeploy.",
            ", ".join(missing),
        )
        raise SystemExit(f"Missing critical env: {', '.join(missing)}")
    logger.info("[STARTUP] Critical config OK (BOT_TOKEN + DATABASE_URL present)")
    
    # Load admin config on startup
    config = load_admin_config()
    set_channel_enabled(config.get("CHANNEL_ENABLED", True))
    set_pro_channel_id(config.get("PRO_CHANNEL_ID", None))
    BOT_MODE        = config.get("BOT_MODE", "everyone")
    _mirror_mode_cfg = config.get("MIRROR_MODE", False)
    if isinstance(_mirror_mode_cfg, str):
        _mirror_mode_cfg = _mirror_mode_cfg.lower() in ("true", "1", "yes")
    set_mirror_mode(_mirror_mode_cfg)
    
    init_db()

    # Start WebSocket price engine — persistent streams to Binance, Kraken, Bybit.
    # Prices land in _ws_price_cache; get_best_price() reads from it first.
    # REST fetchers remain as automatic fallbacks if WS data is stale.
    start_ws_price_engine()
    logger.info("[STARTUP] WebSocket price engine launched (Binance + Kraken + Bybit)")

    # Candle history engine (needed by signal_engine.py — EMA/MACD/ADX/etc.
    # can't be computed from a single current price) and the derivatives
    # engine (funding rate / OI / liquidations, Bybit primary + OKX fallback).
    start_candle_engine()
    logger.info("[STARTUP] Candle engine launched (Binance kline stream)")
    start_derivatives_engine(admin_notify=lambda msg: send(next(iter(ADMIN_IDS)), msg) if ADMIN_IDS else None)
    logger.info("[STARTUP] Derivatives engine launched (Bybit primary, OKX fallback)")

    # Load persisted alert watchlist from DB if it was previously set by admin
    try:
        db = get_db(); c = db.cursor()
        c.execute("SELECT value FROM admin_settings WHERE key='alert_watchlist'")
        row = c.fetchone()
        db.close()
        if row:
            saved_coins = json.loads(row[0])
            if saved_coins:
                KEY_ALERT_COINS.clear()
                KEY_ALERT_COINS.extend(saved_coins)
                logger.info(f"[STARTUP] Loaded alert watchlist from DB: {saved_coins}")
    except Exception as e:
        logger.warning(f"[STARTUP] Could not load watchlist from DB: {e}")

    logger.info("=" * 60)
    logger.info("🚀 Market Pulse Bot v22 - Bug Fix Release")
    logger.info("=" * 60)
    logger.info("✅ V17 FIXES ACTIVE:")
    logger.info("  - PostgreSQL syntax fully corrected (no SQLite leftovers)")
    logger.info("  - get_state() tuple handling fixed across all handlers")
    logger.info("  - Whale detection snapshot logic fixed")
    logger.info("  - Admin config loaded from DB + JSON at startup")
    logger.info("  - AI limit resets at WAT midnight")
    logger.info("  - Double AI build on channel posts eliminated")
    logger.info("  - Midday conditional posting (>2% move required)")
    logger.info("  - Admin-configurable alert watchlist via /setwatchlist")
    logger.info("=" * 60)
    logger.info("📊 Bot Mode: %s" % get_bot_mode().upper())
    logger.info("📢 Channel: %s" % ("ENABLED" if get_channel_enabled() else "DISABLED"))
    logger.info("📢 Pro Channel: %s" % (get_pro_channel_id() if get_pro_channel_id() != "-100XXXXXXXXX" else "NOT SET"))
    logger.info("=" * 60)

    last_update_id = 0
    last_morning_post = 0
    last_midday_post = 0
    last_evening_post = 0
    last_weekly_post = 0
    last_health_check = 0
    last_expiry_check = 0
    last_price_save = 0
    last_watchlist_check = 0
    last_daily_digest = 0
    last_key_alert_check = 0
    last_p2p_check = 0
    last_trade_scan = 0
    last_outcome_score = 0
    last_p2p_snapshot = 0
    morning_posted = False
    midday_posted = False
    evening_posted = False
    weekly_posted = False
    educational_posted = False  # Sunday educational content flag
    engine_report_posted = False
    last_day = None

    while True:
        try:
            now = time.time()
            wat = wat_now()
            wat_h = wat.hour
            wat_day = wat.date()

            if wat_day != last_day:
                morning_posted = False
                midday_posted = False
                evening_posted = False
                _morning_btc_snapshot["price"] = None
                _morning_btc_snapshot["day"] = None
                weekly_posted = False if wat.weekday() != SCHEDULE["weekly_edge_day"] else weekly_posted
                educational_posted = False if wat.weekday() != 6 else educational_posted
                engine_report_posted = False
                last_day = wat_day

            # ── DAILY ENGINE REPORT (private admin, 22:00 WAT) ────────────
            if wat_h == 22 and not engine_report_posted:
                try:
                    threading.Thread(
                        target=send_daily_engine_report,
                        name="DailyEngineReport",
                        daemon=True,
                    ).start()
                except Exception as e:
                    logger.error("[ENGINE REPORT] %s" % e)
                engine_report_posted = True

            # ── HEALTH CHECK ──────────────────────────────────────────────────
            if now - last_health_check >= 600:
                logger.info("[HEALTH] Health check passed")
                last_health_check = now

            # ── EXPIRY REMINDERS ─────────────────────────────────────────────
            if now - last_expiry_check >= 3600:
                last_expiry_check = now

            # ── PRICE HISTORY ────────────────────────────────────────────────
            if now - last_price_save >= 3600:
                save_price_history()
                last_price_save = now

            # ── WATCHLIST ALERTS ─────────────────────────────────────────────
            if now - last_watchlist_check >= 300:
                try:
                    check_watchlist_alerts()
                except Exception as e:
                    logger.error("[WATCHLIST] %s" % e)
                try:
                    check_user_price_alerts()
                except Exception as e:
                    logger.error("[PRICE ALERTS] %s" % e)
                last_watchlist_check = now

            # ── KEY MARKET LEVEL ALERTS ───────────────────────────────────────
            # Poll every 10 minutes for level events — posting is gated inside check_key_market_alerts
            # (proximity, cooldowns, global gap). Not a "post every 10/30 min" schedule.
            if now - last_key_alert_check >= 600:
                try:
                    check_key_market_alerts()
                except Exception as e:
                    logger.error("[KEY ALERT] %s" % e)
                last_key_alert_check = now

            # ── WHALE / BREAKOUT DETECTION ────────────────────────────────────
            
            # ── AUTOMATED TRADE SCANNER ──────────────────────────────────────
            # Runs every 4 hours. Pre-screens coins + forex. Posts best setup to Pro.
            try:
                _scan_iv = get_trade_scan_interval_sec()
            except Exception:
                _scan_iv = 3600
            if now - last_trade_scan >= _scan_iv:
                threading.Thread(
                    target=run_trade_scanner,
                    name="TradeScannerAuto",
                    daemon=True
                ).start()
                last_trade_scan = now

            # Real-time follow-up (private admin notifications on TP/SL/expiry)
            if now - last_outcome_score >= 300:
                try:
                    threading.Thread(
                        target=run_outcome_cycle,
                        name="OutcomeMonitor",
                        daemon=True,
                    ).start()
                except Exception as e:
                    logger.error("[OUTCOME MONITOR] %s" % e)
                try:
                    threading.Thread(
                        target=score_open_trade_ideas,
                        name="OutcomeScorer",
                        daemon=True,
                    ).start()
                except Exception as e:
                    logger.error("[OUTCOME SCORE] %s" % e)
                try:
                    threading.Thread(
                        target=run_shadow_cycle,
                        name="ShadowVerifier",
                        daemon=True,
                    ).start()
                except Exception as e:
                    logger.error("[SHADOW] %s" % e)
                last_outcome_score = now

            # ── P2P RATE MONITORING ───────────────────────────────────────────
            if now - last_p2p_check >= 900:   # check every 15 min
                try:
                    check_p2p_rate_alerts()
                except Exception as e:
                    logger.error("[P2P CHECK] %s" % e)
                try:
                    check_user_p2p_alerts()
                except Exception as e:
                    logger.error("[P2P USER ALERTS] %s" % e)
                last_p2p_check = now

            if now - last_p2p_snapshot >= 3600:
                try:
                    threading.Thread(
                        target=record_all_p2p_snapshots,
                        name="P2PSnapshot",
                        daemon=True,
                    ).start()
                except Exception as e:
                    logger.error("[P2P SNAPSHOT] %s" % e)
                try:
                    check_channel_usdt_ngn_pulse()
                except Exception as e:
                    logger.error("[P2P PULSE] %s" % e)
                last_p2p_snapshot = now

            # ── DAILY DIGEST — fires at 8AM WAT each day ──────────────────
            if wat_h == SCHEDULE["admin_digest_hour_wat"] and (now - last_daily_digest >= 3600):
                try:
                    daily_digest()
                except Exception as e:
                    logger.error("[DAILY DIGEST] %s" % e)
                last_daily_digest = now

            # ── CHANNEL POSTS ─────────────────────────────────────────────────
            # C1 FIX: Content Engine always runs in a daemon thread so it
            # never blocks the poll loop (each call makes 7 AI requests,
            # up to 12 minutes blocking time if run synchronously).
            if get_channel_enabled():
                if wat_h == SCHEDULE["morning_hour_wat"] and not morning_posted:
                    morning_posted = True  # in-memory: prevent re-entry this process
                    if _schedule_already_posted("morning", wat_day) or not _schedule_mark_posted("morning", wat_day):
                        logger.info("[CHANNEL] Morning briefing already sent today — skip (durable lock)")
                        continue
                    logger.info("[CHANNEL] Morning briefing")
                    try:
                        pro_content = build_morning_briefing_pro()
                        # Snapshot BTC price at morning for midday conditional check
                        _morning_btc_price, _ = get_best_price("BTC")
                        if _morning_btc_price and float(_morning_btc_price) > 0:
                            _morning_btc_snapshot["price"] = float(_morning_btc_price)
                            _morning_btc_snapshot["day"] = str(wat_day)
                        else:
                            logger.warning("[CHANNEL] Morning BTC snapshot skipped — price unavailable")
                        if get_bot_mode() == "everyone":
                            post_to_channel(pro_content)
                        else:
                            post_to_channel(build_morning_briefing())
                        post_to_pro_channel(pro_content)
                    except Exception as _me:
                        # Release durable lock so a later process/admin can retry.
                        # Keep morning_posted=True this process to avoid a tight retry loop
                        # every poll cycle during the morning hour.
                        logger.error("[CHANNEL] Morning post failed — releasing lock: %s", _me)
                        try:
                            _schedule_clear_posted("morning", wat_day)
                        except Exception as _cle:
                            logger.error("[CHANNEL] Morning lock clear failed: %s", _cle)
                        # do not re-raise — main loop continues; no rapid NameError spam
                    # Morning Pro Package — crypto + forex + P2P setups in background
                    threading.Thread(
                        target=run_morning_pro_package,
                        name="MorningProPackage",
                        daemon=True
                    ).start()
                    logger.info("[SCHEDULER] Morning Pro Package thread started")
                    # Content Engine runs in background thread
                    try:
                        btc_p, btc_c = get_best_price("BTC")
                        fg_d = get_fear_greed()
                        g, l = get_gainers_losers()
                        buy_r, sell_r, _ = get_p2p_rate("USDT","NGN")
                        mc = {
                            "btc_price": format_price(btc_p),
                            "btc_change": format_change(btc_c),
                            "fg_val": fg_d[0]["value"] if fg_d else "50",
                            "fg_lbl": fg_d[0]["value_classification"] if fg_d else "Neutral",
                            "gainers_str": ", ".join(f"{c} {ch:+.1f}%" for c,_,ch in g[:3]) if g else "N/A",
                            "losers_str": ", ".join(f"{c} {ch:.1f}%" for c,_,ch in l[:2]) if l else "N/A",
                            "p2p_str": f"USDT/NGN Buy ₦{int(buy_r):,} / Sell ₦{int(sell_r):,}" if buy_r else "",
                            "key_insight": "Morning market brief for Nigerian traders",
                        }
                        _t = threading.Thread(
                            target=generate_and_deliver_content_package,
                            args=("morning", pro_content, mc, "scheduled_morning"),
                            daemon=True
                        )
                        _t.start()
                    except Exception as ce:
                        logger.error(f"[CONTENT ENGINE] Morning thread error: {ce}")

                if wat_h == SCHEDULE["midday_hour_wat"] and not midday_posted:
                    midday_posted = True
                    if _schedule_already_posted("midday", wat_day) or not _schedule_mark_posted("midday", wat_day):
                        logger.info("[CHANNEL] Midday already sent today — skip (durable lock)")
                    else:
                      # V2 SPEC: Mid-day update only if market moved >2% since morning
                      btc_now, _ = get_best_price("BTC")
                      btc_morning = None
                      if _morning_btc_snapshot.get("day") == str(wat_day):
                          btc_morning = _morning_btc_snapshot.get("price")
                      significant_move = True  # if no morning snapshot, still allow midday
                      if btc_now and btc_morning and btc_morning > 0:
                          pct_move = abs((btc_now - btc_morning) / btc_morning * 100)
                          if pct_move < 2.0:
                              significant_move = False
                              logger.info(f"[CHANNEL] Midday skipped — BTC only moved {pct_move:.2f}% since morning (threshold: 2%)")
                      if significant_move:
                        logger.info("[CHANNEL] Midday snapshot — significant market move detected")
                        pro_content = build_midday_snapshot_pro()
                        if get_bot_mode() == "everyone":
                            post_to_channel(pro_content)
                        else:
                            post_to_channel(build_midday_snapshot())
                        post_to_pro_channel(pro_content)
                        try:
                            btc_p, btc_c = get_best_price("BTC")
                            fg_d = get_fear_greed()
                            buy_r, sell_r, _ = get_p2p_rate("USDT","NGN")
                            mc = {
                                "btc_price": format_price(btc_p),
                                "btc_change": format_change(btc_c),
                                "fg_val": fg_d[0]["value"] if fg_d else "50",
                                "fg_lbl": fg_d[0]["value_classification"] if fg_d else "Neutral",
                                "p2p_str": f"USDT/NGN Buy ₦{int(buy_r):,} / Sell ₦{int(sell_r):,}" if buy_r else "",
                                "key_insight": "Midday market update — significant move detected",
                            }
                            threading.Thread(
                                target=generate_and_deliver_content_package,
                                args=("midday", pro_content, mc, "scheduled_midday"),
                                daemon=True
                            ).start()
                        except Exception as ce:
                            logger.error(f"[CONTENT ENGINE] Midday thread error: {ce}")

                if wat_h == SCHEDULE["evening_hour_wat"] and not evening_posted:
                    evening_posted = True
                    if _schedule_already_posted("evening", wat_day) or not _schedule_mark_posted("evening", wat_day):
                        logger.info("[CHANNEL] Evening recap already sent today — skip (durable lock)")
                    else:
                      logger.info("[CHANNEL] Evening recap")
                      pro_content = build_evening_recap_pro()
                      if get_bot_mode() == "everyone":
                          post_to_channel(pro_content)
                      else:
                          post_to_channel(build_evening_recap())
                      post_to_pro_channel(pro_content)
                      try:
                          post_to_pro_channel(format_multi_p2p_intelligence(
                              title="P2P INTELLIGENCE — EVENING READ"
                          ))
                      except Exception as e:
                          logger.error("[EVENING P2P] %s" % e)
                      try:
                          btc_p, btc_c = get_best_price("BTC")
                          fg_d = get_fear_greed()
                          g, l = get_gainers_losers()
                          buy_r, sell_r, _ = get_p2p_rate("USDT","NGN")
                          mc = {
                              "btc_price": format_price(btc_p),
                              "btc_change": format_change(btc_c),
                              "fg_val": fg_d[0]["value"] if fg_d else "50",
                              "fg_lbl": fg_d[0]["value_classification"] if fg_d else "Neutral",
                              "gainers_str": ", ".join(f"{c} {ch:+.1f}%" for c,_,ch in g[:3]) if g else "N/A",
                              "losers_str": ", ".join(f"{c} {ch:.1f}%" for c,_,ch in l[:2]) if l else "N/A",
                              "p2p_str": f"USDT/NGN Buy ₦{int(buy_r):,} / Sell ₦{int(sell_r):,}" if buy_r else "",
                              "key_insight": "Evening market recap and tomorrow plan",
                          }
                          threading.Thread(
                              target=generate_and_deliver_content_package,
                              args=("evening", pro_content, mc, "scheduled_evening"),
                              daemon=True
                          ).start()
                      except Exception as ce:
                          logger.error(f"[CONTENT ENGINE] Evening thread error: {ce}")

                if (wat.weekday() == SCHEDULE["weekly_edge_day"] and
                        wat_h == SCHEDULE["weekly_edge_hour"] and
                        not weekly_posted):
                    weekly_posted = True
                    if _schedule_already_posted("weekly", wat_day) or not _schedule_mark_posted("weekly", wat_day):
                        logger.info("[CHANNEL] Weekly Edge already sent today — skip (durable lock)")
                    else:
                      logger.info("[CHANNEL] Weekly Edge")
                      pro_content = build_weekly_edge_pro()
                      if get_bot_mode() == "everyone":
                          post_to_channel(pro_content)
                      else:
                          post_to_channel(build_weekly_edge())
                      post_to_pro_channel(pro_content)
                      try:
                          btc_p, btc_c = get_best_price("BTC")
                          fg_d = get_fear_greed()
                          buy_r, sell_r, _ = get_p2p_rate("USDT","NGN")
                          g, l = get_gainers_losers()
                          mc = {
                              "btc_price": format_price(btc_p),
                              "btc_change": format_change(btc_c),
                              "fg_val": fg_d[0]["value"] if fg_d else "50",
                              "fg_lbl": fg_d[0]["value_classification"] if fg_d else "Neutral",
                              "gainers_str": ", ".join(f"{c} {ch:+.1f}%" for c,_,ch in g[:3]) if g else "N/A",
                              "losers_str": ", ".join(f"{c} {ch:.1f}%" for c,_,ch in l[:2]) if l else "N/A",
                              "p2p_str": f"USDT/NGN Buy ₦{int(buy_r):,} / Sell ₦{int(sell_r):,}" if buy_r else "",
                              "key_insight": "Saturday weekly intelligence report",
                          }
                          threading.Thread(
                              target=generate_and_deliver_content_package,
                              args=("weekly", pro_content, mc, "scheduled_weekly"),
                              daemon=True
                          ).start()
                      except Exception as ce:
                          logger.error(f"[CONTENT ENGINE] Weekly thread error: {ce}")

                # C3 FIX: Educational content fires on SUNDAY at 9AM WAT.
                # Was previously nested inside the Saturday block — making it
                # unreachable (wat.weekday() cannot be both 5 and 6).
                # Private weekly performance → ADMIN_IDS only (never auto Pro)
                if wat.weekday() == 6 and wat_h == 9:
                    try:
                        send_weekly_report_private(force=False)
                    except Exception as _wr:
                        logger.error("[WEEKLY REPORT] %s", _wr)

                if wat.weekday() == 6 and wat_h == 9 and not educational_posted:
                    logger.info("[CHANNEL] Sunday educational content")
                    educational_posted = True
                    threading.Thread(
                        target=build_weekly_educational_content,
                        daemon=True
                    ).start()

            # ── POLLING ───────────────────────────────────────────────────────
            updates = request_json(
                "GET", "https://api.telegram.org/bot%s/getUpdates" % BOT_TOKEN,
                params={"offset": last_update_id, "timeout": 10},
                timeout=20, retries=2, backoff=1.0
            ) or {}

            for u in updates.get("result", []):
                last_update_id = u["update_id"] + 1

                if "message" in u:
                    msg = u["message"]
                    chat_id = msg["chat"]["id"]
                    text = msg.get("text", "")
                    username = msg["from"].get("username", "")
                    first_name = msg["from"].get("first_name", "")

                    if not text:
                        continue

                    _is_new_user = upsert_user(chat_id, username, first_name)
                    log_event(chat_id, text if text.startswith("/") else "text_reply")

                    # ── CHECK BANNED ───────────────────────────────────────────
                    if is_user_banned(chat_id):
                        send(chat_id, "🔒 You are banned from using this bot.")
                        continue

                    # ── CHANNEL LOCK CHECK ────────────────────────────────────
                    if not is_user_in_channel(chat_id) and chat_id not in ADMIN_IDS:
                        send(chat_id,
                             "🔒 <b>Channel Membership Required</b>\n\n"
                             "To use Market Pulse you must join our free channel first:\n\n"
                             "👉 @marketpulseng\n\n"
                             "Join then tap the button below.",
                             [[{"text": "✅ Verified", "callback_data": "verify_join"}]])
                        continue

                    # ═══════════════════════════════════════════════════════════
                    # 🔴 ADMIN COMMANDS (HIDDEN FROM USERS)
                    # ═══════════════════════════════════════════════════════════
                    if chat_id in ADMIN_IDS:
                        # ── ADMIN HELP ──────────────────────────────────────────
                        if text.startswith("/adminhelp"):
                            help_text = (
                                "👑 <b>Admin Commands</b>\n\n"
                                "<b>POSTS</b>\n"
                                "/postnow morning|midday|evening|weekly\n"
                                "/contentpackage morning|midday|evening|weekly\n\n"
                                "<b>TRADES</b>\n"
                                "/trade [COIN] steady|momentum|edge\n"
                                "/tradehistory [COIN] [tier]\n"
                                "/closetrade [ID] hit_t1|hit_t2|stopped|cancelled\n\n"
                                "<b>USERS</b>\n"
                                "/grantpro [ID] [months]\n"
                                "/ban [ID] — /unban [ID]\n"
                                "/broadcast [message]\n\n"
                                "<b>SYSTEM</b>\n"
                                "/stats — /dashboard\n"
                                "/mode everyone|pro\n"
                                "/togglechannel\n"
                                "/packages — /package [ID]\n\n"
                                "Or use /admin for the menu."
                            )
                            send(chat_id, help_text)
                            continue

                        # ── MODE ──────────────────────────────────────────────────
                        if text.startswith("/mode everyone"):
                            set_bot_mode("everyone")
                            config = load_admin_config()
                            config["BOT_MODE"] = "everyone"
                            save_admin_config(config)
                            send(chat_id, "✅ Mode changed to: <b>Everyone Free</b>\n\nAll features are now FREE for everyone.")
                            logger.info("[ADMIN] %s set mode to everyone" % chat_id)
                            continue

                        if text.startswith("/mode pro"):
                            set_bot_mode("pro")
                            config = load_admin_config()
                            config["BOT_MODE"] = "pro"
                            save_admin_config(config)
                            send(chat_id, "✅ Mode changed to: <b>Free & Pro</b>\n\nFree users get limited features. Pro users get everything.")
                            logger.info("[ADMIN] %s set mode to pro" % chat_id)
                            continue

                        # ── GRANT PRO ─────────────────────────────────────────────
                        if text.startswith("/grantpro"):
                            parts = text.split()
                            if len(parts) >= 2:
                                try:
                                    target = int(parts[1])
                                    months = int(parts[2]) if len(parts) >= 3 else 1
                                    if grant_pro(target, months):
                                        send(chat_id, f"✅ Pro granted to <code>{target}</code> for <b>{months}</b> month(s)")
                                        logger.info("[ADMIN] %s granted Pro to %s for %s months" % (chat_id, target, months))
                                    else:
                                        send(chat_id, "❌ Failed to grant Pro.")
                                except Exception as _e:
                                    send(chat_id, "⚠️ Usage: /grantpro CHATID [MONTHS]")
                            else:
                                send(chat_id, "⚠️ Usage: /grantpro CHATID [MONTHS]")
                            continue

                        # ── STATS ──────────────────────────────────────────────────
                        if text.startswith("/stats"):
                            try:
                                db = get_db()
                                c = db.cursor()
                                c.execute("SELECT COUNT(*) FROM users")
                                total_users = c.fetchone()[0]
                                c.execute("SELECT COUNT(*) FROM pro_subscriptions")
                                total_pro = c.fetchone()[0]
                                c.execute("SELECT COUNT(*) FROM alerts WHERE active=1")
                                total_alerts = c.fetchone()[0]
                                c.execute("SELECT COUNT(*) FROM p2p_alerts WHERE active=1")
                                total_p2p_alerts = c.fetchone()[0]
                                c.execute("SELECT COUNT(*) FROM trade_journal")
                                total_trades = c.fetchone()[0]
                                since_24h = (wat_now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
                                c.execute("SELECT COUNT(*) FROM events WHERE timestamp > %s", (since_24h,))
                                active_24h = c.fetchone()[0]
                                c.execute("SELECT COUNT(*) FROM events")
                                total_events = c.fetchone()[0]
                                c.execute("SELECT COUNT(*) FROM watchlists")
                                total_watchlist = c.fetchone()[0]
                                c.execute("SELECT COUNT(*) FROM portfolio")
                                total_portfolio = c.fetchone()[0]
                                c.execute("SELECT COUNT(*) FROM banned_users")
                                total_banned = c.fetchone()[0]
                                db.close()
                                
                                mode = get_bot_mode().upper()
                                text = (
                                    "📊 <b>Market Pulse — Admin Stats</b>\n\n"
                                    f"👤 <b>Users</b>\n"
                                    f"  • Total: <b>{total_users:,}</b>\n"
                                    f"  • Pro: <b>{total_pro:,}</b>\n"
                                    f"  • Active (24h): <b>{active_24h:,}</b>\n"
                                    f"  • Banned: <b>{total_banned:,}</b>\n\n"
                                    f"📈 <b>Content & Data</b>\n"
                                    f"  • Alerts: <b>{total_alerts:,}</b>\n"
                                    f"  • P2P Alerts: <b>{total_p2p_alerts:,}</b>\n"
                                    f"  • Watchlist: <b>{total_watchlist:,}</b>\n"
                                    f"  • Portfolio: <b>{total_portfolio:,}</b>\n"
                                    f"  • Trades: <b>{total_trades:,}</b>\n"
                                    f"  • Events: <b>{total_events:,}</b>\n\n"
                                    f"⚙️ <b>System</b>\n"
                                    f"  • Mode: <b>{mode}</b>\n"
                                    f"  • Channel: <b>{'✅ Enabled' if get_channel_enabled() else '❌ Disabled'}</b>\n"
                                    f"  • Pro Channel: <b>{'✅ Set' if get_pro_channel_id() and get_pro_channel_id() != '-100XXXXXXXXX' else '❌ Not Set'}</b>\n\n"
                                    f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S WAT')}"
                                )
                                send(chat_id, text)
                            except Exception as e:
                                logger.error("[STATS ERROR] %s" % e)
                                send(chat_id, f"⚠️ Error: {str(e)}")
                            continue

                        # ── PUBLISH ────────────────────────────────────────────────
                        if text.startswith("/publish"):
                            parts = text.split()
                            if len(parts) < 2:
                                send(chat_id, "⚠️ Usage: /publish morning | midday | evening | weekly")
                                continue

                            post_type = parts[1].lower()

                            if post_type == "morning":
                                pro_content  = build_morning_briefing_pro()
                                free_content = build_morning_briefing()
                            elif post_type == "midday":
                                pro_content  = build_midday_snapshot_pro()
                                free_content = build_midday_snapshot()
                            elif post_type == "evening":
                                pro_content  = build_evening_recap_pro()
                                free_content = build_evening_recap()
                            elif post_type == "weekly":
                                pro_content  = build_weekly_edge_pro()
                                free_content = build_weekly_edge()
                            else:
                                send(chat_id, "⚠️ Types: morning, midday, evening, weekly")
                                continue

                            if not get_channel_enabled():
                                send(chat_id, "⚠️ Channel posting is disabled. Use /togglechannel to enable.")
                                continue

                            try:
                                main_content = pro_content if get_bot_mode() == "everyone" else free_content
                                result = post_to_channel(main_content)
                                if result and result.get("ok"):
                                    send(chat_id, f"✅ Published <b>{post_type}</b> to main channel")
                                    logger.info("[ADMIN] %s published %s" % (chat_id, post_type))
                                    if get_pro_channel_id() and get_pro_channel_id() != "-100XXXXXXXXX":
                                        post_to_pro_channel(pro_content)
                                        send(chat_id, "✅ Also published to Pro channel")
                                else:
                                    send(chat_id, f"❌ Failed to post: {result}")
                            except Exception as e:
                                logger.error("[PUBLISH ERROR] %s" % e)
                                send(chat_id, f"❌ Error: {e}")
                            continue

                        # ── BROADCAST ──────────────────────────────────────────────
                        if text.startswith("/broadcast"):
                            message = text.replace("/broadcast", "", 1).strip()
                            if not message:
                                send(chat_id, "⚠️ Usage: /broadcast Your message here")
                                continue
                            send(chat_id, f"📢 Send to ALL users?\n\nMessage:\n{message}\n\nReply with <b>/confirm_broadcast</b> to send, or <b>/cancel</b> to stop.")
                            set_state(chat_id, "awaiting_broadcast_confirm", {"message": message})
                            continue

                        if text.startswith("/confirm_broadcast"):
                            state, state_data = get_state(chat_id)
                            if state != "awaiting_broadcast_confirm" or not state_data:
                                send(chat_id, "⚠️ No broadcast pending.")
                                continue
                            message = state_data.get("message")
                            if not message:
                                send(chat_id, "⚠️ No message found.")
                                continue
                            db = get_db()
                            c = db.cursor()
                            c.execute("SELECT chat FROM users")
                            users = c.fetchall()
                            db.close()
                            sent = 0
                            failed = 0
                            send(chat_id, f"📢 Broadcasting to <b>{len(users)}</b> users...")
                            for (user_chat,) in users:
                                if is_user_banned(user_chat):
                                    continue
                                try:
                                    send(int(user_chat), f"📢 <b>Announcement</b>\n\n{message}")
                                    sent += 1
                                except Exception as _e:
                                    failed += 1
                                time.sleep(0.05)
                            clear_state(chat_id)
                            logger.info("[ADMIN] %s broadcast to %s users" % (chat_id, sent))
                            send(chat_id, f"✅ Broadcast complete!\nSent: {sent}\nFailed: {failed}")
                            continue

                        # ── USERS ───────────────────────────────────────────────────
                        if text.startswith("/users"):
                            try:
                                db = get_db()
                                c = db.cursor()
                                c.execute("SELECT COUNT(*) FROM users")
                                total = c.fetchone()[0]
                                c.execute("SELECT COUNT(*) FROM pro_subscriptions")
                                pro = c.fetchone()[0]
                                c.execute("SELECT COUNT(*) FROM users WHERE last_seen >= %s",
                                          ((datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S"),))
                                recent = c.fetchone()[0]
                                c.execute("SELECT chat, username, first_name, last_seen FROM users ORDER BY id DESC LIMIT 20")
                                rows = c.fetchall()
                                db.close()
                                
                                lines = [
                                    f"👤 <b>User Statistics</b>\n",
                                    f"Total: <b>{total:,}</b>",
                                    f"Pro: <b>{pro:,}</b>",
                                    f"Active (7d): <b>{recent:,}</b>",
                                    "",
                                    "━━━━━━━━━━━━━━━━━━━━━━━━━",
                                    "",
                                    "📋 <b>Recent Users (last 20)</b>"
                                ]
                                for chat, username, first_name, last_seen in rows:
                                    name = first_name or username or str(chat)
                                    if is_pro(chat):
                                        name = f"⭐ {name}"
                                    lines.append(f"• {name[:25]} ({chat})")
                                send(chat_id, "\n".join(lines))
                            except Exception as e:
                                logger.error("[USERS ERROR] %s" % e)
                                send(chat_id, f"⚠️ Error: {e}")
                            continue

                        # ── TEST ────────────────────────────────────────────────────
                        if text.startswith("/test"):
                            send(chat_id, "🧪 <b>Running tests...</b>")
                            results = []
                            try:
                                result = post_to_channel("🧪 <b>Test</b>\n\nBot is online and posting correctly.")
                                results.append(("Main Channel", "✅" if result and result.get("ok") else "❌"))
                            except Exception as _e:
                                results.append(("Main Channel", "❌ Error"))
                            if get_pro_channel_id() and get_pro_channel_id() != "-100XXXXXXXXX":
                                try:
                                    result = post_to_pro_channel("🧪 <b>Test</b>\n\nBot is online.")
                                    results.append(("Pro Channel", "✅" if result and result.get("ok") else "❌"))
                                except Exception as _e:
                                    results.append(("Pro Channel", "❌ Error"))
                            else:
                                results.append(("Pro Channel", "⏳ Not set"))
                            try:
                                ai, provider = ask_ai("Say hello in one word")
                                results.append(("AI Service", f"✅ {provider}" if ai else "❌"))
                            except Exception as _e:
                                results.append(("AI Service", "❌"))
                            try:
                                price, change = get_best_price("BTC")
                                results.append(("Price API", f"✅ {format_price(price)}" if price else "❌"))
                            except Exception as _e:
                                results.append(("Price API", "❌"))
                            try:
                                buy, sell, source = get_p2p_rate("USDT", "NGN")
                                results.append(("P2P", f"✅ ₦{int(buy)}" if buy else "❌"))
                            except Exception as _e:
                                results.append(("P2P", "❌"))
                            try:
                                news = get_crypto_news()
                                results.append(("News", f"✅ {len(news) if news else 0} articles" if news else "❌"))
                            except Exception as _e:
                                results.append(("News", "❌"))
                            lines = ["🧪 <b>Test Results</b>\n"]
                            for name, status in results:
                                lines.append(f"{name}: {status}")
                            send(chat_id, "\n".join(lines))
                            continue

                        # ── HEALTH ──────────────────────────────────────────────────
                        if text.startswith("/opsreport") or text.startswith("/ops"):
                            try:
                                from market_pulse.trade_engine_report import build_ops_diagnostic_report
                                send(chat_id, build_ops_diagnostic_report())
                            except Exception as _oe:
                                logger.error("[OPS REPORT] %s", _oe)
                                send(chat_id, f"❌ Ops report failed: {_oe}")
                            continue

                        if text.startswith("/health"):
                            send(chat_id, "🔍 Running health check...")
                            checks = []
                            try:
                                price, _ = get_best_price("BTC")
                                checks.append(("Prices", "✅" if price else "❌", f"BTC {format_price(price)}" if price else "Failed"))
                            except Exception as _e:
                                checks.append(("Prices", "❌", "Failed"))
                            try:
                                buy, sell, source = get_p2p_rate("USDT", "NGN")
                                checks.append(("P2P", "✅" if buy else "❌", f"{source}" if buy else "Failed"))
                            except Exception as _e:
                                checks.append(("P2P", "❌", "Failed"))
                            try:
                                news = get_crypto_news()
                                checks.append(("News", "✅" if news else "❌", f"{len(news) if news else 0} articles"))
                            except Exception as _e:
                                checks.append(("News", "❌", "Failed"))
                            try:
                                fg = get_fear_greed()
                                checks.append(("Fear & Greed", "✅" if fg else "❌", f"{fg[0]['value'] if fg else 'N/A'}/100"))
                            except Exception as _e:
                                checks.append(("Fear & Greed", "❌", "Failed"))
                            try:
                                ai_result, provider = ask_ai("Test")
                                checks.append(("AI", "✅" if ai_result else "❌", provider or "All failed"))
                            except Exception as _e:
                                checks.append(("AI", "❌", "All failed"))
                            try:
                                db = get_db()
                                c = db.cursor()
                                c.execute("SELECT COUNT(*) FROM users")
                                count = c.fetchone()[0]
                                db.close()
                                checks.append(("Database", "✅", f"{count} users"))
                            except Exception as _e:
                                checks.append(("Database", "❌", "Connection failed"))
                            lines = ["🏥 <b>Health Check</b>\n", "<code>Service       Status   Details", "─────────────────────────────────────"]
                            for service, status, detail in checks:
                                lines.append(f"{service:12} {status:8} {detail}")
                            lines.append("</code>")
                            send(chat_id, "\n".join(lines))
                            continue

                        # ── TOGGLECHANNEL ──────────────────────────────────────────
                        if text.startswith("/togglechannel"):
                            toggle_channel_enabled()
                            status = "ENABLED" if get_channel_enabled() else "DISABLED"
                            send(chat_id, f"✅ Channel posting <b>{status}</b>")
                            logger.info("[ADMIN] %s toggled channel to %s" % (chat_id, status))
                            continue

                        # ── SET PRO CHANNEL ─────────────────────────────────────────
                        if text.startswith("/setprochannel"):
                            parts = text.split()
                            if len(parts) >= 2:
                                new_pro_channel_id = parts[1]
                                set_pro_channel_id(new_pro_channel_id)
                                config = load_admin_config()
                                config["PRO_CHANNEL_ID"] = new_pro_channel_id
                                save_admin_config(config)
                                send(chat_id, f"✅ Pro channel set to: <code>{new_pro_channel_id}</code>")
                                logger.info("[ADMIN] %s set pro channel to %s" % (chat_id, new_pro_channel_id))
                            else:
                                send(chat_id, "⚠️ Usage: /setprochannel -100XXXXXXXXX")
                            continue

                        # ── SET CHANNEL ─────────────────────────────────────────────
                        if text.startswith("/setchannel"):
                            parts = text.split()
                            if len(parts) >= 2:
                                CHANNEL_ID = parts[1]
                                send(chat_id, f"✅ Main channel set to: <code>{CHANNEL_ID}</code>")
                                logger.info("[ADMIN] %s set channel to %s" % (chat_id, CHANNEL_ID))
                            else:
                                send(chat_id, "⚠️ Usage: /setchannel -100XXXXXXXXX")
                            continue

                        # ── REFRESH PRICES ──────────────────────────────────────────
                        if text.startswith("/refreshprices"):
                            send(chat_id, "🔄 Refreshing prices...")
                            try:
                                _kraken_cache = {"data": {}, "timestamp": None}
                                _secondary_cache = {"data": {}, "timestamp": None}
                                get_kraken_batch()
                                get_secondary_batch()
                                send(chat_id, "✅ Prices refreshed successfully!")
                                logger.info("[ADMIN] %s refreshed prices" % chat_id)
                            except Exception as e:
                                logger.error("[REFRESH ERROR] %s" % e)
                                send(chat_id, f"❌ Error: {e}")
                            continue

                        # ── CLEAR STATE ─────────────────────────────────────────────
                        if text.startswith("/clearstate"):
                            parts = text.split()
                            if len(parts) >= 2:
                                try:
                                    target = int(parts[1])
                                    clear_state(target)
                                    send(chat_id, f"✅ Cleared state for <code>{target}</code>")
                                    logger.info("[ADMIN] %s cleared state for %s" % (chat_id, target))
                                except Exception as _e:
                                    send(chat_id, "⚠️ Usage: /clearstate CHATID")
                            else:
                                send(chat_id, "⚠️ Usage: /clearstate CHATID")
                            continue

                        # ── WATCHLIST COINS (ADMIN) ──────────────────────────────────
                        if text.startswith("/watchlistcoins"):
                            coins_list = ", ".join(KEY_ALERT_COINS)
                            send(chat_id,
                                f"📋 <b>Alert Watchlist ({len(KEY_ALERT_COINS)} coins)</b>\n\n"
                                f"<code>{coins_list}</code>\n\n"
                                f"Use /setwatchlist COIN1 COIN2 ... to change.\n"
                                f"Available: {', '.join(list(COINS.keys()))}")
                            continue

                        # ── SET WATCHLIST (ADMIN) ────────────────────────────────────
                        if text.startswith("/setwatchlist"):
                            parts = text.split()[1:]
                            if not parts:
                                send(chat_id, "⚠️ Usage: /setwatchlist BTC ETH SOL BNB XRP LINK AVAX SUI")
                                continue
                            valid = [p.upper() for p in parts if p.upper() in COINS]
                            invalid = [p.upper() for p in parts if p.upper() not in COINS]
                            if not valid:
                                send(chat_id, f"❌ No valid coins. Available: {', '.join(list(COINS.keys()))}")
                                continue
                            KEY_ALERT_COINS.clear()
                            KEY_ALERT_COINS.extend(valid)
                            # Save to DB
                            try:
                                db = get_db(); c = db.cursor()
                                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                c.execute("INSERT INTO admin_settings (key, value, updated_at) VALUES ('alert_watchlist',%s,%s) "
                                          "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                                          (json.dumps(valid), now_str))
                                db.commit(); db.close()
                            except Exception as e:
                                logger.error(f"[SETWATCHLIST DB] {e}")
                            msg = f"✅ Alert watchlist updated to <b>{len(valid)} coins</b>:\n<code>{', '.join(valid)}</code>"
                            if invalid:
                                msg += f"\n\n⚠️ Skipped (unknown): {', '.join(invalid)}"
                            send(chat_id, msg)
                            logger.info(f"[ADMIN] {chat_id} set watchlist: {valid}")
                            continue

                        # ── BAN ──────────────────────────────────────────────────────
                        if text.startswith("/ban"):
                            parts = text.split()
                            if len(parts) >= 2:
                                try:
                                    target = int(parts[1])
                                    reason = " ".join(parts[2:]) if len(parts) >= 3 else "No reason provided"
                                    if ban_user(target, reason):
                                        send(chat_id, f"✅ Banned user <code>{target}</code>\nReason: {reason}")
                                        logger.info("[ADMIN] %s banned %s" % (chat_id, target))
                                    else:
                                        send(chat_id, "❌ Failed to ban user.")
                                except Exception as _e:
                                    send(chat_id, "⚠️ Usage: /ban CHATID [REASON]")
                            else:
                                send(chat_id, "⚠️ Usage: /ban CHATID [REASON]")
                            continue

                        # ── UNBAN ────────────────────────────────────────────────────
                        if text.startswith("/unban"):
                            parts = text.split()
                            if len(parts) >= 2:
                                try:
                                    target = int(parts[1])
                                    if unban_user(target):
                                        send(chat_id, f"✅ Unbanned user <code>{target}</code>")
                                        logger.info("[ADMIN] %s unbanned %s" % (chat_id, target))
                                    else:
                                        send(chat_id, "❌ Failed to unban user.")
                                except Exception as _e:
                                    send(chat_id, "⚠️ Usage: /unban CHATID")
                            else:
                                send(chat_id, "⚠️ Usage: /unban CHATID")
                            continue

                        # ── BLACKLIST ────────────────────────────────────────────────
                        if text.startswith("/blacklist"):
                            banned = get_banned_users()
                            if not banned:
                                send(chat_id, "📋 <b>Banned Users</b>\n\nNo users are banned.")
                                continue
                            lines = ["📋 <b>Banned Users</b>\n"]
                            for chat, reason, banned_at in banned[:20]:
                                lines.append(f"• <code>{chat}</code> — {reason[:30]}")
                                lines.append(f"  <i>{banned_at}</i>")
                            if len(banned) > 20:
                                lines.append(f"\n... and {len(banned) - 20} more")
                            send(chat_id, "\n".join(lines))
                            continue

                        # ── LOGS ─────────────────────────────────────────────────────
                        if text.startswith("/logs"):
                            try:
                                with open(LOG_FILE, "r") as f:
                                    lines = f.readlines()[-30:]
                                log_text = "📋 <b>Recent Logs</b>\n\n<code>"
                                for line in lines:
                                    log_text += line[:200] + "\n"
                                log_text += "</code>"
                                send(chat_id, log_text)
                            except Exception as _e:
                                send(chat_id, "⚠️ Could not read logs.")
                            continue

                        # ── POSTNOW ──────────────────────────────────────────────────
                        if text.startswith("/postnow"):
                            parts = text.split()
                            if len(parts) < 2:
                                send(chat_id, "⚠️ Usage: /postnow morning | midday | evening | weekly")
                                continue
                            post_type = parts[1].lower()
                            if post_type == "morning":
                                pro_content  = build_morning_briefing_pro()
                                free_content = build_morning_briefing()
                            elif post_type == "midday":
                                pro_content  = build_midday_snapshot_pro()
                                free_content = build_midday_snapshot()
                            elif post_type == "evening":
                                pro_content  = build_evening_recap_pro()
                                free_content = build_evening_recap()
                            elif post_type == "weekly":
                                pro_content  = build_weekly_edge_pro()
                                free_content = build_weekly_edge()
                            else:
                                send(chat_id, "⚠️ Types: morning, midday, evening, weekly")
                                continue
                            if not get_channel_enabled():
                                send(chat_id, "⚠️ Channel posting is disabled.")
                                continue
                            try:
                                main_content = pro_content if get_bot_mode() == "everyone" else free_content
                                result = post_to_channel(main_content)
                                if result and result.get("ok"):
                                    send(chat_id, f"✅ Posted <b>{post_type}</b> to channel")
                                    logger.info("[ADMIN] %s forced post %s" % (chat_id, post_type))
                                    if get_pro_channel_id() and get_pro_channel_id() != "-100XXXXXXXXX":
                                        post_to_pro_channel(pro_content)
                                else:
                                    send(chat_id, f"❌ Failed: {result}")
                            except Exception as e:
                                logger.error("[POSTNOW ERROR] %s" % e)
                                send(chat_id, f"❌ Error: {e}")
                            continue

                        # ── CANCEL ───────────────────────────────────────────────────
                        if text.startswith("/cancel"):
                            clear_state(chat_id)
                            send(chat_id, "✅ Cancelled.")
                            continue

                        # ── /forex ───────────────────────────────────────────────────
                        if text.startswith("/forex"):
                            parts = text.split()
                            if len(parts) < 3:
                                send(chat_id,
                                    "💱 <b>Forex Trade Idea Generator</b>\n\n"
                                    "Usage: /forex [PAIR] [tier]\n\n"
                                    "Pairs:\n"
                                    "  EUR/USD  GBP/USD\n"
                                    "  EUR/USD   GBP/USD\n\n"
                                    "Tiers: steady | momentum | edge\n\n"
                                    "Examples:\n"
                                    "/forex EUR/USD momentum\n"
                                    "/forex EUR/USD steady\n"
                                    "/forex GBP/USD edge"
                                )
                                continue
                            pair_arg = parts[1].upper().replace("-", "/")
                            tier_arg = parts[2].lower()
                            if pair_arg in ("USDT/NGN", "USD/NGN", "BTC/NGN", "EUR/NGN", "GBP/NGN") or str(pair_arg).upper().endswith("/NGN"):
                                send(chat_id, "⚠️ USDT/NGN is local context/P2P only — not a tradeable setup pair.")
                                continue
                            if pair_arg not in FOREX_PAIRS:
                                send(chat_id, f"⚠️ Unknown pair: {pair_arg}\nAvailable: {', '.join(k for k in FOREX_PAIRS if k != 'USDT/NGN')}")
                                continue
                            if tier_arg not in TRADE_TIERS:
                                send(chat_id, f"⚠️ Unknown tier: {tier_arg}. Use: steady, momentum, edge")
                                continue
                            send(chat_id, f"⏳ Generating <b>{tier_arg.upper()}</b> idea for <b>{pair_arg}</b>...")
                            try:
                                msg, trade, idea_id = generate_forex_trade_idea(pair_arg, tier_arg)
                                if msg and idea_id:
                                    post_to_pro_channel(msg)
                                    send(chat_id,
                                        f"✅ <b>Forex Idea #{idea_id}</b> posted to Pro channel.\n"
                                        f"Pair: {pair_arg} | Tier: {tier_arg.upper()}\n"
                                        f"Use /closetrade {idea_id} [result] when it closes.")
                                else:
                                    send(chat_id, f"⚠️ No quality {tier_arg} setup for {pair_arg} right now.")
                            except Exception as fe:
                                logger.error(f"[/forex CMD] {fe}")
                                send(chat_id, f"❌ Error: {fe}")
                            continue

                        # ── /admin ────────────────────────────────────────────────────
                        if text.strip() == "/admin":
                            send(chat_id, "👑 <b>Admin Panel</b>\n\nSelect a category:", ADMIN_MENU)
                            continue

                        # ── TRADE IDEAS (admin) ───────────────────────────────────────
                        if text.startswith("/trade"):
                            # Usage: /trade BTC momentum | /trade ETH steady | /trade SOL edge
                            parts = text.split()
                            if len(parts) < 3:
                                send(chat_id,
                                    "⚡ <b>Trade Idea Generator</b>\n\n"
                                    "Usage: /trade [COIN] [tier]\n\n"
                                    "Tiers:\n"
                                    "  <b>steady</b>   — Low-medium risk, 8-15% target\n"
                                    "  <b>momentum</b> — Medium-high risk, 15-30% target\n"
                                    "  <b>edge</b>     — HIGH RISK, 30-100%+ target\n\n"
                                    "Examples:\n"
                                    "/trade BTC momentum\n"
                                    "/trade ETH edge\n"
                                    "/trade SOL steady"
                                )
                                continue
                            coin_arg = parts[1].upper()
                            tier_arg = parts[2].lower()
                            if tier_arg not in TRADE_TIERS:
                                send(chat_id, f"⚠️ Unknown tier: {tier_arg}. Use: steady, momentum, edge")
                                continue
                            if coin_arg not in COINS:
                                send(chat_id, f"⚠️ Unknown coin: {coin_arg}. Use one of: {', '.join(list(COINS.keys())[:10])}...")
                                continue
                            send(chat_id, f"⏳ Generating <b>{tier_arg.upper()}</b> idea for <b>{coin_arg}</b>...")
                            try:
                                msg, trade, idea_id = generate_trade_idea(coin_arg, tier_arg)
                                if msg and idea_id:
                                    # Post to Pro channel
                                    post_to_pro_channel(msg)
                                    send(chat_id,
                                        f"✅ <b>Trade Idea #{idea_id}</b> generated and posted to Pro channel.\n\n"
                                        f"Coin: {coin_arg} | Tier: {tier_arg.upper()}\n"
                                        f"Entry: {trade.get('entry','—')} | Stop: {trade.get('stop','—')} | T1: {trade.get('target1','—')}\n\n"
                                        f"Use /closetrade {idea_id} [hit_t1|hit_t2|stopped|cancelled] when trade closes."
                                    )
                                else:
                                    send(chat_id,
                                        f"⚠️ No quality {tier_arg} setup found for {coin_arg} right now.\n"
                                        f"AI could not generate a valid entry/stop/target that meets the tier criteria.\n"
                                        f"Try a different tier or wait for better market structure."
                                    )
                            except Exception as te:
                                logger.error(f"[/trade CMD] {te}")
                                send(chat_id, f"❌ Error: {te}")
                            continue

                        # ── TRADE HISTORY ─────────────────────────────────────────────
                        if text.startswith("/tradehistory") or text.startswith("/trades"):
                            parts = text.split()
                            coin_f = parts[1].upper() if len(parts) > 1 and parts[1].upper() in COINS else None
                            tier_f = parts[2].lower() if len(parts) > 2 and parts[2].lower() in TRADE_TIERS else None
                            rows = get_trade_history(limit=15, coin=coin_f, tier=tier_f)
                            if not rows:
                                send(chat_id, "📋 <b>Trade History</b>\n\nNo trade ideas recorded yet.\n\nGenerate one with /trade [COIN] [tier]")
                            else:
                                lines = ["📋 <b>Trade History</b>", f"<i>Showing last {len(rows)} ideas</i>", ""]
                                for row in rows:
                                    tid, coin, tier, direction, tf, entry, t1, conf, status, created = row
                                    status_emoji = "✅" if status == "closed" else "🟡"
                                    lines.append(
                                        f"{status_emoji} <b>#{tid}</b> {coin} {tier.upper()} {direction} {tf}\n"
                                        f"   Entry: {entry or '—'} → T1: {t1 or '—'} | {conf} | {created[:10]}"
                                    )
                                lines += ["", "Use /closetrade [ID] [result] to close an idea."]
                                send(chat_id, "\n".join(lines))
                            continue

                        # ── CLOSE TRADE ───────────────────────────────────────────────
                        if text.startswith("/closetrade"):
                            parts = text.split()
                            if len(parts) < 3:
                                send(chat_id,
                                    "Usage: /closetrade [ID] [result]\n\n"
                                    "Results: hit_t1 | hit_t2 | stopped | cancelled"
                                )
                                continue
                            try:
                                close_id = int(parts[1])
                                result   = parts[2].lower()
                                valid_results = ("hit_t1","hit_t2","stopped","cancelled")
                                if result not in valid_results:
                                    send(chat_id, f"⚠️ Result must be one of: {', '.join(valid_results)}")
                                    continue
                                ok = close_trade_idea(close_id, result)
                                result_emoji = {"hit_t1":"✅","hit_t2":"🏆","stopped":"❌","cancelled":"⏹"}.get(result,"✅")
                                if ok:
                                    send(chat_id, f"{result_emoji} Trade #{close_id} closed as <b>{result}</b>.\n\nThis will be included in performance tracking.")
                                else:
                                    send(chat_id, f"❌ Could not close trade #{close_id}. Check the ID.")
                            except ValueError:
                                send(chat_id, "⚠️ Invalid ID. Usage: /closetrade 5 hit_t1")
                            continue

                        # ── CONTENT PACKAGE ──────────────────────────────────────────
                        if text.startswith("/contentpackage") or text.startswith("/cp"):
                            parts = text.split()
                            post_type = parts[1].lower() if len(parts) >= 2 else "morning"
                            if post_type not in ("morning","midday","evening","weekly","educational"):
                                send(chat_id,
                                    "📦 <b>Content Package</b>\n\nUsage:\n"
                                    "/contentpackage morning\n"
                                    "/contentpackage midday\n"
                                    "/contentpackage evening\n"
                                    "/contentpackage weekly\n"
                                    "/contentpackage educational")
                                continue
                            send(chat_id, f"⏳ Generating <b>{post_type}</b> content package...\nThis takes ~30 seconds while AI writes each platform format.")
                            try:
                                if post_type == "educational":
                                    tg_text = build_weekly_educational_content()
                                    send(chat_id, f"✅ Educational content package generated and delivered above.")
                                else:
                                    if post_type == "morning":
                                        tg_text = build_morning_briefing_pro()
                                    elif post_type == "midday":
                                        tg_text = build_midday_snapshot_pro()
                                    elif post_type == "evening":
                                        tg_text = build_evening_recap_pro()
                                    else:
                                        tg_text = build_weekly_edge_pro()
                                    btc_p, btc_c = get_best_price("BTC")
                                    fg_d = get_fear_greed()
                                    g, l = get_gainers_losers()
                                    buy_r, sell_r, _ = get_p2p_rate("USDT","NGN")
                                    mc = {
                                        "btc_price": format_price(btc_p),
                                        "btc_change": format_change(btc_c),
                                        "fg_val": fg_d[0]["value"] if fg_d else "50",
                                        "fg_lbl": fg_d[0]["value_classification"] if fg_d else "Neutral",
                                        "gainers_str": ", ".join(f"{c} {ch:+.1f}%" for c,_,ch in g[:3]) if g else "N/A",
                                        "losers_str": ", ".join(f"{c} {ch:.1f}%" for c,_,ch in l[:2]) if l else "N/A",
                                        "p2p_str": f"USDT/NGN Buy ₦{int(buy_r):,} / Sell ₦{int(sell_r):,}" if buy_r else "",
                                        "key_insight": f"Admin-requested {post_type} content package",
                                    }
                                    pkg_id = generate_and_deliver_content_package(post_type, tg_text, mc, f"admin_manual_{post_type}")
                                    send(chat_id, f"✅ <b>Content Package #{pkg_id}</b> generated and delivered above.\n\n"
                                         f"Review each section carefully before publishing to any platform.")
                            except Exception as e:
                                logger.error(f"[CONTENTPACKAGE CMD] {e}")
                                send(chat_id, f"❌ Error generating package: {e}")
                            continue

                        # ── DASHBOARD ─────────────────────────────────────────────────
                        if text.startswith("/dashboard"):
                            send(chat_id, build_admin_dashboard(),
                                [[{"text": "📦 Content Packages", "callback_data": "admin_content_packages"},
                                  {"text": "⬅ Back", "callback_data": "main_menu"}]])
                            continue

                        # ── CONTENT PACKAGES LIST ─────────────────────────────────────
                        if text.startswith("/packages"):
                            pkgs = get_pending_content_packages(limit=10)
                            if not pkgs:
                                send(chat_id, "📦 <b>Content Packages</b>\n\nNo pending packages.")
                            else:
                                lines = ["📦 <b>Pending Content Packages</b>\n"]
                                for pid, ptype, psrc, pdate in pkgs:
                                    lines.append(f"• #{pid} <b>{ptype.upper()}</b> — {psrc} — {pdate[:16]}")
                                lines.append("\nUse /package [ID] to view a specific package.")
                                send(chat_id, "\n".join(lines))
                            continue

                        if text.startswith("/package "):
                            try:
                                pkg_id = int(text.split()[1])
                                pkg = get_content_package_by_id(pkg_id)
                                if not pkg:
                                    send(chat_id, f"❌ Package #{pkg_id} not found.")
                                else:
                                    admin_msg = format_content_package_for_admin(pkg_id, pkg, pkg.get("package_type","?"))
                                    if len(admin_msg) <= 4000:
                                        send(chat_id, admin_msg,
                                            [[{"text": "✅ Approve", "callback_data": f"pkg_approve_{pkg_id}"},
                                              {"text": "🗑 Discard", "callback_data": f"pkg_discard_{pkg_id}"}]])
                                    else:
                                        send(chat_id, admin_msg[:3900] + "...\n\n[truncated — full package in DB]")
                            except (ValueError, IndexError):
                                send(chat_id, "⚠️ Usage: /package [ID]")
                            continue

                    # ═══════════════════════════════════════════════════════════════
                    # 🔵 USER COMMANDS (EVERYONE CAN USE)
                    # ═══════════════════════════════════════════════════════════════

                    # ── START ──────────────────────────────────────────────────────
                    if text.startswith("/start"):
                        clear_state(chat_id)
                        if "ref_PRO_" in text:
                            try:
                                referrer = int(text.split("ref_PRO_")[1].split()[0])
                                record_pro_referral(referrer, chat_id)
                            except Exception as _e:
                                logger.debug("[SILENT EXC] %s" % _e)
                        # Full welcome + trial pitch once (first-ever account only)
                        if _is_new_user:
                            try:
                                send_welcome_onboarding(chat_id)
                            except Exception as _te:
                                logger.debug("[WELCOME] %s", _te)
                        show_main_menu(chat_id)
                        continue

                    # ── HELP ──────────────────────────────────────────────────────
                    if text.startswith("/help") or text.startswith("/commands") or text == "/%s":
                        show_help(chat_id, None)
                        continue

                    # ── MENU ──────────────────────────────────────────────────────
                    if text.startswith("/menu"):
                        show_main_menu(chat_id)
                        continue

                    # ── MARKET ────────────────────────────────────────────────────
                    if text.startswith("/market") or text.startswith("/prices"):
                        show_market(chat_id, None)
                        continue

                    # ── UPGRADE ──────────────────────────────────────────────────
                    if text.startswith("/upgrade") or text.startswith("/pro"):
                        show_upgrade(chat_id, None)
                        continue

                    # ── PORTFOLIO ─────────────────────────────────────────────────
                    if text.startswith("/portfolio") or text.startswith("/port"):
                        show_portfolio(chat_id, None)
                        continue

                    # ── TRADE JOURNAL ─────────────────────────────────────────────
                    if text.startswith("/trades") or text.startswith("/journal"):
                        show_trade_journal(chat_id, None)
                        continue

                    # ── SETTINGS ──────────────────────────────────────────────────
                    if text.startswith("/settings") or text.startswith("/prefs"):
                        show_settings(chat_id, None)
                        continue

                    # ── POSITION CALCULATOR ──────────────────────────────────────
                    if text.startswith("/position") or text.startswith("/pos"):
                        show_position_calculator(chat_id, None)
                        continue

                    # ── VERSION ──────────────────────────────────────────────────
                    if text.startswith("/version") or text.startswith("/ver"):
                        text = (
                            "ℹ️ <b>Market Pulse Bot</b>\n\n"
                            f"📅 Version: <b>v17 - The Intelligence Upgrade</b>\n"
                            f"🤖 Mode: <b>{get_bot_mode().upper()}</b>\n"
                            f"📊 Channel: <b>{'Enabled' if get_channel_enabled() else 'Disabled'}</b>\n"
                            f"👤 Your Status: <b>{get_user_badge(chat_id)}</b>\n\n"
                            f"📅 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S WAT')}"
                        )
                        send(chat_id, text, BACK_MAIN)
                        continue

                    # ── PING ──────────────────────────────────────────────────────
                    if text.startswith("/ping"):
                        send(chat_id, "🏓 <b>Pong!</b>\n\nBot is alive and running.")
                        continue

                    # ── ADD PORTFOLIO ─────────────────────────────────────────────
                    if text.startswith("/addportfolio") or text.startswith("/addport"):
                        parts = text.split()
                        if len(parts) >= 4:
                            try:
                                coin = parts[1].upper()
                                amount = float(parts[2])
                                buy_price = float(parts[3])
                                db = get_db()
                                c = db.cursor()
                                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                c.execute("INSERT INTO portfolio (chat, coin, amount, buy_price, added_at) VALUES (%s,%s,%s,%s,%s)",
                                          (str(chat_id), coin, amount, buy_price, now))
                                db.commit()
                                db.close()
                                send(chat_id, f"✅ Added {amount} {coin} @ {format_price(buy_price)}")
                            except Exception as _e:
                                send(chat_id, "⚠️ Format: /addportfolio BTC 0.5 61000")
                        else:
                            send(chat_id, "⚠️ Format: /addportfolio BTC 0.5 61000")
                        continue

                    # ── REMOVE PORTFOLIO ──────────────────────────────────────────
                    if text.startswith("/removeportfolio") or text.startswith("/removeport"):
                        parts = text.split()
                        if len(parts) >= 2:
                            try:
                                coin = parts[1].upper()
                                db = get_db()
                                c = db.cursor()
                                c.execute("DELETE FROM portfolio WHERE chat=%s AND coin=%s", (str(chat_id), coin))
                                db.commit()
                                db.close()
                                send(chat_id, f"✅ Removed {coin} from portfolio")
                            except Exception as _e:
                                send(chat_id, "⚠️ Error removing coin.")
                        else:
                            send(chat_id, "⚠️ Usage: /removeportfolio COIN")
                        continue

                    # ── ADD TRADE ─────────────────────────────────────────────────
                    if text.startswith("/addtrade"):
                        parts = text.split()
                        if len(parts) >= 5:
                            try:
                                coin = parts[1].upper()
                                direction = parts[2].upper()
                                entry_price = float(parts[3])
                                exit_price = float(parts[4])
                                size = float(parts[5]) if len(parts) >= 6 else 1.0
                                
                                db = get_db()
                                c = db.cursor()
                                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                if direction == "LONG":
                                    pnl = (exit_price - entry_price) * size
                                else:
                                    pnl = (entry_price - exit_price) * size
                                c.execute(
                                    "INSERT INTO trade_journal (chat, coin, direction, entry_price, exit_price, size, pnl, status, opened_at) "
                                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                    (str(chat_id), coin, direction, entry_price, exit_price, size, pnl, 'closed', now)
                                )
                                db.commit()
                                db.close()
                                send(chat_id, f"✅ Trade recorded!\n\n{coin} {direction}\nEntry: {format_price(entry_price)}\nExit: {format_price(exit_price)}\nP&L: {format_price(pnl)}\nSize: {size}")
                            except Exception as _e:
                                send(chat_id, "⚠️ Format: /addtrade BTC LONG 61000 62000 0.5")
                        else:
                            send(chat_id, "⚠️ Format: /addtrade BTC LONG 61000 62000 0.5")
                        continue

                    # ── CLOSE TRADE ──────────────────────────────────────────────
                    if text.startswith("/closetrade"):
                        parts = text.split()
                        if len(parts) >= 2:
                            try:
                                trade_id = int(parts[1])
                                exit_price = float(parts[2]) if len(parts) >= 3 else None
                                result = close_trade(chat_id, trade_id, exit_price)
                                if "error" in result:
                                    send(chat_id, f"⚠️ {result['error']}")
                                else:
                                    send(chat_id, f"✅ Trade closed!\n\nP&L: {format_price(result['pnl'])}\nExit Price: {format_price(result['exit_price'])}")
                            except Exception as _e:
                                send(chat_id, "⚠️ Format: /closetrade TRADE_ID [EXIT_PRICE]")
                        else:
                            send(chat_id, "⚠️ Format: /closetrade TRADE_ID [EXIT_PRICE]")
                        continue

                    # ── WATCHLIST ──────────────────────────────────────────────────
                    if text.startswith("/watchlist") or text.startswith("/wl"):
                        parts = text.split()
                        if len(parts) >= 2:
                            action = parts[1].lower()
                            if action == "add" and len(parts) >= 3:
                                coin = parts[2].upper()
                                try:
                                    db = get_db()
                                    c = db.cursor()
                                    c.execute("INSERT INTO watchlists (chat, coin) VALUES (%s,%s) ON CONFLICT DO NOTHING", (str(chat_id), coin))
                                    db.commit()
                                    db.close()
                                    send(chat_id, f"✅ Added {coin} to watchlist")
                                except Exception as _e:
                                    send(chat_id, "⚠️ Error adding to watchlist.")
                            elif action == "remove" and len(parts) >= 3:
                                coin = parts[2].upper()
                                try:
                                    db = get_db()
                                    c = db.cursor()
                                    c.execute("DELETE FROM watchlists WHERE chat=%s AND coin=%s", (str(chat_id), coin))
                                    db.commit()
                                    db.close()
                                    send(chat_id, f"✅ Removed {coin} from watchlist")
                                except Exception as _e:
                                    send(chat_id, "⚠️ Error removing from watchlist.")
                            elif action == "list":
                                try:
                                    db = get_db()
                                    c = db.cursor()
                                    c.execute("SELECT coin FROM watchlists WHERE chat=%s", (str(chat_id),))
                                    rows = c.fetchall()
                                    db.close()
                                    if not rows:
                                        send(chat_id, "📋 <b>Watchlist</b>\n\nNo coins in watchlist.")
                                    else:
                                        coins = [r[0] for r in rows]
                                        send(chat_id, f"📋 <b>Watchlist</b>\n\n{', '.join(coins)}")
                                except Exception as _e:
                                    send(chat_id, "⚠️ Error loading watchlist.")
                            else:
                                send(chat_id, "⚠️ Usage: /watchlist add|remove|list [COIN]")
                        else:
                            send(chat_id, "⚠️ Usage: /watchlist add|remove|list [COIN]")
                        continue

                    # ── P2P ──────────────────────────────────────────────────────
                    if text.startswith("/p2p"):
                        parts = text.split()
                        if len(parts) >= 2 and parts[1].lower() == "alert":
                            if len(parts) < 4:
                                send(chat_id, "Usage: <code>/p2p alert USDT 1600</code> (or EUR / GBP)")
                                continue
                            ok, msg = set_user_p2p_alert(chat_id, parts[2], parts[3])
                            send(chat_id, ("✅ " if ok else "⚠️ ") + msg)
                            continue
                        if len(parts) >= 2 and parts[1].lower() in ("all", "full"):
                            send(chat_id, format_multi_p2p_intelligence(title="P2P RATES — ALL ASSETS"))
                            continue
                        asset = "USDT"
                        if len(parts) >= 2 and parts[1].upper() in P2P_ASSETS:
                            asset = parts[1].upper()
                        send(
                            chat_id,
                            format_p2p_card(asset, "NGN", include_history=True),
                            [[{"text": "🔄 Refresh", "callback_data": "p2p"},
                              {"text": "USDT", "callback_data": "p2p_usdt"},
                              {"text": "EUR", "callback_data": "p2p_eur"},
                              {"text": "GBP", "callback_data": "p2p_gbp"}],
                             [{"text": "All", "callback_data": "p2p_all"},
                              {"text": "🏠 Main Menu", "callback_data": "main_menu"}]],
                        )
                        continue

                    # ── FEAR & GREED ──────────────────────────────────────────────
                    if text.startswith("/feargreed") or text.startswith("/fg"):
                        fg_data = get_fear_greed()
                        if fg_data:
                            current = fg_data[0]
                            text = (
                                "🧠 <b>Fear & Greed Index</b>\n\n"
                                f"Current: <b>{current['value']}/100</b>\n"
                                f"Status: <b>{current['value_classification']}</b>\n"
                                f"{fg_emoji(current['value'])}\n\n"
                                f"📅 {current['timestamp']}"
                            )
                            if len(fg_data) > 1:
                                week_ago = fg_data[-1]
                                text += f"\n\nWeek ago: {week_ago['value']}/100 ({week_ago['value_classification']})"
                        else:
                            text = "⚠️ Could not fetch Fear & Greed data."
                        send(chat_id, text, [[{"text": "🔄 Refresh", "callback_data": "fear_greed"}, {"text": "🏠 Main Menu", "callback_data": "main_menu"}]])
                        continue

                    # ── NEWS ──────────────────────────────────────────────────────
                    if text.startswith("/news"):
                        news = get_crypto_news()
                        if news:
                            lines = ["📰 <b>Top Crypto News</b>\n"]
                            for i, art in enumerate(news[:5], 1):
                                lines.append(f"{i}. <b>{art.get('title', '')[:80]}</b>")
                                lines.append(f"   {art.get('source', {}).get('title', 'Unknown')}")
                                lines.append("")
                            send(chat_id, "\n".join(lines), [[{"text": "🔄 Refresh", "callback_data": "news"}, {"text": "🏠 Main Menu", "callback_data": "main_menu"}]])
                        else:
                            send(chat_id, "⚠️ No news available.", BACK_MAIN)
                        continue

                    # ── AI ──────────────────────────────────────────────────────
                    if text.startswith("/ai") or text.startswith("/ask"):
                        allowed, used, limit = check_ai_limit(chat_id)
                        if not allowed:
                            send(chat_id, ai_limit_msg(used, limit), UPGRADE_BTN)
                            continue
                        question = text.replace("/ai", "", 1).replace("/ask", "", 1).strip()
                        if not question:
                            set_state(chat_id, "awaiting_ai_question", {})
                            send(chat_id, "🤖 <b>Ask AI</b>\n\nWhat would you like to know?\n\nSend your question below.", [[{"text": "⬅ Cancel", "callback_data": "main_menu"}]])
                            continue
                        track_feature(chat_id, "ai_question")
                        send(chat_id, "🤖 Thinking...")
                        response, provider = ask_ai(question)
                        if response:
                            remaining = (limit - used - 1) if limit else None
                            footer = f"\n\n<i>💬 {remaining} free questions left today.</i>" if remaining is not None and remaining >= 0 else ""
                            send(chat_id, f"🤖 <b>AI ({provider})</b>\n\n{response}{footer}", BACK_MAIN)
                        else:
                            send(chat_id, "⚠️ AI service is currently unavailable. Please try again later.", BACK_MAIN)
                        continue

                    # ── FEEDBACK ──────────────────────────────────────────────────
                    if text.startswith("/feedback") or text.startswith("/fb"):
                        set_state(chat_id, "awaiting_feedback", {})
                        send(chat_id, "💬 <b>Send Feedback</b>\n\nPlease describe your feedback, suggestion, or bug report.", [[{"text": "⬅ Cancel", "callback_data": "main_menu"}]])
                        continue

                    # ── REFERRAL ──────────────────────────────────────────────────
                    if text.startswith("/refer") or text.startswith("/referral"):
                        count = get_pro_referral_count(chat_id)
                        ref_link = f"https://t.me/MarketNgPulseBot?start=ref_PRO_{chat_id}"
                        if is_pro(chat_id):
                            reward, _ = get_pro_referral_reward(chat_id)
                            next_milestone = ""
                            if count < 5:   next_milestone = f"{5-count} more to get 1 month free"
                            elif count < 10: next_milestone = f"{10-count} more to get 3 months free"
                            elif count < 20: next_milestone = f"{20-count} more to get 6 months free"
                            else:            next_milestone = "Maximum tier reached — thank you!"
                            ref_text = (
                                "👥 <b>Your Referral Stats</b>\n\n"
                                f"Total referrals: <b>{count}</b>\n"
                                f"Next milestone: <i>{next_milestone}</i>\n\n"
                                "🎯 <b>Rewards</b>\n"
                                "3 referrals  →  1 month free (paid referrals)\n"
                                "5 referrals  →  1 month free\n"
                                "10 referrals →  3 months free\n"
                                "20 referrals →  6 months free\n\n"
                                "📤 <b>Your link:</b>\n"
                                f"<code>{ref_link}</code>\n\n"
                                "<i>Share this link. Every person who joins through it counts.</i>"
                            )
                        else:
                            ref_text = (
                                "👥 <b>Referral Program</b>\n\n"
                                f"Referrals so far: <b>{count}</b>\n\n"
                                "Refer friends and earn free Pro access:\n\n"
                                "3 referrals  →  1 week Pro free\n"
                                "5 referrals  →  1 month Pro free\n"
                                "10 referrals →  3 months Pro free\n"
                                "20 referrals →  6 months Pro free\n\n"
                                "📤 <b>Your referral link:</b>\n"
                                f"<code>{ref_link}</code>\n\n"
                                "<i>You don't need to be Pro to refer. "
                                "Hit 3 referrals and get your first week on us.</i>"
                            )
                        btns = [[{"text": "💎 Upgrade — ₦5,000/mo", "callback_data": "upgrade"},
                                  {"text": "🏠 Main Menu", "callback_data": "main_menu"}]]
                        send(chat_id, ref_text, btns)
                        continue

                    # ── CANCEL ───────────────────────────────────────────────────
                    if text.startswith("/cancel"):
                        clear_state(chat_id)
                        send(chat_id, "✅ Cancelled.", BACK_MAIN)
                        continue

                    # ── STATE HANDLERS ──────────────────────────────────────────
                    state, state_data = get_state(chat_id)
                    
                    if state == "awaiting_position_calc":
                        handle_position_calc(chat_id, text)
                        continue
                    
                    if state == "awaiting_ai_question":
                        allowed, used, limit = check_ai_limit(chat_id)
                        if not allowed:
                            clear_state(chat_id)
                            send(chat_id, ai_limit_msg(used, limit), UPGRADE_BTN)
                            continue
                        clear_state(chat_id)
                        track_feature(chat_id, "ai_question")
                        send(chat_id, "🤖 Thinking...")
                        # Security: cap user AI input length and strip injection patterns
                        safe_text = text[:500].replace("Ignore previous instructions","").replace("ignore all previous","")
                        response, provider = ask_ai(safe_text)
                        if response:
                            remaining = (limit - used - 1) if limit else None
                            footer = f"\n\n<i>💬 {remaining} free questions left today.</i>" if remaining is not None and remaining >= 0 else ""
                            send(chat_id, f"🤖 <b>AI ({provider})</b>\n\n{response}{footer}", BACK_MAIN)
                        else:
                            send(chat_id, "⚠️ AI service is currently unavailable.", BACK_MAIN)
                        continue
                    
                    if state == "awaiting_feedback":
                        clear_state(chat_id)
                        for admin_id in ADMIN_IDS:
                            send(admin_id, f"💬 <b>User Feedback</b>\n\nUser: <code>{chat_id}</code>\n\n{text}")
                        send(chat_id, "✅ <b>Feedback Sent!</b>\n\nThank you for your feedback.", BACK_MAIN)
                        continue

                    # ── ALERT COIN ────────────────────────────────────────────────
                    if state == "awaiting_alert_coin":
                        coin = text.upper().strip()
                        if coin not in COINS:
                            send(chat_id, f"❌ Unknown coin <b>{coin}</b>. Try BTC, ETH, SOL etc.")
                        else:
                            set_state(chat_id, "awaiting_alert_condition", {"coin": coin})
                            btns = [
                                [{"text": "📈 Price Above", "callback_data": "alert_cond_above"},
                                 {"text": "📉 Price Below", "callback_data": "alert_cond_below"}],
                                [{"text": "❌ Cancel", "callback_data": "menu_alerts"}],
                            ]
                            price, _ = get_best_price(coin)
                            send(chat_id, f"➕ <b>Alert for {coin}</b>\n\nCurrent: <b>{format_price(price)}</b>\n\nAlert when price goes:", btns)
                        continue

                    if state == "awaiting_alert_target":
                        _, sdata = get_state(chat_id)
                        coin = sdata.get("coin", "BTC")
                        cond = sdata.get("condition", "above")
                        try:
                            target = float(text.replace(",", "").replace("$", ""))
                            clear_state(chat_id)
                            db = get_db(); c = db.cursor()
                            c.execute("INSERT INTO alerts (chat, coin, condition, target, active) VALUES (%s,%s,%s,%s,1)",
                                      (str(chat_id), coin, cond, target))
                            db.commit(); db.close()
                            send(chat_id, f"✅ <b>Alert Created!</b>\n\n{coin} will alert you when price goes <b>{cond}</b> <b>{format_price(target)}</b>", BACK_MAIN)
                        except ValueError:
                            send(chat_id, "❌ Invalid price. Send a number like <code>50000</code>")
                        continue

                    # ── WATCHLIST ADD ─────────────────────────────────────────────
                    if state == "awaiting_wl_add":
                        coin = text.upper().strip()
                        if coin not in COINS:
                            send(chat_id, f"❌ Unknown coin <b>{coin}</b>. Try BTC, ETH, SOL etc.")
                        else:
                            try:
                                db = get_db(); c = db.cursor()
                                c.execute("INSERT INTO watchlists (chat, coin) VALUES (%s,%s) ON CONFLICT DO NOTHING", (str(chat_id), coin))
                                db.commit(); db.close()
                                clear_state(chat_id)
                                price, ch = get_best_price(coin)
                                send(chat_id, f"✅ <b>{coin}</b> added to watchlist!\n\nCurrent: <b>{format_price(price)}</b>  {format_change(ch) if ch else ''}",
                                     [[{"text": "⭐ View Watchlist", "callback_data": "watchlist"}]])
                            except Exception as e:
                                send(chat_id, f"⚠️ Could not add {coin}: {e}")
                        continue

                    # ── TRADE SETUP (Pro AI) ──────────────────────────────────────
                    if state == "awaiting_trade_setup_coin":
                        coin = text.upper().strip()
                        if coin not in COINS:
                            clear_state(chat_id)
                            send(chat_id, f"❌ Unknown coin <b>{coin}</b>.")
                            continue
                        pro_user = (get_bot_mode() == "everyone" or is_pro(chat_id))
                        if not pro_user:
                            clear_state(chat_id)
                            send(chat_id, f"Building <b>NORMAL</b> setup for <b>{coin}</b>…")
                            try:
                                msg, trade, idea_id = generate_trade_idea(coin, "momentum")
                                if msg:
                                    send(chat_id, msg[:4000])
                                    send(chat_id, "Free tier = <b>NORMAL</b> only.\n⭐ Pro can also run <b>EDGE</b> and <b>SAFE</b>.\nNFA — DYOR")
                                else:
                                    send(chat_id, f"No valid <b>NORMAL</b> setup for {coin} right now.\nStructure/risk rules did not pass.\n\n⭐ Pro can try EDGE for earlier opportunities.\n<i>NFA — DYOR</i>")
                            except Exception as ex:
                                logger.error("[AI TRADE SETUP] %s", ex)
                                send(chat_id, "Setup engine failed. Try again shortly.")
                            continue
                        set_state(chat_id, "awaiting_trade_setup_tier", {"coin": coin})
                        send(
                            chat_id,
                            f"📊 <b>{coin}</b> — choose setup style:\n\n"
                            "🟡 <b>NORMAL</b> — balanced trend + structure (most common)\n"
                            "🔴 <b>EDGE</b> — earlier opportunity, higher setup risk, smaller size\n"
                            "🟢 <b>SAFE</b> — strict confirmation, fewer trades\n\n"
                            "Tap a button:",
                            [
                                [{"text": "🟡 NORMAL", "callback_data": f"ts_tier_{coin}_momentum"},
                                 {"text": "🔴 EDGE", "callback_data": f"ts_tier_{coin}_edge"}],
                                [{"text": "🟢 SAFE", "callback_data": f"ts_tier_{coin}_steady"}],
                                [{"text": "❌ Cancel", "callback_data": "menu_intelligence"}],
                            ],
                        )
                        continue

                    if state == "awaiting_trade_setup_tier":
                        raw = text.lower().strip()
                        _st, data_st = get_state(chat_id)
                        coin = (data_st or {}).get("coin") if isinstance(data_st, dict) else None
                        clear_state(chat_id)
                        tier_map = {"normal": "momentum", "momentum": "momentum", "edge": "edge", "aggressive": "edge", "safe": "steady", "steady": "steady"}
                        tier = tier_map.get(raw)
                        if not coin or coin not in COINS or not tier:
                            send(chat_id, "Use the buttons, or send: normal / edge / safe")
                            continue
                        send(chat_id, f"Building <b>{tier}</b> setup for <b>{coin}</b>…")
                        try:
                            msg, trade, idea_id = generate_trade_idea(coin, tier)
                            if msg:
                                send(chat_id, msg[:4000])
                            else:
                                send(chat_id, f"No valid setup for {coin} ({tier}) right now.\nRules did not pass.\n<i>NFA — DYOR</i>")
                        except Exception as ex:
                            logger.error("[AI TRADE SETUP] %s", ex)
                            send(chat_id, "Setup engine failed. Try again shortly.")
                        continue

                    if state == "awaiting_grant_pro" and chat_id in ADMIN_IDS:
                        clear_state(chat_id)
                        parts = text.strip().split()
                        try:
                            target_id = int(parts[0])
                            months = int(parts[1]) if len(parts) > 1 else 1
                            ok = grant_pro(target_id, months)
                            if ok:
                                send(chat_id, f"✅ Pro granted to <code>{target_id}</code> for {months} month(s).")
                                try:
                                    send(target_id,
                                        f"🎉 <b>Pro Access Granted!</b>\n\n"
                                        f"You now have Market Pulse Pro for {months} month(s).\n"
                                        f"Enjoy unlimited AI, trade ideas, and full intelligence.")
                                except Exception:
                                    pass
                            else:
                                send(chat_id, "❌ Failed to grant Pro. Check logs.")
                        except (ValueError, IndexError):
                            send(chat_id, "⚠️ Send the user ID: <code>123456789</code> or <code>123456789 3</code> for 3 months")
                        continue

                    if state == "awaiting_broadcast" and chat_id in ADMIN_IDS:
                        clear_state(chat_id)
                        db = get_db(); c = db.cursor()
                        c.execute("SELECT DISTINCT chat FROM users")
                        all_users = c.fetchall(); db.close()
                        sent_count = 0
                        for (uid,) in all_users:
                            try:
                                send(int(uid), f"📣 <b>Announcement</b>\n\n{text}")
                                sent_count += 1
                                time.sleep(0.05)
                            except Exception as _e:
                                logger.debug("[SILENT EXC] %s" % _e)
                        send(chat_id, f"✅ Broadcast sent to <b>{sent_count}</b> users.")
                        continue

                    # ── ADMIN BAN ─────────────────────────────────────────────────
                    if state == "awaiting_ban_id" and chat_id in ADMIN_IDS:
                        clear_state(chat_id)
                        try:
                            target_id = int(text.strip())
                            ban_user(target_id, "Banned by admin")
                            send(chat_id, f"✅ User <code>{target_id}</code> has been banned.")
                        except ValueError:
                            send(chat_id, "❌ Invalid ID. Send a numeric Telegram user ID.")
                        continue

                    # ── PORTFOLIO ADD ─────────────────────────────────────────
                    if state == "awaiting_add_portfolio":
                        parts = text.upper().split()
                        if len(parts) == 3 and parts[0] in COINS:
                            try:
                                coin, amount, buy_price = parts[0], float(parts[1]), float(parts[2])
                                clear_state(chat_id)
                                db = get_db(); c = db.cursor()
                                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                c.execute("INSERT INTO portfolio (chat,coin,amount,buy_price,added_at) VALUES (%s,%s,%s,%s,%s)",
                                          (str(chat_id), coin, amount, buy_price, now_str))
                                db.commit(); db.close()
                                send(chat_id, f"✅ Added <b>{amount} {coin}</b> at <b>${buy_price:,.2f}</b>", BACK_MAIN)
                            except ValueError:
                                send(chat_id, "❌ Invalid format. Use: <code>BTC 0.5 60000</code>")
                        else:
                            send(chat_id, "\u274c Format: <code>COIN AMOUNT BUY_PRICE</code>\nExample: <code>BTC 0.5 60000</code>")
                        continue

                    # ── PORTFOLIO REMOVE ──────────────────────────────────────────
                    if state == "awaiting_remove_portfolio":
                        coin = text.upper().strip()
                        clear_state(chat_id)
                        db = get_db(); c = db.cursor()
                        c.execute("DELETE FROM portfolio WHERE chat=%s AND coin=%s", (str(chat_id), coin))
                        db.commit(); db.close()
                        send(chat_id, f"✅ Removed <b>{coin}</b> from portfolio.", BACK_MAIN)
                        continue

                    # ── TRADE ADD ─────────────────────────────────────────────────
                    if state == "awaiting_add_trade":
                        parts = text.upper().split()
                        if len(parts) >= 3 and parts[0] in COINS and parts[1] in ("LONG","SHORT"):
                            try:
                                coin, direction, entry = parts[0], parts[1].lower(), float(parts[2])
                                size = float(parts[3]) if len(parts) > 3 else 1.0
                                sl = float(parts[4]) if len(parts) > 4 else None
                                tp = float(parts[5]) if len(parts) > 5 else None
                                clear_state(chat_id)
                                db = get_db(); c = db.cursor()
                                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                c.execute("INSERT INTO trade_journal (chat,coin,direction,entry_price,size,stop_loss,take_profit,status,opened_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                          (str(chat_id), coin, direction, entry, size, sl, tp, "open", now_str))
                                db.commit(); db.close()
                                send(chat_id, f"✅ Trade logged\n<b>{direction.upper()} {coin}</b> @ ${entry:,.2f}", BACK_MAIN)
                            except ValueError:
                                send(chat_id, "❌ Invalid format.")
                        else:
                            send(chat_id, "❌ Format: <code>COIN LONG/SHORT ENTRY [SIZE] [SL] [TP]</code>\nExample: <code>BTC LONG 60000 0.1 58000 65000</code>")
                        continue

                    # ── TRADE CLOSE ───────────────────────────────────────────────
                    if state == "awaiting_close_trade":
                        parts = text.split()
                        if len(parts) >= 2:
                            try:
                                trade_id, exit_price = int(parts[0]), float(parts[1])
                                clear_state(chat_id)
                                close_trade(chat_id, trade_id, exit_price)
                                send(chat_id, f"✅ Trade #{trade_id} closed at ${exit_price:,.2f}", BACK_MAIN)
                            except (ValueError, IndexError):
                                send(chat_id, "❌ Format: <code>TRADE_ID EXIT_PRICE</code>\nExample: <code>3 65000</code>")
                        else:
                            send(chat_id, "❌ Format: <code>TRADE_ID EXIT_PRICE</code>")
                        continue

                    # ── COIN SEARCH ───────────────────────────────────────────────
                    if state == "awaiting_coin_search":
                        coin = text.upper().strip()
                        clear_state(chat_id)
                        if coin in COINS:
                            price, change = get_best_price(coin)
                            sd = get_secondary_coin(coin)
                            high = sd.get("usd_24h_high") if sd else None
                            low  = sd.get("usd_24h_low")  if sd else None
                            lines = [
                                f"🔍 <b>{coin} Details</b>\n",
                                f"💰 Price: <b>{format_price(price)}</b>",
                                f"📈 24h Change: <b>{format_change(change)}</b>",
                            ]
                            if high: lines.append(f"⬆️ 24h High: <b>{format_price(high)}</b>")
                            if low:  lines.append(f"⬇️ 24h Low: <b>{format_price(low)}</b>")
                            send(chat_id, "\n".join(lines), BACK_MAIN)
                        else:
                            send(chat_id, f"❌ <b>{coin}</b> not found. Available: {', '.join(list(COINS.keys())[:10])}...", BACK_MAIN)
                        continue

                    # ── CONVERT ───────────────────────────────────────────────────
                    if state == "awaiting_convert":
                        clear_state(chat_id)
                        parts = text.upper().split()
                        try:
                            # Format: 1 BTC NGN  or  100 USD ETH
                            amount_in, from_sym, to_sym = float(parts[0]), parts[1], parts[2]
                            crypto_coins = list(COINS.keys())
                            result = None
                            if from_sym in crypto_coins and to_sym == "NGN":
                                price, _ = get_best_price(from_sym)
                                buy, _, _ = get_p2p_rate("USDT", "NGN")
                                if price and buy:
                                    result = f"{amount_in} {from_sym} ≈ ₦{amount_in * price * buy:,.0f}"
                            elif from_sym in crypto_coins and to_sym in crypto_coins:
                                p1, _ = get_best_price(from_sym)
                                p2, _ = get_best_price(to_sym)
                                if p1 and p2:
                                    result = f"{amount_in} {from_sym} ≈ {amount_in*p1/p2:.6f} {to_sym}"
                            elif from_sym == "NGN" and to_sym in crypto_coins:
                                price, _ = get_best_price(to_sym)
                                _, sell, _ = get_p2p_rate("USDT", "NGN")
                                if price and sell:
                                    result = f"₦{amount_in:,.0f} ≈ {amount_in/sell/price:.8f} {to_sym}"
                            send(chat_id, f"💱 <b>Conversion</b>\n\n{result or 'Could not convert — check symbols.'}", BACK_MAIN)
                        except (ValueError, IndexError):
                            send(chat_id, "❌ Format: <code>AMOUNT FROM TO</code>\nExamples:\n<code>1 BTC NGN</code>\n<code>100 ETH BTC</code>\n<code>50000 NGN BTC</code>", BACK_MAIN)
                        continue

                    # ── PRICE HISTORY ─────────────────────────────────────────────
                    if state == "awaiting_history":
                        coin = text.upper().strip()
                        clear_state(chat_id)
                        if coin not in COINS:
                            send(chat_id, f"❌ Unknown coin {coin}.", BACK_MAIN)
                        else:
                            db = get_db(); c = db.cursor()
                            c.execute("SELECT price, timestamp FROM history WHERE coin=%s ORDER BY id DESC LIMIT 7", (coin,))
                            rows = c.fetchall(); db.close()
                            if rows:
                                lines = [f"📊 <b>{coin} Price History</b>\n"]
                                for price_val, ts in rows:
                                    lines.append(f"• {ts[:16]}  <b>{format_price(price_val)}</b>")
                                send(chat_id, "\n".join(lines), BACK_MAIN)
                            else:
                                send(chat_id, f"No history for {coin} yet.", BACK_MAIN)
                        continue

                    # ── P2P RATE SUBMIT ───────────────────────────────────────────
                    if state == "awaiting_p2p_rate":
                        clear_state(chat_id)
                        try:
                            parts = text.upper().split()
                            if len(parts) == 4:
                                crypto, fiat, buy_r, sell_r = parts[0], parts[1], float(parts[2]), float(parts[3])
                                if buy_r > sell_r and buy_r > 100 and sell_r > 100:
                                    db = get_db(); cur = db.cursor()
                                    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    cur.execute(
                                        "INSERT INTO community_p2p (chat, crypto, fiat, buy_rate, sell_rate, timestamp) VALUES (%s,%s,%s,%s,%s,%s)",
                                        (str(chat_id), crypto, fiat, buy_r, sell_r, now_str)
                                    )
                                    db.commit(); db.close()
                                    send(chat_id,
                                        f"✅ <b>Rate submitted!</b>\n\n"
                                        f"{crypto}/{fiat}   Buy ₦{int(buy_r):,}   Sell ₦{int(sell_r):,}\n\n"
                                        f"<i>Thank you — your submission helps the whole community.</i>",
                                        BACK_MAIN)
                                else:
                                    send(chat_id, "❌ Invalid rates. Buy must be higher than sell and both must be above 100.\nTry: <code>USDT NGN 1620 1590</code>")
                            else:
                                send(chat_id, "❌ Wrong format. Send: <code>USDT NGN 1620 1590</code>")
                        except ValueError:
                            send(chat_id, "❌ Invalid numbers. Try: <code>USDT NGN 1620 1590</code>")
                        continue

                    # ── ALERT CONDITION (intermediate state) ──────────────────────
                    if state == "awaiting_alert_condition":
                        send(chat_id, "Please tap 📈 Price Above or 📉 Price Below on the buttons above.")
                        continue

                    # ── BROADCAST CONFIRM ─────────────────────────────────────────
                    if state == "awaiting_broadcast_confirm" and chat_id in ADMIN_IDS:
                        if text.lower() in ("yes", "confirm", "send"):
                            msg = state_data.get("message", "") if state_data else ""
                            clear_state(chat_id)
                            db = get_db(); c = db.cursor()
                            c.execute("SELECT DISTINCT chat FROM users")
                            all_users = c.fetchall(); db.close()
                            sent_count = 0
                            for (uid,) in all_users:
                                try:
                                    send(int(uid), f"📣 <b>Announcement</b>\n\n{msg}")
                                    sent_count += 1
                                    time.sleep(0.1)  # 10/sec — safe Telegram rate limit
                                except: pass
                            send(chat_id, f"✅ Broadcast sent to {sent_count} users.")
                        else:
                            clear_state(chat_id)
                            send(chat_id, "❌ Broadcast cancelled.")
                        continue

                    # ── TRY AI ON ANY QUESTION ──────────────────────────────────
                    if any(kw in text.lower() for kw in ["what", "how", "why", "when", "where", "is", "are", "can", "will", "tell", "explain"]):
                        send(chat_id, "🤖 Thinking...")
                        response, provider = ask_ai(text)
                        if response:
                            send(chat_id, f"🤖 <b>AI ({provider})</b>\n\n{response}", BACK_MAIN)
                        else:
                            send(chat_id, "⚠️ AI service is currently unavailable.", BACK_MAIN)
                        continue

                # ═══════════════════════════════════════════════════════════════
                # 📊 CALLBACK QUERY HANDLERS
                # ═══════════════════════════════════════════════════════════════
                if "callback_query" in u:
                    q = u["callback_query"]
                    # Always define cb_id — many handlers call answer_cb(cb_id)
                    cb_id = q.get("id")
                    msg_obj = q.get("message") or {}
                    chat_id = (msg_obj.get("chat") or {}).get("id") or q["from"]["id"]
                    message_id = msg_obj.get("message_id")
                    data = q.get("data") or ""
                    username = q["from"].get("username", "")
                    first_name = q["from"].get("first_name", "")
                    try:
                        if cb_id:
                            answer_cb(cb_id)
                    except Exception as _ace:
                        logger.debug("[ANSWER CB] %s", _ace)
                    upsert_user(chat_id, username, first_name)

                    if is_user_banned(chat_id):
                        edit(chat_id, message_id, "🔒 You are banned from using this bot.", BACK_MAIN)
                        continue

                    if not is_user_in_channel(chat_id) and chat_id not in ADMIN_IDS:
                        edit(chat_id, message_id, "🔒 Please join our free channel first:\n\n👉 @marketpulseng\n\nJoin then tap Verify.", [[{"text": "✅ Verified", "callback_data": "verify_join"}]])
                        continue

                    # ── VERIFY JOIN ──────────────────────────────────────────────
                    if data == "verify_join":
                        if is_user_in_channel(chat_id, force=True):
                            edit(chat_id, message_id, "✅ <b>Welcome to Market Pulse!</b>\n\n"
                            "You now have access. Tap the button to get started.",
                            [[{"text": "🚀 Get Started", "callback_data": "main_menu"}]])
                        else:
                            edit(chat_id, message_id, "❌ Still can't find you in the channel.\n\n1. Join @marketpulseng\n2. Come back and tap Try Again.", [[{"text": "✅ Try Again", "callback_data": "verify_join"}]])
                        continue

                    # ── MAIN MENU ────────────────────────────────────────────────
                    if data == "main_menu":
                        clear_state(chat_id)
                        show_main_menu(chat_id, message_id)
                        continue

                    # ── ADMIN MENU NAVIGATION ─────────────────────────────────────
                    if data == "admin_menu" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "👑 <b>Admin Panel</b>\n\nSelect a category:", ADMIN_MENU)
                        continue
                    if data == "adm_analytics" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "📊 <b>Analytics</b>", ADMIN_ANALYTICS_MENU)
                        continue
                    if data == "adm_channel" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "📢 <b>Channel</b>", ADMIN_CHANNEL_MENU)
                        continue
                    if data == "adm_users" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "👥 <b>Users</b>", ADMIN_USERS_MENU)
                        continue
                    if data == "adm_trades" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "⚡ <b>Trades</b>", ADMIN_TRADES_MENU)
                        continue
                    if data == "adm_system" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "🏥 <b>System</b>", ADMIN_SYSTEM_MENU)
                        continue
                    if data == "adm_settings_menu" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "⚙️ <b>Settings</b>", ADMIN_SETTINGS_MENU)
                        continue
                    if data == "adm_toggle_channel" and chat_id in ADMIN_IDS:
                        toggle_channel_enabled()
                        status = "✅ Enabled" if get_channel_enabled() else "⏸ Paused"
                        edit(chat_id, message_id,
                            f"📢 Channel posting is now <b>{status}</b>.",
                            [[{"text": "⬅ Back", "callback_data": "adm_channel"}]])
                        continue

                    if data == "adm_toggle_mirror" and chat_id in ADMIN_IDS:
                        toggle_mirror_mode()
                        status = "🟢 ON" if get_mirror_mode() else "🔴 OFF"
                        edit(chat_id, message_id,
                            f"🪞 <b>Mirror Mode: {status}</b>\n\n"
                            f"{'Pro channel posts are now being mirrored to the free channel.' if get_mirror_mode() else 'Pro channel posts are no longer mirrored to the free channel.'}\n\n"
                            f"⚠️ When ON — free users see all Pro content including trade setups.",
                            [[{"text": "⬅ Back", "callback_data": "adm_channel"}]])
                        continue
                    if data == "adm_grant_pro" and chat_id in ADMIN_IDS:
                        set_state(chat_id, "awaiting_grant_pro")
                        edit(chat_id, message_id,
                            "💎 <b>Grant Pro</b>\n\nSend the user\'s Telegram ID to grant 30 days Pro.\n"
                            "Format: <code>123456789</code> or <code>123456789 3</code> for 3 months.",
                            [[{"text": "❌ Cancel", "callback_data": "adm_users"}]])
                        continue
                    if data == "adm_trade_history" and chat_id in ADMIN_IDS:
                        rows = get_trade_history(limit=10)
                        if not rows:
                            edit(chat_id, message_id, "📋 No trade ideas yet.\n\nGenerate one from ⚡ Trades menu.",
                                [[{"text": "⬅ Back", "callback_data": "adm_trades"}]])
                        else:
                            lines = ["📋 <b>Recent Trade Ideas</b>\n"]
                            for row in rows:
                                tid, coin, tier, direction, tf, entry, t1, conf, status, created = row
                                emoji = {"open":"🟡","closed":"✅"}.get(status,"⚪")
                                lines.append(f"{emoji} <b>#{tid}</b> {coin} {tier.upper()} {direction} | {created[:10]}")
                            edit(chat_id, message_id, "\n".join(lines),
                                [[{"text": "⬅ Back", "callback_data": "adm_trades"}]])
                        continue
                    if data == "adm_gen_trade" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id,
                            "⚡ <b>Generate Trade Idea</b>\n\n"
                            "Use the command format:\n"
                            "<code>/trade BTC momentum</code>\n"
                            "<code>/trade ETH steady</code>\n"
                            "<code>/trade SOL edge</code>\n\n"
                            "Tiers: steady | momentum | edge",
                            [[{"text": "⬅ Back", "callback_data": "adm_trades"}]])
                        continue
                    if data == "adm_performance" and chat_id in ADMIN_IDS:
                        db = None
                        try:
                            db = get_db()
                            c = db.cursor()
                            c.execute("SELECT COUNT(*) FROM trade_ideas")
                            total = c.fetchone()[0]
                            c.execute("SELECT COUNT(*) FROM trade_ideas WHERE status=\'closed\' AND result=\'hit_t1\'")
                            hit_t1 = c.fetchone()[0]
                            c.execute("SELECT COUNT(*) FROM trade_ideas WHERE status=\'closed\' AND result=\'hit_t2\'")
                            hit_t2 = c.fetchone()[0]
                            c.execute("SELECT COUNT(*) FROM trade_ideas WHERE status=\'closed\' AND result=\'stopped\'")
                            stopped = c.fetchone()[0]
                            c.execute("SELECT COUNT(*) FROM trade_ideas WHERE status=\'open\'")
                            open_ideas = c.fetchone()[0]
                            closed = hit_t1 + hit_t2 + stopped
                            win_rate = round((hit_t1 + hit_t2) / closed * 100, 1) if closed > 0 else 0
                            msg = (
                                "📊 <b>Trade Performance</b>\n\n"
                                f"Total ideas: <b>{total}</b>\n"
                                f"Open: <b>{open_ideas}</b>\n"
                                f"Closed: <b>{closed}</b>\n\n"
                                f"✅ Hit T1: <b>{hit_t1}</b>\n"
                                f"🏆 Hit T2: <b>{hit_t2}</b>\n"
                                f"❌ Stopped: <b>{stopped}</b>\n\n"
                                f"Win Rate: <b>{win_rate}%</b>\n"
                                f"<i>Use /closetrade [ID] [result] to record outcomes</i>"
                            )
                        except Exception as e:
                            msg = f"⚠️ Error loading performance: {e}"
                        finally:
                            if db:
                                try: db.close()
                                except Exception: pass
                        edit(chat_id, message_id, msg,
                            [[{"text": "⬅ Back", "callback_data": "adm_trades"}]])
                        continue
                    if data == "adm_mode_menu" and chat_id in ADMIN_IDS:
                        mode = get_bot_mode().upper()
                        edit(chat_id, message_id,
                            f"🤖 <b>Bot Mode</b>\n\nCurrent: <b>{mode}</b>\n\n"
                            "Everyone — all features free\n"
                            "Pro — Free + Pro tiers active",
                            [[{"text": "🌍 Everyone", "callback_data": "adm_mode_everyone"},
                              {"text": "💎 Pro", "callback_data": "adm_mode_pro"}],
                             [{"text": "⬅ Back", "callback_data": "adm_settings_menu"}]])
                        continue
                    if data == "adm_mode_everyone" and chat_id in ADMIN_IDS:
                        set_bot_mode("everyone")
                        cfg = load_admin_config(); cfg["BOT_MODE"] = "everyone"; save_admin_config(cfg)
                        edit(chat_id, message_id, "✅ Mode set to <b>Everyone Free</b>.",
                            [[{"text": "⬅ Back", "callback_data": "adm_settings_menu"}]])
                        continue
                    if data == "adm_mode_pro" and chat_id in ADMIN_IDS:
                        set_bot_mode("pro")
                        cfg = load_admin_config(); cfg["BOT_MODE"] = "pro"; save_admin_config(cfg)
                        edit(chat_id, message_id, "✅ Mode set to <b>Pro</b>.",
                            [[{"text": "⬅ Back", "callback_data": "adm_settings_menu"}]])
                        continue

                    if data == "adm_post_morning" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        try:
                            post_to_channel(build_morning_briefing())
                            post_to_pro_channel(build_morning_briefing_pro())
                            edit(chat_id, message_id, "✅ Morning posts sent.", ADMIN_CHANNEL_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_CHANNEL_MENU)
                        continue

                    if data == "adm_post_midday" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        try:
                            post_to_channel(build_midday_snapshot())
                            post_to_pro_channel(build_midday_snapshot_pro())
                            edit(chat_id, message_id, "✅ Midday posts sent.", ADMIN_CHANNEL_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_CHANNEL_MENU)
                        continue

                    if data == "adm_post_evening" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        try:
                            post_to_channel(build_evening_recap())
                            post_to_pro_channel(build_evening_recap_pro())
                            try:
                                post_to_pro_channel(format_multi_p2p_intelligence(
                                    title="P2P INTELLIGENCE — EVENING READ"))
                            except Exception:
                                pass
                            edit(chat_id, message_id, "✅ Evening posts sent.", ADMIN_CHANNEL_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_CHANNEL_MENU)
                        continue

                    if data == "adm_post_weekly" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        try:
                            post_to_channel(build_weekly_edge())
                            post_to_pro_channel(build_weekly_edge_pro())
                            edit(chat_id, message_id, "✅ Weekly posts sent.", ADMIN_CHANNEL_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_CHANNEL_MENU)
                        continue

                    if data == "adm_morning_pkg" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        edit(chat_id, message_id, "⏳ Running morning Pro package…", ADMIN_CHANNEL_MENU)
                        try:
                            threading.Thread(target=run_morning_pro_package, daemon=True).start()
                            edit(chat_id, message_id, "✅ Morning Pro package started (background).", ADMIN_CHANNEL_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_CHANNEL_MENU)
                        continue

                    if data == "adm_set_channel" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        set_state(chat_id, "awaiting_set_channel", {})
                        edit(chat_id, message_id,
                             "Send Free channel ID (e.g. <code>-100xxxxxxxxxx</code>)\n/cancel to abort.",
                             ADMIN_SETTINGS_MENU)
                        continue

                    if data == "adm_set_pro_channel" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        set_state(chat_id, "awaiting_set_pro_channel", {})
                        edit(chat_id, message_id,
                             "Send Pro channel ID (e.g. <code>-100xxxxxxxxxx</code>)\n/cancel to abort.",
                             ADMIN_SETTINGS_MENU)
                        continue

                    if data == "adm_unban" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        set_state(chat_id, "awaiting_unban_id", {})
                        edit(chat_id, message_id, "Send chat ID to unban:\n/cancel to abort.", ADMIN_USERS_MENU)
                        continue

                    if data == "adm_blacklist" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        try:
                            banned = get_banned_users()
                            if not banned:
                                msg = "Blacklist empty."
                            else:
                                msg = "📋 <b>Blacklist</b>\n\n" + "\n".join(str(x) for x in banned[:50])
                            edit(chat_id, message_id, msg, ADMIN_USERS_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_USERS_MENU)
                        continue

                    if data == "adm_outcome_summary" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        try:
                            edit(chat_id, message_id, outcome_summary(), ADMIN_TRADES_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_TRADES_MENU)
                        continue

                    if data == "adm_run_scanner" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        try:
                            threading.Thread(target=run_trade_scanner, daemon=True).start()
                            edit(chat_id, message_id, "✅ Scanner started in background.", ADMIN_TRADES_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_TRADES_MENU)
                        continue

                    if data == "adm_refresh_prices" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        try:
                            from market_pulse.price_fetchers import get_kraken_batch
                            get_kraken_batch()
                            edit(chat_id, message_id, "✅ Price refresh requested.", ADMIN_SYSTEM_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_SYSTEM_MENU)
                        continue

                    if data == "adm_p2p_snapshot" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        try:
                            record_all_p2p_snapshots()
                            card = format_multi_p2p_intelligence(title="P2P SNAPSHOT (ADMIN)")
                            edit(chat_id, message_id, card, ADMIN_SYSTEM_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_SYSTEM_MENU)
                        continue

                    if data == "adm_clear_state" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        try:
                            clear_state(chat_id)
                            edit(chat_id, message_id, "✅ Your state cleared.", ADMIN_SYSTEM_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_SYSTEM_MENU)
                        continue

                    if data == "adm_test_channels" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        try:
                            ok_f = post_to_channel("🧪 Market Pulse free-channel test.")
                            ok_p = post_to_pro_channel("🧪 Market Pulse Pro-channel test.")
                            edit(chat_id, message_id,
                                 f"Free post: {'OK' if ok_f else 'FAIL'}\nPro post: {'OK' if ok_p else 'FAIL'}",
                                 ADMIN_SYSTEM_MENU)
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ {e}", ADMIN_SYSTEM_MENU)
                        continue

                    if data == "adm_set_watchlist" and chat_id in ADMIN_IDS:
                        answer_cb(cb_id)
                        set_state(chat_id, "awaiting_set_watchlist", {})
                        edit(chat_id, message_id,
                             "Send coins separated by space or comma\n"
                             "Example: <code>BTC ETH SOL BNB XRP</code>\n/cancel to abort.",
                             ADMIN_SETTINGS_MENU)
                        continue


                    # ── MENU NAVIGATION ──────────────────────────────────────────
                    if data == "menu_markets":
                        edit(chat_id, message_id, "📊 <b>Markets</b>\n\nSelect an option:", MARKETS_MENU)
                        continue
                    
                    if data == "menu_intelligence":
                        edit(chat_id, message_id, "🧠 <b>Intelligence</b>\n\nSelect an option:", INTELLIGENCE_MENU)
                        continue
                    
                    if data == "menu_p2p":
                        edit(chat_id, message_id, "🇳🇬 <b>P2P Center</b>\n\nSelect an option:", P2P_MENU)
                        continue

                    if data == "menu_forex":
                        answer_cb(cb_id)
                        edit(chat_id, message_id,
                             "💱 <b>Forex Trade Ideas</b>\n\nSelect a pair:",
                             FOREX_MENU)
                        continue

                    if data == "menu_crypto_idea":
                        answer_cb(cb_id)
                        edit(chat_id, message_id,
                             "⚡ <b>Crypto Trade Idea</b>\n\nSelect a coin:",
                             CRYPTO_IDEA_MENU)
                        continue

                    if data and data.startswith("fx_pair_"):
                        answer_cb(cb_id)
                        pair = data[len("fx_pair_"):]
                        if pair not in FOREX_PAIRS:
                            edit(chat_id, message_id, f"Unknown pair: {pair}", FOREX_MENU)
                            continue
                        edit(chat_id, message_id,
                             f"💱 <b>{pair}</b>\n\nChoose tier:",
                             forex_tier_menu(pair))
                        continue

                    if data and data.startswith("fx_gen_"):
                        answer_cb(cb_id)
                        # fx_gen_USDT_NGN_steady
                        rest = data[len("fx_gen_"):]
                        parts = rest.rsplit("_", 1)
                        if len(parts) != 2:
                            edit(chat_id, message_id, "Invalid forex request.", FOREX_MENU)
                            continue
                        pair_safe, tier = parts
                        pair = pair_safe.replace("_", "/", 1) if pair_safe.count("_") >= 1 else pair_safe
                        # USDT_NGN -> USDT/NGN; EUR_USD -> EUR/USD
                        if "_" in pair_safe:
                            a, b = pair_safe.split("_", 1)
                            pair = f"{a}/{b}"
                        if pair in ("USDT/NGN", "USD/NGN", "BTC/NGN", "EUR/NGN", "GBP/NGN") or str(pair).upper().endswith("/NGN"):
                            edit(chat_id, message_id, "NGN pairs are P2P/rate context only — not trade setups. Use EUR/USD or GBP/USD.", FOREX_MENU)
                            continue
                        if pair not in FOREX_PAIRS or tier not in TRADE_TIERS:
                            edit(chat_id, message_id, "Unknown pair or tier.", FOREX_MENU)
                            continue
                        edit(chat_id, message_id, f"⏳ Generating {tier} idea for {pair}…")
                        try:
                            msg, trade, idea_id = generate_forex_trade_idea(pair, tier)
                            if msg:
                                try:
                                    post_to_pro_channel(msg)
                                except Exception:
                                    pass
                                edit(chat_id, message_id,
                                     f"✅ <b>Forex #{idea_id}</b> — {pair} {tier}\n\n"
                                     f"Posted to Pro channel (if configured).\n\n" + msg[:3500],
                                     FOREX_MENU)
                            else:
                                edit(chat_id, message_id,
                                     f"No quality {tier} setup for {pair} right now.",
                                     FOREX_MENU)
                        except Exception as e:
                            logger.error("[FX BUTTON] %s", e)
                            edit(chat_id, message_id, "⚠️ Forex generation failed.", FOREX_MENU)
                        continue

                    if data and data.startswith("ci_coin_"):
                        answer_cb(cb_id)
                        coin = data[len("ci_coin_"):]
                        edit(chat_id, message_id,
                             f"⚡ <b>{coin}</b>\n\nChoose tier:",
                             crypto_tier_menu(coin))
                        continue

                    if data and data.startswith("ci_gen_"):
                        answer_cb(cb_id)
                        rest = data[len("ci_gen_"):]
                        parts = rest.rsplit("_", 1)
                        if len(parts) != 2:
                            edit(chat_id, message_id, "Invalid request.", CRYPTO_IDEA_MENU)
                            continue
                        coin, tier = parts
                        if tier not in TRADE_TIERS:
                            edit(chat_id, message_id, "Unknown tier.", CRYPTO_IDEA_MENU)
                            continue
                        edit(chat_id, message_id, f"⏳ Generating {tier} idea for {coin}…")
                        try:
                            msg, trade, idea_id = generate_trade_idea(coin, tier)
                            if msg:
                                try:
                                    post_to_pro_channel(msg)
                                except Exception:
                                    pass
                                edit(chat_id, message_id,
                                     f"✅ <b>Idea #{idea_id}</b> — {coin} {tier}\n\n"
                                     f"Posted to Pro channel (if configured).\n\n" + (msg[:3500] if msg else ""),
                                     CRYPTO_IDEA_MENU)
                            else:
                                edit(chat_id, message_id,
                                     f"No quality {tier} setup for {coin} right now.",
                                     CRYPTO_IDEA_MENU)
                        except Exception as e:
                            logger.error("[CI BUTTON] %s", e)
                            edit(chat_id, message_id, "⚠️ Idea generation failed.", CRYPTO_IDEA_MENU)
                        continue

                    if data == "bot_trade_history":
                        answer_cb(cb_id)
                        try:
                            rows = get_trade_history(limit=10)
                            if not rows:
                                edit(chat_id, message_id, "No bot trade ideas yet.", TRADES_MENU)
                            else:
                                lines = ["📋 <b>Recent Bot Trade Ideas</b>\n"]
                                for r in rows:
                                    # row shape depends on get_trade_history
                                    if isinstance(r, dict):
                                        lines.append(
                                            f"#{r.get('id')} {r.get('coin')} {r.get('tier')} "
                                            f"{r.get('direction')} — {r.get('status')}"
                                        )
                                    else:
                                        lines.append(str(r)[:120])
                                edit(chat_id, message_id, "\n".join(lines), TRADES_MENU)
                        except Exception as e:
                            logger.error("[BOT HISTORY] %s", e)
                            edit(chat_id, message_id, "Could not load history.", TRADES_MENU)
                        continue

                    if data == "close_bot_idea":
                        answer_cb(cb_id)
                        set_state(chat_id, "awaiting_close_idea_id", {})
                        edit(chat_id, message_id,
                             "🔒 Send the idea ID to close (e.g. <code>42</code>).\n"
                             "Or /cancel to abort.",
                             TRADES_MENU)
                        continue

                    if data == "p2p_set_alert":
                        answer_cb(cb_id)
                        edit(chat_id, message_id,
                             "🔔 <b>Set P2P Alert</b>\n\nChoose asset:",
                             P2P_ALERT_ASSET_MENU)
                        continue

                    if data and data.startswith("p2p_alert_asset_"):
                        answer_cb(cb_id)
                        asset = data[len("p2p_alert_asset_"):]
                        set_state(chat_id, "awaiting_p2p_alert_target", {"asset": asset})
                        edit(chat_id, message_id,
                             f"Send target <b>buy</b> rate in ₦ for {asset}/NGN\n"
                             f"Example: <code>1600</code>\n/cancel to abort.",
                             P2P_MENU)
                        continue

                    
                    if data == "menu_alerts":
                        if get_bot_mode() == "everyone" or is_pro(chat_id):
                            edit(chat_id, message_id, "🔔 <b>Alerts</b>\n\nSelect an option:", ALERTS_MENU_PRO)
                        else:
                            edit(chat_id, message_id, "🔔 <b>Alerts</b>\n\nSelect an option:", ALERTS_MENU_FREE)
                        continue
                    
                    if data == "menu_portfolio":
                        edit(chat_id, message_id, "💼 <b>Portfolio</b>\n\nSelect an option:", PORTFOLIO_MENU)
                        continue
                    
                    if data == "menu_trades":
                        edit(chat_id, message_id, "📈 <b>Trade Journal</b>\n\nSelect an option:", TRADES_MENU)
                        continue
                    
                    if data == "menu_tools":
                        edit(chat_id, message_id, "🛠 <b>Tools</b>\n\nSelect an option:", TOOLS_MENU)
                        continue
                    
                    if data == "menu_account":
                        if get_bot_mode() == "everyone" or is_pro(chat_id):
                            edit(chat_id, message_id, "👤 <b>My Account</b>\n\nSelect an option:", ACCOUNT_MENU_PRO)
                        else:
                            edit(chat_id, message_id, "👤 <b>My Account</b>\n\nSelect an option:", ACCOUNT_MENU_FREE)
                        continue
                    
                    if data == "help":
                        show_help(chat_id, message_id)
                        continue

                    # ── FEATURES ──────────────────────────────────────────────────
                    if data == "market":
                        show_market(chat_id, message_id)
                        continue
                    
                    if data == "portfolio":
                        show_portfolio(chat_id, message_id)
                        continue
                    
                    if data == "trade_journal":
                        show_trade_journal(chat_id, message_id)
                        continue
                    
                    if data == "settings":
                        show_settings(chat_id, message_id)
                        continue
                    
                    if data == "position_calculator":
                        show_position_calculator(chat_id, message_id)
                        continue
                    
                    if data == "upgrade":
                        show_upgrade(chat_id, message_id)
                        continue
                    
                    if data == "p2p":
                        buy, sell, source = get_p2p_rate("USDT", "NGN")
                        if buy and sell:
                            text = (
                                "💱 <b>USDT/NGN P2P Rates</b>\n\n"
                                f"Buy: <b>₦{int(buy):,}</b>\n"
                                f"Sell: <b>₦{int(sell):,}</b>\n"
                                f"Spread: <b>₦{int(buy - sell):,}</b>\n\n"
                                f"Source: <i>{source}</i>"
                            )
                        else:
                            text = "⚠️ Could not fetch P2P rates."
                        edit(chat_id, message_id, text, [[{"text": "🔄 Refresh", "callback_data": "p2p"}, {"text": "⬅ Back", "callback_data": "menu_p2p"}]])
                        continue
                    
                    if data == "fear_greed":
                        fg_data = get_fear_greed()
                        if fg_data:
                            current = fg_data[0]
                            text = (
                                "🧠 <b>Fear & Greed Index</b>\n\n"
                                f"Current: <b>{current['value']}/100</b>\n"
                                f"Status: <b>{current['value_classification']}</b>\n"
                                f"{fg_emoji(current['value'])}\n\n"
                                f"📅 {current['timestamp']}"
                            )
                            if len(fg_data) > 1:
                                week_ago = fg_data[-1]
                                text += f"\n\nWeek ago: {week_ago['value']}/100 ({week_ago['value_classification']})"
                        else:
                            text = "⚠️ Could not fetch Fear & Greed data."
                        edit(chat_id, message_id, text, [[{"text": "🔄 Refresh", "callback_data": "fear_greed"}, {"text": "⬅ Back", "callback_data": "menu_intelligence"}]])
                        continue
                    
                    if data == "news":
                        news = get_crypto_news()
                        if news:
                            lines = ["📰 <b>Top Crypto News</b>\n"]
                            for i, art in enumerate(news[:5], 1):
                                lines.append(f"{i}. <b>{art.get('title', '')[:80]}</b>")
                                lines.append(f"   {art.get('source', {}).get('title', 'Unknown')}")
                                lines.append("")
                            edit(chat_id, message_id, "\n".join(lines), [[{"text": "🔄 Refresh", "callback_data": "news"}, {"text": "⬅ Back", "callback_data": "menu_intelligence"}]])
                        else:
                            edit(chat_id, message_id, "⚠️ No news available.", [[{"text": "⬅ Back", "callback_data": "menu_intelligence"}]])
                        continue

                    # ── SETTINGS CALLBACKS ──────────────────────────────────────
                    if data == "settings_language":
                        buttons = [
                            [{"text": "🇬🇧 English", "callback_data": "lang_en"}],
                            [{"text": "🇳🇬 Hausa", "callback_data": "lang_ha"}],
                            [{"text": "🇳🇬 Yoruba", "callback_data": "lang_yo"}],
                            [{"text": "🇳🇬 Igbo", "callback_data": "lang_ig"}],
                            [{"text": "⬅ Back", "callback_data": "settings"}]
                        ]
                        edit(chat_id, message_id, "🌐 <b>Select Language</b>", buttons)
                        continue
                    
                    if data.startswith("lang_"):
                        lang = data.split("_")[1]
                        try:
                            db = get_db()
                            c = db.cursor()
                            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            c.execute(
                                "INSERT INTO user_preferences (chat, language, notifications, theme, updated_at) "
                                "VALUES (%s,%s,1,'dark',%s) "
                                "ON CONFLICT(chat) DO UPDATE SET language=excluded.language, updated_at=excluded.updated_at",
                                (str(chat_id), lang, now)
                            )
                            db.commit()
                            db.close()
                            send(chat_id, f"✅ Language set to {lang.upper()}")
                            show_settings(chat_id, message_id)
                        except Exception as _e:
                            send(chat_id, "⚠️ Error saving settings.")
                        continue
                    
                    if data == "settings_notifications":
                        try:
                            db = get_db()
                            c = db.cursor()
                            c.execute("SELECT notifications FROM user_preferences WHERE chat=%s", (str(chat_id),))
                            row = c.fetchone()
                            current = row[0] if row else 1
                            new_val = 0 if current else 1
                            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            c.execute(
                                "INSERT INTO user_preferences (chat, language, notifications, theme, updated_at) "
                                "VALUES (%s,'en',%s,'dark',%s) "
                                "ON CONFLICT(chat) DO UPDATE SET notifications=excluded.notifications, updated_at=excluded.updated_at",
                                (str(chat_id), new_val, now)
                            )
                            db.commit()
                            db.close()
                            send(chat_id, f"✅ Notifications {'On' if new_val else 'Off'}")
                            show_settings(chat_id, message_id)
                        except Exception as _e:
                            send(chat_id, "⚠️ Error saving settings.")
                        continue
                    
                    if data == "settings_theme":
                        try:
                            db = get_db()
                            c = db.cursor()
                            c.execute("SELECT theme FROM user_preferences WHERE chat=%s", (str(chat_id),))
                            row = c.fetchone()
                            current = row[0] if row else "dark"
                            new_val = "light" if current == "dark" else "dark"
                            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            c.execute(
                                "INSERT INTO user_preferences (chat, language, notifications, theme, updated_at) "
                                "VALUES (%s,'en',1,%s,%s) "
                                "ON CONFLICT(chat) DO UPDATE SET theme=excluded.theme, updated_at=excluded.updated_at",
                                (str(chat_id), new_val, now)
                            )
                            db.commit()
                            db.close()
                            send(chat_id, f"✅ Theme set to {new_val.title()}")
                            show_settings(chat_id, message_id)
                        except Exception as _e:
                            send(chat_id, "⚠️ Error saving settings.")
                        continue

                    # ── PORTFOLIO ACTIONS ──────────────────────────────────────
                    if data == "add_portfolio":
                        set_state(chat_id, "awaiting_add_portfolio", {})
                        edit(chat_id, message_id, "➕ <b>Add Portfolio Position</b>\n\nSend in this format:\n<code>BTC 0.5 61000</code>", [[{"text": "⬅ Cancel", "callback_data": "menu_portfolio"}]])
                        continue
                    
                    if data == "remove_portfolio":
                        set_state(chat_id, "awaiting_remove_portfolio", {})
                        edit(chat_id, message_id, "🗑️ <b>Remove Portfolio Position</b>\n\nSend the coin name: <code>BTC</code>", [[{"text": "⬅ Cancel", "callback_data": "menu_portfolio"}]])
                        continue
                    
                    if data == "pnl_summary":
                        portfolio_data = get_portfolio_value(chat_id)
                        if portfolio_data and portfolio_data["positions"]:
                            text = (
                                "📊 <b>P&L Summary</b>\n\n"
                                f"💰 Total Invested: <b>${portfolio_data['total_invested']:.2f}</b>\n"
                                f"📈 Current Value: <b>${portfolio_data['total_current']:.2f}</b>\n"
                                f"📊 Total P&L: <b>{'+' if portfolio_data['total_pnl'] > 0 else ''}{portfolio_data['total_pnl']:.2f}</b>\n"
                                f"📈 P&L %: <b>{'+' if portfolio_data['total_pnl_pct'] > 0 else ''}{portfolio_data['total_pnl_pct']:.1f}%</b>\n\n"
                                f"📊 Positions: <b>{len(portfolio_data['positions'])}</b>"
                            )
                        else:
                            text = "📊 <b>P&L Summary</b>\n\nNo positions yet."
                        edit(chat_id, message_id, text, [[{"text": "⬅ Back", "callback_data": "menu_portfolio"}]])
                        continue

                    # ── TRADE ACTIONS ────────────────────────────────────────────
                    if data == "add_trade":
                        set_state(chat_id, "awaiting_add_trade", {})
                        edit(chat_id, message_id, "➕ <b>Add Trade</b>\n\nFormat: <code>BTC LONG 61000 62000 0.5</code>", [[{"text": "⬅ Cancel", "callback_data": "menu_trades"}]])
                        continue
                    
                    if data == "close_trade":
                        set_state(chat_id, "awaiting_close_trade", {})
                        edit(chat_id, message_id, "🔒 <b>Close Trade</b>\n\nSend: <code>TRADE_ID EXIT_PRICE</code>\n\nOr just <code>TRADE_ID</code> for current price", [[{"text": "⬅ Cancel", "callback_data": "menu_trades"}]])
                        continue
                    
                    if data == "win_rate":
                        try:
                            db = get_db()
                            c = db.cursor()
                            c.execute("SELECT pnl FROM trade_journal WHERE chat=%s AND status='closed'", (str(chat_id),))
                            rows = c.fetchall()
                            db.close()
                            if rows:
                                total_pnl = sum(r[0] for r in rows if r[0])
                                wins = sum(1 for r in rows if r[0] and r[0] > 0)
                                total = len(rows)
                                win_rate = (wins / total) * 100 if total > 0 else 0
                                avg_pnl = total_pnl / total if total > 0 else 0
                                text = (
                                    "📊 <b>Win Rate Analysis</b>\n\n"
                                    f"Total Trades: <b>{total}</b>\n"
                                    f"Wins: <b>{wins}</b>\n"
                                    f"Losses: <b>{total - wins}</b>\n"
                                    f"Win Rate: <b>{win_rate:.1f}%</b>\n"
                                    f"Total P&L: <b>{'+' if total_pnl > 0 else ''}{total_pnl:.2f}</b>\n"
                                    f"Average P&L: <b>{'+' if avg_pnl > 0 else ''}{avg_pnl:.2f}</b>"
                                )
                            else:
                                text = "📊 <b>Win Rate Analysis</b>\n\nNo closed trades yet."
                        except Exception as _e:
                            text = "⚠️ Error loading trade stats."
                        edit(chat_id, message_id, text, [[{"text": "⬅ Back", "callback_data": "menu_trades"}]])
                        continue

                    # ── ASK AI ────────────────────────────────────────────────────
                    if data == "ask_ai":
                        allowed, used, limit = check_ai_limit(chat_id)
                        if not allowed:
                            edit(chat_id, message_id, ai_limit_msg(used, limit), UPGRADE_BTN)
                            continue
                        remaining = (limit - used) if limit else None
                        hint = f"\n\n<i>💬 {remaining} free questions remaining today.</i>" if remaining is not None else ""
                        set_state(chat_id, "awaiting_ai_question", {})
                        edit(chat_id, message_id, f"🤖 <b>Ask AI</b>\n\nWhat would you like to know?{hint}", [[{"text": "⬅ Cancel", "callback_data": "menu_intelligence"}]])
                        continue

                    # ── P2P ACTIONS ──────────────────────────────────────────────
                    if data == "submit_rate":
                        set_state(chat_id, "awaiting_p2p_rate", {})
                        edit(chat_id, message_id, "📤 <b>Submit P2P Rate</b>\n\nFormat: <code>USDT NGN 1530 1520</code>", [[{"text": "⬅ Cancel", "callback_data": "menu_p2p"}]])
                        continue
                    
                    if data == "p2p_alerts":
                        edit(chat_id, message_id, "🔔 <b>P2P Alerts</b>\n\nFeature coming soon!", [[{"text": "⬅ Back", "callback_data": "menu_p2p"}]])
                        continue
                    
                    if data == "arbitrage":
                        opportunities = scan_arbitrage()
                        if opportunities:
                            lines = ["🔄 <b>Arbitrage Opportunities</b>\n"]
                            for opp in opportunities[:5]:
                                lines.append(f"<b>{opp['coin']}</b>")
                                lines.append(f"  Buy: {opp['buy_from']} @ {format_price(opp['buy_price'])}")
                                lines.append(f"  Sell: {opp['sell_to']} @ {format_price(opp['sell_price'])}")
                                lines.append(f"  Gap: <b>{opp['gap_pct']:.2f}%</b>")
                                lines.append("")
                        else:
                            lines = ["🔄 <b>Arbitrage Scanner</b>\n\nNo opportunities found at the moment.\n\n<small>Check back later!</small>"]
                        edit(chat_id, message_id, "\n".join(lines), [[{"text": "🔄 Refresh", "callback_data": "arbitrage"}, {"text": "⬅ Back", "callback_data": "menu_p2p"}]])
                        continue

                    # ── TOOLS ────────────────────────────────────────────────────
                    if data == "coin_search":
                        set_state(chat_id, "awaiting_coin_search", {})
                        edit(chat_id, message_id, "🔍 <b>Search Coin</b>\n\nSend the coin name: <code>BTC</code>", [[{"text": "⬅ Cancel", "callback_data": "menu_tools"}]])
                        continue
                    
                    if data == "convert":
                        set_state(chat_id, "awaiting_convert", {})
                        edit(chat_id, message_id, "🔄 <b>Convert Crypto</b>\n\nFormat: <code>BTC 1.5 USD</code>", [[{"text": "⬅ Cancel", "callback_data": "menu_tools"}]])
                        continue
                    
                    if data == "history":
                        set_state(chat_id, "awaiting_history", {})
                        edit(chat_id, message_id, "📜 <b>Price History</b>\n\nFormat: <code>BTC 1D</code>\n\nTimeframes: 1H, 6H, 1D, 3D, 1W, 1M, 3M, 1Y", [[{"text": "⬅ Cancel", "callback_data": "menu_tools"}]])
                        continue
                    
                    if data == "status":
                        text = (
                            "⚙️ <b>Bot Status</b>\n\n"
                            f"📅 Version: v16\n"
                            f"🤖 Mode: {get_bot_mode().upper()}\n"
                            f"📊 Channel: {'✅ Online' if get_channel_enabled() else '⏸️ Paused'}\n"
                            f"👤 Your Status: {get_user_badge(chat_id)}\n\n"
                            f"🟢 All systems operational."
                        )
                        edit(chat_id, message_id, text, [[{"text": "🔄 Refresh", "callback_data": "status"}, {"text": "⬅ Back", "callback_data": "menu_tools"}]])
                        continue

                    # ── ACCOUNT ──────────────────────────────────────────────────
                    if data == "profile":
                        db = get_db()
                        c = db.cursor()
                        c.execute("SELECT first_name, username, first_seen, last_seen FROM users WHERE chat=%s", (str(chat_id),))
                        row = c.fetchone()
                        db.close()
                        if row:
                            name, username, first_seen, last_seen = row
                            text = (
                                "👤 <b>My Profile</b>\n\n"
                                f"Name: <b>{name or 'N/A'}</b>\n"
                                f"Username: <b>@{username or 'N/A'}</b>\n"
                                f"Status: <b>{get_user_badge(chat_id)}</b>\n"
                                f"Joined: <b>{first_seen}</b>\n"
                                f"Last Active: <b>{last_seen}</b>"
                            )
                        else:
                            text = "👤 <b>My Profile</b>\n\nNo profile data available."
                        edit(chat_id, message_id, text, [[{"text": "⬅ Back", "callback_data": "menu_account"}]])
                        continue
                    
                    if data == "pro_status":
                        if is_pro(chat_id):
                            expiry = get_pro_expiry(chat_id)
                            days = get_pro_days_left(chat_id)
                            refs = get_pro_referral_count(chat_id)
                            text = (
                                "⭐ <b>Pro Status</b>\n\n"
                                f"Status: <b>✅ Active</b>\n"
                                f"Expires: <b>{expiry}</b>\n"
                                f"Days Left: <b>{days}</b>\n"
                                f"Referrals: <b>{refs}</b>\n\n"
                                "🎁 Refer 5+ people for FREE months!"
                            )
                        else:
                            text = (
                                "⭐ <b>Pro Status</b>\n\n"
                                "Status: <b>❌ Not Active</b>\n\n"
                                "💎 Upgrade to Pro:\n"
                                "✅ Unlimited AI\n"
                                "✅ 20 alerts\n"
                                "✅ Trade Journal\n"
                                "✅ Position Calculator\n"
                                "✅ Pro Referrals\n\n"
                                "WhatsApp +2347045850590"
                            )
                        edit(chat_id, message_id, text, [[{"text": "💎 Upgrade", "callback_data": "upgrade"}, {"text": "⬅ Back", "callback_data": "menu_account"}]])
                        continue
                    
                    if data == "my_usage":
                        try:
                            db = get_db()
                            c = db.cursor()
                            c.execute("SELECT COUNT(*) FROM feature_usage WHERE chat=%s", (str(chat_id),))
                            total_usage = c.fetchone()[0]
                            c.execute("SELECT COUNT(*) FROM alerts WHERE chat=%s AND active=1", (str(chat_id),))
                            total_alerts = c.fetchone()[0]
                            c.execute("SELECT COUNT(*) FROM portfolio WHERE chat=%s", (str(chat_id),))
                            total_positions = c.fetchone()[0]
                            c.execute("SELECT COUNT(*) FROM trade_journal WHERE chat=%s", (str(chat_id),))
                            total_trades = c.fetchone()[0]
                            db.close()
                            text = (
                                "📊 <b>My Usage</b>\n\n"
                                f"📈 Total Interactions: <b>{total_usage}</b>\n"
                                f"🔔 Active Alerts: <b>{total_alerts}</b>\n"
                                f"💼 Portfolio Items: <b>{total_positions}</b>\n"
                                f"📈 Trades Logged: <b>{total_trades}</b>"
                            )
                        except Exception as _e:
                            text = "📊 <b>My Usage</b>\n\nCould not load usage data."
                        edit(chat_id, message_id, text, [[{"text": "⬅ Back", "callback_data": "menu_account"}]])
                        continue
                    
                    if data == "referral":
                        if is_pro(chat_id):
                            count = get_pro_referral_count(chat_id)
                            reward, _ = get_pro_referral_reward(chat_id)
                            text = (
                                "👥 <b>Pro Referral Program</b>\n\n"
                                f"📊 Referrals: <b>{count}</b>\n"
                                f"🎁 Next reward: <b>{reward or 'None yet'}</b>\n\n"
                                "🎯 Milestones:\n"
                                "5 referrals → 1 month FREE\n"
                                "10 referrals → 3 months FREE\n"
                                "20 referrals → 6 months FREE\n\n"
                                "📤 Share your referral link:\n"
                                f"<code>https://t.me/MarketNgPulseBot?start=ref_PRO_{chat_id}</code>"
                            )
                        else:
                            text = "👥 <b>Referral Program</b>\n\nUpgrade to Pro to earn FREE months!"
                        edit(chat_id, message_id, text, [[{"text": "💎 Upgrade", "callback_data": "upgrade"}, {"text": "⬅ Back", "callback_data": "menu_account"}]])
                        continue

                    # ── HELP SUB-MENUS ──────────────────────────────────────────
                    if data == "help_commands":
                        show_help(chat_id, message_id)
                        continue
                    
                    if data == "help_howto":
                        text = (
                            "📖 <b>How To Use Market Pulse</b>\n\n"
                            "1. <b>Start</b> — Type /start or /menu\n"
                            "2. <b>Prices</b> — Tap Markets or type /market\n"
                            "3. <b>AI Analysis</b> — Tap Intelligence or type /ai\n"
                            "4. <b>P2P Rates</b> — Tap P2P Center or type /p2p\n"
                            "5. <b>Portfolio</b> — Tap Portfolio or type /portfolio\n"
                            "6. <b>Trades</b> — Tap Trade Journal or type /trades\n"
                            "7. <b>Alerts</b> — Tap Alerts or type /alerts\n\n"
                            "💡 Pro tip: Use /help to see all commands!"
                        )
                        edit(chat_id, message_id, text, [[{"text": "⬅ Back", "callback_data": "help"}]])
                        continue
                    
                    if data == "help_faq":
                        text = (
                            "❓ <b>FAQ</b>\n\n"
                            "❔ <b>Is this free?</b>\n"
                            "Yes! Core features are free. Pro users get more.\n\n"
                            "❔ <b>Where do prices come from?</b>\n"
                            "Kraken → OKX → Bybit → CoinGecko\n\n"
                            "❔ <b>Are P2P rates real?</b>\n"
                            "Yes! From Binance P2P and Bybit P2P.\n\n"
                            "❔ <b>How do I upgrade to Pro?</b>\n"
                            "WhatsApp +2347045850590\n\n"
                            "❔ <b>NFA - DYOR?</b>\n"
                            "Not Financial Advice - Do Your Own Research."
                        )
                        edit(chat_id, message_id, text, [[{"text": "⬅ Back", "callback_data": "help"}]])
                        continue
                    
                    if data == "support":
                        text = (
                            "💬 <b>Support</b>\n\n"
                            "Need help? Contact us:\n\n"
                            "📩 DM: WhatsApp +2347045850590\n"
                            "📢 Channel: @MarketNgPulseBot\n\n"
                            "Or use /feedback to send a message."
                        )
                        edit(chat_id, message_id, text, [[{"text": "⬅ Back", "callback_data": "help"}]])

                    # ── ADMIN CALLBACKS ──────────────────────────────────────────
                    if chat_id in ADMIN_IDS:
                        if data == "admin_stats":
                            db = get_db()
                            c = db.cursor()
                            c.execute("SELECT COUNT(*) FROM users")
                            users = c.fetchone()[0]
                            c.execute("SELECT COUNT(*) FROM pro_subscriptions")
                            pro = c.fetchone()[0]
                            c.execute("SELECT COUNT(*) FROM alerts WHERE active=1")
                            alerts = c.fetchone()[0]
                            db.close()
                            text = (
                                "📊 <b>Admin Stats</b>\n\n"
                                f"👤 Users: <b>{users:,}</b>\n"
                                f"⭐ Pro: <b>{pro:,}</b>\n"
                                f"🔔 Alerts: <b>{alerts:,}</b>\n"
                                f"⚡ Mode: <b>{get_bot_mode().upper()}</b>"
                            )
                            edit(chat_id, message_id, text, [[{"text": "🔄 Refresh", "callback_data": "admin_stats"}, {"text": "⬅ Back", "callback_data": "main_menu"}]])
                            continue
                        
                        if data == "admin_users":
                            db = get_db()
                            c = db.cursor()
                            c.execute("SELECT chat, username, first_name FROM users ORDER BY id DESC LIMIT 10")
                            rows = c.fetchall()
                            db.close()
                            lines = ["👤 <b>Recent Users</b>\n"]
                            for chat, username, first_name in rows:
                                name = first_name or username or str(chat)
                                lines.append(f"• {name[:25]} (<code>{chat}</code>)")
                            edit(chat_id, message_id, "\n".join(lines), [[{"text": "⬅ Back", "callback_data": "adm_analytics"}]])
                            continue
                        
                        if data == "admin_health":
                            ws_status = ws_engine_status()
                            btc_p, _ = _ws_get_cached("BTC")
                            eth_p, _ = _ws_get_cached("ETH")
                            with _ws_lock:
                                cached_count = len(_ws_price_cache)
                            health_msg = (
                                "🏥 <b>System Health</b>\n\n"
                                "⚡ <b>WebSocket Engine</b>\n"
                                f"  Binance: {'🟢 streaming' if btc_p else '🔴 stale/down'}\n"
                                f"  Kraken:  {'🟢 streaming' if eth_p else '🟡 REST fallback'}\n"
                                f"  Prices cached: {cached_count}/{len(COINS)} coins\n\n"
                                "🌐 <b>REST Fallbacks</b>\n"
                                "  OKX / Bybit / CoinGecko: standby\n\n"
                                "🤖 <b>Bot</b>\n"
                                f"  Mode: {get_bot_mode().upper()}\n"
                                f"  Channel: {'✅ ON' if get_channel_enabled() else '⏸ OFF'}\n"
                                "  Poll loop: 🟢 running"
                            )
                            edit(chat_id, message_id, health_msg,
                                [[{"text": "🔄 Refresh", "callback_data": "admin_health"},
                                  {"text": "⬅ Back", "callback_data": "adm_system"}]])
                            continue

                    # ── PRO MENU ──────────────────────────────────────────────────
                    if data == "menu_pro":
                        text = (
                            "⭐ <b>Pro Features</b>\n\n"
                            "✅ Unlimited AI\n"
                            "✅ 20 alerts\n"
                            "✅ 30 watchlist items\n"
                            "✅ 30 portfolio items\n"
                            "✅ Trade Journal\n"
                            "✅ Position Calculator\n"
                            "✅ AI Trade Setups\n"
                            "✅ Pro Channel\n"
                            "✅ Pro Referrals\n\n"
                            f"📅 Expires: <b>{get_pro_expiry(chat_id) or 'N/A'}</b>"
                        )
                        edit(chat_id, message_id, text, [[{"text": "⬅ Back", "callback_data": "main_menu"}]])
                        continue
                    
                    if data == "menu_pro_tools":
                        text = (
                            "📈 <b>Pro Tools</b>\n\n"
                            "✅ Position Calculator\n"
                            "✅ Trade Journal\n"
                            "✅ AI Trade Setups\n"
                            "✅ Smart Alerts\n"
                            "✅ Advanced Analytics\n"
                            "✅ Pro Referrals"
                        )
                        edit(chat_id, message_id, text, [[{"text": "⬅ Back", "callback_data": "main_menu"}]])

                    # ── ALERTS — Create Alert ─────────────────────────────────────
                    if data == "alerts":
                        set_state(chat_id, "awaiting_alert_coin")
                        pro_limit = 20 if (get_bot_mode() == "everyone" or is_pro(chat_id)) else 3
                        db = get_db(); c = db.cursor()
                        c.execute("SELECT COUNT(*) FROM alerts WHERE chat=%s AND active=1", (str(chat_id),))
                        count = c.fetchone()[0]; db.close()
                        if count >= pro_limit:
                            edit(chat_id, message_id,
                                f"⚠️ You have reached your alert limit ({pro_limit}).\n\nDelete an alert first.",
                                [[{"text": "📋 My Alerts", "callback_data": "my_alerts"}, {"text": "⬅ Back", "callback_data": "menu_alerts"}]])
                        else:
                            coins_list = ", ".join(list(COINS.keys())[:15]) + "..."
                            edit(chat_id, message_id,
                                f"➕ <b>Create Price Alert</b>\n\n"
                                f"Send the coin symbol you want to track.\n"
                                f"Example: <code>BTC</code>\n\n"
                                f"Available: {coins_list}",
                                [[{"text": "❌ Cancel", "callback_data": "menu_alerts"}]])

                    # ── ALERTS — My Alerts ────────────────────────────────────────
                    if data == "my_alerts":
                        db = get_db(); c = db.cursor()
                        c.execute("SELECT id, coin, condition, target, label FROM alerts WHERE chat=%s AND active=1", (str(chat_id),))
                        rows = c.fetchall(); db.close()
                        if not rows:
                            edit(chat_id, message_id, "📋 <b>My Alerts</b>\n\nYou have no active alerts.\n\nTap ➕ Create Alert to add one.",
                                [[{"text": "➕ Create Alert", "callback_data": "alerts"}, {"text": "⬅ Back", "callback_data": "menu_alerts"}]])
                        else:
                            lines = ["📋 <b>My Active Alerts</b>\n"]
                            btns = []
                            for row in rows:
                                aid, coin, cond, target, label = row
                                lbl = f" ({label})" if label else ""
                                lines.append(f"• <b>{coin}</b> {cond} <b>{format_price(target)}</b>{lbl}")
                                btns.append([{"text": f"🗑 Delete {coin} {cond} {format_price(target)}", "callback_data": f"del_alert_{aid}"}])
                            btns.append([{"text": "⬅ Back", "callback_data": "menu_alerts"}])
                            edit(chat_id, message_id, "\n".join(lines), btns)

                    # ── ALERTS — Delete single alert ─────────────────────────────
                    if data.startswith("del_alert_"):
                        try:
                            aid = int(data.split("_")[2])
                            db = get_db(); c = db.cursor()
                            c.execute("UPDATE alerts SET active=0 WHERE id=%s AND chat=%s", (aid, str(chat_id)))
                            db.commit(); db.close()
                            edit(chat_id, message_id, "✅ Alert deleted.", [[{"text": "📋 My Alerts", "callback_data": "my_alerts"}]])
                        except Exception as e:
                            logger.error("[DEL ALERT] %s" % e)

                    # ── ALERTS — Watchlist ────────────────────────────────────────
                    if data == "watchlist":
                        db = get_db(); c = db.cursor()
                        c.execute("SELECT coin FROM watchlists WHERE chat=%s", (str(chat_id),))
                        wl = [r[0] for r in c.fetchall()]; db.close()
                        limit = 30 if (get_bot_mode() == "everyone" or is_pro(chat_id)) else 10
                        lines = [f"⭐ <b>My Watchlist</b> ({len(wl)}/{limit})\n"]
                        if wl:
                            for coin in wl:
                                p, ch = get_best_price(coin)
                                lines.append(f"• <b>{coin}</b>  {format_price(p)}  {format_change(ch) if ch else ''}")
                        else:
                            lines.append("Empty — type a coin symbol to add (e.g. <code>BTC</code>)")
                        btns = [
                            [{"text": "➕ Add Coin", "callback_data": "wl_add"}],
                            [{"text": "🗑 Remove Coin", "callback_data": "wl_remove"}],
                            [{"text": "⬅ Back", "callback_data": "menu_alerts"}],
                        ]
                        edit(chat_id, message_id, "\n".join(lines), btns)

                    if data == "wl_add":
                        set_state(chat_id, "awaiting_wl_add")
                        edit(chat_id, message_id, "⭐ <b>Add to Watchlist</b>\n\nSend the coin symbol.\nExample: <code>ETH</code>",
                            [[{"text": "❌ Cancel", "callback_data": "watchlist"}]])

                    if data == "wl_remove":
                        db = get_db(); c = db.cursor()
                        c.execute("SELECT coin FROM watchlists WHERE chat=%s", (str(chat_id),))
                        wl = [r[0] for r in c.fetchall()]; db.close()
                        if not wl:
                            edit(chat_id, message_id, "Your watchlist is empty.", [[{"text": "⬅ Back", "callback_data": "watchlist"}]])
                        else:
                            btns = [[{"text": f"🗑 {coin}", "callback_data": f"wl_del_{coin}"}] for coin in wl]
                            btns.append([{"text": "⬅ Back", "callback_data": "watchlist"}])
                            edit(chat_id, message_id, "🗑 <b>Remove from Watchlist</b>\n\nSelect coin to remove:", btns)

                    if data.startswith("wl_del_"):
                        coin = data.split("wl_del_")[1].upper()
                        db = get_db(); c = db.cursor()
                        c.execute("DELETE FROM watchlists WHERE chat=%s AND coin=%s", (str(chat_id), coin))
                        db.commit(); db.close()
                        edit(chat_id, message_id, f"✅ <b>{coin}</b> removed from watchlist.", [[{"text": "⭐ Watchlist", "callback_data": "watchlist"}]])

                    # ── ALERTS — Smart Alerts (Pro) ───────────────────────────────
                    if data == "smart_alerts":
                        if get_bot_mode() != "everyone" and not is_pro(chat_id):
                            edit(chat_id, message_id, "⭐ <b>Pro Feature</b>\n\nSmart Alerts are available for Pro users only.",
                                [[{"text": "💎 Upgrade", "callback_data": "upgrade"}, {"text": "⬅ Back", "callback_data": "menu_alerts"}]])
                        else:
                            lines = [
                                "⚡ <b>Smart Alerts</b>\n",
                                "Smart Alerts automatically monitor key levels and notify you when:",
                                "• Major coins test key support/resistance",
                                "• 5%+ moves detected on your watchlist",
                                "• AI analysis available on demand",
                                "",
                                "✅ Smart Alerts are <b>active</b> — you will be notified in this channel automatically.",
                            ]
                            edit(chat_id, message_id, "\n".join(lines), [[{"text": "⬅ Back", "callback_data": "menu_alerts"}]])

                    # ── MARKETS — Gainers ─────────────────────────────────────────
                    if data == "gainers":
                        gainers, _ = get_gainers_losers()
                        if gainers:
                            lines = ["📈 <b>Top Gainers (24h)</b>\n"]
                            for coin, price, ch in gainers:
                                lines.append(f"• <b>{coin}</b>  {format_price(price)}  {format_change(ch)}")
                            edit(chat_id, message_id, "\n".join(lines),
                                [[{"text": "📉 Losers", "callback_data": "losers"}, {"text": "⬅ Back", "callback_data": "menu_markets"}]])
                        else:
                            edit(chat_id, message_id, "📈 No gainer data available right now.", [[{"text": "⬅ Back", "callback_data": "menu_markets"}]])

                    # ── MARKETS — Losers ──────────────────────────────────────────
                    if data == "losers":
                        _, losers = get_gainers_losers()
                        if losers:
                            lines = ["📉 <b>Top Losers (24h)</b>\n"]
                            for coin, price, ch in losers:
                                lines.append(f"• <b>{coin}</b>  {format_price(price)}  {format_change(ch)}")
                            edit(chat_id, message_id, "\n".join(lines),
                                [[{"text": "📈 Gainers", "callback_data": "gainers"}, {"text": "⬅ Back", "callback_data": "menu_markets"}]])
                        else:
                            edit(chat_id, message_id, "📉 No loser data available right now.", [[{"text": "⬅ Back", "callback_data": "menu_markets"}]])

                    # ── MARKETS — Charts ──────────────────────────────────────────
                    if data == "charts":
                        edit(chat_id, message_id,
                            "📊 <b>Charts</b>\n\nView live charts on TradingView:\n\n"
                            "• BTC: tradingview.com/chart?symbol=BTCUSD\n"
                            "• ETH: tradingview.com/chart?symbol=ETHUSD\n"
                            "• SOL: tradingview.com/chart?symbol=SOLUSD\n\n"
                            "<i>Tap a coin in /market for quick price data.</i>",
                            [[{"text": "⬅ Back", "callback_data": "menu_markets"}]])

                    # ── MARKETS — Dominance ───────────────────────────────────────
                    if data == "dominance":
                        try:
                            resp = fetch_with_backoff("https://api.coingecko.com/api/v3/global")
                            gdata = resp.get("data", {}) if resp else {}
                            dom = gdata.get("market_cap_percentage", {})
                            btc_d = dom.get("btc", 0)
                            eth_d = dom.get("eth", 0)
                            total = gdata.get("total_market_cap", {}).get("usd", 0)
                            lines = [
                                "🌐 <b>Market Dominance</b>\n",
                                f"BTC Dominance: <b>{btc_d:.1f}%</b>",
                                f"ETH Dominance: <b>{eth_d:.1f}%</b>",
                                f"Others: <b>{100-btc_d-eth_d:.1f}%</b>",
                                "",
                                f"Total Market Cap: <b>${total/1e9:.0f}B</b>",
                            ]
                            edit(chat_id, message_id, "\n".join(lines), [[{"text": "⬅ Back", "callback_data": "menu_markets"}]])
                        except Exception as e:
                            edit(chat_id, message_id, "⚠️ Could not load dominance data.", [[{"text": "⬅ Back", "callback_data": "menu_markets"}]])

                    # ── INTELLIGENCE — Market Outlook ─────────────────────────────
                    if data == "market_outlook":
                        if get_bot_mode() != "everyone" and not is_pro(chat_id):
                            edit(chat_id, message_id,
                                "🔒 <b>Pro Feature</b>\n\n"
                                "Market Outlook is available for Pro users only.\n\n"
                                "Upgrade to get:\n"
                                "• Full AI market analysis\n"
                                "• Daily trade ideas\n"
                                "• Pro channel alerts with AI",
                                [[{"text": "💎 Upgrade to Pro", "callback_data": "upgrade"}, {"text": "⬅ Back", "callback_data": "menu_intelligence"}]])
                            continue
                        allowed, used, limit = check_ai_limit(chat_id)
                        if not allowed:
                            edit(chat_id, message_id, ai_limit_msg(used, limit), UPGRADE_BTN)
                            continue
                        btc_p, btc_c = get_best_price("BTC")
                        eth_p, eth_c = get_best_price("ETH")
                        fg_data = get_fear_greed()
                        fg_val = fg_data[0]["value"] if fg_data else "N/A"
                        fg_lbl = fg_data[0]["value_classification"] if fg_data else "N/A"
                        prompt = (
                            f"BTC: {format_price(btc_p)} ({format_change(btc_c)}), "
                            f"ETH: {format_price(eth_p)} ({format_change(eth_c)}). "
                            f"Fear & Greed: {fg_val}/100 ({fg_lbl}). "
                            f"Give a full market outlook for the next 24-48 hours covering BTC, ETH, and overall sentiment."
                        )
                        track_feature(chat_id, "ai_question")
                        edit(chat_id, message_id, "🔮 <b>Market Outlook</b>\n\n⏳ Analyzing...", None)
                        analysis, provider = ask_ai(prompt)
                        remaining = (limit - used - 1) if limit else None
                        footer = f"\n\n<i>💬 {remaining} free AI uses left today.</i>" if remaining is not None and remaining >= 0 else ""
                        text = f"🔮 <b>Market Outlook</b>\n\n{analysis or 'Analysis unavailable right now.'}\n\n<i>NFA - DYOR</i>{footer}"
                        edit(chat_id, message_id, text, [[{"text": "🔄 Refresh", "callback_data": "market_outlook"}, {"text": "⬅ Back", "callback_data": "menu_intelligence"}]])

                    # ── INTELLIGENCE — Trade Setup ─────────────────────────────────
                    if data and data.startswith("ts_tier_"):
                        clear_state(chat_id)
                        rest = data[len("ts_tier_"):]
                        parts = rest.rsplit("_", 1)
                        answer_cb(cb_id)
                        if len(parts) != 2:
                            edit(chat_id, message_id, "Invalid request.", [[{"text": "⬅ Back", "callback_data": "menu_intelligence"}]])
                            continue
                        coin, tier = parts[0], parts[1]
                        if coin not in COINS or tier not in ("steady", "momentum", "edge"):
                            edit(chat_id, message_id, "Unknown coin or tier.", [[{"text": "⬅ Back", "callback_data": "menu_intelligence"}]])
                            continue
                        if get_bot_mode() != "everyone" and not is_pro(chat_id) and tier != "momentum":
                            edit(chat_id, message_id, "⭐ EDGE and SAFE are <b>Pro</b> only.\nFree users can run <b>NORMAL</b>.",
                                 [[{"text": "💎 Upgrade", "callback_data": "upgrade"}, {"text": "⬅ Back", "callback_data": "menu_intelligence"}]])
                            continue
                        edit(chat_id, message_id, f"Building <b>{tier}</b> setup for <b>{coin}</b>…")
                        try:
                            msg, trade, idea_id = generate_trade_idea(coin, tier)
                            if msg:
                                edit(chat_id, message_id, msg[:3500], [[{"text": "⬅ Intelligence", "callback_data": "menu_intelligence"}]])
                            else:
                                edit(chat_id, message_id, f"No valid setup for {coin} ({tier}) right now.\nStructure/risk rules did not pass.\n\n<i>NFA — DYOR</i>",
                                     [[{"text": "⬅ Back", "callback_data": "menu_intelligence"}]])
                        except Exception as ex:
                            logger.error("[TS TIER] %s", ex)
                            edit(chat_id, message_id, "Setup failed. Try again.", [[{"text": "⬅ Back", "callback_data": "menu_intelligence"}]])
                        continue

                    if data == "trade_setup":
                        set_state(chat_id, "awaiting_trade_setup_coin")
                        if get_bot_mode() == "everyone" or is_pro(chat_id):
                            tip = (
                                "📊 <b>AI Trade Setup (Pro)</b>\n\n"
                                "You can choose:\n"
                                "🟡 <b>NORMAL</b> — balanced trend continuation (default)\n"
                                "🔴 <b>EDGE / AGGRESSIVE</b> — earlier entries, higher setup risk\n"
                                "🟢 <b>SAFE</b> — highest confirmation, fewer trades\n\n"
                                "Send a coin symbol, e.g. <code>BTC</code>\n"
                                "Then pick the tier on the next step."
                            )
                        else:
                            tip = (
                                "📊 <b>AI Trade Setup (Free)</b>\n\n"
                                "Free users get <b>NORMAL</b> setups only (balanced structure + risk rules).\n\n"
                                "⭐ <b>Pro</b> can also generate:\n"
                                "• 🟡 NORMAL — default balanced setups\n"
                                "• 🔴 EDGE — earlier, higher-uncertainty opportunities\n"
                                "• 🟢 SAFE — strict confirmation only\n\n"
                                "Send a coin symbol, e.g. <code>BTC</code>"
                            )
                        edit(
                            chat_id, message_id, tip,
                            [[{"text": "💎 Upgrade", "callback_data": "upgrade"},
                              {"text": "❌ Cancel", "callback_data": "menu_intelligence"}]],
                        )

                    if data == "p2p_history":
                        try:
                            db = get_db(); c = db.cursor()
                            c.execute("SELECT crypto, fiat, buy_rate, sell_rate, timestamp FROM community_p2p ORDER BY id DESC LIMIT 10")
                            rows = c.fetchall(); db.close()
                            if rows:
                                lines = ["📜 <b>Recent P2P Rates</b>\n"]
                                for crypto, fiat, buy, sell, ts in rows:
                                    symbol = P2P_FIATS.get(fiat, ("", fiat))[1]
                                    lines.append(f"• {crypto}/{fiat}  Buy {symbol}{int(buy):,}  Sell {symbol}{int(sell):,}")
                                edit(chat_id, message_id, "\n".join(lines), [[{"text": "⬅ Back", "callback_data": "menu_p2p"}]])
                            else:
                                edit(chat_id, message_id, "No P2P history available yet.", [[{"text": "⬅ Back", "callback_data": "menu_p2p"}]])
                        except Exception as e:
                            edit(chat_id, message_id, "⚠️ Could not load P2P history.", [[{"text": "⬅ Back", "callback_data": "menu_p2p"}]])

                    # ── INTELLIGENCE — Sources ────────────────────────────────────
                    if data == "sources":
                        edit(chat_id, message_id,
                            "📡 <b>Data Sources</b>\n\n"
                            "💰 <b>Prices:</b> Kraken, OKX, Bybit, CoinGecko\n"
                            "📊 <b>Market Data:</b> CoinGecko\n"
                            "😱 <b>Fear & Greed:</b> Alternative.me\n"
                            "📰 <b>News:</b> CryptoPanic, CoinDesk RSS\n"
                            "🤖 <b>AI:</b> DeepSeek (primary), Mistral (fallback), Qwen (fallback)\n"
                            "💱 <b>P2P:</b> Binance P2P, Bybit P2P\n\n"
                            "<i>Data refreshes every 5–10 minutes.</i>",
                            [[{"text": "⬅ Back", "callback_data": "menu_intelligence"}]])

                    # ── ADMIN — Publish button ────────────────────────────────────
                    if data == "admin_publish" and chat_id in ADMIN_IDS:
                        btns = [
                            [{"text": "🌅 Morning", "callback_data": "ap_morning"}, {"text": "⚡ Midday", "callback_data": "ap_midday"}],
                            [{"text": "🌙 Evening", "callback_data": "ap_evening"}, {"text": "📊 Weekly", "callback_data": "ap_weekly"}],
                            [{"text": "⬅ Back", "callback_data": "main_menu"}],
                        ]
                        edit(chat_id, message_id, "📢 <b>Force Publish</b>\n\nChoose post type:", btns)

                    if data == "ap_morning" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "⏳ Publishing...", None)
                        pro_content = build_morning_briefing_pro()
                        post_to_channel(pro_content if get_bot_mode() == "everyone" else build_morning_briefing())
                        post_to_pro_channel(pro_content)
                        edit(chat_id, message_id, "✅ Morning briefing published.", [[{"text": "⬅ Back", "callback_data": "admin_publish"}]])

                    if data == "ap_midday" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "⏳ Publishing...", None)
                        pro_content = build_midday_snapshot_pro()
                        post_to_channel(pro_content if get_bot_mode() == "everyone" else build_midday_snapshot())
                        post_to_pro_channel(pro_content)
                        edit(chat_id, message_id, "✅ Midday snapshot published.", [[{"text": "⬅ Back", "callback_data": "admin_publish"}]])

                    if data == "ap_evening" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "⏳ Publishing...", None)
                        pro_content = build_evening_recap_pro()
                        post_to_channel(pro_content if get_bot_mode() == "everyone" else build_evening_recap())
                        post_to_pro_channel(pro_content)
                        edit(chat_id, message_id, "✅ Evening recap published.", [[{"text": "⬅ Back", "callback_data": "admin_publish"}]])

                    if data == "ap_weekly" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "⏳ Publishing...", None)
                        pro_content = build_weekly_edge_pro()
                        post_to_channel(pro_content if get_bot_mode() == "everyone" else build_weekly_edge())
                        post_to_pro_channel(pro_content)
                        edit(chat_id, message_id, "✅ Weekly edge published.", [[{"text": "⬅ Back", "callback_data": "admin_publish"}]])

                    # ── ADMIN — Settings ──────────────────────────────────────────
                    if data == "admin_settings" and chat_id in ADMIN_IDS:
                        mode = get_bot_mode().upper()
                        ch_status = "✅ Enabled" if get_channel_enabled() else "⏸️ Disabled"
                        edit(chat_id, message_id,
                            f"⚙️ <b>Bot Settings</b>\n\n"
                            f"🤖 Mode: <b>{mode}</b>\n"
                            f"📢 Channel: <b>{ch_status}</b>\n"
                            f"📢 Pro Channel: <b>{'✅ Set' if get_pro_channel_id() and get_pro_channel_id() != '-100XXXXXXXXX' else '❌ Not Set'}</b>\n\n"
                            f"Use /mode everyone or /mode pro to change mode.\n"
                            f"Use /togglechannel to toggle posting.",
                            [[{"text": "⬅ Back", "callback_data": "adm_settings_menu"}]])

                    # ── ADMIN — Broadcast ─────────────────────────────────────────
                    if data == "admin_broadcast" and chat_id in ADMIN_IDS:
                        set_state(chat_id, "awaiting_broadcast")
                        edit(chat_id, message_id,
                            "📣 <b>Broadcast Message</b>\n\nSend the message to broadcast to all users.",
                            [[{"text": "❌ Cancel", "callback_data": "adm_users"}]])

                    # ── ADMIN — Ban user ──────────────────────────────────────────
                    if data == "admin_ban" and chat_id in ADMIN_IDS:
                        set_state(chat_id, "awaiting_ban_id")
                        edit(chat_id, message_id,
                            "🔨 <b>Ban User</b>\n\nSend the Telegram ID of the user to ban.",
                            [[{"text": "❌ Cancel", "callback_data": "adm_users"}]])

                    # ── ADMIN — Logs ──────────────────────────────────────────────
                    if data == "admin_logs" and chat_id in ADMIN_IDS:
                        try:
                            with open(LOG_FILE, "r") as lf:
                                lines = lf.readlines()
                            last = "".join(lines[-30:]) if lines else "No logs."
                            edit(chat_id, message_id, f"📋 <b>Recent Logs</b>\n\n<pre>{last[-3000:]}</pre>",
                                [[{"text": "⬅ Back", "callback_data": "adm_system"}]])
                        except Exception as e:
                            edit(chat_id, message_id, f"⚠️ Could not read logs: {e}", [[{"text": "⬅ Back", "callback_data": "adm_system"}]])

                    # ── ADMIN — Dashboard ────────────────────────────────────────
                    if data == "admin_dashboard" and chat_id in ADMIN_IDS:
                        edit(chat_id, message_id, "⏳ Loading dashboard...", None)
                        dashboard = build_admin_dashboard()
                        edit(chat_id, message_id, dashboard,
                            [[{"text": "📦 Content Packages", "callback_data": "admin_content_packages"},
                              {"text": "🔄 Refresh", "callback_data": "admin_dashboard"}],
                             [{"text": "⬅ Back", "callback_data": "adm_analytics"}]])

                    # ── ADMIN — Content Packages ──────────────────────────────────
                    if data == "admin_content_packages" and chat_id in ADMIN_IDS:
                        pkgs = get_pending_content_packages(limit=8)
                        if not pkgs:
                            edit(chat_id, message_id,
                                "📦 <b>Content Packages</b>\n\nNo pending packages.\n\n"
                                "Use /contentpackage morning|midday|evening|weekly to generate one manually.",
                                [[{"text": "⬅ Back", "callback_data": "adm_channel"}]])
                        else:
                            lines = ["📦 <b>Pending Content Packages</b>\n"]
                            btns = []
                            for pid, ptype, psrc, pdate in pkgs:
                                lines.append(f"• #{pid} <b>{ptype.upper()}</b> — {pdate[:16]}")
                                btns.append([{"text": f"#{pid} {ptype.upper()}", "callback_data": f"pkg_view_{pid}"}])
                            lines.append("\nTap a package to review it.")
                            btns.append([{"text": "⬅ Back", "callback_data": "main_menu"}])
                            edit(chat_id, message_id, "\n".join(lines), btns)

                    if data.startswith("pkg_view_") and chat_id in ADMIN_IDS:
                        try:
                            pkg_id = int(data.split("pkg_view_")[1])
                            pkg = get_content_package_by_id(pkg_id)
                            if not pkg:
                                edit(chat_id, message_id, f"❌ Package #{pkg_id} not found.",
                                    [[{"text": "⬅ Back", "callback_data": "admin_content_packages"}]])
                            else:
                                admin_msg = format_content_package_for_admin(pkg_id, pkg, pkg.get("package_type","?"))
                                # Send as new message (too long to edit into existing)
                                send(chat_id, admin_msg[:4000])
                                edit(chat_id, message_id,
                                    f"📦 <b>Package #{pkg_id}</b> shown above.\n\nMark as:",
                                    [[{"text": "✅ Approve", "callback_data": f"pkg_approve_{pkg_id}"},
                                      {"text": "🗑 Discard", "callback_data": f"pkg_discard_{pkg_id}"}],
                                     [{"text": "⬅ Back", "callback_data": "admin_content_packages"}]])
                        except (ValueError, IndexError):
                            edit(chat_id, message_id, "❌ Invalid package ID.",
                                [[{"text": "⬅ Back", "callback_data": "admin_content_packages"}]])

                    if data.startswith("pkg_approve_") and chat_id in ADMIN_IDS:
                        try:
                            pkg_id = int(data.split("pkg_approve_")[1])
                            mark_package_status(pkg_id, "approved")
                            edit(chat_id, message_id, f"✅ Package #{pkg_id} marked as <b>approved</b>.",
                                [[{"text": "📦 Packages", "callback_data": "admin_content_packages"},
                                  {"text": "⬅ Back", "callback_data": "main_menu"}]])
                        except Exception as _e:
                            edit(chat_id, message_id, "❌ Error updating package.", [[{"text": "⬅ Back", "callback_data": "admin_content_packages"}]])

                    if data.startswith("pkg_discard_") and chat_id in ADMIN_IDS:
                        try:
                            pkg_id = int(data.split("pkg_discard_")[1])
                            mark_package_status(pkg_id, "discarded")
                            edit(chat_id, message_id, f"🗑 Package #{pkg_id} discarded.",
                                [[{"text": "📦 Packages", "callback_data": "admin_content_packages"},
                                  {"text": "⬅ Back", "callback_data": "main_menu"}]])
                        except Exception as _e:
                            edit(chat_id, message_id, "❌ Error updating package.", [[{"text": "⬅ Back", "callback_data": "admin_content_packages"}]])

                    # ── ALERT CONDITION CALLBACKS ─────────────────────────────
                    if data in ("alert_cond_above", "alert_cond_below"):
                        _, sdata = get_state(chat_id)
                        coin = sdata.get("coin", "BTC")
                        cond = "above" if data == "alert_cond_above" else "below"
                        set_state(chat_id, "awaiting_alert_target", {"coin": coin, "condition": cond})
                        price, _ = get_best_price(coin)
                        edit(chat_id, message_id,
                            f"➕ <b>{coin} Alert</b>\n\nCurrent: <b>{format_price(price)}</b>\n\n"
                            f"Send the target price (e.g. <code>65000</code>):",
                            [[{"text": "❌ Cancel", "callback_data": "menu_alerts"}]])

                    # ── LANGUAGE SETTINGS ─────────────────────────────────────────────
                    for lang_code, lang_name in [("en","English"),("ha","Hausa"),("ig","Igbo"),("yo","Yoruba")]:
                        if data == f"lang_{lang_code}":
                            edit(chat_id, message_id,
                                f"✅ Language set to <b>{lang_name}</b>.\n\n<i>Note: Full multilingual support coming soon.</i>",
                                [[{"text": "⬅ Back", "callback_data": "settings_language"}]])

            time.sleep(2)

        except Exception as e:
            logger.error("[MAIN ERROR] %s" % e)
            import traceback
            traceback.print_exc()
            time.sleep(10)

# ═══════════════════════════════════════════════════════════════════════════
