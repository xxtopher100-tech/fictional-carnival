"""Market Pulse Bot — alerts module (split from the real monolithic bot.py)."""

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
from market_pulse.config_runtime import ADMIN_IDS, logger
from market_pulse.db import get_db
from market_pulse.fear_greed import get_fear_greed
from market_pulse.helpers import format_change, format_price, wat_now
from market_pulse.p2p import get_p2p_rate
from market_pulse.price_fetchers import get_best_price, get_secondary_coin
from market_pulse.pro_system import should_show_upsell, FREE_UPSELL_BLOCK
from market_pulse.telegram_api import post_to_channel, post_to_pro_channel, send


# ─── extracted section ───
# 🔔 KEY MARKET ALERT SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

# Key alert coins — Nigerian retail focus (USDT is P2P, not a "key level" coin).
# Order = priority when two coins are equally close to a level.
# Admin can still change with /setwatchlist.
KEY_ALERT_COINS = ["BTC", "SOL", "ETH", "BNB"]

# Priority rank for sort (lower = preferred). Unknown coins rank last.
KEY_ALERT_PRIORITY = {"BTC": 0, "SOL": 1, "ETH": 2, "BNB": 3, "XRP": 4}

# Max ONE alert per check cycle — channel stays clean
MAX_ALERTS_PER_CYCLE = 1

# Real-time follow-through window (seconds) before channel CONFIRMED post.
# Allowed useful range 5–30. Env override: KEY_ALERT_RT_WINDOW_SEC
KEY_ALERT_RT_WINDOW_SEC = int((__import__('os').environ.get('KEY_ALERT_RT_WINDOW_SEC') or '15'))
KEY_ALERT_RT_SAMPLE_SEC = float((__import__('os').environ.get('KEY_ALERT_RT_SAMPLE_SEC') or '1.0'))
KEY_ALERT_RT_HOLD_FRAC = float((__import__('os').environ.get('KEY_ALERT_RT_HOLD_FRAC') or '0.8'))
KEY_ALERT_RT_CONTINUATION_FRAC = float((__import__('os').environ.get('KEY_ALERT_RT_CONT_FRAC') or '0.00015'))


# Min seconds between ANY key alert (not a fixed schedule — a quiet floor)
KEY_ALERT_GLOBAL_MIN_SECONDS = 90  # anti double-post only — not an hourly schedule

# Must be this close to a level to count as a real test (0.5%)
KEY_LEVEL_TOLERANCE = 0.005

# Cooldown: same symbol + level + event type (DB-backed)
KEY_ALERT_COOLDOWN_HOURS = 4

# Extra: same coin any TESTING event cannot re-fire within this many hours
KEY_ALERT_COIN_TESTING_HOURS = 3

# Meaningful movement required before re-alerting the same level.
# Prefer ~1 ATR on the 1h timeframe; fall back to this % of price if ATR unavailable.
KEY_ALERT_MOVE_ATR_MULT = 1.0
KEY_ALERT_MOVE_PCT_FALLBACK = 0.015  # 1.5%

# In-memory last-alert state for movement checks (process lifetime)
# key: "COIN|level|event" -> {"price": float, "ts": float, "level": float}
_last_alert_state = {}


def _level_bucket(level, frac: float = 0.008) -> float:
    """Bucket nearby dynamic levels together (e.g. ETH 2505 vs 2509).

    Uses log spacing so a ~0.8% band shares one key — stops jitter spam.
    """
    try:
        import math
        lv = float(level)
        if lv <= 0:
            return 0.0
        # stable: round log so neighbors within ~frac map to same index
        idx = round(math.log(lv) / math.log(1.0 + frac))
        return round((1.0 + frac) ** idx, 8)
    except Exception:
        try:
            return round(float(level), 2)
        except Exception:
            return 0.0


def _levels_near(a, b, frac: float = 0.008) -> bool:
    try:
        a, b = float(a), float(b)
        if a <= 0 or b <= 0:
            return False
        return abs(a - b) / max(a, b) <= frac
    except Exception:
        return False


def _alert_event_key(coin, level, event_label):
    """Stable key for symbol + level + event type dedup.

    Level is log-bucketed (~0.8%) so dynamic level jitter shares one cooldown.
    """
    level_r = _level_bucket(level)
    event = (event_label or "UNKNOWN").upper().replace(" ", "_")
    return f"key_alert_{coin}|{level_r}|{event}"



def _zone_state_key(coin, level, event_label: str = "") -> str:
    """Restart-safe zone key. TESTING uses one cluster per coin so $2505≈$2509.

    Breakout/other events still bucket by level so different structures stay distinct.
    """
    lab = (event_label or "").upper()
    if "TESTING" in lab:
        return f"key_zone_{coin}|TESTING_CLUSTER"
    level_r = _level_bucket(level)
    return f"key_zone_{coin}|{level_r}"


def _price_in_level_zone(price, level, tol=None) -> bool:
    """True when price is within proximity of the key level (TESTING zone)."""
    if not price or not level:
        return False
    try:
        p, lv = float(price), float(level)
        if lv <= 0:
            return False
        t = KEY_LEVEL_TOLERANCE if tol is None else float(tol)
        return abs(p - lv) / lv <= t
    except Exception:
        return False


def _zone_gate_allow(prev: dict, in_zone: bool, event_label: str, level) -> tuple:
    """Primary dedup: TESTING only on new zone entry; transitions always candidates.

    prev: {"in_zone": bool, "last_event": str, "level": float} or None/empty
    Returns (allow, reason)
    """
    label = (event_label or "").upper()
    prev = prev or {}
    was_in = bool(prev.get("in_zone"))
    last_event = (prev.get("last_event") or "").upper()
    prev_level = prev.get("level")

    # Materially different level vs last tracked for this bucket → allow
    try:
        if prev_level is not None and level is not None:
            pl, lv = float(prev_level), float(level)
            if pl > 0 and abs(lv - pl) / pl >= 0.012:  # ≥1.2% real shift (not 2505 vs 2509)
                return True, "materially_different_level"
    except Exception:
        pass

    is_testing = "TESTING" in label
    is_approaching = "APPROACHING" in label
    is_reclaim = "RECLAIM" in label
    is_breakout = "BREAKOUT" in label
    is_breakdown = "BELOW" in label or "BREAKDOWN" in label
    is_transition = (
        is_breakout
        or is_breakdown
        or is_reclaim
        or "REJECTION" in label
        or "INVALID" in label
    )

    if not in_zone:
        # Outside zone — no TESTING/APPROACHING spam; leave state update to caller
        if is_testing or is_approaching:
            return False, "outside_zone_proximity"
        return False, "outside_zone"

    # In zone now
    if is_testing:
        if not was_in:
            return True, "new_zone_entry"
        # Still in same zone after a prior TESTING → suppress
        if "TESTING" in last_event:
            return False, "still_in_zone_after_testing"
        # Was in zone with a different event → re-test only if event type changes
        if last_event and last_event != label:
            return True, "event_changed_while_in_zone"
        return False, "still_in_zone"

    # APPROACHING: only on new approach after being outside zone (or after different event)
    if is_approaching:
        if not was_in:
            return True, "new_approach"
        if last_event and last_event != label and "APPROACHING" not in last_event:
            return True, "approach_after_other_event"
        return False, "still_approaching_same_zone"

    if is_transition:
        if last_event != label:
            return True, "state_transition"
        return False, "same_transition_repeat"

    # Other labels: allow once per distinct event while in zone
    if last_event != label:
        return True, "new_event_label"
    return False, "duplicate_event"


def _load_zone_state(coin, level, event_label: str = "") -> dict:
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT value FROM admin_settings WHERE key=%s", (_zone_state_key(coin, level, event_label),))
        row = c.fetchone()
        if not row or not row[0]:
            return {}
        data = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or {})
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _save_zone_state(coin, level, in_zone: bool, event_label: str, price=None) -> None:
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        payload = json.dumps({
            "in_zone": bool(in_zone),
            "last_event": event_label or "",
            "level": float(level) if level is not None else None,
            "price": float(price) if price else None,
            "updated_at": now,
        })
        key = _zone_state_key(coin, level, event_label)
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, payload, now),
        )
        db.commit()
    except Exception as e:
        logger.debug("[KEY ZONE SAVE] %s", e)
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


def _clear_zone_if_left(coin, level, price, event_label: str = "") -> None:
    """If price left the zone, mark in_zone=false so a later re-entry can alert."""
    in_zone = _price_in_level_zone(price, level)
    if in_zone:
        return
    # Clear both level bucket and TESTING cluster so re-entry works
    for lab in (event_label, "TESTING RESISTANCE", "TESTING SUPPORT", ""):
        prev = _load_zone_state(coin, level, lab)
        if prev.get("in_zone"):
            _save_zone_state(coin, level, False, prev.get("last_event") or lab, price=price)



def _get_key_alert_cooldown(coin, level=None, event_label=None):
    """Return True if this symbol+level+event is still in cooldown. DB-backed."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        since = (wat_now() - timedelta(hours=KEY_ALERT_COOLDOWN_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
        if level is not None and event_label:
            key = _alert_event_key(coin, level, event_label)
        else:
            key = f"key_alert_{coin}"
        c.execute(
            "SELECT updated_at FROM admin_settings WHERE key=%s AND updated_at >= %s",
            (key, since)
        )
        if c.fetchone() is not None:
            return True
        # Also block if any recent alert for this coin at nearly the same level
        if level is not None:
            level_r = round(float(level), 6)
            c.execute(
                "SELECT key FROM admin_settings "
                "WHERE key LIKE %s AND updated_at >= %s",
                (f"key_alert_{coin}|%", since)
            )
            for row in c.fetchall():
                parts = (row[0] or "").split("|")
                if len(parts) >= 2:
                    try:
                        prev_level = float(parts[1])
                        if abs(prev_level - level_r) / max(level_r, 1e-9) < 0.002:
                            return True
                    except (ValueError, TypeError):
                        pass
        return False
    except Exception as e:
        logger.warning(f"[KEY ALERT CD] {e}")
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass


def _set_key_alert_cooldown(coin, level=None, event_label=None, price=None):
    """Record that we just sent an alert for this symbol+level+event."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        if level is not None and event_label:
            key = _alert_event_key(coin, level, event_label)
        else:
            key = f"key_alert_{coin}"
        value = json.dumps({"price": price, "level": level, "event": event_label}) if price else now
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, value, now)
        )
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (f"key_alert_{coin}", now, now)
        )
        db.commit()
        if level is not None and event_label and price is not None:
            _last_alert_state[_alert_event_key(coin, level, event_label)] = {
                "price": float(price), "ts": time.time(), "level": float(level)
            }
        logger.info(
            f"[KEY ALERT] Cooldown set for {coin}"
            + (f" level={level} event={event_label}" if level is not None else "")
            + f" — next alert in {KEY_ALERT_COOLDOWN_HOURS}h"
        )
    except Exception as e:
        logger.warning(f"[KEY ALERT CD SET] {e}")
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass


def _meaningful_move_away(coin, price, level, event_label):
    """True if price moved ~1 ATR (or pct fallback) from the last alert at this level."""
    key = _alert_event_key(coin, level, event_label)
    prior = _last_alert_state.get(key)
    if not prior:
        return True
    last_price = prior.get("price") or 0
    if last_price <= 0:
        return True
    move = abs(price - last_price)
    threshold = None
    try:
        from market_pulse.candle_engine import get_candles
        from market_pulse.indicators_ext import atr
        candles = get_candles(coin)
        if candles and len(candles) >= 20:
            atr_series = atr(candles, period=14)
            atr_last = atr_series[-1] if atr_series else None
            if atr_last and atr_last > 0:
                threshold = KEY_ALERT_MOVE_ATR_MULT * atr_last
    except Exception as e:
        logger.debug(f"[KEY ALERT MOVE] ATR unavailable for {coin}: {e}")
    if threshold is None:
        threshold = KEY_ALERT_MOVE_PCT_FALLBACK * price
    if move < threshold:
        logger.debug(
            f"[KEY ALERT] {coin} move {move:.4g} < threshold {threshold:.4g} — not a new event"
        )
        return False
    return True


def _15m_level_confirmation(coin, price, level, event_label, fail_open: bool = False):
    """15m structure check for key alerts — COMPLETED candles only.

    Candle source (candle_engine) only appends Binance klines with x=true
    (closed). We still refuse a bar whose open_time is inside the current
    15m wall-clock window, so a forming bar can never confirm.

    Thresholds are **percentage of level** (not ATR):
      TAG_BUF   = 0.08% of level  — wick must reach this band to count as a tag
      REACT_BUF = 0.02% of level  — close must finish back on the hold side
      BREAK_BUF = 0.08% of level  — close must clear level by this for breakout

    TAG (resistance): high >= level - TAG_BUF
    TAG (support):    low  <= level + TAG_BUF

    REACTION (rejection at resistance):
      - tagged resistance, AND
      - close < level - REACT_BUF, AND
      - close in lower 65% of the candle range (wick rejection, not full-body noise)

    REACTION (hold at support):
      - tagged support, AND
      - close > level + REACT_BUF, AND
      - close in upper 65% of the candle range

    BREAKOUT close-through: close > level + BREAK_BUF
    BREAKDOWN close-through: close < level - BREAK_BUF

    fail_open=False (default): missing/incomplete data → not confirmed.
    """
    import time as _time
    label = (event_label or "").upper()
    if "TESTING" not in label and "BREAKOUT" not in label and "BELOW" not in label:
        return True
    try:
        from market_pulse.candle_engine import get_candles_15m, candles_15m_ready
        if not candles_15m_ready(coin, min_candles=3):
            return bool(fail_open)
        candles = get_candles_15m(coin)
        if not candles:
            return bool(fail_open)
        last = candles[-1]
        # Completed-only: open_time must be at least one full 15m period in the past
        ot = last.get("open_time")
        if ot is not None:
            age_sec = _time.time() - float(ot)
            if age_sec < 15 * 60:
                # Still inside the forming window — do not confirm
                return False
        close = last.get("close")
        high = last.get("high")
        low = last.get("low")
        if close is None or high is None or low is None:
            return bool(fail_open)
        close, high, low = float(close), float(high), float(low)
        lv = float(level)
        tag_buf = abs(lv) * 0.0008      # 0.08%
        react_buf = abs(lv) * 0.0002    # 0.02%
        break_buf = abs(lv) * 0.0008    # 0.08%
        candle_range = max(high - low, abs(lv) * 1e-8)

        if "BREAKOUT" in label:
            ok = close > lv + break_buf
            if not ok:
                logger.info(
                    "[KEY ALERT 15m] %s BREAKOUT not confirmed — close %s not through %s",
                    coin, close, level,
                )
            return ok
        if "TRADING BELOW" in label or ("BELOW" in label and "TESTING" not in label):
            ok = close < lv - break_buf
            if not ok:
                logger.info(
                    "[KEY ALERT 15m] %s BELOW not confirmed — close %s not through %s",
                    coin, close, level,
                )
            return ok
        if "TESTING SUPPORT" in label:
            tagged = low <= lv + tag_buf
            closed_above = close > lv + react_buf
            # Close in upper 65% of bar → reaction, not random mid-range noise
            upper_reaction = (close - low) / candle_range >= 0.35
            return bool(tagged and closed_above and upper_reaction)
        if "TESTING RESISTANCE" in label:
            tagged = high >= lv - tag_buf
            closed_below = close < lv - react_buf
            lower_reaction = (high - close) / candle_range >= 0.35
            return bool(tagged and closed_below and lower_reaction)
        return bool(fail_open)
    except Exception as e:
        logger.debug(f"[KEY ALERT 15m] {coin}: {e}")
        return bool(fail_open)


def classify_key_alert_tier(coin, price, level, event_label) -> str:
    """EARLY vs CONFIRMED for channel posts — does not invent direction.

    EARLY: price is near a key level (zone entry) — watch only, no claim of hold/break.
    CONFIRMED: 15m candle shows real interaction (tag + reaction) or true break close.

    Returns: "EARLY" | "CONFIRMED" | "SKIP"
    """
    label = (event_label or "").upper()
    # Structural breaks must prove a close through the level
    if "BREAKOUT" in label:
        if _15m_level_confirmation(coin, price, level, "BREAKOUT", fail_open=False):
            return "CONFIRMED"
        # Proximity above level without close confirmation = early break watch only
        try:
            if float(price) > float(level):
                return "EARLY"
        except Exception:
            pass
        return "SKIP"
    if "BREAKDOWN" in label or "TRADING BELOW" in label or (label.startswith("BELOW") or "BELOW RESISTANCE" in label):
        if _15m_level_confirmation(coin, price, level, "TRADING BELOW", fail_open=False):
            return "CONFIRMED"
        try:
            if float(price) < float(level):
                return "EARLY"
        except Exception:
            pass
        return "SKIP"
    # RECLAIM: treat like structural transition — needs 15m interaction
    if "RECLAIM" in label:
        if _15m_level_confirmation(coin, price, level, event_label, fail_open=False):
            return "CONFIRMED"
        return "EARLY"
    # TESTING: default EARLY (proximity). Upgrade to CONFIRMED only if 15m tagged the level.
    if "TESTING" in label:
        if _15m_level_confirmation(coin, price, level, event_label, fail_open=False):
            return "CONFIRMED"  # confirmed *level test*, still not a trade signal
        return "EARLY"
    # APPROACHING stays EARLY (watch only — channel posts remain CONFIRMED-only)
    return "EARLY"

# ── Dynamic Key Levels ───────────────────────────────────────────────────
# No hardcoded levels. Levels are calculated on-demand from price history
# in the DB (swing highs, swing lows, round numbers near current price).
# Cache: { coin: (levels_list, calculated_at_timestamp) }
_dynamic_levels_cache = {}
_LEVELS_CACHE_TTL = 3600  # recalculate every hour

def _sample_live_price(coin):
    """Live price from WS/cache via get_best_price. None if unavailable."""
    try:
        px, _ = get_best_price(coin)
        return float(px) if px else None
    except Exception:
        return None


def realtime_follow_through(coin, level, event_label, detection_price,
                            window_sec=None, sample_sec=None) -> dict:
    """Sample live price for a few seconds after structural confirmation.

    Fail-safe: insufficient samples → cancelled (not CONFIRMED).
    Does not wait for another 15m candle.
    """
    import time as _time

    window_sec = float(window_sec if window_sec is not None else KEY_ALERT_RT_WINDOW_SEC)
    sample_sec = float(sample_sec if sample_sec is not None else KEY_ALERT_RT_SAMPLE_SEC)
    window_sec = max(5.0, min(30.0, window_sec))
    sample_sec = max(0.5, min(5.0, sample_sec))

    lv = float(level)
    react_buf = abs(lv) * 0.0002
    break_buf = abs(lv) * 0.0008
    cont = abs(lv) * KEY_ALERT_RT_CONTINUATION_FRAC
    label = (event_label or "").upper()
    det_px = float(detection_price) if detection_price else None

    if "BREAKOUT" in label:
        mode = "BREAKOUT"
    elif "TRADING BELOW" in label or ("BELOW" in label and "TESTING" not in label):
        mode = "BREAKDOWN"
    elif "SUPPORT" in label:
        mode = "SUPPORT_HOLD"
    elif "RESISTANCE" in label:
        mode = "RESISTANCE_REJECT"
    else:
        mode = "UNKNOWN"

    t0 = _time.time()
    samples = []
    while _time.time() - t0 < window_sec:
        px = _sample_live_price(coin)
        if px is not None:
            samples.append(px)
        _time.sleep(sample_sec)

    delay = _time.time() - t0
    result = {
        "ok": False,
        "reason": "",
        "samples": samples,
        "high": max(samples) if samples else None,
        "low": min(samples) if samples else None,
        "detection_price": det_px,
        "confirm_price": samples[-1] if samples else None,
        "delay_sec": round(delay, 2),
        "cancelled": True,
        "mode": mode,
        "window_sec": window_sec,
    }
    if len(samples) < 2:
        result["reason"] = "insufficient_live_samples"
        logger.info("[KEY RT] %s %s CANCEL — samples=%s", coin, mode, len(samples))
        return result

    n = len(samples)
    hold_need = max(1, int(n * KEY_ALERT_RT_HOLD_FRAC + 0.999))

    def held(pred):
        return sum(1 for p in samples if pred(p))

    if mode == "RESISTANCE_REJECT":
        if any(p >= lv for p in samples):
            result["reason"] = "reclaimed_resistance"
        elif held(lambda p: p < lv - react_buf) < hold_need:
            result["reason"] = "failed_to_hold_below"
        else:
            result["ok"] = True
            result["cancelled"] = False
            result["reason"] = "hold_below"
    elif mode == "SUPPORT_HOLD":
        if any(p <= lv for p in samples):
            result["reason"] = "lost_support"
        elif held(lambda p: p > lv + react_buf) < hold_need:
            result["reason"] = "failed_to_hold_above"
        else:
            result["ok"] = True
            result["cancelled"] = False
            result["reason"] = "hold_above"
    elif mode == "BREAKOUT":
        if any(p <= lv for p in samples):
            result["reason"] = "fell_back_into_range"
        elif held(lambda p: p > lv + break_buf) < hold_need:
            result["reason"] = "failed_to_hold_breakout"
        else:
            result["ok"] = True
            result["cancelled"] = False
            result["reason"] = "breakout_held"
    elif mode == "BREAKDOWN":
        if any(p >= lv for p in samples):
            result["reason"] = "reclaimed_breakdown"
        elif held(lambda p: p < lv - break_buf) < hold_need:
            result["reason"] = "failed_to_hold_breakdown"
        else:
            result["ok"] = True
            result["cancelled"] = False
            result["reason"] = "breakdown_held"
    else:
        result["reason"] = "unknown_mode"

    logger.info(
        "[KEY RT] %s %s %s n=%s delay=%.1fs hi=%s lo=%s reason=%s",
        coin, mode, "PASS" if result["ok"] else "CANCEL",
        n, delay, result["high"], result["low"], result["reason"],
    )
    return result



def get_dynamic_key_levels(coin, price):
    """Calculate key levels dynamically from stored price history.
    Returns a sorted list of relevant price levels for this coin.
    Falls back to round-number generation if history is insufficient."""
    now = time.time()
    cached = _dynamic_levels_cache.get(coin)
    if cached and (now - cached[1]) < _LEVELS_CACHE_TTL:
        return cached[0]

    levels = set()

    # ── 1. Swing highs and lows from 30-day price history ─────────────────
    db = None
    try:
        db = get_db()
        c = db.cursor()
        since = (wat_now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "SELECT price FROM history WHERE coin=%s AND timestamp >= %s ORDER BY timestamp ASC",
            (coin, since)
        )
        rows = c.fetchall()
        prices = [float(r[0]) for r in rows if r[0]]
    except Exception as e:
        logger.warning(f"[KEY LEVELS] DB read error for {coin}: {e}")
        prices = []
    finally:
        if db:
            try: db.close()
            except Exception: pass

    if len(prices) >= 10:
        # Find swing highs: local max with 3 bars either side
        for i in range(3, len(prices) - 3):
            window = prices[i-3:i+4]
            if prices[i] == max(window):
                levels.add(round(prices[i], _price_decimals(prices[i])))
        # Find swing lows: local min with 3 bars either side
        for i in range(3, len(prices) - 3):
            window = prices[i-3:i+4]
            if prices[i] == min(window):
                levels.add(round(prices[i], _price_decimals(prices[i])))

    # ── 2. Round psychological numbers near current price ──────────────────
    if price:
        levels.update(_round_number_levels(price))

    # ── 3. Filter to levels within 30% of current price ───────────────────
    if price:
        levels = {l for l in levels if l > 0 and abs(l - price) / price <= 0.30}

    result = sorted(levels, reverse=True)
    _dynamic_levels_cache[coin] = (result, now)
    logger.info(f"[KEY LEVELS] {coin}: {len(result)} dynamic levels calculated")
    return result


def _price_decimals(price):
    """Number of decimal places to round to based on price magnitude."""
    if price >= 10000: return 0
    if price >= 1000:  return 0
    if price >= 100:   return 1
    if price >= 10:    return 2
    if price >= 1:     return 3
    return 4


def _round_number_levels(price):
    """Generate psychological round numbers (00, 000, 0000) near a price."""
    levels = set()
    magnitude = 10 ** (len(str(int(price))) - 1)
    for mult in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        levels.add(round(magnitude * mult, _price_decimals(magnitude * mult)))
    for frac in [0.25, 0.50, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0]:
        candidate = round(price * frac / magnitude) * magnitude
        if candidate > 0:
            levels.add(candidate)
    return levels

def _nearest_key_level(price, levels, tolerance=None):
    """Find nearest key level within tolerance. Uses KEY_LEVEL_TOLERANCE by default."""
    if tolerance is None:
        tolerance = KEY_LEVEL_TOLERANCE
    for level in levels:
        if level > 0 and abs(price - level) / level <= tolerance:
            return level
    return None

def _level_label(price, level, prev_event: str = ""):
    """Event-driven label from price vs key level (not a trade signal).

    Support: level BELOW price. Resistance: level ABOVE price.
    APPROACHING = near but not yet in the tight test band.
    TESTING = inside proximity zone.
    BREAKOUT / BREAKDOWN = clear separation through the level.
    RECLAIM = price recovers a level previously lost (BREAKDOWN) or fails a breakout.
    """
    diff_pct = (price - level) / level * 100
    prev = (prev_event or "").upper()

    # Reclaim after a prior structural break (requires previous event context)
    if "BREAKDOWN" in prev or "TRADING BELOW" in prev:
        if 0 < diff_pct <= 1.5:
            return "RECLAIM SUPPORT", "🟢"
    if "BREAKOUT" in prev:
        if -1.5 <= diff_pct < 0:
            return "RECLAIM RESISTANCE", "🟠"

    if diff_pct > 1.5:
        return "BREAKOUT", "🚀"
    if 0.45 < diff_pct <= 1.5:
        return "TESTING SUPPORT", "🟠"
    if 0 < diff_pct <= 0.45:
        return "APPROACHING SUPPORT", "🟠"
    if -0.45 <= diff_pct < 0:
        return "APPROACHING RESISTANCE", "🟡"
    if -1.5 <= diff_pct < -0.45:
        return "TESTING RESISTANCE", "🟡"
    return "BREAKDOWN", "🔴"


def _distance_to_level(price, level):
    try:
        p, lv = float(price), float(level)
        if lv <= 0:
            return None
        pct = (p - lv) / lv * 100.0
        return pct
    except Exception:
        return None


def _scenario_block(price, level, event_label):
    """Actionable watch scenarios — NOT a trade signal.

    Returns list of message lines explaining what to watch for
    breakout vs rejection, and current status (Watch only / Confirmed).
    """
    label = (event_label or "").upper()
    dist = _distance_to_level(price, level)
    dist_s = f"{dist:+.2f}%" if dist is not None else "n/a"
    lvl_s = format_price(level)
    px_s = format_price(price)
    lines = [
        "",
        "👁 <b>SCENARIOS</b>  <i>(watch — not a trade yet)</i>",
        f"Distance to level: <b>{dist_s}</b>",
    ]

    if "TESTING RESISTANCE" in label:
        lines += [
            f"<b>Resistance:</b> {lvl_s}  ·  <b>Price:</b> {px_s}",
            "",
            "<b>BULLISH SCENARIO:</b>",
            f"Confirmed 15m/1H close <b>above {lvl_s}</b> could validate a breakout long.",
            "",
            "<b>BEARISH SCENARIO:</b>",
            f"Rejection (wick into level, close back below) could set up a short if structure agrees.",
            "",
            "<b>INVALIDATION / WATCH:</b>",
            f"Breakout thesis weakens if price rejects and loses the approach structure below.",
            "",
            "<b>CURRENT STATUS:</b> No confirmed trade — <b>Watch only</b>.",
        ]
    elif "TESTING SUPPORT" in label:
        lines += [
            f"<b>Support:</b> {lvl_s}  ·  <b>Price:</b> {px_s}",
            "",
            "<b>BULLISH SCENARIO:</b>",
            f"Hold above {lvl_s} (rejection wick + close back up) can support a long if trend aligns.",
            "",
            "<b>BEARISH SCENARIO:</b>",
            f"Confirmed close <b>below {lvl_s}</b> could validate a breakdown short.",
            "",
            "<b>INVALIDATION / WATCH:</b>",
            f"Long thesis fails on a decisive close under support; short thesis needs that break.",
            "",
            "<b>CURRENT STATUS:</b> No confirmed trade — <b>Watch only</b>.",
        ]
    elif "BREAKOUT" in label:
        lines += [
            f"Price cleared above {lvl_s}.",
            "<b>CURRENT STATUS:</b> Breakout candidate — trade only if risk params validate.",
        ]
    elif "BELOW" in label:
        lines += [
            f"Price trading below {lvl_s}.",
            "<b>CURRENT STATUS:</b> Breakdown / weak structure — trade only if risk params validate.",
        ]
    else:
        lines += [
            f"Level {lvl_s} in play at {px_s}.",
            "<b>CURRENT STATUS:</b> Monitor — no automatic trade.",
        ]
    return lines


def _key_alert_phase(event_label):
    """Map label → phase for state messaging."""
    label = (event_label or "").upper()
    if "BREAKOUT" in label:
        return "CONFIRMED_BREAKOUT"
    if "TESTING SUPPORT" in label:
        return "TESTING"
    if "TESTING RESISTANCE" in label:
        return "TESTING"
    if "BELOW" in label:
        return "CONFIRMED_BREAKDOWN"
    return "WATCH"


def _validate_alert(coin, price, entry, stop, target, label, direction="long"):
    """Pre-send validation. Returns (valid, reason). Direction-aware for long/short."""
    # Check for unresolved placeholders
    combined = f"{coin}{price}{entry}{stop}{target}{label}"
    if re.search(r'\\1|\{[a-z_]+\}|%s|None', str(combined)):
        return False, "Unresolved placeholder detected"
    # Price sanity
    if not price or price <= 0:
        return False, "Invalid price"
    # Entry/stop/target logic — direction-aware
    if entry and stop and target:
        try:
            e = float(str(entry).replace("$","").replace(",",""))
            s = float(str(stop).replace("$","").replace(",",""))
            t = float(str(target).replace("$","").replace(",",""))
            if direction == "long":
                if s >= e:
                    return False, f"Long stop {s} >= entry {e}"
                if t <= e:
                    return False, f"Long target {t} <= entry {e}"
                if e <= 0:
                    return False, "Entry price must be positive"
                rr = (t - e) / (e - s)
            else:  # short
                if s <= e:
                    return False, f"Short stop {s} <= entry {e}"
                if t >= e:
                    return False, f"Short target {t} >= entry {e}"
                rr = (e - t) / (s - e)
            if rr < 1.0:
                return False, f"R:R {rr:.2f} below minimum 1:1"
        except Exception as ex:
            logger.warning(f"[VALIDATE ALERT] Parse error: {ex}")
    return True, "OK"

def _infer_direction(entry, stop, bias=None):
    """Infer long/short from stop placement; bias is a weak fallback only."""
    try:
        e = float(str(entry).replace("$", "").replace(",", ""))
        s = float(str(stop).replace("$", "").replace(",", ""))
        if s < e:
            return "long"
        if s > e:
            return "short"
    except Exception:
        pass
    if bias and str(bias).lower().startswith("bear"):
        return "short"
    return "long"


def _calc_trade_metrics(entry, stop, target, size_usd=1000, direction=None):
    """Calculate R:R and P&L in code. Direction-aware; returns None if invalid."""
    try:
        e = float(str(entry).replace("$", "").replace(",", ""))
        s = float(str(stop).replace("$", "").replace(",", ""))
        t = float(str(target).replace("$", "").replace(",", ""))
        if e <= 0 or s <= 0 or t <= 0:
            return None
        if direction is None:
            direction = _infer_direction(e, s)
        if direction == "long":
            if not (s < e and t > e):
                return None
            risk = e - s
            reward = t - e
        else:
            if not (s > e and t < e):
                return None
            risk = s - e
            reward = e - t
        if risk <= 0:
            return None
        risk_pct = risk / e * 100
        reward_pct = reward / e * 100
        rr = reward / risk
        return {
            "rr": round(rr, 2),
            "risk_pct": round(risk_pct, 2),
            "reward_pct": round(reward_pct, 2),
            "pot_profit": round(size_usd * (reward_pct / 100), 2),
            "pot_loss": round(size_usd * (risk_pct / 100), 2),
            "direction": direction,
        }
    except Exception:
        return None


def _p2p_context_line():
    """Factual USDT/NGN quote only — not a causal claim about crypto levels."""
    try:
        buy, sell, src = get_p2p_rate("USDT", "NGN")
        if not buy or not sell:
            return (
                "Separate context — Nigerian USDT/NGN P2P quote unavailable right now "
                "(independent of this crypto level)."
            )
        spread = abs(float(buy) - float(sell))
        src_s = src or "P2P"
        tag = src_s
        if "Estimated" in (src_s or "") or "Unavailable" in (src_s or ""):
            tag = f"{src_s} — not a live order book"
        return (
            f"Separate context — USDT/NGN: Buy ₦{int(buy):,} / Sell ₦{int(sell):,} "
            f"(spread ₦{int(spread):,}, {tag}). "
            f"Local desk info only; not a cause of this crypto level reaction."
        )
    except Exception:
        return "Separate context — USDT/NGN P2P data unavailable."


def _sanitize_key_situation(text: str) -> str:
    """Drop AI prose that invents naira rates or causal P2P↔crypto links."""
    if not text:
        return ""
    import re as _re
    t = text.strip()
    try:
        t = sanitize_ai_narrative(t, fallback="")
    except Exception:
        pass
    if not t:
        return ""
    # Causal P2P/naira claims → drop entire situation
    if _re.search(
        r"(because|due to|drives|causing|leads to|as a result of).{0,60}(P2P|naira|₦|NGN)"
        r"|(P2P|naira|₦|NGN).{0,60}(because|drives|causing|leads to)",
        t,
        _re.I,
    ):
        return ""
    # Strip invented ₦ figures from situation (live rates live only in P2P block)
    t = _re.sub(r"₦\s*[0-9][0-9,]*\.?[0-9]*", "", t)
    t = _re.sub(r"\b[0-9]{3,4}\s*/\s*\$", "", t)  # e.g. 1500/$
    t = _re.sub(r"\s{2,}", " ", t).strip(" .,;")
    return t[:280]


def _testing_decision(event_label: str) -> str:
    """Useful confirmation condition — not a repeat of CURRENT STATUS."""
    lab = (event_label or "").upper()
    if "RESISTANCE" in lab:
        return (
            "Confirmation needed: 15m/1H close above the level (breakout interest) "
            "or rejection close back below (short interest only if structure agrees). "
            "No trade while still only testing."
        )
    if "SUPPORT" in lab:
        return (
            "Confirmation needed: hold/rejection back above the level (bounce interest) "
            "or 15m/1H close below (breakdown interest). "
            "No trade while still only testing."
        )
    return (
        "Confirmation needed on 15m/1H break or rejection before any trade idea. "
        "Testing alone is not a signal."
    )



def build_free_key_alert(coin, price, change, level, chat_id=None, event_label=None, conf_tier="EARLY", alert_id=None):
    """Free channel: concise level info + one-line watch tip (not full Pro analysis)."""
    status_label, status_arrow = _level_label(price, level)
    if event_label:
        status_label = event_label
    dist = _distance_to_level(price, level)
    dist_s = f"{dist:+.2f}%" if dist is not None else "n/a"
    phase = _key_alert_phase(status_label)

    if "RESISTANCE" in status_label.upper():
        watch = f"Watch: close above {format_price(level)} = breakout interest; rejection = stay cautious."
    elif "SUPPORT" in status_label.upper():
        watch = f"Watch: hold above {format_price(level)} = support; close below = breakdown risk."
    elif "BREAKOUT" in status_label.upper():
        watch = "Breakout in play — Pro channel has full scenario + any validated levels."
    else:
        watch = "Monitor this level. Pro has full scenarios and trade evaluation."

    tier = (conf_tier or "EARLY").upper()
    if tier == "CONFIRMED":
        tier_line = "🟢 <b>CONFIRMED</b> — 15m showed a real tag/reaction at this level"
        watch_note = "Confirmed level interaction — still not a trade signal until structure validates."
    else:
        tier_line = "🟡 <b>EARLY</b> — price near level; continuation not yet proven"
        watch_note = "Early watch only — many early tests reverse. Wait for confirmation."

    if alert_id:
        if tier == "CONFIRMED":
            head = f"⚡ <b>KEY ALERT #{int(alert_id)} — CONFIRMED LEVEL INTERACTION</b>"
        else:
            head = f"⚡ <b>KEY ALERT #{int(alert_id)} — EARLY WATCH</b>"
    else:
        head = f"⚡ <b>FREE KEY LEVEL — {coin}</b>"
    lines = [
        head,
        f"{coin}/USDT",
        f"{status_arrow} <b>{status_label}</b>  ·  Key Level: <b>{format_price(level)}</b>",
        tier_line,
        f"💰 Price: <b>{format_price(price)}</b>  {format_change(change)}",
        f"🎯 Level: <b>{format_price(level)}</b>  ({dist_s} away)",
        f"📌 Phase: <b>{phase}</b>",
        "",
        f"👁 {watch}",
        f"<i>{watch_note}</i>",
        "",
        "<i>Free = heads-up only. Full scenarios + trade rules → Pro channel / Pro bot.</i>",
        "<i>Key level ≠ trade signal. NFA — DYOR  ·  ⚡ Market Pulse</i>",
    ]
    if chat_id and should_show_upsell(chat_id):
        lines += [FREE_UPSELL_BLOCK]
    return "\n".join(lines)


def build_pro_key_alert(coin, price, change, level,
                        entry=None, stop=None, target=None,
                        bias="Neutral", confidence="Uncertain",
                        situation="", context_line="", decision="",
                        conf_tier="EARLY", alert_id=None):
    """Pro key alert with full Trade Hypothesis section."""
    status_label, status_arrow = _level_label(price, level)
    sd = get_secondary_coin(coin)
    high_24 = sd.get("usd_24h_high") if sd else None
    low_24  = sd.get("usd_24h_low")  if sd else None
    buy, sell, p2p_src = get_p2p_rate("USDT", "NGN")

    # Header
    tier = (conf_tier or "EARLY").upper()
    if tier == "CONFIRMED":
        tier_badge = "🟢 <b>CONFIRMED LEVEL INTERACTION</b> — 15m tagged this level with reaction"
    else:
        tier_badge = "🟡 <b>EARLY WATCH</b> — near level only; not a proven hold/break yet"

    if alert_id:
        head = f"🔔 <b>KEY ALERT #{int(alert_id)} — {coin}</b>"
    else:
        head = f"🔔 <b>PRO KEY LEVEL — {coin}</b>"
    lines = [
        head,
        f"{status_arrow} <b>{status_label}</b>  ·  Key Level: <b>{format_price(level)}</b>",
        tier_badge,
        f"💰 Price: <b>{format_price(price)}</b>  {format_change(change)}",
    ]
    if high_24 and low_24:
        lines.append(f"📊 24h Range: {format_price(low_24)} — {format_price(high_24)}")

    # Actionable scenarios (watch vs confirmed)
    lines += _scenario_block(price, level, status_label)
    if tier != "CONFIRMED":
        lines += [
            "",
            "<i>Most early tests do not hold. Treat this as a watch until 15m confirms tag + reaction, "
            "or a clear breakout/breakdown close.</i>",
        ]

    # Analysis
    lines += ["", "· · · · · · · · · · · · · · · · · · ·", ""]
    sit = _sanitize_key_situation(situation or "")
    if sit:
        lines.append(f"<b>SITUATION:</b> {sit}")
    # Always prefer live P2P context over any AI CONTEXT string
    ctx = _p2p_context_line()
    if ctx:
        lines.append(f"<b>LOCAL CONTEXT:</b> {ctx}")
    if decision:
        # Avoid "DECISION: CURRENT STATUS: ..." duplication
        dec = decision.strip()
        if dec.upper().startswith("CURRENT STATUS"):
            dec = _testing_decision(status_label)
        lines.append(f"<b>DECISION:</b> {dec}")


    # Trade Hypothesis — R:R always computed in Python (never taken from AI)
    if entry and stop and target:
        direction = _infer_direction(entry, stop, bias)
        valid, reason = _validate_alert(
            coin, price, entry, stop, target, status_label, direction=direction
        )
        if valid:
            metrics = _calc_trade_metrics(entry, stop, target, direction=direction)
            lines += [
                "",
                "· · · · · · · · · · · · · · · · · · ·",
                "",
                "📐 <b>TRADE HYPOTHESIS</b>  <i>(Illustrative only)</i>",
                f"Market Bias: <b>{bias}</b>",
                f"Direction:   <b>{direction.upper()}</b>",
                f"Entry Zone: <b>{entry}</b>",
                f"Stop Loss:  <b>{stop}</b>",
                f"Target:     <b>{target}</b>",
            ]
            if metrics:
                lines += [
                    f"Risk:Reward: <b>1 : {metrics['rr']}</b>  <i>(calculated)</i>",
                    f"Pot. Profit: <b>+${metrics['pot_profit']:,.0f} (+{metrics['reward_pct']:.2f}%)</b>  per $1,000",
                    f"Pot. Loss:   <b>-${metrics['pot_loss']:,.0f} (-{metrics['risk_pct']:.2f}%)</b>  per $1,000",
                    "<i>R:R is computed from the prices above — not from the AI narrative.</i>",
                ]
            else:
                lines.append("⚠️ Could not compute R:R from these levels.")
            lines += [
                f"Confidence: <b>{confidence}</b>",
                "",
                "Conditions: Price must confirm at this level with a candle close.",
                "Assumes normal market liquidity.",
            ]
        else:
            lines += ["", f"⚠️ Trade setup could not be validated: {reason}. Monitor manually."]
    elif not decision:
        lines += ["", "<b>DECISION:</b> Watch only — no validated trade."]

    # P2P — separate local context (not a driver of the crypto level)
    if buy and sell:
        spread = int(buy - sell)
        lines += [
            "",
            "· · · · · · · · · · · · · · · · · · ·",
            "",
            "💱 <b>NIGERIAN P2P</b> <i>(local context — not a cause of this level)</i>",
            f"Buy: <b>₦{int(buy):,}</b>   Sell: <b>₦{int(sell):,}</b>   Spread: <b>₦{spread:,}</b>",
            f"Source: {p2p_src}",
            "Recommendation: " + (
                "Reasonable to convert now — spread is tight." if spread <= 35
                else "Spread is wide — wait for it to compress unless urgent." if spread >= 50
                else "Moderate spread — convert only if needed."
            ),
        ]

    lines += [
        "",
        "· · · · · · · · · · · · · · · · · · ·",
        "",
        "<i>Illustrative example only. Not financial advice. Estimates are model-generated and not guaranteed.</i>",
        "<i>Pro-only analysis + decision. Not a signal to size up. NFA — DYOR  ·  ⚡ Market Pulse Pro</i>",
    ]
    return "\n".join(lines)

def _format_trade_price(value):
    """Format a trade level without destroying decimals (XRP/SOL-safe).

    Root cause of Pro alert 'stop 1.0 >= entry 1.0': the old parser used
    `:,.0f` which rounded $1.45 and $1.48 both to `$1`.
    """
    v = float(value)
    if v <= 0:
        return None
    if v >= 1000:
        return f"${v:,.0f}"
    if v >= 1:
        return f"${v:,.2f}"
    if v >= 0.01:
        return f"${v:,.4f}"
    return f"${v:,.6f}"


def _parse_price_token(raw):
    """Parse a numeric price token. Rejects empty/non-numeric junk."""
    if raw is None:
        return None
    s = str(raw).strip().replace("$", "").replace(",", "")
    s = s.rstrip(".")
    if not s:
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except (TypeError, ValueError):
        return None


def _parse_ai_trade(ai_text, price):
    """Extract entry, stop, target from AI response. Returns dict or None.

    Prices keep proper decimals. Values far from the live market price are
    dropped so a bad AI/parser result cannot publish collapsed $1 levels for
    an asset trading near $1.50.
    """
    import re as _re
    if not ai_text:
        return None
    try:
        e_m = _re.search(r"Entry(?:\s*(?:zone|price))?[:\s]+\$?([0-9,\.]+)", ai_text, _re.IGNORECASE)
        s_m = _re.search(r"Stop(?:\s*Loss)?[:\s]+\$?([0-9,\.]+)", ai_text, _re.IGNORECASE)
        t_m = _re.search(r"Target(?:\s*1)?[:\s]+\$?([0-9,\.]+)", ai_text, _re.IGNORECASE)
        bias_m = _re.search(r"(Bullish|Bearish|Neutral)", ai_text, _re.IGNORECASE)
        conf_m = _re.search(r"Confidence[:\s]+(High|Moderate|Low|Uncertain)", ai_text, _re.IGNORECASE)
        sit_m = _re.search(r"SITUATION[:\s]*(.+?)(?:\n|$)", ai_text, _re.IGNORECASE)
        ctx_m = _re.search(r"CONTEXT[:\s]*(.+?)(?:\n|$)", ai_text, _re.IGNORECASE)
        dec_m = _re.search(r"DECISION[:\s]*(.+?)(?:\n|$)", ai_text, _re.IGNORECASE)

        e_val = _parse_price_token(e_m.group(1)) if e_m else None
        s_val = _parse_price_token(s_m.group(1)) if s_m else None
        t_val = _parse_price_token(t_m.group(1)) if t_m else None

        ref = float(price) if price else None

        def _near_market(v):
            if v is None:
                return False
            if not ref or ref <= 0:
                return True
            return abs(v - ref) / ref <= 0.40

        if e_val is not None and not _near_market(e_val):
            e_val = None
        if s_val is not None and not _near_market(s_val):
            s_val = None
        if t_val is not None and not _near_market(t_val):
            t_val = None

        entry  = _format_trade_price(e_val) if e_val is not None else None
        stop   = _format_trade_price(s_val) if s_val is not None else None
        target = _format_trade_price(t_val) if t_val is not None else None

        return {
            "entry":   entry,
            "stop":    stop,
            "target":  target,
            "entry_raw": e_val,
            "stop_raw": s_val,
            "target_raw": t_val,
            "bias":    bias_m.group(1).capitalize() if bias_m else "Neutral",
            "confidence": conf_m.group(1).capitalize() if conf_m else "Uncertain",
            "situation": sit_m.group(1).strip() if sit_m else "",
            "context":   ctx_m.group(1).strip() if ctx_m else "",
            "decision":  dec_m.group(1).strip() if dec_m else "",
        }
    except Exception as _e:
        return None



def _get_global_key_alert_blocked():
    """True if any key alert was sent too recently (channel rate limit)."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT value, updated_at FROM admin_settings WHERE key=%s", ("key_alert_global_last",))
        row = c.fetchone()
        if not row:
            return False
        try:
            from datetime import datetime as _dt
            ts = _dt.strptime(str(row[1])[:19], "%Y-%m-%d %H:%M:%S")
            age = (wat_now() - ts).total_seconds()
            return age < KEY_ALERT_GLOBAL_MIN_SECONDS
        except Exception:
            return False
    except Exception as e:
        logger.debug("[KEY ALERT GLOBAL CD] %s", e)
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass


def _set_global_key_alert_stamp():
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            ("key_alert_global_last", "1", now),
        )
        db.commit()
    except Exception as e:
        logger.debug("[KEY ALERT GLOBAL SET] %s", e)
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass


def _coin_testing_blocked(coin, event_label):
    """Block repeat TESTING alerts for same coin within KEY_ALERT_COIN_TESTING_HOURS."""
    label = (event_label or "").upper()
    if "TESTING" not in label:
        return False
    db = None
    try:
        db = get_db()
        c = db.cursor()
        since = (wat_now() - __import__("datetime").timedelta(hours=KEY_ALERT_COIN_TESTING_HOURS)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "SELECT key FROM admin_settings WHERE key LIKE %s AND updated_at >= %s",
            (f"key_alert_{coin}|%", since),
        )
        for row in c.fetchall() or []:
            k = (row[0] or "").upper()
            if "TESTING" in k:
                return True
        return False
    except Exception:
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass


def check_key_market_alerts():
    """Event-driven key level alerts (poll often, post only when justified).

    - Does NOT post on a fixed clock — only when price is truly at a level
    - Nigerian coin priority: BTC, SOL, ETH, BNB
    - Max 1 alert per scan cycle; short global gap (anti double-post only)
    - PRIMARY: TESTING only on new zone entry; transitions on breakout/rejection
    - SECONDARY: cooldowns per coin+level+event + coin TESTING lock
    - Pro trade levels only on confirmation; R:R from code never AI
    """
    triggered = []

    try:
        for coin in KEY_ALERT_COINS:
            price, change = get_best_price(coin)
            if not price:
                continue
            levels = get_dynamic_key_levels(coin, price)
            if not levels:
                continue
            level = _nearest_key_level(price, levels)
            if not level:
                continue
            # Load prior zone/event first so RECLAIM can use last_event context
            prev_zone_probe = _load_zone_state(coin, level, "")
            prev_evt = (prev_zone_probe or {}).get("last_event") or ""
            event_label, _ = _level_label(price, level, prev_event=prev_evt)
            in_zone = _price_in_level_zone(price, level)
            # Track exits so a later re-entry can fire TESTING again
            if not in_zone:
                try:
                    _clear_zone_if_left(coin, level, price, event_label)
                except Exception:
                    pass
                continue  # only act when near a level

            # PRIMARY: zone entry / state transition (restart-safe via admin_settings)
            # TESTING uses per-coin cluster so ETH $2505 and $2509 share state
            prev_zone = _load_zone_state(coin, level, event_label)
            # Prefer richer previous event for gate if cluster state is empty
            if not (prev_zone or {}).get("last_event") and prev_evt:
                prev_zone = dict(prev_zone_probe or {})
            allow, reason = _zone_gate_allow(prev_zone, in_zone, event_label, level)
            if not allow:
                logger.debug(
                    "[KEY ALERT] %s %s @ %s zone-gate skip (%s)",
                    coin, event_label, level, reason,
                )
                continue

            # SECONDARY: existing cooldowns (safety net, not primary dedup)
            if _get_key_alert_cooldown(coin, level, event_label):
                logger.debug(f"[KEY ALERT] {coin} {event_label} @ {level} in {KEY_ALERT_COOLDOWN_HOURS}h cooldown, skipping")
                continue
            if _coin_testing_blocked(coin, event_label):
                logger.debug(f"[KEY ALERT] {coin} TESTING coin-level cooldown, skipping")
                continue
            if not _meaningful_move_away(coin, price, level, event_label):
                logger.debug(f"[KEY ALERT] {coin} still near last-alert price — not a new event")
                continue
            # EARLY vs CONFIRMED — do not label a spike as a sustained confirmed event
            tier = classify_key_alert_tier(coin, price, level, event_label)
            if tier == "SKIP":
                logger.info(
                    "[KEY ALERT] %s %s skip — no early/confirmed structure yet",
                    coin, event_label,
                )
                continue
            proximity = abs(price - level) / level
            # Prefer confirmed over early when sorting (lower proximity first, then confirmed)
            rank = 0 if tier == "CONFIRMED" else 1
            triggered.append((rank, proximity, coin, price, change or 0, level, event_label, tier))

        # Closest to level first; if similar, prefer Nigerian-priority coins (BTC > SOL > ETH > BNB)
        triggered.sort(key=lambda x: (x[0], x[1], KEY_ALERT_PRIORITY.get(x[2], 99)))

        sent = 0
        for _rank, proximity, coin, price, ch, level, event_label, conf_tier in triggered:
            if sent >= MAX_ALERTS_PER_CYCLE:
                break
            if _get_global_key_alert_blocked():
                logger.info("[KEY ALERT] Global rate limit — skip this cycle (min gap between posts)")
                break
            # Quality gate: only channel-post after 15m structure + short RT hold
            if conf_tier != "CONFIRMED":
                logger.info(
                    "[KEY ALERT] %s %s tier=%s — no channel post (EARLY/SKIP)",
                    coin, event_label, conf_tier,
                )
                continue

            rt = realtime_follow_through(coin, level, event_label, price)
            if not rt.get("ok"):
                logger.info(
                    "[KEY ALERT] %s %s RT CANCEL (%s) — no channel post",
                    coin, event_label, rt.get("reason"),
                )
                continue

            pub_price = rt.get("confirm_price") or price
            # Cooldown only after RT pass (failed RT must not block a later real hold)
            _set_key_alert_cooldown(coin, level, event_label, price=pub_price)
            _set_global_key_alert_stamp()
            try:
                _save_zone_state(coin, level, True, event_label, price=pub_price)
            except Exception as _zs:
                logger.debug("[KEY ZONE] save on post: %s", _zs)
            logger.info(
                "[KEY ALERT] %s @ %s — %s [CONFIRMED+RT %.1fs %s]",
                coin, format_price(pub_price), event_label,
                float(rt.get("delay_sec") or 0), rt.get("reason"),
            )
            alert_id = 0
            try:
                from market_pulse.outcome_monitor import register_key_level_watch
                alert_id = int(register_key_level_watch(coin, level, event_label, hours_valid=12) or 0)
            except Exception as _kw:
                logger.debug("[KEY WATCH REG] %s", _kw)
                alert_id = 0
            if alert_id:
                logger.info("[KEY ALERT] published id=#%s %s %s", alert_id, coin, event_label)

            post_to_channel(build_free_key_alert(
                coin, pub_price, ch, level, event_label=event_label, conf_tier="CONFIRMED",
                alert_id=alert_id or None,
            ))

                        # Pro channel — TESTING = watch only; confirmation may evaluate trade
            sd = get_secondary_coin(coin)
            high_24 = sd.get("usd_24h_high") if sd else None
            low_24  = sd.get("usd_24h_low")  if sd else None
            fg_data = get_fear_greed()
            fg_val  = fg_data[0]["value"] if fg_data else "N/A"
            h_str = format_price(high_24) if isinstance(high_24, (int, float)) else "N/A"
            l_str = format_price(low_24) if isinstance(low_24, (int, float)) else "N/A"
            status_label = event_label or _level_label(price, level)[0]
            phase = _key_alert_phase(status_label)

            situation = ""
            context_line = _p2p_context_line()  # live quote only — not AI rates
            try:
                narr_prompt = (
                    f"{coin} at {format_price(price)} ({format_change(ch)}). "
                    f"Phase: {phase}. Status: {status_label} at key level {format_price(level)}. "
                    f"24h High {h_str} Low {l_str}. F&G {fg_val}/100. "
                    f"Respond EXACT format, no asterisks:\n"
                    f"SITUATION: [one sentence ONLY about price vs this crypto key level — "
                    f"no naira, no P2P, no exchange rates, no claim that P2P causes the move]\n"
                    f"Do NOT invent Entry, Stop, Target, R:R, or any Nigerian rate."
                )
                ai_raw, _ = ask_ai(narr_prompt)
                if ai_raw:
                    import re as _re
                    sit_m = _re.search(r"SITUATION[:\s]*(.+?)(?:\n|$)", ai_raw, _re.IGNORECASE)
                    situation = _sanitize_key_situation(sit_m.group(1).strip() if sit_m else "")
            except Exception as _ne:
                logger.debug("[KEY ALERT] narrative skip: %s", _ne)

            entry = stop = target = None
            bias = "Neutral"
            confidence = "Uncertain"
            decision = "Watch only — key level is information, not a trade signal."

            # Only evaluate a trade candidate after breakout/breakdown-style confirmation
            if phase in ("CONFIRMED_BREAKOUT", "CONFIRMED_BREAKDOWN"):
                setup = None
                try:
                    from market_pulse.setup_engine import build_programmatic_setup
                    # NORMAL first; AGGRESSIVE only if NORMAL has no edge
                    setup = build_programmatic_setup(coin, price, tier="momentum")
                    if not setup:
                        setup = build_programmatic_setup(coin, price, tier="edge")
                except Exception as _se:
                    logger.warning("[KEY ALERT] setup_engine: %s", _se)
                    setup = None

                if setup and setup.get("entry") and setup.get("stop") and setup.get("target1"):
                    entry = setup["entry"]
                    stop = setup["stop"]
                    target = setup["target1"]
                    bias = setup.get("bias", "Neutral")
                    confidence = setup.get("confidence", "Moderate")
                    decision = (
                        f"Confirmed structure candidate ({setup.get('display_tier', 'NORMAL')}). "
                        f"Levels from setup engine — not AI-invented. "
                        f"Horizon: {setup.get('expected_horizon', 'see timeframe')}."
                    )
                else:
                    decision = (
                        "No valid trade setup. Level confirmation detected, "
                        "but risk/structure parameters did not pass programmatic validation. "
                        "Monitor manually."
                    )
            else:
                decision = ("EARLY: price is near the level only — no sustained hold/break proven yet. Wait for 15m tag + reaction or a confirmed close through the level." if conf_tier == "EARLY" else _testing_decision(status_label))

            post_to_pro_channel(build_pro_key_alert(
                coin, pub_price, ch, level,
                entry=entry,
                stop=stop,
                target=target,
                bias=bias,
                confidence=confidence,
                situation=situation,
                context_line=context_line,
                decision=decision,
                conf_tier=conf_tier,
                alert_id=alert_id or None,
            ))
            sent += 1
            time.sleep(1)  # brief pause between alerts

    except Exception as e:
        logger.error(f"[KEY ALERT ERROR] {e}")

def daily_digest():
    db = None
    try:
        db = get_db()
        c = db.cursor()
        today_wat = wat_now().strftime("%Y-%m-%d")
        c.execute("SELECT COUNT(*) FROM events WHERE timestamp LIKE %s", (today_wat + "%",))
        total_events = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE first_seen LIKE %s", (today_wat + "%",))
        new_users = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users WHERE last_seen LIKE %s", (today_wat + "%",))
        active_users = c.fetchone()[0]
    except Exception as e:
        logger.error("[DAILY DIGEST ERROR] %s" % e)
        return
    finally:
        if db:
            try: db.close()
            except Exception: pass

    for admin_id in ADMIN_IDS:
        try:
            send(admin_id, (
                f"📊 <b>Daily Digest</b>\n\n"
                f"📅 {today_wat} (WAT)\n"
                f"👤 New Users: <b>{new_users}</b>\n"
                f"🟢 Active Users: <b>{active_users}</b>\n"
                f"📊 Total Events: <b>{total_events}</b>"
            ))
        except Exception as e:
            logger.error("[DAILY DIGEST SEND] admin %s: %s" % (admin_id, e))

# ═══════════════════════════════════════════════════════════════════════════
