"""Market Pulse — automated trade scanner (discovery → qualify → rank → publish).

Does NOT change Entry/SL/TP/ATR strategy math. Those live in setup_engine /
edge_trade_engine. This module only controls how opportunities are found,
recorded, ranked, and published.
"""

from __future__ import annotations

import os
import random
import time
from datetime import timedelta

from market_pulse.config_runtime import logger
from market_pulse.db import get_db
from market_pulse.alerts import _calc_trade_metrics
from market_pulse.edge_trade_engine import (
    _gather_trade_analytics,
    _tier_conditions_met,
    generate_trade_idea,
    mark_trade_publication,
)
from market_pulse.fear_greed import get_fear_greed
from market_pulse.forex_trade_engine import generate_forex_trade_idea, get_forex_rate
from market_pulse.helpers import wat_now
from market_pulse.price_fetchers import get_best_price
from market_pulse.telegram_api import post_to_pro_channel
from market_pulse.trade_engine_report import (
    finish_scan_run,
    record_candidate,
    start_scan_run,
)
from market_pulse.macro_event_scanner import apply_macro_publication_gate

# ── Markets (USDT/NGN is context-only — not listed here) ─────────────────────
SCANNER_CRYPTO_COINS = ["BTC", "ETH", "SOL", "BNB", "XRP", "AVAX", "LINK", "DOGE"]
SCANNER_FOREX_PAIRS = ["USD/NGN", "BTC/NGN", "EUR/USD", "GBP/USD"]
SCANNER_TIER_ORDER = ["steady", "momentum", "edge"]

# ── Configurable publish policy (env) ───────────────────────────────────────
def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


# Discovery is never stopped by these; they only limit how many ranked setups post.
MAX_TRADES_PER_SCAN = _env_int("MAX_TRADES_PER_SCAN", 2)
MAX_TRADES_PER_DAY = _env_int("MAX_TRADES_PER_DAY", 5)
# Seconds between scan *starts* (default 1H to align with 1H structure; was 14400).
TRADE_SCAN_INTERVAL_SEC = _env_int("TRADE_SCAN_INTERVAL_SEC", 3600)

# Major-crypto correlation groups (simple deterministic exposure)
_MAJOR_CRYPTO = frozenset({"BTC", "ETH", "SOL", "BNB", "AVAX", "LINK", "DOGE", "XRP"})

_scanner_daily_count = {"date": "", "count": 0}


def _map_reject_reason(reason: str | None) -> str:
    rs = str(reason or "").lower()
    if not rs:
        return "REJECTED"
    if "candle" in rs or "data" in rs or "price" in rs and "unavail" in rs:
        return "INSUFFICIENT_CANDLES"
    if "price" in rs and ("none" in rs or "unavailable" in rs or "no " in rs):
        return "PRICE_UNAVAILABLE"
    if "f&g" in rs or "fear" in rs or "greed" in rs:
        return "FEAR_GREED_FAILED"
    if "trend" in rs or "ema" in rs or "ma" in rs:
        return "TREND_FAILED"
    if "structure" in rs or "level" in rs:
        return "STRUCTURE_FAILED"
    if "vol" in rs:
        return "VOLATILITY_FAILED"
    if "news" in rs or "blackout" in rs:
        return "NEWS_BLACKOUT"
    if "dead range" in rs or "rsi" in rs:
        return "STRUCTURE_FAILED" if "dead" in rs else "TREND_FAILED"
    return "REJECTED"


def _scanner_get_cooldown():
    """True if last scan start is still inside TRADE_SCAN_INTERVAL_SEC."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT updated_at FROM admin_settings WHERE key='auto_scanner_last'")
        row = c.fetchone()
        if not row or not row[0]:
            return False
        last = str(row[0])[:19]
        from datetime import datetime
        try:
            last_dt = datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return False
        now = wat_now()
        try:
            delta = (now.replace(tzinfo=None) - last_dt).total_seconds()
        except Exception:
            delta = (now - last_dt).total_seconds()
        return delta < TRADE_SCAN_INTERVAL_SEC
    except Exception as e:
        logger.warning(f"[SCANNER CD] {e}")
        return False
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _scanner_try_acquire():
    """Multi-worker lock for one scan window (interval = TRADE_SCAN_INTERVAL_SEC)."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now()
        since = (now - timedelta(seconds=TRADE_SCAN_INTERVAL_SEC)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "SELECT updated_at FROM admin_settings WHERE key='auto_scanner_last' AND updated_at >= %s",
            (since,),
        )
        if c.fetchone():
            return False
        stamp = now.strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES ('auto_scanner_last', %s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at "
            "WHERE admin_settings.updated_at < %s",
            (stamp, stamp, since),
        )
        db.commit()
        c.execute("SELECT value, updated_at FROM admin_settings WHERE key='auto_scanner_last'")
        row = c.fetchone()
        if not row:
            return False
        return str(row[1]) >= since and str(row[0]) == stamp
    except Exception as e:
        logger.warning(f"[SCANNER LOCK] {e}")
        try:
            if db:
                db.rollback()
        except Exception:
            pass
        return True
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _scanner_set_cooldown():
    """Stamp last scan time (also used as publish-side refresh)."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES ('auto_scanner_last',%s,%s) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (now, now),
        )
        db.commit()
    except Exception as e:
        logger.warning(f"[SCANNER CD SET] {e}")
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


def _daily_published_count() -> int:
    """Durable daily publish count from trade_scan_candidates (fallback memory)."""
    global _scanner_daily_count
    today = wat_now().strftime("%Y-%m-%d")
    if _scanner_daily_count["date"] != today:
        _scanner_daily_count = {"date": today, "count": 0}
    db = None
    try:
        db = get_db()
        c = db.cursor()
        start, end = f"{today} 00:00:00", f"{today} 23:59:59"
        c.execute(
            """
            SELECT COUNT(*) FROM trade_scan_candidates
            WHERE status='PUBLISHED' AND created_at >= %s AND created_at <= %s
            """,
            (start, end),
        )
        n = int((c.fetchone() or [0])[0] or 0)
        _scanner_daily_count["count"] = n
        return n
    except Exception:
        return int(_scanner_daily_count.get("count") or 0)
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _correlation_group(symbol: str, direction: str) -> str:
    sym = (symbol or "").upper().split("/")[0]
    d = (direction or "long").lower()
    side = "long" if d.startswith("long") or d == "buy" else "short"
    if sym in _MAJOR_CRYPTO:
        return f"major_crypto_{side}"
    if "/" in (symbol or ""):
        return f"forex_{symbol}_{side}"
    return f"{sym}_{side}"


def _rank_score(trade: dict, tier: str) -> float:
    """Signal-time only score. No future candles / outcomes."""
    score = 0.0
    conf = str((trade or {}).get("confidence") or "Moderate")
    score += {"High": 30.0, "Moderate": 15.0, "Low": 5.0}.get(conf, 10.0)
    tier_l = (tier or "momentum").lower()
    score += {"steady": 18.0, "safe": 18.0, "momentum": 12.0, "normal": 12.0, "edge": 8.0, "aggressive": 8.0}.get(
        tier_l, 10.0
    )
    try:
        m = _calc_trade_metrics(
            str(trade.get("entry", "")),
            str(trade.get("stop", "")),
            str(trade.get("target1", "")),
        )
        if m and m.get("rr"):
            score += min(float(m["rr"]), 5.0) * 10.0
    except Exception:
        pass
    return score


def run_trade_scanner():
    """
    Full funnel each cycle:

      DISCOVER all markets × tiers
      → RECORD pass/fail
      → GENERATE/QUALIFY setups for pre-screen passers
      → RANK
      → CORRELATION suppress
      → PUBLISH up to caps (never abort discovery early)
    """
    global _scanner_daily_count

    today = wat_now().strftime("%Y-%m-%d")
    if _scanner_daily_count["date"] != today:
        _scanner_daily_count = {"date": today, "count": 0}

    # Interval lock — does not skip discovery logic when we do run
    if _scanner_get_cooldown() or not _scanner_try_acquire():
        logger.info(
            "[SCANNER] Interval lock active (every %ss) — skipping this tick",
            TRADE_SCAN_INTERVAL_SEC,
        )
        return

    logger.info(
        "[SCANNER] Starting full scan | max_per_scan=%s max_per_day=%s interval=%ss",
        MAX_TRADES_PER_SCAN,
        MAX_TRADES_PER_DAY,
        TRADE_SCAN_INTERVAL_SEC,
    )
    scan_run_id = start_scan_run()
    markets_touched = 0
    scan_errors = 0

    fg_data = get_fear_greed()
    fg_val = fg_data[0]["value"] if fg_data else "50"

    # ── Phase A: DISCOVER + pre-screen (all markets × tiers) ─────────────
    prequalified = []  # list of (asset_type, identifier, tier)

    for tier in SCANNER_TIER_ORDER:
        for coin in SCANNER_CRYPTO_COINS:
            markets_touched += 1
            try:
                price, _ = get_best_price(coin)
                if not price:
                    record_candidate(
                        scan_run_id, coin, tier, "REJECTED",
                        rejection_reason="PRICE_UNAVAILABLE",
                    )
                    continue
                analytics = _gather_trade_analytics(coin, price)
                ok, reason = _tier_conditions_met(tier, analytics, fg_val)
                if ok:
                    prequalified.append(("crypto", coin, tier))
                    record_candidate(scan_run_id, coin, tier, "QUALIFIED")
                    logger.info("[SCANNER] %s %s pre-screen OK (%s)", coin, tier, reason)
                else:
                    code = _map_reject_reason(reason)
                    record_candidate(
                        scan_run_id, coin, tier, "REJECTED",
                        rejection_reason=code,
                    )
                    logger.debug("[SCANNER] %s %s rejected: %s", coin, tier, reason)
            except Exception as e:
                scan_errors += 1
                record_candidate(
                    scan_run_id, coin, tier, "REJECTED",
                    rejection_reason="GENERATION_ERROR",
                )
                logger.warning("[SCANNER] %s %s error: %s", coin, tier, e)

    for tier in SCANNER_TIER_ORDER:
        for pair_key in SCANNER_FOREX_PAIRS:
            markets_touched += 1
            try:
                rate, _, _, _ = get_forex_rate(pair_key)
                if not rate:
                    record_candidate(
                        scan_run_id, pair_key, tier, "REJECTED",
                        rejection_reason="PRICE_UNAVAILABLE",
                    )
                    continue
                fg = int(fg_val) if str(fg_val).isdigit() else 50
                if tier == "edge" and not (fg > 70 or fg < 30):
                    record_candidate(
                        scan_run_id, pair_key, tier, "REJECTED",
                        rejection_reason="FEAR_GREED_FAILED",
                    )
                    continue
                if tier == "steady" and (fg >= 80 or fg <= 15):
                    record_candidate(
                        scan_run_id, pair_key, tier, "REJECTED",
                        rejection_reason="FEAR_GREED_FAILED",
                    )
                    continue
                prequalified.append(("forex", pair_key, tier))
                record_candidate(scan_run_id, pair_key, tier, "QUALIFIED")
                logger.info("[SCANNER] %s %s pre-screen OK", pair_key, tier)
            except Exception as e:
                scan_errors += 1
                record_candidate(
                    scan_run_id, pair_key, tier, "REJECTED",
                    rejection_reason="GENERATION_ERROR",
                )
                logger.warning("[SCANNER] %s %s error: %s", pair_key, tier, e)

    logger.info(
        "[SCANNER] Discovery done — %s prequalified of %s market×tier checks",
        len(prequalified),
        markets_touched,
    )

    # ── Phase B: GENERATE full setups for every prequalified candidate ───
    ranked = []  # dicts with score, trade, msg, idea_id, ...

    for asset_type, identifier, tier in prequalified:
        try:
            if asset_type == "crypto":
                msg, trade, idea_id = generate_trade_idea(identifier, tier)
            else:
                msg, trade, idea_id = generate_forex_trade_idea(identifier, tier)

            if not msg or not idea_id or not trade:
                record_candidate(
                    scan_run_id, identifier, tier, "NO_SETUP",
                    rejection_reason="NO_SETUP",
                )
                continue

            direction = str((trade or {}).get("direction") or "long")
            score = _rank_score(trade, tier)
            ranked.append(
                {
                    "asset_type": asset_type,
                    "identifier": identifier,
                    "tier": tier,
                    "direction": direction,
                    "score": score,
                    "msg": msg,
                    "trade": trade,
                    "idea_id": int(idea_id),
                }
            )
            # Keep QUALIFIED until publish decision; update score on row via new record
            record_candidate(
                scan_run_id,
                identifier,
                tier,
                "QUALIFIED",
                direction=direction,
                idea_id=int(idea_id),
                score=score,
            )
            logger.info(
                "[SCANNER] Setup ready %s %s dir=%s score=%.1f id=#%s",
                identifier, tier, direction, score, idea_id,
            )
        except Exception as e:
            scan_errors += 1
            record_candidate(
                scan_run_id, identifier, tier, "REJECTED",
                rejection_reason="GENERATION_ERROR",
            )
            logger.error("[SCANNER] generate %s %s: %s", identifier, tier, e)

    # ── Phase C: RANK best → worst ───────────────────────────────────────
    ranked.sort(key=lambda x: float(x.get("score") or 0), reverse=True)

    # ── Phase C2: MACRO CHECK (gate only; shadow mode does not suppress) ─
    still_ranked = []
    for item in ranked:
        try:
            apply_macro_publication_gate(item)
        except Exception as e:
            logger.warning("[SCANNER] macro gate error: %s", e)
            item["macro_state"] = "ELEVATED"
            item["macro_would_block"] = False
            item["macro_enforce_block"] = False
            item["macro_event_name"] = "MACRO_EVAL_ERROR"
        logger.info(
            "[MACRO] idea=#%s %s state=%s would_block=%s shadow=%s event=%s",
            item.get("idea_id"),
            item.get("identifier"),
            item.get("macro_state"),
            item.get("macro_would_block"),
            item.get("macro_shadow_mode"),
            item.get("macro_event_name"),
        )
        if item.get("macro_enforce_block"):
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "SUPPRESSED",
                rejection_reason="MACRO_BLOCK",
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=item.get("score"),
            )
            mark_trade_publication(item.get("idea_id"), "SUPPRESSED", "MACRO_BLOCK")
            logger.info(
                "[SCANNER] MACRO_BLOCK suppressed #%s %s (%s)",
                item.get("idea_id"),
                item.get("identifier"),
                item.get("macro_event_name"),
            )
            continue
        still_ranked.append(item)
    ranked = still_ranked

    # ── Phase D: CORRELATION + CAPS → PUBLISH or SUPPRESS ────────────────
    already_today = _daily_published_count()
    room_day = max(0, MAX_TRADES_PER_DAY - already_today)
    room_scan = max(0, MAX_TRADES_PER_SCAN)
    budget = min(room_day, room_scan)

    used_groups = set()
    published = 0

    if budget <= 0:
        logger.info(
            "[SCANNER] Publish budget 0 (day=%s/%s) — all qualified suppressed",
            already_today,
            MAX_TRADES_PER_DAY,
        )
        for item in ranked:
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "SUPPRESSED",
                rejection_reason="PUBLISH_LIMIT",
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=item.get("score"),
            )
            mark_trade_publication(item.get("idea_id"), "SUPPRESSED", "PUBLISH_LIMIT")
        finish_scan_run(scan_run_id, markets_scanned=markets_touched, error_count=scan_errors)
        return

    for item in ranked:
        group = _correlation_group(item["identifier"], item.get("direction") or "long")
        if group in used_groups:
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "SUPPRESSED",
                rejection_reason="CORRELATED_SUPPRESSED",
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=item.get("score"),
            )
            mark_trade_publication(item.get("idea_id"), "SUPPRESSED", "CORRELATED_SUPPRESSED")
            logger.info(
                "[SCANNER] SUPPRESSED %s %s — correlated with stronger setup (%s)",
                item["identifier"], item["tier"], group,
            )
            continue

        if published >= budget:
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "SUPPRESSED",
                rejection_reason="PUBLISH_LIMIT",
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=item.get("score"),
            )
            mark_trade_publication(item.get("idea_id"), "SUPPRESSED", "PUBLISH_LIMIT")
            continue

        try:
            post_to_pro_channel(item["msg"])
            used_groups.add(group)
            published += 1
            _scanner_daily_count["count"] = already_today + published
            mark_trade_publication(item.get("idea_id"), "PUBLISHED", "PRO_CHANNEL")
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "PUBLISHED",
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=item.get("score"),
            )
            logger.info(
                "[SCANNER] PUBLISHED #%s %s %s score=%.1f (%s/%s this scan)",
                item["idea_id"],
                item["identifier"],
                item["tier"],
                item["score"],
                published,
                budget,
            )
        except Exception as e:
            scan_errors += 1
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "REJECTED",
                rejection_reason="TELEGRAM_ERROR",
                idea_id=item.get("idea_id"),
            )
            mark_trade_publication(item.get("idea_id"), "PUBLISH_FAILED", "TELEGRAM_ERROR")
            logger.error("[SCANNER] Telegram publish failed: %s", e)

    if published:
        _scanner_set_cooldown()

    finish_scan_run(scan_run_id, markets_scanned=markets_touched, error_count=scan_errors)
    logger.info(
        "[SCANNER] Complete — prequalified=%s setups=%s published=%s errors=%s",
        len(prequalified),
        len(ranked),
        published,
        scan_errors,
    )


def get_trade_scan_interval_sec() -> int:
    """Used by handlers scheduler."""
    return TRADE_SCAN_INTERVAL_SEC
