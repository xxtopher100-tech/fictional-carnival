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
from market_pulse.message_integrity import classify_vs_active_open
from market_pulse.blueprint_v31 import (
    assess_crypto_price_quality,
    final_price_check,
    STATUS_SKIPPED_DATA_QUALITY,
    STATUS_EXPIRED_BEFORE_PUBLISH,
    BLUEPRINT_VERSION,
)
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
SCANNER_FOREX_PAIRS = ["EUR/USD", "GBP/USD"]  # no NGN trade pairs — P2P/rates stay elsewhere
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
# Hard ceiling even when a later setup is better (anti-spam)
MAX_TRADES_PER_DAY_HARD = _env_int("MAX_TRADES_PER_DAY_HARD", 8)
# Forex is secondary product — do not let SAFE FX fill the whole scan budget
MAX_FOREX_PER_SCAN = _env_int("MAX_FOREX_PER_SCAN", 1)
# Seconds between scan *starts* (default 1H to align with 1H structure; was 14400).
TRADE_SCAN_INTERVAL_SEC = _env_int("TRADE_SCAN_INTERVAL_SEC", 3600)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except Exception:
        return default


# Soft daily cap: if day is "full", still allow a post when score beats weakest
# published today by at least this margin.
SCORE_REPLACE_MARGIN = _env_float("SCORE_REPLACE_MARGIN", 12.0)
# Quality floor — weak setups should not burn a seat
MIN_PUBLISH_SCORE = _env_float("MIN_PUBLISH_SCORE", 40.0)

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



def _weakest_published_score_today() -> float | None:
    """Lowest rank-score among PUBLISHED candidates today (None if none)."""
    today = wat_now().strftime("%Y-%m-%d")
    db = None
    try:
        db = get_db()
        c = db.cursor()
        start, end = f"{today} 00:00:00", f"{today} 23:59:59"
        c.execute(
            """
            SELECT MIN(score) FROM trade_scan_candidates
            WHERE status='PUBLISHED' AND created_at >= %s AND created_at <= %s
              AND score IS NOT NULL
            """,
            (start, end),
        )
        row = c.fetchone()
        if not row or row[0] is None:
            return None
        return float(row[0])
    except Exception as e:
        logger.debug("[SCANNER] weakest score today: %s", e)
        return None
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _may_publish_over_soft_cap(candidate_score: float, already_today: int) -> tuple[bool, str]:
    """When soft daily cap is full, allow a clearly better setup up to HARD max.

    Returns (allowed, reason_code).
    """
    score = float(candidate_score or 0)
    if already_today < MAX_TRADES_PER_DAY:
        return True, "UNDER_SOFT_CAP"
    if already_today >= MAX_TRADES_PER_DAY_HARD:
        return False, "HARD_DAY_CAP"
    weakest = _weakest_published_score_today()
    if weakest is None:
        # Cap full but no scores stored — allow only high-confidence overflow
        if score >= MIN_PUBLISH_SCORE + SCORE_REPLACE_MARGIN:
            return True, "OVERFLOW_NO_BASELINE"
        return False, "SOFT_CAP_NO_BASELINE"
    if score >= weakest + SCORE_REPLACE_MARGIN:
        return True, "OVERFLOW_BETTER_THAN_WEAKEST"
    return False, "SOFT_CAP_NOT_BETTER"


def _correlation_group(symbol: str, direction: str) -> str:
    sym = (symbol or "").upper().split("/")[0]
    d = (direction or "long").lower()
    side = "long" if d.startswith("long") or d == "buy" else "short"
    if sym in _MAJOR_CRYPTO:
        return f"major_crypto_{side}"
    if "/" in (symbol or ""):
        return f"forex_{symbol}_{side}"
    return f"{sym}_{side}"


def _rank_score(trade: dict, tier: str, asset_type: str = "crypto") -> float:
    """Signal-time only score. No future candles / outcomes.

    Crypto is the primary MarketPulse product; forex is secondary context.
    A small crypto bias prevents SAFE FX from consuming the entire publish budget.
    Does NOT change Entry/SL/TP math.
    """
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
    # Product priority (not strategy quality): crypto > forex at publish time
    if (asset_type or "").lower() == "crypto":
        score += 25.0
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
    logger.info("[SCANNER] Blueprint v%s scan_run=%s", BLUEPRINT_VERSION, scan_run_id)
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
                dq, dq_reason = assess_crypto_price_quality(coin, price)
                if dq == "BLOCKED":
                    record_candidate(
                        scan_run_id, coin, tier, "REJECTED",
                        rejection_reason=STATUS_SKIPPED_DATA_QUALITY,
                    )
                    logger.info("[SCANNER] %s %s SKIPPED_DATA_QUALITY (%s)", coin, tier, dq_reason)
                    continue
                if dq == "DEGRADED":
                    logger.debug("[SCANNER] %s data degraded: %s", coin, dq_reason)
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
            score = _rank_score(trade, tier, asset_type=asset_type)
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
    # Soft daily cap (MAX_TRADES_PER_DAY) + hard ceiling (MAX_TRADES_PER_DAY_HARD).
    # If the soft cap is full, a clearly better setup may still post (overflow).
    already_today = _daily_published_count()
    room_scan = max(0, MAX_TRADES_PER_SCAN)
    weakest_today = _weakest_published_score_today()

    used_groups = set()
    published = 0
    forex_published = 0
    overflow_posts = 0

    logger.info(
        "[SCANNER] Publish policy soft_day=%s/%s hard=%s scan_cap=%s min_score=%.1f "
        "replace_margin=%.1f weakest_today=%s",
        already_today,
        MAX_TRADES_PER_DAY,
        MAX_TRADES_PER_DAY_HARD,
        room_scan,
        MIN_PUBLISH_SCORE,
        SCORE_REPLACE_MARGIN,
        weakest_today,
    )

    for item in ranked:
        group = _correlation_group(item["identifier"], item.get("direction") or "long")
        score = float(item.get("score") or 0)

        if group in used_groups:
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "SUPPRESSED",
                rejection_reason="CORRELATED_SUPPRESSED",
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=score,
            )
            mark_trade_publication(item.get("idea_id"), "SUPPRESSED", "CORRELATED_SUPPRESSED")
            logger.info(
                "[SCANNER] SUPPRESSED %s %s — correlated with stronger setup (%s)",
                item["identifier"], item["tier"], group,
            )
            continue

        # Quality floor — do not burn seats on weak setups
        if score < MIN_PUBLISH_SCORE:
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "SUPPRESSED",
                rejection_reason="BELOW_MIN_SCORE",
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=score,
            )
            mark_trade_publication(item.get("idea_id"), "SUPPRESSED", "BELOW_MIN_SCORE")
            logger.info(
                "[SCANNER] SUPPRESSED %s %s score=%.1f < min %.1f",
                item["identifier"], item["tier"], score, MIN_PUBLISH_SCORE,
            )
            continue

        # Cap forex so secondary FX cannot fill every Pro slot
        if (item.get("asset_type") or "") == "forex" and forex_published >= MAX_FOREX_PER_SCAN:
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "SUPPRESSED",
                rejection_reason="FOREX_CAP",
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=score,
            )
            mark_trade_publication(item.get("idea_id"), "SUPPRESSED", "FOREX_CAP")
            logger.info(
                "[SCANNER] FOREX_CAP suppressed %s %s (max %s/scan)",
                item["identifier"], item["tier"], MAX_FOREX_PER_SCAN,
            )
            continue

        # Per-scan seat limit (always enforced)
        if published >= room_scan:
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "SUPPRESSED",
                rejection_reason="SCAN_CAP",
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=score,
            )
            mark_trade_publication(item.get("idea_id"), "SUPPRESSED", "SCAN_CAP")
            continue

        # Soft daily cap with overflow for clearly better setups
        projected_today = already_today + published
        ok_day, day_reason = _may_publish_over_soft_cap(score, projected_today)
        if not ok_day:
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "SUPPRESSED",
                rejection_reason=day_reason,
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=score,
            )
            mark_trade_publication(item.get("idea_id"), "SUPPRESSED", day_reason)
            logger.info(
                "[SCANNER] SUPPRESSED %s %s score=%.1f — %s (day %s soft=%s hard=%s)",
                item["identifier"],
                item["tier"],
                score,
                day_reason,
                projected_today,
                MAX_TRADES_PER_DAY,
                MAX_TRADES_PER_DAY_HARD,
            )
            continue


        # Active thesis / similar open setup suppression
        try:
            entry_v = None
            tr = item.get("trade") or {}
            if tr.get("entry") is not None:
                try:
                    entry_v = float(tr.get("entry"))
                except Exception:
                    entry_v = None
            tf = (tr.get("timeframe") if isinstance(tr, dict) else None) or item.get("timeframe") or ""
            cls, exist_id = classify_vs_active_open(
                item.get("identifier") or item.get("coin") or "",
                item.get("direction") or (tr.get("direction") if isinstance(tr, dict) else "") or "",
                tf,
                entry_v or 0.0,
            )
            if cls == "SIMILAR_ACTIVE_SETUP" and exist_id and exist_id != item.get("idea_id"):
                record_candidate(
                    scan_run_id,
                    item["identifier"],
                    item["tier"],
                    "SUPPRESSED",
                    rejection_reason=f"SIMILAR_ACTIVE_SETUP:{exist_id}",
                    direction=item.get("direction"),
                    idea_id=item.get("idea_id"),
                    score=score,
                )
                mark_trade_publication(
                    item.get("idea_id"), "SUPPRESSED", f"SIMILAR_ACTIVE_SETUP:{exist_id}"
                )
                logger.info(
                    "[SCANNER] SUPPRESSED %s %s — similar to open #%s",
                    item["identifier"], item["tier"], exist_id,
                )
                continue
        except Exception as _sim_e:
            logger.debug("[SCANNER] similar check: %s", _sim_e)

        is_overflow = projected_today >= MAX_TRADES_PER_DAY

        # v3.1 final price check — expire before publish if entry already ran away
        try:
            tr = item.get("trade") or {}
            ent = tr.get("entry")
            try:
                ent_f = float(ent) if ent is not None else 0.0
            except Exception:
                ent_f = 0.0
            ident = item.get("identifier") or ""
            if item.get("asset_type") == "crypto" and ent_f > 0:
                ok_px, px_reason, _live = final_price_check(
                    ident, ent_f, item.get("direction") or tr.get("direction") or ""
                )
                if not ok_px:
                    record_candidate(
                        scan_run_id,
                        item["identifier"],
                        item["tier"],
                        "SUPPRESSED",
                        rejection_reason=STATUS_EXPIRED_BEFORE_PUBLISH,
                        direction=item.get("direction"),
                        idea_id=item.get("idea_id"),
                        score=score,
                    )
                    mark_trade_publication(
                        item.get("idea_id"), "SUPPRESSED", STATUS_EXPIRED_BEFORE_PUBLISH
                    )
                    logger.info(
                        "[SCANNER] EXPIRED_BEFORE_PUBLISH %s %s — %s",
                        item["identifier"], item["tier"], px_reason,
                    )
                    continue
        except Exception as _fpc:
            logger.debug("[SCANNER] final price check: %s", _fpc)

        try:
            post_to_pro_channel(item["msg"])
            used_groups.add(group)
            published += 1
            if (item.get("asset_type") or "") == "forex":
                forex_published += 1
            if is_overflow:
                overflow_posts += 1
            _scanner_daily_count["count"] = already_today + published
            pub_reason = "PRO_CHANNEL_OVERFLOW" if is_overflow else "PRO_CHANNEL"
            mark_trade_publication(item.get("idea_id"), "PUBLISHED", pub_reason)
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "PUBLISHED",
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=score,
            )
            logger.info(
                "[SCANNER] PUBLISHED #%s %s %s score=%.1f (%s/%s this scan)%s",
                item["idea_id"],
                item["identifier"],
                item["tier"],
                score,
                published,
                room_scan,
                f" OVERFLOW vs weakest={weakest_today}" if is_overflow else "",
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
        "[SCANNER] Complete — prequalified=%s setups=%s published=%s overflow=%s errors=%s",
        len(prequalified),
        len(ranked),
        published,
        overflow_posts,
        scan_errors,
    )


def get_trade_scan_interval_sec() -> int:
    """Used by handlers scheduler."""
    return TRADE_SCAN_INTERVAL_SEC
