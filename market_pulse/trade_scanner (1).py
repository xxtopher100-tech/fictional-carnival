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
from market_pulse.publication_gate import publish_canonical_trade
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


# Analytics-only counters (do NOT block publication). Burst spacing lives in publication_gate.
MAX_TRADES_PER_SCAN = _env_int("MAX_TRADES_PER_SCAN", 2)  # legacy env; not a suppress reason
MAX_TRADES_PER_DAY = _env_int("MAX_TRADES_PER_DAY", 5)  # analytics only
MAX_TRADES_PER_DAY_HARD = _env_int("MAX_TRADES_PER_DAY_HARD", 8)  # analytics only
MAX_FOREX_PER_SCAN = _env_int("MAX_FOREX_PER_SCAN", 1)  # ranking preference only
# Seconds between scan *starts* (default 1H to align with 1H structure; was 14400).
TRADE_SCAN_INTERVAL_SEC = _env_int("TRADE_SCAN_INTERVAL_SEC", 3600)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except Exception:
        return default


SCORE_REPLACE_MARGIN = _env_float("SCORE_REPLACE_MARGIN", 12.0)  # analytics
# Quality floor — weak setups should not publish (slightly higher default = fewer thin ideas)
MIN_PUBLISH_SCORE = _env_float("MIN_PUBLISH_SCORE", 48.0)

# After several published stops on the same side, sit out that side for a while (chop protection).
# Does NOT flip direction and does NOT change Entry/SL/TP math.
STOP_CLUSTER_HOURS = _env_int("STOP_CLUSTER_HOURS", 18)
STOP_CLUSTER_MIN = _env_int("STOP_CLUSTER_MIN", 2)  # tighter: 2 published stops → sit that side out

# Forex publish policy (FX was the loss-streak source: tight stops + stacked EUR/GBP)
FOREX_PUBLISH_ENABLED = os.environ.get("FOREX_PUBLISH_ENABLED", "1").strip().lower() not in (
    "0", "false", "no", "off",
)
MAX_FOREX_PUBLISH_PER_DAY = _env_int("MAX_FOREX_PUBLISH_PER_DAY", 1)  # hard cap: max 1 FX trade/day
MIN_STOP_PCT_CRYPTO = _env_float("MIN_STOP_PCT_CRYPTO", 0.35)  # reject stop tighter than this %
MIN_STOP_PCT_FOREX = _env_float("MIN_STOP_PCT_FOREX", 0.25)

# Major-crypto correlation groups (simple deterministic exposure)
_MAJOR_CRYPTO = frozenset({"BTC", "ETH", "SOL", "BNB", "AVAX", "LINK", "DOGE", "XRP"})

_scanner_daily_count = {"date": "", "count": 0}


def _normalize_side(direction: str) -> str:
    d = (direction or "long").lower().strip()
    if d.startswith("short") or d in ("sell", "s"):
        return "short"
    return "long"


def _recent_stop_cluster_sides(hours: int | None = None, min_stops: int | None = None) -> set[str]:
    """Return {'long'} and/or {'short'} if that side has ≥ min published STOP_HITs in the window.

    Used only as a publish sit-out — not a reverse-entry signal.
    """
    hours = int(hours if hours is not None else STOP_CLUSTER_HOURS)
    min_stops = int(min_stops if min_stops is not None else STOP_CLUSTER_MIN)
    if hours <= 0 or min_stops <= 0:
        return set()
    db = None
    blocked: set[str] = set()
    try:
        db = get_db()
        c = db.cursor()
        since = (wat_now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """
            SELECT LOWER(COALESCE(direction, '')), COUNT(*)
            FROM trade_ideas
            WHERE COALESCE(result, '') IN ('STOP_HIT', 'BE_EXIT')
              AND COALESCE(publication_status, 'PUBLISHED') = 'PUBLISHED'
              AND COALESCE(closed_at, created_at, '') >= %s
            GROUP BY 1
            """,
            (since,),
        )
        for row in c.fetchall() or []:
            side = _normalize_side(str(row[0] or ""))
            n = int(row[1] or 0)
            if n >= min_stops:
                blocked.add(side)
                logger.info(
                    "[SCANNER] Stop-cluster sit-out: %s side has %s published stops in %sh",
                    side, n, hours,
                )
        return blocked
    except Exception as e:
        logger.debug("[SCANNER] stop-cluster check: %s", e)
        return set()
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


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
    """DEPRECATED as a blocker — daily quotas no longer suppress publication.

    Kept for analytics/logging only. Always returns (True, ...).
    """
    score = float(candidate_score or 0)
    if already_today < MAX_TRADES_PER_DAY:
        return True, "UNDER_SOFT_CAP"
    # Former HARD_DAY_CAP / SOFT_CAP paths — no longer block
    return True, "DAILY_CAP_DISABLED"


def _correlation_group(symbol: str, direction: str) -> str:
    sym = (symbol or "").upper().split("/")[0]
    d = (direction or "long").lower()
    side = "long" if d.startswith("long") or d == "buy" or "buy" in d else "short"
    if sym in _MAJOR_CRYPTO:
        return f"major_crypto_{side}"
    # All EUR/GBP (and other FX) same side share one slot — no stacked EUR+GBP buys
    if "/" in (symbol or "") or sym in ("EUR", "GBP"):
        return f"forex_{side}"
    return f"{sym}_{side}"


def _risk_is_tradeable(entry, stop, direction: str, asset_type: str = "crypto") -> tuple[bool, str]:
    """Reject garbage risk: stop≈entry, wrong side, or microscopic stops."""
    try:
        e = float(entry)
        s = float(stop)
    except Exception:
        return False, "INVALID_LEVELS"
    if e <= 0 or s <= 0:
        return False, "NON_POSITIVE_LEVELS"
    risk = abs(e - s)
    risk_pct = (risk / e) * 100.0
    d = (direction or "long").lower()
    is_long = d.startswith("long") or d == "buy" or "buy" in d
    if is_long and s >= e:
        return False, "LONG_STOP_GE_ENTRY"
    if (not is_long) and s <= e:
        return False, "SHORT_STOP_LE_ENTRY"
    min_pct = MIN_STOP_PCT_FOREX if (asset_type or "").lower() == "forex" else MIN_STOP_PCT_CRYPTO
    if risk_pct < min_pct:
        return False, f"STOP_TOO_TIGHT:{risk_pct:.3f}%<{min_pct}%"
    if (asset_type or "").lower() == "forex" and e < 5 and risk < 0.0015:
        return False, "FOREX_STOP_BELOW_PIP_FLOOR"
    return True, "OK"


def _forex_published_today() -> int:
    today = wat_now().strftime("%Y-%m-%d")
    db = None
    try:
        db = get_db()
        c = db.cursor()
        start, end = f"{today} 00:00:00", f"{today} 23:59:59"
        c.execute(
            """
            SELECT COUNT(*) FROM trade_ideas
            WHERE COALESCE(publication_status,'') = 'PUBLISHED'
              AND created_at >= %s AND created_at <= %s
              AND (
                UPPER(COALESCE(coin,'')) LIKE '%%EUR%%'
                OR UPPER(COALESCE(coin,'')) LIKE '%%GBP%%'
                OR UPPER(COALESCE(coin,'')) LIKE '%%/USD%%'
              )
            """,
            (start, end),
        )
        return int((c.fetchone() or [0])[0] or 0)
    except Exception:
        return 0
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _rank_score(
    trade: dict,
    tier: str,
    asset_type: str = "crypto",
    symbol: str | None = None,
) -> float:
    """Signal-time only score. No future candles / outcomes.

    Crypto is the primary MarketPulse product; forex is secondary context.
    Opportunity engine boosts BTC/ETH and vol/break setups (small/normal/big).
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
        # Opportunity layer: candles + vol + F&G + funding bias → rank boost
        try:
            from market_pulse.opportunity_engine import assess_opportunity

            coin = (symbol or trade.get("coin") or "").upper().split("/")[0]
            if coin:
                opp = assess_opportunity(coin)
                boost = float(opp.get("score_boost") or 0.0)
                score += boost
                size = (opp.get("size") or "NONE").upper()
                # Slight tier affinity: BIG favors momentum/edge; SMALL favors steady
                if size == "BIG" and tier_l in ("momentum", "normal", "edge", "aggressive"):
                    score += 8.0
                elif size == "SMALL" and tier_l in ("steady", "safe"):
                    score += 4.0
                trade["_opportunity_size"] = size
                trade["_opportunity_reasons"] = list(opp.get("reasons") or [])[:6]
        except Exception as _opp_e:
            logger.debug("[SCANNER] opportunity rank: %s", _opp_e)
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
            score = _rank_score(trade, tier, asset_type=asset_type, symbol=identifier)
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

    # ── Phase D: CORRELATION + QUALITY → GATE (no daily/scan hard caps) ───
    # Daily quotas are analytics-only. Burst spacing owned by publication_gate.
    already_today = _daily_published_count()  # analytics only

    used_groups = set()
    published = 0
    queued = 0
    forex_published = 0
    overflow_posts = 0

    stop_blocked_sides = _recent_stop_cluster_sides()
    fx_today = _forex_published_today()
    logger.info(
        "[SCANNER] Publish policy: NO daily/scan cap blockers | analytics_day=%s "
        "min_score=%.1f stop_cluster_block=%s forex_enabled=%s fx_today=%s/%s",
        already_today,
        MIN_PUBLISH_SCORE,
        sorted(stop_blocked_sides) or "none",
        FOREX_PUBLISH_ENABLED,
        fx_today,
        MAX_FOREX_PUBLISH_PER_DAY,
    )

    for item in ranked:
        group = _correlation_group(item["identifier"], item.get("direction") or "long")
        score = float(item.get("score") or 0)
        side = _normalize_side(item.get("direction") or "")
        is_fx = (item.get("asset_type") or "") == "forex"

        # Forex: optional kill-switch + hard daily max (default 1)
        if is_fx:
            if not FOREX_PUBLISH_ENABLED:
                record_candidate(
                    scan_run_id, item["identifier"], item["tier"], "SUPPRESSED",
                    rejection_reason="FOREX_DISABLED", direction=item.get("direction"),
                    idea_id=item.get("idea_id"), score=score,
                )
                mark_trade_publication(item.get("idea_id"), "SUPPRESSED", "FOREX_DISABLED")
                continue
            if fx_today + forex_published >= MAX_FOREX_PUBLISH_PER_DAY:
                record_candidate(
                    scan_run_id, item["identifier"], item["tier"], "SUPPRESSED",
                    rejection_reason="FOREX_DAILY_CAP", direction=item.get("direction"),
                    idea_id=item.get("idea_id"), score=score,
                )
                mark_trade_publication(item.get("idea_id"), "SUPPRESSED", "FOREX_DAILY_CAP")
                logger.info(
                    "[SCANNER] SUPPRESSED %s — forex daily cap %s",
                    item["identifier"], MAX_FOREX_PUBLISH_PER_DAY,
                )
                continue

        # Chop protection: do not keep stacking the same side after a stop cluster
        if side in stop_blocked_sides:
            record_candidate(
                scan_run_id,
                item["identifier"],
                item["tier"],
                "SUPPRESSED",
                rejection_reason="STOP_CLUSTER_SITOUT",
                direction=item.get("direction"),
                idea_id=item.get("idea_id"),
                score=score,
            )
            mark_trade_publication(item.get("idea_id"), "SUPPRESSED", "STOP_CLUSTER_SITOUT")
            logger.info(
                "[SCANNER] SUPPRESSED %s %s — stop-cluster sit-out (%s side)",
                item["identifier"], item["tier"], side,
            )
            continue

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

        # BIG/NORMAL on BTC/ETH: slightly lower floor so real legs can surface
        min_need = MIN_PUBLISH_SCORE
        try:
            tr0 = item.get("trade") or {}
            opp_sz = str(tr0.get("_opportunity_size") or "").upper()
            ident0 = (item.get("identifier") or "").upper().split("/")[0]
            if opp_sz == "BIG" and ident0 in ("BTC", "ETH"):
                min_need = max(36.0, MIN_PUBLISH_SCORE - 10.0)
            elif opp_sz == "NORMAL" and ident0 in ("BTC", "ETH"):
                min_need = max(40.0, MIN_PUBLISH_SCORE - 5.0)
        except Exception:
            min_need = MIN_PUBLISH_SCORE
        if score < min_need:
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
                item["identifier"], item["tier"], score, min_need,
            )
            continue

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

        try:
            tr = item.get("trade") or {}
            ent = tr.get("entry")
            stp = tr.get("stop")
            try:
                ent_f = float(ent) if ent is not None else 0.0
            except Exception:
                ent_f = 0.0
            # Hard risk gate — never publish stop≈entry / wrong-side stops
            ok_risk, risk_reason = _risk_is_tradeable(
                ent, stp, item.get("direction") or tr.get("direction") or "",
                asset_type=item.get("asset_type") or "crypto",
            )
            if not ok_risk:
                record_candidate(
                    scan_run_id,
                    item["identifier"],
                    item["tier"],
                    "SUPPRESSED",
                    rejection_reason=f"RISK_REJECT:{risk_reason}",
                    direction=item.get("direction"),
                    idea_id=item.get("idea_id"),
                    score=score,
                )
                mark_trade_publication(
                    item.get("idea_id"), "SUPPRESSED", f"RISK_REJECT:{risk_reason}"
                )
                logger.info(
                    "[SCANNER] RISK_REJECT %s %s — %s",
                    item["identifier"], item["tier"], risk_reason,
                )
                continue
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
            tr = item.get("trade") or {}
            pub_msg = item.get("msg") or ""
            # Layer 3: attach opportunity footer + persist snapshot (crypto only)
            if (item.get("asset_type") or "") == "crypto":
                try:
                    from market_pulse.opportunity_engine import (
                        assess_opportunity,
                        format_opportunity_footer,
                        persist_opportunity_snapshot,
                    )
                    _coin = (item.get("identifier") or "").upper().split("/")[0]
                    if _coin:
                        _opp = assess_opportunity(_coin)
                        tr["_opportunity_size"] = _opp.get("size")
                        tr["_opportunity_reasons"] = list(_opp.get("reasons") or [])[:8]
                        item["trade"] = tr
                        foot = format_opportunity_footer(_opp)
                        if foot and foot not in pub_msg:
                            pub_msg = (pub_msg.rstrip() + "\n" + foot).strip()
                        persist_opportunity_snapshot(int(item.get("idea_id") or 0), _opp)
                except Exception as _ofe:
                    logger.debug("[SCANNER] opportunity footer: %s", _ofe)
            ok_pub, pub_code = publish_canonical_trade(
                msg=pub_msg,
                idea_id=int(item.get("idea_id") or 0),
                symbol=item.get("identifier") or "",
                direction=item.get("direction") or tr.get("direction") or "",
                timeframe=tr.get("timeframe") or item.get("timeframe") or "",
                entry=tr.get("entry"),
                stop=tr.get("stop"),
                target1=tr.get("target1"),
                market_type=item.get("asset_type") or "crypto",
                tier=item.get("tier") or "",
                source="scanner",
            )
            if not ok_pub:
                status_row = "QUEUED" if pub_code == "TEMPORARILY_QUEUED" else "SUPPRESSED"
                record_candidate(
                    scan_run_id,
                    item["identifier"],
                    item["tier"],
                    status_row,
                    rejection_reason=pub_code,
                    direction=item.get("direction"),
                    idea_id=item.get("idea_id"),
                    score=score,
                )
                if pub_code == "TEMPORARILY_QUEUED":
                    queued += 1
                    used_groups.add(group)
                logger.info(
                    "[SCANNER] GATE %s %s %s — %s",
                    pub_code, item["identifier"], item["tier"], item.get("idea_id"),
                )
                continue
            used_groups.add(group)
            published += 1
            if (item.get("asset_type") or "") == "forex":
                forex_published += 1
            _scanner_daily_count["count"] = already_today + published
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
                "[SCANNER] PUBLISHED #%s %s %s score=%.1f",
                item["idea_id"],
                item["identifier"],
                item["tier"],
                score,
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
        "[SCANNER] Complete — prequalified=%s setups=%s published=%s queued=%s errors=%s",
        len(prequalified),
        len(ranked),
        published,
        queued,
        scan_errors,
    )


def get_trade_scan_interval_sec() -> int:
    """Used by handlers scheduler."""
    return TRADE_SCAN_INTERVAL_SEC
