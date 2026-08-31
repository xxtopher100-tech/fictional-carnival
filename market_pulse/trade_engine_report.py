"""Daily private admin report for the trade discovery/publish funnel.

Durable counters in admin_settings + queries on trade_ideas.
Does not change strategy math.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

from market_pulse.config_runtime import logger
from market_pulse.db import get_db
from market_pulse.helpers import wat_now
from market_pulse.outcome_monitor import _notify_admins


def _ensure_report_schema():
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_scan_runs (
                id SERIAL PRIMARY KEY,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT DEFAULT 'running',
                markets_scanned INT DEFAULT 0,
                candidates_detected INT DEFAULT 0,
                qualified_count INT DEFAULT 0,
                published_count INT DEFAULT 0,
                error_count INT DEFAULT 0
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_scan_candidates (
                id SERIAL PRIMARY KEY,
                scan_run_id INT,
                symbol TEXT NOT NULL,
                tier TEXT,
                direction TEXT,
                status TEXT NOT NULL,
                rejection_reason TEXT,
                score DOUBLE PRECISION,
                idea_id INT,
                created_at TEXT NOT NULL
            )
            """
        )
        db.commit()
    except Exception as e:
        logger.warning("[ENGINE REPORT] schema: %s", e)
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


def start_scan_run() -> int:
    """Insert a scan run row; returns id (0 on failure)."""
    _ensure_report_schema()
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO trade_scan_runs (started_at, status) VALUES (%s,'running') RETURNING id",
            (now,),
        )
        rid = c.fetchone()[0]
        db.commit()
        return int(rid)
    except Exception as e:
        logger.debug("[ENGINE REPORT] start_scan_run: %s", e)
        if db:
            try:
                db.rollback()
            except Exception:
                pass
        return 0
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def record_candidate(
    scan_run_id: int,
    symbol: str,
    tier: str,
    status: str,
    rejection_reason: str | None = None,
    direction: str | None = None,
    idea_id: int | None = None,
    score: float | None = None,
):
    """Persist one funnel event. Best-effort; never raises into scanner."""
    if not scan_run_id:
        return
    _ensure_report_schema()
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """
            INSERT INTO trade_scan_candidates
            (scan_run_id, symbol, tier, direction, status, rejection_reason, score, idea_id, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                scan_run_id,
                symbol,
                tier,
                direction,
                status,
                rejection_reason,
                score,
                idea_id,
                now,
            ),
        )
        db.commit()
    except Exception as e:
        logger.debug("[ENGINE REPORT] record_candidate: %s", e)
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


def finish_scan_run(scan_run_id: int, markets_scanned: int = 0, error_count: int = 0):
    if not scan_run_id:
        return
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """
            SELECT
              COUNT(*) FILTER (WHERE status IN ('QUALIFIED','PUBLISHED','SUPPRESSED')),
              COUNT(*) FILTER (WHERE status = 'PUBLISHED'),
              COUNT(*)
            FROM trade_scan_candidates WHERE scan_run_id=%s
            """,
            (scan_run_id,),
        )
        row = c.fetchone() or (0, 0, 0)
        qual, pub, total = int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
        c.execute(
            """
            UPDATE trade_scan_runs SET
              completed_at=%s, status='done',
              markets_scanned=%s, candidates_detected=%s,
              qualified_count=%s, published_count=%s, error_count=%s
            WHERE id=%s
            """,
            (now, markets_scanned, total, qual, pub, error_count, scan_run_id),
        )
        db.commit()
    except Exception as e:
        logger.debug("[ENGINE REPORT] finish_scan_run: %s", e)
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


def _day_bounds_wat(day_str: str | None = None):
    """Return (start, end) WAT strings for a calendar day."""
    if day_str:
        start = f"{day_str} 00:00:00"
        end = f"{day_str} 23:59:59"
        return start, end
    d = wat_now().strftime("%Y-%m-%d")
    return f"{d} 00:00:00", f"{d} 23:59:59"


def build_daily_engine_report(day_str: str | None = None) -> str:
    """Build private admin report text for a WAT calendar day."""
    _ensure_report_schema()
    day = day_str or wat_now().strftime("%Y-%m-%d")
    start, end = _day_bounds_wat(day)
    db = None
    try:
        db = get_db()
        c = db.cursor()

        c.execute(
            "SELECT COUNT(*) FROM trade_scan_runs WHERE started_at >= %s AND started_at <= %s",
            (start, end),
        )
        scan_cycles = int((c.fetchone() or [0])[0] or 0)

        c.execute(
            """
            SELECT COALESCE(SUM(markets_scanned),0) FROM trade_scan_runs
            WHERE started_at >= %s AND started_at <= %s
            """,
            (start, end),
        )
        markets_scanned = int((c.fetchone() or [0])[0] or 0)

        c.execute(
            """
            SELECT status, COALESCE(rejection_reason,''), COUNT(*)
            FROM trade_scan_candidates
            WHERE created_at >= %s AND created_at <= %s
            GROUP BY status, COALESCE(rejection_reason,'')
            """,
            (start, end),
        )
        by_status = defaultdict(int)
        by_reason = defaultdict(int)
        for st, reason, cnt in c.fetchall() or []:
            by_status[st] = by_status[st] + int(cnt)
            if st in ("REJECTED", "BLOCKED", "SUPPRESSED", "NO_SETUP") and reason:
                by_reason[reason] = by_reason[reason] + int(cnt)

        c.execute(
            """
            SELECT COALESCE(tier,'?'), COUNT(*) FROM trade_scan_candidates
            WHERE created_at >= %s AND created_at <= %s AND status='PUBLISHED'
            GROUP BY COALESCE(tier,'?')
            """,
            (start, end),
        )
        by_tier_pub = {r[0]: int(r[1]) for r in (c.fetchall() or [])}

        c.execute(
            """
            SELECT symbol, COUNT(*) FROM trade_scan_candidates
            WHERE created_at >= %s AND created_at <= %s AND status='PUBLISHED'
            GROUP BY symbol
            """,
            (start, end),
        )
        by_asset = {r[0]: int(r[1]) for r in (c.fetchall() or [])}

        # Outcomes: terminal transitions closed today (once per idea)
        c.execute(
            """
            SELECT COALESCE(result, lifecycle_status, ''), COUNT(*)
            FROM trade_ideas
            WHERE closed_at >= %s AND closed_at <= %s
              AND COALESCE(coin,'') NOT ILIKE '%%USDT/NGN%%'
              AND COALESCE(publication_status, 'PUBLISHED') = 'PUBLISHED'
            GROUP BY COALESCE(result, lifecycle_status, '')
            """,
            (start, end),
        )
        outcomes = {str(r[0]): int(r[1]) for r in (c.fetchall() or [])}

        c.execute(
            """
            SELECT COUNT(*) FROM trade_ideas
            WHERE status='open' AND COALESCE(coin,'') NOT ILIKE '%%USDT/NGN%%'
              AND COALESCE(publication_status, 'PUBLISHED') = 'PUBLISHED'
            """
        )
        still_open = int((c.fetchone() or [0])[0] or 0)

        raw = sum(by_status.values())
        published = by_status.get("PUBLISHED", 0)
        qualified = by_status.get("QUALIFIED", 0) + published + by_status.get("SUPPRESSED", 0)

        def _r(*keys):
            return sum(by_reason.get(k, 0) for k in keys)

        lines = [
            "📊 <b>MARKETPULSE DAILY ENGINE REPORT</b>",
            f"Date: <b>{day}</b> WAT",
            f"Scan cycles: <b>{scan_cycles}</b>",
            f"Markets scanned (sum): <b>{markets_scanned}</b>",
            "",
            "<b>DISCOVERY</b>",
            f"Raw candidate records: <b>{raw}</b>",
            "",
            "<b>REJECTIONS / BLOCKS</b>",
            f"Insufficient data: {_r('INSUFFICIENT_CANDLES','PRICE_UNAVAILABLE')}",
            f"Trend: {_r('TREND_FAILED')}",
            f"Structure: {_r('STRUCTURE_FAILED')}",
            f"Volatility: {_r('VOLATILITY_FAILED')}",
            f"Fear & Greed: {_r('FEAR_GREED_FAILED')}",
            f"News: {_r('NEWS_BLACKOUT')}",
            f"Cooldown: {_r('COOLDOWN')}",
            f"Duplicate: {_r('DUPLICATE','DUPLICATE_LEVEL')}",
            f"Existing trade: {_r('EXISTING_OPEN_TRADE','SAME_DIRECTION_OPEN')}",
            f"Correlation: {_r('CORRELATED_SUPPRESSED')}",
            f"Publish limit: {_r('PUBLISH_LIMIT','LOW_RANK')}",
            f"Other/NO_SETUP: {by_status.get('NO_SETUP', 0) + by_status.get('REJECTED', 0)}",
            "",
            f"<b>QUALIFIED (incl. published/suppressed):</b> {qualified}",
            "",
            "<b>PUBLISHING</b>",
            f"Published: <b>{published}</b>",
            f"Suppressed after qualify: {by_status.get('SUPPRESSED', 0)}",
            "",
            "<b>BY TIER (published)</b>",
            f"SAFE/steady: {by_tier_pub.get('steady', 0) + by_tier_pub.get('SAFE', 0)}",
            f"NORMAL/momentum: {by_tier_pub.get('momentum', 0) + by_tier_pub.get('NORMAL', 0)}",
            f"EDGE/aggressive: {by_tier_pub.get('edge', 0) + by_tier_pub.get('AGGRESSIVE', 0)}",
            "",
            "<b>BY ASSET (published)</b>",
        ]
        if by_asset:
            for sym, n in sorted(by_asset.items(), key=lambda x: -x[1]):
                lines.append(f"{sym}: {n}")
        else:
            lines.append("None")

        lines += [
            "",
            "<b>OUTCOMES TODAY</b> (closed_at on this WAT day)",
            f"TP1: {outcomes.get('TP1_HIT', 0) + outcomes.get('TARGET_HIT', 0)}",
            f"TP2: {outcomes.get('TP2_HIT', 0)}",
            f"STOP: {outcomes.get('STOP_HIT', 0) + outcomes.get('BE_EXIT', 0)}",
            f"EXPIRED: {outcomes.get('EXPIRED', 0) + outcomes.get('SETUP_EXPIRED', 0)}",
            f"AMBIGUOUS: {outcomes.get('AMBIGUOUS', 0)}",
            f"Still open (ledger): <b>{still_open}</b>",
            "",
            "",
            "<b>INTERNAL / SUPPRESSED (research only)</b>",
            "<i>Not included in official TP/SL stats above</i>",
            "",
            "<i>Private admin only · funnel visibility · NFA</i>",
        ]
        return "\n".join(lines)
    except Exception as e:
        logger.error("[ENGINE REPORT] build: %s", e)
        return f"📊 DAILY ENGINE REPORT\nDate: {day}\n⚠️ Build error: {e}"
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def send_daily_engine_report(force: bool = False) -> bool:
    """Send once per WAT day unless force=True."""
    _ensure_report_schema()
    day = wat_now().strftime("%Y-%m-%d")
    key = f"daily_engine_report_{day}"
    db = None
    try:
        db = get_db()
        c = db.cursor()
        if not force:
            c.execute("SELECT value FROM admin_settings WHERE key=%s", (key,))
            if c.fetchone():
                logger.info("[ENGINE REPORT] already sent for %s", day)
                return False
        text = build_daily_engine_report(day)
        _notify_admins(text)
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            (key, "1", now),
        )
        db.commit()
        logger.info("[ENGINE REPORT] sent for %s", day)
        return True
    except Exception as e:
        logger.error("[ENGINE REPORT] send: %s", e)
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
