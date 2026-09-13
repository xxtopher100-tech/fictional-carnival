"""
Feedback engine — reads lifecycle/monitor results for human review.

Does NOT auto-rewrite strategy. Uses trade_ideas ledger.
"""

from __future__ import annotations

from collections import Counter
from market_pulse.config_runtime import logger
from market_pulse.db import get_db
from market_pulse.helpers import wat_now
from datetime import timedelta


def collect_feedback(days: int = 7) -> dict:
    """Aggregate recent published trade outcomes."""
    out = {
        "period_days": days,
        "total_closed": 0,
        "by_result": {},
        "by_coin": {},
        "by_tier": {},
        "notes": [],
    }
    db = None
    try:
        db = get_db()
        c = db.cursor()
        since = (wat_now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """
            SELECT coin, COALESCE(tier,''), COALESCE(result,''), COALESCE(direction,''),
                   COALESCE(publication_status,'')
            FROM trade_ideas
            WHERE status='closed'
              AND COALESCE(closed_at, created_at, '') >= %s
              AND COALESCE(publication_status,'PUBLISHED') = 'PUBLISHED'
            """,
            (since,),
        )
        rows = c.fetchall() or []
        out["total_closed"] = len(rows)
        rc, cc, tc = Counter(), Counter(), Counter()
        for coin, tier, result, direction, pub in rows:
            r = (result or "UNKNOWN").upper()
            rc[r] += 1
            cc[(coin or "?").upper()] += 1
            tc[(tier or "?").lower()] += 1
        out["by_result"] = dict(rc)
        out["by_coin"] = dict(cc.most_common(12))
        out["by_tier"] = dict(tc)

        stops = rc.get("STOP_HIT", 0) + rc.get("BE_EXIT", 0)
        wins = rc.get("TP1_HIT", 0) + rc.get("TP2_HIT", 0)
        if out["total_closed"] >= 5:
            if stops > wins * 2:
                out["notes"].append("Stops dominate wins — review regime / frequency.")
            if wins > stops:
                out["notes"].append("Wins outpace stops in this window.")
        if not rows:
            out["notes"].append("No closed published trades in window.")
    except Exception as e:
        out["notes"].append(f"error:{e}")
        logger.warning("[FEEDBACK] %s", e)
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass
    return out


def format_feedback_report(data: dict | None = None) -> str:
    data = data or collect_feedback(7)
    lines = [
        "📊 FEEDBACK REPORT (private)",
        f"Window: last {data.get('period_days')} days",
        f"Closed published trades: {data.get('total_closed')}",
        "",
        "Results:",
    ]
    for k, v in sorted((data.get("by_result") or {}).items(), key=lambda x: -x[1]):
        lines.append(f"  {k}: {v}")
    lines.append("")
    lines.append("By coin:")
    for k, v in (data.get("by_coin") or {}).items():
        lines.append(f"  {k}: {v}")
    if data.get("notes"):
        lines.append("")
        lines.append("Notes:")
        for n in data["notes"]:
            lines.append(f"  · {n}")
    lines.append("")
    lines.append("For human review only — does not change live rules.")
    return "\n".join(lines)
