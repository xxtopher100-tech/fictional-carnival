"""
Key alerts engine v2 — level events + major-move early alerts.

Still uses proven check_key_market_alerts for level logic.
Adds major-movement Pro alerts (not trade entries).
"""

from __future__ import annotations

from market_pulse.config_runtime import logger
from market_pulse.major_movement import scan_majors, format_major_move_alert, should_emit_alert


def run_key_alerts_cycle(include_legacy_levels: bool = True) -> dict:
    """
    1) Optional legacy key-level scan (alerts.py)
    2) Major-move developing alerts for majors
    """
    stats = {"legacy": 0, "major_move": 0, "errors": 0}

    if include_legacy_levels:
        try:
            from market_pulse.alerts import check_key_market_alerts
            check_key_market_alerts()
            stats["legacy"] = 1
        except Exception as e:
            stats["errors"] += 1
            logger.warning("[KEY ALERTS V2] legacy: %s", e)

    try:
        from market_pulse.telegram_api import post_to_pro_channel
        dets = scan_majors()
        for det in dets:
            coin = det.get("coin") or ""
            if not should_emit_alert(coin, det):
                continue
            text = format_major_move_alert(det)
            try:
                post_to_pro_channel(text)
                stats["major_move"] += 1
                logger.info("[KEY ALERTS V2] major-move posted %s conf=%s", coin, det.get("confidence"))
            except Exception as pe:
                stats["errors"] += 1
                logger.warning("[KEY ALERTS V2] post %s: %s", coin, pe)
    except Exception as e:
        stats["errors"] += 1
        logger.warning("[KEY ALERTS V2] major scan: %s", e)

    return stats
