"""Market Pulse Bot — edge_trade_engine module (split from the real monolithic bot.py)."""

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
from market_pulse.ai_narrative_guard import sanitize_ai_narrative, append_narrative_rules
from market_pulse.alerts import (
    _calc_trade_metrics, _validate_alert, _infer_direction,
    _format_trade_price, _parse_price_token,
)
from market_pulse.config_runtime import logger
from market_pulse.db import get_db
from market_pulse.fear_greed import get_fear_greed
from market_pulse.helpers import format_price, wat_now
from market_pulse.p2p import get_p2p_rate
from market_pulse.price_fetchers import get_best_price, get_secondary_coin
from market_pulse.setup_engine import build_programmatic_setup, news_market_flag, resolve_horizon, compute_valid_until
from market_pulse.telegram_api import send


# ─── extracted section ───
# ⚡ EDGE TRADE ENGINE — THREE-TIER TRADE SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

TRADE_TIERS = {
    # Keys kept as steady/momentum/edge for DB + callers. Display = SAFE/NORMAL/AGGRESSIVE.
    "steady": {
        "label": "SAFE TRADE", "emoji": "🟢",
        "risk_desc": "Highest confirmation — capital preservation",
        "max_stop_pct": 5.0, "min_target_pct": 8.0, "min_rr": 1.5,
        "max_size": "2-4% of portfolio",
        "display_tier": "SAFE",
    },
    "momentum": {
        "label": "NORMAL TRADE", "emoji": "🟡",
        "risk_desc": "Balanced setup — default trading tier",
        "max_stop_pct": 8.0, "min_target_pct": 12.0, "min_rr": 1.5,
        "max_size": "2-3% of portfolio",
        "display_tier": "NORMAL",
    },
    "edge": {
        "label": "AGGRESSIVE TRADE", "emoji": "🔴",
        "risk_desc": "HIGHER SETUP RISK — calculated early opportunity (not larger size)",
        "max_stop_pct": 12.0, "min_target_pct": 20.0, "min_rr": 1.8,
        "max_size": "1-2% of portfolio MAX",
        "display_tier": "AGGRESSIVE",
    },
}
# Aliases so callers can use safe/normal/aggressive
TRADE_TIERS["safe"] = TRADE_TIERS["steady"]
TRADE_TIERS["normal"] = TRADE_TIERS["momentum"]
TRADE_TIERS["aggressive"] = TRADE_TIERS["edge"]


def _normalize_tier(tier):
    t = (tier or "momentum").lower().strip()
    return {"safe": "steady", "normal": "momentum", "aggressive": "edge"}.get(t, t)

EDGE_DISCLAIMER = (
    ("\u2501" * 24)
    + "\n"
    + "\u26a0\ufe0f <b>RISK DISCLAIMER</b>\n"
    + "This is a HIGH-RISK setup. You can LOSE your entire position. "
    + "Only trade money you can afford to lose completely. "
    + "Past setups do not guarantee future results. "
    + "Market Pulse takes no responsibility for trading outcomes.\n"
    + "NFA \u2014 DYOR \u2014 Trade at your own risk.\n"
    + ("\u2501" * 24)
)


STANDARD_DISCLAIMER = (
    "<i>Illustrative only. Not financial advice. "
    "Always use a stop loss. NFA \u2014 DYOR \u2014 manage your risk.</i>\n"
    "\u26a1 Market Pulse Pro"
)



def _strip_all_disclaimers(text: str) -> str:
    """Aggressively remove every risk-disclaimer / footer block (any count)."""
    if not text:
        return ""
    import re as _re
    t = str(text)
    for _ in range(50):
        upper = t.upper()
        idx = upper.find("RISK DISCLAIMER")
        if idx < 0:
            idx = upper.find("HIGH-RISK SETUP")
        if idx < 0:
            break
        cut_from = idx
        line_start = t.rfind("\n", 0, idx)
        if line_start >= 0:
            prefix = t[line_start + 1:idx]
            if all(
                ch in "━─\u2501\u2500-= \t\u26a0\ufe0f*" or ord(ch) > 0x2500
                for ch in prefix.strip()
            ) or "⚠" in prefix:
                cut_from = line_start + 1
        t = t[:cut_from].rstrip()
    patterns = [
        r"(?is)<i>\s*Illustrative only\..*?</i>\s*(?:\u26a1|⚡)?\s*Market Pulse Pro\s*",
        r"(?is)Illustrative only\.\s*Not financial advice\..*?NFA\s*[—\-]\s*DYOR.*?(?:\u26a1|⚡)?\s*Market Pulse Pro\s*",
        r"(?is)<i>NFA\s*[—\-]\s*manage your risk\.?\s*[·.]?\s*(?:\u26a1|⚡)?\s*Market Pulse Pro</i>\s*",
        r"(?is)NFA\s*[—\-]\s*manage your risk\.?\s*[·.]?\s*(?:\u26a1|⚡)\s*Market Pulse Pro\s*",
        r"(?m)^(?:\u26a1|⚡)\s*Market Pulse Pro\s*$",
    ]
    for pat in patterns:
        t = _re.sub(pat, "", t)
    t = _re.sub(r"(?:(?:\u26a1|⚡)\s*Market Pulse Pro\s*){2,}", "⚡ Market Pulse Pro\n", t)
    t = _re.sub(r"(?m)^[\u2501\u2500━─\-=]{5,}\s*$", "", t)
    t = _re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def _finalize_trade_message(body: str, tier: str) -> str:
    """Body + exactly one disclaimer. Never stacks."""
    clean = _strip_all_disclaimers(body)
    disc = EDGE_DISCLAIMER if (tier or "").lower() == "edge" else STANDARD_DISCLAIMER
    return clean.rstrip() + "\n\n" + disc.strip()


def _gather_trade_analytics(coin, price):
    """Pull rich market data from price history DB for AI context.
    Returns a dict of calculated indicators."""
    analytics = {
        "rsi_14": None,
        "above_ma20": None,
        "pct_from_30d_high": None,
        "pct_from_30d_low": None,
        "volume_trend": None,
        "price_30d_high": None,
        "price_30d_low": None,
    }
    db = None
    try:
        db = get_db()
        c = db.cursor()
        since_30d = (wat_now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "SELECT price FROM history WHERE coin=%s AND timestamp >= %s ORDER BY timestamp ASC",
            (coin, since_30d)
        )
        rows = c.fetchall()
        prices = [float(r[0]) for r in rows if r[0]]

        if len(prices) >= 14:
            # RSI-14 approximation using Wilder smoothing
            gains, losses = [], []
            for i in range(1, len(prices)):
                delta = prices[i] - prices[i-1]
                gains.append(max(delta, 0))
                losses.append(max(-delta, 0))
            avg_gain = sum(gains[-14:]) / 14
            avg_loss = sum(losses[-14:]) / 14
            if avg_loss > 0:
                rs = avg_gain / avg_loss
                analytics["rsi_14"] = round(100 - (100 / (1 + rs)), 1)
            else:
                analytics["rsi_14"] = 100.0

        if len(prices) >= 20:
            ma20 = sum(prices[-20:]) / 20
            analytics["above_ma20"] = price > ma20

        if len(prices) >= 5:
            high_30d = max(prices)
            low_30d  = min(prices)
            analytics["price_30d_high"] = high_30d
            analytics["price_30d_low"]  = low_30d
            analytics["pct_from_30d_high"] = round((price - high_30d) / high_30d * 100, 1)
            analytics["pct_from_30d_low"]  = round((price - low_30d)  / low_30d  * 100, 1)

        # Volume trend: compare recent 7 data points to previous 7
        if len(prices) >= 14:
            recent_vol  = sum(abs(prices[i]-prices[i-1]) for i in range(len(prices)-7, len(prices)))
            prev_vol    = sum(abs(prices[i]-prices[i-1]) for i in range(len(prices)-14, len(prices)-7))
            if prev_vol > 0:
                analytics["volume_trend"] = "rising" if recent_vol > prev_vol * 1.1 else (
                    "falling" if recent_vol < prev_vol * 0.9 else "flat"
                )

    except Exception as e:
        logger.warning(f"[TRADE ANALYTICS] {coin}: {e}")
    finally:
        if db:
            try: db.close()
            except Exception: pass

    return analytics


def _analytics_to_str(a):
    """Format analytics dict into a concise string for the AI prompt."""
    parts = []
    if a["rsi_14"] is not None:
        rsi = a["rsi_14"]
        zone = "oversold" if rsi < 35 else ("overbought" if rsi > 65 else "neutral")
        parts.append(f"RSI-14: {rsi} ({zone})")
    if a["above_ma20"] is not None:
        parts.append(f"Price {'above' if a['above_ma20'] else 'below'} 20-day average")
    if a["pct_from_30d_high"] is not None:
        parts.append(f"{a['pct_from_30d_high']:+.1f}% from 30d high ({format_price(a['price_30d_high'])})")
    if a["pct_from_30d_low"] is not None:
        parts.append(f"{a['pct_from_30d_low']:+.1f}% from 30d low ({format_price(a['price_30d_low'])})")
    if a["volume_trend"]:
        parts.append(f"Volatility trend: {a['volume_trend']}")
    return " | ".join(parts) if parts else "Insufficient history (< 14 data points)"


def _tier_conditions_met(tier, analytics, fg_val):
    """Pre-screen by tier. Keys: steady=SAFE, momentum=NORMAL, edge=AGGRESSIVE.

    SAFE: strict — reject extremes, need calm/structured tape.
    NORMAL: need directional bias; reject pure dead range.
    AGGRESSIVE: need a catalyst (extension / extreme / F&G); still not random.
    """
    tier = (tier or "momentum").lower()
    if tier in ("safe",):
        tier = "steady"
    elif tier in ("normal",):
        tier = "momentum"
    elif tier in ("aggressive",):
        tier = "edge"

    rsi = analytics.get("rsi_14")
    above_ma = analytics.get("above_ma20")
    vol_trend = analytics.get("volume_trend")
    pct_high = analytics.get("pct_from_30d_high")
    fg = int(fg_val) if str(fg_val).isdigit() else 50

    # ── SAFE (steady) ───────────────────────────────────────────────────
    if tier == "steady":
        if rsi is not None and (rsi > 70 or rsi < 30):
            return False, f"SAFE: RSI {rsi:.0f} extreme — wait for mean reversion structure"
        if vol_trend == "rising" and fg >= 70:
            return False, "SAFE: rising vol + elevated greed — skip"
        if fg >= 80 or fg <= 15:
            return False, f"SAFE: F&G {fg} too extreme"
        # Prefer not fighting MA when RSI is stretched against it
        if rsi is not None and above_ma is True and rsi > 68:
            return False, "SAFE: extended above MA with high RSI"
        if rsi is not None and above_ma is False and rsi < 32:
            return False, "SAFE: extended below MA with low RSI"
        return True, "SAFE pre-screen ok"

    # ── NORMAL (momentum) ───────────────────────────────────────────────
    if tier == "momentum":
        if vol_trend == "flat" and rsi is not None and 42 <= rsi <= 58:
            return False, "NORMAL: dead range (flat vol, neutral RSI) — no trade"
        if rsi is not None and (rsi > 82 or rsi < 18):
            return False, f"NORMAL: RSI {rsi:.0f} too extreme even for default tier"
        return True, "NORMAL pre-screen ok"

    # ── AGGRESSIVE (edge) — v3.1: NORMAL quality floor + specific additional edge ──
    # EDGE must never mean a weaker setup than NORMAL.
    if tier == "edge":
        ok_n, reason_n = _tier_conditions_met("momentum", analytics, fg_val)
        if not ok_n:
            return False, f"EDGE blocked by NORMAL floor: {reason_n}"
        has_catalyst = False
        if rsi is not None and (rsi >= 65 or rsi <= 35):
            has_catalyst = True
        if pct_high is not None and abs(float(pct_high)) <= 4.0:
            has_catalyst = True  # near 30d extreme
        if fg >= 72 or fg <= 28:
            has_catalyst = True
        if vol_trend == "rising":
            has_catalyst = True
        if not has_catalyst:
            return False, "EDGE: no additional catalyst (extension/extreme/F&G/vol)"
        return True, "EDGE pre-screen ok (NORMAL floor + catalyst)"

    return True, "ok"



def _build_trade_ai_prompt(coin, price, tier, sd, fg_val, p2p_str, analytics=None):
    tier_cfg = TRADE_TIERS[tier]
    h24 = sd.get("usd_24h_high") if sd else None
    l24 = sd.get("usd_24h_low") if sd else None
    h_str = format_price(h24) if isinstance(h24, (int, float)) else "N/A"
    l_str = format_price(l24) if isinstance(l24, (int, float)) else "N/A"
    analytics_str = _analytics_to_str(analytics) if analytics else "No history data"
    tf_guide = {
        "steady":   "Daily or Weekly. Prefer established structure.",
        "momentum": "4H or Daily. Breakouts or trend continuations.",
        "edge":     "1H or 4H. High-conviction momentum setups only.",
    }
    return (
        f"You are a professional crypto analyst generating a {tier_cfg['risk_desc']} trade idea "
        f"for Nigerian traders on Market Pulse Pro.\n\n"
        f"COIN: {coin} | PRICE: {format_price(price)} | 24H: {l_str}—{h_str}\n"
        f"FEAR & GREED: {fg_val}/100 | P2P: {p2p_str}\n"
        f"MARKET DATA: {analytics_str}\n\n"
        f"TIER: {tier_cfg['label']} — {tier_cfg['risk_desc']}\n"
        f"TIMEFRAME: {tf_guide[tier]}\n"
        f"STOP MAX: {tier_cfg['max_stop_pct']}% | TARGET MIN: {tier_cfg['min_target_pct']}% | MIN R:R: {tier_cfg['min_rr']}:1\n"
        f"Do NOT state any risk/reward or R:R ratio — levels only.\n\n"
        f"Use the MARKET DATA above (RSI, MA, distance from highs/lows) to justify your setup.\n"
        f"If the data does not support a {tier} setup, say so — do not force a trade.\n\n"
        f"Respond ONLY in this exact format. No asterisks. Plain text:\n"
        f"TIMEFRAME: [1H / 4H / Daily / Weekly]\n"
        f"DIRECTION: [Long / Short]\n"
        f"RATIONALE: [2 sentences — must reference the market data above]\n"
        f"NIGERIAN ANGLE: [1 sentence ONLY if P2P figures were provided — else omit or say unavailable]\n"
        f"Market Bias: [Strongly Bullish / Bullish / Neutral / Bearish / Strongly Bearish]\n"
        f"Entry: $[price]\n"
        f"Stop Loss: $[price]\n"
        f"Target 1: $[price]\n"
        f"Target 2: $[price or none]\n"
        f"Invalidation: $[price]\n"
        f"Confidence: [High / Moderate / Low]\n"
        f"If no quality setup: TIMEFRAME: None\nDIRECTION: None\nEntry: none"
    )


def _parse_trade_idea(ai_text, price):
    """Parse AI trade idea. Preserves decimals; rejects levels far from market."""
    if not ai_text:
        return None
    try:
        def _get(pattern, text):
            m = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            return m.group(1).strip() if m else None

        def _pf(patterns, text):
            if isinstance(patterns, str):
                patterns = [patterns]
            for pattern in patterns:
                raw = _get(pattern, text)
                if not raw or raw.lower() in ("none", "n/a", "-", "$none"):
                    continue
                val = _parse_price_token(raw)
                if val is None:
                    continue
                ref = float(price) if price else None
                if ref and ref > 0 and abs(val - ref) / ref > 0.40:
                    continue
                formatted = _format_trade_price(val)
                if formatted:
                    return formatted
            return None

        rationale = _get(r"RATIONALE[:\s]*(.+?)(?=\n\s*NIGERIAN|\n\s*Market Bias|\n\s*Entry:|\Z)", ai_text)
        if rationale:
            rationale = re.sub(
                r"(?is)[━\-_=\u2501]{3,}.*?RISK DISCLAIMER.*?(?:own risk\.|DYOR|Trade at your own risk\.).*?(?:[━\-_=\u2501]{3,})?",
                "",
                rationale,
            ).strip()
            rationale = re.sub(
                r"(?is)(?:⚠️\s*)?RISK DISCLAIMER.*?(?:own risk\.|Trade at your own risk\.)",
                "",
                rationale,
            ).strip()
            rationale = re.sub(
                r"(?is)This is a HIGH-RISK setup\..*?(?:own risk\.|Trade at your own risk\.)",
                "",
                rationale,
            ).strip()
            rationale = re.sub(r"(?is)NFA\s*[—\-]+\s*DYOR.*?(?:own risk\.|$)", "", rationale).strip()
            rationale = re.sub(r"[━\-_=\u2501]{3,}", "", rationale).strip()
            rationale = re.sub(r"\n{2,}", "\n", rationale).strip()

        return {
            "timeframe":    _get(r"TIMEFRAME[:\s]+(\S+)", ai_text) or "4H",
            "direction":    _get(r"DIRECTION[:\s]+(\w+)", ai_text) or "Long",
            "rationale":    rationale,
            "ng_angle":     _get(r"NIGERIAN ANGLE[:\s]*(.+?)(?=\n\s*Market Bias|\n\s*Entry:|\Z)", ai_text),
            "bias":         _get(r"Market Bias[:\s]*(.+?)(?=\n|\Z)", ai_text) or "Neutral",
            "entry":        _pf([r"Entry[:\s]+\$?([0-9,\.]+)"], ai_text),
            "stop":         _pf([
                r"Stop\s*Loss[:\s]+\$?([0-9,\.]+)",
                r"Stop[:\s]+\$?([0-9,\.]+)",
                r"SL[:\s]+\$?([0-9,\.]+)",
            ], ai_text),
            "target1":      _pf([
                r"Target\s*1[:\s]+\$?([0-9,\.]+)",
                r"TP\s*1[:\s]+\$?([0-9,\.]+)",
            ], ai_text),
            "target2":      _pf([
                r"Target\s*2[:\s]+\$?([0-9,\.]+)",
                r"TP\s*2[:\s]+\$?([0-9,\.]+)",
            ], ai_text),
            "invalidation": _pf([r"Invalidation[:\s]+\$?([0-9,\.]+)"], ai_text),
            "confidence":   _get(r"Confidence[:\s]+(\w+)", ai_text) or "Moderate",
        }
    except Exception as e:
        logger.warning(f"[TRADE PARSE] {e}")
        return None


def save_trade_idea(coin, tier, trade, ai_raw=""):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        # Optional horizon columns
        for col, typ in (
            ("valid_until", "TEXT"),
            ("expected_horizon", "TEXT"),
            ("lifecycle_status", "TEXT"),
            ("publication_status", "TEXT"),
            ("publication_reason", "TEXT"),
        ):
            try:
                c.execute(f"ALTER TABLE trade_ideas ADD COLUMN IF NOT EXISTS {col} {typ}")
            except Exception:
                try:
                    db.rollback()
                except Exception:
                    pass
                db = get_db()
                c = db.cursor()
        try:
            db.commit()
        except Exception:
            pass

        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        tier_key = _normalize_tier(tier) if "_normalize_tier" in dir() else (tier or "momentum")
        try:
            tier_key = _normalize_tier(tier)
        except Exception:
            tier_key = tier
        if tier_key not in TRADE_TIERS:
            tier_key = "momentum"

        hz = None
        try:
            hz = resolve_horizon(trade.get("timeframe"), tier=tier_key)
        except Exception:
            hz = {"timeframe": trade.get("timeframe") or "1H", "expected_horizon": "Typically hours to ~1-2 days", "valid_hours": 60}

        tf = trade.get("timeframe") or hz.get("timeframe") or "1H"
        expected_horizon = trade.get("expected_horizon") or hz.get("expected_horizon")
        valid_until = trade.get("valid_until")
        if not valid_until:
            try:
                valid_until = compute_valid_until(now, hz.get("valid_hours", 60))
            except Exception:
                valid_until = now

        # Do not open a duplicate while an idea is still valid
        direction = trade.get("direction", "Long")
        try:
            c.execute(
                """
                SELECT id, valid_until, entry, stop, target1 FROM trade_ideas
                WHERE coin=%s AND tier=%s AND direction=%s AND status='open'
                ORDER BY id DESC LIMIT 1
                """,
                (coin, tier_key, direction),
            )
            existing = c.fetchone()
            if existing:
                eid, vu, e_ent, e_stop, e_t1 = existing
                still_valid = True
                if vu:
                    try:
                        from datetime import datetime as _dt
                        still_valid = _dt.strptime(str(vu)[:19], "%Y-%m-%d %H:%M:%S") >= _dt.strptime(now[:19], "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        still_valid = True
                if still_valid:
                    logger.info(
                        "[TRADE IDEAS] retained existing #%s for %s %s %s (still valid until %s) — skip new",
                        eid, coin, tier_key, direction, vu,
                    )
                    return eid
        except Exception as e:
            logger.debug("[TRADE IDEAS] duplicate check: %s", e)

        metrics = _calc_trade_metrics(trade.get("entry",""), trade.get("stop",""), trade.get("target1",""))
        rr_str = f"1:{metrics['rr']}" if metrics else "N/A"
        try:
            c.execute(
                """INSERT INTO trade_ideas
                   (coin, tier, direction, timeframe, entry, stop, target1, target2,
                    bias, confidence, rr, invalidation, max_size_pct, ai_rationale, status, created_at,
                    valid_until, expected_horizon, lifecycle_status,
                    publication_status, publication_reason)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open',%s,%s,%s,%s,%s,%s) RETURNING id""",
                (coin, tier_key, direction, tf,
                 trade.get("entry"), trade.get("stop"), trade.get("target1"), trade.get("target2"),
                 trade.get("bias","Neutral"), trade.get("confidence","Moderate"), rr_str,
                 trade.get("invalidation"), TRADE_TIERS[tier_key]["max_size"],
                 ai_raw[:500] if ai_raw else "", now,
                 valid_until, expected_horizon, trade.get("lifecycle_status") or "ENTRY_NOT_REACHED",
                 "PENDING", None)
            )
        except Exception:
            # Fallback without new columns
            c.execute(
                """INSERT INTO trade_ideas
                   (coin, tier, direction, timeframe, entry, stop, target1, target2,
                    bias, confidence, rr, invalidation, max_size_pct, ai_rationale, status, created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open',%s) RETURNING id""",
                (coin, tier_key, direction, tf,
                 trade.get("entry"), trade.get("stop"), trade.get("target1"), trade.get("target2"),
                 trade.get("bias","Neutral"), trade.get("confidence","Moderate"), rr_str,
                 trade.get("invalidation"), TRADE_TIERS[tier_key]["max_size"],
                 ai_raw[:500] if ai_raw else "", now)
            )
        idea_id = c.fetchone()[0]
        db.commit()
        logger.info(f"[TRADE IDEAS] #{idea_id} saved — {coin} {tier_key} horizon={expected_horizon} until={valid_until}")
        return idea_id
    except Exception as e:
        logger.error(f"[TRADE IDEAS] Save error: {e}")
        if db:
            try: db.rollback()
            except Exception: pass
        return 0
    finally:
        if db:
            try: db.close()
            except Exception: pass



def mark_trade_publication(idea_id, status: str, reason: str | None = None) -> bool:
    """Set publication_status: PUBLISHED | SUPPRESSED | PUBLISH_FAILED | PENDING.

    Official performance uses only PUBLISHED.
    """
    if not idea_id:
        return False
    status = (status or "").upper().strip()
    if status not in ("PUBLISHED", "SUPPRESSED", "PUBLISH_FAILED", "PENDING"):
        logger.warning("[TRADE IDEAS] invalid publication_status %s", status)
        return False
    db = None
    try:
        db = get_db()
        c = db.cursor()
        try:
            c.execute("ALTER TABLE trade_ideas ADD COLUMN IF NOT EXISTS publication_status TEXT")
            c.execute("ALTER TABLE trade_ideas ADD COLUMN IF NOT EXISTS publication_reason TEXT")
        except Exception:
            pass
        c.execute(
            "UPDATE trade_ideas SET publication_status=%s, publication_reason=%s WHERE id=%s",
            (status, reason, int(idea_id)),
        )
        db.commit()
        logger.info("[TRADE IDEAS] #%s publication_status=%s reason=%s", idea_id, status, reason)
        return True
    except Exception as e:
        logger.error("[TRADE IDEAS] mark publication #%s: %s", idea_id, e)
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


def is_official_trade_clause(alias: str = "") -> str:
    """SQL fragment: only official published trades (legacy NULL = published)."""
    col = f"{alias}.publication_status" if alias else "publication_status"
    return f"(COALESCE({col}, 'PUBLISHED') = 'PUBLISHED')"


def build_trade_idea_message(coin, price, tier, trade, idea_id=0):
    tier = _normalize_tier(tier)
    if tier not in TRADE_TIERS:
        tier = "momentum"
    tier_cfg = TRADE_TIERS[tier]
    direction = (trade.get("direction") or "Long").lower()
    if direction not in ("long", "short"):
        direction = _infer_direction(trade.get("entry", ""), trade.get("stop", ""), trade.get("bias"))
    metrics = _calc_trade_metrics(
        trade.get("entry", ""), trade.get("stop", ""), trade.get("target1", ""),
        direction=direction,
    )
    created_s = trade.get("created_at") or wat_now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        from datetime import datetime as _dt
        _cd = _dt.strptime(str(created_s)[:19], "%Y-%m-%d %H:%M:%S")
        created_h = _cd.strftime("%d %b %Y · %H:%M") + " WAT"
    except Exception:
        created_h = f"{created_s} WAT"
    lines = [
        f"{tier_cfg['emoji']} <b>{tier_cfg['label']} #{idea_id}</b>",
        f"📅 <b>{created_h}</b>",
        f"<b>{coin}/USDT</b>  \u00b7  {trade.get('direction','Long').upper()}  \u00b7  {trade.get('timeframe','4H')}",
        f"<i>{tier_cfg['risk_desc']}</i>",
        "",
        f"\U0001f4b0 Current: <b>{format_price(price)}</b>",
        f"\U0001f4c8 Bias: <b>{trade.get('bias','Neutral')}</b>",
        "",
    ]

    # Time horizon (expectation — not a promise targets hit in this window)
    hz_label = trade.get("expected_horizon")
    vu = trade.get("valid_until")
    tf = trade.get("timeframe") or "1H"
    if not hz_label:
        try:
            hz_label = resolve_horizon(tf, None).get("expected_horizon")
        except Exception:
            hz_label = None
    if hz_label:
        lines += [
            f"⏱ <b>Horizon:</b> {tf} setup — {hz_label}",
            "<i>Expected holding window, not a guarantee that the target is hit in time.</i>",
        ]
        if vu:
            lines.append(f"Valid until: <b>{vu}</b> WAT")
        lines.append("")
    if trade.get("rationale"):
        rat = sanitize_ai_narrative(_strip_all_disclaimers(trade.get("rationale") or ""), fallback="")
        if rat:
            lines += ["📋 <b>SETUP</b>", rat, ""]
    if trade.get("ng_angle"):
        nga = sanitize_ai_narrative(_strip_all_disclaimers(trade.get("ng_angle") or ""), fallback="")
        if nga:
            lines += ["🇳🇬 <b>NIGERIAN ANGLE</b>", nga, ""]
    lines += ["· " * 18, ""]
    entry = trade.get("entry", "—")
    stop  = trade.get("stop", "—")
    t1    = trade.get("target1", "—")
    t2    = trade.get("target2")
    inv   = trade.get("invalidation", "—")
    conf  = trade.get("confidence", "Moderate")
    lines += [
        "📐 <b>LEVELS</b>",
        f"Entry:        <b>{entry}</b>",
        f"Stop Loss:    <b>{stop}</b>",
        f"Target 1:     <b>{t1}</b>",
    ]
    if t2:
        lines.append(f"Target 2:     <b>{t2}</b>  <i>(aggressive)</i>")
    lines += [f"Invalidation: <b>{inv}</b>", ""]
    if metrics:
        lines += [
            "📊 <b>RISK METRICS</b>",
            f"Risk:Reward:  <b>1 : {metrics['rr']}</b>",
            f"Stop Risk:    <b>-{metrics['risk_pct']:.1f}%</b>  (${metrics['pot_loss']:,.0f} per $1,000)",
            f"T1 Reward:    <b>+{metrics['reward_pct']:.1f}%</b>  (${metrics['pot_profit']:,.0f} per $1,000)",
            f"Confidence:   <b>{conf}</b>",
            f"Max Size:     <b>{tier_cfg['max_size']}</b>",
            "",
        ]
    if trade.get("management"):
        mgmt = _strip_all_disclaimers(trade.get("management") or "")
        if mgmt:
            lines += [
                "🛡 <b>TRADE MANAGEMENT</b>",
                mgmt,
                "",
            ]
    lines += ["· " * 18, ""]
    body = "\n".join(lines)
    return _finalize_trade_message(body, tier)




def generate_trade_idea(coin, tier="momentum"):
    """Full pipeline: gather analytics → pre-screen → AI → parse → validate → save → return."""
    try:
        tier = _normalize_tier(tier)
        price, _ = get_best_price(coin)
        if not price:
            return None, None, 0
        sd      = get_secondary_coin(coin)
        fg_data = get_fear_greed()
        fg_val  = fg_data[0]["value"] if fg_data else "50"
        buy, sell, _ = get_p2p_rate("USDT", "NGN")
        p2p_str = f"USDT/NGN Buy \u20a6{int(buy):,} / Sell \u20a6{int(sell):,}" if buy else "N/A"

        # Gather rich market analytics from price history
        analytics = _gather_trade_analytics(coin, price)

        # Pre-screen: check if market conditions support this tier
        ok, reason = _tier_conditions_met(tier, analytics, fg_val)
        if not ok:
            logger.info(f"[TRADE ENGINE] {coin} {tier} pre-screened out: {reason}")
            return None, None, 0

        # ── Python owns levels for all tiers (ATR + structure). AI explains only.
        if tier in ("steady", "momentum", "edge"):
            setup = build_programmatic_setup(coin, price, tier=tier)
            if not setup:
                logger.info(f"[TRADE ENGINE] {coin} {tier} — setup_engine found no edge")
                return None, None, 0
            try:
                news = news_market_flag(coin)
                explain_prompt = append_narrative_rules(
                    f"{coin} {tier.upper()} setup already computed by rules.\n"
                    f"Direction: {setup['direction']} Entry: {setup['entry']} "
                    f"Stop: {setup['stop']} TP1: {setup['target1']} TP2: {setup['target2']}\n"
                    f"Reasons: {setup.get('rationale')}\n"
                    f"News flag: {news.get('flag')}\n"
                    f"Write 2 short technical sentences about price vs levels only.\n"
                    f"Do NOT invent P2P/naira causality, whales, or news.\n"
                    f"Do NOT change any prices, R:R, or invent new levels."
                )
                ai_raw, _ = ask_ai(explain_prompt)
                if ai_raw:
                    ai_clean = sanitize_ai_narrative(ai_raw.strip()[:500], fallback="")
                    if ai_clean:
                        setup["rationale"] = (ai_clean + "\n\n" + (setup.get("rationale") or "")).strip()
            except Exception as _e:
                logger.debug(f"[TRADE ENGINE] {tier} narrative skip: {_e}")
            direction = setup.get("direction", "Long").lower()
            valid, reason = _validate_alert(
                coin, price,
                setup.get("entry", ""), setup.get("stop", ""), setup.get("target1", ""),
                tier, direction=direction,
            )
            if not valid:
                logger.warning(f"[TRADE ENGINE] {coin} {tier} validation failed: {reason}")
                return None, None, 0
            idea_id = save_trade_idea(coin, tier, setup, ai_raw=setup.get("rationale", ""))
            msg = build_trade_idea_message(coin, price, tier, setup, idea_id)
            note = (
                "\n\n<i>Levels from setup engine (structure + ATR). "
                "AI text is explanation only. Management is guidance only.</i>"
            )
            core = _strip_all_disclaimers(msg)
            msg = _finalize_trade_message(core + note, tier)
            return msg, setup, idea_id

        # Fallback unknown tier
        prompt  = _build_trade_ai_prompt(coin, price, tier, sd, fg_val, p2p_str, analytics)
        ai_raw, _ = ask_ai(prompt)
        if not ai_raw:
            return None, None, 0
        trade = _parse_trade_idea(ai_raw, price)
        if not trade or not trade.get("entry"):
            return None, None, 0
        if trade["entry"] and trade["entry"].lower() in ("$none", "none"):
            logger.info(f"[TRADE ENGINE] {coin} {tier} — AI found no quality setup")
            return None, None, 0
        if not trade.get("stop") or not trade.get("target1"):
            logger.warning(f"[TRADE ENGINE] {coin} {tier} — missing stop or target1 after parse")
            return None, None, 0
        direction = (trade.get("direction") or "Long").lower()
        if direction not in ("long", "short"):
            direction = _infer_direction(trade.get("entry"), trade.get("stop"), trade.get("bias"))
            trade["direction"] = direction.capitalize()
        valid, reason = _validate_alert(
            coin, price,
            trade.get("entry", ""), trade.get("stop", ""), trade.get("target1", ""),
            tier, direction=direction,
        )
        if not valid:
            logger.warning(f"[TRADE ENGINE] {coin} {tier} validation failed: {reason}")
            return None, None, 0
        idea_id = save_trade_idea(coin, tier, trade, ai_raw)
        analytics_str = _analytics_to_str(analytics) if analytics else ""
        msg = build_trade_idea_message(coin, price, tier, trade, idea_id)
        if analytics_str and "Insufficient" not in analytics_str:
            msg += f"\n\n<i>\U0001f4ca Data: {analytics_str}</i>"
        return msg, trade, idea_id
    except Exception as e:
        logger.error(f"[TRADE ENGINE] {coin} {tier}: {e}")
        return None, None, 0


def get_trade_history(limit=10, coin=None, tier=None):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        filters, params = [], []
        if coin:
            filters.append("coin=%s"); params.append(coin)
        if tier:
            filters.append("tier=%s"); params.append(tier)
        where = ("WHERE " + " AND ".join(filters)) if filters else ""
        params.append(limit)
        c.execute(
            f"SELECT id, coin, tier, direction, timeframe, entry, target1, confidence, status, created_at "
            f"FROM trade_ideas {where} ORDER BY id DESC LIMIT %s", params
        )
        return c.fetchall()
    except Exception as e:
        logger.error(f"[TRADE HISTORY] {e}")
        return []
    finally:
        if db:
            try: db.close()
            except Exception: pass


def close_trade_idea(idea_id, result):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "UPDATE trade_ideas SET status='closed', closed_at=%s, result=%s WHERE id=%s",
            (now, result, idea_id)
        )
        db.commit()
        return True
    except Exception as e:
        logger.error(f"[CLOSE TRADE] {e}")
        if db:
            try: db.rollback()
            except Exception: pass
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass




def check_user_price_alerts():
    """Check all active user-set price alerts. Batch-deactivates triggered alerts."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT id, chat, coin, condition, target, label FROM alerts WHERE active=1")
        rows = c.fetchall()
    except Exception as e:
        logger.error(f"[PRICE ALERTS LOAD] {e}")
        return
    finally:
        if db:
            try: db.close()
            except Exception: pass

    triggered_ids = []
    for row in rows:
        aid, chat_id, coin, condition, target, label = row
        try:
            price, _ = get_best_price(coin)
            if not price:
                continue
            fired = (condition == "above" and price >= target) or                     (condition == "below" and price <= target)
            if fired:
                lbl = f" ({label})" if label else ""
                arrow = "📈" if condition == "above" else "📉"
                msg = (
                    f"🔔 <b>PRICE ALERT TRIGGERED</b>\n\n"
                    f"{arrow} <b>{coin}</b> is now <b>{condition}</b> your target{lbl}\n"
                    f"💰 Current: <b>{format_price(price)}</b>\n"
                    f"🎯 Target: <b>{format_price(target)}</b>\n\n"
                    f"<i>NFA - DYOR</i>"
                )
                send(int(chat_id), msg)
                triggered_ids.append(aid)
                logger.info(f"[PRICE ALERT] {coin} {condition} {target} triggered for {chat_id}")
        except Exception as e:
            logger.error(f"[PRICE ALERT] {coin} for {chat_id}: {e}")

    # Batch-deactivate all triggered alerts in one query
    if triggered_ids:
        db2 = None
        try:
            db2 = get_db()
            c2 = db2.cursor()
            c2.execute("UPDATE alerts SET active=0 WHERE id = ANY(%s)", (triggered_ids,))
            db2.commit()
        except Exception as e:
            logger.error(f"[PRICE ALERT DEACTIVATE] {e}")
            if db2:
                try: db2.rollback()
                except Exception: pass
        finally:
            if db2:
                try: db2.close()
                except Exception: pass



def check_watchlist_alerts():
    """Single-query watchlist check — no N+1 pattern."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT chat, coin FROM watchlists ORDER BY chat")
        rows = c.fetchall()
    except Exception as e:
        logger.error("[WATCHLIST ALERT ERROR] %s" % e)
        return
    finally:
        if db:
            try: db.close()
            except Exception: pass

    from collections import defaultdict
    watchlists = defaultdict(list)
    for chat_id, coin in rows:
        watchlists[chat_id].append(coin)

    for chat_id, coins in watchlists.items():
        for coin in coins:
            try:
                price, change = get_best_price(coin)
                if price and change and abs(change) > 5:
                    direction = "🚀 UP" if change > 0 else "🔴 DOWN"
                    send(chat_id, (
                        f"🔔 <b>Watchlist Alert</b>\n\n"
                        f"{coin} is {direction} <b>{abs(change):.2f}%</b>\n"
                        f"Current: {format_price(price)}\n\n"
                        f"<i>NFA - DYOR</i>"
                    ))
            except Exception as e:
                logger.error(f"[WATCHLIST ALERT] {coin} for {chat_id}: {e}")

# ═══════════════════════════════════════════════════════════════════════════
