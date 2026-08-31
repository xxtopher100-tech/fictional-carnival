"""MarketPulse Macro Event Scanner — publication risk gate only.

Does NOT modify Entry/SL/TP/ATR/tier/strategy math.
Does NOT auto-close open trades.
Does NOT predict direction.

Primary clock: scheduled economic calendar (not RSS).
RSS news_market_flag remains a separate reactive supplement.

INITIAL CONFIGURATION windows are tunable via env and must be validated
against the trade ledger before treating them as optimal.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from market_pulse.config_runtime import logger
from market_pulse.helpers import wat_now

# ── Env / INITIAL CONFIGURATION (not scientifically proven) ─────────────────
MACRO_SHADOW_MODE = os.environ.get("MACRO_SHADOW_MODE", "true").lower() in ("1", "true", "yes")
MACRO_ENABLED = os.environ.get("MACRO_ENABLED", "true").lower() in ("1", "true", "yes")
# When calendar missing: normal | elevated | block  (never silent "all clear")
MACRO_UNAVAILABLE_POLICY = os.environ.get("MACRO_UNAVAILABLE_POLICY", "elevated").lower()

# Windows in minutes — INITIAL CONFIGURATION
MACRO_CRITICAL_PRE_MIN = int(os.environ.get("MACRO_CRITICAL_PRE_MIN", "120"))
MACRO_CRITICAL_POST_MIN = int(os.environ.get("MACRO_CRITICAL_POST_MIN", "60"))
MACRO_HIGH_PRE_MIN = int(os.environ.get("MACRO_HIGH_PRE_MIN", "60"))
MACRO_HIGH_POST_MIN = int(os.environ.get("MACRO_HIGH_POST_MIN", "30"))
MACRO_MEDIUM_PRE_MIN = int(os.environ.get("MACRO_MEDIUM_PRE_MIN", "30"))
MACRO_MEDIUM_POST_MIN = int(os.environ.get("MACRO_MEDIUM_POST_MIN", "15"))
# Event "live" window around scheduled time
MACRO_EVENT_WINDOW_MIN = int(os.environ.get("MACRO_EVENT_WINDOW_MIN", "15"))

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
MACRO_CALENDAR_JSON_URL = os.environ.get("MACRO_CALENDAR_JSON_URL", "").strip()
MACRO_EVENTS_JSON_PATH = os.environ.get("MACRO_EVENTS_JSON_PATH", "").strip()

_cache = {"events": None, "fetched_at": 0.0, "source": None, "error": None}
_CACHE_TTL = 1800  # 30 min


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_utc(ts: Any) -> Optional[datetime]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    s = str(ts).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        if "T" in s:
            dt = datetime.fromisoformat(s.replace(" ", "T")[:25])
        else:
            dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _event_id(name: str, when: datetime, country: str = "US") -> str:
    raw = f"{country}|{name}|{when.strftime('%Y-%m-%dT%H:%M')}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def _classify_importance(name: str, impact: str | None = None) -> str:
    n = (name or "").lower()
    imp = (impact or "").lower()
    if any(k in n for k in ("fomc", "fed rate", "federal funds", "rate decision", "ecb rate", "boe rate", "boj rate")):
        if "minute" in n:
            return "MEDIUM"
        return "CRITICAL"
    if any(k in n for k in ("nonfarm", "non-farm", "nfp", "payroll")):
        return "HIGH"
    if "unemployment" in n and "claim" not in n:
        return "HIGH"
    if re.search(r"\bcpi\b", n) or "consumer price" in n:
        return "HIGH"
    if re.search(r"\bpce\b", n) or "personal consumption" in n:
        return "HIGH"
    if "gdp" in n:
        return "MEDIUM"
    if imp in ("3", "high", "red"):
        return "HIGH"
    if imp in ("2", "medium", "orange"):
        return "MEDIUM"
    if imp in ("1", "low", "yellow"):
        return "MEDIUM"  # still track
    return "MEDIUM"


def _asset_relevance(symbol: str, event: dict) -> str:
    """Simple deterministic relevance — not ML."""
    sym = (symbol or "").upper()
    cur = (event.get("currency") or "USD").upper()
    country = (event.get("country") or "").upper()
    base = sym.split("/")[0]
    quote = sym.split("/")[1] if "/" in sym else ""

    crypto = {"BTC", "ETH", "SOL", "BNB", "XRP", "AVAX", "LINK", "DOGE"}
    if base in crypto or sym in crypto:
        if cur == "USD" or country in ("US", "USA"):
            return "HIGH"
        return "MEDIUM"

    if "/" in sym:
        if cur and (cur == base or cur == quote):
            return "HIGH"
        if cur == "USD" and ("USD" in sym or "USDT" in sym):
            return "HIGH"
        if cur in ("EUR", "GBP") and cur in sym:
            return "HIGH"
        return "LOW"
    return "MEDIUM"


def _normalize_event(raw: dict, source: str) -> Optional[dict]:
    name = (
        raw.get("event_name")
        or raw.get("event")
        or raw.get("title")
        or raw.get("name")
        or ""
    ).strip()
    if not name:
        return None
    when = _parse_utc(
        raw.get("scheduled_time_utc")
        or raw.get("scheduled_at")
        or raw.get("time")
        or raw.get("date")
        or raw.get("datetime")
    )
    if when is None:
        return None
    country = (raw.get("country") or "US").upper()
    currency = (raw.get("currency") or "USD").upper()
    impact = raw.get("importance") or raw.get("impact") or raw.get("impact_level")
    importance = _classify_importance(name, str(impact) if impact is not None else None)
    eid = raw.get("event_id") or _event_id(name, when, country)
    status = "released" if raw.get("actual") not in (None, "") else "scheduled"
    return {
        "event_id": str(eid),
        "event_name": name,
        "country": country,
        "currency": currency,
        "scheduled_time_utc": when,
        "importance": importance,
        "source": source,
        "status": status,
        "actual": raw.get("actual"),
    }


def _fetch_finnhub(now: datetime) -> list:
    if not FINNHUB_API_KEY:
        return []
    try:
        import requests
        start = (now - timedelta(days=2)).strftime("%Y-%m-%d")
        end = (now + timedelta(days=14)).strftime("%Y-%m-%d")
        url = "https://finnhub.io/api/v1/calendar/economic"
        r = requests.get(
            url,
            params={"token": FINNHUB_API_KEY, "from": start, "to": end},
            timeout=12,
        )
        if r.status_code != 200:
            logger.warning("[MACRO] Finnhub HTTP %s", r.status_code)
            return []
        data = r.json() or {}
        rows = data.get("economicCalendar") or data.get("economic_calendar") or []
        out = []
        for row in rows:
            # Finnhub: time is often "2026-08-29 12:30:00"
            ev = _normalize_event(
                {
                    "event": row.get("event"),
                    "country": row.get("country"),
                    "currency": row.get("currency") or "USD",
                    "time": row.get("time"),
                    "impact": row.get("impact"),
                    "actual": row.get("actual"),
                },
                "finnhub",
            )
            if ev:
                out.append(ev)
        return out
    except Exception as e:
        logger.warning("[MACRO] Finnhub fetch error: %s", e)
        return []


def _fetch_json_url() -> list:
    if not MACRO_CALENDAR_JSON_URL:
        return []
    try:
        import requests
        r = requests.get(MACRO_CALENDAR_JSON_URL, timeout=12)
        if r.status_code != 200:
            return []
        data = r.json()
        rows = data if isinstance(data, list) else (data.get("events") or [])
        out = []
        for row in rows:
            ev = _normalize_event(row, "json_url")
            if ev:
                out.append(ev)
        return out
    except Exception as e:
        logger.warning("[MACRO] JSON URL fetch error: %s", e)
        return []


def _fetch_json_path() -> list:
    if not MACRO_EVENTS_JSON_PATH or not os.path.isfile(MACRO_EVENTS_JSON_PATH):
        return []
    try:
        with open(MACRO_EVENTS_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        rows = data if isinstance(data, list) else (data.get("events") or [])
        out = []
        for row in rows:
            ev = _normalize_event(row, "json_file")
            if ev:
                out.append(ev)
        return out
    except Exception as e:
        logger.warning("[MACRO] JSON file error: %s", e)
        return []


def load_macro_events(force: bool = False) -> tuple[list, str, Optional[str]]:
    """Return (events, source, error). error set on total failure."""
    now_m = time.time()
    if (
        not force
        and _cache["events"] is not None
        and (now_m - float(_cache["fetched_at"] or 0)) < _CACHE_TTL
    ):
        return list(_cache["events"] or []), str(_cache["source"] or ""), _cache.get("error")

    events: list = []
    source = "none"
    err = None

    for fetcher, label in (
        (_fetch_finnhub, "finnhub"),
        (_fetch_json_url, "json_url"),
        (_fetch_json_path, "json_file"),
    ):
        try:
            if label == "finnhub":
                batch = fetcher(_utc_now())
            else:
                batch = fetcher()
        except Exception as e:
            batch = []
            logger.debug("[MACRO] provider %s: %s", label, e)
        if batch:
            events = batch
            source = label
            break

    if not events:
        err = "MACRO_DATA_UNAVAILABLE"
        logger.warning("[MACRO] MACRO_DATA_UNAVAILABLE — no calendar provider returned events")
        source = "unavailable"

    # Dedupe by event_id
    seen = set()
    uniq = []
    for ev in events:
        eid = ev.get("event_id")
        if eid in seen:
            continue
        seen.add(eid)
        uniq.append(ev)

    _cache["events"] = uniq
    _cache["fetched_at"] = now_m
    _cache["source"] = source
    _cache["error"] = err
    return uniq, source, err


def _windows_for(importance: str) -> tuple[int, int]:
    imp = (importance or "MEDIUM").upper()
    if imp == "CRITICAL":
        return MACRO_CRITICAL_PRE_MIN, MACRO_CRITICAL_POST_MIN
    if imp == "HIGH":
        return MACRO_HIGH_PRE_MIN, MACRO_HIGH_POST_MIN
    return MACRO_MEDIUM_PRE_MIN, MACRO_MEDIUM_POST_MIN


def evaluate_macro_for_asset(symbol: str, now_utc: Optional[datetime] = None) -> dict:
    """Compute macro risk state for one symbol. Never mutates trade levels.

    Returns dict:
      macro_state: NORMAL | ELEVATED | BLOCK
      macro_event_id, macro_event_name, macro_importance
      macro_minutes_to_event
      macro_would_block: bool
      macro_data_available: bool
      macro_source
      asset_relevance
      shadow_mode
    """
    if not MACRO_ENABLED:
        return {
            "macro_state": "NORMAL",
            "macro_event_id": None,
            "macro_event_name": None,
            "macro_importance": None,
            "macro_minutes_to_event": None,
            "macro_would_block": False,
            "macro_data_available": True,
            "macro_source": "disabled",
            "asset_relevance": "LOW",
            "shadow_mode": MACRO_SHADOW_MODE,
        }

    now = now_utc or _utc_now()
    events, source, err = load_macro_events()

    if not events:
        # Safe fallback — never silent "all clear" (missing calendar ≠ no risk)
        err = err or "MACRO_DATA_UNAVAILABLE"
        policy = MACRO_UNAVAILABLE_POLICY
        state = "NORMAL"
        if policy == "block":
            state = "BLOCK"
        elif policy in ("elevated", "elevate"):
            state = "ELEVATED"
        would_block = state == "BLOCK"
        logger.info("[MACRO] state=%s reason=MACRO_DATA_UNAVAILABLE policy=%s", state, policy)
        return {
            "macro_state": state,
            "macro_event_id": None,
            "macro_event_name": "MACRO_DATA_UNAVAILABLE",
            "macro_importance": None,
            "macro_minutes_to_event": None,
            "macro_would_block": would_block,
            "macro_data_available": False,
            "macro_source": source,
            "asset_relevance": "HIGH",
            "shadow_mode": MACRO_SHADOW_MODE,
        }

    best = None  # highest severity active window
    severity_rank = {"BLOCK": 3, "ELEVATED": 2, "NORMAL": 1}

    for ev in events:
        when = ev.get("scheduled_time_utc")
        if not isinstance(when, datetime):
            continue
        rel = _asset_relevance(symbol, ev)
        if rel == "LOW":
            continue

        pre, post = _windows_for(ev.get("importance") or "MEDIUM")
        # Tighter windows for MEDIUM relevance
        if rel == "MEDIUM":
            pre = max(15, pre // 2)
            post = max(10, post // 2)

        delta_min = (when - now).total_seconds() / 60.0
        # Inside pre-event
        in_pre = 0 <= delta_min <= pre
        # Event window around scheduled time
        in_event = abs(delta_min) <= MACRO_EVENT_WINDOW_MIN
        # Post-event
        in_post = (-post) <= delta_min < 0 and not in_event

        imp = (ev.get("importance") or "MEDIUM").upper()
        state = "NORMAL"
        if in_pre or in_event or in_post:
            if imp == "CRITICAL" and (in_pre or in_event or in_post):
                # BLOCK only for HIGH relevance + CRITICAL in pre/event; post = ELEVATED or BLOCK
                if rel == "HIGH" and (in_pre or in_event):
                    state = "BLOCK"
                elif rel == "HIGH":
                    state = "BLOCK" if in_post and post >= 30 else "ELEVATED"
                else:
                    state = "ELEVATED"
            elif imp == "HIGH" and (in_pre or in_event or in_post):
                if rel == "HIGH" and (in_pre or in_event):
                    state = "BLOCK"
                else:
                    state = "ELEVATED"
            elif imp == "MEDIUM" and (in_pre or in_event):
                state = "ELEVATED"

        if state == "NORMAL":
            continue

        cand = {
            "macro_state": state,
            "macro_event_id": ev.get("event_id"),
            "macro_event_name": ev.get("event_name"),
            "macro_importance": imp,
            "macro_minutes_to_event": round(delta_min, 1),
            "macro_would_block": state == "BLOCK",
            "macro_data_available": True,
            "macro_source": source,
            "asset_relevance": rel,
            "shadow_mode": MACRO_SHADOW_MODE,
        }
        if best is None or severity_rank[state] > severity_rank[best["macro_state"]]:
            best = cand
        elif best and severity_rank[state] == severity_rank[best["macro_state"]]:
            # nearer event wins
            if abs(delta_min) < abs(best.get("macro_minutes_to_event") or 1e9):
                best = cand

    if not best:
        return {
            "macro_state": "NORMAL",
            "macro_event_id": None,
            "macro_event_name": None,
            "macro_importance": None,
            "macro_minutes_to_event": None,
            "macro_would_block": False,
            "macro_data_available": True,
            "macro_source": source,
            "asset_relevance": "LOW",
            "shadow_mode": MACRO_SHADOW_MODE,
        }

    logger.info(
        "[MACRO] %s state=%s event=%s imp=%s mins=%s would_block=%s shadow=%s",
        symbol,
        best["macro_state"],
        best.get("macro_event_name"),
        best.get("macro_importance"),
        best.get("macro_minutes_to_event"),
        best.get("macro_would_block"),
        MACRO_SHADOW_MODE,
    )
    return best


def apply_macro_publication_gate(item: dict) -> dict:
    """Attach macro fields to a ranked candidate; set suppress flags if enforcing BLOCK.

    item must include identifier / idea_id. Does not change trade levels.
    """
    symbol = item.get("identifier") or item.get("symbol") or ""
    m = evaluate_macro_for_asset(symbol)
    item["macro_state"] = m.get("macro_state")
    item["macro_event_id"] = m.get("macro_event_id")
    item["macro_event_name"] = m.get("macro_event_name")
    item["macro_importance"] = m.get("macro_importance")
    item["macro_minutes_to_event"] = m.get("macro_minutes_to_event")
    item["macro_would_block"] = bool(m.get("macro_would_block"))
    item["macro_data_available"] = m.get("macro_data_available")
    item["macro_shadow_mode"] = m.get("shadow_mode")

    # Re-read module global so tests / runtime toggles apply
    import market_pulse.macro_event_scanner as _self
    shadow = bool(getattr(_self, "MACRO_SHADOW_MODE", True))
    item["macro_shadow_mode"] = shadow
    enforce = (not shadow) and bool(m.get("macro_would_block"))
    item["macro_enforce_block"] = bool(enforce)
    return item
