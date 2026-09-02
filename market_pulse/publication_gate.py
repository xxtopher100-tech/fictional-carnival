"""Central publication gate — signal identity, fingerprint, cross-tier dedup, immutability.

All Pro trade posts should go through publish_canonical_trade().
Does not change entry/stop/TP math.
"""
from __future__ import annotations

import hashlib
import re
import time
from typing import Any, Optional, Tuple

try:
    from market_pulse.config_runtime import logger
except Exception:
    import logging
    logger = logging.getLogger("publication_gate")

try:
    from market_pulse.db import get_db
except Exception:
    get_db = None  # type: ignore

# post_to_pro_channel imported lazily inside publish_canonical_trade


def _norm_level(v: Any, *, decimals: int = 4) -> str:
    """Deterministic level string — no raw float equality."""
    if v is None or v == "" or v == "—":
        return ""
    try:
        x = float(str(v).replace(",", "").replace("$", "").replace("₦", "").strip())
    except Exception:
        return re.sub(r"\s+", "", str(v).lower())[:32]
    if abs(x) >= 1000:
        return f"{x:.2f}"
    if abs(x) >= 1:
        return f"{x:.{min(decimals, 4)}f}".rstrip("0").rstrip(".")
    return f"{x:.6f}".rstrip("0").rstrip(".")


def signal_fingerprint(
    *,
    market_type: str,
    symbol: str,
    direction: str,
    timeframe: str,
    entry: Any,
    stop: Any,
    target1: Any,
    thesis: str = "",
) -> str:
    """Deterministic identity of a setup (not tier-specific)."""
    d = (direction or "").lower()
    if "long" in d or "buy" in d:
        d_norm = "long"
    elif "short" in d or "sell" in d:
        d_norm = "short"
    else:
        d_norm = d.strip() or "unknown"
    parts = [
        (market_type or "crypto").lower()[:8],
        (symbol or "").upper().replace(" ", ""),
        d_norm,
        (timeframe or "").upper().replace(" ", "")[:8],
        _norm_level(entry),
        _norm_level(stop),
        _norm_level(target1),
        (thesis or "").lower()[:24],
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def public_signal_id(idea_id: int, market_type: str = "crypto") -> str:
    """Human-facing immutable id. Never reused (tied to idea_id)."""
    prefix = "MP-F" if (market_type or "").lower() == "forex" else "MP-C"
    return f"{prefix}-{int(idea_id):04d}"


def _ensure_gate_schema(c) -> None:
    for col, typ in (
        ("signal_fingerprint", "TEXT"),
        ("public_signal_id", "TEXT"),
        ("idempotency_key", "TEXT"),
        ("published_at", "TEXT"),
        ("levels_frozen", "TEXT"),
    ):
        try:
            c.execute(f"ALTER TABLE trade_ideas ADD COLUMN IF NOT EXISTS {col} {typ}")
        except Exception:
            pass
    try:
        c.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_ideas_idempotency "
            "ON trade_ideas (idempotency_key) WHERE idempotency_key IS NOT NULL"
        )
    except Exception:
        pass
    try:
        c.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_ideas_fingerprint "
            "ON trade_ideas (signal_fingerprint)"
        )
    except Exception:
        pass


def classify_against_active(
    *,
    market_type: str,
    symbol: str,
    direction: str,
    timeframe: str,
    entry: Any,
    stop: Any,
    target1: Any,
    idea_id: Optional[int] = None,
) -> Tuple[str, Optional[int]]:
    """Return (NEW_SIGNAL|DUPLICATE_ACTIVE|SIMILAR_ACTIVE|CROSS_TIER_DUPLICATE, existing_id)."""
    fp = signal_fingerprint(
        market_type=market_type,
        symbol=symbol,
        direction=direction,
        timeframe=timeframe,
        entry=entry,
        stop=stop,
        target1=target1,
    )
    if not get_db:
        return "NEW_SIGNAL", None
    db = None
    try:
        db = get_db()
        c = db.cursor()
        _ensure_gate_schema(c)
        # Exact fingerprint among open or published
        c.execute(
            """
            SELECT id, tier, COALESCE(publication_status,''), COALESCE(status,'open')
            FROM trade_ideas
            WHERE signal_fingerprint = %s
              AND (status = 'open' OR COALESCE(publication_status,'') = 'PUBLISHED')
            ORDER BY id DESC LIMIT 5
            """,
            (fp,),
        )
        rows = c.fetchall() or []
        for row in rows:
            eid, tier, pub, st = row
            if idea_id and int(eid) == int(idea_id):
                continue
            if (pub or "").upper() == "PUBLISHED" or (st or "") == "open":
                return "DUPLICATE_ACTIVE", int(eid)

        # Cross-tier: same symbol+direction+normalized levels, any tier
        d = (direction or "").lower()
        d_like = "%long%" if ("long" in d or "buy" in d) else (
            "%short%" if ("short" in d or "sell" in d) else f"%{d}%"
        )
        c.execute(
            """
            SELECT id, entry, stop, target1, tier, COALESCE(publication_status,''), status
            FROM trade_ideas
            WHERE UPPER(coin) = UPPER(%s)
              AND status = 'open'
              AND (direction ILIKE %s)
            ORDER BY id DESC LIMIT 15
            """,
            (symbol, d_like),
        )
        e_n, s_n, t_n = _norm_level(entry), _norm_level(stop), _norm_level(target1)
        for row in c.fetchall() or []:
            eid, e2, s2, t2, tier, pub, st = row
            if idea_id and int(eid) == int(idea_id):
                continue
            if _norm_level(e2) == e_n and _norm_level(s2) == s_n and _norm_level(t2) == t_n:
                return "CROSS_TIER_DUPLICATE", int(eid)
            # Similar entry within ~1.5%
            try:
                a, b = float(e_n or 0), float(_norm_level(e2) or 0)
                if a > 0 and b > 0 and abs(a - b) / a * 100 <= 1.5:
                    return "SIMILAR_ACTIVE", int(eid)
            except Exception:
                pass
        return "NEW_SIGNAL", None
    except Exception as e:
        logger.warning("[PUB GATE] classify: %s", e)
        return "NEW_SIGNAL", None
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def attach_fingerprint(idea_id: int, **kwargs) -> str:
    """Persist fingerprint + public id on the idea row (idempotent)."""
    fp = signal_fingerprint(**kwargs)
    pub_id = public_signal_id(idea_id, kwargs.get("market_type") or "crypto")
    if not get_db or not idea_id:
        return fp
    db = None
    try:
        db = get_db()
        c = db.cursor()
        _ensure_gate_schema(c)
        c.execute(
            """
            UPDATE trade_ideas
            SET signal_fingerprint = COALESCE(signal_fingerprint, %s),
                public_signal_id = COALESCE(public_signal_id, %s)
            WHERE id = %s
            """,
            (fp, pub_id, int(idea_id)),
        )
        db.commit()
    except Exception as e:
        logger.debug("[PUB GATE] attach_fp: %s", e)
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
    return fp


def freeze_published_levels(idea_id: int) -> None:
    """Mark levels immutable after successful publish — never overwrite financial fields."""
    if not get_db or not idea_id:
        return
    db = None
    try:
        db = get_db()
        c = db.cursor()
        _ensure_gate_schema(c)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """
            UPDATE trade_ideas
            SET levels_frozen = '1',
                published_at = COALESCE(published_at, %s),
                publication_status = 'PUBLISHED'
            WHERE id = %s
            """,
            (now, int(idea_id)),
        )
        db.commit()
    except Exception as e:
        logger.debug("[PUB GATE] freeze: %s", e)
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def try_claim_idempotency(idea_id: int, idem_key: str) -> bool:
    """Atomic claim. Returns False if this publication was already claimed."""
    if not get_db or not idea_id or not idem_key:
        return True
    db = None
    try:
        db = get_db()
        c = db.cursor()
        _ensure_gate_schema(c)
        # Already published?
        c.execute(
            "SELECT COALESCE(publication_status,''), COALESCE(idempotency_key,'') FROM trade_ideas WHERE id=%s",
            (int(idea_id),),
        )
        row = c.fetchone()
        if row and (row[0] or "").upper() == "PUBLISHED":
            return False
        if row and row[1] == idem_key:
            return False
        c.execute(
            """
            UPDATE trade_ideas
            SET idempotency_key = %s
            WHERE id = %s
              AND (idempotency_key IS NULL OR idempotency_key = '' OR idempotency_key = %s)
              AND COALESCE(publication_status,'') IS DISTINCT FROM 'PUBLISHED'
            RETURNING id
            """,
            (idem_key, int(idea_id), idem_key),
        )
        claimed = c.fetchone() is not None
        db.commit()
        return claimed
    except Exception as e:
        logger.warning("[PUB GATE] claim: %s", e)
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


def publish_canonical_trade(
    *,
    msg: str,
    idea_id: int,
    symbol: str,
    direction: str,
    timeframe: str = "",
    entry: Any = None,
    stop: Any = None,
    target1: Any = None,
    market_type: str = "crypto",
    tier: str = "",
    source: str = "scanner",
) -> Tuple[bool, str]:
    """Single publication gate for all paths.

    Returns (ok, reason_code).
    """
    if not msg or not idea_id:
        return False, "MISSING_MSG_OR_ID"

    fp = attach_fingerprint(
        int(idea_id),
        market_type=market_type,
        symbol=symbol,
        direction=direction,
        timeframe=timeframe,
        entry=entry,
        stop=stop,
        target1=target1,
    )
    cls, exist_id = classify_against_active(
        market_type=market_type,
        symbol=symbol,
        direction=direction,
        timeframe=timeframe,
        entry=entry,
        stop=stop,
        target1=target1,
        idea_id=int(idea_id),
    )
    if cls in ("DUPLICATE_ACTIVE", "CROSS_TIER_DUPLICATE", "SIMILAR_ACTIVE"):
        try:
            from market_pulse.edge_trade_engine import mark_trade_publication
            mark_trade_publication(idea_id, "SUPPRESSED", f"{cls}:{exist_id}")
        except Exception:
            pass
        logger.info(
            "[PUB GATE] SUPPRESSED #%s %s as %s (existing #%s) source=%s",
            idea_id, symbol, cls, exist_id, source,
        )
        return False, cls

    idem = f"pub:{fp}:{int(idea_id)}"
    if not try_claim_idempotency(int(idea_id), idem):
        logger.info("[PUB GATE] IDEMPOTENT skip #%s %s", idea_id, symbol)
        return False, "IDEMPOTENT_SKIP"

    # Prefix public id once
    pub_id = public_signal_id(int(idea_id), market_type)
    body = msg
    if pub_id not in body:
        body = f"<b>{pub_id}</b>\n" + body

    try:
        from market_pulse.telegram_api import post_to_pro_channel
        post_to_pro_channel(body)
    except Exception as e:
        logger.error("[PUB GATE] Telegram failed #%s: %s", idea_id, e)
        try:
            from market_pulse.edge_trade_engine import mark_trade_publication
            mark_trade_publication(idea_id, "PUBLISH_FAILED", str(e)[:120])
        except Exception:
            pass
        return False, "TELEGRAM_ERROR"

    freeze_published_levels(int(idea_id))
    try:
        from market_pulse.edge_trade_engine import mark_trade_publication
        mark_trade_publication(idea_id, "PUBLISHED", f"GATE:{source}")
    except Exception:
        pass
    logger.info(
        "[PUB GATE] PUBLISHED %s #%s %s %s source=%s fp=%s",
        pub_id, idea_id, symbol, direction, source, fp,
    )
    return True, "PUBLISHED"


def ensure_confidence(text: str, confidence: str = "Moderate") -> str:
    """Prevent 'Confidence none' vs Moderate inconsistency in free text."""
    if not text:
        return text
    conf = (confidence or "Moderate").strip() or "Moderate"
    out = re.sub(
        r"(?i)Confidence\s*:\s*(none|n/a|null|—|-)\b",
        f"Confidence: {conf}",
        text,
    )
    return out
