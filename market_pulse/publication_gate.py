"""Central publication gate — signal identity, fingerprint, cross-tier dedup, immutability.

All Pro trade posts should go through publish_canonical_trade().
Does not change entry/stop/TP math.
"""
from __future__ import annotations

import hashlib
import json
import os
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


# Burst spacing — NOT a daily quota. Configurable via Railway env.
NORMAL_PUBLICATION_COOLDOWN_SECONDS = int(
    os.environ.get("NORMAL_PUBLICATION_COOLDOWN_SECONDS", "600")
)
_LAST_PUB_KEY = "pub_gate_last_publish_ts"


def get_publication_cooldown_sec() -> int:
    return max(0, int(NORMAL_PUBLICATION_COOLDOWN_SECONDS))


def _get_last_publish_ts() -> float:
    if not get_db:
        return 0.0
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT value FROM admin_settings WHERE key=%s", (_LAST_PUB_KEY,))
        row = c.fetchone()
        if not row or row[0] is None:
            return 0.0
        return float(row[0])
    except Exception:
        return 0.0
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass


def _set_last_publish_ts(ts: Optional[float] = None) -> None:
    if not get_db:
        return
    db = None
    try:
        db = get_db()
        c = db.cursor()
        val = str(float(ts if ts is not None else time.time()))
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at",
            (_LAST_PUB_KEY, val, now),
        )
        db.commit()
    except Exception as e:
        logger.debug("[PUB GATE] set last pub ts: %s", e)
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


def _ensure_queue_schema(c) -> None:
    try:
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_publish_queue (
                id SERIAL PRIMARY KEY,
                idea_id INTEGER NOT NULL,
                msg TEXT NOT NULL,
                payload TEXT,
                ready_at DOUBLE PRECISION NOT NULL,
                status TEXT DEFAULT 'QUEUED',
                created_at TEXT,
                UNIQUE(idea_id)
            )
            """
        )
    except Exception:
        pass


def enqueue_publication(
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
    delay_sec: Optional[int] = None,
) -> bool:
    """Persist a qualified trade for later publish (survives restart)."""
    if not get_db or not idea_id or not msg:
        return False
    delay = int(delay_sec if delay_sec is not None else get_publication_cooldown_sec())
    ready = time.time() + max(1, delay)
    payload = {
        "symbol": symbol,
        "direction": direction,
        "timeframe": timeframe,
        "entry": entry,
        "stop": stop,
        "target1": target1,
        "market_type": market_type,
        "tier": tier,
        "source": source,
    }
    db = None
    try:
        db = get_db()
        c = db.cursor()
        _ensure_queue_schema(c)
        now = time.strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """
            INSERT INTO trade_publish_queue (idea_id, msg, payload, ready_at, status, created_at)
            VALUES (%s,%s,%s,%s,'QUEUED',%s)
            ON CONFLICT (idea_id) DO UPDATE SET
                msg = EXCLUDED.msg,
                payload = EXCLUDED.payload,
                ready_at = LEAST(trade_publish_queue.ready_at, EXCLUDED.ready_at),
                status = 'QUEUED'
            """,
            (int(idea_id), msg, json.dumps(payload), ready, now),
        )
        db.commit()
        try:
            from market_pulse.edge_trade_engine import mark_trade_publication
            mark_trade_publication(idea_id, "TEMPORARILY_QUEUED", f"BURST_COOLDOWN:{delay}s")
        except Exception:
            pass
        logger.info(
            "[PUB GATE] QUEUED #%s %s ready_in=%ss source=%s",
            idea_id, symbol, delay, source,
        )
        return True
    except Exception as e:
        logger.warning("[PUB GATE] enqueue failed #%s: %s", idea_id, e)
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


def _post_body(msg: str, idea_id: int, market_type: str) -> str:
    pub_id = public_signal_id(int(idea_id), market_type)
    body = msg
    if pub_id not in body:
        body = f"<b>{pub_id}</b>\n" + body
    return body


def _do_telegram_publish(
    *,
    msg: str,
    idea_id: int,
    symbol: str,
    direction: str,
    market_type: str,
    source: str,
    fp: str,
) -> Tuple[bool, str]:
    body = _post_body(msg, idea_id, market_type)
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
    _set_last_publish_ts(time.time())
    pub_id = public_signal_id(int(idea_id), market_type)
    logger.info(
        "[PUB GATE] PUBLISHED %s #%s %s %s source=%s fp=%s",
        pub_id, idea_id, symbol, direction, source, fp,
    )
    return True, "PUBLISHED"


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
    skip_burst_queue: bool = False,
) -> Tuple[bool, str]:
    """Single publication gate for all paths.

    Returns (ok, reason_code).
    ok=True only when Telegram post succeeded.
    TEMPORARILY_QUEUED is not a permanent suppress — process_publication_queue drains it.
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

    # Burst spacing: queue instead of permanent suppress (no daily quota)
    cooldown = get_publication_cooldown_sec()
    if not skip_burst_queue and cooldown > 0:
        last = _get_last_publish_ts()
        elapsed = time.time() - last if last > 0 else cooldown + 1
        if last > 0 and elapsed < cooldown:
            remain = int(cooldown - elapsed)
            ok_q = enqueue_publication(
                msg=msg,
                idea_id=int(idea_id),
                symbol=symbol,
                direction=direction,
                timeframe=timeframe,
                entry=entry,
                stop=stop,
                target1=target1,
                market_type=market_type,
                tier=tier,
                source=source,
                delay_sec=max(1, remain),
            )
            return False, "TEMPORARILY_QUEUED" if ok_q else "QUEUE_FAILED"

    idem = f"pub:{fp}:{int(idea_id)}"
    if not try_claim_idempotency(int(idea_id), idem):
        logger.info("[PUB GATE] IDEMPOTENT skip #%s %s", idea_id, symbol)
        return False, "IDEMPOTENT_SKIP"

    return _do_telegram_publish(
        msg=msg,
        idea_id=int(idea_id),
        symbol=symbol,
        direction=direction,
        market_type=market_type,
        source=source,
        fp=fp,
    )


def process_publication_queue(limit: int = 3) -> int:
    """Drain due queued trades. Re-checks duplicates; respects burst spacing.

    Returns number successfully published this call.
    """
    if not get_db:
        return 0
    published_n = 0
    db = None
    try:
        db = get_db()
        c = db.cursor()
        _ensure_queue_schema(c)
        now = time.time()
        c.execute(
            """
            SELECT id, idea_id, msg, payload FROM trade_publish_queue
            WHERE status = 'QUEUED' AND ready_at <= %s
            ORDER BY ready_at ASC
            LIMIT %s
            """,
            (now, int(limit)),
        )
        rows = c.fetchall() or []
    except Exception as e:
        logger.warning("[PUB GATE] queue read: %s", e)
        return 0
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass

    for qid, idea_id, msg, payload_s in rows:
        try:
            payload = json.loads(payload_s or "{}")
        except Exception:
            payload = {}
        symbol = payload.get("symbol") or ""
        direction = payload.get("direction") or ""
        timeframe = payload.get("timeframe") or ""
        entry = payload.get("entry")
        stop = payload.get("stop")
        target1 = payload.get("target1")
        market_type = payload.get("market_type") or "crypto"
        source = payload.get("source") or "queue"

        # Still within global cooldown? push ready_at forward
        cooldown = get_publication_cooldown_sec()
        last = _get_last_publish_ts()
        if last > 0 and cooldown > 0 and (time.time() - last) < cooldown:
            remain = cooldown - (time.time() - last)
            db2 = None
            try:
                db2 = get_db()
                c2 = db2.cursor()
                c2.execute(
                    "UPDATE trade_publish_queue SET ready_at=%s WHERE id=%s",
                    (time.time() + max(1, remain), int(qid)),
                )
                db2.commit()
            except Exception:
                pass
            finally:
                if db2:
                    try:
                        db2.close()
                    except Exception:
                        pass
            continue

        # Re-classify before publish
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
            db2 = None
            try:
                db2 = get_db()
                c2 = db2.cursor()
                c2.execute(
                    "UPDATE trade_publish_queue SET status='DROPPED' WHERE id=%s",
                    (int(qid),),
                )
                db2.commit()
                from market_pulse.edge_trade_engine import mark_trade_publication
                mark_trade_publication(idea_id, "SUPPRESSED", f"QUEUE_{cls}:{exist_id}")
            except Exception:
                pass
            finally:
                if db2:
                    try:
                        db2.close()
                    except Exception:
                        pass
            continue

        ok, code = publish_canonical_trade(
            msg=msg,
            idea_id=int(idea_id),
            symbol=symbol,
            direction=direction,
            timeframe=timeframe,
            entry=entry,
            stop=stop,
            target1=target1,
            market_type=market_type,
            tier=payload.get("tier") or "",
            source=f"queue:{source}",
            skip_burst_queue=True,
        )
        db2 = None
        try:
            db2 = get_db()
            c2 = db2.cursor()
            st = "PUBLISHED" if ok else ("DROPPED" if code != "TELEGRAM_ERROR" else "QUEUED")
            c2.execute(
                "UPDATE trade_publish_queue SET status=%s WHERE id=%s",
                (st, int(qid)),
            )
            db2.commit()
        except Exception:
            pass
        finally:
            if db2:
                try:
                    db2.close()
                except Exception:
                    pass
        if ok:
            published_n += 1
            # One successful post per cycle respects spacing for the next item
            break
    return published_n


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
