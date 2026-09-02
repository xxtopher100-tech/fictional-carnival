"""Durable daily schedule locks (survive restart / multi-worker).

Extracted from handlers.py.
"""
from __future__ import annotations

from market_pulse.config_runtime import logger
from market_pulse.db import get_db
from market_pulse.helpers import wat_now

def _schedule_lock_key(post_type, wat_date):
    return f"sched_posted_{post_type}_{wat_date.isoformat()}"


def _schedule_already_posted(post_type, wat_date):
    """True if this post_type was already successfully marked for wat_date."""
    key = _schedule_lock_key(post_type, wat_date)
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT value FROM admin_settings WHERE key=%s", (key,))
        row = c.fetchone()
        return bool(row and row[0])
    except Exception as e:
        logger.warning("[SCHEDULER] lock read failed (%s): %s — allowing post" % (key, e))
        return False  # fail-open only if DB down; in-memory flag still helps single process
    finally:
        if db:
            try: db.close()
            except Exception: pass


def _schedule_clear_posted(post_type, wat_date):
    """Remove durable lock so a failed scheduled post can retry same day."""
    key = _schedule_lock_key(post_type, wat_date)
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("DELETE FROM admin_settings WHERE key=%s", (key,))
        db.commit()
        logger.info("[SCHEDULER] Cleared lock %s", key)
    except Exception as e:
        logger.warning("[SCHEDULER] clear lock failed: %s", e)
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass


def _schedule_mark_posted(post_type, wat_date):

    """Persist that post_type was sent for wat_date. Returns True if we acquired the lock (first writer)."""
    key = _schedule_lock_key(post_type, wat_date)
    db = None
    try:
        db = get_db()
        c = db.cursor()
        # Insert-only: if row exists, we are second writer → do not post again
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s, %s, %s) "
            "ON CONFLICT (key) DO NOTHING",
            (key, "1", wat_now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        db.commit()
        # Did we insert? rowcount 1 = we own the lock; 0 = someone else already posted
        return c.rowcount == 1
    except Exception as e:
        logger.warning("[SCHEDULER] lock write failed (%s): %s" % (key, e))
        try:
            if db: db.rollback()
        except Exception:
            pass
        return True  # fail-open for single-process: still allow in-memory path
    finally:
        if db:
            try: db.close()
            except Exception: pass



# ─── extracted section ───
# BTC price at morning briefing — used by midday >2% move gate.
# Module-level so it survives across scheduler loop iterations; reset on new WAT day.
