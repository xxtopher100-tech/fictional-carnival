"""Pure lifecycle rules — no DB/Telegram imports (testable offline)."""
from __future__ import annotations
from typing import Optional


def is_long(direction: str) -> bool:
    d = (direction or "long").lower()
    return d.startswith("long") or d in ("buy", "l") or "buy" in d


def resolve_from_extremes(
    direction: str,
    entry: float,
    stop: Optional[float],
    t1: Optional[float],
    t2: Optional[float],
    hi: Optional[float],
    lo: Optional[float],
    entry_seen: bool,
) -> Optional[str]:
    if not entry_seen or entry is None:
        return None
    long = is_long(direction)
    hit_stop = hit_t1 = hit_t2 = False
    if long:
        if stop is not None and lo is not None and lo <= stop:
            hit_stop = True
        if t2 is not None and hi is not None and hi >= t2:
            hit_t2 = True
        if t1 is not None and hi is not None and hi >= t1:
            hit_t1 = True
    else:
        if stop is not None and hi is not None and hi >= stop:
            hit_stop = True
        if t2 is not None and lo is not None and lo <= t2:
            hit_t2 = True
        if t1 is not None and lo is not None and lo <= t1:
            hit_t1 = True

    if hit_stop and (hit_t1 or hit_t2):
        return "AMBIGUOUS"
    if hit_stop:
        return "STOP_HIT"
    if hit_t2:
        return "TP2_HIT"
    if hit_t1:
        return "TP1_HIT"
    return None
