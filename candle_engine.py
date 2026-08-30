"""
Market Pulse Bot — candle (OHLCV history) engine.
====================================================
signal_engine.py needs a rolling window of past candles (EMA200, ADX, etc.
all require history — they can't be computed from a single current price).
This module maintains that window per coin, kept fresh via WebSocket —
NOT continuous REST polling.

Data flow:
  1. Startup: ONE REST call per coin (Binance's public klines endpoint,
     free, no auth) to backfill ~250 closed 1h candles.
  2. After that: a single combined WebSocket stream
     (<symbol>@kline_1h for every coin, same combined-stream pattern
     price_engine.py already uses for Binance) pushes new candles
     continuously. Zero further REST calls while connected.
  3. On reconnect: a small REST gap-fill (last ~5 candles) to cover
     whatever was missed while disconnected — NOT a full re-backfill,
     so a flaky connection doesn't turn into repeated heavy REST use.

Only CLOSED candles ("x": true in Binance's payload) ever enter the
persistent buffer — a still-forming candle is never treated as final,
since its high/low/close will keep changing until it closes.

Verified against Binance's own docs:
  REST:  GET /api/v3/klines?symbol=BTCUSDT&interval=1h&limit=250
         -> [[openTime, open, high, low, close, volume, closeTime, ...], ...]
  WS:    <symbol>@kline_1h combined stream
         -> {"data": {"k": {"t":.., "o":.., "h":.., "l":.., "c":.., "v":.., "x": bool}}}

Same known caveat as price_engine.py's Binance connection: this hits the
same host/IP-based access restriction — if Binance is geo-blocked on this
server, both the REST backfill and the WS stream will fail here too.
"""

import json
import threading
import time

from market_pulse.config_runtime import COINS, logger
from market_pulse.helpers import fetch_with_backoff
from market_pulse.websocket_protocol import _ws_handshake, _ws_recv_frame

CANDLE_INTERVAL = "1h"
MAX_CANDLES = 300          # keep a bit more than signal_engine's min_candles needs
BACKFILL_LIMIT = 250
GAP_FILL_LIMIT = 5         # small REST catch-up after a reconnect, not a full re-backfill

_lock = threading.Lock()
_candles: dict = {}        # coin -> list of closed candle dicts, oldest -> newest
_last_update: dict = {}    # coin -> unix ts of last accepted closed candle
_started = False


def _parse_binance_kline_row(row):
    """One row from either the REST klines array or a WS 'k' object.
    Accepts both shapes since they carry the same fields under different
    representations (REST: positional array; WS: named dict)."""
    if isinstance(row, dict):
        return {
            "open_time": row["t"] / 1000,
            "open": float(row["o"]),
            "high": float(row["h"]),
            "low": float(row["l"]),
            "close": float(row["c"]),
            "volume": float(row["v"]),
        }
    return {
        "open_time": row[0] / 1000,
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "volume": float(row[5]),
    }


def _merge_candle(coin, candle):
    """Insert/replace a closed candle by open_time, keep sorted, cap length."""
    with _lock:
        buf = _candles.setdefault(coin, [])
        for i, c in enumerate(buf):
            if c["open_time"] == candle["open_time"]:
                buf[i] = candle
                break
        else:
            buf.append(candle)
        buf.sort(key=lambda c: c["open_time"])
        if len(buf) > MAX_CANDLES:
            del buf[: len(buf) - MAX_CANDLES]
        _last_update[coin] = time.time()


def _rest_backfill(coin, limit=BACKFILL_LIMIT):
    """One-time (or gap-fill) REST call. Returns True on success."""
    symbol = coin + "USDT"
    try:
        resp = fetch_with_backoff(
            f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={CANDLE_INTERVAL}&limit={limit}"
        )
        if not resp or not isinstance(resp, list):
            return False
        for row in resp:
            candle = _parse_binance_kline_row(row)
            _merge_candle(coin, candle)
        return True
    except Exception as e:
        logger.warning("[CANDLE ENGINE] REST backfill failed for %s: %s" % (coin, e))
        return False


def get_candles(coin):
    """Return a copy of the candle buffer for `coin` (oldest -> newest), or [] if none yet."""
    with _lock:
        return list(_candles.get(coin, []))


def candles_ready(coin, min_candles=60):
    return len(get_candles(coin)) >= min_candles


def _kline_ws_thread():
    """Persistent combined kline stream for every coin. Reconnects with backoff.
    Same host/pattern as price_engine.py's Binance thread — separate connection
    since it's a different stream type (klines, not miniTicker)."""
    coins = [c for c in COINS if c not in ("USDT", "USDC")]
    streams = "/".join(f"{c.lower()}usdt@kline_{CANDLE_INTERVAL}" for c in coins)
    path = f"/stream?streams={streams}"
    host = "stream.binance.com"
    port = 9443
    backoff = 2
    first_connect = True

    while True:
        sock = None
        try:
            logger.info("[CANDLE ENGINE] Connecting kline stream...")
            sock = _ws_handshake(host, path, port)
            sock.settimeout(45)
            backoff = 2
            logger.info("[CANDLE ENGINE] Connected — streaming %s candles for %d coins"
                        % (CANDLE_INTERVAL, len(coins)))

            if not first_connect:
                # Reconnect gap-fill: catch up on whatever closed while we
                # were disconnected — small, bounded, not a full re-backfill.
                for coin in coins:
                    _rest_backfill(coin, limit=GAP_FILL_LIMIT)
                logger.info("[CANDLE ENGINE] Reconnect gap-fill complete")
            first_connect = False

            while True:
                opcode, payload = _ws_recv_frame(sock)
                if opcode is None:
                    continue
                msg = json.loads(payload.decode("utf-8"))
                data = msg.get("data", msg)
                k = data.get("k")
                if not k:
                    continue
                sym = k.get("s", "").lower()
                coin = next((c for c in coins if c.lower() + "usdt" == sym), None)
                if not coin:
                    continue
                if not k.get("x"):
                    continue  # still-forming candle — never treated as final
                candle = _parse_binance_kline_row(k)
                _merge_candle(coin, candle)

        except Exception as e:
            logger.warning("[CANDLE ENGINE] Disconnected: %s — reconnecting in %ds" % (e, backoff))
        finally:
            if sock:
                try: sock.close()
                except Exception: pass

        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


def start_candle_engine():
    """Call once at bot startup. Safe to call multiple times — only starts once."""
    global _started
    if _started:
        return
    _started = True

    coins = [c for c in COINS if c not in ("USDT", "USDC")]
    logger.info("[CANDLE ENGINE] Backfilling %d coins via REST (one-time)..." % len(coins))
    for coin in coins:
        _rest_backfill(coin)
    logger.info("[CANDLE ENGINE] Backfill complete — starting WS stream")

    t = threading.Thread(target=_kline_ws_thread, name="CandleEngine", daemon=True)
    t.start()


def candle_engine_status():
    now = time.time()
    with _lock:
        counts = {c: len(v) for c, v in _candles.items()}
        ages = {c: now - _last_update.get(c, 0) for c in _candles}
    ready = sum(1 for c in COINS if counts.get(c, 0) >= 60)
    lines = [f"Candle Engine: {ready}/{len(COINS)} coins have enough history"]
    stale = [c for c, age in ages.items() if age > 3600 * 2]
    if stale:
        lines.append(f"  Stale (no new candle in 2h+): {', '.join(stale)}")
    return "\n".join(lines)
