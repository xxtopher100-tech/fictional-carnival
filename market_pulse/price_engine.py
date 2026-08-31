"""Market Pulse Bot — price_engine module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import COINS, logger


# ─── extracted section ───
# ⚡ WEBSOCKET PRICE ENGINE
# ═══════════════════════════════════════════════════════════════════════════
# Maintains persistent WebSocket connections to Binance and Kraken.
# Prices land in _ws_price_cache — a shared dict read by get_best_price().
#
# Architecture:
#   • One daemon thread per exchange (Binance stream, Kraken stream)
#   • Auto-reconnects with exponential backoff on disconnect/error
#   • Falls back to REST if WebSocket hasn't received a price within
#     WS_STALE_SECONDS (60 s) — REST fetchers are unchanged below
#   • No third-party libraries — uses only Python stdlib ssl + socket
#
# Exchanges:
#   Binance  — wss://stream.binance.com:9443  (no API key required)
#   Kraken   — wss://ws.kraken.com            (no API key required)
#   Bybit    — wss://stream.bybit.com/v5/public/spot (no API key required)
#
# The REST fetchers for OKX/Bybit/CoinGecko remain as final fallbacks.
# ═══════════════════════════════════════════════════════════════════════════

# ── Shared price cache ────────────────────────────────────────────────────
# Per-exchange, not last-write-wins: { "BTC": { "Binance": {"price":.., "change":.., "ts":..},
#                                                "Bybit": {...}, "Kraken": {...} } }
# This lets reads apply a defined priority order (Binance > Bybit > Kraken)
# instead of whichever exchange happened to write most recently — three
# concurrent WS threads writing to a flat cache would otherwise make the
# effective price source undefined and prone to flicker between exchanges.
_ws_price_cache: dict = {}
_ws_lock = threading.Lock()
WS_STALE_SECONDS = 60  # If no update in 60s, treat as stale, fall to REST

EXCHANGE_PRIORITY = ("Binance", "Bybit", "Kraken")

# ── Heartbeat / data-quality tracking ─────────────────────────────────────
_ws_heartbeat: dict = {}          # {"Binance": last_message_ts, ...}
_ws_reject_count: dict = {}       # {"Binance": n_rejected_ticks, ...}
WS_HEARTBEAT_WARN_SECONDS = 90    # watchdog logs a warning past this
SPIKE_REJECT_PCT = 25.0           # reject a tick that jumps >25% vs that
                                    # SAME exchange's own last good price
                                    # (checked per-exchange, not cross-exchange,
                                    # since two real exchanges can legitimately
                                    # differ slightly — that's not a spike)


def _touch_heartbeat(exchange):
    with _ws_lock:
        _ws_heartbeat[exchange] = time.time()

# ── Symbol maps ───────────────────────────────────────────────────────────
# Binance uses lowercase concatenated pairs e.g. "btcusdt"
_BINANCE_STREAM_MAP = {
    coin.lower() + "usdt": coin
    for coin in COINS
    if coin not in ("USDT", "USDC")   # Stablecoins don't need a stream
}
# Binance also carries USDT/USDC via a BUSD pair — skip them

# Kraken WS uses its own symbol format e.g. "XBT/USD"
_KRAKEN_WS_MAP = {
    "XBT/USD": "BTC",
    "ETH/USD": "ETH",
    "SOL/USD": "SOL",
    "XRP/USD": "XRP",
    "DOGE/USD": "DOGE",
    "ADA/USD": "ADA",
    "LTC/USD": "LTC",
    "DOT/USD": "DOT",
    "LINK/USD": "LINK",
    "ATOM/USD": "ATOM",
    "FIL/USD": "FIL",
    "TRX/USD": "TRX",
    "AVAX/USD": "AVAX",
    "NEAR/USD": "NEAR",
    "UNI/USD": "UNI",
}

# Bybit WS symbol format e.g. "BTCUSDT"
_BYBIT_WS_MAP = {
    coin + "USDT": coin
    for coin in COINS
    if coin not in ("USDT", "USDC")
}


from market_pulse.websocket_protocol import (
    _ws_handshake, _ws_recv_frame, _ws_send_pong, _ws_recv_exact, _ws_send_text,
)

def _validate_price(coin, price, exchange):
    """
    Rejects non-positive/non-finite prices and single-tick spikes that look
    like a parsing error rather than a real market move — checked against
    THIS exchange's own last cached price, not other exchanges (two real
    exchanges legitimately differing by a fraction of a percent is normal,
    not a spike). Returns True if the price should be cached.
    """
    try:
        price = float(price)
    except (TypeError, ValueError):
        return False
    if price <= 0 or price != price or price in (float("inf"), float("-inf")):
        return False

    with _ws_lock:
        prior_entry = _ws_price_cache.get(coin, {}).get(exchange)
    if prior_entry and prior_entry.get("price"):
        prior = prior_entry["price"]
        if prior > 0:
            move_pct = abs(price - prior) / prior * 100
            if move_pct > SPIKE_REJECT_PCT:
                with _ws_lock:
                    _ws_reject_count[exchange] = _ws_reject_count.get(exchange, 0) + 1
                logger.warning(
                    "[WS %s] Rejected suspicious tick for %s: %.8g -> %.8g (%.1f%% jump)"
                    % (exchange.upper(), coin, prior, price, move_pct)
                )
                return False
    return True


def _ws_cache_price(coin, price, change=None, exchange="unknown"):
    """Thread-safe write into this exchange's slot of the price cache, after validation."""
    if not _validate_price(coin, price, exchange):
        return
    with _ws_lock:
        coin_entry = _ws_price_cache.setdefault(coin, {})
        existing = coin_entry.get(exchange, {})
        coin_entry[exchange] = {
            "price": float(price),
            "change": change if change is not None else existing.get("change"),
            "ts": time.time(),
        }


def _ws_get_cached(coin, priority=EXCHANGE_PRIORITY):
    """
    Return (price, change) from the WebSocket cache, preferring exchanges
    in `priority` order. Falls through to ANY fresh exchange not in the
    priority list (so a future extra source still gets used rather than
    silently ignored). Returns (None, None) if nothing fresh exists.
    """
    now = time.time()
    with _ws_lock:
        coin_entry = dict(_ws_price_cache.get(coin, {}))
    for exchange in priority:
        entry = coin_entry.get(exchange)
        if entry and now - entry["ts"] <= WS_STALE_SECONDS:
            return entry["price"], entry.get("change")
    # Nothing in the priority list was fresh — try anything else that is.
    for exchange, entry in coin_entry.items():
        if exchange in priority:
            continue
        if now - entry["ts"] <= WS_STALE_SECONDS:
            return entry["price"], entry.get("change")
    return None, None


def _ws_get_cached_by_exchange(coin, exchange):
    """Return (price, change, age_seconds) for one specific exchange, or (None, None, None)."""
    with _ws_lock:
        entry = _ws_price_cache.get(coin, {}).get(exchange)
    if not entry:
        return None, None, None
    return entry["price"], entry.get("change"), time.time() - entry["ts"]


# ── Binance WebSocket thread ──────────────────────────────────────────────

def _binance_ws_thread():
    """Persistent Binance combined stream. Reconnects with backoff."""
    streams = "/".join(f"{sym}@miniTicker" for sym in sorted(_BINANCE_STREAM_MAP))
    path = f"/stream?streams={streams}"
    host = "stream.binance.com"
    port = 9443
    backoff = 2

    while True:
        sock = None
        try:
            logger.info("[WS BINANCE] Connecting...")
            sock = _ws_handshake(host, path, port)
            sock.settimeout(45)   # Binance sends keepalive every ~20s
            backoff = 2           # Reset on successful connect
            logger.info("[WS BINANCE] Connected — streaming %d pairs" % len(_BINANCE_STREAM_MAP))

            while True:
                opcode, payload = _ws_recv_frame(sock)
                _touch_heartbeat("Binance")
                if opcode is None:
                    continue   # Ping frame — skip
                msg = json.loads(payload.decode("utf-8"))
                # Combined stream wraps data in {"stream":..., "data":{...}}
                data = msg.get("data", msg)
                sym = data.get("s", "").lower()   # e.g. "btcusdt"
                coin = _BINANCE_STREAM_MAP.get(sym)
                if coin:
                    last = float(data.get("c", 0) or data.get("lastPrice", 0))
                    open24h = float(data.get("o", 0) or 0)
                    change = ((last - open24h) / open24h * 100) if open24h else None
                    if last > 0:
                        _ws_cache_price(coin, last, change, exchange="Binance")

        except Exception as e:
            logger.warning("[WS BINANCE] Disconnected: %s — reconnecting in %ds" % (e, backoff))
        finally:
            if sock:
                try: sock.close()
                except Exception: pass

        time.sleep(backoff)
        backoff = min(backoff * 2, 60)   # Cap at 60s


# ── Kraken WebSocket thread ───────────────────────────────────────────────

def _kraken_ws_thread():
    """Persistent Kraken v2 WebSocket. Reconnects with backoff."""
    host = "ws.kraken.com"
    path = "/"
    pairs = list(_KRAKEN_WS_MAP.keys())
    backoff = 2

    # Kraken also provides 24h open via the ticker channel
    _kraken_open24h = {}   # symbol -> open24h price

    while True:
        sock = None
        try:
            logger.info("[WS KRAKEN] Connecting...")
            sock = _ws_handshake(host, path, port=443)
            sock.settimeout(30)
            backoff = 2
            logger.info("[WS KRAKEN] Connected")

            # Subscribe to ticker channel
            sub_msg = json.dumps({
                "event": "subscribe",
                "pair": pairs,
                "subscription": {"name": "ticker"}
            })
            _ws_send_text(sock, sub_msg)

            while True:
                opcode, payload = _ws_recv_frame(sock)
                _touch_heartbeat("Kraken")
                if opcode is None:
                    continue
                msg = json.loads(payload.decode("utf-8"))

                # Kraken sends [channelID, data, "ticker", "XBT/USD"]
                if isinstance(msg, list) and len(msg) == 4 and msg[2] == "ticker":
                    ticker = msg[1]
                    symbol = msg[3]   # e.g. "XBT/USD"
                    coin = _KRAKEN_WS_MAP.get(symbol)
                    if coin:
                        last = float(ticker.get("c", [0])[0])
                        open24h = float(ticker.get("o", [0])[0])
                        change = ((last - open24h) / open24h * 100) if open24h else None
                        if last > 0:
                            _ws_cache_price(coin, last, change, exchange="Kraken")

        except Exception as e:
            logger.warning("[WS KRAKEN] Disconnected: %s — reconnecting in %ds" % (e, backoff))
        finally:
            if sock:
                try: sock.close()
                except Exception: pass

        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


# ── Bybit WebSocket thread ────────────────────────────────────────────────

def _bybit_ws_thread():
    """Persistent Bybit v5 spot WebSocket. Covers coins not on Kraken/Binance.

    Bybit requires the CLIENT to send {"op":"ping"} every 20 seconds or the
    server will close the connection. We use a 19s socket timeout so the recv
    loop wakes up slightly before the deadline and sends a ping proactively.
    """
    host = "stream.bybit.com"
    path = "/v5/public/spot"
    # Only subscribe to coins that Binance doesn't already cover
    binance_covered = set(c.upper() for c in _BINANCE_STREAM_MAP.values())
    bybit_only = [sym for sym, coin in _BYBIT_WS_MAP.items() if coin not in binance_covered]
    if not bybit_only:
        logger.info("[WS BYBIT] No extra coins to stream — thread idle")
        return

    backoff = 2
    BYBIT_PING_INTERVAL = 19  # seconds — Bybit disconnects after 20s silence

    while True:
        sock = None
        try:
            logger.info("[WS BYBIT] Connecting...")
            sock = _ws_handshake(host, path, port=443)
            # Timeout slightly under ping interval so we wake up to send ping
            sock.settimeout(BYBIT_PING_INTERVAL)
            backoff = 2
            logger.info("[WS BYBIT] Connected — streaming %d pairs" % len(bybit_only))

            # Subscribe to tickers
            sub_msg = json.dumps({
                "op": "subscribe",
                "args": [f"tickers.{sym}" for sym in bybit_only]
            })
            _ws_send_text(sock, sub_msg)

            last_ping = time.time()

            while True:
                try:
                    opcode, payload = _ws_recv_frame(sock)
                except TimeoutError:
                    # Socket timed out — time to send keepalive ping
                    _ws_send_text(sock, json.dumps({"op": "ping"}))
                    last_ping = time.time()
                    continue
                except OSError as oe:
                    if "timed out" in str(oe).lower():
                        _ws_send_text(sock, json.dumps({"op": "ping"}))
                        last_ping = time.time()
                        continue
                    raise

                if opcode is None:
                    continue

                _touch_heartbeat("Bybit")
                msg = json.loads(payload.decode("utf-8"))

                if msg.get("topic", "").startswith("tickers."):
                    data = msg.get("data", {})
                    sym = msg["topic"].replace("tickers.", "")
                    coin = _BYBIT_WS_MAP.get(sym)
                    if coin:
                        last = float(data.get("lastPrice", 0) or 0)
                        open24h = float(data.get("prevPrice24h", 0) or 0)
                        change = ((last - open24h) / open24h * 100) if open24h else None
                        if last > 0:
                            _ws_cache_price(coin, last, change, exchange="Bybit")

                elif msg.get("op") in ("pong", "ping"):
                    pass  # Server acknowledged our ping — connection healthy

                # Proactive ping if somehow no timeout fired yet
                if time.time() - last_ping >= BYBIT_PING_INTERVAL:
                    _ws_send_text(sock, json.dumps({"op": "ping"}))
                    last_ping = time.time()

        except Exception as e:
            logger.warning("[WS BYBIT] Disconnected: %s — reconnecting in %ds" % (e, backoff))
        finally:
            if sock:
                try: sock.close()
                except Exception: pass

        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


# ── Bybit ping keepalive ──────────────────────────────────────────────────
# Bybit disconnects if no message is received in 20s.
# We handle this by maintaining a reference to the socket and sending pings.
# The simpler approach: reconnect quickly (backoff=2s) on disconnect.
# The _bybit_ws_thread already handles that — no separate ping thread needed.


# ── WebSocket engine startup ──────────────────────────────────────────────

_ws_started = False


def _ws_watchdog_thread():
    """
    Periodically checks per-exchange heartbeats. A silently frozen socket
    (connected, no exception, but no messages arriving) would otherwise
    only be caught after sock.settimeout() expires and raises — this gives
    earlier, explicit visibility via the logs, independent of any single
    exchange's own timeout value.
    """
    while True:
        time.sleep(30)
        now = time.time()
        with _ws_lock:
            snapshot = dict(_ws_heartbeat)
        for ex in EXCHANGE_PRIORITY:
            last = snapshot.get(ex)
            if last is None:
                continue  # hasn't sent a first message yet — fine during startup
            silence = now - last
            if silence > WS_HEARTBEAT_WARN_SECONDS:
                logger.warning(
                    "[WS WATCHDOG] %s has been silent for %.0fs (no live socket exception raised yet)"
                    % (ex, silence)
                )


def start_ws_price_engine():
    """Launch all WebSocket threads. Called once at bot startup.
    Safe to call multiple times — only starts threads once."""
    global _ws_started
    if _ws_started:
        return
    _ws_started = True

    threads = [
        ("Binance",  _binance_ws_thread),
        ("Kraken",   _kraken_ws_thread),
        ("Bybit",    _bybit_ws_thread),
        ("Watchdog", _ws_watchdog_thread),
    ]
    for name, target in threads:
        t = threading.Thread(target=target, name=f"WS-{name}", daemon=True)
        t.start()
        logger.info("[WS ENGINE] Started %s thread" % name)


def ws_engine_status():
    """Return a status string for admin health checks."""
    now = time.time()
    with _ws_lock:
        cache_snapshot = {c: dict(exchanges) for c, exchanges in _ws_price_cache.items()}
        heartbeats = dict(_ws_heartbeat)
        rejects = dict(_ws_reject_count)

    # A coin counts as "fresh" if get_cached's priority order would find something.
    fresh_coins = 0
    for coin in COINS:
        entries = cache_snapshot.get(coin, {})
        if any(now - e["ts"] <= WS_STALE_SECONDS for e in entries.values()):
            fresh_coins += 1

    lines = [
        "WebSocket Engine",
        f"  Fresh prices: {fresh_coins}/{len(COINS)} coins",
        "  Per-exchange (priority order Binance > Bybit > Kraken):",
    ]
    for ex in EXCHANGE_PRIORITY:
        last = heartbeats.get(ex)
        age_str = f"last message {now - last:.0f}s ago" if last else "no data yet"
        rej = rejects.get(ex, 0)
        n_coins = sum(1 for c in cache_snapshot.values() if ex in c and now - c[ex]["ts"] <= WS_STALE_SECONDS)
        lines.append(f"    {ex}: {age_str}, {n_coins} coins fresh" + (f", {rej} ticks rejected" if rej else ""))

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
