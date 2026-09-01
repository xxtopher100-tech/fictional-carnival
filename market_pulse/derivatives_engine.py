"""
Market Pulse Bot — derivatives data engine.
====================================================
Funding rate, open interest, order book, and liquidations — sourced from
real exchange WebSocket feeds only. Nothing in this module invents,
estimates, or interpolates a value: if a provider doesn't send it, the
corresponding field stays None and callers must treat that as "unknown",
not "zero".

────────────────────────────────────────────────────────────────────────
WHY BYBIT FIRST
────────────────────────────────────────────────────────────────────────
Bybit's v5 linear (USDT-perpetual) ticker topic already carries funding
rate, mark price, index price, and open interest in the SAME message —
verified against Bybit's own API docs (bybit-exchange.github.io/docs/v5/
websocket/public/ticker). That means funding + OI need only one
subscription, not a stream plus a separate REST poll. Bybit also has a
free public order book stream (orderbook.{depth}.{symbol}) and a free
public liquidation stream (allLiquidation.{symbol}) — all confirmed public,
no API key, no paid tier; Bybit's own docs state WebSocket usage doesn't
even count against their REST rate limits.

Binance was evaluated as an alternative/backup and rejected for THIS
phase: Binance Futures has no WebSocket push for open interest at all
(REST-poll only, GET /fapi/v1/openInterest), and Binance restructured
its futures WebSocket routing into /public, /market, /private paths —
the old unrouted URLs were permanently decommissioned 2026-04-23. Adding
Binance derivatives later is a real, separate piece of work (a new
provider class handling REST-polled OI alongside WS-pushed funding/depth/
liquidations under the new routing) — not a copy-paste of this file.

Kraken and OKX/Bitget are NOT implemented. Kraken's derivatives live on
an entirely separate platform (Kraken Futures — different host, different
auth) from the spot API already in use elsewhere in this bot; that's a
full new exchange integration, not an addition to this module. OKX and
Bitget haven't been evaluated at all yet — no claims are made about them.

────────────────────────────────────────────────────────────────────────
ARCHITECTURE
────────────────────────────────────────────────────────────────────────
DerivativesProvider   — abstract interface every exchange adapter implements.
BybitLinearProvider    — the only concrete implementation right now.
ProviderManager        — holds a priority-ordered list of providers, tracks
                          health, and exposes the ACTIVE provider's data
                          under one normalized interface. With only one
                          provider registered, "failover" degrades to
                          "detect Bybit is down and say so" — the switching
                          logic is there but nothing to switch TO yet.
normalize_*()           — pure functions, exchange payload -> normalized dict.
                          Kept separate from the provider classes so they're
                          unit-testable without a live socket.

HOW TO ADD A NEW PROVIDER LATER:
    1. Subclass DerivativesProvider, implement run()/stop()/is_healthy().
    2. Write a normalize_<exchange>_*() function per message type, matching
       the NORMALIZED_FIELDS schema below exactly.
    3. Register an instance with ProviderManager(providers=[bybit, new_one]),
       ordered by priority (index 0 = primary).
    Nothing in signal_engine.py or anywhere else needs to change — they only
    ever call get_snapshot()/get_orderbook()/on_liquidation(), never touch
    a provider directly.

────────────────────────────────────────────────────────────────────────
FAILOVER / DATA-QUALITY MODEL (see engineering notes in the accompanying
chat message for the full explanation — summarized here for future readers)
────────────────────────────────────────────────────────────────────────
- Failover: each provider tracks its own per-symbol last-update timestamp.
  ProviderManager.get_snapshot() walks providers in priority order and
  returns the first one that is healthy AND fresh for that symbol. A
  provider is "unhealthy" after N consecutive reconnect failures; it's
  retried in the background on its own backoff schedule and rejoins
  rotation automatically once it produces a fresh message again.
- Duplicate suppression: liquidation events are deduped on
  (exchange, symbol, side, price, size, timestamp) within a short rolling
  window — the same liquidation re-broadcast or replayed on reconnect
  won't fire the callback twice.
- Staleness: DERIV_STALE_SECONDS (default 45s) — same pattern as
  WS_STALE_SECONDS in price_engine.py. A snapshot older than that is
  treated as unavailable, not returned as if it were live.
- Cross-provider price reconciliation: NOT implemented, and deliberately
  not attempted with a single provider — there's nothing to reconcile
  against yet. When a second provider is added, this file will need an
  explicit policy (e.g. "reject if providers disagree by >X%") rather than
  silently averaging or picking one arbitrarily — averaging two prices
  that disagree is itself a form of fabricating a number neither exchange
  actually quoted.
"""

import json
import threading
import time
from collections import deque

from market_pulse.config_runtime import COINS, logger
from market_pulse.websocket_protocol import (
    _ws_handshake as _protocol_open_websocket,
    _ws_recv_frame as _protocol_recv_frame,
    _ws_send_text as _protocol_send_text,
)

DERIV_STALE_SECONDS = 45
UNHEALTHY_AFTER_FAILURES = 5

# Bybit linear (USDT perpetual) uses 1000x multipliers for some low-priced coins.
# Subscribing to SHIBUSDT on /v5/public/linear is rejected; 1000SHIBUSDT is valid.
BYBIT_LINEAR_REMAP = {
    "SHIB": "1000SHIBUSDT",
}
# Coins with no reliable OKX SWAP under {COIN}-USDT-SWAP naming (log once, skip).
OKX_SWAP_SKIP_COINS = frozenset({"FET", "TON"})
UNHEALTHY_AFTER_FAILURES = 5      # consecutive reconnect failures before a provider is marked down
LIQUIDATION_DEDUPE_WINDOW = 500   # how many recent liquidation keys to remember

# The exact set of fields every normalized snapshot carries. A provider
# that can't populate a field leaves it None — never a guessed value.
NORMALIZED_FIELDS = (
    "exchange", "symbol", "timestamp",
    "price", "volume_24h",
    "mark_price", "index_price",
    "funding_rate", "next_funding_time",
    "open_interest", "open_interest_value",
)


def _empty_snapshot(exchange, symbol):
    return {f: None for f in NORMALIZED_FIELDS} | {"exchange": exchange, "symbol": symbol}


# ═══════════════════════════════════════════════════════════════════════════
# NORMALIZATION — pure functions, unit-testable without a socket
# ═══════════════════════════════════════════════════════════════════════════

def _to_float(v):
    """Bybit sends numbers as strings; empty string means 'not applicable',
    not zero — must not be coerced to 0.0."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def normalize_bybit_ticker(msg, coin):
    """
    msg["data"] fields verified against Bybit v5 linear ticker docs:
    lastPrice, markPrice, indexPrice, openInterest, openInterestValue,
    fundingRate, nextFundingTime, volume24h. A 'delta' message may omit
    fields present in the prior 'snapshot' — callers must merge onto the
    last known snapshot, not treat a delta as a full replacement (handled
    by BybitLinearProvider._update_snapshot, not here).
    """
    data = msg.get("data", {})
    ts = msg.get("ts")
    out = _empty_snapshot("Bybit", coin)
    out["timestamp"] = ts / 1000 if ts else time.time()
    if "lastPrice" in data:
        out["price"] = _to_float(data.get("lastPrice"))
    if "volume24h" in data:
        out["volume_24h"] = _to_float(data.get("volume24h"))
    if "markPrice" in data:
        out["mark_price"] = _to_float(data.get("markPrice"))
    if "indexPrice" in data:
        out["index_price"] = _to_float(data.get("indexPrice"))
    if "fundingRate" in data:
        out["funding_rate"] = _to_float(data.get("fundingRate"))
    if "nextFundingTime" in data:
        nft = _to_float(data.get("nextFundingTime"))
        out["next_funding_time"] = nft / 1000 if nft else None
    if "openInterest" in data:
        out["open_interest"] = _to_float(data.get("openInterest"))
    if "openInterestValue" in data:
        out["open_interest_value"] = _to_float(data.get("openInterestValue"))
    return out


def normalize_bybit_orderbook(msg, coin):
    """
    msg["data"] fields verified against Bybit v5 orderbook docs: s, b
    (bids), a (asks), u (update id), seq. Each bid/ask is [price, size] as
    strings; a size of "0" means "remove this level" in a delta message —
    that removal logic lives in BybitLinearProvider._apply_orderbook_delta,
    not here (this function only converts one raw message to floats).
    """
    data = msg.get("data", {})
    ts = msg.get("ts")
    return {
        "exchange": "Bybit",
        "symbol": coin,
        "timestamp": ts / 1000 if ts else time.time(),
        "type": msg.get("type"),  # "snapshot" or "delta"
        "bids": [(_to_float(p), _to_float(s)) for p, s in data.get("b", [])],
        "asks": [(_to_float(p), _to_float(s)) for p, s in data.get("a", [])],
        "update_id": data.get("u"),
        "seq": data.get("seq"),
    }


def normalize_bybit_liquidation(entry, coin):
    """
    One entry from the allLiquidation.{symbol} data array. Fields verified
    against Bybit v5 docs: T (ms timestamp), s (symbol), S (side), v
    (size), p (price) — all strings except T.
    """
    return {
        "exchange": "Bybit",
        "symbol": coin,
        "timestamp": (entry.get("T") or 0) / 1000 or time.time(),
        "side": entry.get("S"),
        "price": _to_float(entry.get("p")),
        "size": _to_float(entry.get("v")),
    }


# ═══════════════════════════════════════════════════════════════════════════
# PROVIDER INTERFACE
# ═══════════════════════════════════════════════════════════════════════════

class DerivativesProvider:
    """
    Interface every exchange adapter implements. Nothing outside this file
    (and ProviderManager) should ever call an exchange-specific method —
    everything goes through this shape.
    """
    name = "unnamed"

    def start(self):
        """Launch background thread(s). Must not block the caller."""
        raise NotImplementedError

    def stop(self):
        """Signal shutdown and let the background thread(s) exit cleanly."""
        raise NotImplementedError

    def is_healthy(self):
        """True if the provider is connected and not past its failure threshold."""
        raise NotImplementedError

    def get_snapshot(self, coin):
        """Return a normalized snapshot dict for `coin`, or None if unavailable/stale."""
        raise NotImplementedError

    def get_orderbook(self, coin):
        """Return the current normalized order book for `coin`, or None."""
        raise NotImplementedError


# ═══════════════════════════════════════════════════════════════════════════
# BYBIT LINEAR PROVIDER
# ═══════════════════════════════════════════════════════════════════════════

class BybitLinearProvider(DerivativesProvider):
    name = "Bybit"

    def __init__(self, coins=None, orderbook_depth=50, on_liquidation=None):
        self._coins = [c for c in (coins or COINS) if c not in ("USDT", "USDC")]
        self._symbol_map = {}
        self._skipped_syms = set()  # rejected by exchange — do not resubscribe spam
        for c in self._coins:
            if c in BYBIT_LINEAR_REMAP:
                self._symbol_map[BYBIT_LINEAR_REMAP[c]] = c
            else:
                self._symbol_map[c + "USDT"] = c
        self._depth = orderbook_depth
        self._on_liquidation = on_liquidation  # callback(normalized_liquidation_dict)

        self._lock = threading.Lock()
        self._snapshots: dict = {}   # coin -> normalized snapshot dict
        self._orderbooks: dict = {}  # coin -> {"bids": {price:size}, "asks": {price:size}, "ts":...}
        self._last_update: dict = {}  # coin -> unix ts, for staleness checks
        self._seen_liquidations = deque(maxlen=LIQUIDATION_DEDUPE_WINDOW)
        self._seen_liquidations_set = set()

        self._consecutive_failures = 0
        self._connected = False
        self._stop_event = threading.Event()
        self._thread = None

    # ── lifecycle ──────────────────────────────────────────────────────
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="Derivatives-Bybit", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def is_healthy(self):
        return self._connected and self._consecutive_failures < UNHEALTHY_AFTER_FAILURES

    # ── read interface ────────────────────────────────────────────────
    def get_snapshot(self, coin):
        with self._lock:
            snap = self._snapshots.get(coin)
            last = self._last_update.get(coin)
        if not snap or not last:
            return None
        if time.time() - last > DERIV_STALE_SECONDS:
            return None
        return dict(snap)

    def get_orderbook(self, coin, top_n=20):
        with self._lock:
            book = self._orderbooks.get(coin)
        if not book:
            return None
        if time.time() - book["ts"] > DERIV_STALE_SECONDS:
            return None
        bids = sorted(book["bids"].items(), key=lambda kv: -kv[0])[:top_n]
        asks = sorted(book["asks"].items(), key=lambda kv: kv[0])[:top_n]
        return {
            "exchange": "Bybit", "symbol": coin, "timestamp": book["ts"],
            "bids": bids, "asks": asks,
        }

    # ── internal: connection loop ────────────────────────────────────
    def _run(self):
        host = "stream.bybit.com"
        path = "/v5/public/linear"
        backoff = 2
        PING_INTERVAL = 19  # Bybit disconnects after 20s of client silence

        while not self._stop_event.is_set():
            sock = None
            try:
                logger.info("[DERIV BYBIT] Connecting...")
                sock = _protocol_open_websocket(host, path, 443)
                sock.settimeout(PING_INTERVAL)
                self._connected = True
                self._consecutive_failures = 0
                backoff = 2
                logger.info("[DERIV BYBIT] Connected — subscribing %d symbols" % len(self._symbol_map))

                topics = []
                for sym in list(self._symbol_map.keys()):
                    if sym in self._skipped_syms:
                        continue
                    topics += [f"tickers.{sym}", f"orderbook.{self._depth}.{sym}", f"allLiquidation.{sym}"]
                if not topics:
                    logger.warning("[DERIV BYBIT] No symbols left to subscribe — idling this cycle")
                # Bybit caps args per subscribe message; chunk defensively.
                for i in range(0, len(topics), 10):
                    _protocol_send_text(sock, json.dumps({"op": "subscribe", "args": topics[i:i + 10]}))

                while not self._stop_event.is_set():
                    try:
                        opcode, payload = _protocol_recv_frame(sock)
                    except TimeoutError:
                        _protocol_send_text(sock, json.dumps({"op": "ping"}))
                        continue
                    except OSError as oe:
                        if "timed out" in str(oe).lower():
                            _protocol_send_text(sock, json.dumps({"op": "ping"}))
                            continue
                        raise

                    if opcode is None:
                        continue
                    self._handle_message(payload)

            except Exception as e:
                self._connected = False
                self._consecutive_failures += 1
                logger.warning(
                    "[DERIV BYBIT] Disconnected (%d consecutive failures): %s — reconnecting in %ds"
                    % (self._consecutive_failures, e, backoff)
                )
            finally:
                if sock:
                    try: sock.close()
                    except Exception: pass

            if self._stop_event.wait(backoff):
                break
            backoff = min(backoff * 2, 60)

        self._connected = False
        logger.info("[DERIV BYBIT] Stopped cleanly")

    # ── internal: message handling / validation ──────────────────────
    def _handle_message(self, raw_payload):
        try:
            msg = json.loads(raw_payload.decode("utf-8"))
        except Exception:
            return  # malformed frame — drop, don't crash the loop

        if not isinstance(msg, dict):
            return

        if "success" in msg and "op" in msg and msg.get("op") in ("subscribe", "ping"):
            if not msg.get("success"):
                ret = str(msg.get("ret_msg") or msg.get("retMsg") or "")
                # e.g. "tickers.SHIBUSDT handler not found" / invalid symbol
                skipped = None
                for sym in list(self._symbol_map.keys()):
                    if sym and sym in ret:
                        skipped = sym
                        break
                if skipped and skipped not in self._skipped_syms:
                    self._skipped_syms.add(skipped)
                    coin = self._symbol_map.get(skipped, "?")
                    logger.warning(
                        "[DERIV BYBIT] %s unsupported on linear stream (%s) — skipped",
                        skipped, coin,
                    )
                elif not skipped:
                    logger.warning("[DERIV BYBIT] Subscribe/ping rejected: %s" % ret)
            return

        topic = msg.get("topic", "")
        if topic.startswith("tickers."):
            self._handle_ticker(msg, topic)
        elif topic.startswith("orderbook."):
            self._handle_orderbook(msg, topic)
        elif topic.startswith("allLiquidation."):
            self._handle_liquidation(msg, topic)

    def _coin_from_topic(self, topic):
        sym = topic.split(".")[-1]
        return self._symbol_map.get(sym)

    def _handle_ticker(self, msg, topic):
        coin = self._coin_from_topic(topic)
        if not coin:
            return
        normalized = normalize_bybit_ticker(msg, coin)
        with self._lock:
            existing = self._snapshots.get(coin)
            if existing and msg.get("type") == "delta":
                # Delta messages only carry changed fields — merge onto
                # the last known snapshot rather than overwriting with Nones.
                merged = dict(existing)
                for k, v in normalized.items():
                    if v is not None:
                        merged[k] = v
                normalized = merged
            self._snapshots[coin] = normalized
            self._last_update[coin] = time.time()

    def _handle_orderbook(self, msg, topic):
        coin = self._coin_from_topic(topic)
        if not coin:
            return
        parsed = normalize_bybit_orderbook(msg, coin)
        with self._lock:
            book = self._orderbooks.get(coin)
            if parsed["type"] == "snapshot" or book is None:
                book = {"bids": {}, "asks": {}, "ts": parsed["timestamp"]}
            for price, size in parsed["bids"]:
                if price is None:
                    continue
                if size == 0:
                    book["bids"].pop(price, None)
                else:
                    book["bids"][price] = size
            for price, size in parsed["asks"]:
                if price is None:
                    continue
                if size == 0:
                    book["asks"].pop(price, None)
                else:
                    book["asks"][price] = size
            book["ts"] = parsed["timestamp"]
            self._orderbooks[coin] = book

    def _handle_liquidation(self, msg, topic):
        coin = self._coin_from_topic(topic)
        if not coin:
            return
        entries = msg.get("data") or []
        if isinstance(entries, dict):
            entries = [entries]
        for entry in entries:
            liq = normalize_bybit_liquidation(entry, coin)
            key = (liq["exchange"], liq["symbol"], liq["side"], liq["price"], liq["size"], liq["timestamp"])
            if key in self._seen_liquidations_set:
                continue  # duplicate — reconnect replay or re-broadcast
            self._seen_liquidations_set.add(key)
            self._seen_liquidations.append(key)
            while len(self._seen_liquidations_set) > LIQUIDATION_DEDUPE_WINDOW:
                old = self._seen_liquidations.popleft()
                self._seen_liquidations_set.discard(old)
            if self._on_liquidation:
                try:
                    self._on_liquidation(liq)
                except Exception as e:
                    logger.warning("[DERIV BYBIT] on_liquidation callback error: %s" % e)


# ═══════════════════════════════════════════════════════════════════════════
# NORMALIZATION — OKX (verified field names, tickers + funding-rate only)
# ═══════════════════════════════════════════════════════════════════════════
#
# Verified against OKX's own v5 docs and real captured payload examples:
#   - Subscribe: {"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT-SWAP"}]}
#   - tickers data fields: instType, instId, last, askPx, bidPx, open24h,
#     high24h, low24h, vol24h, volCcy24h, ts
#   - funding-rate data fields: instId, instType, fundingRate, fundingTime,
#     nextFundingRate, nextFundingTime
#   - Keepalive: OKX uses a plain TEXT "ping"/"pong" (not JSON), and drops
#     the connection after 30s of silence — different enough from Bybit's
#     JSON ping that the two providers can't share ping logic.
#
# NOT implemented for OKX: open interest and order book. I could only find
# OKX's REST endpoint names for these (/api/v5/public/open-interest,
# /api/v5/market/books), not a verified WS payload field-by-field shape —
# guessing at field names here would risk silently mislabeling data, so
# get_snapshot() for OKX leaves open_interest/open_interest_value as None
# rather than a wrong guess, and get_orderbook() isn't implemented for OKX
# at all. Confirm the exact WS payload before adding either.

def normalize_okx_ticker(data, coin):
    """One entry from a `tickers` channel data array."""
    out = _empty_snapshot("OKX", coin)
    out["timestamp"] = _to_float(data.get("ts")) / 1000 if data.get("ts") else time.time()
    out["price"] = _to_float(data.get("last"))
    out["volume_24h"] = _to_float(data.get("vol24h"))
    return out


def normalize_okx_funding(data, coin):
    """One entry from a `funding-rate` channel data array."""
    out = _empty_snapshot("OKX", coin)
    ft = _to_float(data.get("fundingTime"))
    nft = _to_float(data.get("nextFundingTime"))
    out["timestamp"] = ft / 1000 if ft else time.time()
    out["funding_rate"] = _to_float(data.get("fundingRate"))
    out["next_funding_time"] = nft / 1000 if nft else None
    return out


class OKXProvider(DerivativesProvider):
    """
    Secondary derivatives provider. Ticker (price) + funding rate only —
    see the NOT-implemented note above for why open interest and order
    book are deliberately left out rather than guessed at.
    """
    name = "OKX"

    def __init__(self, coins=None):
        self._coins = [c for c in (coins or COINS) if c not in ("USDT", "USDC")]
        self._symbol_map = {}
        self._skipped_inst = set()
        for c in self._coins:
            if c in OKX_SWAP_SKIP_COINS:
                logger.info("[DERIV OKX] Unsupported instrument %s-USDT-SWAP — skipped", c)
                continue
            self._symbol_map[c + "-USDT-SWAP"] = c

        self._lock = threading.Lock()
        self._snapshots: dict = {}
        self._last_update: dict = {}

        self._consecutive_failures = 0
        self._connected = False
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="Derivatives-OKX", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def is_healthy(self):
        return self._connected and self._consecutive_failures < UNHEALTHY_AFTER_FAILURES

    def get_snapshot(self, coin):
        with self._lock:
            snap = self._snapshots.get(coin)
            last = self._last_update.get(coin)
        if not snap or not last:
            return None
        if time.time() - last > DERIV_STALE_SECONDS:
            return None
        return dict(snap)

    def get_orderbook(self, coin):
        return None  # not implemented — see class docstring

    def _run(self):
        host = "ws.okx.com"
        path = "/ws/v5/public"
        port = 8443
        backoff = 2
        PING_INTERVAL = 25  # OKX drops the connection after 30s of silence

        while not self._stop_event.is_set():
            sock = None
            try:
                logger.info("[DERIV OKX] Connecting...")
                sock = _protocol_open_websocket(host, path, port)
                sock.settimeout(PING_INTERVAL)
                self._connected = True
                self._consecutive_failures = 0
                backoff = 2
                logger.info("[DERIV OKX] Connected — subscribing %d symbols" % len(self._symbol_map))

                args = []
                for sym in list(self._symbol_map.keys()):
                    if sym in self._skipped_inst:
                        continue
                    args += [{"channel": "tickers", "instId": sym}, {"channel": "funding-rate", "instId": sym}]
                for i in range(0, len(args), 20):
                    _protocol_send_text(sock, json.dumps({"op": "subscribe", "args": args[i:i + 20]}))

                while not self._stop_event.is_set():
                    try:
                        opcode, payload = _protocol_recv_frame(sock)
                    except TimeoutError:
                        _protocol_send_text(sock, "ping")
                        continue
                    except OSError as oe:
                        if "timed out" in str(oe).lower():
                            _protocol_send_text(sock, "ping")
                            continue
                        raise

                    if opcode is None:
                        continue
                    if payload == b"pong":
                        continue
                    self._handle_message(payload)

            except Exception as e:
                self._connected = False
                self._consecutive_failures += 1
                logger.warning(
                    "[DERIV OKX] Disconnected (%d consecutive failures): %s — reconnecting in %ds"
                    % (self._consecutive_failures, e, backoff)
                )
            finally:
                if sock:
                    try: sock.close()
                    except Exception: pass

            if self._stop_event.wait(backoff):
                break
            backoff = min(backoff * 2, 60)

        self._connected = False
        logger.info("[DERIV OKX] Stopped cleanly")

    def _handle_message(self, raw_payload):
        try:
            msg = json.loads(raw_payload.decode("utf-8"))
        except Exception:
            return
        if not isinstance(msg, dict):
            return
        if msg.get("event") in ("subscribe", "error"):
            if msg.get("event") == "error":
                msg_txt = str(msg.get("msg") or "")
                arg = msg.get("arg") or {}
                inst = arg.get("instId") or ""
                # Parse instId from message text if present
                if not inst:
                    for sym in list(self._symbol_map.keys()):
                        if sym in msg_txt:
                            inst = sym
                            break
                if inst and inst not in self._skipped_inst:
                    self._skipped_inst.add(inst)
                    logger.warning("[DERIV OKX] Unsupported instrument %s — skipped", inst)
                elif not inst:
                    logger.warning("[DERIV OKX] Subscribe error: %s" % msg_txt)
            return

        arg = msg.get("arg", {})
        channel = arg.get("channel")
        inst_id = arg.get("instId")
        coin = self._symbol_map.get(inst_id)
        if not coin or channel not in ("tickers", "funding-rate"):
            return

        for entry in msg.get("data", []):
            if channel == "tickers":
                normalized = normalize_okx_ticker(entry, coin)
            else:
                normalized = normalize_okx_funding(entry, coin)
            with self._lock:
                existing = self._snapshots.get(coin)
                if existing:
                    merged = dict(existing)
                    for k, v in normalized.items():
                        if v is not None:
                            merged[k] = v
                    normalized = merged
                self._snapshots[coin] = normalized
                self._last_update[coin] = time.time()


# ═══════════════════════════════════════════════════════════════════════════
# PROVIDER MANAGER — failover wrapper around N providers
# ═══════════════════════════════════════════════════════════════════════════

class ProviderManager:
    """
    Holds providers in priority order (index 0 = primary). Every read call
    walks the list and returns the first healthy+fresh answer. Nothing
    here knows Bybit-specific details — it only calls the DerivativesProvider
    interface, so adding a second provider is a one-line registration.
    """

    def __init__(self, providers, admin_notify=None):
        self._providers = list(providers)
        self._admin_notify = admin_notify  # callable(str) -> notify admin, e.g. send a Telegram message
        self._all_down_notified = False

    def start_all(self):
        for p in self._providers:
            p.start()

    def stop_all(self):
        for p in self._providers:
            p.stop()

    def active_provider_name(self):
        for p in self._providers:
            if p.is_healthy():
                return p.name
        return None

    def get_snapshot(self, coin):
        any_healthy = False
        result = None
        for p in self._providers:
            if not p.is_healthy():
                continue
            any_healthy = True
            snap = p.get_snapshot(coin)
            if snap is not None:
                result = snap
                break
        self._check_all_down(any_healthy)
        return result

    def get_orderbook(self, coin):
        for p in self._providers:
            if not p.is_healthy():
                continue
            book = p.get_orderbook(coin)
            if book is not None:
                return book
        return None

    def _check_all_down(self, any_healthy):
        all_down = not any_healthy and len(self._providers) > 0
        if all_down and not self._all_down_notified:
            msg = "[DERIVATIVES] All providers unhealthy — no funding/OI/orderbook data available."
            logger.error(msg)
            if self._admin_notify:
                try:
                    self._admin_notify(msg)
                except Exception:
                    pass
            self._all_down_notified = True
        elif any_healthy:
            self._all_down_notified = False  # reset so a future outage notifies again


# ═══════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL SINGLETON — the interface the rest of the app should use
# ═══════════════════════════════════════════════════════════════════════════

_manager: "ProviderManager | None" = None


def start_derivatives_engine(admin_notify=None, on_liquidation=None):
    """
    Call once at bot startup. Safe to call multiple times — only starts once.
    `admin_notify` — optional callable(str) to alert the admin if every
    provider goes down (e.g. wire this to send() on your admin chat id).
    `on_liquidation` — optional callable(normalized_liquidation_dict),
    invoked from the provider's background thread — keep it fast/non-blocking.
    """
    global _manager
    if _manager is not None:
        return
    bybit = BybitLinearProvider(on_liquidation=on_liquidation)
    okx = OKXProvider()
    _manager = ProviderManager(providers=[bybit, okx], admin_notify=admin_notify)
    _manager.start_all()


def stop_derivatives_engine():
    global _manager
    if _manager:
        _manager.stop_all()
        _manager = None


def get_derivatives_snapshot(coin):
    """Normalized funding/OI/mark-price snapshot for `coin`, or None if unavailable."""
    if _manager is None:
        return None
    return _manager.get_snapshot(coin)


def get_orderbook(coin):
    """Normalized top-of-book bids/asks for `coin`, or None if unavailable."""
    if _manager is None:
        return None
    return _manager.get_orderbook(coin)


def derivatives_engine_status():
    """Admin health-check string."""
    if _manager is None:
        return "Derivatives Engine: not started"
    active = _manager.active_provider_name()
    lines = [f"Derivatives Engine — active provider: {active or 'NONE (all down)'}"]
    for p in _manager._providers:
        lines.append(f"  {p.name}: {'healthy' if p.is_healthy() else 'DOWN'}")
    return "\n".join(lines)
