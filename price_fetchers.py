"""Market Pulse Bot — price_fetchers module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import COINS, coin_key, kraken_pair, logger
from market_pulse.db import get_db
from market_pulse.helpers import fetch_with_backoff, wat_now
from market_pulse.price_engine import _ws_get_cached


# ─── extracted section ───
# 💰 PRICE FETCHERS
# ═══════════════════════════════════════════════════════════════════════════

_kraken_keymap = {}
_kraken_cache = {"data": {}, "timestamp": None}
_secondary_cache = {"data": {}, "timestamp": None}
_morning_btc_snapshot = {}  # Stores BTC price at morning post time for midday threshold check
_fiat_cache = {"data": {}, "timestamp": None}

def get_kraken_keymap():
    global _kraken_keymap
    if _kraken_keymap:
        return _kraken_keymap
    pairs = sorted({kraken_pair(c) for c in COINS if kraken_pair(c)})
    resp = fetch_with_backoff(f"https://api.kraken.com/0/public/AssetPairs?pair={','.join(pairs)}")
    if resp and not resp.get("error"):
        for key, info in resp.get("result", {}).items():
            altname = info.get("altname")
            if altname:
                _kraken_keymap[altname] = key
    return _kraken_keymap

def get_kraken_batch():
    global _kraken_cache
    now = wat_now()
    if (_kraken_cache["timestamp"] and
            (now - _kraken_cache["timestamp"]).total_seconds() < 15):
        return _kraken_cache["data"]
    pairs = sorted({kraken_pair(c) for c in COINS if kraken_pair(c)})
    resp = fetch_with_backoff(f"https://api.kraken.com/0/public/Ticker?pair={','.join(pairs)}")
    if not resp or resp.get("error"):
        return _kraken_cache["data"]
    keymap = get_kraken_keymap()
    result = resp.get("result", {})
    prices = {}
    for coin in COINS:
        pair = kraken_pair(coin)
        if not pair:
            continue
        entry = result.get(keymap.get(pair, pair))
        if entry:
            try:
                prices[coin] = float(entry["c"][0])
            except Exception as _e:
                logger.debug("[SILENT EXC] %s" % _e)
    _kraken_cache["data"] = prices
    _kraken_cache["timestamp"] = now
    return prices

def get_binance_price(coin):
    """Binance spot REST ticker — free, public, no key. Note: this hits the
    same host/IP-based restriction as the Binance WS stream, so if Binance
    WS is geo-blocked on this server, this will be too — it's not a
    workaround for that, just keeps the REST chain consistent with the
    requested priority order."""
    try:
        resp = fetch_with_backoff(f"https://api.binance.com/api/v3/ticker/price?symbol={coin}USDT")
        if resp and resp.get("price"):
            return float(resp["price"])
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)
    return None

def get_kraken_price(coin):
    if not kraken_pair(coin):
        return None
    return get_kraken_batch().get(coin)

def get_okx_price(coin):
    try:
        resp = fetch_with_backoff(f"https://www.okx.com/api/v5/market/ticker?instId={coin}-USDT")
        if resp and resp.get("code") == "0":
            data = resp.get("data", [])
            if data:
                return float(data[0].get("last", 0))
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)
    return None

def get_bybit_price(coin):
    try:
        resp = fetch_with_backoff(f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={coin}USDT")
        if resp and resp.get("retCode") == 0:
            data = resp.get("result", {}).get("list", [])
            if data:
                return float(data[0].get("lastPrice", 0))
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)
    return None

def get_coingecko_price(coin):
    try:
        coin_id = COINS[coin][1]
        resp = fetch_with_backoff(f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd")
        if resp and coin_id in resp:
            return resp[coin_id].get("usd")
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)
    return None

def get_price_with_fallback(coin):
    # 1. WebSocket cache — always first, zero latency, no REST timeout risk.
    #    Already applies the Binance > Bybit > Kraken priority internally.
    ws_price, _ = _ws_get_cached(coin)
    if ws_price:
        return ws_price
    # 2. Binance REST — primary (same host/IP restriction as Binance WS —
    #    if WS is geo-blocked here, this will fail too, and fall through)
    price = get_binance_price(coin)
    if price:
        return price
    # 3. Bybit REST — secondary
    price = get_bybit_price(coin)
    if price:
        return price
    # 4. Kraken REST batch (cached 15s) — reliable, no API key needed
    price = get_kraken_price(coin)
    if price:
        return price
    # 5. OKX REST — quaternary
    price = get_okx_price(coin)
    if price:
        return price
    # 6. CoinGecko REST — last resort
    price = get_coingecko_price(coin)
    if price:
        return price
    return None

def get_secondary_batch():
    global _secondary_cache
    now = wat_now()
    if (_secondary_cache["timestamp"] and
            (now - _secondary_cache["timestamp"]).total_seconds() < 60):
        return _secondary_cache["data"]
    
    resp = fetch_with_backoff("https://www.okx.com/api/v5/market/tickers?instType=SPOT")
    result = {}
    if resp and resp.get("code") == "0":
        for row in resp.get("data", []):
            inst = row.get("instId", "")
            coin = inst.replace("-USDT", "")
            if coin in COINS:
                try:
                    last = float(row["last"])
                    open24h = float(row["open24h"]) if row.get("open24h") else None
                    change = ((last - open24h) / open24h * 100) if open24h else None
                    result[coin_key(coin)] = {
                        "usd": last,
                        "usd_24h_change": change,
                        "usd_24h_high": float(row["high24h"]) if row.get("high24h") else None,
                        "usd_24h_low": float(row["low24h"]) if row.get("low24h") else None,
                    }
                except Exception as _e:
                    logger.debug("[SILENT EXC] %s" % _e)
    
    if not result:
        # CoinGecko markets endpoint — free, no key, 30 calls/min (replaces CryptoCompare 25/day limit)
        try:
            cg_ids = ",".join(v[1] for v in COINS.values() if v[1])
            resp = fetch_with_backoff(
                f"https://api.coingecko.com/api/v3/coins/markets"
                f"?vs_currency=usd&ids={cg_ids}&order=market_cap_desc&per_page=100"
                f"&price_change_percentage=24h"
            )
            if resp:
                for item in resp:
                    cg_id = item.get("id")
                    if cg_id:
                        result[cg_id] = {
                            "usd": item.get("current_price"),
                            "usd_24h_change": item.get("price_change_percentage_24h"),
                            "usd_24h_high": item.get("high_24h"),
                            "usd_24h_low": item.get("low_24h"),
                        }
        except Exception as e:
            logger.warning("[SECONDARY BATCH] CoinGecko: %s" % e)
    
    _secondary_cache["data"] = result
    _secondary_cache["timestamp"] = now
    return result

def get_secondary_coin(coin):
    return get_secondary_batch().get(coin_key(coin))

def get_best_price(coin):
    """Return (price, change_24h_pct) using WebSocket cache when available,
    falling back through Kraken/OKX/Bybit/CoinGecko REST as needed."""
    if coin not in COINS:
        return None, None

    # Try WebSocket cache first — includes change% when available
    ws_price, ws_change = _ws_get_cached(coin)
    if ws_price:
        # If WS gave us a change%, use it; otherwise supplement from secondary batch
        if ws_change is not None:
            return ws_price, ws_change
        sd = get_secondary_coin(coin)
        change = sd.get("usd_24h_change") if sd else None
        return ws_price, change

    # Fall back to REST chain
    price = get_price_with_fallback(coin)
    sd = get_secondary_coin(coin)
    change = sd.get("usd_24h_change") if sd else None
    if price:
        return price, change
    if sd:
        return sd.get("usd"), change
    return None, None

def get_fiat_rates():
    from market_pulse.p2p import get_p2p_rate
    """Get USD-based fiat exchange rates. Cached 4 hours.
    Sources: frankfurter.app (primary, unlimited) + open.er-api.com (fallback).
    NGN added separately via P2P-derived rate since frankfurter.app excludes NGN."""
    global _fiat_cache
    now = wat_now()
    if (_fiat_cache["timestamp"] and
            (now - _fiat_cache["timestamp"]).total_seconds() < 14400):
        return _fiat_cache["data"]

    rates = {}

    # Primary: frankfurter.app — no rate limit, no API key needed
    try:
        resp = fetch_with_backoff("https://api.frankfurter.app/latest?from=USD")
        if resp and "rates" in resp:
            rates = resp["rates"]
            rates["USD"] = 1.0
            logger.info("[FIAT RATES] frankfurter.app loaded %d rates" % len(rates))
    except Exception as e:
        logger.warning("[FIAT RATES] frankfurter: %s" % e)

    # Fallback to open.er-api.com if primary failed
    if not rates:
        try:
            resp = fetch_with_backoff("https://open.er-api.com/v6/latest/USD")
            if resp and "rates" in resp:
                rates = resp["rates"]
                logger.info("[FIAT RATES] er-api fallback loaded %d rates" % len(rates))
        except Exception as e:
            logger.warning("[FIAT RATES] er-api fallback: %s" % e)

    # ── NGN: frankfurter.app excludes NGN ────────────────────────────────
    # Derive NGN rate from Binance P2P USDT/NGN (most accurate parallel rate)
    # Since USDT ≈ $1, USDT/NGN rate ≈ USD/NGN parallel market rate
    if "NGN" not in rates:
        try:
            buy_ngn, sell_ngn, source = get_p2p_rate("USDT", "NGN")
            if buy_ngn and sell_ngn:
                # Use midpoint of buy/sell as the USD/NGN rate
                ngn_rate = (buy_ngn + sell_ngn) / 2
                rates["NGN"] = round(ngn_rate, 2)
                logger.info("[FIAT RATES] NGN derived from P2P: ₦%.0f/USD (source: %s)" % (ngn_rate, source))
        except Exception as e:
            logger.warning("[FIAT RATES] NGN P2P derivation: %s" % e)

    if rates:
        _fiat_cache["data"] = rates
        _fiat_cache["timestamp"] = now

    return _fiat_cache["data"] or {}

# ═══════════════════════════════════════════════════════════════════════════
# 📊 MISSING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

_STABLECOINS = {"USDT", "USDC"}  # Exclude from gainers/losers

def get_gainers_losers():
    prices = {}
    for coin in COINS:
        if coin in _STABLECOINS:
            continue
        price, change = get_best_price(coin)
        if price and change is not None:
            prices[coin] = {"price": price, "change": change}
    
    if not prices:
        return [], []
    
    sorted_coins = sorted(prices.items(), key=lambda x: x[1]["change"], reverse=True)
    gainers = [(c, p["price"], p["change"]) for c, p in sorted_coins[:5] if p["change"] > 0]
    losers = [(c, p["price"], p["change"]) for c, p in sorted_coins[-5:] if p["change"] < 0]
    return gainers, losers

def get_okx_batch():
    try:
        resp = fetch_with_backoff("https://www.okx.com/api/v5/market/tickers?instType=SPOT")
        if resp and resp.get("code") == "0":
            result = {}
            for row in resp.get("data", []):
                inst = row.get("instId", "")
                coin = inst.replace("-USDT", "")
                if coin in COINS:
                    result[coin] = {"price": float(row["last"])}
            return result
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)
    return {}

def get_coingecko_batch():
    try:
        # COINS[symbol] = (kraken_pair, coingecko_id) — unpack correctly
        ids = [v[1] for v in COINS.values()]
        resp = fetch_with_backoff(f"https://api.coingecko.com/api/v3/simple/price?ids={','.join(ids)}&vs_currencies=usd")
        if resp:
            result = {}
            for coin, (_, cg_id) in COINS.items():
                if cg_id in resp and resp[cg_id].get("usd"):
                    result[coin] = {"price": resp[cg_id]["usd"]}
            return result
    except Exception as e:
        logger.warning("[COINGECKO BATCH] %s" % e)
    return {}

def save_price_history():
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for coin in COINS:
            price, _ = get_best_price(coin)
            if price and price > 0:
                rows.append((coin, price, now))
        if rows:
            c.executemany("INSERT INTO history (coin, price, timestamp) VALUES (%s, %s, %s)", rows)
            db.commit()
            logger.info("[HISTORY] Saved %d price records" % len(rows))
    except Exception as e:
        logger.error("[HISTORY ERROR] %s" % e)
        if db:
            try: db.rollback()
            except Exception: pass
    finally:
        if db:
            try: db.close()
            except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════
