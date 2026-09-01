"""Market Pulse Bot — P2P system (USDT / EUR / GBP vs NGN).

Upgrades:
- Multi-asset: USDT, EUR, GBP (vs NGN)
- Always show source (Binance / Bybit / Estimated / Unavailable)
- In-memory cache to avoid API hammering
- Soft failure messages
- Hourly history in DB + vs 24h / 7d comparison
- Spread quality labels + advice
- Optional user target alerts (buy below X)
- Soft Fear & Greed context on intelligence cards
"""

from __future__ import annotations

import random
import threading
import time
from datetime import timedelta

import requests

from market_pulse.config_runtime import USER_AGENTS, logger
from market_pulse.db import get_db
from market_pulse.helpers import wat_now
from market_pulse.price_fetchers import get_best_price, get_fiat_rates

# ── Supported assets (all quoted vs NGN for Nigerian users) ─────────────────
P2P_ASSETS = ("USDT", "EUR", "GBP")
P2P_FIAT = "NGN"

# Cache: key -> {buy, sell, source, ts}
_p2p_cache: dict = {}
_p2p_lock = threading.Lock()
_P2P_CACHE_TTL = 300  # 5 minutes

# Bybit P2P often returns 404 for NGN pairs here — cool down instead of spamming.
_bybit_p2p_cooldown_until: dict = {}  # "ASSET/FIAT" -> unix ts
_BYBIT_P2P_COOLDOWN_SEC = 1800  # 30 minutes
_bybit_p2p_cooldown_logged: set = set()


def _p2p_median(prices):
    if not prices:
        return None
    prices = sorted(prices)
    return prices[len(prices) // 2]


def get_binance_p2p(side, asset, fiat_code):
    try:
        headers = {
            "Content-Type": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json",
            "Referer": "https://p2p.binance.com/",
            "Origin": "https://p2p.binance.com",
        }
        payload = {
            "asset": asset,
            "fiat": fiat_code,
            "merchantCheck": False,
            "page": 1,
            "publisherType": None,
            "rows": 10,
            "tradeType": side,
        }
        resp = requests.post(
            "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search",
            json=payload,
            headers=headers,
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning("[BINANCE P2P] HTTP %s for %s/%s %s", resp.status_code, asset, fiat_code, side)
            return None
        ads = (resp.json() or {}).get("data") or []
        prices = []
        for a in ads:
            try:
                price = (a.get("adv") or {}).get("price")
                if price:
                    prices.append(float(price))
            except Exception:
                continue
        return _p2p_median(prices) if prices else None
    except Exception as e:
        logger.error("[BINANCE P2P ERROR] %s/%s %s: %s", asset, fiat_code, side, e)
        return None


def get_bybit_p2p(side, asset, fiat_code):
    """Bybit OTC list. On HTTP 404, cool down the pair to avoid log spam."""
    pair_key = f"{(asset or '').upper()}/{(fiat_code or '').upper()}"
    now = time.time()
    until = _bybit_p2p_cooldown_until.get(pair_key, 0)
    if now < until:
        return None  # still in cooldown — silent

    try:
        bybit_side = "1" if side == "BUY" else "0"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
        }
        resp = requests.post(
            "https://api2.bybit.com/fiat/otc/item/list",
            json={
                "userId": "",
                "tokenId": asset,
                "currencyId": fiat_code,
                "payment": [],
                "side": bybit_side,
                "size": "10",
                "page": "1",
                "amount": "",
                "authMaker": False,
                "canTrade": False,
            },
            headers=headers,
            timeout=10,
        )
        if resp.status_code == 404:
            _bybit_p2p_cooldown_until[pair_key] = now + _BYBIT_P2P_COOLDOWN_SEC
            if pair_key not in _bybit_p2p_cooldown_logged:
                _bybit_p2p_cooldown_logged.add(pair_key)
                logger.warning(
                    "[P2P] Bybit source returned 404 for %s — disabled for %s minutes",
                    pair_key,
                    int(_BYBIT_P2P_COOLDOWN_SEC // 60),
                )
            return None
        if resp.status_code != 200:
            logger.warning("[BYBIT P2P] HTTP %s for %s/%s %s", resp.status_code, asset, fiat_code, side)
            return None
        items = ((resp.json() or {}).get("result") or {}).get("items") or []
        prices = [float(i["price"]) for i in items if i.get("price")]
        # Success clears cooldown for this pair
        _bybit_p2p_cooldown_until.pop(pair_key, None)
        _bybit_p2p_cooldown_logged.discard(pair_key)
        return _p2p_median(prices) if prices else None
    except Exception as e:
        logger.error("[BYBIT P2P ERROR] %s/%s %s: %s", asset, fiat_code, side, e)
        return None


def _estimate_p2p(crypto, fiat):
    """Last-resort estimate from spot + fiat FX. Marked clearly as Estimated."""
    try:
        rates = get_fiat_rates() or {}
        fiat_per_usd = rates.get(fiat)
        if not fiat_per_usd:
            return None, None

        crypto = (crypto or "USDT").upper()
        if crypto in ("USDT", "USD", "USDC"):
            val = float(fiat_per_usd)
        elif crypto in ("EUR", "GBP"):
            # rates[crypto] is units of crypto per 1 USD from Frankfurter-style table
            # We need fiat per 1 crypto: (fiat per USD) / (crypto per USD)
            c_per_usd = rates.get(crypto)
            if not c_per_usd or c_per_usd <= 0:
                return None, None
            # If EUR rate is "how many EUR per 1 USD", then 1 EUR = fiat_per_usd / c_per_usd NGN
            # Actually Frankfurter returns: amount of currency for 1 EUR base often —
            # In this codebase get_fiat_rates returns map like NGN: 1600 meaning 1 USD = 1600 NGN
            # and EUR: 0.92 meaning 1 USD = 0.92 EUR → 1 EUR = NGN/EUR_per_USD
            val = float(fiat_per_usd) / float(c_per_usd)
        else:
            price, _ = get_best_price(crypto)
            if not price:
                return None, None
            val = float(price) * float(fiat_per_usd)

        buy = round(val * 1.015, 2)
        sell = round(val * 0.985, 2)
        return buy, sell
    except Exception as e:
        logger.warning("[P2P ESTIMATE] %s/%s: %s", crypto, fiat, e)
        return None, None


def get_p2p_rate(crypto="USDT", fiat="NGN", use_cache=True):
    """
    Return (buy, sell, source_str).

    source_str is always one of:
      Binance P2P | Bybit P2P | Estimated ⚠️ | Unavailable
    """
    crypto = (crypto or "USDT").upper().strip()
    fiat = (fiat or "NGN").upper().strip()
    if crypto == "USD":
        crypto = "USDT"
    if crypto == "POUND" or crypto == "POUNDS":
        crypto = "GBP"

    cache_key = f"{crypto}/{fiat}"
    now = time.time()
    if use_cache:
        with _p2p_lock:
            hit = _p2p_cache.get(cache_key)
            if hit and now - hit["ts"] < _P2P_CACHE_TTL:
                return hit["buy"], hit["sell"], hit["source"]

    buy = sell = None
    source = "Unavailable"

    try:
        buy = get_binance_p2p("BUY", crypto, fiat)
        sell = get_binance_p2p("SELL", crypto, fiat)
        if buy and sell and buy > 0 and sell > 0:
            source = "Binance P2P"
    except Exception as e:
        logger.debug("[P2P] Binance path: %s", e)

    if source == "Unavailable":
        try:
            buy = get_bybit_p2p("BUY", crypto, fiat)
            sell = get_bybit_p2p("SELL", crypto, fiat)
            if buy and sell and buy > 0 and sell > 0:
                source = "Bybit P2P"
        except Exception as e:
            logger.debug("[P2P] Bybit path: %s", e)

    if source == "Unavailable":
        eb, es = _estimate_p2p(crypto, fiat)
        if eb and es:
            buy, sell, source = eb, es, "Estimated ⚠️"
        else:
            buy, sell, source = None, None, "Unavailable"
            logger.warning("[P2P] Unavailable for %s/%s", crypto, fiat)

    with _p2p_lock:
        _p2p_cache[cache_key] = {
            "buy": buy,
            "sell": sell,
            "source": source,
            "ts": now,
        }

    return buy, sell, source


def spread_quality(buy, sell):
    """Return (label, advice) for NGN spreads."""
    if not buy or not sell or buy <= 0:
        return "Unknown", "Rates unavailable — check Binance/Bybit P2P manually."
    spread = abs(float(buy) - float(sell))
    pct = spread / float(buy) * 100
    if spread <= 25 or pct <= 0.5:
        return "Tight", "Reasonable to convert if you need naira or USDT now."
    if spread >= 60 or pct >= 1.5:
        return "Wide", "Spread is wide — wait for compression unless urgent."
    return "Moderate", "Convert only if you need the funds; spread is okay, not perfect."


def _avg_from_history(crypto, fiat, hours):
    db = None
    try:
        db = get_db()
        c = db.cursor()
        since = (wat_now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """
            SELECT AVG(buy_rate), AVG(sell_rate)
            FROM p2p_rate_history
            WHERE asset=%s AND fiat=%s AND recorded_at >= %s
              AND buy_rate IS NOT NULL AND sell_rate IS NOT NULL
            """,
            (crypto, fiat, since),
        )
        row = c.fetchone()
        if row and row[0] is not None:
            return float(row[0]), float(row[1])
    except Exception as e:
        logger.debug("[P2P HISTORY AVG] %s", e)
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass
    return None, None


def record_p2p_rate(crypto="USDT", fiat="NGN", buy=None, sell=None, source=None):
    """Persist one snapshot (call from hourly job / intelligence builders)."""
    if buy is None or sell is None:
        return
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        spread = float(buy) - float(sell)
        c.execute(
            """
            INSERT INTO p2p_rate_history (asset, fiat, buy_rate, sell_rate, spread, source, recorded_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            (crypto, fiat, float(buy), float(sell), spread, source or "", now),
        )
        db.commit()
    except Exception as e:
        logger.warning("[P2P HISTORY SAVE] %s", e)
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


def format_p2p_card(crypto="USDT", fiat="NGN", include_history=True, fg_val=None):
    """Build a Telegram HTML card for one asset."""
    buy, sell, source = get_p2p_rate(crypto, fiat)
    if not buy or not sell:
        return (
            f"💱 <b>{crypto}/{fiat} P2P</b>\n"
            f"Status: <b>Unavailable</b>\n"
            f"Source: {source}\n"
            f"<i>Check Binance/Bybit P2P manually. Bot will retry on next cycle.</i>"
        )

    record_p2p_rate(crypto, fiat, buy, sell, source)
    spread = float(buy) - float(sell)
    label, advice = spread_quality(buy, sell)

    lines = [
        f"💱 <b>{crypto}/{fiat} P2P Rates</b>",
        f"Buy:  <b>₦{int(buy):,}</b>",
        f"Sell: <b>₦{int(sell):,}</b>",
        f"Spread: <b>₦{int(abs(spread)):,}</b>  ·  Quality: <b>{label}</b>",
        f"Source: <b>{source}</b>",
        f"Time: {wat_now().strftime('%Y-%m-%d %H:%M')} WAT",
        "",
        f"💡 {advice}",
    ]

    if include_history:
        a24b, a24s = _avg_from_history(crypto, fiat, 24)
        a7b, a7s = _avg_from_history(crypto, fiat, 24 * 7)
        if a24b:
            d24 = float(buy) - a24b
            arrow = "↑" if d24 > 0 else ("↓" if d24 < 0 else "→")
            lines.append(f"vs 24h avg buy: {arrow} ₦{int(abs(d24)):,}")
        if a7b:
            d7 = float(buy) - a7b
            arrow = "↑" if d7 > 0 else ("↓" if d7 < 0 else "→")
            lines.append(f"vs 7d avg buy: {arrow} ₦{int(abs(d7)):,}")

    if fg_val is not None:
        try:
            fg = int(fg_val)
            if fg >= 70:
                lines.append("Sentiment: High greed — USDT demand can stay firm.")
            elif fg <= 30:
                lines.append("Sentiment: Fear — some sellers may ease USDT premiums.")
        except Exception:
            pass

    lines += ["", "<i>Rates change through the day. NFA — confirm on the exchange before trading.</i>"]
    return "\n".join(lines)


def format_multi_p2p_intelligence(assets=None, title="P2P INTELLIGENCE"):
    """Morning/evening multi-asset read for Pro (and /p2p all)."""
    assets = assets or list(P2P_ASSETS)
    fg_val = None
    try:
        from market_pulse.fear_greed import get_fear_greed
        fg = get_fear_greed()
        fg_val = fg[0]["value"] if fg else None
    except Exception:
        pass

    blocks = [f"💱 <b>{title}</b>", f"{wat_now().strftime('%Y-%m-%d %H:%M')} WAT", ""]
    for asset in assets:
        buy, sell, source = get_p2p_rate(asset, "NGN")
        if buy and sell:
            record_p2p_rate(asset, "NGN", buy, sell, source)
            spread = abs(float(buy) - float(sell))
            label, _ = spread_quality(buy, sell)
            blocks.append(
                f"<b>{asset}/NGN</b>\n"
                f"Buy ₦{int(buy):,} · Sell ₦{int(sell):,} · Spread ₦{int(spread):,} ({label})\n"
                f"Source: {source}"
            )
            a24b, _ = _avg_from_history(asset, "NGN", 24)
            if a24b:
                d = float(buy) - a24b
                blocks.append(f"vs 24h: {'+' if d >= 0 else ''}{int(d):,} ₦")
            blocks.append("")
        else:
            blocks.append(f"<b>{asset}/NGN</b> — Unavailable ({source})\n")

    if fg_val is not None:
        blocks.append(f"Fear & Greed: {fg_val}/100 (soft context only)")
    blocks.append("<i>P2P ads move fast. Confirm on Binance/Bybit before sending funds. NFA.</i>")
    return "\n".join(blocks)


def record_all_p2p_snapshots():
    """Hourly job: refresh cache + write history for USDT/EUR/GBP."""
    for asset in P2P_ASSETS:
        try:
            buy, sell, source = get_p2p_rate(asset, "NGN", use_cache=False)
            if buy and sell:
                record_p2p_rate(asset, "NGN", buy, sell, source)
                logger.info("[P2P SNAPSHOT] %s/NGN buy=%s sell=%s (%s)", asset, buy, sell, source)
        except Exception as e:
            logger.warning("[P2P SNAPSHOT] %s: %s", asset, e)





def check_channel_usdt_ngn_pulse():
    """Post when USDT/NGN mid moves ~1%+; 3h cooldown. Event-driven, not clock-driven."""
    from market_pulse.telegram_api import post_to_channel, post_to_pro_channel
    from market_pulse.helpers import wat_now
    from market_pulse.db import get_db
    from datetime import datetime as _dt
    import logging
    logger = logging.getLogger("market_pulse")

    buy, sell, src = get_p2p_rate("USDT", "NGN", use_cache=False)
    if not buy or not sell:
        return False
    mid = (float(buy) + float(sell)) / 2.0
    spread = abs(float(buy) - float(sell))
    label, advice = spread_quality(buy, sell)

    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT value, updated_at FROM admin_settings WHERE key=%s", ("p2p_channel_pulse_usdt",))
        row = c.fetchone()
        now = wat_now()
        if row:
            try:
                last_mid = float(row[0])
                last_ts = _dt.strptime(str(row[1])[:19], "%Y-%m-%d %H:%M:%S")
                age_h = (now - last_ts).total_seconds() / 3600.0
                move_pct = abs(mid - last_mid) / last_mid * 100.0 if last_mid > 0 else 99.0
                if age_h < 3.0 and move_pct < 1.0:
                    return False
            except Exception:
                pass

        free_msg = (
            "💱 <b>USDT/NGN PULSE</b>\n\n"
            + f"Buy <b>₦{int(buy):,}</b>  ·  Sell <b>₦{int(sell):,}</b>\n"
            + f"Spread <b>₦{int(spread):,}</b>  ·  {label}\n"
            + f"<i>{src or 'P2P'}  ·  NFA</i>"
        )
        pro_msg = (
            "💱 <b>PRO — USDT/NGN PULSE</b>\n\n"
            + f"Buy <b>₦{int(buy):,}</b>  ·  Sell <b>₦{int(sell):,}</b>\n"
            + f"Mid ~₦{int(mid):,}  ·  Spread ₦{int(spread):,} ({label})\n"
            + f"{advice}\n\n"
            + f"<i>Source: {src or 'P2P'}  ·  Nigerian street rate  ·  NFA</i>"
        )
        post_to_channel(free_msg)
        post_to_pro_channel(pro_msg)
        now_s = now.strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO admin_settings (key, value, updated_at) VALUES (%s,%s,%s) "
            "ON CONFLICT (key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
            ("p2p_channel_pulse_usdt", str(mid), now_s),
        )
        db.commit()
        logger.info("[P2P PULSE] USDT/NGN mid=%s", int(mid))
        return True
    except Exception as e:
        logger.warning("[P2P PULSE] %s", e)
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


def check_user_p2p_alerts():
    """Notify users when buy rate is at or below their target (p2p_user_alerts table)."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            """
            SELECT id, chat_id, asset, fiat, target_buy, active
            FROM p2p_user_alerts
            WHERE active = 1
            """
        )
        rows = c.fetchall() or []
    except Exception as e:
        logger.debug("[P2P USER ALERTS] table/query: %s", e)
        return
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass

    if not rows:
        return

    from market_pulse.telegram_api import send

    for row in rows:
        try:
            alert_id, chat_id, asset, fiat, target, active = row
            buy, sell, source = get_p2p_rate(asset or "USDT", fiat or "NGN")
            if not buy or target is None:
                continue
            if float(buy) <= float(target):
                send(
                    chat_id,
                    (
                        f"🔔 <b>P2P rate alert</b>\n\n"
                        f"{asset}/{fiat} buy is <b>₦{int(buy):,}</b> "
                        f"(your target ≤ ₦{int(float(target)):,})\n"
                        f"Sell: ₦{int(sell):,} · Source: {source}\n\n"
                        f"<i>Confirm on the exchange before trading. NFA.</i>"
                    ),
                )
                # deactivate one-shot
                db2 = get_db()
                try:
                    c2 = db2.cursor()
                    c2.execute("UPDATE p2p_user_alerts SET active=0 WHERE id=%s", (alert_id,))
                    db2.commit()
                finally:
                    try:
                        db2.close()
                    except Exception:
                        pass
        except Exception as e:
            logger.warning("[P2P USER ALERT] %s", e)


def set_user_p2p_alert(chat_id, asset, target_buy, fiat="NGN"):
    """Create/replace a user alert. Returns (ok, message)."""
    asset = (asset or "USDT").upper()
    if asset not in P2P_ASSETS:
        return False, f"Asset must be one of: {', '.join(P2P_ASSETS)}"
    try:
        target = float(str(target_buy).replace(",", "").replace("₦", "").strip())
    except Exception:
        return False, "Invalid target number."
    if target <= 0:
        return False, "Target must be positive."

    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            """
            INSERT INTO p2p_user_alerts (chat_id, asset, fiat, target_buy, active, created_at)
            VALUES (%s,%s,%s,%s,1,%s)
            """,
            (str(chat_id), asset, fiat, target, now),
        )
        db.commit()
        return True, f"Alert set: notify when {asset}/{fiat} buy ≤ ₦{int(target):,}"
    except Exception as e:
        logger.error("[P2P SET ALERT] %s", e)
        return False, "Could not save alert. Try again later."
    finally:
        if db:
            try:
                db.close()
            except Exception:
                pass
