"""Market Pulse Bot — channel_posts module (split from the real monolithic bot.py)."""

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

from market_pulse.ai_engine import ask_ai
from market_pulse.alert_formatter import build_alert_message, build_no_signal_message
from market_pulse.candle_engine import candles_ready, get_candles
from market_pulse.config_runtime import logger
from market_pulse.db import get_db
from market_pulse.fear_greed import get_fear_greed, get_latest_news
from market_pulse.helpers import format_change, format_price, wat_now
from market_pulse.p2p import get_p2p_rate
from market_pulse.price_fetchers import get_best_price, get_gainers_losers, get_secondary_coin
from market_pulse.signal_engine import analyze


# ─── extracted section ───
# 📊 CHANNEL POST BUILDERS
# ═══════════════════════════════════════════════════════════════════════════

def _morning_data():
    btc_price, btc_change = get_best_price("BTC")
    eth_price, eth_change = get_best_price("ETH")
    sol_price, sol_change = get_best_price("SOL")
    bnb_price, bnb_change = get_best_price("BNB")
    fg_data   = get_fear_greed()
    gainers, losers = get_gainers_losers()
    today    = wat_now().strftime("%A, %b %d")
    time_str = wat_now().strftime("%I:%M %p WAT")
    btc_sd   = get_secondary_coin("BTC")
    btc_high = btc_sd.get("usd_24h_high") if btc_sd else None
    btc_low  = btc_sd.get("usd_24h_low")  if btc_sd else None
    buy, sell, p2p_src = get_p2p_rate("USDT", "NGN")
    eur_buy, eur_sell, _ = get_p2p_rate("EUR", "NGN")
    gbp_buy, gbp_sell, _ = get_p2p_rate("GBP", "NGN")
    # Approximate naira marks using P2P mid (street rate)
    mid = None
    if buy and sell:
        mid = (float(buy) + float(sell)) / 2.0
    btc_ngn = (float(btc_price) * mid) if (btc_price and mid) else None
    eth_ngn = (float(eth_price) * mid) if (eth_price and mid) else None
    return dict(
        btc_price=btc_price, btc_change=btc_change,
        eth_price=eth_price, eth_change=eth_change,
        sol_price=sol_price, sol_change=sol_change,
        bnb_price=bnb_price, bnb_change=bnb_change,
        fg_data=fg_data, gainers=gainers, losers=losers,
        today=today, time_str=time_str,
        btc_high=btc_high, btc_low=btc_low,
        buy=buy, sell=sell, p2p_src=p2p_src or "",
        eur_buy=eur_buy, eur_sell=eur_sell,
        gbp_buy=gbp_buy, gbp_sell=gbp_sell,
        btc_ngn=btc_ngn, eth_ngn=eth_ngn, mid=mid,
    )

def _morning_base(d):
    fg_val = d["fg_data"][0]["value"] if d["fg_data"] else "N/A"
    fg_lbl = d["fg_data"][0]["value_classification"] if d["fg_data"] else "Neutral"
    lines = [
        "\U0001f305 <b>MARKET PULSE — MORNING BRIEFING</b>",
        f"<i>{d['today']}  |  {d['time_str']}</i>", "",
        "· · · · · · · · · · · · · · · · · · ·", "",
        f"📈 BTC: <b>{format_price(d['btc_price'])}</b>  {format_change(d['btc_change'])}"
        + (f"  ·  ~₦{int(d['btc_ngn']):,}" if d.get('btc_ngn') else ""),
        f"📈 ETH: <b>{format_price(d['eth_price'])}</b>  {format_change(d['eth_change'])}",
        f"📈 SOL: <b>{format_price(d['sol_price'])}</b>  {format_change(d['sol_change'])}",
        f"📈 BNB: <b>{format_price(d['bnb_price'])}</b>  {format_change(d['bnb_change'])}",
        "",
        f"🧠 Fear & Greed: <b>{fg_val}/100</b> — {fg_lbl}", "",
    ]
    if d.get("btc_high") and d.get("btc_low"):
        lines += [f"📊 BTC 24h Range: <b>{format_price(d['btc_low'])}</b> — <b>{format_price(d['btc_high'])}</b>", ""]
    if d.get("gainers"):
        lines += [f"📈 <b>TOP MOVER:</b> <b>{d['gainers'][0][0]}</b> +{d['gainers'][0][2]:.2f}%", ""]
    if d["buy"] and d["sell"]:
        lines += [f"💱 <b>USDT/NGN</b>  Buy \u20a6{int(d['buy']):,}  |  Sell \u20a6{int(d['sell']):,}  Spread \u20a6{int(d['buy']-d['sell']):,}", ""]
    return lines

def build_morning_briefing():
    """Free — Nigerian pulse: majors + USDT/NGN + naira marks."""
    d = _morning_data()
    fg_val = d["fg_data"][0]["value"] if d["fg_data"] else "N/A"
    fg_num = int(fg_val) if str(fg_val).isdigit() else 50
    if fg_num <= 25:   mood = "Extreme Fear — historically a zone of opportunity, still manage risk."
    elif fg_num <= 45: mood = "Fear — cautious. Wait for confirmation."
    elif fg_num <= 60: mood = "Neutral — no forced bias."
    elif fg_num <= 80: mood = "Greed — protect gains; avoid FOMO."
    else:              mood = "Extreme Greed — overheated risk."

    lines = [
        "🌅 <b>MORNING BRIEFING</b>",
        f"<i>{d['today']}  ·  {d['time_str']}</i>",
        "",
        "🇳🇬 <b>NAIRA DESK</b>",
    ]
    if d.get("buy") and d.get("sell"):
        spread = int(d["buy"] - d["sell"])
        lines.append(
            f"💱 <b>USDT/NGN</b>  Buy ₦{int(d['buy']):,}  ·  Sell ₦{int(d['sell']):,}  ·  Spread ₦{spread:,}"
        )
    else:
        lines.append("💱 USDT/NGN — rate unavailable right now")
    lines += ["", "📈 <b>CRYPTO (NG focus)</b>"]
    if d.get("btc_price"):
        btc_line = f"BTC  <b>{format_price(d['btc_price'])}</b>  {format_change(d['btc_change'])}"
        if d.get("btc_ngn"):
            btc_line += f"  ·  ~₦{int(d['btc_ngn']):,}"
        lines.append(btc_line)
    if d.get("sol_price"):
        lines.append(f"SOL  <b>{format_price(d['sol_price'])}</b>  {format_change(d['sol_change'])}")
    if d.get("eth_price"):
        eth_line = f"ETH  <b>{format_price(d['eth_price'])}</b>  {format_change(d['eth_change'])}"
        if d.get("eth_ngn"):
            eth_line += f"  ·  ~₦{int(d['eth_ngn']):,}"
        lines.append(eth_line)
    if d.get("bnb_price"):
        lines.append(f"BNB  <b>{format_price(d['bnb_price'])}</b>  {format_change(d['bnb_change'])}")
    lines += [
        "",
        f"🧠 Fear & Greed  <b>{fg_val}/100</b>",
        f"<i>{mood}</i>",
        "",
        "📌 Key levels & USDT rate moves → channels when conditions are real (not on a clock).",
        "💎 Pro: deeper P2P, EUR/GBP, scenarios, trade tiers.",
        "",
        "<i>NFA — DYOR  ·  ⚡ Market Pulse</i>",
    ]
    return "\n".join(lines)


def get_track_record_line(limit=10):
    """
    Summarizes the last `limit` closed trade ideas as a win/loss line for
    the top of the Pro morning briefing. Returns "" if there's no closed
    history yet (nothing to show), rather than a misleading 0-0 record.
    """
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            "SELECT result FROM trade_ideas WHERE status='closed' AND result IS NOT NULL "
            "ORDER BY id DESC LIMIT %s", (limit,)
        )
        results = [r[0] for r in c.fetchall()]
        if not results:
            return ""
        wins = sum(1 for r in results if str(r).lower() in ("win", "won", "tp", "profit"))
        losses = len(results) - wins
        win_rate = round(wins / len(results) * 100)
        return f"\U0001f3c6 <b>Track Record</b> — last {len(results)} calls: {wins}W-{losses}L ({win_rate}%)"
    except Exception as e:
        logger.debug("[TRACK RECORD] %s" % e)
        return ""
    finally:
        if db:
            try: db.close()
            except Exception: pass


def update_pro_decision(coin, status, entry, stop, target):
    """
    Upserts the bot's current standing call on `coin` — fixes a real bug
    where this was called but never implemented anywhere in the bot.
    """
    db = None
    try:
        db = get_db()
        c = db.cursor()
        now = wat_now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute(
            "INSERT INTO pro_decisions (coin, status, entry, stop, target, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (coin) DO UPDATE SET "
            "status=EXCLUDED.status, entry=EXCLUDED.entry, stop=EXCLUDED.stop, "
            "target=EXCLUDED.target, updated_at=EXCLUDED.updated_at",
            (coin, status, entry, stop, target, now)
        )
        db.commit()
        return True
    except Exception as e:
        logger.debug("[PRO DECISION] %s" % e)
        if db:
            try: db.rollback()
            except Exception: pass
        return False
    finally:
        if db:
            try: db.close()
            except Exception: pass


def _btc_technical_grounding():
    """
    Returns (result_or_None, technical_block_str) — the real signal_engine
    analysis for BTC, plus a plain-text summary meant to be injected into
    an AI prompt as grounding data. Used by every Pro AI-narrative function
    (morning/midday/evening/weekly) so none of them ask the AI to invent a
    specific Entry/Stop/Target from a prompt with no real technical basis —
    that gap was the likely root cause of inaccurate AI trade levels.
    """
    result = None
    technical_block = ("No real-time technical read available — do not state a specific "
                        "Entry/Stop/Target; speak generally instead.")
    try:
        if candles_ready("BTC"):
            result = analyze(get_candles("BTC"), symbol="BTC")
            if result.get("signal"):
                technical_block = (
                    f"BTC real technical read: {result['direction'].upper()} bias, "
                    f"{result['category']} setup, {result['confidence']}% confidence. "
                    f"Entry {format_price(result['entry'])}, "
                    f"Stop {format_price(result['stop_loss'])}, "
                    f"Target {format_price(result['take_profit'][0])}. "
                    f"Confirmed by: {'; '.join(result['reasons'][:3])}."
                )
            else:
                reason = (result.get("risks") or ["no clean setup right now"])[0]
                technical_block = (
                    f"BTC technical read: no confirmed setup right now ({reason}). "
                    f"Do not state a specific Entry/Stop/Target — say markets are "
                    f"unclear/consolidating instead."
                )
    except Exception as e:
        logger.debug("[SIGNAL ENGINE - AI GROUNDING] %s" % e)
    return result, technical_block


def _append_signal_engine_section(lines, btc_signal_result):
    """Shared transparency block: shows exactly what grounded the AI's
    Entry/Stop/Target above, appended after every Pro AI-narrative section."""
    try:
        if btc_signal_result is not None:
            lines += ["", "· · · · · · · · · · · · · · · · · · ·", "",
                      "📐 <b>SIGNAL ENGINE — REAL TECHNICAL DATA</b>",
                      "<i>Rule-based, not AI — this is what grounded the analysis above</i>", ""]
            if btc_signal_result.get("signal"):
                lines.append(build_alert_message("BTC/USDT", btc_signal_result))
            else:
                lines.append(build_no_signal_message("BTC/USDT", btc_signal_result))
    except Exception as e:
        logger.debug("[SIGNAL ENGINE - SECTION APPEND] %s" % e)


def build_morning_briefing_pro():
    """Pro — same base + Nigerian context AI analysis + entry/stop/target."""
    d = _morning_data()
    fg_val = d["fg_data"][0]["value"] if d["fg_data"] else "N/A"
    fg_lbl = d["fg_data"][0]["value_classification"] if d["fg_data"] else "Neutral"
    fg_num = int(fg_val) if str(fg_val).isdigit() else 50
    r = round(d["btc_price"] * 1.02, 2) if d["btc_price"] else None
    s = round(d["btc_price"] * 0.98, 2) if d["btc_price"] else None
    p2p_buy, p2p_sell, p2p_src = d.get("buy"), d.get("sell"), d.get("p2p_src") or ""
    p2p_str = (
        f"USDT/NGN Buy ₦{int(p2p_buy):,} / Sell ₦{int(p2p_sell):,} "
        f"Spread ₦{int(p2p_buy - p2p_sell):,} via {p2p_src or 'P2P'}"
    ) if p2p_buy and p2p_sell else "P2P unavailable"

    track = get_track_record_line()
    lines = [
        "🌅 <b>PRO MORNING BRIEFING</b>",
        f"<i>{d['today']}  ·  {d['time_str']}</i>",
        "",
        "🇳🇬 <b>NAIRA DESK</b>",
        p2p_str,
    ]
    if d.get("eur_buy") and d.get("eur_sell"):
        lines.append(f"EUR/NGN  Buy ₦{int(d['eur_buy']):,}  ·  Sell ₦{int(d['eur_sell']):,}")
    if d.get("gbp_buy") and d.get("gbp_sell"):
        lines.append(f"GBP/NGN  Buy ₦{int(d['gbp_buy']):,}  ·  Sell ₦{int(d['gbp_sell']):,}")
    lines += ["", "📈 <b>CRYPTO</b>"]
    if d.get("btc_price"):
        line = f"BTC  {format_price(d['btc_price'])}  {format_change(d['btc_change'])}"
        if d.get("btc_ngn"):
            line += f"  ·  ~₦{int(d['btc_ngn']):,}"
        lines.append(line)
    if d.get("sol_price"):
        lines.append(f"SOL  {format_price(d['sol_price'])}  {format_change(d['sol_change'])}")
    if d.get("eth_price"):
        line = f"ETH  {format_price(d['eth_price'])}  {format_change(d['eth_change'])}"
        if d.get("eth_ngn"):
            line += f"  ·  ~₦{int(d['eth_ngn']):,}"
        lines.append(line)
    if d.get("bnb_price"):
        lines.append(f"BNB  {format_price(d['bnb_price'])}  {format_change(d['bnb_change'])}")
    lines += ["", f"🧠 Fear & Greed  <b>{fg_val}/100</b> — {fg_lbl}", ""]
    if track:
        lines += [track, ""]

    if d.get("btc_high") and d.get("btc_low"):

        lines += ["",
                  f"📊 BTC 24h Range   <b>{format_price(d['btc_low'])}</b> — <b>{format_price(d['btc_high'])}</b>"]

    if r and s:
        lines += ["",
                  "🎯 <b>KEY LEVELS</b>",
                  f"Resistance   <b>{format_price(r)}</b>",
                  f"Support      <b>{format_price(s)}</b>"]

    if d.get("gainers"):
        lines += ["", "🏆 <b>EARLY MOVERS</b>"]
        for coin, price, chg in d.get("gainers") or []:
            lines.append(f"{'📈' if chg>=0 else '📉'} <b>{coin}</b>   {format_price(price)}   {chg:+.1f}%")

    if d.get("losers"):
        lines += ["", "⚠️ <b>LAGGING</b>"]
        for coin, price, chg in d.get("losers") or []:
            lines.append(f"📉 <b>{coin}</b>   {format_price(price)}   {chg:.1f}%")

    if p2p_buy and p2p_sell:
        lines += ["",
                  "💱 <b>USDT/NGN</b>",
                  f"Buy \u20a6{int(p2p_buy):,}   Sell \u20a6{int(p2p_sell):,}   Spread \u20a6{int(p2p_buy-p2p_sell):,}",
                  f"<i>Source: {p2p_src}</i>"]

    try:
        news = get_latest_news(limit=2)
        if news:
            lines += ["", "📰 <b>MARKET NEWS</b>"]
            for n in news[:2]:
                lines.append(f"· <i>{n.get('title','')[:110]}</i>")
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)

    g_str = ", ".join(f"{c} {ch:+.1f}%" for c,_,ch in (d.get("gainers") or [])[:3]) or "flat"
    l_str = ", ".join(f"{c} {ch:.1f}%" for c,_,ch in (d.get("losers") or [])[:2]) or "none"

    btc_signal_result, technical_block = _btc_technical_grounding()

    ai_prompt = (
        f"Morning brief for Nigerian crypto traders. "
        f"BTC {format_price(d['btc_price'])} ({format_change(d['btc_change'])}), "
        f"ETH {format_price(d['eth_price'])} ({format_change(d['eth_change'])}), "
        f"SOL {format_price(d['sol_price'])}. Fear & Greed {fg_val}/100 ({fg_lbl}). "
        f"Movers: {g_str}. Lagging: {l_str}. {p2p_str}. \n\n"
        f"REAL TECHNICAL DATA (from live indicator calculations — treat this as ground truth): "
        f"{technical_block}\n\n"
        f"Write SITUATION / CONTEXT / DECISION. "
        f"DECISION: base any Entry, Stop, Target STRICTLY on the REAL TECHNICAL DATA above — "
        f"never invent price levels that contradict or go beyond what it states. "
        f"If it says no confirmed setup, say so plainly instead of forcing a trade idea. "
        f"Also say whether the P2P spread makes it worth converting naira right now."
    )
    ai, _ = ask_ai(ai_prompt)
    if not ai:
        ai = "Markets are setting up. Watch key levels and size your positions correctly."

    import re as _re
    em = _re.search(r"Entry[:\s]+([$\u20a60-9,.kK]+)", ai, _re.IGNORECASE)
    sm = _re.search(r"Stop[:\s]+([$\u20a60-9,.kK]+)", ai, _re.IGNORECASE)
    tm = _re.search(r"Target[:\s]+([$\u20a60-9,.kK]+)", ai, _re.IGNORECASE)
    update_pro_decision("BTC", "watching",
        em.group(1) if em else format_price(s or 0),
        sm.group(1) if sm else "",
        tm.group(1) if tm else "")

    lines += [
        "",
        "· · · · · · · · · · · · · · · · · · ·",
        "",
        "🧠 <b>MORNING ANALYSIS</b>",
        "",
        ai,
    ]

    # Independent, rule-based section — now doubly useful: it's the same
    # real data that just grounded the AI above, shown transparently so
    # readers can see exactly what the AI's Entry/Stop/Target was based on,
    # rather than taking the AI's numbers on faith.
    _append_signal_engine_section(lines, btc_signal_result)

    lines += [
        "",
        "<i>NFA — manage your risk.  ·  ⚡ Market Pulse Pro</i>",
    ]
    return "\n".join(lines)

def build_midday_snapshot():
    """Free — prices, sentiment, P2P, teaser only."""
    btc_price, btc_change = get_best_price("BTC")
    eth_price, eth_change = get_best_price("ETH")
    sol_price, sol_change = get_best_price("SOL")
    bnb_price, bnb_change = get_best_price("BNB")
    xrp_price, xrp_change = get_best_price("XRP")
    fg_data = get_fear_greed()
    fg_val = fg_data[0]["value"] if fg_data else "N/A"
    fg_lbl = fg_data[0]["value_classification"] if fg_data else "Neutral"
    buy, sell, _ = get_p2p_rate("USDT","NGN")
    today = wat_now().strftime("%b %d")
    time_str = wat_now().strftime("%I:%M %p WAT")
    lines = [
        "\u26a1 <b>MIDDAY SNAPSHOT</b>",
        f"<i>{today}  ·  {time_str}</i>",
        "",
        f"BTC   <b>{format_price(btc_price)}</b>   {format_change(btc_change)}",
        f"ETH   <b>{format_price(eth_price)}</b>   {format_change(eth_change)}",
        f"SOL   <b>{format_price(sol_price)}</b>   {format_change(sol_change)}",
        f"BNB   <b>{format_price(bnb_price)}</b>   {format_change(bnb_change)}",
        f"XRP   <b>{format_price(xrp_price)}</b>   {format_change(xrp_change)}",
        "",
        f"🧠 Fear & Greed   <b>{fg_val}/100</b> — {fg_lbl}",
    ]
    if buy and sell:
        lines += [
            "",
            f"💱 USDT/NGN   Buy \u20a6{int(buy):,}   Sell \u20a6{int(sell):,}   Spread \u20a6{int(buy-sell):,}",
        ]
    lines += [
        "",
        "· · · · · · · · · · · · · · · · · · ·",
        "",
        "📤 <b>Seen a good P2P rate today?</b> Submit it inside the bot — tap P2P Center → Submit Rate. It takes 10 seconds and helps everyone.",
        "",
        "💎 <b>Pro members have the AI midday read right now — what the afternoon likely holds and the exact level to enter or wait.</b>",
        "Pro ₦5,000/month — payment verification: WhatsApp +2347045850590 (no trade questions).",
        "",
        "<i>NFA - DYOR  ·  ⚡ Market Pulse</i>",
    ]
    return "\n".join(lines)

def build_midday_snapshot_pro():
    """Pro — full midday with AI afternoon read and live setup."""
    btc_price, btc_change = get_best_price("BTC")
    eth_price, eth_change = get_best_price("ETH")
    sol_price, sol_change = get_best_price("SOL")
    bnb_price, bnb_change = get_best_price("BNB")
    xrp_price, xrp_change = get_best_price("XRP")
    fg_data = get_fear_greed()
    gainers, losers = get_gainers_losers()
    fg_val = fg_data[0]["value"] if fg_data else "N/A"
    fg_lbl = fg_data[0]["value_classification"] if fg_data else "Neutral"
    buy, sell, src = get_p2p_rate("USDT","NGN")
    today = wat_now().strftime("%b %d")
    time_str = wat_now().strftime("%I:%M %p WAT")
    g_str = ", ".join(f"{c} {ch:+.1f}%" for c,_,ch in gainers[:3]) if gainers else "flat"
    l_str = ", ".join(f"{c} {ch:.1f}%" for c,_,ch in losers[:2]) if losers else "none"
    p2p_str = (f"USDT/NGN Buy \u20a6{int(buy):,} / Sell \u20a6{int(sell):,} Spread \u20a6{int(buy-sell):,} via {src}") if buy else ""

    lines = [
        "\u26a1 <b>MIDDAY SNAPSHOT — PRO</b>",
        f"<i>{today}  ·  {time_str}</i>",
        "",
        "📊 <b>PRICES</b>",
        f"BTC   <b>{format_price(btc_price)}</b>   {format_change(btc_change)}",
        f"ETH   <b>{format_price(eth_price)}</b>   {format_change(eth_change)}",
        f"SOL   <b>{format_price(sol_price)}</b>   {format_change(sol_change)}",
        f"BNB   <b>{format_price(bnb_price)}</b>   {format_change(bnb_change)}",
        f"XRP   <b>{format_price(xrp_price)}</b>   {format_change(xrp_change)}",
        "",
        f"🧠 Fear & Greed   <b>{fg_val}/100</b> — {fg_lbl}",
    ]

    if gainers:
        lines += ["", "📈 <b>LEADING</b>"]
        for coin, price, chg in gainers[:3]:
            lines.append(f"<b>{coin}</b>   {format_price(price)}   {chg:+.1f}%")

    if losers:
        lines += ["", "📉 <b>LAGGING</b>"]
        for coin, price, chg in losers[:3]:
            lines.append(f"<b>{coin}</b>   {format_price(price)}   {chg:.1f}%")

    if buy and sell:
        lines += ["",
                  "💱 <b>USDT/NGN</b>",
                  f"Buy \u20a6{int(buy):,}   Sell \u20a6{int(sell):,}   Spread \u20a6{int(buy-sell):,}",
                  f"<i>Source: {src}</i>"]

    btc_signal_result, technical_block = _btc_technical_grounding()

    ai_prompt = (
        f"Midday read for Nigerian traders. BTC {format_price(btc_price)} ({format_change(btc_change)}), "
        f"ETH {format_price(eth_price)} ({format_change(eth_change)}). "
        f"Fear & Greed {fg_val}/100. Leading: {g_str}. Lagging: {l_str}. {p2p_str}. \n\n"
        f"REAL TECHNICAL DATA (from live indicator calculations — treat this as ground truth): "
        f"{technical_block}\n\n"
        f"SITUATION / CONTEXT / DECISION format. "
        f"DECISION: hold, add or reduce — the exact level you are watching and what triggers action. "
        f"Base any Entry/Stop/Target STRICTLY on the REAL TECHNICAL DATA above — "
        f"never invent price levels that contradict or go beyond what it states. "
        f"If it says no confirmed setup, say so plainly instead of forcing a trade idea."
    )
    ai, _ = ask_ai(ai_prompt)
    if not ai:
        ai = "Market consolidating. Wait for a directional close before committing."

    lines += [
        "",
        "· · · · · · · · · · · · · · · · · · ·",
        "",
        "🧠 <b>MIDDAY READ</b>",
        "",
        ai,
    ]
    _append_signal_engine_section(lines, btc_signal_result)
    lines += [
        "",
        "<i>NFA — manage your risk.  ·  ⚡ Market Pulse Pro</i>",
    ]
    return "\n".join(lines)

def build_evening_recap():
    """Free — prices, sentiment, P2P, teaser only."""
    btc_price, btc_change = get_best_price("BTC")
    eth_price, eth_change = get_best_price("ETH")
    sol_price, sol_change = get_best_price("SOL")
    bnb_price, bnb_change = get_best_price("BNB")
    xrp_price, xrp_change = get_best_price("XRP")
    fg_data = get_fear_greed()
    fg_val = fg_data[0]["value"] if fg_data else "N/A"
    fg_lbl = fg_data[0]["value_classification"] if fg_data else "Neutral"
    buy, sell, _ = get_p2p_rate("USDT","NGN")
    today = wat_now().strftime("%b %d")
    time_str = wat_now().strftime("%I:%M %p WAT")
    lines = [
        "\U0001f319 <b>EVENING RECAP</b>",
        f"<i>{today}  ·  {time_str}</i>",
        "",
        f"BTC   <b>{format_price(btc_price)}</b>   {format_change(btc_change)}",
        f"ETH   <b>{format_price(eth_price)}</b>   {format_change(eth_change)}",
        f"SOL   <b>{format_price(sol_price)}</b>   {format_change(sol_change)}",
        f"BNB   <b>{format_price(bnb_price)}</b>   {format_change(bnb_change)}",
        f"XRP   <b>{format_price(xrp_price)}</b>   {format_change(xrp_change)}",
        "",
        f"🧠 Fear & Greed   <b>{fg_val}/100</b> — {fg_lbl}",
    ]
    if buy and sell:
        lines += [
            "",
            f"💱 USDT/NGN   Buy \u20a6{int(buy):,}   Sell \u20a6{int(sell):,}   Spread \u20a6{int(buy-sell):,}",
        ]
    lines += [
        "",
        "· · · · · · · · · · · · · · · · · · ·",
        "",
        "📤 <b>End of day ritual:</b> Submit today's best P2P rate inside the bot. Your submissions keep our data sharp for the whole community.",
        "",
        "💎 <b>Pro members have tomorrow's exact trade plan right now — entry zone, stop loss and target going into tomorrow.</b>",
        "Pro ₦5,000/month — payment verification: WhatsApp +2347045850590 (no trade questions).",
        "",
        "<i>NFA - DYOR  ·  ⚡ Market Pulse</i>",
    ]
    return "\n".join(lines)

def build_evening_recap_pro():
    """Pro — full evening recap + AI tomorrow plan with entry/stop/target."""
    btc_price, btc_change = get_best_price("BTC")
    eth_price, eth_change = get_best_price("ETH")
    sol_price, sol_change = get_best_price("SOL")
    bnb_price, bnb_change = get_best_price("BNB")
    xrp_price, xrp_change = get_best_price("XRP")
    fg_data = get_fear_greed()
    gainers, losers = get_gainers_losers()
    fg_val = fg_data[0]["value"] if fg_data else "N/A"
    fg_lbl = fg_data[0]["value_classification"] if fg_data else "Neutral"
    buy, sell, src = get_p2p_rate("USDT","NGN")
    today = wat_now().strftime("%b %d")
    time_str = wat_now().strftime("%I:%M %p WAT")
    g_str = ", ".join(f"{c} {ch:+.1f}%" for c,_,ch in gainers[:3]) if gainers else "none"
    l_str = ", ".join(f"{c} {ch:.1f}%" for c,_,ch in losers[:3]) if losers else "none"
    p2p_str = (f"USDT/NGN Buy \u20a6{int(buy):,} / Sell \u20a6{int(sell):,} "
               f"Spread \u20a6{int(buy-sell):,} via {src}") if buy else ""

    lines = [
        "\U0001f319 <b>EVENING RECAP — PRO</b>",
        f"<i>{today}  ·  {time_str}</i>",
        "",
        "📊 <b>CLOSING PRICES</b>",
        f"BTC   <b>{format_price(btc_price)}</b>   {format_change(btc_change)}",
        f"ETH   <b>{format_price(eth_price)}</b>   {format_change(eth_change)}",
        f"SOL   <b>{format_price(sol_price)}</b>   {format_change(sol_change)}",
        f"BNB   <b>{format_price(bnb_price)}</b>   {format_change(bnb_change)}",
        f"XRP   <b>{format_price(xrp_price)}</b>   {format_change(xrp_change)}",
        "",
        f"🧠 Fear & Greed   <b>{fg_val}/100</b> — {fg_lbl}",
    ]

    if gainers:
        lines += ["", "🏆 <b>DAY WINNERS</b>"]
        for coin, price, chg in gainers[:3]:
            lines.append(f"📈 <b>{coin}</b>   {format_price(price)}   {chg:+.1f}%")

    if losers:
        lines += ["", "📉 <b>DAY LOSERS</b>"]
        for coin, price, chg in losers[:3]:
            lines.append(f"📉 <b>{coin}</b>   {format_price(price)}   {chg:.1f}%")

    if buy and sell:
        lines += ["",
                  "💱 <b>USDT/NGN</b>",
                  f"Buy \u20a6{int(buy):,}   Sell \u20a6{int(sell):,}   Spread \u20a6{int(buy-sell):,}",
                  f"<i>Source: {src}</i>"]

    try:
        btc_sd = get_secondary_coin("BTC")
        btc_high = btc_sd.get("usd_24h_high") if btc_sd else None
        btc_low  = btc_sd.get("usd_24h_low")  if btc_sd else None
        if btc_high and btc_low and btc_price:
            mid = (btc_high + btc_low) / 2
            bias = "upper half" if btc_price > mid else "lower half"
            direction = "bullish" if btc_price > mid else "bearish"
            lines += ["",
                      "🌙 <b>OVERNIGHT WATCH</b>",
                      f"BTC closed in the <b>{bias}</b> of today's range — {direction} bias into tomorrow.",
                      f"Range: <b>{format_price(btc_low)}</b> — <b>{format_price(btc_high)}</b>"]
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)

    try:
        news = get_latest_news(limit=2)
        if news:
            lines += ["", "📰 <b>EVENING HEADLINES</b>"]
            for n in news[:2]:
                lines.append(f"· <i>{n.get('title','')[:110]}</i>")
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)

    btc_signal_result, technical_block = _btc_technical_grounding()

    ai_prompt = (
        f"Evening wrap for Nigerian traders. "
        f"BTC {format_price(btc_price)} ({format_change(btc_change)}), "
        f"ETH {format_price(eth_price)} ({format_change(eth_change)}). "
        f"Fear & Greed {fg_val}/100. Winners: {g_str}. Losers: {l_str}. {p2p_str}. \n\n"
        f"REAL TECHNICAL DATA (from live indicator calculations — treat this as ground truth): "
        f"{technical_block}\n\n"
        f"SITUATION / CONTEXT / DECISION. "
        f"SITUATION: what did the market do today in one sentence. "
        f"CONTEXT: what it means for Nigerian traders — naira angle or overnight risk. "
        f"DECISION: exact plan going into tomorrow. Base any Entry zone/stop loss/target STRICTLY "
        f"on the REAL TECHNICAL DATA above — never invent price levels that contradict or go beyond "
        f"what it states. If it says no confirmed setup, clearly state: wait — and give one reason."
    )
    ai, _ = ask_ai(ai_prompt)
    if not ai:
        ai = "Markets closed with mixed signals. Stay patient and wait for cleaner setups."

    lines += [
        "",
        "· · · · · · · · · · · · · · · · · · ·",
        "",
        "🔮 <b>TOMORROW'S PLAN</b>",
        "",
        ai,
    ]
    _append_signal_engine_section(lines, btc_signal_result)
    lines += [
        "",
        "<i>NFA — manage your risk.  ·  ⚡ Market Pulse Pro</i>",
    ]
    return "\n".join(lines)

def build_weekly_edge():
    """Free — top 3 movers only. Everything else is pro."""
    db = get_db(); c = db.cursor()
    since = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    performers = []
    for coin in ["BTC","ETH","SOL","BNB","XRP","DOGE","ADA"]:
        c.execute("SELECT price FROM history WHERE coin=%s AND timestamp>=%s ORDER BY id ASC LIMIT 1",(coin,since))
        first = c.fetchone()
        c.execute("SELECT price FROM history WHERE coin=%s ORDER BY id DESC LIMIT 1",(coin,))
        last = c.fetchone()
        if first and last and first[0]:
            chg = (last[0]-first[0])/first[0]*100
            performers.append((coin,last[0],first[0],chg))
    db.close()
    performers.sort(key=lambda x: x[3], reverse=True)
    week_start = (datetime.now()-timedelta(days=7)).strftime("%b %d")
    week_end   = datetime.now().strftime("%b %d")
    buy, sell, _ = get_p2p_rate("USDT","NGN")
    lines = [
        "🔥 <b>WEEKLY EDGE</b>",
        f"<i>{week_start} — {week_end}</i>",
        "",
        "📊 <b>THIS WEEK</b>",
    ]
    for coin,now_p,start_p,chg in performers[:3]:
        arrow = "📈" if chg >= 0 else "📉"
        lines.append(f"{arrow} <b>{coin}</b>   {chg:+.1f}%")
    if buy and sell:
        lines += ["",
                  f"💱 USDT/NGN   Buy \u20a6{int(buy):,}   Sell \u20a6{int(sell):,}"]
    lines += [
        "",
        "· · · · · · · · · · · · · · · · · · ·",
        "",
        "📤 <b>Submit your P2P rate</b> inside the bot — tap P2P Center → Submit Rate. Every rate submitted improves our community data.",
        "",
        "💎 <b>The Pro Weekly Edge is out.</b>",
        "What actually moved markets this week. The one coin set up for next week.",
        "Exact entry, stop and target. What the AI would do going into Monday.",
        "",
        "Pro ₦5,000/month — payment verification: WhatsApp +2347045850590 (no trade questions).",
        "",
        "<i>NFA - DYOR  ·  ⚡ Market Pulse</i>",
    ]
    return "\n".join(lines)

def build_weekly_edge_pro():
    """Pro — full weekly intelligence. Feels like inside information."""
    db = get_db(); c = db.cursor()
    since = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    performers = []
    for coin in ["BTC","ETH","SOL","BNB","XRP","DOGE","ADA"]:
        c.execute("SELECT price FROM history WHERE coin=%s AND timestamp>=%s ORDER BY id ASC LIMIT 1",(coin,since))
        first = c.fetchone()
        c.execute("SELECT price FROM history WHERE coin=%s ORDER BY id DESC LIMIT 1",(coin,))
        last = c.fetchone()
        if first and last and first[0]:
            chg = (last[0]-first[0])/first[0]*100
            performers.append((coin,last[0],first[0],chg))
    db.close()
    performers.sort(key=lambda x: x[3], reverse=True)
    week_start = (datetime.now()-timedelta(days=7)).strftime("%b %d")
    week_end   = datetime.now().strftime("%b %d")
    buy, sell, source = get_p2p_rate("USDT","NGN")
    fg_data = get_fear_greed()
    fg_val  = fg_data[0]["value"] if fg_data else "N/A"
    fg_lbl  = fg_data[0]["value_classification"] if fg_data else "Neutral"
    perf_str = ", ".join(f"{co} {ch:+.1f}%" for co,_,_,ch in performers[:7])
    top_coin = performers[0][0] if performers else "BTC"
    top_chg  = performers[0][3] if performers else 0
    bot_coin = performers[-1][0] if performers else "ETH"
    bot_chg  = performers[-1][3] if performers else 0
    p2p_str  = (f"USDT/NGN: Buy \u20a6{int(buy):,} / Sell \u20a6{int(sell):,} "
                f"Spread \u20a6{int(buy-sell):,} via {source}") if buy else "P2P unavailable"

    lines = [
        "🔥 <b>WEEKLY EDGE — PRO</b>",
        f"<i>{week_start} — {week_end}  ·  Saturday Intelligence Report</i>",
        "",
        "📊 <b>WEEK IN NUMBERS</b>",
        "",
    ]
    for coin,now_p,start_p,chg in performers[:7]:
        arrow = "📈" if chg >= 0 else "📉"
        lines.append(f"{arrow} <b>{coin}</b>   {format_price(start_p)} → <b>{format_price(now_p)}</b>   <b>{chg:+.1f}%</b>")

    lines += [
        "",
        f"🧠 Sentiment   <b>{fg_val}/100</b> — {fg_lbl}",
    ]
    if buy and sell:
        lines += [f"💱 USDT/NGN   Buy \u20a6{int(buy):,}   Sell \u20a6{int(sell):,}   Spread \u20a6{int(buy-sell):,}"]
        lines += [f"<i>Source: {source}</i>"]

    btc_signal_result, technical_block = _btc_technical_grounding()

    ai_prompt = (
        f"You are writing the Saturday weekly intelligence brief for serious Nigerian crypto traders "
        f"who pay ₦5,000/month for premium access. This should feel like a sharp analyst's private note — "
        f"confident, specific, and impossible to ignore. Not generic. Not a recap of prices they already saw. "
        f"Data: {week_start}–{week_end}. All coins: {perf_str}. "
        f"Best: {top_coin} ({top_chg:+.1f}%). Worst: {bot_coin} ({bot_chg:+.1f}%). "
        f"Fear & Greed: {fg_val}/100 ({fg_lbl}). {p2p_str}. \n\n"
        f"REAL TECHNICAL DATA for BTC entering next week (from live indicator calculations — "
        f"treat this as ground truth, not the 7-day performance numbers above which are just context): "
        f"{technical_block}\n\n"
        f"Write in plain text, no asterisks, no headers with colons. "
        f"Use this exact structure with a blank line between each section:\n"
        f"WHAT DROVE THIS WEEK: 2 sentences. Tell them what actually moved markets — macro, sentiment shift, key events. Not just prices. Make it feel like they are getting context others missed.\n\n"
        f"THE NIGERIAN ANGLE: 1–2 sentences. What did the naira and P2P spread do this week? Was it a good week to buy USDT or hold naira? Be direct.\n\n"
        f"THE ONE COIN FOR NEXT WEEK: Name one coin. Give the exact price level you are watching. Explain in one sentence why the setup is interesting. This should feel like a tip from someone who has done the work.\n\n"
        f"LEVELS TO WATCH: BTC key resistance above. BTC key support below. One line each, no fluff.\n\n"
        f"MY POSITION GOING INTO NEXT WEEK: Base this STRICTLY on the REAL TECHNICAL DATA above — "
        f"never invent an entry/stop/target that contradicts or goes beyond what it states. "
        f"If it says no confirmed setup, say you are flat/sitting out and give the one reason from the data.\n\n"
        f"End with exactly: NFA — manage your risk."
    )
    ai, _ = ask_ai(ai_prompt)
    if not ai:
        # Fallback template — also grounded in the real technical data, not
        # an invented number. This used to compute a fake entry as
        # `performers[0][1] * 0.97`, an arbitrary multiplier with no real
        # basis — same category of problem as the AI inventing levels.
        if btc_signal_result and btc_signal_result.get("signal"):
            position_line = (
                f"MY POSITION: {btc_signal_result['direction'].upper()}, {btc_signal_result['category']} "
                f"setup at {btc_signal_result['confidence']}% confidence. "
                f"Entry {format_price(btc_signal_result['entry'])}, "
                f"stop {format_price(btc_signal_result['stop_loss'])}, "
                f"target {format_price(btc_signal_result['take_profit'][0])}."
            )
        else:
            position_line = "MY POSITION: Flat — no confirmed technical setup on BTC right now. Sitting out until one forms."
        ai = (
            "WHAT DROVE THIS WEEK: Markets moved on macro uncertainty and position squeezes rather than any single catalyst — the kind of week where patience was the best trade.\n\n"
            f"THE NIGERIAN ANGLE: P2P spread stayed manageable at \u20a6{int(buy-sell) if buy and sell else 35}. Reasonable week to accumulate if you had naira sitting idle.\n\n"
            f"THE ONE COIN FOR NEXT WEEK: {top_coin} — the setup is clean and the volume supports it.\n\n"
            "LEVELS TO WATCH: BTC needs to clear resistance cleanly. Support is the line that cannot break.\n\n"
            f"{position_line}"
        )

    lines += [
        "",
        "· · · · · · · · · · · · · · · · · · ·",
        "",
        ai,
    ]
    _append_signal_engine_section(lines, btc_signal_result)
    lines += [
        "",
        "· · · · · · · · · · · · · · · · · · ·",
        "",
        "<i>NFA — manage your risk.  ·  ⚡ Market Pulse Pro</i>",
    ]
    return "\n".join(lines)



# ═══════════════════════════════════════════════════════════════════════════
