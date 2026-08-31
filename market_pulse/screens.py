"""Market Pulse Bot — screens module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import coin_key, logger
from market_pulse.db import get_db
from market_pulse.helpers import format_change, format_price
from market_pulse.menus import BACK_MAIN, MAIN_MENU, MAIN_MENU_FREE, MAIN_MENU_PRO, get_user_badge
from market_pulse.portfolio import get_portfolio_value
from market_pulse.price_fetchers import get_kraken_batch, get_secondary_batch
from market_pulse.pro_system import get_paid_pro_referral_count, get_bot_mode, get_pro_expiry, get_pro_referral_count, get_pro_source, get_pro_days_left, is_pro
from market_pulse.telegram_api import edit, send
from market_pulse.users import clear_state, set_state, track_feature


# ─── extracted section ───
# 📊 SCREEN HANDLERS
# ═══════════════════════════════════════════════════════════════════════════


def build_welcome_message(chat_id):
    """Full onboarding: Free vs Pro, bot vs channels, risks, trial terms."""
    import os
    pro = is_pro(chat_id)
    source = None
    days = None
    try:
        source = get_pro_source(chat_id)
        days = get_pro_days_left(chat_id)
    except Exception:
        pass

    free_invite = (os.environ.get("FREE_CHANNEL_INVITE") or os.environ.get("CHANNEL_INVITE") or "").strip()
    pro_invite = (os.environ.get("PRO_CHANNEL_INVITE") or "").strip()

    lines = [
        "🚀 <b>Welcome to Market Pulse</b>",
        "",
        "AI-assisted market intelligence for Nigerian traders.",
        "Educational tools only — <b>not financial advice</b>.",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "📱 <b>THE BOT</b> (this chat)",
        "━━━━━━━━━━━━━━━━━━━━",
        "<b>Free</b>",
        "• Live prices & basic market view",
        "• Limited AI questions / day",
        "• Key-level alerts (watch-style)",
        "• Portfolio / journal basics",
        "",
        "<b>Pro</b> (subscription or trial)",
        "• Full AI tools & higher limits",
        "• SAFE / NORMAL / AGGRESSIVE trade ideas",
        "• Forex & P2P intelligence",
        "• Richer alerts & trade hypotheses",
        "• Priority features as they ship",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "📢 <b>CHANNELS</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        "<b>Free channel</b>",
        "• Public key-level alerts",
        "• Morning / market snapshots (free version)",
        "• Educational posts",
        "",
        "<b>Pro channel</b>",
        "• Full Pro alerts & scenarios",
        "• Trade setups (SAFE / NORMAL / AGGRESSIVE)",
        "• Morning Pro package, evening / weekly when scheduled",
        "• Deeper context for serious monitoring",
        "",
    ]
    if free_invite:
        lines += [f"Free channel: {free_invite}"]
    if pro_invite and pro:
        lines += [f"Pro channel invite (while Pro is active): {pro_invite}"]
    elif pro_invite and not pro:
        lines += ["Pro channel invite is shared with active Pro members."]

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "🎁 <b>NEW USER TRIAL</b>",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    if pro and source == "welcome_trial":
        days_txt = f"{days} day(s)" if days is not None else "7 days"
        lines += [
            f"You are on a <b>one-time 7-day Pro trial</b> (~{days_txt} left).",
            "• Full Pro <b>bot</b> features during the trial",
            "• Pro channel invite only while trial/subscription is active",
            "• When the trial ends you return to <b>Free</b>",
            "",
            "<b>How to keep or get free Pro after the trial:</b>",
            "• <b>Refer friends</b> — /upgrade for your link "
            "(free Pro when they <b>pay</b> after using your link)",
            "• Or <b>subscribe</b> — ₦5,000/month via WhatsApp payment verification only",
            "• Trial does not auto-renew",
        ]
    elif pro:
        lines += [
            f"You currently have Pro access until <b>{get_pro_expiry(chat_id) or 'N/A'}</b>.",
            "Earn more free Pro time by referring friends — see /upgrade for your link.",
        ]
    else:
        lines += [
            "New accounts receive a <b>one-time 7-day Pro trial</b> on first use.",
            "After it ends, Free limits apply.",
            "",
            "<b>Get free Pro:</b> friends use your /upgrade link and <b>pay</b> for Pro (3 paid → 1 month, 5 → 3 months, 10 → 6 months).",
            "<b>Or subscribe:</b> ₦5,000/month — WhatsApp payment verification only.",
        ]

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "🧠 <b>HOW SIGNALS WORK</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        "• Key levels = <b>what to watch</b>, not automatic trades",
        "• Trade ideas only when structure + risk rules pass",
        "• Tiers: 🟢 SAFE · 🟡 NORMAL · 🔴 AGGRESSIVE",
        "• R:R is calculated in code — not invented by AI",
        "• Bot does <b>not</b> place exchange orders for you",
        "",
        "━━━━━━━━━━━━━━━━━━━━",
        "⚠️ <b>RISKS & DISCLAIMER</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        "Crypto and FX are highly volatile. You can lose money.",
        "Past setups do not predict future results.",
        "Always use your own risk management and a stop loss.",
        "<b>NFA — DYOR — Trade at your own risk.</b>",
        "",
        "Type /menu anytime · /upgrade for Pro, referrals & payment",
    ]
    return "\n".join(lines)


def send_welcome_onboarding(chat_id):
    """Send full welcome (called from /start)."""
    try:
        from market_pulse.telegram_api import send
        send(chat_id, build_welcome_message(chat_id))
    except Exception as e:
        try:
            from market_pulse.config_runtime import logger
            logger.warning("[WELCOME] %s", e)
        except Exception:
            pass


def show_main_menu(chat_id, message_id=None):
    text = (
        "🚀 <b>Market Pulse</b>\n\n"
        f"👤 {get_user_badge(chat_id)}\n\n"
        "AI-powered crypto intelligence for Nigerian traders.\n\n"
        "Choose a category:"
    )
    
    if get_bot_mode() == "everyone":
        menu = MAIN_MENU
    elif is_pro(chat_id):
        menu = MAIN_MENU_PRO
    else:
        menu = MAIN_MENU_FREE
    
    if message_id:
        edit(chat_id, message_id, text, menu)
    else:
        send(chat_id, text, menu)

def show_market(chat_id, message_id=None):
    track_feature(chat_id, "market")
    kraken_batch = get_kraken_batch()
    secondary = get_secondary_batch()
    
    lines = [
        "📈 <b>Live Market Prices</b>",
        "",
        "<code>Coin    Price       24h %",
        "──────────────────────────────"
    ]
    
    for coin in ["BTC", "ETH", "SOL", "BNB", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT"]:
        price = kraken_batch.get(coin)
        sd = secondary.get(coin_key(coin))
        if price is None and sd:
            price = sd.get("usd")
        change = sd.get("usd_24h_change") if sd else None
        if price:
            lines.append(f"{coin:6} {format_price(price):10} {format_change(change)}")
    
    lines.append("</code>")
    lines.append("")
    lines.append(f"<i>👤 {get_user_badge(chat_id)}</i>")
    
    buttons = [
        [{"text": "🔄 Refresh", "callback_data": "market"},
         {"text": "⬅ Back", "callback_data": "main_menu"}]
    ]
    
    if message_id:
        edit(chat_id, message_id, "\n".join(lines), buttons)
    else:
        send(chat_id, "\n".join(lines), buttons)

def show_upgrade(chat_id, message_id=None):
    if is_pro(chat_id):
        ref_count = get_paid_pro_referral_count(chat_id)
        next_tier = ""
        if ref_count < 3:   next_tier = f"{3-ref_count} more paid referral(s) → 1 month free"
        elif ref_count < 5: next_tier = f"{5-ref_count} more paid referral(s) → 3 months free"
        elif ref_count < 10:next_tier = f"{10-ref_count} more paid referral(s) → 6 months free"
        else:               next_tier = "Maximum paid-referral tier reached — thank you!"
        text = (
            "⭐ <b>You are Pro!</b>\n\n"
            f"📅 Expires: <b>{get_pro_expiry(chat_id) or 'N/A'}</b>\n"
            f"👥 Paid referrals: <b>{ref_count}</b>   <i>{next_tier}</i>\n\n"
            "Your referral link:\n"
            f"<code>https://t.me/MarketNgPulseBot?start=ref_PRO_{chat_id}</code>\n\n"
            "<i>Share this link. Free Pro when friends start with your link and pay. "
            "No link or trial-only = no credit.</i>"
        )
    else:
        text = (
            "💎 <b>Market Pulse Pro</b>\n\n"
            "Everything free users get, plus:\n\n"
            "🧠 AI analysis on every morning, midday and evening post\n"
            "🎯 Exact entry, stop loss and target — every day\n"
            "🔔 Key level alerts with AI breakout analysis\n"
            "🐋 Whale alerts with AI trade decision\n"
            "💱 P2P rate alerts with naira context\n"
            "📊 Saturday Weekly Edge — full intelligence report\n"
            "📈 Unlimited AI questions\n"
            "⚙️ 20 price alerts, 30 watchlist items\n"
            "📒 Trade Journal + Position Calculator\n\n"
            "💰 <b>₦5,000/month</b>\n\n"
            "👥 <b>Earn free Pro — paid referrals only:</b>\n"
            "3 friends pay for Pro → <b>1 month</b> free for you\n"
            "5 friends pay → <b>3 months</b> free\n"
            "10 friends pay → <b>6 months</b> free\n"
            "<i>They must open the bot with your link, then pay. "
            "Trial alone or no link does not count.</i>\n\n"
            "📩 <b>Pro upgrade — payment verification only</b>\n"
            "WhatsApp: <b>+2347045850590</b>\n"
            "<a href=\"https://wa.me/2347045850590\">Tap to open WhatsApp</a>\n\n"
            "<i>Send payment proof on WhatsApp only. "
            "No trade questions or advice on that line — "
            "markets stay in the bot and channels. "
            "Activated after verification.</i>"
        )
    
    buttons = [[{"text": "🏠 Main Menu", "callback_data": "main_menu"}]]
    
    if message_id:
        edit(chat_id, message_id, text, buttons)
    else:
        send(chat_id, text, buttons)

def show_help(chat_id, message_id=None):
    text = (
        "📚 <b>Market Pulse Commands</b>\n\n"
        "📊 <b>Markets</b>\n"
        "/market - Live prices\n"
        "/charts - Price charts\n"
        "/gainers - Top gainers\n"
        "/losers - Top losers\n"
        "/dominance - Market dominance\n\n"
        "🧠 <b>Intelligence</b>\n"
        "/ai - Ask AI\n"
        "/news - AI news\n"
        "/feargreed - Fear & Greed\n"
        "/outlook - Market outlook\n\n"
        "🇳🇬 <b>P2P</b>\n"
        "/p2p - P2P rates\n"
        "/p2palerts - P2P alerts\n"
        "/arbitrage - Arbitrage scanner\n\n"
        "🔔 <b>Alerts</b>\n"
        "/alert - Create alert\n"
        "/alerts - My alerts\n"
        "/watchlist - Watchlist\n\n"
        "💼 <b>Portfolio</b>\n"
        "/portfolio - My portfolio\n"
        "/addportfolio - Add position\n"
        "/removeportfolio - Remove position\n\n"
        "📈 <b>Trade Journal</b>\n"
        "/addtrade - Add trade\n"
        "/closetrade - Close trade\n"
        "/trades - My trades\n\n"
        "🛠 <b>Tools</b>\n"
        "/position - Position calculator\n"
        "/convert - Convert crypto\n"
        "/search - Search coin\n\n"
        "👤 <b>Account</b>\n"
        "/upgrade - Upgrade to Pro\n"
        "/referral - Referral program\n"
        "/settings - User settings\n"
        "/feedback - Send feedback\n\n"
        "ℹ️ <b>Info</b>\n"
        "/help - This menu\n"
        "/version - Bot version\n"
        "/ping - Check bot status"
    )
    
    buttons = [[{"text": "🏠 Main Menu", "callback_data": "main_menu"}]]
    
    if message_id:
        edit(chat_id, message_id, text, buttons)
    else:
        send(chat_id, text, buttons)

def show_portfolio(chat_id, message_id=None):
    portfolio_data = get_portfolio_value(chat_id)
    
    if not portfolio_data or not portfolio_data["positions"]:
        text = (
            "💼 <b>Portfolio</b>\n\n"
            "No positions yet.\n\n"
            "Add positions:\n"
            "<code>/addportfolio BTC 0.5 61000</code>"
        )
        buttons = [
            [{"text": "➕ Add Position", "callback_data": "add_portfolio"}],
            [{"text": "🏠 Main Menu", "callback_data": "main_menu"}]
        ]
        
        if message_id:
            edit(chat_id, message_id, text, buttons)
        else:
            send(chat_id, text, buttons)
        return
    
    lines = ["💼 <b>Portfolio</b>\n"]
    
    for pos in portfolio_data["positions"]:
        pnl_emoji = "📈" if pos["pnl"] > 0 else "📉" if pos["pnl"] < 0 else "➖"
        lines.append(f"{pnl_emoji} <b>{pos['coin']}</b>")
        lines.append(f"  Amount: {pos['amount']:.4f}")
        lines.append(f"  Entry: {format_price(pos['buy_price'])}")
        lines.append(f"  Current: {format_price(pos['current_price'])}")
        lines.append(f"  P&L: <b>{'+' if pos['pnl'] > 0 else ''}{pos['pnl']:.2f}</b> ({'+' if pos['pnl_pct'] > 0 else ''}{pos['pnl_pct']:.1f}%)")
        lines.append("")
    
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📊 Total Invested: ${portfolio_data['total_invested']:.2f}")
    lines.append(f"📊 Current Value: ${portfolio_data['total_current']:.2f}")
    pnl_emoji = "📈" if portfolio_data["total_pnl"] > 0 else "📉" if portfolio_data["total_pnl"] < 0 else "➖"
    lines.append(f"{pnl_emoji} Total P&L: <b>{'+' if portfolio_data['total_pnl'] > 0 else ''}{portfolio_data['total_pnl']:.2f}</b> ({'+' if portfolio_data['total_pnl_pct'] > 0 else ''}{portfolio_data['total_pnl_pct']:.1f}%)")
    lines.append("")
    lines.append("<i>NFA - DYOR</i>")
    
    buttons = [
        [{"text": "🔄 Refresh", "callback_data": "portfolio"}],
        [{"text": "➕ Add", "callback_data": "add_portfolio"},
         {"text": "🗑️ Remove", "callback_data": "remove_portfolio"}],
        [{"text": "🏠 Main Menu", "callback_data": "main_menu"}]
    ]
    
    if message_id:
        edit(chat_id, message_id, "\n".join(lines), buttons)
    else:
        send(chat_id, "\n".join(lines), buttons)

def show_trade_journal(chat_id, message_id=None):
    if not is_pro(chat_id) and get_bot_mode() != "everyone":
        text = "🔒 <b>Pro Feature</b>\n\nTrade Journal is only available to Pro users."
        if message_id:
            edit(chat_id, message_id, text, [[{"text": "💎 Upgrade", "callback_data": "upgrade"}, {"text": "⬅ Back", "callback_data": "menu_account"}]])
        else:
            send(chat_id, text, [[{"text": "💎 Upgrade", "callback_data": "upgrade"}, {"text": "⬅ Back", "callback_data": "menu_account"}]])
        return
    
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT id, coin, direction, entry_price, exit_price, size, pnl, status FROM trade_journal "
                  "WHERE chat=%s ORDER BY id DESC LIMIT 20", (str(chat_id),))
        rows = c.fetchall()
        db.close()
        
        if not rows:
            text = (
                "📈 <b>Trade Journal</b>\n\n"
                "No trades yet.\n\n"
                "Add a trade:\n"
                "<code>/addtrade BTC LONG 61000 62000 0.5</code>"
            )
            buttons = [
                [{"text": "➕ Add Trade", "callback_data": "add_trade"}],
                [{"text": "🏠 Main Menu", "callback_data": "main_menu"}]
            ]
            
            if message_id:
                edit(chat_id, message_id, text, buttons)
            else:
                send(chat_id, text, buttons)
            return
        
        lines = ["📈 <b>Trade Journal</b>\n"]
        total_pnl = 0
        wins = 0
        closed_trades = 0
        
        for tid, coin, direction, entry, exit_price, size, pnl, status in rows:
            if status == "closed" and pnl is not None:
                total_pnl += pnl
                closed_trades += 1
                if pnl > 0:
                    wins += 1
            pnl_str = f"+${pnl:.2f}" if pnl and pnl > 0 else f"-${abs(pnl):.2f}" if pnl else "Open"
            status_emoji = "✅" if status == "closed" else "⏳"
            lines.append(f"{status_emoji} #{tid} <b>{coin}</b> {direction}")
            lines.append(f"   Entry: {format_price(entry)} → Exit: {format_price(exit_price) if exit_price else 'Open'}")
            lines.append(f"   Size: {size} | P&L: <b>{pnl_str}</b>")
            lines.append("")
        
        if closed_trades > 0:
            win_rate = (wins / closed_trades) * 100 if closed_trades > 0 else 0
            lines.append(f"📊 Total P&L: <b>+${total_pnl:.2f}</b>")
            lines.append(f"📊 Win Rate: <b>{win_rate:.1f}%</b> ({wins}/{closed_trades})")
        
        lines.append("")
        lines.append("<i>Use /addtrade to record trades</i>")
        
        buttons = [
            [{"text": "🔄 Refresh", "callback_data": "trade_journal"}],
            [{"text": "➕ Add", "callback_data": "add_trade"},
             {"text": "🔒 Close", "callback_data": "close_trade"}],
            [{"text": "🏠 Main Menu", "callback_data": "main_menu"}]
        ]
        
        if message_id:
            edit(chat_id, message_id, "\n".join(lines), buttons)
        else:
            send(chat_id, "\n".join(lines), buttons)
    except Exception as e:
        logger.error("[TRADE JOURNAL ERROR] %s" % e)
        if message_id:
            edit(chat_id, message_id, "⚠️ Error loading trades.", BACK_MAIN)
        else:
            send(chat_id, "⚠️ Error loading trades.")

def show_settings(chat_id, message_id=None):
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT language, notifications, theme FROM user_preferences WHERE chat=%s", (str(chat_id),))
        row = c.fetchone()
        db.close()
        
        if not row:
            db = get_db()
            c = db.cursor()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute("INSERT INTO user_preferences (chat, language, notifications, theme, updated_at) VALUES (%s,%s,%s,%s,%s)",
                      (str(chat_id), 'en', 1, 'dark', now))
            db.commit()
            db.close()
            language, notifications, theme = 'en', 1, 'dark'
        else:
            language, notifications, theme = row
        
        text = (
            "⚙️ <b>User Settings</b>\n\n"
            f"🌐 Language: <b>{language.upper()}</b>\n"
            f"🔔 Notifications: <b>{'✅ On' if notifications else '❌ Off'}</b>\n"
            f"🎨 Theme: <b>{theme.title()}</b>\n\n"
            "Tap to change:"
        )
        
        buttons = [
            [{"text": f"🌐 Language ({language.upper()})", "callback_data": "settings_language"}],
            [{"text": f"🔔 Notifications ({'On' if notifications else 'Off'})", "callback_data": "settings_notifications"}],
            [{"text": f"🎨 Theme ({theme.title()})", "callback_data": "settings_theme"}],
            [{"text": "🏠 Main Menu", "callback_data": "main_menu"}]
        ]
        
        if message_id:
            edit(chat_id, message_id, text, buttons)
        else:
            send(chat_id, text, buttons)
    except Exception as e:
        logger.error("[SETTINGS ERROR] %s" % e)
        if message_id:
            edit(chat_id, message_id, "⚠️ Error loading settings.", BACK_MAIN)
        else:
            send(chat_id, "⚠️ Error loading settings.")

def show_position_calculator(chat_id, message_id=None):
    if not is_pro(chat_id) and get_bot_mode() != "everyone":
        text = "🔒 <b>Pro Feature</b>\n\nPosition Calculator is only available to Pro users."
        if message_id:
            edit(chat_id, message_id, text, [[{"text": "💎 Upgrade", "callback_data": "upgrade"}, {"text": "⬅ Back", "callback_data": "menu_account"}]])
        else:
            send(chat_id, text, [[{"text": "💎 Upgrade", "callback_data": "upgrade"}, {"text": "⬅ Back", "callback_data": "menu_account"}]])
        return
    
    set_state(chat_id, "awaiting_position_calc", {})
    text = (
        "📐 <b>Position Size Calculator</b>\n\n"
        "Enter your account details:\n\n"
        "Format: <code>ACCOUNT_SIZE RISK_PERCENT ENTRY_PRICE STOP_LOSS</code>\n\n"
        "Example: <code>10000 2 98200 97000</code>\n\n"
        "Account: $10,000 | Risk: 2% | Entry: $98,200 | SL: $97,000"
    )
    
    if message_id:
        edit(chat_id, message_id, text, [[{"text": "⬅ Cancel", "callback_data": "main_menu"}]])
    else:
        send(chat_id, text, [[{"text": "⬅ Cancel", "callback_data": "main_menu"}]])

def handle_position_calc(chat_id, text):
    clear_state(chat_id)
    parts = text.strip().replace(",", "").split()
    if len(parts) != 4:
        send(chat_id, "⚠️ Format: <code>ACCOUNT_SIZE RISK_PERCENT ENTRY_PRICE STOP_LOSS</code>")
        return
    
    try:
        account = float(parts[0])
        risk_pct = float(parts[1])
        entry = float(parts[2])
        sl = float(parts[3])
        
        if account <= 0 or risk_pct <= 0 or entry <= 0 or sl <= 0:
            raise ValueError
        
        risk_amount = account * (risk_pct / 100)
        risk_per_unit = abs(entry - sl)
        position_size = risk_amount / risk_per_unit
        position_value = position_size * entry
        
        lines = [
            "📐 <b>Position Size Calculator</b>",
            "",
            f"Account: <b>${account:,.2f}</b>",
            f"Risk: <b>{risk_pct:.1f}%</b> (${risk_amount:,.2f})",
            f"Entry: <b>{format_price(entry)}</b>",
            f"Stop Loss: <b>{format_price(sl)}</b>",
            "",
            "· · · · · · · · · · · · · · · · · · ·",
            "",
            f"📊 Position Size: <b>{position_size:.4f}</b> units",
            f"💰 Position Value: <b>${position_value:,.2f}</b>",
            f"💸 Risk per Unit: <b>${risk_per_unit:.2f}</b>",
            "",
            "· · · · · · · · · · · · · · · · · · ·",
            "",
            "<i>NFA - DYOR</i>",
        ]
        
        send(chat_id, "\n".join(lines), [
            [{"text": "🔄 Calculate Again", "callback_data": "position_calculator"}],
            [{"text": "🏠 Main Menu", "callback_data": "main_menu"}]
        ])
    except Exception as _e:
        send(chat_id, "⚠️ Invalid input. Use numbers only.")

# ═══════════════════════════════════════════════════════════════════════════
