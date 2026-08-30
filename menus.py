"""Market Pulse Bot — menus module (split from the real monolithic bot.py)."""

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

from market_pulse.pro_system import is_pro


# ─── extracted section ───
# 🏠 MENUS
# ═══════════════════════════════════════════════════════════════════════════

def get_user_badge(chat_id):
    if is_pro(chat_id):
        return "⭐ Pro User"
    else:
        return "👤 Free User"

BACK_MAIN = [[{"text": "🏠 Main Menu", "callback_data": "main_menu"}]]

MAIN_MENU = [
    [{"text": "📊 Markets", "callback_data": "menu_markets"}],
    [{"text": "🧠 Intelligence", "callback_data": "menu_intelligence"}],
    [{"text": "🇳🇬 P2P Center", "callback_data": "menu_p2p"}],
    [{"text": "🔔 Alerts", "callback_data": "menu_alerts"}],
    [{"text": "💼 Portfolio", "callback_data": "menu_portfolio"}],
    [{"text": "📈 Trade Journal", "callback_data": "menu_trades"}],
    [{"text": "🛠 Tools", "callback_data": "menu_tools"}],
    [{"text": "👤 My Account", "callback_data": "menu_account"}],
    [{"text": "❓ Help", "callback_data": "help"}],
]

MAIN_MENU_FREE = [
    [{"text": "📊 Markets", "callback_data": "menu_markets"}],
    [{"text": "🧠 Intelligence", "callback_data": "menu_intelligence"}],
    [{"text": "🇳🇬 P2P Center", "callback_data": "menu_p2p"}],
    [{"text": "🔔 Alerts", "callback_data": "menu_alerts"}],
    [{"text": "💼 Portfolio", "callback_data": "menu_portfolio"}],
    [{"text": "📈 Trade Journal", "callback_data": "menu_trades"}],
    [{"text": "🛠 Tools", "callback_data": "menu_tools"}],
    [{"text": "👤 My Account", "callback_data": "menu_account"}],
    [{"text": "💎 Upgrade to Pro", "callback_data": "upgrade"}],
    [{"text": "❓ Help", "callback_data": "help"}],
]

MAIN_MENU_PRO = [
    [{"text": "⭐ Pro Menu", "callback_data": "menu_pro"}],
    [{"text": "📊 Markets", "callback_data": "menu_markets"}],
    [{"text": "🧠 Intelligence", "callback_data": "menu_intelligence"}],
    [{"text": "🇳🇬 P2P Center", "callback_data": "menu_p2p"}],
    [{"text": "🔔 Alerts", "callback_data": "menu_alerts"}],
    [{"text": "💼 Portfolio", "callback_data": "menu_portfolio"}],
    [{"text": "📈 Trade Journal", "callback_data": "menu_trades"}],
    [{"text": "🛠 Tools", "callback_data": "menu_tools"}],
    [{"text": "📈 Pro Tools", "callback_data": "menu_pro_tools"}],
    [{"text": "👤 My Account", "callback_data": "menu_account"}],
    [{"text": "❓ Help", "callback_data": "help"}],
]

MARKETS_MENU = [
    [{"text": "📈 Live Market", "callback_data": "market"}],
    [{"text": "🔥 Gainers", "callback_data": "gainers"}],
    [{"text": "📉 Losers", "callback_data": "losers"}],
    [{"text": "🌐 Dominance", "callback_data": "dominance"}],
    [{"text": "🔄 Arbitrage", "callback_data": "arbitrage"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

INTELLIGENCE_MENU = [
    [{"text": "🤖 Ask AI", "callback_data": "ask_ai"}],
    [{"text": "📰 AI News", "callback_data": "news"}],
    [{"text": "🧠 Fear & Greed", "callback_data": "fear_greed"}],
    [{"text": "📈 Market Outlook", "callback_data": "market_outlook"}],
    [{"text": "🎯 Trade Setup", "callback_data": "trade_setup"}],
    [{"text": "📡 Sources", "callback_data": "sources"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

P2P_MENU = [
    [{"text": "💱 P2P Rates", "callback_data": "p2p"}],
    [{"text": "📤 Submit Rate", "callback_data": "submit_rate"}],
    [{"text": "🔔 P2P Alerts", "callback_data": "p2p_alerts"}],
    [{"text": "🔄 Arbitrage Scanner", "callback_data": "arbitrage"}],
    [{"text": "📊 My P2P History", "callback_data": "p2p_history"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

ALERTS_MENU = [
    [{"text": "➕ Create Alert", "callback_data": "alerts"}],
    [{"text": "📋 My Alerts", "callback_data": "my_alerts"}],
    [{"text": "⭐ Watchlist", "callback_data": "watchlist"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

ALERTS_MENU_FREE = [
    [{"text": "➕ Create Alert (3 max)", "callback_data": "alerts"}],
    [{"text": "📋 My Alerts", "callback_data": "my_alerts"}],
    [{"text": "⭐ Watchlist (10 max)", "callback_data": "watchlist"}],
    [{"text": "💎 Upgrade for Unlimited", "callback_data": "upgrade"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

ALERTS_MENU_PRO = [
    [{"text": "➕ Create Alert (20 max)", "callback_data": "alerts"}],
    [{"text": "📋 My Alerts", "callback_data": "my_alerts"}],
    [{"text": "⭐ Watchlist (30 max)", "callback_data": "watchlist"}],
    [{"text": "⚡ Smart Alerts", "callback_data": "smart_alerts"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

PORTFOLIO_MENU = [
    [{"text": "💼 View Portfolio", "callback_data": "portfolio"}],
    [{"text": "➕ Add Position", "callback_data": "add_portfolio"}],
    [{"text": "🗑️ Remove Position", "callback_data": "remove_portfolio"}],
    [{"text": "📊 P&L Summary", "callback_data": "pnl_summary"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

TRADES_MENU = [
    [{"text": "📈 My Trades", "callback_data": "trade_journal"}],
    [{"text": "➕ Add Trade", "callback_data": "add_trade"}],
    [{"text": "🔒 Close Trade", "callback_data": "close_trade"}],
    [{"text": "📊 Win Rate", "callback_data": "win_rate"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

TOOLS_MENU = [
    [{"text": "🔍 Search Coin", "callback_data": "coin_search"}],
    [{"text": "🔄 Convert", "callback_data": "convert"}],
    [{"text": "📐 Position Calculator", "callback_data": "position_calculator"}],
    [{"text": "📜 Price History", "callback_data": "history"}],
    [{"text": "⚙️ Bot Status", "callback_data": "status"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

ACCOUNT_MENU_FREE = [
    [{"text": "👤 My Profile", "callback_data": "profile"}],
    [{"text": "💼 Portfolio", "callback_data": "portfolio"}],
    [{"text": "👥 Referral", "callback_data": "referral"}],
    [{"text": "📊 My Usage", "callback_data": "my_usage"}],
    [{"text": "⚙️ Settings", "callback_data": "settings"}],
    [{"text": "💎 Upgrade to Pro", "callback_data": "upgrade"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

ACCOUNT_MENU_PRO = [
    [{"text": "👤 My Profile", "callback_data": "profile"}],
    [{"text": "⭐ Pro Status", "callback_data": "pro_status"}],
    [{"text": "💼 Portfolio", "callback_data": "portfolio"}],
    [{"text": "📈 Trade Journal", "callback_data": "trade_journal"}],
    [{"text": "📐 Position Calculator", "callback_data": "position_calculator"}],
    [{"text": "👥 Referral", "callback_data": "referral"}],
    [{"text": "📊 My Usage", "callback_data": "my_usage"}],
    [{"text": "⚙️ Settings", "callback_data": "settings"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

HELP_MENU = [
    [{"text": "📚 All Commands", "callback_data": "help_commands"}],
    [{"text": "📖 How To Use", "callback_data": "help_howto"}],
    [{"text": "❓ FAQ", "callback_data": "help_faq"}],
    [{"text": "💬 Support", "callback_data": "support"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

# ── Admin menu: 6 grouped submenus instead of 10 flat buttons ────────────
ADMIN_MENU = [
    [{"text": "📊 Analytics", "callback_data": "adm_analytics"},
     {"text": "📢 Channel",   "callback_data": "adm_channel"}],
    [{"text": "👥 Users",     "callback_data": "adm_users"},
     {"text": "⚡ Trades",    "callback_data": "adm_trades"}],
    [{"text": "🏥 System",    "callback_data": "adm_system"},
     {"text": "⚙️ Settings",  "callback_data": "adm_settings_menu"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]
ADMIN_ANALYTICS_MENU = [
    [{"text": "📊 Dashboard",  "callback_data": "admin_dashboard"}],
    [{"text": "📈 Stats",      "callback_data": "admin_stats"}],
    [{"text": "👤 Users",      "callback_data": "admin_users"}],
    [{"text": "⬅ Back",       "callback_data": "admin_menu"}],
]
ADMIN_CHANNEL_MENU = [
    [{"text": "📰 Publish Post",    "callback_data": "admin_publish"}],
    [{"text": "📦 Content Packages","callback_data": "admin_content_packages"}],
    [{"text": "🔄 Toggle Channel",  "callback_data": "adm_toggle_channel"}],
    [{"text": "🪞 Mirror Mode",     "callback_data": "adm_toggle_mirror"}],
    [{"text": "⬅ Back",            "callback_data": "admin_menu"}],
]
ADMIN_USERS_MENU = [
    [{"text": "📢 Broadcast",  "callback_data": "admin_broadcast"}],
    [{"text": "🔒 Ban User",   "callback_data": "admin_ban"}],
    [{"text": "💎 Grant Pro",  "callback_data": "adm_grant_pro"}],
    [{"text": "⬅ Back",       "callback_data": "admin_menu"}],
]
ADMIN_TRADES_MENU = [
    [{"text": "📋 Trade History",  "callback_data": "adm_trade_history"}],
    [{"text": "⚡ Generate Trade", "callback_data": "adm_gen_trade"}],
    [{"text": "📊 Performance",    "callback_data": "adm_performance"}],
    [{"text": "⬅ Back",           "callback_data": "admin_menu"}],
]
ADMIN_SYSTEM_MENU = [
    [{"text": "🏥 Health",    "callback_data": "admin_health"}],
    [{"text": "📋 Logs",      "callback_data": "admin_logs"}],
    [{"text": "⬅ Back",      "callback_data": "admin_menu"}],
]
ADMIN_SETTINGS_MENU = [
    [{"text": "⚙️ Bot Settings", "callback_data": "admin_settings"}],
    [{"text": "🤖 Bot Mode",     "callback_data": "adm_mode_menu"}],
    [{"text": "⬅ Back",         "callback_data": "admin_menu"}],
]


# ═══════════════════════════════════════════════════════════════════════════
