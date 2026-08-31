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
    [{"text": "🔥 Gainers", "callback_data": "gainers"}, {"text": "📉 Losers", "callback_data": "losers"}],
    [{"text": "🌐 Dominance", "callback_data": "dominance"}],
    [{"text": "🔄 Arbitrage", "callback_data": "arbitrage"}],
    [{"text": "💱 Forex Ideas", "callback_data": "menu_forex"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

INTELLIGENCE_MENU = [
    [{"text": "🤖 Ask AI", "callback_data": "ask_ai"}],
    [{"text": "📰 AI News", "callback_data": "news"}],
    [{"text": "🧠 Fear & Greed", "callback_data": "fear_greed"}],
    [{"text": "📈 Market Outlook", "callback_data": "market_outlook"}],
    [{"text": "🎯 AI Trade Setup", "callback_data": "trade_setup"}],
    [{"text": "⚡ Crypto Idea", "callback_data": "menu_crypto_idea"}],
    [{"text": "💱 Forex Idea", "callback_data": "menu_forex"}],
    [{"text": "📡 Sources", "callback_data": "sources"}],
    [{"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

P2P_MENU = [
    [{"text": "💱 USDT/NGN", "callback_data": "p2p_usdt"},
     {"text": "🇪🇺 EUR/NGN", "callback_data": "p2p_eur"},
     {"text": "🇬🇧 GBP/NGN", "callback_data": "p2p_gbp"}],
    [{"text": "📋 All P2P Rates", "callback_data": "p2p_all"}],
    [{"text": "🔔 Set P2P Alert", "callback_data": "p2p_set_alert"}],
    [{"text": "📤 Submit Rate", "callback_data": "submit_rate"}],
    [{"text": "🔔 My P2P Alerts", "callback_data": "p2p_alerts"}],
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
    [{"text": "📈 My Journal", "callback_data": "trade_journal"}],
    [{"text": "📋 Bot Idea History", "callback_data": "bot_trade_history"}],
    [{"text": "⚡ New Crypto Idea", "callback_data": "menu_crypto_idea"}],
    [{"text": "💱 New Forex Idea", "callback_data": "menu_forex"}],
    [{"text": "🔒 Close Idea", "callback_data": "close_bot_idea"}],
    [{"text": "➕ Add Manual Trade", "callback_data": "add_trade"}],
    [{"text": "🔒 Close Journal Trade", "callback_data": "close_trade"}],
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

# ── Forex pair picker ───────────────────────────────────────────────────────
FOREX_MENU = [
    [{"text": "USDT/NGN", "callback_data": "fx_pair_USDT/NGN"},
     {"text": "EUR/NGN", "callback_data": "fx_pair_EUR/NGN"}],
    [{"text": "GBP/NGN", "callback_data": "fx_pair_GBP/NGN"},
     {"text": "USD/NGN", "callback_data": "fx_pair_USD/NGN"}],
    [{"text": "EUR/USD", "callback_data": "fx_pair_EUR/USD"},
     {"text": "GBP/USD", "callback_data": "fx_pair_GBP/USD"}],
    [{"text": "BTC/NGN", "callback_data": "fx_pair_BTC/NGN"}],
    [{"text": "⬅ Back", "callback_data": "menu_markets"},
     {"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

def forex_tier_menu(pair_key):
    """Tier buttons after pair selected. pair_key embedded in callback."""
    safe = pair_key.replace("/", "_")
    return [
        [{"text": "🟢 Steady", "callback_data": f"fx_gen_{safe}_steady"},
         {"text": "🟡 Momentum", "callback_data": f"fx_gen_{safe}_momentum"}],
        [{"text": "🔴 Edge", "callback_data": f"fx_gen_{safe}_edge"}],
        [{"text": "⬅ Pairs", "callback_data": "menu_forex"},
         {"text": "🏠 Main Menu", "callback_data": "main_menu"}],
    ]

# ── Crypto idea coin picker ─────────────────────────────────────────────────
CRYPTO_IDEA_MENU = [
    [{"text": "BTC", "callback_data": "ci_coin_BTC"},
     {"text": "ETH", "callback_data": "ci_coin_ETH"},
     {"text": "SOL", "callback_data": "ci_coin_SOL"}],
    [{"text": "BNB", "callback_data": "ci_coin_BNB"},
     {"text": "XRP", "callback_data": "ci_coin_XRP"},
     {"text": "AVAX", "callback_data": "ci_coin_AVAX"}],
    [{"text": "DOGE", "callback_data": "ci_coin_DOGE"},
     {"text": "ADA", "callback_data": "ci_coin_ADA"},
     {"text": "LINK", "callback_data": "ci_coin_LINK"}],
    [{"text": "⬅ Back", "callback_data": "menu_trades"},
     {"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]

def crypto_tier_menu(coin):
    return [
        [{"text": "🟢 Steady", "callback_data": f"ci_gen_{coin}_steady"},
         {"text": "🟡 Momentum", "callback_data": f"ci_gen_{coin}_momentum"}],
        [{"text": "🔴 Edge", "callback_data": f"ci_gen_{coin}_edge"}],
        [{"text": "⬅ Coins", "callback_data": "menu_crypto_idea"},
         {"text": "🏠 Main Menu", "callback_data": "main_menu"}],
    ]

# ── P2P alert asset picker ──────────────────────────────────────────────────
P2P_ALERT_ASSET_MENU = [
    [{"text": "USDT", "callback_data": "p2p_alert_asset_USDT"},
     {"text": "EUR", "callback_data": "p2p_alert_asset_EUR"},
     {"text": "GBP", "callback_data": "p2p_alert_asset_GBP"}],
    [{"text": "⬅ Back", "callback_data": "menu_p2p"},
     {"text": "🏠 Main Menu", "callback_data": "main_menu"}],
]


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
    [{"text": "📰 Publish Post", "callback_data": "admin_publish"}],
    [{"text": "📦 Content Packages", "callback_data": "admin_content_packages"}],
    [{"text": "🌅 Post Morning", "callback_data": "adm_post_morning"},
     {"text": "☀️ Post Midday", "callback_data": "adm_post_midday"}],
    [{"text": "🌙 Post Evening", "callback_data": "adm_post_evening"},
     {"text": "📅 Post Weekly", "callback_data": "adm_post_weekly"}],
    [{"text": "🚀 Morning Pro Pkg", "callback_data": "adm_morning_pkg"}],
    [{"text": "🔄 Toggle Channel", "callback_data": "adm_toggle_channel"}],
    [{"text": "🪞 Mirror Mode", "callback_data": "adm_toggle_mirror"}],
    [{"text": "📝 Set Free Channel", "callback_data": "adm_set_channel"}],
    [{"text": "⭐ Set Pro Channel", "callback_data": "adm_set_pro_channel"}],
    [{"text": "⬅ Back", "callback_data": "admin_menu"}],
]
ADMIN_USERS_MENU = [
    [{"text": "📢 Broadcast", "callback_data": "admin_broadcast"}],
    [{"text": "🔒 Ban User", "callback_data": "admin_ban"},
     {"text": "✅ Unban User", "callback_data": "adm_unban"}],
    [{"text": "📋 Blacklist", "callback_data": "adm_blacklist"}],
    [{"text": "💎 Grant Pro", "callback_data": "adm_grant_pro"}],
    [{"text": "👥 User List", "callback_data": "admin_users"}],
    [{"text": "⬅ Back", "callback_data": "admin_menu"}],
]
ADMIN_TRADES_MENU = [
    [{"text": "📋 Trade History", "callback_data": "adm_trade_history"}],
    [{"text": "⚡ Crypto Idea", "callback_data": "menu_crypto_idea"},
     {"text": "💱 Forex Idea", "callback_data": "menu_forex"}],
    [{"text": "📊 Performance", "callback_data": "adm_performance"}],
    [{"text": "📈 Outcome Summary", "callback_data": "adm_outcome_summary"}],
    [{"text": "🔎 Run Scanner Now", "callback_data": "adm_run_scanner"}],
    [{"text": "🔒 Close Idea by ID", "callback_data": "close_bot_idea"}],
    [{"text": "⬅ Back", "callback_data": "admin_menu"}],
]
ADMIN_SYSTEM_MENU = [
    [{"text": "🏥 Health", "callback_data": "admin_health"}],
    [{"text": "📋 Logs", "callback_data": "admin_logs"}],
    [{"text": "🔄 Refresh Prices", "callback_data": "adm_refresh_prices"}],
    [{"text": "💱 P2P Snapshot Now", "callback_data": "adm_p2p_snapshot"}],
    [{"text": "🧹 Clear My State", "callback_data": "adm_clear_state"}],
    [{"text": "🧪 Test Channels", "callback_data": "adm_test_channels"}],
    [{"text": "⬅ Back", "callback_data": "admin_menu"}],
]
ADMIN_SETTINGS_MENU = [
    [{"text": "⚙️ Bot Settings", "callback_data": "admin_settings"}],
    [{"text": "🤖 Bot Mode", "callback_data": "adm_mode_menu"}],
    [{"text": "⭐ Alert Watchlist", "callback_data": "adm_set_watchlist"}],
    [{"text": "📝 Set Free Channel", "callback_data": "adm_set_channel"}],
    [{"text": "⭐ Set Pro Channel", "callback_data": "adm_set_pro_channel"}],
    [{"text": "⬅ Back", "callback_data": "admin_menu"}],
]


# ═══════════════════════════════════════════════════════════════════════════
