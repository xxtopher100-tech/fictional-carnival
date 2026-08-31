"""Market Pulse Bot — portfolio module (split from the real monolithic bot.py)."""

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

from market_pulse.db import get_db
from market_pulse.price_fetchers import get_best_price


# ─── extracted section ───
# 📊 PORTFOLIO FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def get_portfolio_value(chat_id):
    try:
        db = get_db()
        c = db.cursor()
        c.execute("SELECT coin, amount, buy_price FROM portfolio WHERE chat=%s", (str(chat_id),))
        rows = c.fetchall()
        db.close()
        
        total_invested = 0
        total_current = 0
        positions = []
        
        for coin, amount, buy_price in rows:
            current_price, _ = get_best_price(coin)
            if current_price:
                invested = amount * buy_price
                current = amount * current_price
                pnl = current - invested
                pnl_pct = (pnl / invested) * 100 if invested > 0 else 0
                positions.append({
                    "coin": coin,
                    "amount": amount,
                    "buy_price": buy_price,
                    "current_price": current_price,
                    "invested": invested,
                    "current": current,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct
                })
                total_invested += invested
                total_current += current
        
        return {
            "positions": positions,
            "total_invested": total_invested,
            "total_current": total_current,
            "total_pnl": total_current - total_invested,
            "total_pnl_pct": ((total_current - total_invested) / total_invested * 100) if total_invested > 0 else 0
        }
    except Exception as _e:
        return None

# ═══════════════════════════════════════════════════════════════════════════
