"""Market Pulse Bot — trade_journal module (split from the real monolithic bot.py)."""

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
# 📈 TRADE JOURNAL FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def close_trade(chat_id, trade_id, exit_price=None):
    try:
        db = get_db()
        c = db.cursor()
        
        c.execute("SELECT coin, direction, entry_price, size, status FROM trade_journal WHERE id=%s AND chat=%s",
                  (trade_id, str(chat_id)))
        row = c.fetchone()
        
        if not row:
            return {"error": "Trade not found"}
        
        coin, direction, entry_price, size, status = row
        
        if status == "closed":
            return {"error": "Trade already closed"}
        
        if exit_price is None:
            exit_price, _ = get_best_price(coin)
            if not exit_price:
                return {"error": "Could not get current price"}
        
        if direction == "LONG":
            pnl = (exit_price - entry_price) * size
        else:
            pnl = (entry_price - exit_price) * size
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        c.execute("UPDATE trade_journal SET exit_price=%s, pnl=%s, status='closed', closed_at=%s WHERE id=%s",
                  (exit_price, pnl, now, trade_id))
        db.commit()
        db.close()
        
        return {"pnl": pnl, "exit_price": exit_price}
    except Exception as e:
        return {"error": str(e)}

# ═══════════════════════════════════════════════════════════════════════════
