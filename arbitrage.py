"""Market Pulse Bot — arbitrage module (split from the real monolithic bot.py)."""

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

from market_pulse.price_fetchers import get_coingecko_batch, get_kraken_batch, get_okx_batch


# ─── extracted section ───
# 🔄 ARBITRAGE SCANNER
# ═══════════════════════════════════════════════════════════════════════════

def scan_arbitrage():
    opportunities = []
    kraken = get_kraken_batch()
    okx = get_okx_batch()
    cg = get_coingecko_batch()
    
    sources = [("Kraken", kraken), ("OKX", okx), ("CoinGecko", cg)]
    
    for coin in ["BTC", "ETH", "SOL", "BNB", "XRP"]:
        prices = []
        for name, data in sources:
            if coin in data and data[coin].get("price"):
                p = data[coin]["price"]
                if p and p > 0:
                    prices.append((name, float(p)))
        if len(prices) < 2:
            continue
        prices.sort(key=lambda x: x[1])
        low_src, low_price = prices[0]
        high_src, high_price = prices[-1]
        gap_pct = (high_price - low_price) / low_price * 100
        if gap_pct >= 0.3:
            opportunities.append({
                "coin": coin,
                "buy_from": low_src,
                "buy_price": low_price,
                "sell_to": high_src,
                "sell_price": high_price,
                "gap_pct": gap_pct
            })
    
    return opportunities

# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MAIN RUN LOOP
# ═══════════════════════════════════════════════════════════════════════════



# ═══════════════════════════════════════════════════════════════════════════
