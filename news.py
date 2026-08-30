"""Market Pulse Bot — news module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import NEWS_RSS_FEEDS, logger
from market_pulse.helpers import get_random_headers, wat_now


# ─── extracted section ───
# 📰 NEWS SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

_news_cache = {"data": None, "timestamp": None}

def _parse_rss(xml_text, source_name):
    articles = []
    try:
        root = ET.fromstring(xml_text)
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            url = (item.findtext("link") or "").strip()
            if title and url:
                articles.append({"title": title, "url": url, "source": {"title": source_name}})
    except Exception as _e:
        logger.debug("[SILENT EXC] %s" % _e)
    return articles

def get_crypto_news():
    global _news_cache
    now = wat_now()
    if (_news_cache["timestamp"] and (now - _news_cache["timestamp"]).total_seconds() < 900):
        return _news_cache["data"]
    
    all_articles = []
    for source_name, rss_url in NEWS_RSS_FEEDS:
        try:
            r = requests.get(rss_url, timeout=8, headers=get_random_headers())
            if r.status_code == 200:
                all_articles.extend(_parse_rss(r.text, source_name)[:3])
        except Exception as _e:
            continue
    
    if all_articles:
        _news_cache["data"] = all_articles[:10]
        _news_cache["timestamp"] = now
        return _news_cache["data"]
    
    return _news_cache["data"]

# ═══════════════════════════════════════════════════════════════════════════
