"""Market Pulse Bot — whale_detection module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import P2P_FIATS, logger
from market_pulse.db import get_db
from market_pulse.p2p import get_p2p_rate
from market_pulse.telegram_api import send


# ─── extracted section ───
# 🐋 WHALE / BREAKOUT DETECTION
# ═══════════════════════════════════════════════════════════════════════════

_whale_price_cache = {}  # coin -> price at last hourly snapshot
_whale_snapshot_ready = False  # True after first snapshot has been taken
_morning_btc_snapshot = {}  # {"price": float} — BTC price at morning post time, for midday conditional check



def check_p2p_rate_alerts():
    """Check user-set P2P rate alerts and notify when target is crossed."""
    db = None
    try:
        db = get_db()
        c = db.cursor()
        c.execute(
            "SELECT id, chat, crypto, fiat, condition, target FROM p2p_alerts WHERE active=1"
        )
        rows = c.fetchall()
    except Exception as e:
        logger.error("[P2P ALERTS] Load error: %s" % e)
        return
    finally:
        if db:
            try: db.close()
            except Exception: pass

    triggered_ids = []
    for row in rows:
        aid, chat_id, crypto, fiat, condition, target = row
        try:
            buy, sell, source = get_p2p_rate(crypto, fiat)
            if not buy or not sell:
                continue
            rate = buy if condition == "buy_below" else sell
            fired = (condition == "buy_below" and rate <= target) or                     (condition == "sell_above" and rate >= target)
            if fired:
                symbol = P2P_FIATS.get(fiat, ("", fiat))[1]
                direction = "dropped to or below" if condition == "buy_below" else "reached or above"
                msg = (
                    f"🔔 <b>P2P RATE ALERT</b>\n\n"
                    f"💱 {crypto}/{fiat} {condition.replace('_',' ').title()}\n"
                    f"Current rate: <b>{symbol}{int(rate):,}</b>\n"
                    f"Your target:  <b>{symbol}{int(target):,}</b>\n\n"
                    f"Rate has {direction} your target.\n"
                    f"<i>Source: {source}  ·  NFA</i>"
                )
                send(int(chat_id), msg)
                triggered_ids.append(aid)
        except Exception as e:
            logger.error(f"[P2P ALERT] {crypto}/{fiat} for {chat_id}: {e}")

    if triggered_ids:
        db2 = None
        try:
            db2 = get_db()
            c2 = db2.cursor()
            c2.execute("UPDATE p2p_alerts SET active=0 WHERE id = ANY(%s)", (triggered_ids,))
            db2.commit()
        except Exception as e:
            logger.error("[P2P ALERT DEACTIVATE] %s" % e)
            if db2:
                try: db2.rollback()
                except Exception: pass
        finally:
            if db2:
                try: db2.close()
                except Exception: pass




# ═══════════════════════════════════════════════════════════════════════════
