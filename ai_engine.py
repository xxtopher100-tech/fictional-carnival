"""Market Pulse Bot — ai_engine module (split from the real monolithic bot.py)."""

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

from market_pulse.config_runtime import DEEPSEEK_KEY, MISTRAL_KEY, QWEN_KEY, logger


# ─── extracted section ───
# 🤖 AI SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

AI_SYSTEM_PROMPT = """
You are a professional crypto analyst writing for Nigerian crypto traders. You understand Nigerian FX dynamics deeply — P2P rates, naira volatility, CBN policy, dollar scarcity.

CRITICAL RULES:
1. NEVER use asterisks (*) for anything — not bold, not bullets, not emphasis.
2. Use Telegram HTML tags: <b>price</b> for bold numbers and key levels only.
3. NEVER invent historical events, institutional activity, or macro news. Only state what can be observed from price data.
4. Separate facts from predictions. Facts come from data. Predictions are scenarios, not certainties.
5. If no quality setup exists, say so clearly — never force a trade.
6. Be concise. No padding. No generic phrases.

STRUCTURED FORMAT (use exactly — fields are parsed by code):
SITUATION: [One sentence — what is happening RIGHT NOW at this price level. Use correct terminology: Testing Support / Testing Resistance / Breakout / Breakdown. Bold the key level.]
CONTEXT: [One sentence — Nigerian trader angle. P2P implication or naira risk. Bold any key naira figure.]
Market Bias: [Bullish / Bearish / Neutral]
Entry: $[exact price or "none" if no setup]
Stop: $[exact price or "none"]
Target: $[exact price or "none"]
Confidence: [High / Moderate / Low / Uncertain — based on trend, momentum, and level strength only]
DECISION: [One sentence — exactly what you would do right now, or clearly state: Wait — [reason]]

End with: NFA — manage your risk.
"""

def ask_deepseek(question):
    if not DEEPSEEK_KEY:
        return None
    try:
        resp = requests.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": "Bearer %s" % DEEPSEEK_KEY,
                     "Content-Type": "application/json"},
            json={
                "model": "deepseek-chat",
                "messages": [{"role": "system", "content": AI_SYSTEM_PROMPT},
                             {"role": "user", "content": question}],
                "max_tokens": 800,
                "temperature": 0.7,
            },
            timeout=30
        )
        data = resp.json()
        if "choices" in data:
            return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.error("[DEEPSEEK ERROR] %s" % e)
    return None

def ask_mistral(question):
    if not MISTRAL_KEY:
        return None
    try:
        resp = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers={"Authorization": "Bearer %s" % MISTRAL_KEY,
                     "Content-Type": "application/json"},
            json={
                "model": "mistral-small-latest",
                "messages": [{"role": "system", "content": AI_SYSTEM_PROMPT},
                             {"role": "user", "content": question}],
                "max_tokens": 800,
            },
            timeout=30
        )
        data = resp.json()
        if "choices" in data:
            return data["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.error("[MISTRAL ERROR] %s" % e)
    return None

def ask_qwen(question):
    if not QWEN_KEY:
        return None
    try:
        resp = requests.post(
            "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
            headers={"Authorization": "Bearer %s" % QWEN_KEY,
                     "Content-Type": "application/json"},
            json={
                "model": "qwen-turbo",
                "input": {"messages": [{"role": "system", "content": AI_SYSTEM_PROMPT},
                                       {"role": "user", "content": question}]},
                "parameters": {"max_tokens": 800}
            },
            timeout=30
        )
        data = resp.json()
        if "output" in data and "text" in data["output"]:
            return data["output"]["text"].strip()
    except Exception as e:
        logger.error("[QWEN ERROR] %s" % e)
    return None

def _clean_ai_response(text):
    if not text:
        return text
    # Convert markdown bold to Telegram HTML bold
    text = re.sub(r'[*][*](.+?)[*][*]', r'<b>\1</b>', text, flags=re.DOTALL)
    text = re.sub(r'[*](.+?)[*]', r'<b>\1</b>', text)
    text = text.replace('*', '')
    # Strip markdown headers (### / ## / #)
    text = re.sub(r'^#{1,3}\s+', '', text, flags=re.MULTILINE)
    # Collapse excessive blank lines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

def ask_ai(question):
    providers = [
        ("DeepSeek", ask_deepseek),
        ("Mistral", ask_mistral),
        ("Qwen", ask_qwen),
    ]
    for name, func in providers:
        try:
            result = func(question)
            if result:
                return _clean_ai_response(result), name
        except Exception as _e:
            continue
    return None, None

# ═══════════════════════════════════════════════════════════════════════════
