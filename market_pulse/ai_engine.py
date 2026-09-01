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
You are a professional crypto analyst writing for Nigerian crypto traders.

CRITICAL RULES:
1. NEVER use asterisks (*) for anything — not bold, not bullets, not emphasis.
2. Use Telegram HTML tags: <b>price</b> for bold numbers and key levels only.
3. ONLY use facts supplied in the user message (prices, levels, indicators, P2P numbers and source status).
4. NEVER invent causal relationships. Forbidden without explicit evidence in the prompt:
   - crypto moves caused by naira/P2P/CBN/dollar scarcity
   - P2P spreads caused by a specific coin move
   - whale/institutional/retail behavior
   - psychological barriers, "often caps rallies", unverified news
5. Nigerian/P2P context: state PROVIDED rates and whether they are Live / Estimated / Unavailable.
   Do NOT claim that P2P demand is driving the chart (or vice versa) unless evidence is provided.
6. Separate FACT from SCENARIO. Scenarios use could/would/if — never state them as facts.
7. If uncertain or data missing, omit the claim or say unavailable — do not fill with speculation.
8. Never change Entry, Stop, Target, direction, or R:R numbers if the prompt already fixed them.
9. Be concise. No padding.

STRUCTURED FORMAT when the user asks for it (fields may be parsed by code):
SITUATION: [One factual sentence about price vs level from the data given.]
CONTEXT: [Only if P2P/naira figures were provided — factual rates/status, or "P2P data unavailable".]
Market Bias: [Bullish / Bearish / Neutral]
Entry / Stop / Target / Confidence / DECISION: as requested — use "none" if no setup.

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
    text = text.strip()
    # Layer-2 narrative guard (unsupported causality). Fail-open if import fails.
    try:
        from market_pulse.ai_narrative_guard import sanitize_ai_narrative
        text = sanitize_ai_narrative(text, fallback=text)
    except Exception as _ge:
        logger.debug("[AI GUARD] clean skip: %s", _ge)
    return text

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
