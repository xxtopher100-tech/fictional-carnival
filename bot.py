"""
Market Pulse Bot — v26 "Morning Pro Package + Mirror Mode + Data Fixes"
============================================
AI-powered crypto intelligence for Nigerian traders.

This file is now just the entry point. All logic lives in the
market_pulse/ package — see market_pulse/handlers.py for the main
Telegram poll loop and command/callback routing.
"""

from market_pulse.handlers import run

if __name__ == "__main__":
    run()
