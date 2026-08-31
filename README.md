# Market Pulse Bot

AI-powered crypto market intelligence for Nigerian traders — live prices,
technical analysis, AI trade narratives, and a deterministic rule-based
signal engine, delivered via Telegram.

## Before you deploy — read this first

**Binance and Bybit are geo-blocked on a default (US-region) Railway
deployment.** Only Kraken will connect if you don't fix this. When you
create the Railway project, set the **Region** to **Europe West** or
**Asia Southeast** during setup — not the default. This isn't a code bug,
it's a network-level restriction on Binance/Bybit's side.

**Hardcoded fallback IDs**: `config_runtime.py` has real-looking numeric
defaults for `ADMIN_IDS`, `CHANNEL_ID`, and `PRO_CHANNEL_ID` baked in as
fallbacks if the environment variables aren't set. Make sure you set all
three explicitly in Railway — don't rely on the fallback.

## Setup

1. Create a new Railway project, connect it to this repo.
2. Add a Postgres database to the project (Railway can provision one) —
   `DATABASE_URL` gets set automatically when you do.
3. Set the deployment **region** as noted above.
4. Add these environment variables in Railway's Variables tab:

   | Variable | What it is |
   |---|---|
   | `BOT_TOKEN` | Telegram bot token from @BotFather — generate a fresh one, don't reuse an old one |
   | `ADMIN_IDS` | Your Telegram user ID(s) |
   | `CHANNEL_ID` | Free channel's Telegram ID |
   | `PRO_CHANNEL_ID` | Pro channel's Telegram ID |
   | `DEEPSEEK_KEY` / `MISTRAL_KEY` / `QWEN_KEY` | Whichever AI provider key(s) the AI trade engine uses |
   | `ADMIN_CODE` | Optional, defaults to blank |

   (`DATABASE_URL` is set automatically by Railway's Postgres addon.)

5. Deploy. `init_db()` creates all tables automatically on first boot,
   including `pro_decisions` (new this rebuild).

## Verify it's actually working

Check the logs for:
- `[WS BINANCE]`, `[WS KRAKEN]`, `[WS BYBIT]` all showing **Connected**,
  not repeated reconnect attempts
- `[CANDLE ENGINE] Backfilling...` then `Connected — streaming...`
- `[DERIV BYBIT] Connected` and `[DERIV OKX] Connected`

## Architecture

`bot.py` is a 15-line entry point. Everything else lives under
`market_pulse/`, one file per concern:

- **Core**: `config_runtime`, `helpers`, `db`, `users`, `telegram_api`,
  `menus`, `channel_lock`, `pro_system`
- **Data**: `price_engine` (live WS prices, Binance>Bybit>Kraken priority
  with automatic REST fallback), `price_fetchers`, `candle_engine`
  (OHLCV history via WS kline stream), `derivatives_engine` (funding
  rate/OI/liquidations, Bybit primary + OKX fallback),
  `websocket_protocol` (shared low-level RFC 6455 client)
- **Analysis**: `indicators_ext` (EMA/MACD/ADX/etc.), `market_structure`
  (support/resistance, breakouts, FVGs), `patterns` (candlestick + chart
  patterns), `signal_engine` (deterministic multi-confirmation scoring),
  `alert_formatter`
- **Content**: `channel_posts`, `content_engine`, `alerts`, `ai_engine`,
  `edge_trade_engine`, `forex_trade_engine`, `whale_detection`,
  `arbitrage`, `trade_scanner`, `screens`, `morning_package`
- **Everything else**: `handlers.py` — the main Telegram poll loop and
  command/callback router (~3,000 lines; it's one dispatch function in
  the original code, so it wasn't split further)

## What's real vs. not yet wired in

The signal engine (`signal_engine.py`) now grounds every Pro AI briefing
(morning/midday/evening/weekly) in real computed technicals instead of
letting the AI invent Entry/Stop/Target numbers, and shows its own
independent read alongside the AI's. This currently only covers **BTC**.

**Deliberately not built** (rather than guessed at):
- OKX open interest and order book — couldn't verify real WS payload
  field names, only REST endpoint names
- Binance and Kraken derivatives (funding/OI/liquidations)
- Signal engine coverage beyond BTC

## A note on trust

The signal engine's math (indicators, pattern detection, structure
detection) has been unit-tested against known-correct synthetic data and
real documented exchange payload examples — the calculations are verified
correct. Its actual trading *judgment* has not been backtested against
real historical market data. Treat its early output as unproven until
you've watched it against real prices for a while.
