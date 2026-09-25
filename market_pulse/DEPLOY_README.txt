Market Pulse — final fix pack (deploy all)

FILES
  setup_lockout.py       Smart post-SL (min wait + reclaim, not dumb 6h freeze)
  trade_lifecycle.py     Close DMs with dates; lockout on STOP; notify PUBLISHED only
  forex_trade_engine.py  Dedupe; market direction buy/sell; lockout
  edge_trade_engine.py   Lockout; open list; history with dates
  handlers.py            /opentrades + richer /trades
  major_movement.py      Move anti-spam + move→wait→setup bridge
  key_alerts_engine.py   Runs bridge each cycle

AFTER DEPLOY
  /opentrades   — see open setups + generated dates
  /trades       — history with generated/closed dates

OPTIONAL ENV
  SETUP_LOCKOUT_MIN_HOURS=1
  SETUP_LOCKOUT_HOURS=24
  FOREX_PUBLISH_ENABLED=1

Optional SQL (zombie unpublished opens):
  UPDATE trade_ideas SET status='closed', result='EXPIRED'
  WHERE status='open'
    AND (publication_status IS NULL OR publication_status <> 'PUBLISHED');
