"""
Market Pulse Bot — full-spec trade alert formatter.
====================================================
Turns a market_pulse.signal_engine.analyze() result into the Telegram
alert format: every field the spec requires, every recommendation
explained (no black-box alerts).
"""

from market_pulse.helpers import format_price as _format_price, wat_now


def html_text(s):
    """Telegram messages use parse_mode=HTML — escape reserved characters
    in any dynamic text (coin names, AI-generated reasons/risks) so a
    stray '<', '>', or '&' can't break message rendering. No equivalent
    helper existed anywhere in the real bot, so this is new, not moved."""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

CATEGORY_STYLE = {
    "safe":      {"emoji": "\U0001f7e2", "label": "SAFE SETUP",
                  "desc": "Highest-probability setup — strong confirmations, cleaner but usually smaller move."},
    "medium":    {"emoji": "\U0001f7e1", "label": "MEDIUM RISK",
                  "desc": "Balanced setup — good reward potential, missing one or two confirmations."},
    "high_risk": {"emoji": "\U0001f534", "label": "HIGH RISK / HIGH REWARD",
                  "desc": "Aggressive, early entry — bigger reward potential, naturally higher chance of failure."},
}


def _reward_potential(entry, take_profit, direction):
    """% move from entry to the furthest target (TP3) — the 'estimated reward potential'."""
    if not take_profit or not entry:
        return None
    furthest = take_profit[-1]
    if direction == "long":
        return (furthest - entry) / entry * 100
    return (entry - furthest) / entry * 100


def build_alert_message(pair, result, timeframe="4H"):
    """
    `pair` — e.g. "BTC/USDT".
    `result` — the dict returned by market_pulse.signal_engine.analyze().
    Returns an HTML-formatted string ready for Telegram (parse_mode=HTML),
    or None if `result["signal"]` is False (caller should not publish).
    """
    if not result.get("signal"):
        return None

    style = CATEGORY_STYLE[result["category"]]
    direction_label = "LONG \U0001f7e2" if result["direction"] == "long" else "SHORT \U0001f534"
    reward_pct = _reward_potential(result["entry"], result["take_profit"], result["direction"])

    lines = [
        f"{style['emoji']} <b>{style['label']}</b>",
        f"<i>{style['desc']}</i>",
        "",
        f"<b>{html_text(pair)}</b>  \u00b7  {direction_label}  \u00b7  {timeframe}",
        f"Current Price: <b>{_format_price(result['current_price'])}</b>",
        "",
        "\U0001f4d0 <b>TRADE LEVELS</b>",
        f"Entry:          <b>{_format_price(result['entry'])}</b>",
        f"Stop Loss:      <b>{_format_price(result['stop_loss'])}</b>",
        f"Take Profit 1:  <b>{_format_price(result['take_profit'][0])}</b>",
        f"Take Profit 2:  <b>{_format_price(result['take_profit'][1])}</b>",
        f"Take Profit 3:  <b>{_format_price(result['take_profit'][2])}</b>",
        f"Risk : Reward:  <b>1 : {result['risk_reward']}</b>",
        "",
        "\U0001f4ca <b>ASSESSMENT</b>",
        f"Trade Category:    <b>{style['label']}</b>",
        f"Market Trend:      <b>{(result.get('trend') or 'mixed').capitalize()}</b>",
        f"Confidence Score:  <b>{result['confidence']}%</b>",
    ]
    if reward_pct is not None:
        lines.append(f"Reward Potential:  <b>+{reward_pct:.1f}%</b> (to TP3)")
    lines.append("")

    if result.get("reasons"):
        lines.append("\U0001f4a1 <b>WHY THIS TRADE</b>")
        lines += [f"\u2022 {html_text(r)}" for r in result["reasons"]]
        lines.append("")

    if result.get("confirmations"):
        met = [c["name"].replace("_", " ") for c in result["confirmations"] if c["met"]]
        if met:
            lines.append("\u2705 <b>CONFIRMATIONS USED</b>")
            lines.append(", ".join(met))
            lines.append("")

    if result.get("risks"):
        lines.append("\u26a0\ufe0f <b>POSSIBLE RISKS</b>")
        lines += [f"\u2022 {html_text(r)}" for r in result["risks"]]
        lines.append("")

    lines.append(f"<i>{wat_now().strftime('%Y-%m-%d %H:%M')} WAT</i>")

    return "\n".join(lines)


def build_no_signal_message(pair, result):
    """
    Optional: a short admin-only note explaining why no trade was published
    for `pair` this cycle — keeps the reasoning transparent even when
    nothing gets sent to the channel.
    """
    risks = result.get("risks") or ["No qualifying setup."]
    return (
        f"\u23f8 <b>{html_text(pair)}</b> \u2014 no signal this cycle\n"
        + "\n".join(f"\u2022 {html_text(r)}" for r in risks)
    )
