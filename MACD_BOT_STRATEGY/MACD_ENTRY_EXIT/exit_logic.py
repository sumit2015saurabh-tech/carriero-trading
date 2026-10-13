"""
=============================================================================
QTI Fund  |  Model: !JFMX3X_NONCORR_MACD_V3.x
FILE: exit_logic.py
DESCRIPTION:
    Implements the Trade Exit Logic (Risk Management) as specified in
    "Entry & Exit Logic Specification v1.5".

    Two exit methods are supported:
    ──────────────────────────────
    • Condition 1 – 5-Bar Trailing Stop approach
        Trailing stops are tightened by looking at the lowest low (LONG)
        or highest high (SHORT) over the previous 5 one-minute candles.

    • Condition 2 – Percentage-Based Trailing Stop approach
        Trailing stops follow price at a fixed percentage distance
        (TrailingPerc %). Selected by setting TrailingStop_Perc = "Y" in
        config.json.

    Which method a symbol uses is resolved PER STOCK (2026-07-31): the Stocks
    sheet's "TrailingStop_Perc" column (col AC) overrides the Global Settings
    value for that symbol; a blank cell inherits the global. So one stock can
    run the percentage trail while the rest stay on the 5-bar method.

    Exit scenarios:
    ───────────────
    A – Price trending in our favour  → tighten trailing stop
    B – Price reaches stop level      → close position
    C – End-of-Day time reached       → close at market

    LIVE-EDIT DESIGN:
        config.json is re-read on EVERY call to process_exit() so any
        parameter changes take effect immediately without a restart.

USAGE:
    from exit_logic import process_exit, ExitAction

    # Build a position dict (created when entry fills):
    position = {
        "symbol"           : "AAPL",
        "direction"        : "LONG",        # "LONG" or "SHORT"
        "entry_price"      : 182.50,
        "stop_price"       : 181.90,
        "stop_limit_price" : 181.90,
        "target_price"     : 184.30,
        "be_activated"     : False,         # has breakeven been triggered?
        "max_high"         : 182.50,        # highest price seen since entry (LONG)
        "min_low"          : 182.50,        # lowest price seen since entry (SHORT)
        "high_history"     : [],            # last N 1-min highs (used by 5-bar)
        "low_history"      : [],            # last N 1-min lows
        "prev_stop"        : 181.90,        # stop price from previous bar
    }

    tick = {
        "current_time" : datetime.now(),
        "price"        : 183.10,
        "high"         : 183.20,
        "low"          : 182.80,
        "close"        : 183.00,
        "prev_high"    : 182.90,            # previous bar high
        "prev_low"     : 182.70,            # previous bar low
    }

    action = process_exit(position, tick)
    print(action)   # ExitAction dict
=============================================================================
"""

import json
import os
import logging
from datetime import datetime, time as dtime
from typing import Dict, Any

# ─── Path to shared config ────────────────────────────────────────────────────
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

# ─── Logger ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [EXIT] %(levelname)s – %(message)s",
)
logger = logging.getLogger("exit_logic")


# =============================================================================
# CONFIG LOADER  (re-reads disk on every call → live edits work instantly)
# =============================================================================

_CFG_CACHE: Dict[str, Any] = None
_CFG_MTIME: float = -1.0

def load_config() -> Dict[str, Any]:
    """Return config.json, re-reading from disk ONLY when its mtime changes.
    Called on every bar/tick, so caching avoids a disk read + json parse on the
    hot path (which was blocking the IB reader thread). excel_sync bumps the
    mtime on any edit, so changes still apply instantly."""
    global _CFG_CACHE, _CFG_MTIME
    try:
        mt = os.path.getmtime(CONFIG_PATH)
    except OSError:
        mt = -1.0
    if _CFG_CACHE is None or mt != _CFG_MTIME:
        with open(CONFIG_PATH, "r") as fh:
            _CFG_CACHE = json.load(fh)
        _CFG_MTIME = mt
    return _CFG_CACHE


# Lazy import to avoid circular dependency at module load time. Used by
# process_exit() to apply per-stock overrides (mpl, bem, signal_l, …) on the
# exit side — previously this merge only happened in entry_logic, so BE/trail
# math used global values regardless of per-stock settings.
def _resolve_stock_cfg(symbol: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    from entry_logic import _resolve_stock_cfg as _resolver
    return _resolver(symbol, cfg)


def _parse_time(t_str: str) -> dtime:
    return datetime.strptime(t_str, "%H:%M").time()


# =============================================================================
# ExitAction — what the caller should do after process_exit() returns
# =============================================================================

def _make_action(
    action_type: str,
    symbol: str,
    direction: str,
    updates: Dict = None,
    orders: Dict = None,
    reason: str = "",
) -> Dict:
    """
    Build a standardised action dict.

    action_type values
    ──────────────────
    NONE            – nothing changed, keep monitoring
    UPDATE_STOPS    – modify the stop/stop-limit orders with new prices
    CLOSE_LIMIT     – close position at the target limit price
    CLOSE_MARKET    – close position at market (end-of-day)
    STOP_HIT        – stop was hit; TradeStation will execute automatically
    """
    return {
        "action"    : action_type,
        "symbol"    : symbol,
        "direction" : direction,
        "updates"   : updates or {},
        "orders"    : orders or {},
        "reason"    : reason,
    }


# =============================================================================
# SHARED HELPERS
# =============================================================================

def _keep_n_bars(history: list, value: float, n: int = 6) -> list:
    """
    Append a new bar value to a rolling history list capped at n items.
    Oldest item is dropped when the list exceeds n.
    """
    history.append(value)
    return history[-n:]


def _lowest_of_prev5(low_history: list) -> float:
    """Return the minimum of up to the 5 most recent lows (excluding current bar)."""
    prev = low_history[:-1] if len(low_history) > 1 else low_history
    return min(prev[-5:])


def _highest_of_prev5(high_history: list) -> float:
    """Return the maximum of up to the 5 most recent highs (excluding current bar)."""
    prev = high_history[:-1] if len(high_history) > 1 else high_history
    return max(prev[-5:])


# =============================================================================
# CONDITION 1 — 5-BAR TRAILING STOP
# =============================================================================

def _exit_5bar_long(position: Dict, tick: Dict, cfg: Dict) -> Dict:
    """
    5-bar trailing stop logic for LONG positions.

    Scenario A (price moves in our favour):
        a) Once High > Entry + MPL*BEM AND Close > StopPrice + 0.02
           → Activate breakeven: move stops to Entry + MPL*BEM*0.5
        b) After (a) is active: if Low > prev_Low
           → Trail stops to lowest low of previous 5 bars
        c) If price >= Target → close at limit

    Scenario B (price moves against us):
        Stop/Stop-Limit is hit → TradeStation handles execution.
        We detect this and return STOP_HIT so the caller can clean up state.

    Scenario C is handled in the main process_exit() function.
    """
    sym        = position["symbol"]
    entry      = position["entry_price"]
    stop       = position["stop_price"]
    be_active  = position["be_activated"]
    mpl        = cfg["MPL_dollar_Amount"]
    bem        = cfg["BEMultiplier"]
    tgt        = cfg["TargetMultiplier"]
    sls        = cfg["StopLimitSpread"]
    verbose    = cfg.get("AdditionalPrintLogs", "Y").upper() == "Y"

    high       = tick["high"]
    low        = tick["low"]
    close      = tick["close"]

    # ── Update rolling history ────────────────────────────────────────────
    position["high_history"] = _keep_n_bars(position.get("high_history", []), high)
    position["low_history"]  = _keep_n_bars(position.get("low_history", []),  low)

    # ── Scenario C gate is handled upstream ──────────────────────────────

    # ── Scenario B: stop hit ──────────────────────────────────────────────
    if close <= stop:
        if verbose:
            logger.info("[%s] LONG stop hit at %.4f (stop=%.4f)", sym, close, stop)
        return _make_action("STOP_HIT", sym, "LONG",
                            reason=f"Close {close} <= Stop {stop}")

    # ── Scenario A-c: TARGET (CLOSE_LIMIT) — v1.5 spec, RE-ENABLED 2026-07-04 ─
    # Take profit at Entry + MPL*TargetMultiplier (per user: restore spec exit).
    target_price = entry + (mpl * tgt) if entry > 0 else float("inf")
    if close >= target_price:
        if verbose:
            logger.info("[%s] LONG target hit: close=%.4f >= target=%.4f",
                        sym, close, target_price)
        return _make_action(
            "CLOSE_LIMIT", sym, "LONG",
            orders={"limit_price": round(target_price, 2)},
            reason=f"Target {target_price:.4f} reached",
        )

    # ── Scenario A-a: BREAKEVEN activation (v1.5 spec) ───────────────────
    # IF High > Entry + MPL*BEM  AND  Close > Stop + 0.02
    #   → move stops to Entry + MPL*BEM*0.5 (stop order sits 0.05 below).
    if not be_active:
        be_trigger = entry + (mpl * bem)
        if high > be_trigger and close > (stop + 0.02):
            anchor   = entry + (mpl * bem * 0.5)     # stop-limit stop/limit price
            new_stop = round(anchor - 0.05, 2)       # stop (market) order price
            position["be_activated"]     = True
            position["stop_price"]       = new_stop
            position["stop_limit_price"] = round(anchor, 2)
            if verbose:
                logger.info("[%s] LONG breakeven activated: stop=%.4f (anchor=%.4f)",
                            sym, new_stop, anchor)
            return _make_action(
                "UPDATE_STOPS", sym, "LONG",
                updates={"stop_price": new_stop},
                reason=f"Breakeven activated (5-bar LONG): stop={new_stop:.4f}",
            )
        return _make_action("NONE", sym, "LONG", reason="Awaiting breakeven (5-bar LONG)")

    # ── Scenario A-b: 5-BAR TRAIL (v1.5 spec, only after breakeven) ──────
    # IF current Low > previous Low → trail stops to the lowest low of the
    # previous 5 candles (stop order sits 0.05 below). Ratchets UP only.
    lows     = position["low_history"]
    prev_low = lows[-2] if len(lows) >= 2 else lows[-1]
    if low > prev_low:
        lowest5  = _lowest_of_prev5(lows)
        new_stop = round(lowest5 - 0.05, 2)
        if new_stop > position["stop_price"]:        # never loosen the stop
            position["stop_price"]       = new_stop
            position["stop_limit_price"] = round(lowest5, 2)
            if verbose:
                logger.info("[%s] LONG 5-bar trail: lowest5=%.4f new_stop=%.4f",
                            sym, lowest5, new_stop)
            return _make_action(
                "UPDATE_STOPS", sym, "LONG",
                updates={"stop_price": new_stop},
                reason=f"5-bar trail: stop={new_stop:.4f} (lowest5={lowest5:.4f})",
            )

    return _make_action("NONE", sym, "LONG", reason="No exit condition met (5-bar LONG)")


def _exit_5bar_short(position: Dict, tick: Dict, cfg: Dict) -> Dict:
    """
    5-bar trailing stop logic for SHORT positions.

    Mirror of _exit_5bar_long with directions reversed.
    """
    sym        = position["symbol"]
    entry      = position["entry_price"]
    stop       = position["stop_price"]
    be_active  = position["be_activated"]
    mpl        = cfg["MPL_dollar_Amount"]
    bem        = cfg["BEMultiplier"]
    tgt        = cfg["TargetMultiplier"]
    sls        = cfg["StopLimitSpread"]
    verbose    = cfg.get("AdditionalPrintLogs", "Y").upper() == "Y"

    high       = tick["high"]
    low        = tick["low"]
    close      = tick["close"]

    position["high_history"] = _keep_n_bars(position.get("high_history", []), high)
    position["low_history"]  = _keep_n_bars(position.get("low_history", []),  low)

    # ── Scenario B ────────────────────────────────────────────────────────
    if close >= stop:
        return _make_action("STOP_HIT", sym, "SHORT",
                            reason=f"Close {close} >= Stop {stop}")

    # ── Scenario A-c: TARGET (CLOSE_LIMIT) — v1.5 spec, RE-ENABLED 2026-07-04 ─
    # Take profit at Entry - MPL*TargetMultiplier (buy-to-cover).
    target_price = entry - (mpl * tgt) if entry > 0 else float("-inf")
    if close <= target_price:
        if verbose:
            logger.info("[%s] SHORT target hit: close=%.4f <= target=%.4f",
                        sym, close, target_price)
        return _make_action(
            "CLOSE_LIMIT", sym, "SHORT",
            orders={"limit_price": round(target_price, 2)},
            reason=f"Target {target_price:.4f} reached",
        )

    # ── Scenario A-a: BREAKEVEN activation (v1.5 spec) ───────────────────
    # IF Low < Entry - MPL*BEM  AND  Close < Stop - 0.02
    #   → move stops to Entry - MPL*BEM*0.5 (stop order sits 0.05 above).
    if not be_active:
        be_trigger = entry - (mpl * bem)
        if low < be_trigger and close < (stop - 0.02):
            anchor   = entry - (mpl * bem * 0.5)
            new_stop = round(anchor + 0.05, 2)
            position["be_activated"]     = True
            position["stop_price"]       = new_stop
            position["stop_limit_price"] = round(anchor, 2)
            if verbose:
                logger.info("[%s] SHORT breakeven activated: stop=%.4f (anchor=%.4f)",
                            sym, new_stop, anchor)
            return _make_action(
                "UPDATE_STOPS", sym, "SHORT",
                updates={"stop_price": new_stop},
                reason=f"Breakeven activated (5-bar SHORT): stop={new_stop:.4f}",
            )
        return _make_action("NONE", sym, "SHORT", reason="Awaiting breakeven (5-bar SHORT)")

    # ── Scenario A-b: 5-BAR TRAIL (v1.5 spec, only after breakeven) ──────
    # IF current High < previous High → trail stops to the highest high of the
    # previous 5 candles (stop order sits 0.05 above). Ratchets DOWN only.
    highs     = position["high_history"]
    prev_high = highs[-2] if len(highs) >= 2 else highs[-1]
    if high < prev_high:
        highest5 = _highest_of_prev5(highs)
        new_stop = round(highest5 + 0.05, 2)
        if new_stop < position["stop_price"]:        # never loosen the stop
            position["stop_price"]       = new_stop
            position["stop_limit_price"] = round(highest5, 2)
            if verbose:
                logger.info("[%s] SHORT 5-bar trail: highest5=%.4f new_stop=%.4f",
                            sym, highest5, new_stop)
            return _make_action(
                "UPDATE_STOPS", sym, "SHORT",
                updates={"stop_price": new_stop},
                reason=f"5-bar trail: stop={new_stop:.4f} (highest5={highest5:.4f})",
            )

    return _make_action("NONE", sym, "SHORT", reason="No exit condition met (5-bar SHORT)")


# =============================================================================
# CONDITION 2 — PERCENTAGE-BASED TRAILING STOP
# =============================================================================

def _exit_pct_long(position: Dict, tick: Dict, cfg: Dict) -> Dict:
    """
    Percentage-based trailing stop for LONG positions.
    Runs at the tick level after breakeven is activated.

    If a new High is hit AND stop is already above entry:
        new_stop = New_High * (1 - TrailingPerc/100)

    Stops only ever move UP.
    Scenario A-c (target) and Scenario A-a (breakeven) are identical to
    the 5-bar approach.
    """
    sym        = position["symbol"]
    entry      = position["entry_price"]
    stop       = position["stop_price"]
    be_active  = position["be_activated"]
    mpl        = cfg["MPL_dollar_Amount"]
    bem        = cfg["BEMultiplier"]
    tgt        = cfg["TargetMultiplier"]
    sls        = cfg["StopLimitSpread"]
    trail_perc = float(cfg["TrailingPerc"]) / 100.0   # e.g. 0.5 → 0.005
    verbose    = cfg.get("AdditionalPrintLogs", "Y").upper() == "Y"

    price      = tick["price"]   # current tick price
    high       = tick.get("high", price)
    close      = tick.get("close", price)

    # Track the running maximum high for percentage trailing
    if high > position.get("max_high", entry):
        position["max_high"] = high

    # ── Scenario B ────────────────────────────────────────────────────────
    if price <= stop:
        return _make_action("STOP_HIT", sym, "LONG",
                            reason=f"Price {price} <= Stop {stop}")

    # ── Scenario A-c: TARGET (CLOSE_LIMIT) — v1.5 spec, RE-ENABLED 2026-07-04 ─
    target_price = entry + (mpl * tgt) if entry > 0 else float("inf")
    if price >= target_price:
        return _make_action("CLOSE_LIMIT", sym, "LONG",
            orders={"limit_price": round(target_price, 2)},
            reason=f"Target {target_price:.4f} reached")

    # ── Scenario A-a: breakeven (identical to 5-bar) ─────────────────────
    if not be_active:
        be_trigger = entry + (mpl * bem)
        if high > be_trigger and close > (stop + 0.02):
            new_stop_sl = entry + (mpl * bem * 0.5)
            new_stop    = new_stop_sl - 0.05
            position["be_activated"]     = True
            position["stop_price"]       = round(new_stop, 2)
            position["stop_limit_price"] = round(new_stop_sl, 2)
            return _make_action(
                "UPDATE_STOPS", sym, "LONG",
                updates={
                    "stop_price"            : round(new_stop, 2),
                    "stop_limit_stop_price" : round(new_stop_sl + sls, 2),
                    "stop_limit_limit_price": round(new_stop_sl, 2),
                },
                reason="Breakeven activated (pct LONG)",
            )
        return _make_action("NONE", sym, "LONG", reason="Awaiting breakeven (pct)")

    # ── Scenario A-b: percentage trail ────────────────────────────────────
    # Only runs after breakeven activated AND stop is above entry
    if stop > entry:
        new_high = position["max_high"]
        new_stop  = new_high * (1 - trail_perc) - 0.05
        new_sl    = new_high * (1 - trail_perc)

        if new_stop > position["stop_price"]:   # trail only moves UP
            if verbose:
                logger.info(
                    "[%s] LONG pct-trail: new_high=%.4f trail_perc=%.2f%% new_stop=%.4f",
                    sym, new_high, trail_perc * 100, new_stop
                )
            position["stop_price"]       = round(new_stop, 2)
            position["stop_limit_price"] = round(new_sl, 2)
            return _make_action(
                "UPDATE_STOPS", sym, "LONG",
                updates={
                    "stop_price"            : round(new_stop, 2),
                    "stop_limit_stop_price" : round(new_sl + sls, 2),
                    "stop_limit_limit_price": round(new_sl, 2),
                },
                reason=f"Pct trail: new_high={new_high:.4f}",
            )

    return _make_action("NONE", sym, "LONG", reason="No exit condition met (pct LONG)")


def _exit_pct_short(position: Dict, tick: Dict, cfg: Dict) -> Dict:
    """
    Percentage-based trailing stop for SHORT positions.
    Stops trail price upward — only move DOWN.
    """
    sym        = position["symbol"]
    entry      = position["entry_price"]
    stop       = position["stop_price"]
    be_active  = position["be_activated"]
    mpl        = cfg["MPL_dollar_Amount"]
    bem        = cfg["BEMultiplier"]
    tgt        = cfg["TargetMultiplier"]
    sls        = cfg["StopLimitSpread"]
    trail_perc = float(cfg["TrailingPerc"]) / 100.0
    verbose    = cfg.get("AdditionalPrintLogs", "Y").upper() == "Y"

    price      = tick["price"]
    low        = tick.get("low", price)
    close      = tick.get("close", price)

    # Track the running minimum low for percentage trailing
    if low < position.get("min_low", entry):
        position["min_low"] = low

    # ── Scenario B ────────────────────────────────────────────────────────
    if price >= stop:
        return _make_action("STOP_HIT", sym, "SHORT",
                            reason=f"Price {price} >= Stop {stop}")

    # ── Scenario A-c: TARGET (CLOSE_LIMIT) — v1.5 spec, RE-ENABLED 2026-07-04 ─
    target_price = entry - (mpl * tgt) if entry > 0 else float("-inf")
    if price <= target_price:
        return _make_action("CLOSE_LIMIT", sym, "SHORT",
            orders={"limit_price": round(target_price, 2)},
            reason=f"Target {target_price:.4f} reached")

    # ── Scenario A-a: breakeven ───────────────────────────────────────────
    if not be_active:
        be_trigger = entry - (mpl * bem)
        if low < be_trigger and close < (stop - 0.02):
            new_stop_sl = entry - (mpl * bem * 0.5)
            new_stop    = new_stop_sl + 0.05
            position["be_activated"]     = True
            position["stop_price"]       = round(new_stop, 2)
            position["stop_limit_price"] = round(new_stop_sl, 2)
            return _make_action(
                "UPDATE_STOPS", sym, "SHORT",
                updates={
                    "stop_price"            : round(new_stop, 2),
                    "stop_limit_stop_price" : round(new_stop_sl - sls, 2),
                    "stop_limit_limit_price": round(new_stop_sl, 2),
                },
                reason="Breakeven activated (pct SHORT)",
            )
        return _make_action("NONE", sym, "SHORT", reason="Awaiting breakeven (pct)")

    # ── Scenario A-b: percentage trail ────────────────────────────────────
    if stop < entry:
        new_low  = position["min_low"]
        new_stop = new_low * (1 + trail_perc) + 0.05
        new_sl   = new_low * (1 + trail_perc)

        if new_stop < position["stop_price"]:   # trail only moves DOWN for shorts
            if verbose:
                logger.info(
                    "[%s] SHORT pct-trail: new_low=%.4f trail_perc=%.2f%% new_stop=%.4f",
                    sym, new_low, trail_perc * 100, new_stop
                )
            position["stop_price"]       = round(new_stop, 2)
            position["stop_limit_price"] = round(new_sl, 2)
            return _make_action(
                "UPDATE_STOPS", sym, "SHORT",
                updates={
                    "stop_price"            : round(new_stop, 2),
                    "stop_limit_stop_price" : round(new_sl - sls, 2),
                    "stop_limit_limit_price": round(new_sl, 2),
                },
                reason=f"Pct trail: new_low={new_low:.4f}",
            )

    return _make_action("NONE", sym, "SHORT", reason="No exit condition met (pct SHORT)")


# =============================================================================
# TECHNICAL EXIT — delta/gamma on the FAST LINE (MACD line) only   (2026-05-30)
# =============================================================================
# Momentum-reversal exit computed on the fast line f(t) = MACD line
# (EMA_fast − EMA_slow). The SIGNAL / slow line is NOT used.
#     delta(t) = f(t) − f(t-1)            slope of the fast line
#     gamma(t) = delta(t) − delta(t-1)    rate of change of the slope
# A LONG  is exited when the fast line rolls over : delta < 0 AND gamma < 0
#         (slope negative AND accelerating down — a confirmed downturn)
# A SHORT is exited (mirror image)         when   : delta > 0 AND gamma > 0
# tick["macd"] is the engine's MACD history, newest-first (macd[0] = current bar).
#
# (2026-05-30) SIGNED gamma — long needs gamma<0, short needs gamma>0 (a
#   confirmed, accelerating turn — not just the first reversal tick).
# (2026-06-02, updated per user) GENERIC — profit gate REMOVED. The technical
#   exit now fires on a signed delta/gamma reversal whether the position is in
#   profit OR in a loss, so slow bleeders that never reach the stop also get
#   cut on the fast-line turn (the native stop remains the loss-side backstop).
def _tech_exit(position: Dict, tick: Dict, cfg: Dict = None, intrabar: bool = False):
    macd = tick.get("macd") or []
    if len(macd) < 3:
        return None
    direction = position["direction"]

    delta      = macd[0] - macd[1]
    delta_prev = macd[1] - macd[2]
    gamma      = delta - delta_prev
    rolled = ((direction == "LONG"  and delta < 0 and gamma < 0) or
              (direction == "SHORT" and delta > 0 and gamma > 0))
    if not rolled:
        return None
    # 2026-06-05 (per user): until the peak profit reaches 3*MPL, the rollover
    # only exits if it is at least as STEEP as the move at entry, compared on
    # ABSOLUTE slope (the entry slope is in our favour, the rollover slope is
    # against us, so the signs are opposite and only magnitudes are comparable).
    # At/after 3*MPL the rollover exits on its own (delta/gamma alone).
    mpl = float((cfg or {}).get("MPL_dollar_Amount", 0.0) or 0.0) or float(position.get("mpl", 0.0) or 0.0)
    entry = position["entry_price"]
    if direction == "LONG":
        peak = max(position.get("max_high", entry), tick.get("high", entry))
        gain = peak - entry
    else:
        trough = min(position.get("min_low", entry), tick.get("low", entry))
        gain = entry - trough
    if mpl > 0 and gain < 3.0 * mpl:
        entry_slope = abs(float(position.get("entry_slope_abs", 0.0) or 0.0))
        if abs(delta) < entry_slope:
            if not intrabar:
                logger.info("[%s] tech-exit GATED (steep-slope): rollover |delta|=%.6f < "
                            "entry_slope=%.6f  (gain=%.4f < 3*MPL=%.4f) — holding",
                            position.get("symbol", "?"), abs(delta), entry_slope, gain, 3.0 * mpl)
            return None
    return delta, gamma


# =============================================================================
# MAIN PUBLIC FUNCTION
# =============================================================================

def _intrabar_trail(position: Dict, tick: Dict, cfg: Dict) -> Dict:
    """2026-06-11: LIVE-PRICE trail — ratchet the protective stop on the live
    (intra-bar) tick, not only on committed bar closes. Uses the SAME MPL-zone
    math as the 5-bar trail (gain vs entry → new_stop), but driven by the live
    price so the stop follows the move immediately. This is what protects a gain
    when the committed-bar feed is delayed/dropped (the 2026-06-11 SPY case where
    +$1,690 went unprotected because the trail waited on bar-closes that stopped
    arriving). Stop-HIT detection, EoD and the 5-bar history stay on REAL closes
    (the IBKR bracket stop handles an adverse hit), so this only ever RATCHETS the
    stop in the favorable direction — it never widens it or exits."""
    sym       = position["symbol"]
    direction = position["direction"]
    entry     = position["entry_price"]
    mpl       = cfg.get("MPL_dollar_Amount", 0) or 0
    verbose   = cfg.get("AdditionalPrintLogs", "Y").upper() == "Y"
    price     = tick.get("price", tick.get("close"))
    if mpl <= 0 or price is None or entry in (None, 0):
        return _make_action("NONE", sym, direction, reason="intra-bar: live-trail n/a")

    # 2026-08-06: minimum hop. The live trail is now evaluated several times a
    # SECOND, so without this a fast move would emit dozens of 1-cent modifies,
    # flooding the stop-update history and the broker channel. The position's
    # stop is mutated ONLY when a modify is actually sent, so what the bot
    # believes and what rests at the broker can never drift apart.
    min_move = float(cfg.get("LiveTrailMinMove", 0.02) or 0.02)

    if direction == "LONG":
        peak = max(position.get("max_high", price), tick.get("high", price), price)
        position["max_high"] = peak
        gain = peak - entry
        if gain > 0:
            new_stop = (entry - mpl + 0.5 * gain) if gain < 3.0 * mpl else (entry + gain - 2.5 * mpl)
            new_stop = round(new_stop, 2)
            if new_stop >= position["stop_price"] + min_move:   # ratchet UP only
                # 2026-08-06: do NOT claim breakeven until the stop is genuinely
                # at or above entry. The old code flipped be_activated on the
                # first favourable tick, while the ratcheted stop was still
                # BELOW entry — that silently bypassed the configured BE rule
                # (High > Entry + MPL*BEM) on the bar-close path.
                if new_stop >= entry:
                    position["be_activated"] = True
                position["stop_price"]       = new_stop
                position["stop_limit_price"] = new_stop
                if verbose:
                    logger.info("[%s] LONG LIVE-trail: peak=%.4f gain=%.4f new_stop=%.4f",
                                sym, peak, gain, new_stop)
                return _make_action("UPDATE_STOPS", sym, "LONG",
                                    updates={"stop_price": new_stop},
                                    reason=f"LIVE-trail: stop={new_stop:.4f} (gain={gain:.4f}, mpl={mpl:.4f})")
    else:  # SHORT
        trough = min(position.get("min_low", price), tick.get("low", price), price)
        position["min_low"] = trough
        gain = entry - trough
        if gain > 0:
            new_stop = (entry + mpl - 0.5 * gain) if gain < 3.0 * mpl else (entry - gain + 2.5 * mpl)
            new_stop = round(new_stop, 2)
            if new_stop <= position["stop_price"] - min_move:   # ratchet DOWN only
                if new_stop <= entry:                # true breakeven for a short
                    position["be_activated"] = True
                position["stop_price"]       = new_stop
                position["stop_limit_price"] = new_stop
                if verbose:
                    logger.info("[%s] SHORT LIVE-trail: trough=%.4f gain=%.4f new_stop=%.4f",
                                sym, trough, gain, new_stop)
                return _make_action("UPDATE_STOPS", sym, "SHORT",
                                    updates={"stop_price": new_stop},
                                    reason=f"LIVE-trail: stop={new_stop:.4f} (gain={gain:.4f}, mpl={mpl:.4f})")
    return _make_action("NONE", sym, direction, reason="intra-bar: live-trail no ratchet")


def _guard_monotonic_stop(position: Dict, action: Dict, prev_stop) -> Dict:
    """
    2026-08-06: a stop must never move AWAY from the market. Ever.

    Two paths now update the same stop — the tick-driven live trail and the
    bar-close 5-bar / percentage trail. They run at different speeds off
    different inputs, so the bar path can compute a level that is *behind* one
    the live trail already reached (e.g. breakeven activation assigns
    entry + MPL*BEM*0.5 outright, with no ratchet check of its own). Sending
    that would widen a stop that is already protecting a gain.

    This is the single choke point every UPDATE_STOPS passes through: if the new
    stop is worse than the one we came in with, the mutation is rolled back and
    the action is dropped. Nothing downstream can loosen a stop.
    """
    try:
        if action.get("action") != "UPDATE_STOPS" or prev_stop in (None, 0):
            return action
        new_stop = float(action.get("updates", {}).get("stop_price"))
        prev = float(prev_stop)
        long_ = str(position.get("direction", "")).upper() == "LONG"
        loosening = (new_stop < prev) if long_ else (new_stop > prev)
        if not loosening:
            return action
        logger.warning("[%s] STOP LOOSENING BLOCKED: %s tried to move the stop "
                       "%.4f → %.4f (away from the market). Keeping %.4f. Reason "
                       "given: %s", position.get("symbol"),
                       "5-bar/pct" if not action.get("reason", "").startswith("LIVE")
                       else "live trail", prev, new_stop, prev, action.get("reason"))
        position["stop_price"] = prev                 # roll the mutation back
        position["stop_limit_price"] = prev
        return _make_action("NONE", position.get("symbol"),
                            position.get("direction"),
                            reason=f"stop loosening blocked ({new_stop:.4f} < {prev:.4f})"
                                   if long_ else
                                   f"stop loosening blocked ({new_stop:.4f} > {prev:.4f})")
    except (TypeError, ValueError, AttributeError):
        return action                                  # never break the exit path


def process_exit(position: Dict, tick: Dict, intrabar: bool = False) -> Dict:
    """
    Main entry point for exit processing.  Called every minute (5-bar mode)
    or on every tick (percentage mode) for all open positions.

    Parameters
    ──────────
    position : dict  (mutated in-place when stops are updated)
        symbol           – ticker
        direction        – "LONG" or "SHORT"
        entry_price      – price at which the position was entered
        stop_price       – current active stop price
        stop_limit_price – current stop-limit price
        target_price     – original target price (informational)
        be_activated     – bool: has breakeven been triggered?
        max_high         – running max high (used by pct trail for LONG)
        min_low          – running min low  (used by pct trail for SHORT)
        high_history     – list of 1-min bar highs (used by 5-bar LONG)
        low_history      – list of 1-min bar lows  (used by 5-bar SHORT)

    tick : dict
        current_time – datetime
        price        – current price (tick price for pct mode; close for 5-bar)
        high         – bar high
        low          – bar low
        close        – bar close
        prev_high    – previous bar's high (used by 5-bar SHORT)
        prev_low     – previous bar's low  (used by 5-bar LONG)

    Returns
    ───────
    ExitAction dict with keys:
        action     – NONE | UPDATE_STOPS | CLOSE_LIMIT | CLOSE_MARKET | STOP_HIT
        symbol     – ticker
        direction  – LONG or SHORT
        updates    – new stop/limit prices  (populated for UPDATE_STOPS)
        orders     – order prices           (populated for CLOSE_LIMIT)
        reason     – human-readable string
    """

    # ── 1. Re-load config from disk on every call ────────────────────────
    cfg     = load_config()
    sym     = position["symbol"]
    # Apply per-stock overrides (mpl, bem, signal_l, ...) so BE / trail / target
    # math uses the symbol-specific values rather than the global defaults.
    cfg     = _resolve_stock_cfg(sym, cfg)
    verbose = cfg.get("AdditionalPrintLogs", "Y").upper() == "Y"
    direction = position["direction"]
    now       = tick["current_time"].time()

    # ── 2. Scenario C: End-of-Day forced exit ────────────────────────────
    if not intrabar:                              # EoD only on real bar closes
        eod_time = _parse_time(cfg["EoDExitTime"])
        if now >= eod_time:
            logger.info("[%s] EoD exit triggered at %s", sym, now)
            return _make_action(
                "CLOSE_MARKET", sym, direction,
                reason=f"End-of-Day exit at {eod_time}",
            )

    # ── 2b. [REMOVED 2026-07-04, per user] fast-line delta/gamma technical
    #        exit — reverted to the v1.5-spec exit (5-bar / percentage trail
    #        + target). The delta/gamma CLOSE_TECH exit AND the 2026-06-11
    #        intra-bar live-price MPL trail are both disabled below. The
    #        _tech_exit() and _intrabar_trail() functions are left defined but
    #        are no longer called.
    # tech = _tech_exit(position, tick, cfg, intrabar)
    # if tech is not None:
    #     delta, gamma = tech
    #     logger.info("[%s] TECHNICAL exit (%s)%s: delta=%.6f gamma=%.6f",
    #                 sym, direction, " [intra-bar]" if intrabar else "", delta, gamma)
    #     return _make_action(
    #         "CLOSE_TECH", sym, direction,
    #         reason=(f"Fast-line {direction} reversal: delta={delta:.6f} "
    #                 f"gamma={gamma:.6f} (signed gamma{', intra-bar' if intrabar else ''})"),
    #     )
    #
    # if intrabar:
    #     return _intrabar_trail(position, tick, cfg)

    # ── 2c. LIVE (tick-driven) TRAIL — RE-ENABLED 2026-08-06, per user ───────
    # Orders no longer go to a broker: the shadow simulator prices every resting
    # stop against the live tick stream, so the trail must move at that same
    # speed instead of waiting for an exit-candle close. _intrabar_trail()
    # ratchets ONLY in the favourable direction and never exits a position, so
    # the worst it can do is protect a gain sooner. The delta/gamma technical
    # exit above stays disabled — only the trail was asked for.
    # Set LiveTrail=N in config to fall back to bar-close-only trailing.
    if intrabar:
        if str(cfg.get("LiveTrail", "Y") or "Y").strip().upper() != "Y":
            return _make_action("NONE", sym, direction,
                                reason="intra-bar: live trail disabled (LiveTrail=N)")
        _prev_stop = position.get("stop_price")      # capture BEFORE the mutation
        return _guard_monotonic_stop(
            position, _intrabar_trail(position, tick, cfg), _prev_stop)

    # ── 3. Select exit method from config ────────────────────────────────
    # 2026-07-31: cfg here is ALREADY per-stock resolved (step 1), so a
    # "trailing_stop_perc" set on this symbol in Excel overrides the global
    # TrailingStop_Perc. Blank per-stock => the global value is used.
    # str() guards a cell that arrives as a bool/number rather than "Y"/"N".
    use_pct = str(cfg.get("TrailingStop_Perc", "N") or "N").strip().upper() == "Y"

    # v1.5 spec: Condition 1 (5-bar) is evaluated on completed bars only;
    # Condition 2 (percentage) runs at the tick level. So on an intra-bar
    # (live) evaluation, only the percentage method acts — the 5-bar method
    # waits for the bar close.
    if intrabar and not use_pct:
        return _make_action("NONE", sym, direction,
                            reason="intra-bar: 5-bar trail waits for the bar close")

    if verbose:
        logger.info(
            "[%s] process_exit direction=%s method=%s stop=%.4f",
            sym, direction, "PCT" if use_pct else "5BAR",
            position.get("stop_price", 0),
        )

    # ── 4. Dispatch to the correct handler ───────────────────────────────
    # 2026-08-06: capture the stop BEFORE dispatch. Every UPDATE_STOPS returns
    # through _guard_monotonic_stop so no handler — including breakeven
    # activation, which assigns its level outright — can widen a stop the live
    # trail has already tightened.
    prev_stop = position.get("stop_price")
    if direction == "LONG":
        action = (_exit_pct_long(position, tick, cfg) if use_pct
                  else _exit_5bar_long(position, tick, cfg))
    else:  # SHORT
        action = (_exit_pct_short(position, tick, cfg) if use_pct
                  else _exit_5bar_short(position, tick, cfg))
    return _guard_monotonic_stop(position, action, prev_stop)


# =============================================================================
# QUICK SELF-TEST  (run: python exit_logic.py)
# =============================================================================

if __name__ == "__main__":
    from datetime import datetime

    # ── Simulate a LONG position that is in profit ────────────────────────
    pos = {
        "symbol"          : "TEST",
        "direction"       : "LONG",
        "entry_price"     : 100.00,
        "stop_price"      : 99.40,    # entry - MPL*SLM (0.60 * 1)
        "stop_limit_price": 98.80,    # entry - MPL*SLM*2
        "target_price"    : 101.80,   # entry + MPL*TargetMult (0.60*3)
        "be_activated"    : False,
        "max_high"        : 100.00,
        "min_low"         : 100.00,
        "high_history"    : [],
        "low_history"     : [],
    }

    # Tick: price has moved up enough to trigger breakeven
    t = {
        "current_time": datetime.strptime("10:10", "%H:%M"),
        "price"       : 100.70,
        "high"        : 100.75,
        "low"         : 100.30,
        "close"       : 100.65,
        "prev_high"   : 100.50,
        "prev_low"    : 100.10,
    }

    import pprint
    print("\n=== Exit Action (5-bar, LONG) ===")
    action = process_exit(pos, t)
    pprint.pprint(action)
    print(f"Position after: be_activated={pos['be_activated']}  stop={pos['stop_price']}")

    # ── Simulate a trailing tick ──────────────────────────────────────────
    pos["low_history"] = [100.10, 100.20, 100.30, 100.35, 100.40, 100.50]
    t2 = {
        "current_time": datetime.strptime("10:11", "%H:%M"),
        "price"       : 100.90,
        "high"        : 100.95,
        "low"         : 100.60,    # low is higher than prev → triggers trail
        "close"       : 100.85,
        "prev_high"   : 100.75,
        "prev_low"    : 100.50,
    }
    print("\n=== Exit Action (5-bar, trailing tick) ===")
    action2 = process_exit(pos, t2)
    pprint.pprint(action2)
