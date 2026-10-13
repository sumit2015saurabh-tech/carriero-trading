"""
=============================================================================
QTI Fund  |  Model: !JFMX3X_NONCORR_MACD_V3.x
FILE: entry_logic.py

CONDITION SUMMARY
─────────────────────────────────────────────────────────────────────────────

BEFORE MeanStartTime
────────────────────
LONG  (Signal_L == 0  →  crossover mode):
    1. If MACDLE_BLW_ZLine=Y  →  MACDSignal[0] < 0
    2. ZL_MACD > ZL_Signal
    3. MACD[0] < MACDSignal[0]
    4. MACD[0] crosses ABOVE MACDSignal[0] this bar
       i.e. MACD[0] > MACDSignal[0]  AND  MACD[1] <= MACDSignal[1]

LONG  (Signal_L = 1/2/3  →  consecutive candles mode):
    1. If MACDLE_BLW_ZLine=Y  →  MACDSignal[0] < 0
    2. ZL_MACD > ZL_Signal
    3. MACD[0] < MACDSignal[0]
    4. MACD[0] > MACD[Signal_L]           current bar is an up-candle
    5. MACD[1] < MACD[Signal_L + 1]       1 bar ago was a down-candle
    6. MACD[2] < MACD[Signal_L + 2]       2 bars ago was a down-candle

SHORT (Signal_S == 0  →  crossover mode):
    1. If MACDSE_ABV_ZLine=Y  →  MACD[0] > 0 AND MACDSignal[0] > 0
    2. ZL_MACD < ZL_Signal
    3. MACD[0] > MACDSignal[0]
    4. MACD[0] crosses BELOW MACDSignal[0] this bar
       i.e. MACD[0] < MACDSignal[0]  AND  MACD[1] >= MACDSignal[1]

SHORT (Signal_S = 1/2/3  →  consecutive candles mode):
    1. If MACDSE_ABV_ZLine=Y  →  MACDSignal[0] > 0
    2. ZL_MACD < ZL_Signal
    3. MACD[0] > MACDSignal[0]
    4. MACD[0] < MACD[Signal_S]           current bar is a down-candle
    5. MACD[1] > MACD[Signal_S + 1]       1 bar ago was an up-candle
    6. MACD[2] > MACD[Signal_S + 2]       2 bars ago was an up-candle

AFTER MeanStartTime  (all above PLUS one extra condition each)
──────────────────────────────────────────────────────────────
LONG  extra:
    MACDSignal[0] < MACDSignal_Neg_Mean * MeanMultiplier_L
    MACDSignal must be more negative than the session negative mean x multiplier.

SHORT extra:
    MACDSignal[0] > MACDSignal_Pos_Mean * MeanMultiplier_S
    MACDSignal must be more positive than the session positive mean x multiplier.

INDEX CORRELATION  (gate logic lives here; consumed by ibkr_broker.py)
──────────────────────────────────────────────────────────────────────
Per-stock switch: index_filter = Y/N in config.json (Stocks → SYMBOL).
ibkr_broker calls check_index_alignment() AFTER check_entry_signal returns
a non-NONE signal. The index is evaluated using the SAME multi-condition
framework as the stock, with its own per-index parameters:

    Signal_L, Signal_S          (0 → crossover; 1/2/3 → consecutive candles)
    MACDLE_BLW_ZLine            (LONG ZL gate on the index)
    MACDSE_ABV_ZLine            (SHORT ZL gate on the index)

Per-index overrides live in config.json under Indexes → SYMBOL. If absent,
the global MACD_*-paired defaults are used (Signal_L=2, Signal_S=2,
MACDLE_BLW_ZLine='N', MACDSE_ABV_ZLine='N').
=============================================================================
"""

import json
import os
import logging
from datetime import datetime, time as dtime
from typing import Dict, Any, Optional

# ─── Paths ────────────────────────────────────────────────────────────────────
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

# ─── Logger ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ENTRY] %(levelname)s - %(message)s",
)
logger = logging.getLogger("entry_logic")


# =============================================================================
# CONFIG LOADER
# =============================================================================

_CFG_CACHE: Dict[str, Any] = None
_CFG_MTIME: float = -1.0

def load_config() -> Dict[str, Any]:
    """Return config.json, re-reading from disk ONLY when the file changes.
    excel_sync rewrites config.json on every Excel edit, which bumps the mtime,
    so live edits still take effect instantly — but the hot path (every bar/tick)
    now does a cheap stat() instead of an open()+json.parse() of the whole file.
    This keeps heavy I/O off the IB reader thread."""
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


def _resolve_stock_cfg(symbol: str, cfg: Dict) -> Dict:
    """
    Merge per-stock + per-portfolio overrides into a copy of the global config.

    Inheritance chain:
        per-stock field  →  per-portfolio default  →  global default

    Per-stock fields in config.json (under Stocks → SYMBOL):
        mpl               → overrides MPL_dollar_Amount
        bem               → overrides BEMultiplier
        signal_l          → overrides Signal_L
        signal_s          → overrides Signal_S
        mean_multiplier_l → overrides MeanMultiplier_L
        mean_multiplier_s → overrides MeanMultiplier_S
        macdle_blw_zline  → overrides MACDLE_BLW_ZLine
        macdse_abv_zline  → overrides MACDSE_ABV_ZLine
        trailing_perc     → overrides TrailingPerc
        trailing_stop_perc→ overrides TrailingStop_Perc (Y=pct trail, N=5-bar)
        portfolio         → assigns stock to a portfolio (defaults fall-through)

    If a per-stock field is absent or None, the assigned portfolio's value
    is used. If neither has it, the global default applies.
    """
    merged = dict(cfg)  # shallow copy of global config
    stock_entry = cfg.get("Stocks", {}).get(symbol, {})

    # Look up portfolio defaults (if assigned)
    portfolios = cfg.get("Portfolios", {}) or {}
    pname = str(stock_entry.get("portfolio", "") or "").strip()
    portfolio_entry = portfolios.get(pname, {}) if pname else {}

    _OVERRIDES = {
        "mpl":               "MPL_dollar_Amount",
        "bem":               "BEMultiplier",        # per-stock BE multiplier override
        "signal_l":          "Signal_L",
        "signal_s":          "Signal_S",
        "mean_multiplier_l": "MeanMultiplier_L",
        "mean_multiplier_s": "MeanMultiplier_S",
        "diff_multiplier_l": "DiffMultiplier_L",    # NEW: multiplier on MACD-Differential mean (LONG)
        "diff_multiplier_s": "DiffMultiplier_S",    # NEW: multiplier on MACD-Differential mean (SHORT)
        "macdle_blw_zline":  "MACDLE_BLW_ZLine",     # legacy (unused after 2026-06-02)
        "macdse_abv_zline":  "MACDSE_ABV_ZLine",     # legacy (unused after 2026-06-02)
        "macd_long_line":    "MACD_LONG_LINE",       # 2026-06-02: zero-line gate for LONG (1/2/3)
        "macd_short_line":   "MACD_SHORT_LINE",      # 2026-06-02: zero-line gate for SHORT (1/2/3)
        "trailing_perc":     "TrailingPerc",        # per-stock pct-trail override
        # 2026-07-31: per-stock choice of WHICH trail method runs (Y=percentage,
        # N=5-bar). Blank in Excel => inherits the global TrailingStop_Perc.
        "trailing_stop_perc": "TrailingStop_Perc",
        # 2026-05-22: per-stock mean-time overrides. Same setting governs
        # BOTH MACD and MACD-Diff means (single time per equity, per spec).
        "mean_calc_start_time": "MeanCalcStartTime",
        "mean_start_time":      "MeanStartTime",
    }

    for local_key, global_key in _OVERRIDES.items():
        # Stock-level wins
        val = stock_entry.get(local_key)
        if val is not None and val != "":
            merged[global_key] = val
            continue
        # Portfolio fallback
        pval = portfolio_entry.get(local_key)
        if pval is not None and pval != "":
            merged[global_key] = pval

    # Per-stock quantity overrides PositionSize (no portfolio level for qty
    # — position size is intentionally per-symbol, since portfolio sizing
    # would couple together symbols with different price levels).
    qty = stock_entry.get("quantity")
    if qty is not None:
        merged["PositionSize"] = qty

    return merged


def _resolve_index_cfg(index_symbol: str, cfg: Dict) -> Dict:
    """
    Build a merged cfg for an index using per-index overrides under
    cfg["Indexes"][SYMBOL]. The keys mirror the per-stock override keys
    so the same multi-condition framework can be reused on the index's
    own MACD history.

    Per-index fields in config.json (under Indexes → SYMBOL):
        signal_l           → overrides Signal_L
        signal_s           → overrides Signal_S
        macdle_blw_zline   → overrides MACDLE_BLW_ZLine
        macdse_abv_zline   → overrides MACDSE_ABV_ZLine
        mean_multiplier_l  → overrides MeanMultiplier_L
        mean_multiplier_s  → overrides MeanMultiplier_S

    Mirrors the per-stock signal-evaluation parameter set so the index
    gate runs the same multi-condition framework on the index's own MACD.

    If a per-index field is absent or None, the conservative defaults below
    are applied (NOT the stock's globals — indexes get their own defaults
    so toggling a stock's params does not silently change index gating).
    """
    merged = dict(cfg)
    idx_entry = (cfg.get("Indexes") or {}).get(index_symbol.upper(), {}) or {}

    # Force index-specific defaults — overwrite any inherited stock globals so
    # the index gate is governed only by per-index settings (or these defaults).
    merged["Signal_L"]          = 2
    merged["Signal_S"]          = 2
    merged["MACDLE_BLW_ZLine"]  = "N"
    merged["MACDSE_ABV_ZLine"]  = "N"
    merged["MeanMultiplier_L"]  = 1.0
    merged["MeanMultiplier_S"]  = 1.0

    _OVERRIDES = {
        "signal_l":           "Signal_L",
        "signal_s":           "Signal_S",
        "macdle_blw_zline":   "MACDLE_BLW_ZLine",
        "macdse_abv_zline":   "MACDSE_ABV_ZLine",
        "mean_multiplier_l":  "MeanMultiplier_L",
        "mean_multiplier_s":  "MeanMultiplier_S",
        # 2026-05-22: per-index diff-multiplier overrides
        "diff_multiplier_l":  "DiffMultiplier_L",
        "diff_multiplier_s":  "DiffMultiplier_S",
        # 2026-05-22: per-index mean-time overrides
        "mean_calc_start_time": "MeanCalcStartTime",
        "mean_start_time":      "MeanStartTime",
    }
    for local_key, global_key in _OVERRIDES.items():
        val = idx_entry.get(local_key)
        if val is not None and val != "":
            merged[global_key] = val

    return merged


def _parse_time(t_str: str) -> dtime:
    return datetime.strptime(t_str, "%H:%M").time()


# =============================================================================
# HELPER - build bracket order prices
# =============================================================================

def _build_entry_orders(direction: str, close: float, cfg: Dict) -> Dict:
    mpl  = float(cfg["MPL_dollar_Amount"])
    slm  = float(cfg["SLMultiplier"])
    tgt  = float(cfg["TargetMultiplier"])
    sls  = float(cfg["StopLimitSpread"])
    qty  = int(cfg["PositionSize"])
    acct = cfg["iAccount"]

    if direction == "LONG":
        parent_limit     = close
        stop_price       = close - mpl * slm
        stop_limit_stop  = close - (mpl * slm * 2)
        stop_limit_limit = close - (mpl * slm * 2) - sls
        target_limit     = close + (mpl * tgt)
    else:  # SHORT
        parent_limit     = close
        stop_price       = close + mpl * slm
        stop_limit_stop  = close + (mpl * slm * 2)
        stop_limit_limit = close + (mpl * slm * 2) + sls
        target_limit     = close - (mpl * tgt)

    return {
        "account"   : acct,
        "direction" : direction,
        "quantity"  : qty,
        "parent_order": {
            "type"        : "LIMIT",
            "limit_price" : round(parent_limit, 4),
            "lifespan_min": 1,
        },
        "child_stop_order": {
            "type"      : "STOP",
            "stop_price": round(stop_price, 4),
            "lifespan"  : "DAY",
        },
        "child_stop_limit_order": {
            "type"       : "STOP_LIMIT",
            "stop_price" : round(stop_limit_stop, 4),
            "limit_price": round(stop_limit_limit, 4),
            "lifespan"   : "DAY",
        },
        "child_target_limit_order": {
            "type"       : "LIMIT",
            "limit_price": round(target_limit, 4),
            "lifespan"   : "DAY",
        },
    }


# =============================================================================
# LEGACY ENTRY LOGIC (preserved as string literal — kept for reference).
# Replaced 2026-05-18 by the cross-based logic below. The old rules
# (Signal_L/Signal_S consecutive candles + zero-line gates + after-mean
# multiplier + 3-condition index alignment) are inside _LEGACY_ENTRY_LOGIC.
# To restore the old behavior: extract the string, paste it back into the
# module, and re-wire check_entry_signal / check_index_alignment.
# =============================================================================

_LEGACY_ENTRY_LOGIC = r'''
# =============================================================================
# LONG - BEFORE MeanStartTime
# =============================================================================

def _long_before_mean(bar: Dict, cfg: Dict) -> tuple:
    """Returns (passes: bool, reason: str)."""
    macd        = bar["macd"]
    macd_signal = bar["macd_signal"]
    macd_zl     = bar["macd_zl"]
    macd_zl_sig = bar["macd_zl_signal"]
    signal_l    = int(cfg["Signal_L"])
    zline_check = str(cfg.get("MACDLE_BLW_ZLine", "Y")).upper()

    # 1. Zero-line gate (optional)
    if zline_check == "Y" and macd_signal[0] >= 0:
        return False, (
            f"LONG BLOCKED: MACDSignal[0]={macd_signal[0]:.5f} must be < 0"
            f" (MACDLE_BLW_ZLine=Y)"
        )

    # 2. ZL direction must favour LONG
    if macd_zl <= macd_zl_sig:
        return False, (
            f"LONG BLOCKED: ZL_MACD({macd_zl:.5f}) must be"
            f" > ZL_Signal({macd_zl_sig:.5f})"
        )

    # 3. Crossover mode — skip the "MACD < Signal" prerequisite because
    #    the crossover check in step 4a already validates the direction.
    if signal_l == 0:
        crossed = (macd[0] > macd_signal[0]) and (macd[1] <= macd_signal[1])
        if crossed:
            return True, "LONG OK (crossover): MACD crossed above Signal"
        return False, "LONG BLOCKED (crossover): no crossover this bar"

    # 4b. Consecutive candles mode — MACD must be below Signal
    if macd[0] >= macd_signal[0]:
        return False, (
            f"LONG BLOCKED: MACD[0]({macd[0]:.5f}) must be"
            f" < MACDSignal[0]({macd_signal[0]:.5f})"
        )

    if signal_l > 3:
        logger.warning("[ENTRY] Signal_L=%d clamped to 3", signal_l)
        signal_l = 3

    needed = signal_l + 3
    if len(macd) < needed:
        return False, (
            f"LONG BLOCKED: need {needed} MACD bars, have {len(macd)}"
        )

    up_now = macd[0] > macd[signal_l]
    dn_1   = macd[1] < macd[signal_l + 1]
    dn_2   = macd[2] < macd[signal_l + 2]

    if up_now and dn_1 and dn_2:
        return True, (
            f"LONG OK (Signal_L={signal_l}): "
            f"MACD[0]({macd[0]:.5f})>MACD[{signal_l}]({macd[signal_l]:.5f}) "
            f"MACD[1]({macd[1]:.5f})<MACD[{signal_l+1}]({macd[signal_l+1]:.5f}) "
            f"MACD[2]({macd[2]:.5f})<MACD[{signal_l+2}]({macd[signal_l+2]:.5f})"
        )
    return False, (
        f"LONG BLOCKED (Signal_L={signal_l}):"
        f" up_now={up_now} dn_1={dn_1} dn_2={dn_2}"
    )


# =============================================================================
# LONG - AFTER MeanStartTime
# =============================================================================

def _long_after_mean(bar: Dict, cfg: Dict) -> tuple:
    """
    All _long_before_mean conditions PLUS:
        MACDSignal[0] < MACDSignal_Neg_Mean * MeanMultiplier_L

    MACDSignal_Neg_Mean is the session average of MACDSignal when it was
    below zero - always a negative number (e.g. -0.10).
    MeanMultiplier_L multiplies that negative number making it more negative
    (e.g. -0.10 * 1.5 = -0.15).
    MACDSignal[0] must be below (more negative than) that threshold.
    """
    passes, reason = _long_before_mean(bar, cfg)
    if not passes:
        return False, reason

    neg_mean  = float(bar["macd_signal_neg_mean"])
    mult_l    = float(cfg.get("MeanMultiplier_L", cfg.get("MeanMultiplier", 1.0)))
    macd_sig  = float(bar["macd_signal"][0])
    threshold = neg_mean * mult_l

    if macd_sig < threshold:
        return True, (
            f"LONG OK (after-mean): MACDSig({macd_sig:.5f})"
            f" < NegMean({neg_mean:.5f}) x MeanMult_L({mult_l})"
            f" = {threshold:.5f}"
        )
    return False, (
        f"LONG BLOCKED (after-mean): MACDSig({macd_sig:.5f})"
        f" not < NegMean({neg_mean:.5f}) x MeanMult_L({mult_l})"
        f" = {threshold:.5f}"
    )


# =============================================================================
# SHORT - BEFORE MeanStartTime
# =============================================================================

def _short_before_mean(bar: Dict, cfg:Dict) -> tuple:
    """Returns (passes: bool, reason: str)."""
    macd        = bar["macd"]
    macd_signal = bar["macd_signal"]
    macd_zl     = bar["macd_zl"]
    macd_zl_sig = bar["macd_zl_signal"]
    signal_s    = int(cfg["Signal_S"])
    zline_check = str(cfg.get("MACDSE_ABV_ZLine", "Y")).upper()

    # 1. Zero-line gate (optional)
    if zline_check == "Y":
        if signal_s == 0:
            if not (macd[0] > 0 and macd_signal[0] > 0):
                return False, (
                    f"SHORT BLOCKED: crossover mode needs MACD[0]({macd[0]:.5f}) > 0"
                    f" AND MACDSignal[0]({macd_signal[0]:.5f}) > 0"
                    f"(MACDSE_ABV_ZLine=Y)"
                )
        else:
            if macd_signal[0] <= 0:
                return False, (
                    f"SHORT BLOCKED: MACDSignal[0]({macd_signal[0]:.5f})"
                    f" must be > 0 (MACDSE_ABV_ZLine=Y)"
                )

    # 2. ZL direction must favour SHORT
    if macd_zl >= macd_zl_sig:
        return False, (
            f"SHORT BLOCKED: ZL_MACD({macd_zl:.5f}) must be"
            f" < ZL_Signal({macd_zl_sig:.5f})"
        )

    # 3. Crossover mode — skip the "MACD > Signal" prerequisite because
    #    the crossover check in step 4a already validates the direction.
    if signal_s == 0:
        crossed = (macd[0] < macd_signal[0]) and (macd[1] >= macd_signal[1])
        if crossed:
            return True, "SHORT OK (crossover): MACD crossed below Signal"
        return False, "SHORT BLOCKED (crossover): no crossover this bar"

    # 4b. Consecutive candles mode — MACD must be above Signal
    if macd[0] <= macd_signal[0]:
        return False, (
            f"SHORT BLOCKED: MACD[0]({macd[0]:.5f}) must be"
            f" > MACDSignal[0]({macd_signal[0]:.5f})"
        )

    if signal_s > 3:
        logger.warning("[ENTRY] Signal_S=%d clamped to 3", signal_s)
        signal_s = 3

    needed = signal_s + 3
    if len(macd) < needed:
        return False, (
            f"SHORT BLOCKED: need {needed} MACD bars, have {len(macd)}"
        )

    dn_now = macd[0] < macd[signal_s]
    up_1   = macd[1] > macd[signal_s + 1]
    up_2   = macd[2] > macd[signal_s + 2]

    if dn_now and up_1 and up_2:
        return True, (
            f"SHORT OK (Signal_S={signal_s}): "
            f"MACD[0]({macd[0]:.5f})<MACD[{signal_s}]({macd[signal_s]:.5f}) "
            f"MACD[1]({macd[1]:.5f})>MACD[{signal_s+1}]({macd[signal_s+1]:.5f}) "
            f"MACD[2]({macd[2]:.5f})>MACD[{signal_s+2}]({macd[signal_s+2]:.5f})"
        )
    return False, (
        f"SHORT BLOCKED (Signal_S={signal_s}):"
        f" dn_now={dn_now} up_1={up_1} up_2={up_2}"
    )


# =============================================================================
# SHORT - AFTER MeanStartTime
# =============================================================================

def _short_after_mean(bar: Dict, cfg: Dict) -> tuple:
    """
    All _short_before_mean conditions PLUS:
        MACDSignal[0] > MACDSignal_Pos_Mean * MeanMultiplier_S

    MACDSignal_Pos_Mean is the session average of MACDSignal when it was
    above zero - always a positive number (e.g. 0.10).
    MeanMultiplier_S multiplies it (e.g. 0.10 * 1.5 = 0.15).
    MACDSignal[0] must be above that threshold.
    """
    passes, reason = _short_before_mean(bar, cfg)
    if not passes:
        return False, reason

    pos_mean  = float(bar["macd_signal_pos_mean"])
    mult_s    = float(cfg.get("MeanMultiplier_S", cfg.get("MeanMultiplier", 1.0)))
    macd_sig  = float(bar["macd_signal"][0])
    threshold = pos_mean * mult_s

    if macd_sig > threshold:
        return True, (
            f"SHORT OK (after-mean): MACDSig({macd_sig:.5f})"
            f" > PosMean({pos_mean:.5f}) x MeanMult_S({mult_s})"
            f" = {threshold:.5f}"
        )
    return False, (
        f"SHORT BLOCKED (after-mean): MACDSig({macd_sig:.5f})"
        f" not > PosMean({pos_mean:.5f}) x MeanMult_S({mult_s})"
        f" = {threshold:.5f}"
    )


# =============================================================================
# INDEX ALIGNMENT — multi-condition framework on the index's own MACD
# =============================================================================

def _idx_long_check(idx_fields: Dict, idx_cfg: Dict) -> tuple:
    """
    LONG-side index alignment using the SAME rule set as a stock.
    Returns (passes: bool, reason: str).
    """
    macd        = idx_fields["macd"]
    macd_signal = idx_fields["macd_signal"]
    macd_zl     = idx_fields["macd_zl"]
    macd_zl_sig = idx_fields["macd_zl_signal"]
    signal_l    = int(idx_cfg["Signal_L"])
    zline_check = str(idx_cfg.get("MACDLE_BLW_ZLine", "N")).upper()

    # 1. Optional zero-line gate on the index's MACD signal
    if zline_check == "Y" and macd_signal[0] >= 0:
        return False, (
            f"index LONG BLOCKED: MACDSignal[0]={macd_signal[0]:.5f} must be < 0"
            f" (MACDLE_BLW_ZLine=Y)"
        )

    # 2. ZL alignment must favour LONG
    if macd_zl <= macd_zl_sig:
        return False, (
            f"index LONG BLOCKED: ZL_MACD({macd_zl:.5f}) must be"
            f" > ZL_Signal({macd_zl_sig:.5f})"
        )

    # 3. Crossover mode
    if signal_l == 0:
        crossed = (macd[0] > macd_signal[0]) and (macd[1] <= macd_signal[1])
        if crossed:
            return True, "index LONG OK (crossover): MACD crossed above Signal"
        return False, "index LONG BLOCKED (crossover): no crossover this bar"

    # 4. Consecutive candles: still below signal
    if macd[0] >= macd_signal[0]:
        return False, (
            f"index LONG BLOCKED: MACD[0]({macd[0]:.5f}) must be"
            f" < MACDSignal[0]({macd_signal[0]:.5f})"
        )

    if signal_l > 3:
        signal_l = 3
    needed = signal_l + 3
    if len(macd) < needed:
        return False, f"index LONG BLOCKED: need {needed} MACD bars, have {len(macd)}"

    up_now = macd[0] > macd[signal_l]
    dn_1   = macd[1] < macd[signal_l + 1]
    dn_2   = macd[2] < macd[signal_l + 2]

    if up_now and dn_1 and dn_2:
        return True, (
            f"index LONG OK (Signal_L={signal_l}): "
            f"MACD[0]({macd[0]:.5f})>MACD[{signal_l}]({macd[signal_l]:.5f}) "
            f"MACD[1]({macd[1]:.5f})<MACD[{signal_l+1}]({macd[signal_l+1]:.5f}) "
            f"MACD[2]({macd[2]:.5f})<MACD[{signal_l+2}]({macd[signal_l+2]:.5f})"
        )
    return False, (
        f"index LONG BLOCKED (Signal_L={signal_l}):"
        f" up_now={up_now} dn_1={dn_1} dn_2={dn_2}"
    )


def _idx_short_check(idx_fields: Dict, idx_cfg: Dict) -> tuple:
    """
    SHORT-side index alignment — mirror of _idx_long_check.
    """
    macd        = idx_fields["macd"]
    macd_signal = idx_fields["macd_signal"]
    macd_zl     = idx_fields["macd_zl"]
    macd_zl_sig = idx_fields["macd_zl_signal"]
    signal_s    = int(idx_cfg["Signal_S"])
    zline_check = str(idx_cfg.get("MACDSE_ABV_ZLine", "N")).upper()

    # 1. Optional zero-line gate
    if zline_check == "Y":
        if signal_s == 0:
            if not (macd[0] > 0 and macd_signal[0] > 0):
                return False, (
                    f"index SHORT BLOCKED: crossover mode needs MACD[0]({macd[0]:.5f}) > 0"
                    f" AND MACDSignal[0]({macd_signal[0]:.5f}) > 0"
                    f" (MACDSE_ABV_ZLine=Y)"
                )
        else:
            if macd_signal[0] <= 0:
                return False, (
                    f"index SHORT BLOCKED: MACDSignal[0]({macd_signal[0]:.5f})"
                    f" must be > 0 (MACDSE_ABV_ZLine=Y)"
                )

    # 2. ZL alignment must favour SHORT
    if macd_zl >= macd_zl_sig:
        return False, (
            f"index SHORT BLOCKED: ZL_MACD({macd_zl:.5f}) must be"
            f" < ZL_Signal({macd_zl_sig:.5f})"
        )

    # 3. Crossover mode
    if signal_s == 0:
        crossed = (macd[0] < macd_signal[0]) and (macd[1] >= macd_signal[1])
        if crossed:
            return True, "index SHORT OK (crossover): MACD crossed below Signal"
        return False, "index SHORT BLOCKED (crossover): no crossover this bar"

    # 4. Consecutive candles: still above signal
    if macd[0] <= macd_signal[0]:
        return False, (
            f"index SHORT BLOCKED: MACD[0]({macd[0]:.5f}) must be"
            f" > MACDSignal[0]({macd_signal[0]:.5f})"
        )

    if signal_s > 3:
        signal_s = 3
    needed = signal_s + 3
    if len(macd) < needed:
        return False, f"index SHORT BLOCKED: need {needed} MACD bars, have {len(macd)}"

    dn_now = macd[0] < macd[signal_s]
    up_1   = macd[1] > macd[signal_s + 1]
    up_2   = macd[2] > macd[signal_s + 2]

    if dn_now and up_1 and up_2:
        return True, (
            f"index SHORT OK (Signal_S={signal_s}): "
            f"MACD[0]({macd[0]:.5f})<MACD[{signal_s}]({macd[signal_s]:.5f}) "
            f"MACD[1]({macd[1]:.5f})>MACD[{signal_s+1}]({macd[signal_s+1]:.5f}) "
            f"MACD[2]({macd[2]:.5f})>MACD[{signal_s+2}]({macd[signal_s+2]:.5f})"
        )
    return False, (
        f"index SHORT BLOCKED (Signal_S={signal_s}):"
        f" dn_now={dn_now} up_1={up_1} up_2={up_2}"
    )


def _idx_long_check_after_mean(idx_fields: Dict, idx_cfg: Dict) -> tuple:
    """
    LONG-side index alignment AFTER MeanStartTime.

    Mirrors _long_after_mean: all _idx_long_check conditions PLUS an
    additional threshold check on the index's own MACDSignal vs its
    session negative mean.
    """
    passes, reason = _idx_long_check(idx_fields, idx_cfg)
    if not passes:
        return False, reason

    neg_mean = idx_fields.get("macd_signal_neg_mean")
    if neg_mean is None:
        return False, "index LONG BLOCKED (after-mean): neg_mean not yet available"

    mult_l    = float(idx_cfg.get("MeanMultiplier_L", 1.0))
    macd_sig  = float(idx_fields["macd_signal"][0])
    threshold = float(neg_mean) * mult_l

    if macd_sig < threshold:
        return True, (
            f"index LONG OK (after-mean): MACDSig({macd_sig:.5f})"
            f" < NegMean({float(neg_mean):.5f}) x MeanMult_L({mult_l})"
            f" = {threshold:.5f}"
        )
    return False, (
        f"index LONG BLOCKED (after-mean): MACDSig({macd_sig:.5f})"
        f" not < NegMean({float(neg_mean):.5f}) x MeanMult_L({mult_l})"
        f" = {threshold:.5f}"
    )


def _idx_short_check_after_mean(idx_fields: Dict, idx_cfg: Dict) -> tuple:
    """
    SHORT-side index alignment AFTER MeanStartTime.
    Mirrors _short_after_mean.
    """
    passes, reason = _idx_short_check(idx_fields, idx_cfg)
    if not passes:
        return False, reason

    pos_mean = idx_fields.get("macd_signal_pos_mean")
    if pos_mean is None:
        return False, "index SHORT BLOCKED (after-mean): pos_mean not yet available"

    mult_s    = float(idx_cfg.get("MeanMultiplier_S", 1.0))
    macd_sig  = float(idx_fields["macd_signal"][0])
    threshold = float(pos_mean) * mult_s

    if macd_sig > threshold:
        return True, (
            f"index SHORT OK (after-mean): MACDSig({macd_sig:.5f})"
            f" > PosMean({float(pos_mean):.5f}) x MeanMult_S({mult_s})"
            f" = {threshold:.5f}"
        )
    return False, (
        f"index SHORT BLOCKED (after-mean): MACDSig({macd_sig:.5f})"
        f" not > PosMean({float(pos_mean):.5f}) x MeanMult_S({mult_s})"
        f" = {threshold:.5f}"
    )


def check_index_alignment(index_symbol: str, direction: str,
                           idx_fields: Optional[Dict],
                           bar_time: Optional[Any] = None) -> tuple:
    """
    Public entry point — called from check_entry_signal after a stock signal
    fires and that stock has index_filter='Y'.

    Mirrors the stock pipeline: BEFORE MeanStartTime runs the basic
    multi-condition gate; AFTER MeanStartTime adds the MeanMultiplier
    threshold check on the index's MACDSignal vs its session pos/neg mean.

    Parameters
    ----------
    index_symbol : str
        e.g. "QQQ" — used to look up per-index overrides in config.json.
    direction : "LONG" | "SHORT"
        Stock's signal direction; the index must align with this.
    idx_fields : dict
        IndexMACDStore.get(<idx>) payload. None -> blocked.
    bar_time : datetime.time | datetime | None
        The bar's current_time, used to determine before/after MeanStartTime.
        If None, the BEFORE branch is used (conservative fallback).

    Returns
    -------
    (passes: bool, reason: str)
    """
    if idx_fields is None:
        return False, f"{index_symbol} MACD not ready yet — blocking signal"

    cfg     = load_config()
    idx_cfg = _resolve_index_cfg(index_symbol, cfg)

    # Determine before/after MeanStartTime, mirroring stock check_entry_signal
    after_mean = False
    if bar_time is not None:
        try:
            mean_start = _parse_time(cfg["MeanStartTime"])
            t = bar_time.time() if hasattr(bar_time, "time") else bar_time
            after_mean = t >= mean_start
        except Exception:
            after_mean = False

    if direction == "LONG":
        if after_mean:
            return _idx_long_check_after_mean(idx_fields, idx_cfg)
        return _idx_long_check(idx_fields, idx_cfg)
    elif direction == "SHORT":
        if after_mean:
            return _idx_short_check_after_mean(idx_fields, idx_cfg)
        return _idx_short_check(idx_fields, idx_cfg)
    else:
        return False, f"index alignment: unknown direction {direction!r}"


# =============================================================================
# MAIN PUBLIC FUNCTION
# =============================================================================

def check_entry_signal(bar: Dict, open_positions: Optional[Dict] = None) -> Dict:
    """
    Evaluate MACD entry conditions for a completed bar. If a stock has
    index_filter='Y', the named index is also evaluated using the SAME
    multi-condition framework (see check_index_alignment) and must agree
    with the stock's signal direction; otherwise the signal is suppressed.

    Parameters
    ----------
    bar : dict
        symbol, close, current_time, macd (list), macd_signal (list),
        macd_zl, macd_zl_signal, macd_signal_pos_mean, macd_signal_neg_mean

    open_positions : dict {symbol: True}
        Prevents double entry into the same stock.

    Returns
    -------
    dict with keys: signal, symbol, orders, reason
    """
    raw_cfg = load_config()
    symbol  = bar.get("symbol", "UNKNOWN")
    cfg     = _resolve_stock_cfg(symbol, raw_cfg) # merge per-stock overrides
    verbose = str(cfg.get("AdditionalPrintLogs", "Y")).upper() == "Y"

    result = {"signal": "NONE", "symbol": symbol, "orders": {}, "reason": ""}

    # ExecutionType gate
    exec_type = int(cfg["ExecutionType"])
    if exec_type == 4:
        result["reason"] = "ExecutionType=4: all trading disabled"
        return result

    auto_manual = str(cfg.get("Auto_Manual", "A")).upper()

    # Trading hours gate
    now         = bar["current_time"].time()
    start_trade = _parse_time(cfg["TimeToStartTrading"])
    stop_trade  = _parse_time(cfg["TimeToStopTrading"])
    mean_start  = _parse_time(cfg["MeanStartTime"])

    if not (start_trade <= now <= stop_trade):
        result["reason"] = f"Outside trading hours ({start_trade}-{stop_trade})"
        return result

    # Duplicate position gate
    if open_positions and open_positions.get(symbol):
        result["reason"] = f"{symbol}: position already open"
        return result

    before_mean = now < mean_start

    if verbose:
        logger.info("[%s] Evaluating  time=%s  before_mean=%s  exec=%d"
                    "  Signal_L=%s  Signal_S=%s  ZL_gate=%s/%s  MPL=%.2f",
                    symbol, now, before_mean, exec_type,
                    cfg["Signal_L"], cfg["Signal_S"],
                    cfg.get("MACDLE_BLW_ZLine", "N"), cfg.get("MACDSE_ABV_ZLine", "N"),
                    float(cfg["MPL_dollar_Amount"]))

    # Evaluate LONG
    long_ok, long_reason = False, "Not evaluated"
    if exec_type in (2, 3):
        if before_mean:
            long_ok, long_reason = _long_before_mean(bar, cfg)
        else:
            long_ok, long_reason = _long_after_mean(bar, cfg)

    # Evaluate SHORT
    short_ok, short_reason = False, "Not evaluated"
    if exec_type in (1, 3):
        if before_mean:
            short_ok, short_reason = _short_before_mean(bar, cfg)
        else:
            short_ok, short_reason = _short_after_mean(bar, cfg)

    # Determine direction (LONG preferred if both fire simultaneously)
    if long_ok:
        direction   = "LONG"
        macd_reason = long_reason
    elif short_ok:
        direction   = "SHORT"
        macd_reason = short_reason
    else:
        result["reason"] = (
            f"No signal | LONG: {long_reason} | SHORT: {short_reason}"
        )
        if verbose:
            logger.info("[%s] No signal", symbol)
        return result

    # Index alignment gate (per-stock switch)
    # The per-stock entry has index_filter='Y'/'N' and index='SPY'/'QQQ'/'SMH'.
    # When 'Y', the index's own MACD must pass the same multi-condition
    # framework as the stock (using per-index Signal_L/S + ZL gates).
    stock_entry = (raw_cfg.get("Stocks") or {}).get(symbol, {}) or {}
    use_idx     = str(stock_entry.get("index_filter", "N")).strip().upper() == "Y"
    idx_name    = str(stock_entry.get("index", "") or "").strip().upper()
    if use_idx:
        if not idx_name:
            result["reason"] = (
                f"{symbol}: index_filter=Y but no index assigned — blocking signal"
            )
            logger.warning("[%s] %s", symbol, result["reason"])
            return result
        from indicators import IndexMACDStore                  # lazy: avoid circular
        idx_fields = IndexMACDStore.get(idx_name)
        idx_pass, idx_reason = check_index_alignment(
            idx_name, direction, idx_fields, bar.get("current_time"),
        )
        if not idx_pass:
            result["reason"] = f"{symbol} signal blocked by {idx_name}: {idx_reason}"
            logger.info("[%s] Signal BLOCKED by index filter — %s", symbol, idx_reason)
            return result
        if verbose:
            logger.info("[%s] Index gate PASSED (%s) — %s", symbol, idx_name, idx_reason)

    # Build orders
    orders = _build_entry_orders(direction, bar["close"], cfg)
    orders["send_to_market"] = (auto_manual == "A")

    result.update({
        "signal"    : direction,
        "orders"    : orders,
        "reason"    : macd_reason,
        "auto_mode" : (auto_manual == "A"),
    })

    logger.info("[%s] *** %s SIGNAL ***  close=%.4f  auto=%s  before_mean=%s",
                symbol, direction, bar["close"], auto_manual == "A", before_mean)
    return result

'''  # ── end of _LEGACY_ENTRY_LOGIC ──────────────────────────────────────


# =============================================================================
# NEW CROSS-BASED ENTRY LOGIC (replaces the legacy logic above).
#
# Trigger: MACD line crossing zero (= Fast EMA crossing Slow EMA = MACD
# Differential changing sign). Per user spec (2026-05-18):
#   - LONG  on negative → positive cross
#   - SHORT on positive → negative cross
#   - One entry per bar maximum (intra-bar wobble can't cause repeat fires)
#   - No mean filter, no zero-line gate, no consecutive-candle pattern
#   - Index filter simplified: index's Fast > Slow for LONG, Fast < Slow
#     for SHORT (no ZL gates, no patterns)
#   - Position-open guard preserved (one position per symbol at a time)
#
# The cross event itself is detected by IndicatorPipeline (intra-bar via
# _check_intrabar_cross, at bar close via _on_second_bar_closed). This
# function just consumes the `_cross_event` field and applies gates.
# =============================================================================

def check_index_alignment(index_symbol: str, direction: str,
                           idx_fields: Optional[Dict],
                           bar_time: Optional[Any] = None,
                           max_age_sec: Optional[float] = None) -> tuple:
    """
    Simplified index filter — Fast EMA vs Slow EMA (= MACD line vs 0).
        LONG  requires index MACD line > 0  (Fast > Slow)
        SHORT requires index MACD line < 0  (Fast < Slow)
    Blocks immediately if the index MACD is not ready yet.

    2026-06-11: fail-closed FRESHNESS — when max_age_sec is given (~2x the index
    resolution), REJECT if the index value is stale (no committed index bar within
    that window). The freshness clock advances only on COMMITTED bars, so a frozen
    feed OR a just-recovered/unsettled value (the bad QQQ +0.09 that contradicted
    the settled -0.79) cannot pass the gate.
    """
    if idx_fields is None:
        return False, f"{index_symbol} MACD not ready yet — blocking signal"

    if max_age_sec is not None:
        try:
            from indicators import IndexMACDStore
            age = IndexMACDStore.get_age_sec(index_symbol)
        except Exception:
            age = None
        if age is None:
            return False, f"{index_symbol} MACD freshness unknown — blocking signal"
        if age > max_age_sec:
            return False, (f"{index_symbol} MACD STALE ({age:.0f}s > {max_age_sec:.0f}s) — "
                           f"blocking signal (no committed index bar recently)")

    # MACD line = Fast - Slow. macd[0] is the most recent value (the
    # provisional one for intra-bar synthetic bars).
    try:
        macd_line = float(idx_fields["macd"][0])
    except (KeyError, IndexError, TypeError, ValueError):
        return False, f"{index_symbol}: MACD value unavailable in fields"

    if direction == "LONG":
        ok = macd_line > 0.0
        verdict = "ALIGNED" if ok else "BLOCKED"
        return ok, (f"{index_symbol} [{verdict}] Fast>Slow check: "
                    f"MACD({macd_line:.5f}) {'>' if ok else '<='} 0")
    elif direction == "SHORT":
        ok = macd_line < 0.0
        verdict = "ALIGNED" if ok else "BLOCKED"
        return ok, (f"{index_symbol} [{verdict}] Fast<Slow check: "
                    f"MACD({macd_line:.5f}) {'<' if ok else '>='} 0")
    return False, f"index alignment: unknown direction {direction!r}"


def check_entry_signal(bar: Dict, open_positions: Optional[Dict] = None) -> Dict:
    """
    Hybrid LONG/SHORT entry — user theory (2026-05-18):

      TRIGGER (either fires):
        (1) Pattern:  MACD[0] > MACD[N]                (LONG)   — "Fast minus N"
                      MACD[0] < MACD[N]                (SHORT)
                      where N = Signal_L for LONG, Signal_S for SHORT.
        (2) Cross:    MACD line crosses zero this bar  (= Fast crossing Slow)
                      LONG  on negative → positive flip
                      SHORT on positive → negative flip

      FILTERS (both must pass, applied after trigger):
        (A) Signal-mean filter:
                LONG : Signal[0] <  Signal_neg_mean × MeanMultiplier_L
                SHORT: Signal[0] >  Signal_pos_mean × MeanMultiplier_S
            "Signal is past its session-mean by the multiplier" — current
            value is in extreme territory.
        (B) Diff-mean filter:
                LONG : MACD[0]   >  Diff_neg_mean   × DiffMultiplier_L
                SHORT: MACD[0]   <  Diff_pos_mean   × DiffMultiplier_S
            "MACD differential has bounced back toward zero from its
            negative/positive session mean" — early reversal signature.

    Pre-checks (always run first):
      • ExecutionType = 4 → NONE
      • Outside trading window → NONE
      • Position already open → NONE
      • Direction allowed by ExecutionType

    Index filter (simplified Fast > Slow) runs after all the above pass.

    Verbose logging is gated by `AdditionalPrintLogs` — when Y, every
    cross-or-pattern fire prints a one-line breakdown of every check, so
    you can see WHY an entry was allowed or blocked.
    """
    raw_cfg = load_config()
    symbol  = bar.get("symbol", "UNKNOWN")
    cfg     = _resolve_stock_cfg(symbol, raw_cfg)
    verbose = str(cfg.get("AdditionalPrintLogs", "Y")).upper() == "Y"

    result = {"signal": "NONE", "symbol": symbol, "orders": {}, "reason": ""}

    # ── Pre-checks ───────────────────────────────────────────────────────
    exec_type = int(cfg["ExecutionType"])
    if exec_type == 4:
        result["reason"] = "ExecutionType=4: all trading disabled"
        return result

    # 2026-08-04 (per user): the Stocks "Enabled" flag is now a LIVE off-switch.
    # It was previously read ONLY by the hot-add daemon when CREATING a pipeline,
    # so setting it to N mid-session did nothing — an already-running symbol kept
    # trading while the sheet said it was off. Checked here, beside the
    # ExecutionType gate and BEFORE any bar math, so N stops new entries on the
    # very next evaluation (~1-2s after saving the Excel).
    # ENTRIES ONLY: an open position keeps its stop / breakeven / trail / EoD
    # exit, so disabling a symbol can never strand an unprotected position.
    _enabled = str(
        ((raw_cfg.get("Stocks") or {}).get(symbol, {}) or {}).get("enabled", "Y")
        or "Y"
    ).strip().upper()
    if _enabled == "N":
        if verbose:
            logger.info("[%s] entry SKIPPED — Enabled=N in Excel "
                        "(exits on any open position are unaffected)", symbol)
        result["reason"] = "disabled in Excel (Enabled=N)"
        return result

    auto_manual = str(cfg.get("Auto_Manual", "A")).upper()

    bar_time = bar.get("current_time")
    if bar_time is None:
        result["reason"] = "bar missing current_time"
        return result
    now = bar_time.time() if hasattr(bar_time, "time") else bar_time
    start_trade = _parse_time(cfg["TimeToStartTrading"])
    stop_trade  = _parse_time(cfg["TimeToStopTrading"])
    if not (start_trade <= now <= stop_trade):
        result["reason"] = f"Outside trading hours ({start_trade}-{stop_trade})"
        return result

    if open_positions and open_positions.get(symbol):
        result["reason"] = f"{symbol}: position already open"
        return result

    # ── Read indicator state from bar dict ───────────────────────────────
    macd_hist     = bar.get("macd")          or []
    sig_hist      = bar.get("macd_signal")   or []
    sig_pos_mean  = float(bar.get("macd_signal_pos_mean", 0.0))
    sig_neg_mean  = float(bar.get("macd_signal_neg_mean", 0.0))
    diff_pos_mean = float(bar.get("macd_diff_pos_mean",   0.0))
    diff_neg_mean = float(bar.get("macd_diff_neg_mean",   0.0))

    if len(macd_hist) < 2 or len(sig_hist) < 2:
        result["reason"] = f"insufficient history: macd={len(macd_hist)} sig={len(sig_hist)}"
        return result

    macd_now = float(macd_hist[0])
    macd_prev = float(macd_hist[1])               # last closed bar BEFORE the current evaluation
    sig_now  = float(sig_hist[0])
    sig_prev = float(sig_hist[1])                 # previous signal-line value (for MACD/signal crossover)

    # ── Per-direction lookback distance for the pattern ──────────────────
    signal_l = max(1, int(cfg.get("Signal_L", 1)))  # ≥1 — pattern requires at least 1-bar lookback
    signal_s = max(1, int(cfg.get("Signal_S", 1)))
    mean_mult_l = float(cfg.get("MeanMultiplier_L", 1.0))
    mean_mult_s = float(cfg.get("MeanMultiplier_S", 1.0))
    diff_mult_l = float(cfg.get("DiffMultiplier_L", 1.0))
    diff_mult_s = float(cfg.get("DiffMultiplier_S", 1.0))

    # ── Mean-filter time gate ───────────────────────────────────────────
    # Means only start accumulating at MeanCalcStartTime (default 10:30).
    # Before that, mean = 0 and the filter thresholds collapse to 0 ×
    # multiplier = 0 — which would reject every LONG with negative MACD
    # and every SHORT with positive MACD. That's not what the user wants.
    #
    # Rule: filters apply only after MeanStartTime AND only when the
    # relevant mean has actually accumulated a non-zero value.
    # Before then, the trigger alone (Pattern OR Cross) is enough.
    try:
        mean_start = _parse_time(cfg.get("MeanStartTime", "10:30"))
        filters_active = now >= mean_start
    except Exception:
        filters_active = True

    # ── Build the per-direction check helpers ────────────────────────────
    def _check_long():
        # Trigger 1: Pattern (Fast minus N reversal — direction agnostic name
        # in user's vocab; here implemented on the MACD line itself)
        pattern_ok = False
        pattern_reason = ""
        if len(macd_hist) > signal_l:
            macd_n = float(macd_hist[signal_l])
            turn_up    = macd_now > macd_n
            below_slow = macd_now < sig_now          # 2026-06-01: LONG pattern valid only while fast is still BELOW slow
            pattern_ok = turn_up and below_slow
            pattern_reason = (f"MACD[0]={macd_now:+.5f} {'>' if turn_up else '<='} "
                              f"MACD[{signal_l}]={macd_n:+.5f}  & fast<slow({sig_now:+.5f})={below_slow}")
        else:
            pattern_reason = f"need {signal_l+1} bars, have {len(macd_hist)}"

        # Trigger 2: SIGNAL-LINE crossover — MACD line crossing UP through its
        # signal line THIS bar ⇒ LONG. (2026-05-29: replaced the old zero-line
        # cross.) 2026-07-11 (per user): the 5%-proximity "near-cross" logic was
        # REMOVED entirely — an entry fires ONLY on the exact committed crossover.
        signal_cross = (macd_prev <= sig_prev) and (macd_now > sig_now)
        gap = abs(macd_now - sig_now)
        cross_ok = signal_cross
        # 2026-06-02: MACD_LONG_LINE gates the WHOLE long entry vs the zero line
        # (applies to pattern OR cross). 1=only ABOVE zero (macd>0),
        # 2=only BELOW zero (macd<0), 3=irrespective.
        # 2026-06-05 (per user): the MACD_LONG_LINE zero flag NO LONGER gates the
        # entry. The PATTERN trigger fires irrespective of the zero flag. The
        # CROSS trigger is gated by the session mean vs the slow line:
        # LONG cross passes if MACDLongMean(signal_neg_mean) >= MeanMultiplier_L
        # x SlowLine(signal). Fail-open while the mean is not established (==0).
        long_line = int(float(cfg.get("MACD_LONG_LINE", 3) or 3))
        if (not filters_active) or sig_neg_mean == 0.0:
            # 2026-07-14: CrossMeanGate now respects mean_start_time (like A/B).
            mean_ok = True
            zl_detail = ("cross mean-gate OFF (pre mean_start_time)"
                         if not filters_active else "cross mean-gate skipped (LongMean=0)")
        else:
            _thr = mean_mult_l * sig_now
            mean_ok = sig_neg_mean >= _thr
            zl_detail = (f"LongMean({sig_neg_mean:+.5f}) "
                         f"{'>=' if mean_ok else '<'} MultL({mean_mult_l})×Slow({sig_now:+.5f})={_thr:+.5f}")
        cross_reason = (f"MACD={macd_now:+.5f} vs Sig={sig_now:+.5f}  gap={gap:.5f}  "
                        f"sigX={'YES' if signal_cross else 'no'}  "
                        f"CrossMeanGate({zl_detail})={'ok' if mean_ok else 'BLOCK'}")

        # Filter A: Signal in extreme negative
        sig_threshold = sig_neg_mean * mean_mult_l
        if not filters_active or sig_neg_mean == 0.0:
            filter_a_ok = True
            filter_a_reason = (f"SKIPPED (pre-{cfg.get('MeanStartTime','10:30')} "
                               f"or mean=0) — Sig={sig_now:+.5f}")
        else:
            filter_a_ok = sig_now < sig_threshold
            filter_a_reason = (f"Sig={sig_now:+.5f} {'<' if filter_a_ok else '>='} "
                               f"SigMean−({sig_neg_mean:+.5f}) × MeanMultL({mean_mult_l}) "
                               f"= {sig_threshold:+.5f}")

        # Filter B: MACD bouncing up from negative mean
        diff_threshold = diff_neg_mean * diff_mult_l
        if not filters_active or diff_neg_mean == 0.0:
            filter_b_ok = True
            filter_b_reason = (f"SKIPPED (pre-{cfg.get('MeanStartTime','10:30')} "
                               f"or mean=0) — MACD={macd_now:+.5f}")
        else:
            filter_b_ok = macd_now > diff_threshold
            filter_b_reason = (f"MACD={macd_now:+.5f} {'>' if filter_b_ok else '<='} "
                               f"DiffMean−({diff_neg_mean:+.5f}) × DiffMultL({diff_mult_l}) "
                               f"= {diff_threshold:+.5f}")

        # 2026-06-08 (per user): PATTERN-ONLY. Crossover trigger DISABLED.
        # Direction gated by MACD_LONG_LINE vs zero (1=only above 0, 2=only
        # below 0, 3=either) — "go only when allowed". Plus mean-gate:
        # MeanLong >= MultL × Slow (fail-open while mean=0). Filters A/B no
        # longer gate the entry in pattern-only mode.
        zl_ok = (long_line == 3) or (long_line == 1 and macd_now > 0.0) or (long_line == 2 and macd_now < 0.0)
        cross_pass = cross_ok and mean_ok     # retained for logging
        # 2026-06-09 (per user): per-stock trigger mode from the "Signal" column
        # (Stocks col M -> config 'signal_mode'): 0 = CROSSOVER only, 1 = PATTERN
        # only, anything else / blank = BOTH (pattern OR cross).
        _smode = (raw_cfg.get("Stocks") or {}).get(symbol, {}).get("signal_mode")
        try:
            _smode = int(_smode) if _smode not in (None, "") else None
        except (ValueError, TypeError):
            _smode = None
        if _smode == 0:
            _core_ok = cross_ok
        elif _smode == 1:
            _core_ok = pattern_ok
        else:
            _core_ok = pattern_ok or cross_ok
        trigger_ok = _core_ok and zl_ok and mean_ok
        all_ok = trigger_ok
        return (all_ok, pattern_ok, cross_ok, filter_a_ok, filter_b_ok,
                pattern_reason, cross_reason, filter_a_reason, filter_b_reason)

    def _check_short():
        pattern_ok = False
        pattern_reason = ""
        if len(macd_hist) > signal_s:
            macd_n = float(macd_hist[signal_s])
            turn_dn    = macd_now < macd_n
            above_slow = macd_now > sig_now          # 2026-06-01: SHORT pattern valid only while fast is still ABOVE slow
            pattern_ok = turn_dn and above_slow
            pattern_reason = (f"MACD[0]={macd_now:+.5f} {'<' if turn_dn else '>='} "
                              f"MACD[{signal_s}]={macd_n:+.5f}  & fast>slow({sig_now:+.5f})={above_slow}")
        else:
            pattern_reason = f"need {signal_s+1} bars, have {len(macd_hist)}"

        # SIGNAL-LINE crossover — MACD line crossing DOWN through its signal line
        # THIS bar ⇒ SHORT. 2026-07-11 (per user): the 5%-proximity "near-cross"
        # logic was REMOVED entirely — an entry fires ONLY on the exact committed
        # crossover.
        signal_cross = (macd_prev >= sig_prev) and (macd_now < sig_now)
        gap = abs(macd_now - sig_now)
        cross_ok = signal_cross
        # 2026-06-02: MACD_SHORT_LINE gates the WHOLE short entry vs the zero
        # line (pattern OR cross). 1=only ABOVE zero (macd>0),
        # 2=only BELOW zero (macd<0), 3=irrespective.
        # 2026-06-05 (per user): zero flag no longer gates; pattern fires
        # irrespective; the CROSS is gated by SHORT mean vs the slow line:
        # SHORT cross passes if MACDShortMean(signal_pos_mean) <= MeanMultiplier_S
        # x SlowLine(signal). Fail-open while the mean is not established (==0).
        short_line = int(float(cfg.get("MACD_SHORT_LINE", 3) or 3))
        if (not filters_active) or sig_pos_mean == 0.0:
            # 2026-07-14: CrossMeanGate now respects mean_start_time (like A/B).
            mean_ok = True
            zl_detail = ("cross mean-gate OFF (pre mean_start_time)"
                         if not filters_active else "cross mean-gate skipped (ShortMean=0)")
        else:
            _thr = mean_mult_s * sig_now
            mean_ok = sig_pos_mean <= _thr
            zl_detail = (f"ShortMean({sig_pos_mean:+.5f}) "
                         f"{'<=' if mean_ok else '>'} MultS({mean_mult_s})×Slow({sig_now:+.5f})={_thr:+.5f}")
        cross_reason = (f"MACD={macd_now:+.5f} vs Sig={sig_now:+.5f}  gap={gap:.5f}  "
                        f"sigX={'YES' if signal_cross else 'no'}  "
                        f"CrossMeanGate({zl_detail})={'ok' if mean_ok else 'BLOCK'}")

        sig_threshold = sig_pos_mean * mean_mult_s
        if not filters_active or sig_pos_mean == 0.0:
            filter_a_ok = True
            filter_a_reason = (f"SKIPPED (pre-{cfg.get('MeanStartTime','10:30')} "
                               f"or mean=0) — Sig={sig_now:+.5f}")
        else:
            filter_a_ok = sig_now > sig_threshold
            filter_a_reason = (f"Sig={sig_now:+.5f} {'>' if filter_a_ok else '<='} "
                               f"SigMean+({sig_pos_mean:+.5f}) × MeanMultS({mean_mult_s}) "
                               f"= {sig_threshold:+.5f}")

        diff_threshold = diff_pos_mean * diff_mult_s
        if not filters_active or diff_pos_mean == 0.0:
            filter_b_ok = True
            filter_b_reason = (f"SKIPPED (pre-{cfg.get('MeanStartTime','10:30')} "
                               f"or mean=0) — MACD={macd_now:+.5f}")
        else:
            filter_b_ok = macd_now < diff_threshold
            filter_b_reason = (f"MACD={macd_now:+.5f} {'<' if filter_b_ok else '>='} "
                               f"DiffMean+({diff_pos_mean:+.5f}) × DiffMultS({diff_mult_s}) "
                               f"= {diff_threshold:+.5f}")

        # 2026-06-08 (per user): PATTERN-ONLY. Crossover trigger DISABLED.
        # Direction gated by MACD_SHORT_LINE vs zero (1=only above 0, 2=only
        # below 0, 3=either) — "go only when allowed". Plus mean-gate:
        # MeanShort <= MultS × Slow (fail-open while mean=0). Filters A/B no
        # longer gate the entry in pattern-only mode.
        zl_ok = (short_line == 3) or (short_line == 1 and macd_now > 0.0) or (short_line == 2 and macd_now < 0.0)
        cross_pass = cross_ok and mean_ok     # retained for logging
        # 2026-06-09 (per user): per-stock trigger mode from the "Signal" column
        # (Stocks col M -> config 'signal_mode'): 0 = CROSSOVER only, 1 = PATTERN
        # only, anything else / blank = BOTH (pattern OR cross).
        _smode = (raw_cfg.get("Stocks") or {}).get(symbol, {}).get("signal_mode")
        try:
            _smode = int(_smode) if _smode not in (None, "") else None
        except (ValueError, TypeError):
            _smode = None
        if _smode == 0:
            _core_ok = cross_ok
        elif _smode == 1:
            _core_ok = pattern_ok
        else:
            _core_ok = pattern_ok or cross_ok
        trigger_ok = _core_ok and zl_ok and mean_ok
        all_ok = trigger_ok
        return (all_ok, pattern_ok, cross_ok, filter_a_ok, filter_b_ok,
                pattern_reason, cross_reason, filter_a_reason, filter_b_reason)

    # 2026-06-10 (per user): per-stock Direction (Stocks col AB -> config
    # 'direction_mode'), NUMERIC like ExecutionType: 1=short-only, 2=long-only,
    # 3=both, 4=off. Blank/invalid falls back to the global ExecutionType.
    # Overrides the global ExecutionType for that one stock.
    _dm = ((raw_cfg.get("Stocks") or {}).get(symbol, {}) or {}).get("direction_mode")
    try:
        _eff_type = int(_dm) if _dm not in (None, "") else exec_type
    except (ValueError, TypeError):
        _eff_type = exec_type
    long_eval  = _check_long()  if _eff_type in (2, 3) else None
    short_eval = _check_short() if _eff_type in (1, 3) else None

    # Detailed per-check breakdown — printed when AdditionalPrintLogs=Y AND
    # at least one trigger fired (otherwise log noise would be huge — the
    # check is called on every tick via intra-bar pipeline events).
    def _log_breakdown(label, ev):
        if not verbose or ev is None: return
        all_ok, p, c, fa, fb, pr, cr, far, fbr = ev
        if not (p or c):
            return                                  # nothing interesting happened
        logger.info(
            "[%s] %s eval:  trigger=%s%s  filterA=%s  filterB=%s",
            symbol, label,
            "PATTERN" if p else "—",
            ("+CROSS" if c else ""),
            "PASS" if fa else "FAIL", "PASS" if fb else "FAIL")
        logger.info("[%s]   pattern: %s", symbol, pr)
        logger.info("[%s]   cross  : %s", symbol, cr)
        logger.info("[%s]   filterA: %s", symbol, far)
        logger.info("[%s]   filterB: %s", symbol, fbr)

    _log_breakdown("LONG",  long_eval)
    _log_breakdown("SHORT", short_eval)

    # ── Determine direction (LONG preferred on simultaneous fire) ────────
    if long_eval and long_eval[0]:
        direction = "LONG"
        eval_used = long_eval
    elif short_eval and short_eval[0]:
        direction = "SHORT"
        eval_used = short_eval
    else:
        # Build a concise blocked-reason. 2026-07-14: ALWAYS include the
        # pattern/cross state (even when NO trigger fired) so the log shows
        # exactly why the symbol did not enter — not just "no trigger fired".
        bits = []
        if long_eval:
            _, p, c, fa, fb, pr, cr, *_ = long_eval
            _trg = ("PATTERN" if p else "") + ("+CROSS" if c else "") or "none"
            bits.append(f"LONG(trig={_trg},filterA={fa},filterB={fb}; pat: {pr}; cross: {cr})")
        if short_eval:
            _, p, c, fa, fb, pr, cr, *_ = short_eval
            _trg = ("PATTERN" if p else "") + ("+CROSS" if c else "") or "none"
            bits.append(f"SHORT(trig={_trg},filterA={fa},filterB={fb}; pat: {pr}; cross: {cr})")
        if not bits:
            bits.append("direction OFF for this symbol (Direction/ExecutionType)")
        result["reason"] = "no entry — " + "  |  ".join(bits)
        return result

    all_ok, patt_ok, cross_ok, *_ = eval_used
    intrabar_flag = bool(bar.get("_intrabar", False))

    # ── Index filter (Fast vs Slow on the index) ─────────────────────────
    stock_entry = (raw_cfg.get("Stocks") or {}).get(symbol, {}) or {}
    use_idx     = str(stock_entry.get("index_filter", "N")).strip().upper() == "Y"
    idx_name    = str(stock_entry.get("index", "") or "").strip().upper()
    if use_idx:
        if not idx_name:
            result["reason"] = (f"{symbol}: index_filter=Y but no index "
                                "assigned — blocking signal")
            logger.warning("[%s] %s", symbol, result["reason"])
            return result
        from indicators import (IndexMACDStore, resolve_symbol_params,   # lazy: avoid circular
                                 resolution_to_seconds)
        idx_fields = IndexMACDStore.get(idx_name)
        # 2026-06-11: stale-index guard window = 2x the index resolution.
        try:
            _idx_res     = resolve_symbol_params(idx_name, raw_cfg, as_index=True).get("resolution", "")
            _idx_max_age = 2.0 * resolution_to_seconds(_idx_res)
        except Exception:
            _idx_max_age = None
        idx_pass, idx_reason = check_index_alignment(
            idx_name, direction, idx_fields, bar.get("current_time"),
            max_age_sec=_idx_max_age,
        )
        if not idx_pass:
            result["reason"] = f"{symbol} blocked by {idx_name}: {idx_reason}"
            logger.info("[%s] Signal BLOCKED by index filter — %s", symbol, idx_reason)
            return result
        if verbose:
            logger.info("[%s] Index gate PASSED (%s) — %s", symbol, idx_name, idx_reason)

    # ── Build orders ─────────────────────────────────────────────────────
    orders = _build_entry_orders(direction, float(bar["close"]), cfg)
    orders["send_to_market"] = (auto_manual == "A")

    trig_str = "PATTERN" if patt_ok else "CROSS"
    if patt_ok and cross_ok:
        trig_str = "PATTERN+CROSS"
    reason = (f"{direction} via {trig_str} "
              f"({'intra-bar' if intrabar_flag else 'bar-close'})")
    result.update({
        "signal"    : direction,
        "orders"    : orders,
        "reason"    : reason,
        "auto_mode" : (auto_manual == "A"),
    })

    logger.info("[%s] *** %s SIGNAL ***  close=%.4f  auto=%s  intrabar=%s  reason=%s",
                symbol, direction, float(bar["close"]),
                auto_manual == "A", intrabar_flag, reason)
    return result


# =============================================================================
# SELF-TEST  (rewritten for cross-based logic)
# =============================================================================

if __name__ == "__main__":
    import sys
    print("=" * 65)
    print("entry_logic.py  self-test")
    print("=" * 65)

    failures = []

    def check(label, got_ok, got_reason, expect_pass):
        ok = (got_ok == expect_pass)
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
        print(f"         {got_reason}")
        if not ok:
            failures.append(label)

    # Helper: minimal bar dict that the new hybrid check_entry_signal
    # expects. Needs ≥2 MACD/Signal history entries so the trigger checks
    # have something to compare against.
    def make_bar(symbol="AAPL", t="10:00", close=150.0,
                 cross_event=None, intrabar=False,
                 macd_hist=None, sig_hist=None,
                 sig_neg_mean=0.0, sig_pos_mean=0.0,
                 diff_neg_mean=0.0, diff_pos_mean=0.0):
        if macd_hist is None:
            macd_hist = [0.0, 0.0]
        if sig_hist is None:
            sig_hist = [0.0, 0.0]
        return {
            "symbol": symbol, "close": close,
            "current_time": datetime.strptime(t, "%H:%M"),
            "_cross_event": cross_event,
            "_intrabar"   : intrabar,
            "macd"        : macd_hist,
            "macd_signal" : sig_hist,
            "macd_zl": 0.0, "macd_zl_signal": 0.0,
            "macd_signal_pos_mean": sig_pos_mean,
            "macd_signal_neg_mean": sig_neg_mean,
            "macd_diff_pos_mean"  : diff_pos_mean,
            "macd_diff_neg_mean"  : diff_neg_mean,
            "ema_fast": 0.0, "ema_slow": 0.0,
        }

    print("\n-- Hybrid (pattern OR cross) + dual-mean filter --")
    # LONG case that passes everything:
    #   pattern: macd[0]=+0.05 > macd[1]=-0.02  → PASS
    #   cross  : prev=-0.02 → now=+0.05 → BULLISH_ZERO → PASS
    #   filter A (mult=1.0): sig=-0.20 < sig_neg_mean=-0.10 × 1.0 = -0.10 → PASS
    #   filter B (mult=1.0): macd=+0.05 > diff_neg_mean=-0.10 × 1.0 = -0.10 → PASS
    sig = check_entry_signal(make_bar(
        cross_event="BULLISH_ZERO",
        macd_hist=[0.05, -0.02],
        sig_hist=[-0.20, -0.18],
        sig_neg_mean=-0.10, diff_neg_mean=-0.10))
    check("LONG: pattern+cross+filtersOK → fires",
          sig["signal"], sig["reason"], expect_pass="LONG")

    # LONG blocked by filter A — Signal not negative enough:
    # With AAPL's mean_multiplier_l=0.2 in config, threshold = -1.00 × 0.2 = -0.20.
    # sig=-0.05 is NOT < -0.20 → filter A fails.
    sig = check_entry_signal(make_bar(
        cross_event="BULLISH_ZERO",
        macd_hist=[0.05, -0.02],
        sig_hist=[-0.05, -0.18],
        sig_neg_mean=-1.0, diff_neg_mean=-0.10))
    check("LONG: filter A FAIL → blocked",
          sig["signal"], sig["reason"], expect_pass="NONE")

    # SHORT case that passes everything:
    sig = check_entry_signal(make_bar(
        cross_event="BEARISH_ZERO",
        macd_hist=[-0.05, 0.02],
        sig_hist=[0.20, 0.18],
        sig_pos_mean=0.10, diff_pos_mean=0.10))
    check("SHORT: pattern+cross+filtersOK → fires",
          sig["signal"], sig["reason"], expect_pass="SHORT")

    # No trigger at all → NONE
    # Use equal MACD values so neither LONG nor SHORT pattern fires, and
    # no zero-cross occurs either.
    sig = check_entry_signal(make_bar(
        cross_event=None,
        macd_hist=[0.01, 0.01],
        sig_hist=[0.05, 0.04]))
    check("No trigger → NONE",
          sig["signal"], sig["reason"], expect_pass="NONE")

    print("\n-- Position-open guard --")
    sig_blocked = check_entry_signal(
        make_bar(cross_event="BULLISH_ZERO"),
        open_positions={"AAPL": True})
    check("Open position blocks new entry",
          sig_blocked["signal"], sig_blocked["reason"], expect_pass="NONE")

    print("\n-- Trading-hours gate --")
    sig_early = check_entry_signal(make_bar(cross_event="BULLISH_ZERO", t="06:00"))
    check("Pre-market cross blocked",
          sig_early["signal"], sig_early["reason"], expect_pass="NONE")

    print("\n-- Index filter: Fast vs Slow only --")
    # LONG requires index MACD > 0 (Fast > Slow)
    long_align = {"macd": [0.05]}
    ok, reason = check_index_alignment("SPY", "LONG", long_align)
    check("Index LONG: MACD=+0.05 ALIGNED", ok, reason, expect_pass=True)

    short_block_long = {"macd": [-0.05]}
    ok, reason = check_index_alignment("SPY", "LONG", short_block_long)
    check("Index LONG: MACD=-0.05 BLOCKED", ok, reason, expect_pass=False)

    # SHORT requires index MACD < 0 (Fast < Slow)
    short_align = {"macd": [-0.05]}
    ok, reason = check_index_alignment("SPY", "SHORT", short_align)
    check("Index SHORT: MACD=-0.05 ALIGNED", ok, reason, expect_pass=True)

    long_block_short = {"macd": [0.05]}
    ok, reason = check_index_alignment("SPY", "SHORT", long_block_short)
    check("Index SHORT: MACD=+0.05 BLOCKED", ok, reason, expect_pass=False)

    ok, reason = check_index_alignment("SPY", "LONG", None)
    check("Index not ready: BLOCKS", ok, reason, expect_pass=False)

    print()
    if failures:
        print(f"FAILED tests: {failures}")
        sys.exit(1)
    print("ALL TESTS PASSED")