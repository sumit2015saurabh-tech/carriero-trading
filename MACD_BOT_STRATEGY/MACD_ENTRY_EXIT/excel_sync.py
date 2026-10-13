#!/usr/bin/env python3
"""
excel_sync.py
=============
Watches strategy_config.xlsx for changes.
When you save the Excel file, this script reads the new values and
writes them immediately to config.json — no restart of the strategy needed.

Additionally, every change is logged to a daily CSV file:
    changes_DD_MM_YYYY.csv
with columns:
    Timestamp | Sheet | Symbol/Section | Field | Old Value | New Value | Applied To Config

Usage:
    python excel_sync.py                  # watches files in same folder
    python excel_sync.py /path/to/folder  # watches a specific folder

Keep this running in a terminal alongside your strategy.
"""

import csv
import json
import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import openpyxl
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ── Timezone ──────────────────────────────────────────────────────────────────
EST = ZoneInfo("America/New_York")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SYNC] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("excel_sync")

# ── Paths ─────────────────────────────────────────────────────────────────────
FOLDER    = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
XLSX_FILE = FOLDER / "strategy_config.xlsx"
JSON_FILE = FOLDER / "config.json"

CSV_HEADERS = [
    "Timestamp",
    "Sheet",
    "Symbol / Section",
    "Field",
    "Old Value",
    "New Value",
    "Applied To Config",
]


# =============================================================================
# CSV CHANGE LOGGER
# =============================================================================

def _csv_path() -> Path:
    """Return today's CSV path: changes_DD_MM_YYYY.csv (EST date)."""
    tag = datetime.now(EST).strftime("%d_%m_%Y")
    return FOLDER / f"changes_{tag}.csv"


def _log_changes_to_csv(changes: list[dict]):
    """
    Write changes to today's CSV using a read-modify-write strategy.

    Rule: one row per (Sheet, Symbol/Section) per day — no matter how many
    times the user saves the Excel file.

    If a row for that stock/section already exists today:
        - Old Value  → NEVER changed  (always reflects original state)
        - New Value  → REPLACED with the latest full entry
        - Field      → UNION of all changed fields seen so far today
        - Timestamp  → updated to latest save time
    If no row exists yet today → a new row is appended.
    """
    path = _csv_path()

    # ── Load existing rows ────────────────────────────────────────────────
    existing_rows: list[dict] = []
    if path.exists():
        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                existing_rows = list(csv.DictReader(f))
        except Exception as e:
            log.error("Could not read existing CSV for merge: %s", e)

    # ── Build lookup: (Sheet, Symbol/Section) → row index ────────────────
    key_to_idx: dict[tuple, int] = {}
    for i, row in enumerate(existing_rows):
        key = (row["Sheet"], row["Symbol / Section"])
        key_to_idx[key] = i

    ts      = datetime.now(EST).strftime("%Y-%m-%d %H:%M:%S")
    added   = 0
    updated = 0

    for change in changes:
        key = (change["Sheet"], change["Symbol / Section"])

        if key in key_to_idx:
            # ── Row exists — UPDATE in place, never touch Old Value ───────
            idx = key_to_idx[key]
            row = existing_rows[idx]

            # Merge field names (union of all fields changed since first entry)
            prev_fields = {f.strip() for f in row["Field"].split(",") if f.strip()}
            new_fields  = {f.strip() for f in change["Field"].split(",") if f.strip()}
            merged      = prev_fields | new_fields
            merged.discard("[ALL FIELDS]")   # promote from placeholder to real names

            row["Field"]             = ", ".join(sorted(merged)) if merged else "[ALL FIELDS]"
            row["New Value"]         = change["New Value"]       # always latest full entry
            row["Timestamp"]         = ts
            row["Applied To Config"] = change["Applied To Config"]
            # row["Old Value"]  ← intentionally NOT updated
            updated += 1

        else:
            # ── No row yet today — add a fresh one ────────────────────────
            change["Timestamp"] = ts
            existing_rows.append(change)
            key_to_idx[key] = len(existing_rows) - 1
            added += 1

    # ── Write all rows back atomically ────────────────────────────────────
    tmp = str(path) + ".tmp"
    try:
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()
            writer.writerows(existing_rows)
        os.replace(tmp, path)
        log.info(
            "Change log updated → %s  (%d new row(s), %d row(s) updated)",
            path.name, added, updated,
        )
    except Exception as e:
        log.error("Failed to write CSV log: %s", e)


def _make_change_row(
    sheet: str,
    section: str,
    field: str,
    old_val,
    new_val,
    applied: bool,
) -> dict:
    """Build one CSV row dict."""
    return {
        "Timestamp"         : datetime.now(EST).strftime("%Y-%m-%d %H:%M:%S"),
        "Sheet"             : sheet,
        "Symbol / Section"  : section,
        "Field"             : field,
        "Old Value"         : "" if old_val is None else str(old_val),
        "New Value"         : "" if new_val is None else str(new_val),
        "Applied To Config" : "YES" if applied else "NO",
    }


# =============================================================================
# HELPERS
# =============================================================================

def _str(v):
    """Normalise a cell value to a clean string, or '' if empty."""
    if v is None:
        return ""
    return str(v).strip()

def _upper(v):
    return _str(v).upper()

def _num(v, default):
    """Convert to int/float, return default if blank or not a number.
    Returns int when the value is a whole number (e.g. 3.0 → 3),
    EXCEPT when called via _float() which always forces a float result.
    """
    s = _str(v)
    if s == "":
        return default
    try:
        f = float(s)
        return int(f) if f == int(f) else f
    except ValueError:
        return default

def _float(v, default=0.0):
    """
    Always return a float, NEVER collapse to int.
    Fixes: BEMultiplier/MPL/MeanMultiplier decimal rounding (Apr 2026).

    _num(1.0) returns int(1) — strips the decimal.
    _float(1.0) always returns float(1.0).

    Works for per-stock fields where default="" (blank cell = omit from config):
      _float(None, "")  → ""   (blank cell — field will be removed by cleanup)
      _float(0.6,  "")  → 0.6  (valid decimal preserved)
      _float(1.0,  "")  → 1.0  (whole number kept as float, NOT collapsed to int 1)
      _float(1,    "")  → 1.0  (integer input still becomes float)
    """
    s = _str(v)
    if s == "":
        # Blank cell — return the default as-is (may be "" for optional per-stock fields)
        if isinstance(default, str):
            return default
        return float(default)
    try:
        return float(s)
    except ValueError:
        if isinstance(default, str):
            return default
        return float(default)

def _yn(v, default="N"):
    """Normalise a Y/N cell."""
    u = _upper(v)
    return u if u in ("Y", "N") else default


# =============================================================================
# SHEET READERS
# =============================================================================

def read_global_settings(ws) -> dict:
    """
    Reads every row in Global Settings where column A is a known config key
    and column B is the value.  Skips section headers (merged/empty B).
    Returns a flat dict of {config_key: value}.
    """
    PARAM_MAP = {
        "iAccount"           : ("iAccount",            "str"),
        "Auto_Manual"        : ("Auto_Manual",          "str"),
        "ExecutionType"      : ("ExecutionType",        "int"),
        "PositionSize"       : ("PositionSize",         "int"),
        "EntryOrderType"     : ("EntryOrderType",       "str"),  # MKT or LMT
        "MPL_dollar_Amount"  : ("MPL_dollar_Amount",    "float"),
        "BEMultiplier"       : ("BEMultiplier",         "float"),
        "TargetMultiplier"   : ("TargetMultiplier",     "float"),
        "SLMultiplier"       : ("SLMultiplier",         "float"),
        "MeanMultiplier_L"   : ("MeanMultiplier_L",     "float"),
        "MeanMultiplier_S"   : ("MeanMultiplier_S",     "float"),
        "TrailingPerc"       : ("TrailingPerc",         "float"),
        "StopLimitSpread"    : ("StopLimitSpread",      "float"),
        "TimeToStartTrading" : ("TimeToStartTrading",   "str"),
        "TimeToStopTrading"  : ("TimeToStopTrading",    "str"),
        "MeanStartTime"      : ("MeanStartTime",        "str"),
        "EoDExitTime"        : ("EoDExitTime",          "str"),
        "Signal_L"           : ("Signal_L",             "int"),
        "Signal_S"           : ("Signal_S",             "int"),
        "MACDLE_BLW_ZLine"   : ("MACDLE_BLW_ZLine",     "yn"),
        "MACDSE_ABV_ZLine"   : ("MACDSE_ABV_ZLine",     "yn"),
        "TrailingStop_Perc"  : ("TrailingStop_Perc",    "yn"),
        "AdditionalPrintLogs": ("AdditionalPrintLogs",  "yn"),
        "Interval"           : ("Interval",             "int"),
        "MACD_Resolution"    : ("MACD_Resolution",      "str"),
        "MACD_SecBarSize"    : ("MACD_SecBarSize",       "int"),
        "MACD_Fast"          : ("MACD_Fast",            "int"),
        "MACD_Slow"          : ("MACD_Slow",            "int"),
        "MACD_Signal"        : ("MACD_Signal",          "int"),
        "MACD_PriceSource"   : ("MACD_PriceSource",     "str"),
        "MACD_HistoryDepth"  : ("MACD_HistoryDepth",    "int"),
        "MACD_SeedBars"      : ("MACD_SeedBars",        "int"),
    }

    result = {}
    for row in ws.iter_rows(min_row=3, values_only=True):
        key_raw = _str(row[0]) if row else ""
        val_raw = row[1]       if len(row) > 1 else None
        if not key_raw or key_raw not in PARAM_MAP:
            continue
        cfg_key, t = PARAM_MAP[key_raw]
        if t == "int":
            result[cfg_key] = _num(val_raw, 0)
        elif t == "float":
            # FIX: use _float() not _num() — ensures decimals are preserved
            # e.g. BEMultiplier=1.0 stays 1.0 not collapsed to int 1
            result[cfg_key] = _float(val_raw, 0.0)
        elif t == "yn":
            result[cfg_key] = _yn(val_raw)
        else:
            result[cfg_key] = _str(val_raw)
    return result


def read_stocks(ws) -> dict:
    """
    Reads the Stocks sheet.  Expects headers in row 2.
    Returns a dict keyed by UPPER symbol.
    Rows with an empty Symbol cell are skipped.
    """
    stocks = {}
    for row in ws.iter_rows(min_row=3, values_only=True):
        if not row or not _str(row[0]):
            continue
        sym = _upper(row[0])

        raw_res = _str(row[5]) if len(row) > 5 else ""
        resolution = raw_res.replace(" ", "").lower() if raw_res else ""
        # 2026-06-03: col 6 is now "Entry Candles" (= entry resolution); a new
        # "Exit Candles" column is appended at col 27 (index 26) so existing
        # column indices don't shift. Blank exit → falls back to entry.
        raw_exit = _str(row[26]) if len(row) > 26 else ""
        exit_candles = raw_exit.replace(" ", "").lower() if raw_exit else ""
        # 2026-06-10: per-stock "Direction" appended at col 28 (index 27) so existing
        # indices don't shift. NUMERIC like ExecutionType: 1=short-only, 2=long-only,
        # 3=both, 4=off. Blank => fall back to the global ExecutionType.
        direction_mode = _num(row[27], "") if (len(row) > 27 and row[27] not in (None, "")) else ""

        entry = {
            "enabled"           : _yn(row[1] if len(row) > 1 else None, "N"),
            "quantity"          : _num(row[2] if len(row) > 2 else None, 100),
            "index_filter"      : _yn(row[3] if len(row) > 3 else None, "N"),
            "index"             : _upper(row[4]) if len(row) > 4 else "",
            "macd_resolution"   : resolution,
            "entry_candles"     : resolution,
            "exit_candles"      : exit_candles,
            "direction_mode"    : direction_mode,
            "macd_fast"         : _num(row[6], "") if len(row) > 6 else "",
            "macd_slow"         : _num(row[7], "") if len(row) > 7 else "",
            "macd_signal_period": _num(row[8], "") if len(row) > 8 else "",
            "macd_price_source" : _str(row[9]) if len(row) > 9 else "",
        }

        # Per-stock override columns
        if len(row) > 11 and row[11] is not None:
            entry["mpl"] = _float(row[11], "")   # decimal preserved (e.g. 0.6, 1.0, 0.5)
        # col M "Signal" (2026-06-09): per-stock trigger mode — 0=crossover only,
        # 1=pattern only, anything else/blank=both (pattern OR cross)
        if len(row) > 12 and row[12] is not None:
            entry["signal_mode"] = _num(row[12], "")
        if len(row) > 13 and row[13] is not None:
            entry["signal_l"] = _num(row[13], "")
        if len(row) > 14 and row[14] is not None:
            entry["signal_s"] = _num(row[14], "")
        if len(row) > 15 and row[15] is not None:
            entry["mean_multiplier_l"] = _float(row[15], "")   # decimal preserved
        if len(row) > 16 and row[16] is not None:
            entry["mean_multiplier_s"] = _float(row[16], "")   # decimal preserved
        # 2026-06-02: cols R/S (index 17/18) repurposed BLW_Zline/ABV_Zline →
        # MACD_LONG_LINE (col R/17) gates LONG, MACD_SHORT_LINE (col S/18) gates
        # SHORT, each 1/2/3: 1=trade only ABOVE zero, 2=only BELOW zero, 3=irrespective.
        if len(row) > 17 and row[17] is not None and _str(row[17]) != "":
            entry["macd_long_line"] = _num(row[17], "")
        if len(row) > 18 and row[18] is not None and _str(row[18]) != "":
            entry["macd_short_line"] = _num(row[18], "")
        # FIX: per-stock BEM override (col 20 / index 19). Default 0.5 if blank
        # — handled by the cleanup loop below + the global fallback in
        # entry_logic._resolve_stock_cfg.
        if len(row) > 19 and row[19] is not None:
            entry["bem"] = _float(row[19], "")

        # Per-stock TrailingPerc override (col 21 / index 20). Used by
        # exit_logic.py percentage-trail. Falls back to global TrailingPerc
        # via entry_logic._resolve_stock_cfg when blank.
        if len(row) > 20 and row[20] is not None:
            entry["trailing_perc"] = _float(row[20], "")

        # NEW: Portfolio assignment (col 22 / index 21). Stocks inherit
        # defaults from their assigned portfolio. Blank → "DEFAULT" portfolio
        # (or just globals if no Portfolios sheet exists).
        if len(row) > 21 and _str(row[21]):
            entry["portfolio"] = _str(row[21])

        # 2026-05-22: Per-stock MeanCalcStartTime (col 23 / index 22) and
        # MeanStartTime (col 24 / index 23). Override the global defaults
        # for THIS stock only. Used by indicators.py (mean accumulation gate)
        # and entry_logic.py (mean filter activation gate). Same time pair
        # applies to BOTH MACD and MACD-Diff means (single time setting per
        # equity governs both, per user spec).
        if len(row) > 22 and _str(row[22]):
            entry["mean_calc_start_time"] = _str(row[22])
        if len(row) > 23 and _str(row[23]):
            entry["mean_start_time"] = _str(row[23])

        # 2026-05-22: Per-stock DiffMultiplier_L (col 25 / index 24) and
        # DiffMultiplier_S (col 26 / index 25). Override the global
        # DiffMultiplier_L / DiffMultiplier_S (default 1.0). Wired through
        # entry_logic._resolve_stock_cfg._OVERRIDES so the per-stock value
        # takes precedence over the global.
        if len(row) > 24 and row[24] is not None:
            entry["diff_multiplier_l"] = _float(row[24], "")
        if len(row) > 25 and row[25] is not None:
            entry["diff_multiplier_s"] = _float(row[25], "")

        # 2026-07-31: per-stock "TrailingStop_Perc" appended at col 29 (index 28)
        # so existing indices don't shift. Y = percentage trail, N = 5-bar trail.
        # BLANK => inherit the global TrailingStop_Perc (pruned just below), which
        # is why _yn() is only applied to a non-empty cell — the usual _yn default
        # of "N" would otherwise silently pin every stock to the 5-bar method.
        if len(row) > 28 and _str(row[28]):
            entry["trailing_stop_perc"] = _yn(row[28], "")

        # Remove blank optional fields so they fall back to global defaults.
        # Condition: blank string ("") = cell was empty; 0 or 0.0 = zero value = meaningless.
        # Note: 0.5, 0.6, 1.0 etc. are NOT equal to 0 so they are correctly kept.
        for k in ("macd_resolution", "macd_fast", "macd_slow",
                  "macd_signal_period", "macd_price_source",
                  "mpl", "signal_l", "signal_s",
                  "mean_multiplier_l", "mean_multiplier_s",
                  "diff_multiplier_l", "diff_multiplier_s",
                  "macd_long_line", "macd_short_line",
                  "bem", "trailing_perc", "trailing_stop_perc", "portfolio",
                  "mean_calc_start_time", "mean_start_time"):
            if k in entry and (entry[k] == "" or entry[k] == 0):
                del entry[k]

        stocks[sym] = entry
    return stocks


def read_portfolios(ws) -> dict:
    """
    Reads the Portfolios sheet. Each row defines a named portfolio with
    default field values for stocks assigned to it. Inheritance chain at
    runtime: per-stock field → portfolio field → global default.

    Expected layout (row 2 headers, data from row 3):
        col 0  Portfolio Name     (required — skipped if blank)
        col 1  MACD Resolution
        col 2  MACD Fast
        col 3  MACD Slow
        col 4  MACD Signal
        col 5  Price Source
        col 6  MPL
        col 7  Signal_L
        col 8  Signal_S
        col 9  Mean Multiplier_L
        col 10 Mean Multiplier_S
        col 11 MACDLE_BLW_Zline
        col 12 MACDSE_ABV_Zline
        col 13 BEM
        col 14 TrailingPerc
        col 15 Notes (ignored)

    Returns dict keyed by portfolio name. Empty/blank cells are dropped
    so the inheritance falls through to the global default.
    """
    portfolios: dict = {}
    if ws is None:
        return portfolios
    for row in ws.iter_rows(min_row=3, values_only=True):
        if not row or not _str(row[0]):
            continue
        name = _str(row[0])

        raw_res = _str(row[1]) if len(row) > 1 else ""
        entry: dict = {
            "macd_resolution"   : raw_res.replace(" ", "").lower() if raw_res else "",
            "macd_fast"         : _num(row[2], "") if len(row) > 2 else "",
            "macd_slow"         : _num(row[3], "") if len(row) > 3 else "",
            "macd_signal_period": _num(row[4], "") if len(row) > 4 else "",
            "macd_price_source" : _str(row[5]).strip().lower() if len(row) > 5 else "",
            "mpl"               : _float(row[6], "") if len(row) > 6 else "",
            "signal_l"          : _num(row[7], "") if len(row) > 7 else "",
            "signal_s"          : _num(row[8], "") if len(row) > 8 else "",
            "mean_multiplier_l" : _float(row[9], "") if len(row) > 9 else "",
            "mean_multiplier_s" : _float(row[10], "") if len(row) > 10 else "",
            "macdle_blw_zline"  : _yn(row[11], "") if len(row) > 11 and _str(row[11]) else "",
            "macdse_abv_zline"  : _yn(row[12], "") if len(row) > 12 and _str(row[12]) else "",
            "bem"               : _float(row[13], "") if len(row) > 13 else "",
            "trailing_perc"     : _float(row[14], "") if len(row) > 14 else "",
        }

        # Drop blanks so resolve_symbol_params falls through to globals.
        for k in list(entry.keys()):
            if entry[k] == "" or entry[k] == 0:
                del entry[k]

        portfolios[name] = entry
    return portfolios


def read_indexes(ws) -> dict:
    """
    Reads the Indexes sheet — schema mirrors the Stocks sheet for the
    parameters that apply to a non-traded index (signal evaluation +
    after-mean threshold).

    Columns (0-indexed):
        0   Symbol
        1   Enabled (Y/N)
        2   MACD Params  (informational column; data may be ignored)
        3   Notes
        4   Interval Choice
        5   Signal_L            (per-index override; default 2; ALWAYS rounded to int)
        6   Signal_S            (per-index override; default 2; ALWAYS rounded to int)
        7   MACDLE_BLW_ZLine    (per-index override; default N)
        8   MACDSE_ABV_ZLine    (per-index override; default N)
        9   Mean Multiplier_L   (per-index override; default 1.0)
        10  Mean Multiplier_S   (per-index override; default 1.0)
        11  MACD Resolution     (per-index override; default global MACD_Resolution)
        12  MACD Fast           (per-index override; default global MACD_Fast)
        13  MACD Slow           (per-index override; default global MACD_Slow)
        14  MACD Signal         (per-index override; default global MACD_Signal)
        15  MACD Price Source   (per-index override; default global MACD_PriceSource)

    Per-index fields are written into config.json under Indexes → SYMBOL.
    Blank cells fall back to globals via resolve_symbol_params in indicators.py.

    Signal_L/Signal_S are categorical (0/1/2/3); we round to int before
    storing so a stray decimal in Excel cannot cause display/value drift.
    """
    indexes = {}
    for row in ws.iter_rows(min_row=3, values_only=True):
        if not row or not _str(row[0]):
            continue
        sym = _upper(row[0])

        entry = {"enabled": _yn(row[1] if len(row) > 1 else None, "N")}

        # Signal_L / Signal_S — force integer (categorical params)
        if len(row) > 3 and row[3] is not None:
            try:
                entry["signal_l"] = int(round(float(row[3])))
            except (TypeError, ValueError):
                entry["signal_l"] = ""
        if len(row) > 4 and row[4] is not None:
            try:
                entry["signal_s"] = int(round(float(row[4])))
            except (TypeError, ValueError):
                entry["signal_s"] = ""

        # ZL gates — Y/N text
        if len(row) > 5 and _str(row[5]):
            entry["macdle_blw_zline"] = _yn(row[5], "")
        if len(row) > 6 and _str(row[6]):
            entry["macdse_abv_zline"] = _yn(row[6], "")

        # Mean multipliers — preserve decimal (use _float)
        if len(row) > 7 and row[7] is not None:
            entry["mean_multiplier_l"] = _float(row[7], "")
        if len(row) > 8 and row[8] is not None:
            entry["mean_multiplier_s"] = _float(row[8], "")

        # NEW: per-index MACD calculation parameters
        if len(row) > 9 and _str(row[9]):
            entry["macd_resolution"] = _str(row[9]).replace(" ", "").lower()
        if len(row) > 10 and row[10] is not None:
            entry["macd_fast"] = _num(row[10], "")
        if len(row) > 11 and row[11] is not None:
            entry["macd_slow"] = _num(row[11], "")
        if len(row) > 12 and row[12] is not None:
            entry["macd_signal_period"] = _num(row[12], "")
        if len(row) > 13 and _str(row[13]):
            entry["macd_price_source"] = _str(row[13]).strip().lower()

        # 2026-05-22: Per-index MeanCalcStartTime (col 17 / index 16) and
        # MeanStartTime (col 18 / index 17). Override the global defaults
        # for THIS index only.
        if len(row) > 14 and _str(row[14]):
            entry["mean_calc_start_time"] = _str(row[14])
        if len(row) > 15 and _str(row[15]):
            entry["mean_start_time"] = _str(row[15])

        # 2026-05-22: Per-index DiffMultiplier_L (col 19 / index 18) and
        # DiffMultiplier_S (col 20 / index 19).
        if len(row) > 16 and row[16] is not None:
            entry["diff_multiplier_l"] = _float(row[16], "")
        if len(row) > 17 and row[17] is not None:
            entry["diff_multiplier_s"] = _float(row[17], "")

        # Drop blank/zero so they fall back to defaults.
        # Note: signal_l == 0 (crossover mode) is intentionally treated as
        # "use default" — to set crossover mode for an index, edit
        # config.json directly, same convention as stocks.
        for k in ("signal_l", "signal_s",
                  "macdle_blw_zline", "macdse_abv_zline",
                  "mean_multiplier_l", "mean_multiplier_s",
                  "diff_multiplier_l", "diff_multiplier_s",
                  "macd_resolution", "macd_fast", "macd_slow",
                  "macd_signal_period", "macd_price_source",
                  "mean_calc_start_time", "mean_start_time"):
            if k in entry and (entry[k] == "" or entry[k] == 0):
                del entry[k]

        indexes[sym] = entry
    return indexes


# =============================================================================
# DIFF HELPERS  —  produce granular per-field change records
# =============================================================================

def _diff_global(old_cfg: dict, new_globals: dict) -> list[dict]:
    """
    Compare new global settings against existing config.json values.
    Returns ONE row per sync summarising ALL changed global fields.

    Fields Changed  → comma-separated list of changed param names
    Old Value       → JSON dict of {field: old_value, ...}
    New Value       → JSON dict of {field: new_value, ...}
    """
    changed = {}
    for field, new_val in new_globals.items():
        old_val = old_cfg.get(field)
        if old_val != new_val:
            changed[field] = {"old": old_val, "new": new_val}

    if not changed:
        return []

    fields_str = ", ".join(sorted(changed.keys()))
    old_vals   = {f: v["old"] for f, v in changed.items()}
    new_vals   = {f: v["new"] for f, v in changed.items()}

    return [_make_change_row(
        sheet   = "Global Settings",
        section = "Global",
        field   = fields_str,
        old_val = json.dumps(old_vals),
        new_val = json.dumps(new_vals),
        applied = True,
    )]


def _diff_stocks(old_stocks: dict, new_stocks: dict) -> list[dict]:
    """
    Compare new Stocks sheet data against existing config.json Stocks section.
    Always produces ONE row per stock per day — even across multiple saves.

    The _log_changes_to_csv function enforces this by doing a read-modify-write
    on the CSV: if the stock already has a row today it is UPDATED, never duplicated.

    - New stock added in Excel    → 1 row, Old Value = "DOES NOT EXIST IN CONFIG",
                                    New Value = full current stock entry (JSON)
    - Existing stock with changes → 1 row, Fields = changed field names,
                                    Old Value = JSON of original values for those fields,
                                    New Value = FULL current stock entry (JSON) so the
                                    row always reflects the latest complete state
    - Stock removed from Excel    → 1 row, Applied = YES (deleted from config)
    """
    rows = []
    all_syms = set(old_stocks) | set(new_stocks)

    for sym in sorted(all_syms):
        old_entry = old_stocks.get(sym)
        new_entry = new_stocks.get(sym)

        if old_entry is None:
            # Brand-new stock — log full entry, Old Value = did not exist
            rows.append(_make_change_row(
                sheet   = "Stocks",
                section = sym,
                field   = "[ALL FIELDS]",
                old_val = "DOES NOT EXIST IN CONFIG",
                new_val = json.dumps(new_entry),   # full current entry
                applied = True,
            ))

        elif new_entry is None:
            # ── FIX: Stock removed from Excel → DELETE from config.json ──
            rows.append(_make_change_row(
                sheet   = "Stocks",
                section = sym,
                field   = "[ALL FIELDS]",
                old_val = json.dumps(old_entry),
                new_val = "REMOVED FROM EXCEL — deleted from config",
                applied = True,                    # ← was False before fix
            ))

        else:
            # Existing stock — find which fields changed
            all_fields = set(old_entry) | set(new_entry)
            changed = {}
            for field in sorted(all_fields):
                ov = old_entry.get(field)
                nv = new_entry.get(field)
                if ov != nv:
                    changed[field] = ov   # keep old value for reference

            if changed:
                rows.append(_make_change_row(
                    sheet   = "Stocks",
                    section = sym,
                    field   = ", ".join(sorted(changed.keys())),
                    old_val = json.dumps(changed),    # original values of changed fields
                    new_val = json.dumps(new_entry),  # FULL latest entry (not just deltas)
                    applied = True,
                ))
    return rows


def _diff_portfolios(old_p: dict, new_p: dict) -> list[dict]:
    """
    Compare Portfolios sheet against existing config.json Portfolios section.
    Same row-per-name-per-day strategy as stocks/indexes.
    """
    rows = []
    all_names = set(old_p) | set(new_p)
    for name in sorted(all_names):
        old_entry = old_p.get(name)
        new_entry = new_p.get(name)
        if old_entry is None:
            rows.append(_make_change_row(
                sheet   = "Portfolios",
                section = name,
                field   = "[ALL FIELDS]",
                old_val = "DOES NOT EXIST IN CONFIG",
                new_val = json.dumps(new_entry),
                applied = True,
            ))
        elif new_entry is None:
            rows.append(_make_change_row(
                sheet   = "Portfolios",
                section = name,
                field   = "[ALL FIELDS]",
                old_val = json.dumps(old_entry),
                new_val = "REMOVED FROM EXCEL — deleted from config",
                applied = True,
            ))
        else:
            all_fields = set(old_entry) | set(new_entry)
            changed = {}
            for f in sorted(all_fields):
                if old_entry.get(f) != new_entry.get(f):
                    changed[f] = old_entry.get(f)
            if changed:
                rows.append(_make_change_row(
                    sheet   = "Portfolios",
                    section = name,
                    field   = ", ".join(sorted(changed.keys())),
                    old_val = json.dumps(changed),
                    new_val = json.dumps(new_entry),
                    applied = True,
                ))
    return rows


def _diff_indexes(old_indexes: dict, new_indexes: dict) -> list[dict]:
    """
    Compare new Indexes sheet data against existing config.json Indexes section.
    """
    rows = []
    all_syms = set(old_indexes) | set(new_indexes)

    for sym in sorted(all_syms):
        old_entry = old_indexes.get(sym)
        new_entry = new_indexes.get(sym)

        if old_entry is None:
            rows.append(_make_change_row(
                sheet   = "Indexes",
                section = sym,
                field   = "enabled",
                old_val = "DOES NOT EXIST IN CONFIG",
                new_val = new_entry.get("enabled", ""),
                applied = True,
            ))

        elif new_entry is None:
            # ── FIX: Index removed from Excel → DELETE from config.json ──
            rows.append(_make_change_row(
                sheet   = "Indexes",
                section = sym,
                field   = "enabled",
                old_val = old_entry.get("enabled", ""),
                new_val = "REMOVED FROM EXCEL — deleted from config",
                applied = True,                    # ← was False before fix
            ))

        else:
            # Detect any field that changed on this index. Tracked fields
            # mirror the per-stock signal-evaluation params + per-index MACD
            # calculation overrides.
            tracked = ("enabled", "signal_l", "signal_s",
                       "macdle_blw_zline", "macdse_abv_zline",
                       "mean_multiplier_l", "mean_multiplier_s",
                       "macd_resolution", "macd_fast", "macd_slow",
                       "macd_signal_period", "macd_price_source")
            changed = {}
            for f in tracked:
                if old_entry.get(f) != new_entry.get(f):
                    changed[f] = {"old": old_entry.get(f), "new": new_entry.get(f)}
            if changed:
                fields_str = ", ".join(sorted(changed.keys()))
                old_vals   = {f: v["old"] for f, v in changed.items()}
                new_vals   = {f: v["new"] for f, v in changed.items()}
                rows.append(_make_change_row(
                    sheet   = "Indexes",
                    section = sym,
                    field   = fields_str,
                    old_val = json.dumps(old_vals),
                    new_val = json.dumps(new_vals),
                    applied = True,
                ))
    return rows


# =============================================================================
# MAIN SYNC FUNCTION
# =============================================================================

def sync_excel_to_json():
    """Read the Excel file, compute a granular diff, update config.json, write CSV log."""

    # ── 1. Load Excel ─────────────────────────────────────────────────────────
    try:
        wb = openpyxl.load_workbook(str(XLSX_FILE), data_only=True)
    except Exception as e:
        log.error("Cannot open Excel file: %s", e)
        return

    try:
        ws_global  = wb["Global Settings"]
        ws_stocks  = wb["Stocks"]
        # FIX: Accept both "Indexes" and "Index" sheet tab names
        if "Indexes" in wb.sheetnames:
            ws_indexes = wb["Indexes"]
        elif "Index" in wb.sheetnames:
            ws_indexes = wb["Index"]
            log.warning("Sheet named 'Index' (not 'Indexes') — reading it anyway.")
        else:
            raise KeyError("Indexes")
    except KeyError as e:
        log.error("Sheet not found in Excel: %s — is this the right file?", e)
        return

    # Portfolios sheet is OPTIONAL — bot still works without it. If absent,
    # stocks fall back to globals directly (existing behavior preserved).
    ws_portfolios = wb["Portfolios"] if "Portfolios" in wb.sheetnames else None

    new_globals    = read_global_settings(ws_global)
    new_stocks     = read_stocks(ws_stocks)
    new_indexes    = read_indexes(ws_indexes)
    new_portfolios = read_portfolios(ws_portfolios)

    # ── 2. Load existing config.json ──────────────────────────────────────────
    try:
        with open(JSON_FILE) as f:
            cfg = json.load(f)
    except Exception as e:
        log.error("Cannot read config.json: %s", e)
        return

    old_stocks     = cfg.get("Stocks",     {})
    old_indexes    = cfg.get("Indexes",    {})
    old_portfolios = cfg.get("Portfolios", {})

    # ── 3. Compute granular diffs ─────────────────────────────────────────────
    csv_rows = []
    csv_rows += _diff_global(cfg, new_globals)
    csv_rows += _diff_stocks(old_stocks, new_stocks)
    csv_rows += _diff_indexes(old_indexes, new_indexes)
    csv_rows += _diff_portfolios(old_portfolios, new_portfolios)

    # ── FIX: Detect index-level changes even when diff produces no CSV rows ──
    # Edge case: if Index enabled value is identical in Excel and config,
    # _diff_indexes returns [] → csv_rows empty → early return skips everything.
    # We detect index structural changes separately so the apply block always runs
    # when the Index sheet actually differs from config.
    _index_changed     = (new_indexes != old_indexes)
    _portfolios_changed = (new_portfolios != old_portfolios)

    if not csv_rows and not _index_changed and not _portfolios_changed:
        log.info("No changes detected — config.json unchanged")
        return

    if not csv_rows and (_index_changed or _portfolios_changed):
        log.info("Index/Portfolio changes detected without field-level diff — applying.")

    # ── 4. Apply changes to config ────────────────────────────────────────────

    # Apply global settings
    for k, v in new_globals.items():
        cfg[k] = v

    # FIX — Interval / MACD_Resolution seconds issue (Apr 2026):
    # When MACD_Resolution changes (e.g. "30s", "1min"), MACD_SecBarSize MUST
    # match or the SecondBarAggregator in indicators.py builds wrong-sized bars.
    # Previously the user had to update BOTH fields manually — easy to forget.
    # Now: if MACD_Resolution was set in Excel, auto-derive MACD_SecBarSize.
    import re as _re
    if "MACD_Resolution" in new_globals:
        _res = str(cfg.get("MACD_Resolution", "")).strip().lower()
        _sec = None
        _m = _re.match(r'^(\d+)s$', _res)
        if _m: _sec = int(_m.group(1))
        _m = _re.match(r'^(\d+)min$', _res)
        if _m: _sec = int(_m.group(1)) * 60
        _m = _re.match(r'^(\d+)$', _res)
        if _m: _sec = int(_m.group(1))
        if _sec is not None:
            _old_sec = cfg.get("MACD_SecBarSize")
            cfg["MACD_SecBarSize"] = _sec
            if _old_sec != _sec:
                log.info(
                    "Auto-set MACD_SecBarSize=%d from MACD_Resolution=%r "
                    "(was %s) — no manual update needed.",
                    _sec, cfg["MACD_Resolution"], _old_sec)

    # ── FIX: Full two-way sync for Stocks ─────────────────────────────────
    # Add / update stocks present in Excel
    for sym, entry in new_stocks.items():
        cfg["Stocks"][sym] = entry
    # Delete stocks that were removed from Excel
    for sym in list(cfg["Stocks"].keys()):
        if sym not in new_stocks:
            del cfg["Stocks"][sym]
            log.info("Removed stock from config.json: %s", sym)

    # ── FIX: Full two-way sync for Indexes ────────────────────────────────
    # Add / update indexes present in Excel
    for sym, entry in new_indexes.items():
        cfg["Indexes"][sym] = entry
    # Delete indexes that were removed from Excel
    for sym in list(cfg["Indexes"].keys()):
        if sym not in new_indexes:
            del cfg["Indexes"][sym]
            log.info("Removed index from config.json: %s", sym)

    # ── Portfolios full two-way sync (new section) ────────────────────────
    # If the Portfolios sheet is absent (older xlsx), new_portfolios is {} —
    # in that case leave the Portfolios section alone so prior portfolio
    # data isn't accidentally wiped if the sheet gets temporarily removed.
    if new_portfolios or ws_portfolios is not None:
        cfg.setdefault("Portfolios", {})
        for name, entry in new_portfolios.items():
            cfg["Portfolios"][name] = entry
        for name in list(cfg["Portfolios"].keys()):
            if name not in new_portfolios:
                del cfg["Portfolios"][name]
                log.info("Removed portfolio from config.json: %s", name)

    # ── 5. Write updated config.json atomically ───────────────────────────────
    success = False
    tmp = str(JSON_FILE) + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(cfg, f, indent=4)
        os.replace(tmp, JSON_FILE)
        success = True
        log.info("config.json updated  (%d change(s))", len(csv_rows))
    except Exception as e:
        log.error("Failed to write config.json: %s", e)
        # Mark all rows as not applied since write failed
        for row in csv_rows:
            row["Applied To Config"] = "NO — config write failed"

    # ── 6. Print summary to terminal ──────────────────────────────────────────
    for row in csv_rows:
        log.info(
            "  [%s] %s → %s.%s: %r → %r  [%s]",
            row["Sheet"],
            row["Symbol / Section"],
            row["Sheet"],
            row["Field"],
            row["Old Value"],
            row["New Value"],
            row["Applied To Config"],
        )

    # ── 7. Write CSV change log ───────────────────────────────────────────────
    _log_changes_to_csv(csv_rows)


# =============================================================================
# FILE WATCHER
# =============================================================================

class ExcelChangeHandler(FileSystemEventHandler):
    def __init__(self):
        self._last_sync = 0.0
        self._debounce  = 1.5   # seconds — Excel saves multiple events in a burst

    def on_modified(self, event):
        if event.is_directory:
            return
        if not str(event.src_path).endswith("strategy_config.xlsx"):
            return
        now = time.time()
        if now - self._last_sync < self._debounce:
            return
        self._last_sync = now
        log.info("Excel file changed — syncing to config.json ...")
        time.sleep(0.4)   # brief wait for Excel to finish writing
        sync_excel_to_json()

    # Some editors/OS fire on_created instead of on_modified on save
    on_created = on_modified


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    if not XLSX_FILE.exists():
        log.error("Excel file not found: %s", XLSX_FILE)
        log.error("Run build_excel.py first to generate it.")
        sys.exit(1)
    if not JSON_FILE.exists():
        log.error("config.json not found: %s", JSON_FILE)
        sys.exit(1)

    log.info("━" * 60)
    log.info("excel_sync  started")
    log.info("  Watching  : %s", XLSX_FILE)
    log.info("  Target    : %s", JSON_FILE)
    log.info("  Change log: changes_DD_MM_YYYY.csv  (daily, EST date)")
    log.info("  Edit the Excel file and save — changes apply instantly.")
    log.info("  Every field change is logged to the daily CSV.")
    log.info("  New stocks in Excel are ADDED to config.json automatically.")
    log.info("  Stocks removed from Excel are DELETED from config.json.")
    log.info("  Indexes removed from Excel are DELETED from config.json.")
    log.info("━" * 60)

    # Initial sync on startup
    sync_excel_to_json()

    observer = Observer()
    observer.schedule(ExcelChangeHandler(), str(FOLDER), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        log.info("excel_sync stopped.")
    observer.join()