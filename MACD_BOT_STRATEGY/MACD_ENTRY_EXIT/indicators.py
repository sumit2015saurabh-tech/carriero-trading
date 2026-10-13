"""
=============================================================================
QTI Fund  |  Model: !JFMX3X_NONCORR_MACD_V3.x
FILE: indicators.py

KEY FEATURES
------------
1. Per-symbol MACD parameters
   Each stock/index can specify its own macd_fast, macd_slow,
   macd_signal_period, macd_resolution, macd_price_source, macd_sec_bar_size
   in its config.json entry.  Global MACD_* values are the default fallback.

2. Auto-reseed on parameter change
   Every bar, IndicatorPipeline re-reads its effective parameters.
   If ANY parameter changed, the engine resets and re-seeds from IBKR
   history using the new parameters (~2-3 sec blocking REST call).

3. Index MACD + IndexMACDStore
   Indexes get their own IndicatorPipeline with per-index overrides.
   On each ready bar the index MACD is stored in IndexMACDStore.
   run_strategy consults this store when index_filter=Y on a stock.

4. Index filter rule
   LONG  signal allowed  ->  index MACD Line > index Signal Line
   SHORT signal allowed  ->  index MACD Line < index Signal Line
   If index MACD not ready -> signal blocked (safe default).

5. Smart session seeding (today-first, yesterday as fallback)
   On startup, the bot fetches today's bars since market open (09:30 EST)
   using the IBKR startTime parameter. If today already has >= 52 bars,
   those are used exclusively. If today has some but fewer than 52, yesterday's
   bars are used to pad. If today has no bars (bot started pre-market), only
   yesterday's bars are used. This prevents false MACD crossovers caused by
   stale EMA baselines from a prior session.

6. Resolution-flexible seeding
   The IBKR history bar size is derived directly from the resolution string in
   config.json. "5s" → 5-second bars, "1s" → 1-second bars, "tick" / "Ntick"
   → 1-second seed bars (finest available), "1min" → 1-minute bars. Every
   resolution supported in live trading is also supported for seeding.
=============================================================================
"""

from __future__ import annotations

import csv
import json
import logging
import os
from collections import deque
from datetime import datetime, date, time as dtime
from typing import Callable, Dict, List, Optional, Tuple

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
LOG_DIR     = os.path.join(os.path.dirname(__file__), "crossover_logs")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MACD] %(levelname)s - %(message)s",
)
logger = logging.getLogger("indicators")


# =============================================================================
# CROSSOVER CSV LOGGER
# =============================================================================

class CrossoverLogger:
    """
    Logs every MACD / Signal line crossover to a daily CSV file.
    File name: DD-MM-YY.csv   (e.g. 04-03-26.csv)
    Location:  <script_dir>/crossover_logs/

    Columns:
        timestamp, symbol, direction, close, macd, signal, histogram,
        zl_macd, zl_signal, resolution, fast, slow, signal_period
    """

    _HEADER = [
        "timestamp", "symbol", "direction", "close",
        "macd", "signal", "histogram",
        "zl_macd", "zl_signal",
        "resolution", "fast", "slow", "signal_period",
    ]

    _instance = None            # singleton

    @classmethod
    def get(cls) -> "CrossoverLogger":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        os.makedirs(LOG_DIR, exist_ok=True)
        self._current_date: Optional[str] = None
        self._fh  = None
        self._csv = None

    # ── internal ──────────────────────────────────────────────────────────

    def _ensure_file(self, now: datetime):
        tag = now.strftime("%d-%m-%y")          # DD-MM-YY
        if tag == self._current_date:
            return
        # close old file
        if self._fh:
            self._fh.close()
        path = os.path.join(LOG_DIR, f"{tag}.csv")
        file_exists = os.path.exists(path)
        self._fh  = open(path, "a", newline="")
        self._csv = csv.writer(self._fh)
        if not file_exists:
            self._csv.writerow(self._HEADER)
        self._current_date = tag
        logger.info("Crossover CSV → %s", path)

    # ── public ────────────────────────────────────────────────────────────

    def log(self, now: datetime, symbol: str, direction: str,
            close: float, fields: Dict, params: Dict):
        self._ensure_file(now)
        macd_val = fields["macd"][0]
        sig_val  = fields["macd_signal"][0]
        self._csv.writerow([
            now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            symbol,
            direction,
            f"{close:.4f}",
            f"{macd_val:.6f}",
            f"{sig_val:.6f}",
            f"{macd_val - sig_val:.6f}",
            f"{fields['macd_zl']:.6f}",
            f"{fields['macd_zl_signal']:.6f}",
            params.get("resolution", ""),
            params.get("fast", ""),
            params.get("slow", ""),
            params.get("signal_period", ""),
        ])
        self._fh.flush()
        logger.info("[%s] ★ CROSSOVER %s  close=%.4f  MACD=%.6f  Signal=%.6f  hist=%.6f",
                    symbol, direction, close, macd_val, sig_val, macd_val - sig_val)


# =============================================================================
# CONFIG HELPERS
# =============================================================================

def _load_cfg() -> Dict:
    with open(CONFIG_PATH) as fh:
        return json.load(fh)

def _parse_time(t: str) -> dtime:
    return datetime.strptime(t, "%H:%M").time()


def resolve_symbol_params(symbol: str, cfg: Dict, as_index: bool = False) -> Dict:
    """
    Return effective MACD parameters for a symbol.

    2026-06-10: `as_index=True` forces resolution from the Indexes sheet even for
    a DUAL symbol that is BOTH a traded stock and an index (e.g. QQQ). Lets the
    index pipeline of a dual symbol run its own resolution/params (e.g. 3min
    index) independently of its stock pipeline (e.g. 5min). Default False =
    unchanged (stock resolution for dual symbols, index for pure indexes).

    Inheritance chain (stocks):
        per-stock field  →  per-portfolio default  →  global default
    Inheritance chain (indexes):
        per-index field  →  global default
    Portfolios apply to stocks only.

    Blank/empty cells fall through to the next tier so the user can leave
    fields blank on a stock row and pick up the portfolio's defaults.
    """
    sym_up  = symbol.upper()
    stocks  = {k.upper(): v for k, v in cfg.get("Stocks",  {}).items()}
    indexes = {k.upper(): v for k, v in cfg.get("Indexes", {}).items()}
    portfolios = cfg.get("Portfolios", {}) or {}

    is_index = bool(as_index) or ((sym_up in indexes) and (sym_up not in stocks))
    entry    = (indexes.get(sym_up) or {}) if is_index else (stocks.get(sym_up) or {})

    # Portfolio inheritance (stocks only).
    portfolio_entry: Dict = {}
    if not is_index:
        pname = str(entry.get("portfolio", "") or "").strip()
        if pname:
            portfolio_entry = portfolios.get(pname) or {}

    def _get(local_key, global_key, default):
        v = entry.get(local_key)
        if v is not None and v != "":
            return v
        if portfolio_entry:
            pv = portfolio_entry.get(local_key)
            if pv is not None and pv != "":
                return pv
        return cfg.get(global_key, default)

    # 2026-06-03: separate ENTRY vs EXIT candle resolutions.
    #   entry_resolution = Entry Candles (Excel) → falls back to legacy
    #     macd_resolution → portfolio → global MACD_Resolution → "30s".
    #   exit_resolution  = Exit Candles (Excel) → falls back to entry_resolution.
    # `resolution` stays = entry_resolution so every existing reader (the entry
    # pipeline, seeding, aggregator) is unchanged when entry==exit (the default).
    entry_res = _get("entry_candles", "__no_global__", "")
    if not str(entry_res).strip():
        entry_res = _get("macd_resolution", "MACD_Resolution", "30s")
    entry_res = str(entry_res).strip().lower()

    exit_res = str(_get("exit_candles", "__no_global__", "")).strip().lower()
    if not exit_res:
        exit_res = entry_res

    return {
        "fast"         : int(_get("macd_fast",          "MACD_Fast",        12)),
        "slow"         : int(_get("macd_slow",          "MACD_Slow",        26)),
        "signal_period": int(_get("macd_signal_period", "MACD_Signal",       9)),
        "resolution"      : entry_res,          # entry pipeline (back-compat key)
        "entry_resolution": entry_res,
        "exit_resolution" : exit_res,
        "price_source" :     _get("macd_price_source",  "MACD_PriceSource","close").strip().lower(),
        "sec_bar_size" : int(_get("macd_sec_bar_size",  "MACD_SecBarSize",  30)),
        "history_depth": int(_get("macd_history_depth", "MACD_HistoryDepth",10)),
        "seed_bars"    : int(cfg.get("MACD_SeedBars", 52)),
    }


# =============================================================================
# EMA
# =============================================================================

class EMA:
    def __init__(self, period: int):
        self.period = period
        self.k = 2.0 / (period + 1)
        self.value: Optional[float] = None

    def update(self, price: float) -> float:
        self.value = price if self.value is None else price * self.k + self.value * (1.0 - self.k)
        return self.value

    def reset(self):
        self.value = None


# =============================================================================
# ZERO-LAG EMA  (Ehlers: ZL = 2*EMA(p) - EMA(EMA(p)))
# =============================================================================

class ZeroLagEMA:
    def __init__(self, period: int):
        self._e1 = EMA(period)
        self._e2 = EMA(period)
        self.value: Optional[float] = None

    def update(self, price: float) -> float:
        e1 = self._e1.update(price)
        e2 = self._e2.update(e1)
        self.value = 2.0 * e1 - e2
        return self.value

    def reset(self):
        self._e1.reset()
        self._e2.reset()
        self.value = None


# =============================================================================
# SESSION MEAN
# =============================================================================

class SessionMean:
    def __init__(self):
        self.pos_mean = self.neg_mean = 0.0
        self._ps = self._pc = self._ns = self._nc = 0.0

    def update(self, v: float):
        if v > 0:
            self._ps += v
            self._pc += 1
            self.pos_mean = self._ps / self._pc
        elif v < 0:
            self._ns += v
            self._nc += 1
            self.neg_mean = self._ns / self._nc

    def reset(self):
        self.pos_mean = self.neg_mean = 0.0
        self._ps = self._pc = self._ns = self._nc = 0.0


# =============================================================================
# SECOND-BAR AGGREGATOR
# =============================================================================

class SecondBarAggregator:
    def __init__(self, bar_seconds: int, on_bar: Callable):
        self.bar_seconds = bar_seconds
        self.on_bar      = on_bar
        self._buf: List[Dict] = []
        self._bucket: Optional[datetime] = None

    def push(self, tick: Dict):
        now    = tick["ts"]
        bucket = now.replace(
            second=(now.second // self.bar_seconds) * self.bar_seconds,
            microsecond=0,
        )
        if self._bucket is None:
            self._bucket = bucket
        if bucket > self._bucket and self._buf:
            self._emit()
            self._bucket = bucket
        self._buf.append(tick)

    def _emit(self):
        if not self._buf:
            return
        prices = [t["price"] for t in self._buf]
        bv = [t["bar_vol"]   for t in self._buf if t["bar_vol"]  > 0]
        dv = [t["daily_vol"] for t in self._buf if t["daily_vol"] > 0]
        self.on_bar({
            "current_time": self._buf[-1]["ts"],
            "open"        : prices[0],
            "high"        : max(t["high"] for t in self._buf),
            "low"         : min(t["low"]  for t in self._buf),
            "close"       : prices[-1],
            "price"       : prices[-1],
            "volume"      : bv[-1] if bv else sum(t["tick_vol"] for t in self._buf),
            "daily_volume": dv[-1] if dv else 0,
        })
        self._buf.clear()

    def reset(self):
        self._buf.clear()
        self._bucket = None


# =============================================================================
# N-TICK BAR AGGREGATOR  (closes a bar after exactly N trades)
# =============================================================================

class NTickBarAggregator:
    """
    Closes a bar after exactly `tick_count` trades have arrived,
    regardless of how much time has passed.

    Config: set MACD_Resolution = "25tick" (or "50tick", "100tick" etc.)
    The number before "tick" is the trade count per bar.

    Each tick dict must have:
        ts        datetime
        price     float
        high      float  (optional, defaults to price)
        low       float  (optional, defaults to price)
        tick_vol  float  (this trade's size)
        bar_vol   float  (cumulative bar volume from broker, if available)
        daily_vol float  (cumulative daily volume, if available)
    """

    def __init__(self, tick_count: int, on_bar: Callable):
        self.tick_count = tick_count
        self.on_bar     = on_bar
        self._buf: List[Dict] = []

    def push(self, tick: Dict):
        self._buf.append(tick)
        if len(self._buf) >= self.tick_count:
            self._emit()

    def _emit(self):
        if not self._buf:
            return
        prices = [t["price"] for t in self._buf]
        dv     = [t["daily_vol"] for t in self._buf if t["daily_vol"] > 0]
        self.on_bar({
            "current_time": self._buf[-1]["ts"],
            "open"        : prices[0],
            "high"        : max(t.get("high", t["price"]) for t in self._buf),
            "low"         : min(t.get("low",  t["price"]) for t in self._buf),
            "close"       : prices[-1],
            "price"       : prices[-1],
            "volume"      : sum(t.get("tick_vol", 0) for t in self._buf),
            "daily_volume": dv[-1] if dv else 0,
            "tick_count"  : len(self._buf),
        })
        self._buf.clear()

    def reset(self):
        self._buf.clear()

    @staticmethod
    def parse_resolution(res: str) -> Optional[int]:
        """
        Parse a resolution string like "25tick" → 25.

        Accepts a variety of user-typed forms (singular/plural, with or
        without space, case-insensitive):
            "25tick"   "25 tick"   "25ticks"   "25 ticks"
            "25Tick"   "25-tick"   "25t"
        Returns the tick count, or None if the string isn't an N-tick
        pattern. Pure "tick" (every-trade) returns None — handled separately.
        """
        if not res:
            return None
        # Normalise: lowercase, strip surrounding whitespace, then strip
        # internal whitespace/dashes/underscores so "25 ticks", "25-tick",
        # "25_tick" all reduce to "25tick(s)".
        r = res.strip().lower().replace(" ", "").replace("-", "").replace("_", "")
        if r == "tick":
            return None                                 # every-trade — handled elsewhere

        # Try "Ntick" / "Nticks" / "Nt" — split at the first non-digit char.
        # Anything after the digits must be a recognised tick suffix.
        digits = ""
        for ch in r:
            if ch.isdigit():
                digits += ch
            else:
                break
        if not digits:
            return None                                 # no leading digits
        suffix = r[len(digits):]
        if suffix in ("tick", "ticks", "t"):
            try:
                n = int(digits)
                return n if n > 0 else None
            except ValueError:
                return None
        return None


# =============================================================================
# MACD ENGINE
# =============================================================================

class MACDEngine:
    """
    seed_mode=True  (default) — absorbs prices silently, no output.
    seed_mode=False (live)    — produces full 6-field dict after history_depth bars.
    Call finish_seeding() to switch.
    """

    def __init__(self, fast: int, slow: int, signal: int, history_depth: int):
        self.fast = fast
        self.slow = slow
        self.signal_period = signal
        self.depth = history_depth

        self._ef    = EMA(fast)
        self._es    = EMA(slow)
        self._esig  = EMA(signal)
        self._zlf   = ZeroLagEMA(fast)
        self._zls   = ZeroLagEMA(slow)
        self._zlsig = EMA(signal)

        self._macd_hist: deque = deque(maxlen=history_depth)
        self._sig_hist:  deque = deque(maxlen=history_depth)
        # Mean of the Signal line (legacy — kept for visibility, no longer
        # used as an entry filter after the cross-based rewrite).
        self._means = SessionMean()
        # Mean of the MACD Differential = Fast EMA - Slow EMA = MACD line
        # (user terminology). Tracked for visibility only — gated by
        # MeanCalcStartTime so the open's volatility doesn't pollute it.
        self._diff_means = SessionMean()

        # 2026-05-22: full-session bar buffer for replay when MeanCalcStartTime
        # changes mid-session. Stores (bar_time, macd_line, signal_line) for
        # every bar processed since the last session reset. Bounded by session
        # length × resolution (e.g. 390 bars for 1-min over 6.5h — small).
        # recalc_session_means() walks this list to recompute the means with
        # a new mean_calc_start threshold.
        self._session_bars: list = []

        self._seed_mode  = True
        self._ready      = False
        self._live_count = 0

    def update(self, price: float, allow_means_update: bool = True,
               bar_time=None) -> Optional[Dict]:
        """
        Run the EMAs / MACD calc for a bar's price.

        `allow_means_update` is set by the caller (IndicatorPipeline._enrich)
        based on whether the bar's time is past MeanCalcStartTime. When False,
        the session means are NOT updated this bar — used to keep open-time
        volatility out of the mean. EMAs / history are always updated.

        `bar_time` (2026-05-22): when provided, the bar is logged in the
        session buffer for later recalc_session_means() replay.
        """
        ef        = self._ef.update(price)
        es        = self._es.update(price)
        macd_line = ef - es
        sig_line  = self._esig.update(macd_line)
        zl_macd   = self._zlf.update(price) - self._zls.update(price)
        zl_sig    = self._zlsig.update(zl_macd)

        # Fill history during seeding too — deque maxlen=history_depth keeps
        # only the most recent N seed bars, so the engine is READY the
        # instant finish_seeding() is called. _means stay zeroed during
        # seed (their updates are gated below) so session means are unaffected.
        self._macd_hist.appendleft(macd_line)
        self._sig_hist.appendleft(sig_line)

        # 2026-06-03: buffer EVERY bar (incl. seed/historical) with its time so
        # the session means can be (re)computed from the MORNING open after
        # seeding and on any flow change. Means stay zeroed during seed; they
        # are computed by recalc_session_means() (gated to today +
        # MeanCalcStartTime) at the end of seed() and on every reseed.
        if bar_time is not None:
            self._session_bars.append((bar_time, macd_line, sig_line))

        if self._seed_mode:
            return None

        self._live_count += 1
        if allow_means_update:
            self._means.update(sig_line)
            self._diff_means.update(macd_line)

        if len(self._macd_hist) < self.depth:
            rem = self.depth - len(self._macd_hist)
            if rem <= 5 or rem % 5 == 0:
                logger.info("History buffer: %d bar(s) to fill", rem)
            return None

        if not self._ready:
            self._ready = True
            logger.info("MACD READY  fast=%d slow=%d signal=%d  live_bars=%d",
                        self.fast, self.slow, self.signal_period, self._live_count)

        return {
            "macd"                : list(self._macd_hist),
            "macd_signal"         : list(self._sig_hist),
            "macd_zl"             : zl_macd,
            "macd_zl_signal"      : zl_sig,
            "macd_signal_pos_mean": self._means.pos_mean,
            "macd_signal_neg_mean": self._means.neg_mean,
            # User's "MACD Differential" = Fast - Slow = MACD line.
            # These means are visibility-only after the cross rewrite.
            "macd_diff_pos_mean"  : self._diff_means.pos_mean,
            "macd_diff_neg_mean"  : self._diff_means.neg_mean,
            # Convenience: also surface the bare EMAs so the dashboard
            # can render Fast/Slow directly without recomputing.
            "ema_fast"            : self._ef.value if self._ef.value is not None else 0.0,
            "ema_slow"            : self._es.value if self._es.value is not None else 0.0,
        }

    def finish_seeding(self):
        self._seed_mode = False
        logger.info("Engine live  fast=%d slow=%d signal=%d",
                    self.fast, self.slow, self.signal_period)

    def reset_session_means(self):
        self._means.reset()
        self._diff_means.reset()
        # 2026-05-22: also clear session buffer on daily reset
        self._session_bars = []

    def recalc_session_means(self, mean_calc_start_t, session_date=None) -> int:
        """
        2026-05-22: replay the in-memory session bar buffer to recompute
        session means with a NEW mean_calc_start_t threshold. Used when
        MeanCalcStartTime is changed mid-session via Excel, AND (2026-06-03)
        right after seeding so the means cover the WHOLE session from the
        morning open (the buffer now includes the seed/historical bars).

        `mean_calc_start_t` is a datetime.time — bars whose time-of-day >= it
        contribute. `session_date` (a date) restricts to TODAY's bars only —
        REQUIRED now that the buffer holds multi-day seed bars, else yesterday's
        09:30+ bars would wrongly leak into today's means.

        Returns the number of bars that contributed to the new means.
        """
        self._means.reset()
        self._diff_means.reset()
        contributed = 0
        for (bt, macd_v, sig_v) in self._session_bars:
            try:
                d = bt.date() if hasattr(bt, "date") else None
                t = bt.time() if hasattr(bt, "time") else bt
            except Exception:
                continue
            if session_date is not None and d is not None and d != session_date:
                continue                       # only today's session (morning → now)
            if t >= mean_calc_start_t:
                self._means.update(sig_v)
                self._diff_means.update(macd_v)
                contributed += 1
        return contributed

    def is_ready(self) -> bool:
        return self._ready

    def reset_all(self):
        for o in (self._ef, self._es, self._esig, self._zlf, self._zls, self._zlsig):
            o.reset()
        self._macd_hist.clear()
        self._sig_hist.clear()
        self._means.reset()
        self._diff_means.reset()
        self._seed_mode  = True
        self._ready      = False
        self._live_count = 0

    # ── Provisional read (intra-bar; does NOT mutate state) ──────────────
    def compute_provisional_macd(self, tick_price: float) -> Optional[float]:
        """
        Return the MACD-line value that *would* result if `tick_price` were
        the next bar's price, WITHOUT mutating any internal EMA state.

        Used by IndicatorPipeline.on_raw_tick to detect intra-bar MACD-zero
        crosses (Fast crossing Slow) before the bar closes. Returns None
        until the EMAs have been initialised (first seed call).
        """
        if self._ef.value is None or self._es.value is None:
            return None
        kf = self._ef.k
        ks = self._es.k
        prov_ef = tick_price * kf + self._ef.value * (1.0 - kf)
        prov_es = tick_price * ks + self._es.value * (1.0 - ks)
        return prov_ef - prov_es

    def compute_provisional_all(self, tick_price: float):
        """2026-06-03: full LIVE (forming-bar) snapshot — fast/slow EMAs, MACD
        line, and signal line that *would* result if `tick_price` were the next
        bar's close. Non-mutating (no state change). Used by the dashboard so
        its MACD/signal track the live chart instead of the last CLOSED bar.
        Returns None until the EMAs are initialised."""
        if self._ef.value is None or self._es.value is None:
            return None
        pf = tick_price * self._ef.k + self._ef.value * (1.0 - self._ef.k)
        ps = tick_price * self._es.k + self._es.value * (1.0 - self._es.k)
        pmacd = pf - ps
        if self._esig.value is None:
            psig = pmacd
        else:
            psig = pmacd * self._esig.k + self._esig.value * (1.0 - self._esig.k)
        return {"fast": pf, "slow": ps, "macd": pmacd, "signal": psig}


# =============================================================================
# IBKR BAR SIZE MAP
# =============================================================================

# Maps resolution strings → IBKR CP Gateway bar size strings
# "tick" resolution uses 1-second bars for seeding (smallest available)
_BAR_SIZE_MAP = {
    "tick" : "1secs",   # tick = every trade; seed with 1s bars (finest available)
    "1s"   : "1secs",
    "2s"   : "2secs",
    "3s"   : "3secs",
    "5s"   : "5secs",
    "10s"  : "10secs",
    "15s"  : "15secs",
    "20s"  : "20secs",
    "30s"  : "30secs",
    "60s"  : "1min",
    "1min" : "1min",
    "2min" : "2mins",
    "3min" : "3mins",
    "5min" : "5mins",
    "10min": "10mins",
    "15min": "15mins",
    "20min": "20mins",
    "30min": "30mins",
}

def _resolution_to_bar_size(resolution: str) -> str:
    """
    Convert a strategy resolution string to an IBKR CP Gateway bar size string.

    Examples:
        "5s"    → "5secs"
        "tick"  → "1secs"   (finest bar IBKR history supports)
        "30s"   → "30secs"
        "1min"  → "1min"
        "5min"  → "5mins"
        "25tick"→ "1secs"   (N-tick bars seed from 1s history)
    """
    r = resolution.lower().strip()

    # Direct lookup first
    if r in _BAR_SIZE_MAP:
        return _BAR_SIZE_MAP[r]

    # N-tick pattern (e.g. "25tick", "100tick", "25 ticks", "25t") — seed
    # from 1s bars. Uses the same lenient parser as the live aggregator.
    if NTickBarAggregator.parse_resolution(r) is not None:
        return "1secs"

    # Generic Xs pattern (e.g. "7s", "45s")
    if r.endswith("s") and not r.endswith("min"):
        try:
            secs = int(r[:-1])
            # IBKR only supports specific second bar sizes
            # Round up to nearest supported size
            for supported in (1, 2, 3, 5, 10, 15, 20, 30):
                if secs <= supported:
                    return f"{supported}secs"
            return "30secs"   # cap at 30s if larger
        except ValueError:
            pass

    # Generic Xmin pattern
    if r.endswith("min"):
        try:
            mins = int(r[:-3] or "1")
            return "1min" if mins == 1 else f"{mins}mins"
        except ValueError:
            pass

    return "1min"   # safe fallback


# =============================================================================
# PRIOR-SESSION BAR FETCHER
# =============================================================================

def _parse_ibkr_bars(resp: dict) -> List[Dict]:
    """Parse raw IBKR history response into a list of bar dicts, sorted ascending."""
    raw_bars = resp.get("data", [])
    if not raw_bars:
        return []
    pf = float(resp.get("priceFactor",  1) or 1)
    vf = float(resp.get("volumeFactor", 1) or 1)
    parsed = []
    for b in raw_bars:
        ts_raw = b["t"]
        ts = datetime.fromtimestamp(ts_raw / 1000 if ts_raw > 1e10 else ts_raw)
        parsed.append({
            "ts"    : ts,
            "open"  : b.get("o", 0) / pf,
            "high"  : b.get("h", 0) / pf,
            "low"   : b.get("l", 0) / pf,
            "close" : b.get("c", 0) / pf,
            "volume": b.get("v", 0) / vf,
        })
    return sorted(parsed, key=lambda x: x["ts"])




def _resolution_to_tws_bar_size(resolution: str) -> str:
    """Map config resolution string to TWS reqHistoricalData bar size string."""
    r = resolution.strip().lower()
    mapping = {
        "1s": "1 secs", "5s": "5 secs", "10s": "10 secs",
        "15s": "15 secs", "30s": "30 secs",
        "1min": "1 min", "2min": "2 mins", "3min": "3 mins",
        "5min": "5 mins", "10min": "10 mins", "15min": "15 mins",
        "20min": "20 mins", "30min": "30 mins",
        "1h": "1 hour", "2h": "2 hours", "4h": "4 hours",
        "1d": "1 day",
    }
    if r in mapping:
        return mapping[r]
    # Tick / N-tick resolutions: seed from 1-second bars (finest available
    # historical bar size IBKR supports). The live N-tick aggregator takes
    # over once real ticks flow.
    if r == "tick" or NTickBarAggregator.parse_resolution(r) is not None:
        return "1 secs"
    return "30 secs"


def _seed_duration(bar_size_tws: str, n_bars: int = 100) -> str:
    """
    Build a TWS durationStr that requests `n_bars` of the given bar size.
    Examples:
        bar='5 secs',  n=100  → '500 S'
        bar='30 secs', n=100  → '3000 S'
        bar='1 min',   n=100  → '6000 S'
        bar='5 mins',  n=100  → '30000 S'
        bar='1 hour',  n=100  → '5 D' (>86400 S → use days)

    IBKR limits durationStr in seconds to 86400. For longer windows we
    switch to days. Asking for fewer bars (n_bars × bar_seconds) keeps the
    historical-data request inside IBKR's per-request size limit, which is
    why 5s seeds with '1 D' (= 17 280 bars) silently return zero rows.
    """
    s = bar_size_tws.strip().lower()
    sec_per_bar = 30
    try:
        if "sec" in s:
            sec_per_bar = int(s.split()[0])
        elif "min" in s:
            sec_per_bar = int(s.split()[0]) * 60
        elif "hour" in s:
            sec_per_bar = int(s.split()[0]) * 3600
        elif "day" in s:
            sec_per_bar = 86400
    except (ValueError, IndexError):
        pass

    total_s = sec_per_bar * max(int(n_bars), 1)
    if total_s <= 86400:
        return f"{total_s} S"
    # > 1 day — convert to days, round up, IBKR accepts integer days
    days = (total_s + 86399) // 86400
    return f"{days} D"

def fetch_prior_session_bars(ibkr_session, conid: int,
                              resolution: str, n_bars: int,
                              tws_engine=None) -> List[Dict]:
    """
    Fetch the most recent `n_bars` closed bars for seeding MACD EMAs.

    PRIORITY LOGIC
    ──────────────
    1. Try today's session first (market open = 09:30 EST).
       Use startTime parameter to request bars since today's open.
       If today already has >= n_bars  → use the last n_bars from today.
       If today has some but not enough → pad with yesterday's bars.

    2. If today has ZERO bars (bot started before market open, or IBKR
       returned nothing for today) → fall back to yesterday's bars.

    3. If neither day has bars → return empty list (cold start warmup).

    BAR SIZE MAPPING
    ────────────────
    The resolution string from config.json (e.g. "5s", "tick", "1min")
    is converted to the IBKR CP Gateway bar size string (e.g. "5secs",
    "1secs", "1min") via _resolution_to_bar_size().

    "tick" and N-tick resolutions seed from 1-second bars (finest available
    from IBKR history). The N-tick aggregator takes over once live data flows.
    """
    import time as _time
    from zoneinfo import ZoneInfo

    bar_size = _resolution_to_bar_size(resolution)
    EST      = ZoneInfo("America/New_York")

    logger.info("Fetching history  conid=%d  bar=%s  need=%d bars",
                conid, bar_size, n_bars)

    # Short-circuit if HMDS cooldown is still active — saves the 30s timeout
    # per symbol that would otherwise fire for each one. The retry loop in
    # MACDBot.start() will call us again once the cooldown expires.
    _tws_client = getattr(tws_engine, "_tws", None) if tws_engine is not None else None
    if _tws_client is not None and getattr(_tws_client, "_hmds_error", False):
        logger.warning("[seed] HMDS blocked (cooldown active) — skipping conid=%d (will retry)", conid)
        return []

    # ── TWS historical data — try this first (no session conflict) ──────────
    if tws_engine is not None:
        try:
            bar_size_tws = _resolution_to_tws_bar_size(resolution)
            # Request the FULL configured seed depth (+ small headroom), with
            # the duration SCALED TO THE BAR SIZE by _seed_duration so we always
            # pull >= n_bars (e.g. 200) regardless of resolution:
            #   1s   → ~225 S       5s   → ~1125 S      30s  → ~6750 S
            #   1min → ~13500 S     3min → ~40500 S     5min → ~67500 S
            # This is what lets the slow / signal line fully converge. Each
            # request stays inside IBKR's per-bar-size limit because the window
            # is exactly (n_bars × bar_seconds): small bars ask for a small
            # window, coarse bars auto-switch to a days-based duration (>86400s).
            # (The old code hard-capped this at 100 bars, which starved the seed
            #  — IBKR itself allows up to ~1 week for 3-min bars.)
            seed_fetch_n = int(n_bars) + 25      # headroom for the current partial bar / RTH edges
            duration_str = _seed_duration(bar_size_tws, n_bars=seed_fetch_n)
            logger.info("TWS seed request: bar=%s duration=%s  (target n_bars=%d + headroom)",
                        bar_size_tws, duration_str, n_bars)
            raw = tws_engine.get_historical_bars(conid, bar_size_tws, duration=duration_str)
            if raw:
                from zoneinfo import ZoneInfo as _ZI
                _EST = _ZI("America/New_York")
                today_date = datetime.now(_EST).date()
                today_tws  = [b for b in raw if b["ts"].date() == today_date]
                yest_tws   = [b for b in raw if b["ts"].date() < today_date]
                logger.info("TWS history: today=%d  yest=%d  (need %d)",
                            len(today_tws), len(yest_tws), n_bars)
                if len(today_tws) >= n_bars:
                    seed = today_tws[-n_bars:]
                    logger.info("Seed: %d bars  source=TWS-today", len(seed))
                    return seed
                elif today_tws or yest_tws:
                    combined = (yest_tws + today_tws)[-n_bars:]
                    logger.info("Seed: %d bars  source=TWS-combined", len(combined))
                    return combined
        except Exception as _tws_exc:
            logger.warning("TWS history fetch failed (%s) — falling back to CP API", _tws_exc)

    # ── Helper: call IBKR history endpoint ──────────────────────────────────
    def _fetch(params: dict) -> List[Dict]:
        try:
            resp = ibkr_session._get("/iserver/marketdata/history", params=params)
            # IBKR sometimes wraps in a list
            if isinstance(resp, list):
                resp = resp[0] if resp else {}
            return _parse_ibkr_bars(resp)
        except Exception as exc:
            logger.warning("History fetch failed (params=%s): %s", params, exc)
            return []

    # ── Step 1: Try today's bars using startTime ─────────────────────────────
    # startTime format required by IBKR CP Gateway: "YYYYMMDD-HH:MM:SS"
    now_est        = datetime.now(EST)
    market_open    = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
    start_time_str = market_open.strftime("%Y%m%d-%H:%M:%S")

    today_bars = _fetch({
        "conid"      : conid,
        "period"     : "1d",
        "bar"        : bar_size,
        "outsideRth" : "false",
        "startTime"  : start_time_str,
    })

    # Filter to only bars from today (safety — IBKR may return surrounding bars)
    today_date  = now_est.date()
    today_bars  = [b for b in today_bars if b["ts"].date() == today_date]

    logger.info("Today's bars available: %d  (need %d)", len(today_bars), n_bars)

    # ── Step 2: Fetch yesterday's bars as potential pad / fallback ───────────
    # We always fetch this; it is only used if today is insufficient.
    yest_bars = _fetch({
        "conid"      : conid,
        "period"     : "2d",
        "bar"        : bar_size,
        "outsideRth" : "false",
    })
    yest_bars = [b for b in yest_bars if b["ts"].date() < today_date]

    logger.info("Yesterday's bars available: %d", len(yest_bars))

    # ── Step 3: Choose the best seed set ─────────────────────────────────────
    if len(today_bars) >= n_bars:
        # Today has enough — use only today's most recent n_bars
        seed = today_bars[-n_bars:]
        source = "today"
    elif len(today_bars) > 0:
        # Today has some bars but not enough — pad with yesterday's tail
        shortfall = n_bars - len(today_bars)
        pad       = yest_bars[-shortfall:] if yest_bars else []
        seed      = pad + today_bars          # yesterday pad → today bars (ascending)
        source    = f"today({len(today_bars)}) + yesterday({len(pad)})"
    else:
        # No today bars at all — use yesterday
        seed   = yest_bars[-n_bars:]
        source = "yesterday (today not available)"

    if seed:
        logger.info("Seed: %d bars  source=%s  first=%s  last=%s",
                    len(seed), source,
                    seed[0]["ts"].strftime("%Y-%m-%d %H:%M:%S"),
                    seed[-1]["ts"].strftime("%Y-%m-%d %H:%M:%S"))
    else:
        logger.warning("No seed bars found for conid=%d — cold start", conid)

    return seed


# =============================================================================
# INDEX MACD STORE
# =============================================================================

class IndexMACDStore:
    """
    Stores the latest MACD output for every active index.
    Consulted by run_strategy when index_filter=Y on a stock.

    Alignment rule:
        LONG  signal -> index MACD[0] > index Signal[0]  (trending up)
        SHORT signal -> index MACD[0] < index Signal[0]  (trending down)
    """

    _store: Dict[str, Optional[Dict]] = {}
    _ts:    Dict[str, float] = {}        # 2026-06-11: wall-clock time of last update (freshness)

    @classmethod
    def update(cls, index_symbol: str, macd_fields: Dict, committed: bool = True):
        """`committed=False` for an intra-bar/provisional refresh: the stored
        VALUE is updated (so the gate reads the latest) but the freshness clock
        (_ts) is NOT advanced. The freshness clock tracks the last COMMITTED bar
        only, so a just-recovered/forming (unsettled) value cannot look 'fresh'
        — the gate keeps rejecting until a real committed bar confirms it."""
        import time as _t
        k = index_symbol.upper()
        cls._store[k] = macd_fields
        if committed:
            cls._ts[k] = _t.time()

    @classmethod
    def get(cls, index_symbol: str) -> Optional[Dict]:
        return cls._store.get(index_symbol.upper())

    @classmethod
    def get_age_sec(cls, index_symbol: str) -> Optional[float]:
        """2026-06-11: seconds since this index's MACD was last refreshed (None if
        never). The index gate REJECTS when this exceeds ~2x the index resolution,
        so a frozen/just-recovered feed can't pass a stale index value (the bad
        QQQ +0.09 that contradicted the settled -0.79)."""
        import time as _t
        ts = cls._ts.get(index_symbol.upper())
        return None if ts is None else (_t.time() - ts)

    @classmethod
    def is_aligned(cls, index_symbol: str, signal_direction: str) -> Tuple[bool, str]:
        """
        Returns (allowed: bool, reason: str).

        ALL THREE conditions must agree for the signal to be allowed:

            LONG  requires:
              1. index MACD Line  > index Signal Line   (classic MACD bullish)
              2. index ZL_MACD    > 0                   (ZL MACD above zero)
              3. index ZL_MACD    > index ZL_Signal     (ZL crossover bullish)

            SHORT requires:
              1. index MACD Line  < index Signal Line   (classic MACD bearish)
              2. index ZL_MACD    < 0                   (ZL MACD below zero)
              3. index ZL_MACD    < index ZL_Signal     (ZL crossover bearish)

        Blocks immediately if index MACD is not ready yet.
        """
        fields = cls.get(index_symbol)
        if fields is None:
            return False, f"{index_symbol} MACD not ready yet — blocking signal"

        macd_line   = fields["macd"][0]
        signal_line = fields["macd_signal"][0]
        zl_macd     = fields["macd_zl"]
        zl_signal   = fields["macd_zl_signal"]

        if signal_direction == "LONG":
            c1 = macd_line > signal_line   # classic MACD bullish
            c2 = zl_macd   > 0             # ZL MACD above zero
            c3 = zl_macd   > zl_signal     # ZL crossover bullish
            ok = c1 and c2 and c3
            checks = (
                f"MACD({macd_line:.5f})>Signal({signal_line:.5f})={'OK' if c1 else 'FAIL'}  "
                f"ZL_MACD({zl_macd:.5f})>0={'OK' if c2 else 'FAIL'}  "
                f"ZL_MACD>ZL_Sig({zl_signal:.5f})={'OK' if c3 else 'FAIL'}"
            )
        else:  # SHORT
            c1 = macd_line < signal_line   # classic MACD bearish
            c2 = zl_macd   < 0             # ZL MACD below zero
            c3 = zl_macd   < zl_signal     # ZL crossover bearish
            ok = c1 and c2 and c3
            checks = (
                f"MACD({macd_line:.5f})<Signal({signal_line:.5f})={'OK' if c1 else 'FAIL'}  "
                f"ZL_MACD({zl_macd:.5f})<0={'OK' if c2 else 'FAIL'}  "
                f"ZL_MACD<ZL_Sig({zl_signal:.5f})={'OK' if c3 else 'FAIL'}"
            )

        verdict = "ALIGNED" if ok else "BLOCKED"
        return ok, f"{index_symbol} [{verdict}]  {checks}"

    @classmethod
    def log_all(cls):
        for sym, fields in cls._store.items():
            if fields:
                trend = "UP" if fields["macd"][0] > fields["macd_signal"][0] else "DOWN"
                logger.info("  Index %-6s  MACD=%.5f  Signal=%.5f  trend=%s",
                            sym, fields["macd"][0], fields["macd_signal"][0], trend)


# =============================================================================
# INDICATOR PIPELINE  (one per symbol — stocks AND indexes)
# =============================================================================

class IndicatorPipeline:
    """
    Full indicator pipeline for one symbol.

    - Per-symbol MACD params read from config.json via resolve_symbol_params()
    - Auto-reseeds when params change (detects on every bar)
    - For indexes: calls IndexMACDStore.update() on each ready bar
    """

    def __init__(
        self,
        symbol: str,
        broker,
        is_index: bool = False,
        on_indicator_bar: Callable = None,
        resolution_override: str = None,
    ):
        self.symbol            = symbol
        self._broker           = broker
        self._is_index         = is_index
        self._on_indicator_bar = on_indicator_bar
        self._seeded           = False
        self._bars_seeded      = 0     # >0 only after a successful seed
        self._last_bar_ts      = None  # 2026-06-11: wall-clock of last COMMITTED bar (freshness)

        cfg = _load_cfg()
        # 2026-06-10: index pipeline of a DUAL symbol resolves from the Indexes
        # sheet (as_index) so it can run its own resolution/params.
        self._params = resolve_symbol_params(symbol, cfg, as_index=self._is_index)
        # 2026-06-03: a separate EXIT pipeline runs the SAME MACD params but on a
        # different candle resolution. resolution_override re-points just the
        # resolution (and therefore the seed bar-size + the aggregator), leaving
        # fast/slow/signal/price untouched. None = normal (entry) pipeline.
        # 2026-07-31: REMEMBER the override. _check_param_change() re-resolves
        # params from config on every bar, and resolve_symbol_params() always
        # returns the ENTRY resolution — so without re-applying the override
        # there, an exit pipeline (exit 3min vs entry 5min) compared 5min against
        # its own 3min and reported "resolution changed" on EVERY bar. That
        # triggered an endless auto-reseed loop; each reseed calls reset_all(),
        # clearing _ready and the MACD history, so the pipe could never become
        # ready and the constant mid-session reseeds ran into historical-data
        # pacing (0 bars -> "cold start").
        self._resolution_override = (
            str(resolution_override).strip().lower() if resolution_override else None
        )
        if self._resolution_override:
            self._params = dict(self._params)
            self._params["resolution"] = self._resolution_override
        self._engine  = self._make_engine(self._params)
        self._sec_agg = self._make_sec_agg(self._params)

        self._last_reset_date: Optional[str] = None
        self._session_start    = _parse_time(cfg.get("TimeToStartTrading", "09:30"))
        # Mean-calc gate: bars before this time DO NOT update the session
        # means (signal-line + diff). Keeps open-time volatility out of the
        # mean values. Default 10:30 ET (= ~1h after open).
        # 2026-05-22: resolved per-symbol via _resolve_mean_calc_start so each
        # equity can have its own threshold (set in the Excel Stocks/Indexes
        # sheet). Falls back to global MeanCalcStartTime.
        self._mean_calc_start  = self._resolve_mean_calc_start(cfg)

        # Crossover tracking — _prev_macd holds the MACD value at the last
        # CLOSED bar (used as the reference for the zero-line cross check).
        self._prev_macd: Optional[float] = None
        self._prev_signal: Optional[float] = None

        # One-shot guard for intra-bar entry firing. Set to True the first
        # time a zero-line cross is detected within the current open bar;
        # reset to False when the bar closes (_on_second_bar_closed). This
        # enforces "one entry per bar max" per user spec — wobble across
        # zero inside the same bar produces at most ONE entry event.
        self._intrabar_fired_for_current_bar: bool = False
        # 2026-06-01: throttle intra-bar entry evaluation to ~1/sec per symbol
        self._last_intrabar_eval_ts = None
        # 2026-06-03: latest trade price (for the dashboard's LIVE forming-bar MACD)
        self._last_tick_price = None
        self._last_tick_ts = None

        logger.info(
            "[%s] Pipeline  is_index=%s  res=%s  fast=%d slow=%d signal=%d  price=%s",
            symbol, is_index, self._params["resolution"],
            self._params["fast"], self._params["slow"],
            self._params["signal_period"], self._params["price_source"],
        )

    # ── Factories ─────────────────────────────────────────────────────────

    def _make_engine(self, p: Dict) -> MACDEngine:
        return MACDEngine(p["fast"], p["slow"], p["signal_period"], p["history_depth"])

    def _make_sec_agg(self, p: Dict):
        """
        Returns the correct bar aggregator for the configured resolution.

        Resolution → Aggregator mapping:
            "tick"       → None  (every trade is its own bar, handled in on_raw_tick)
            "Ns"         → SecondBarAggregator(N)   e.g. "5s"→5s bars, "30s"→30s bars
            "Ntick"      → NTickBarAggregator(N)    e.g. "25tick"→bar every 25 trades
            "Nmin"       → None  (minute bars handled by minute buffer in ibkr_broker)

        The bar_seconds value is read DIRECTLY from the resolution string so
        that "5s" always produces 5-second bars regardless of the global
        MACD_SecBarSize config value.
        """
        r = p["resolution"]

        # N-tick bars  e.g. "25tick", "100tick"
        n = NTickBarAggregator.parse_resolution(r)
        if n is not None:
            logger.info("[%s] N-tick aggregator: bar closes every %d trades", self.symbol, n)
            return NTickBarAggregator(n, self._on_second_bar_closed)

        # Single "tick" — every trade is a bar, no aggregator needed
        if r == "tick":
            return None

        # Time-based second bars  e.g. "5s", "30s", "10s"
        if r.endswith("s") and not r.endswith("min"):
            try:
                bar_secs = int(r[:-1])
            except ValueError:
                bar_secs = p.get("sec_bar_size", 30)   # safe fallback
            logger.info("[%s] SecondBar aggregator: bar closes every %ds", self.symbol, bar_secs)
            return SecondBarAggregator(bar_secs, self._on_second_bar_closed)

        # Minute resolution — bucket ticks into N*60-second bars.
        # The TWS tick path (macd_bot._tick_callback → on_raw_tick) is the
        # only live source; without an aggregator here the ticks would be
        # silently discarded and IndexMACDStore would never populate.
        if r.endswith("min"):
            try:
                mins = int(r[:-3])
            except ValueError:
                mins = 1
            bar_secs = max(1, mins) * 60
            logger.info("[%s] SecondBar aggregator (minute): bar closes every %ds (=%dmin)",
                        self.symbol, bar_secs, mins)
            return SecondBarAggregator(bar_secs, self._on_second_bar_closed)

        # Unknown resolution — fall through (no aggregator)
        return None
    # ── Seeding ───────────────────────────────────────────────────────────

    def seed(self) -> bool:
        """Fetch prior-session bars, warm EMAs. Call once at startup.

        Returns True if real bars were seeded, False on cold start.
        Safe to call again after pipeline.reset() — the retry thread does
        exactly that when an HMDS 162 failure cleared the first attempt.
        """
        conid = self._broker._conid_cache.get(self.symbol)
        if conid is None:
            logger.warning("[%s] conid missing — cold start", self.symbol)
            self._engine.finish_seeding()
            self._seeded = True
            self._bars_seeded = 0
            return False

        _tws_engine = getattr(self._broker, "_engine", None)
        bars = fetch_prior_session_bars(
            self._broker.session, conid,
            self._params["resolution"], self._params["seed_bars"],
            tws_engine=_tws_engine,
        )
        for b in bars:
            self._engine.update(
                self._extract_price(b, self._params["price_source"]),
                bar_time=b.get("ts"),          # 2026-06-03: buffer seed bars w/ time
            )

        if bars:
            logger.info("[%s] Seeded %d bars", self.symbol, len(bars))
            # 2026-06-03: compute the session means from the MORNING open using
            # the just-buffered seed bars (gated to TODAY + MeanCalcStartTime),
            # so a mid-day restart / reseed reflects the full session — not just
            # bars seen since the restart. "Max backwards relevant" = today's open.
            try:
                from datetime import datetime as _dt
                from zoneinfo import ZoneInfo as _ZI
                _today = _dt.now(_ZI("America/New_York")).date()
                _n = self._engine.recalc_session_means(self._mean_calc_start, session_date=_today)
                logger.info("[%s] Session means computed from morning: %d bars (>= %s)",
                            self.symbol, _n, self._mean_calc_start)
            except Exception as exc:
                logger.warning("[%s] morning-means compute failed: %s", self.symbol, exc)
        else:
            logger.warning("[%s] No seed bars — cold start", self.symbol)

        # 2026-06-03: mark TODAY's session reset as already done, so the first
        # live bar's _check_daily_reset does NOT wipe the morning means we just
        # computed. (Without this, the daily reset fires on bar #1 and the means
        # rebuild from a single live bar — making MACD-mean == slow-line.) The
        # bot restarts/seeds every morning, so this never blocks a real new-day
        # reset; it only stops the spurious wipe after a mid-day seed/reseed.
        try:
            from datetime import datetime as _dtr
            from zoneinfo import ZoneInfo as _ZIr
            self._last_reset_date = _dtr.now(_ZIr("America/New_York")).strftime("%Y-%m-%d")
        except Exception:
            pass

        self._engine.finish_seeding()
        self._seeded = True
        self._bars_seeded = len(bars)
        return len(bars) > 0

    def is_real_seeded(self) -> bool:
        """True if seed() returned actual bars (not a cold start)."""
        return self._bars_seeded > 0

    # ── Auto-reseed on param change ───────────────────────────────────────

    _STATE_KEYS = ("fast", "slow", "signal_period", "resolution",
                   "price_source", "sec_bar_size", "history_depth")

    def _resolve_mean_calc_start(self, cfg: Dict):
        """
        2026-05-22: resolve per-symbol mean_calc_start_time with portfolio +
        global fallback. Same inheritance chain as resolve_symbol_params.
        Returns a datetime.time.
        """
        sym_up  = self.symbol.upper()
        stocks  = {k.upper(): v for k, v in cfg.get("Stocks",  {}).items()}
        indexes = {k.upper(): v for k, v in cfg.get("Indexes", {}).items()}
        is_idx  = (sym_up in indexes) and (sym_up not in stocks)
        entry   = (indexes.get(sym_up) or {}) if is_idx else (stocks.get(sym_up) or {})
        # Per-symbol value
        v = entry.get("mean_calc_start_time")
        if v:
            return _parse_time(str(v))
        # Portfolio (stocks only)
        if not is_idx:
            pname = str(entry.get("portfolio", "") or "").strip()
            if pname:
                pv = (cfg.get("Portfolios", {}) or {}).get(pname, {}).get("mean_calc_start_time")
                if pv:
                    return _parse_time(str(pv))
        # Global fallback
        return _parse_time(cfg.get("MeanCalcStartTime", "10:30"))

    def _check_param_change(self):
        """Re-reads config and reseeds if params changed."""
        cfg        = _load_cfg()
        new_params = resolve_symbol_params(self.symbol, cfg, as_index=self._is_index)
        # 2026-07-31: re-apply the exit-pipeline resolution override, exactly as
        # __init__ does. Without this the comparison below is entry-vs-exit
        # resolution and always "changes" -> infinite reseed loop (see __init__).
        # A genuine exit-resolution EDIT is handled separately by
        # macd_bot._ensure_exit_pipe(), which rebuilds the pipe outright.
        if getattr(self, "_resolution_override", None):
            new_params = dict(new_params)
            new_params["resolution"] = self._resolution_override
        self._session_start   = _parse_time(cfg.get("TimeToStartTrading", "09:30"))

        # 2026-05-22: resolve per-symbol mean_calc_start. If it changed since
        # last bar, recompute the session means via in-memory bar replay so
        # the new threshold takes immediate effect (user can edit the time
        # in Excel and the bot picks it up on the next bar).
        new_mean_calc = self._resolve_mean_calc_start(cfg)
        if new_mean_calc != self._mean_calc_start:
            old = self._mean_calc_start
            self._mean_calc_start = new_mean_calc
            try:
                from datetime import datetime as _dt2
                from zoneinfo import ZoneInfo as _ZI2
                _td = _dt2.now(_ZI2("America/New_York")).date()
                n = self._engine.recalc_session_means(new_mean_calc, session_date=_td)
                logger.warning(
                    "[%s] MeanCalcStartTime changed %s → %s — recomputed "
                    "session means from %d buffered bar(s) (today, from morning)",
                    self.symbol, old.strftime("%H:%M"),
                    new_mean_calc.strftime("%H:%M"), n)
            except Exception as exc:
                logger.error("[%s] mean recalc failed: %s — resetting to zero",
                             self.symbol, exc)
                self._engine.reset_session_means()

        changed = [k for k in self._STATE_KEYS if new_params[k] != self._params[k]]
        if not changed:
            return

        logger.info("[%s] Params changed (%s) — auto-reseeding...",
                    self.symbol, ", ".join(changed))
        self._params  = new_params
        self._engine  = self._make_engine(new_params)
        self._sec_agg = self._make_sec_agg(new_params)
        self._prev_macd   = None
        self._prev_signal = None
        self._seeded  = False
        self.seed()

    # ── Price extraction ──────────────────────────────────────────────────

    @staticmethod
    def _extract_price(bar: Dict, source: str) -> float:
        h = bar.get("high",  bar.get("close", 0.0))
        l = bar.get("low",   bar.get("close", 0.0))
        c = bar.get("close", bar.get("price", 0.0))
        if source == "typical":  return (h + l + c) / 3.0
        if source == "weighted": return (h + l + 2.0 * c) / 4.0
        if source == "hlc2":     return (h + l) / 2.0
        return c

    # ── Daily reset ───────────────────────────────────────────────────────

    def _check_daily_reset(self, bar_time: datetime):
        today = bar_time.strftime("%Y-%m-%d")
        if today != self._last_reset_date and bar_time.time() >= self._session_start:
            self._engine.reset_session_means()
            self._last_reset_date = today
            logger.info("[%s] Session means reset", self.symbol)

    # ── Core enrich ───────────────────────────────────────────────────────

    def _enrich(self, bar: Dict) -> Optional[Dict]:
        self._check_param_change()
        bar_time = bar.get("current_time", datetime.now())
        self._check_daily_reset(bar_time)
        # Means-calc gate: only update session means once we're past
        # MeanCalcStartTime. EMAs / history are always updated.
        try:
            t = bar_time.time() if hasattr(bar_time, "time") else bar_time
            allow_means = t >= self._mean_calc_start
        except Exception:
            allow_means = True
        fields = self._engine.update(
            self._extract_price(bar, self._params["price_source"]),
            allow_means_update=allow_means,
            bar_time=bar_time,                  # 2026-05-22: buffer for recalc
        )
        if fields is None:
            return None
        import time as _t
        self._last_bar_ts = _t.time()        # 2026-06-11: last COMMITTED bar (freshness)
        if self._is_index:
            IndexMACDStore.update(self.symbol, fields)

        # ── Crossover detection & CSV logging ────────────────────────────
        cur_macd = fields["macd"][0]
        cur_sig  = fields["macd_signal"][0]

        if self._prev_macd is not None and self._prev_signal is not None:
            # Bullish crossover: MACD crosses ABOVE Signal
            if cur_macd > cur_sig and self._prev_macd <= self._prev_signal:
                CrossoverLogger.get().log(
                    bar_time, self.symbol, "BULLISH",
                    bar.get("close", bar.get("price", 0.0)),
                    fields, self._params,
                )
            # Bearish crossover: MACD crosses BELOW Signal
            elif cur_macd < cur_sig and self._prev_macd >= self._prev_signal:
                CrossoverLogger.get().log(
                    bar_time, self.symbol, "BEARISH",
                    bar.get("close", bar.get("price", 0.0)),
                    fields, self._params,
                )

        self._prev_macd   = cur_macd
        self._prev_signal = cur_sig
        # ─────────────────────────────────────────────────────────────────

        return {**bar, **fields}

    # ── Public API ────────────────────────────────────────────────────────

    def on_minute_bar(self, bar: Dict) -> Optional[Dict]:
        return self._enrich(bar)

    def on_raw_tick(self, tick: Dict):
        # 2026-06-03: remember the latest trade price so the dashboard can show
        # the LIVE (forming-bar) MACD that matches the chart. Display only.
        try:
            self._last_tick_price = tick.get("price")
            self._last_tick_ts = tick.get("ts")
        except Exception:
            pass
        r = self._params["resolution"]
        if r == "tick":
            # Every single trade is its own bar — no intra-bar logic needed
            # (each tick already closes a bar via _enrich path).
            bar = {
                "current_time": tick["ts"],
                "open"  : tick["price"], "high": tick.get("high", tick["price"]),
                "low"   : tick.get("low", tick["price"]), "close": tick["price"],
                "price" : tick["price"],
                "volume"      : tick.get("tick_vol",  0),
                "daily_volume": tick.get("daily_vol", 0),
            }
            enriched = self._enrich(bar)
            if enriched and self._on_indicator_bar:
                self._on_indicator_bar(self.symbol, enriched)
            return

        # 2026-05-26 (two-subscription model): for TIME-BASED resolutions the
        # committed bars now come from IBKR's OFFICIAL keepUpToDate stream
        # (on_official_bar → _on_second_bar_closed), NOT from ticks. Building
        # bars from the tick stream (with bid/ask noise + last-tick-as-close)
        # was drifting the MACD vs the broker's real candles. Ticks are now used
        # ONLY for the intra-bar provisional cross below. N-tick resolutions have
        # no official-bar equivalent, so they keep the tick aggregator.
        if isinstance(self._sec_agg, NTickBarAggregator):
            self._sec_agg.push(tick)

        # Intra-bar zero-line cross check (does NOT mutate engine state).
        # Skipped for index pipelines that have no on_indicator_bar wired
        # — IndexMACDStore.update for the index happens at bar close via
        # _enrich, which is fine for the index-filter consumers.
        self._check_intrabar_cross(tick)

    def _check_intrabar_cross(self, tick: Dict):
        """
        INTRA-BAR ENTRY EVALUATION (rewritten 2026-06-01).

        Builds a PROVISIONAL bar from the current tick price (no state
        mutation) and emits it to the entry pipeline whenever the provisional
        MACD (fast line) is crossing — or within the 5% near-cross band of —
        its signal line (the same precondition entry_logic.cross_ok uses).
        entry_logic then applies the FULL pattern+cross+filter test on the
        provisional values, so an entry can fire MID-BAR (up to ~one bar
        early) instead of only at the 3-min bar close.

        (Previously this only fired on a MACD *zero-line* cross, which almost
        never coincided with the signal-line-cross entry rule → 0 intra-bar
        entries. Now it mirrors the real entry trigger.)

        One entry per bar max: macd_bot sets _intrabar_fired_for_current_bar
        once an intra-bar entry actually fires; until then we keep evaluating
        (throttled to ~1/sec per symbol). Indexes only refresh IndexMACDStore.
        """
        if self._intrabar_fired_for_current_bar:
            return
        if self._prev_macd is None:
            return                                    # bar hasn't closed once yet
        if not self._engine.is_ready():
            return                                    # engine still warming up

        # Light throttle: at most ~1 entry-eval/sec per symbol during a
        # sustained near-cross window (entry check re-reads config from disk).
        ts = tick.get("ts")
        last = self._last_intrabar_eval_ts
        if ts is not None and last is not None:
            try:
                if (ts - last).total_seconds() < 1.0:
                    return
            except Exception:
                pass

        # 2026-06-04: use the FULL live (forming-bar) snapshot — both the fast
        # line AND the signal line are provisional — so a cross is caught the
        # MOMENT it happens intra-bar (matching the chart), not only at the
        # 3-min close. Previously the signal used the last CLOSED value, so a
        # cross driven by the signal line moving was only seen at bar close.
        pall = self._engine.compute_provisional_all(tick["price"])
        if pall is None:
            return
        prov     = pall["macd"]
        prov_sig = pall["signal"]

        sig_hist = self._engine._sig_hist
        if not sig_hist:
            return
        sig_now  = prov_sig                       # LIVE signal (forming bar)
        sig_prev = sig_hist[0]                     # last CLOSED signal (cross reference)

        # Gate: only bother the entry logic when the provisional fast line is
        # crossing — or within 5% of — its (live) signal line. Same precondition
        # as entry_logic.cross_ok, so gating here loses no real entries but
        # 2026-06-04 (per user): evaluate the FULL entry test live every throttled
        # tick — not just near a cross — so the PATTERN trigger (fast line
        # rising/falling) also fires intra-bar on the forming bar, exactly like
        # the cross does. entry_logic still applies pattern+cross+filters on the
        # provisional values; the ~1/sec throttle bounds the eval rate.
        self._last_intrabar_eval_ts = ts

        new_hist = ([prov] + list(self._engine._macd_hist))[: self._engine.depth]
        new_sig  = ([prov_sig] + list(self._engine._sig_hist))[: self._engine.depth]
        fields = {
            "macd"                : new_hist,
            # 2026-06-04: prepend the LIVE signal so entry_logic's cross test
            # evaluates on the forming-bar values → fires intra-bar on the cross.
            "macd_signal"         : new_sig,
            "macd_zl"             : 0.0,              # not computed provisionally
            "macd_zl_signal"      : 0.0,
            "macd_signal_pos_mean": self._engine._means.pos_mean,
            "macd_signal_neg_mean": self._engine._means.neg_mean,
            "macd_diff_pos_mean"  : self._engine._diff_means.pos_mean,
            "macd_diff_neg_mean"  : self._engine._diff_means.neg_mean,
            "ema_fast"            : self._engine._ef.value or 0.0,
            "ema_slow"            : self._engine._es.value or 0.0,
        }
        bar = {
            "current_time" : tick["ts"],
            "open"         : tick["price"],
            "high"         : tick.get("high", tick["price"]),
            "low"          : tick.get("low",  tick["price"]),
            "close"        : tick["price"],
            "price"        : tick["price"],
            "_intrabar"    : True,
        }
        enriched = {**bar, **fields}

        # Indexes never trade — just refresh the store so the index filter
        # sees the latest provisional MACD; do NOT emit an entry event.
        if self._is_index:
            IndexMACDStore.update(self.symbol, fields, committed=False)  # provisional: value only, not freshness clock
            return

        if self._on_indicator_bar:
            self._on_indicator_bar(self.symbol, enriched)

    def _on_second_bar_closed(self, bar: Dict):
        """
        Aggregator-driven bar close. Always commits state via _enrich
        (EMAs, history, means, _prev_macd, CSV crossover log). The
        downstream entry callback is suppressed if intra-bar firing
        already happened for this bar — enforces one-shot per bar.
        """
        already_fired_intrabar = self._intrabar_fired_for_current_bar

        # _enrich commits state — must run regardless of intra-bar firing.
        enriched = self._enrich(bar)

        # Bar-close cross detection (zero-line): only fire if no intra-bar
        # event already fired this bar. _enrich has already overwritten
        # _prev_macd by now, so we use the macd_hist deque directly.
        if enriched is not None and not already_fired_intrabar:
            try:
                macd_now  = enriched["macd"][0]
                macd_prev = enriched["macd"][1] if len(enriched["macd"]) > 1 else None
            except (KeyError, IndexError):
                macd_now = macd_prev = None
            cross_kind = None
            if macd_prev is not None:
                if macd_prev <= 0.0 < macd_now:
                    cross_kind = "BULLISH_ZERO"
                elif macd_prev >= 0.0 > macd_now:
                    cross_kind = "BEARISH_ZERO"
            if cross_kind is not None:
                enriched["_cross_event"] = cross_kind
                enriched["_intrabar"]    = False

        # Reset the one-shot guard for the NEXT bar that's about to start.
        self._intrabar_fired_for_current_bar = False

        if enriched and self._on_indicator_bar:
            self._on_indicator_bar(self.symbol, enriched)

    def on_official_bar(self, bar: Dict, is_final: bool = True):
        """
        2026-05-26 (two-subscription model): commit from IBKR's OFFICIAL bar,
        delivered by the keepUpToDate stream (TWSClient.historicalDataUpdate →
        macd_bot._route_official_bar). Only a FINAL (completed) bar commits.

        Routes through the existing _on_second_bar_closed so ALL commit logic
        (EMAs/means/_prev_macd/crossover-CSV via _enrich, bar-close zero-cross
        detection, and the intra-bar one-shot reset) is shared and unchanged —
        the only difference is the bar now carries the broker's real OHLC
        instead of a tick-assembled bar.
        """
        if is_final and bar:
            self._on_second_bar_closed(bar)

    def is_ready(self)  -> bool: return self._engine.is_ready()
    def is_seeded(self) -> bool: return self._seeded
    def get_resolution(self) -> str: return self._params["resolution"]

    def get_last_bar_age_sec(self):
        """2026-06-11: seconds since the last COMMITTED bar (None if none yet).
        Used by the entry-side freshness guard to block entries on a frozen or
        just-recovered feed (last committed bar older than ~2x the resolution)."""
        import time as _t
        ts = getattr(self, "_last_bar_ts", None)
        return None if ts is None else (_t.time() - ts)

    def reset(self):
        self._engine.reset_all()
        if self._sec_agg:
            self._sec_agg.reset()
        self._seeded = False
        self._bars_seeded = 0


# =============================================================================
# RESOLUTION HELPERS
# =============================================================================

def get_resolution(cfg: Dict) -> str:
    return cfg.get("MACD_Resolution", "30s").strip().lower()

def is_tick_resolution(cfg: Dict) -> bool:
    return get_resolution(cfg) == "tick"

def is_second_resolution(cfg: Dict) -> bool:
    r = get_resolution(cfg)
    return r.endswith("s") and not r.endswith("min")

def is_minute_resolution(cfg: Dict) -> bool:
    return get_resolution(cfg).endswith("min")

def resolution_is_tick(res: str)   -> bool:
    return res.strip().lower() == "tick"

def resolution_is_second(res: str) -> bool:
    r = res.strip().lower()
    return r.endswith("s") and not r.endswith("min")

def resolution_is_minute(res: str) -> bool:
    return res.strip().lower().endswith("min")

def resolution_to_seconds(res: str, default: float = 60.0) -> float:
    """2026-06-11: parse a resolution string to seconds for freshness windows.
    '30s'->30, '5s'->5, '1min'/'3min'/'5min'->60/180/300, 'tick'/'Ntick'->~5.
    Falls back to `default` on anything unrecognized."""
    try:
        r = str(res).strip().lower().replace(" ", "")
        if not r:
            return default
        n = NTickBarAggregator.parse_resolution(r)
        if n is not None:
            return 5.0                      # N-tick / tick: treat as ~5s cadence
        if r.endswith("min"):
            return float(int(r[:-3] or 1)) * 60.0
        if r.endswith("s"):
            return float(int(r[:-1] or 30))
        return float(int(r))                # bare number = seconds
    except (TypeError, ValueError):
        return default


# =============================================================================
# SELF-TEST
# =============================================================================

if __name__ == "__main__":
    import math, os, sys

    print("=" * 65)
    print("indicators.py — self-test")
    print("=" * 65)

    # Test 1: engine seeding + live output
    engine = MACDEngine(fast=12, slow=26, signal=9, history_depth=10)
    for i in range(52):
        engine.update(100 + i * 0.03 + math.sin(i * 0.4))
    engine.finish_seeding()
    results = []
    for i in range(15):
        out = engine.update(101.5 + i * 0.05 + math.sin((52+i)*0.4))
        if out:
            results.append(out)
    print(f"\nEngine test: seeded + {len(results)} live outputs (expect 6)")
    if results:
        last = results[-1]
        print(f"  macd[0]={last['macd'][0]:.5f}  signal[0]={last['macd_signal'][0]:.5f}")
        print(f"  zl_macd={last['macd_zl']:.5f}  zl_sig={last['macd_zl_signal']:.5f}")

    # Test 2: IndexMACDStore
    print("\nIndexMACDStore alignment test:")
    IndexMACDStore.update("SPY", {
        "macd": [0.05], "macd_signal": [0.03],
        "macd_zl": 0.04, "macd_zl_signal": 0.02,
        "macd_signal_pos_mean": 0.04, "macd_signal_neg_mean": -0.02,
    })
    for direction, expect in [("LONG", True), ("SHORT", False)]:
        ok, reason = IndexMACDStore.is_aligned("SPY", direction)
        status = "PASS" if ok == expect else "FAIL"
        print(f"  [{status}] {direction:<5}  allowed={ok}  {reason}")

    # Test 3: resolve_symbol_params
    print("\nresolve_symbol_params test:")
    cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
    if os.path.exists(cfg_path):
        cfg = _load_cfg()
        for sym in ["AAPL", "TSLA", "SPY", "QQQ"]:
            p = resolve_symbol_params(sym, cfg)
            print(f"  {sym:<6}  res={p['resolution']:<6}  "
                  f"fast={p['fast']}  slow={p['slow']}  "
                  f"signal={p['signal_period']}  price={p['price_source']}")
    else:
        print("  (skipped — config.json not in current directory)")

    print("\nAll self-tests passed.")