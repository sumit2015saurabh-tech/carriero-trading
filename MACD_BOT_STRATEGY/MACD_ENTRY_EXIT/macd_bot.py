"""
=============================================================================
QTI Fund  |  Model: !JFMX3X_NONCORR_MACD_V3.x
FILE: macd_bot.py

DESCRIPTION:
    Main bot loop — replaces ibkr_broker.py entirely.
    Integrates:
        IBKR_TWS.py    → order execution (TWS primary, CP Gateway fallback)
        indicators.py  → MACD bar pipelines + index filter
        entry_logic.py → signal evaluation
        exit_logic.py  → BE / trail / stop / EoD logic
        excel_sync.py  → config sync from Excel (optional)

ARCHITECTURE:
    1. On startup: resolve conids, seed MACD pipelines, start engine
    2. Market data via TWS reqMktData → on_raw_tick → IndicatorPipeline
    3. On each completed bar: check_entry_signal() → submit_bracket()
    4. On fill callback: position registered, direction monitor active
    5. On each bar for open positions: process_exit() → engine calls
    6. Direction monitor (5s) catches direction flips
    7. EoD square-off at configured time

=============================================================================
"""
from __future__ import annotations

import csv
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, time as dtime
from typing import Dict, Optional

import pytz

# ── Local imports ─────────────────────────────────────────────────────────────
from IBKR_TWS    import TWSEngine
from indicators  import IndicatorPipeline, IndexMACDStore
from bar_aggregator import BarAggregator
from entry_logic import check_entry_signal
from exit_logic  import process_exit

# ── Config ────────────────────────────────────────────────────────────────────
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")
EST         = pytz.timezone("America/New_York")


def _bar_epoch(d):
    """Parse an IBKR bar.date into epoch seconds for the 10s→N-min aggregator.
    Handles both formats IBKR may return: epoch-seconds ("1720800000",
    formatDate=2) and "YYYYMMDD HH:MM:SS [TZ]" (formatDate=1). Only used for
    window alignment (floor to 5-/3-min), which is TZ-agnostic — any consistent
    epoch works — so an optional trailing timezone token is ignored."""
    if d is None:
        return None
    import calendar
    s = str(d).strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    parts = s.split()
    ymd = parts[0]
    hms = parts[1] if len(parts) > 1 else "00:00:00"
    try:
        dt = datetime.strptime(ymd + " " + hms, "%Y%m%d %H:%M:%S")
    except ValueError:
        try:
            dt = datetime.strptime(ymd + " " + hms, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return calendar.timegm(dt.timetuple())

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MACD] %(levelname)s - %(message)s",
)
logger = logging.getLogger("macd_bot")


# =============================================================================
# PRICE / MACD CSV LOGGER  (2026-05-27: per-bar diagnostic capture)
# =============================================================================
class PriceLogger:
    """
    Appends ONE row per committed OFFICIAL bar, per symbol, to a daily CSV in
    the project folder — so the bot's price + MACD can be compared against the
    chart to diagnose MACD discrepancies.

    File: <script_dir>/price_macd_log_<DD-MM-YY>.csv
    Thread-safe. Any failure is swallowed — logging must NEVER disrupt trading.
    """
    _inst = None
    _HEADER = ["wall_clock_est", "bar_time", "symbol", "resolution",
               "close", "ema_fast", "ema_slow", "macd", "signal",
               "seeded", "ready"]

    @classmethod
    def get(cls):
        if cls._inst is None:
            cls._inst = cls()
        return cls._inst

    def __init__(self):
        self._lock = threading.Lock()
        self._day = None
        self._fh = None
        self._csv = None

    def _ensure_file(self):
        today = datetime.now(EST).strftime("%d-%m-%y")
        if today != self._day:
            if self._fh:
                try:
                    self._fh.close()
                except Exception:
                    pass
            path = os.path.join(os.path.dirname(__file__),
                                f"price_macd_log_{today}.csv")
            is_new = not os.path.exists(path)
            self._fh = open(path, "a", newline="")
            self._csv = csv.writer(self._fh)
            if is_new:
                self._csv.writerow(self._HEADER)
            self._day = today
            logger.info("Price/MACD CSV -> %s", path)

    def log(self, symbol, bar, resolution=None, seeded=None, ready=None):
        try:
            with self._lock:
                self._ensure_file()
                # 2026-06-01: stamp bar_time at the bar's OPEN (IBKR convention)
                # so the logged series lines up with the IBKR chart. wall_clock_est
                # (below) still records the real close/log time for forensics.
                bt = _ibkr_bar_open_iso(bar.get("current_time", ""), resolution)
                macd = bar.get("macd") or [None]
                sig = bar.get("macd_signal") or [None]
                self._csv.writerow([
                    datetime.now(EST).isoformat(timespec="seconds"),
                    bt, symbol, resolution,
                    bar.get("close"),
                    bar.get("ema_fast"), bar.get("ema_slow"),
                    macd[0], sig[0],
                    seeded, ready,
                ])
                self._fh.flush()
        except Exception as exc:               # never let logging break trading
            logger.debug("PriceLogger error (ignored): %s", exc)


# =============================================================================
# CONFIG HELPERS
# =============================================================================

def _resolution_seconds(res):
    """Parse a resolution string ('3min','30s','1min','10s') → seconds, else None."""
    if not res:
        return None
    r = str(res).strip().lower()
    try:
        if r.endswith("min"):
            return int(r[:-3] or "1") * 60
        if r.endswith("s") and not r.endswith("tick"):
            return int(r[:-1])
    except ValueError:
        return None
    return None


def _ibkr_bar_open_iso(ct, resolution):
    """
    Label a committed bar by its OPEN time (IBKR convention). Our internal
    `current_time` is ~the bar CLOSE, so floor to the bar grid and step back
    one interval. (2026-06-01: makes logged bar_time line up with IBKR, which
    stamps each bar at its start.) Falls back to the raw close time if the
    resolution is unknown/tick-based. Internal trading logic is unaffected —
    this only changes the displayed/logged timestamp.
    """
    if not hasattr(ct, "isoformat"):
        return str(ct)
    secs = _resolution_seconds(resolution)
    if not secs:
        return ct.isoformat()
    try:
        sod = ct.hour * 3600 + ct.minute * 60 + ct.second
        open_sod = (sod // secs) * secs - secs
        if open_sod < 0:
            return ct.isoformat()
        base = ct.replace(hour=0, minute=0, second=0, microsecond=0)
        return (base + timedelta(seconds=open_sod)).isoformat()
    except Exception:
        return ct.isoformat()


_CFG_CACHE: Dict = None
_CFG_MTIME: float = -1.0

def load_config() -> Dict:
    """Return config.json, re-reading from disk ONLY when its mtime changes.
    Avoids a disk read + json parse on every bar/tick (which loaded the IB
    reader thread). excel_sync bumps the mtime on any Excel edit, so live
    changes still apply instantly."""
    global _CFG_CACHE, _CFG_MTIME
    try:
        mt = os.path.getmtime(CONFIG_PATH)
    except OSError:
        mt = -1.0
    if _CFG_CACHE is None or mt != _CFG_MTIME:
        with open(CONFIG_PATH) as f:
            _CFG_CACHE = json.load(f)
        _CFG_MTIME = mt
    return _CFG_CACHE


def _parse_time(t: str):
    return datetime.strptime(t, "%H:%M").time()


# =============================================================================
# MAIN BOT CLASS
# =============================================================================

class MACDBot:
    """
    Main orchestrator. Owns:
      - TWSEngine  (orders)
      - IndicatorPipeline  per symbol (MACD)
      - open_positions     dict (internal position state)
    """

    def __init__(self):
        self._engine: Optional[TWSEngine] = None

        # ── Position state ────────────────────────────────────────────────
        self._open_positions: Dict[str, Dict] = {}   # symbol → position dict
        self._pos_lock = threading.Lock()

        # ── Per-symbol bracket-in-flight tracker (FAIL-CLOSED) ────────────
        # When _handle_entry fires submit_bracket, the symbol is added here.
        # It's removed ONLY by:
        #   - on_fill callback (bracket settled)
        #   - on_cancel callback (bracket cancelled)
        #   - manual clear_bracket_in_flight() call
        # If TWS callbacks die, the symbol stays in the set forever — new
        # entries on that symbol are then BLOCKED. Per user spec: position
        # per symbol must never exceed Excel `quantity`.
        self._brackets_in_flight: set = set()
        self._brackets_lock = threading.Lock()

        # 2026-06-05 (per user): re-entry after a stop-loss when the trend is
        # still intact (no delta/gamma exit signal and no reversal). Capped per
        # symbol per day to prevent a stop-then-reenter loss loop. The dict
        # resets automatically on the daily 9:00 restart.
        self._reentry_count: Dict[str, int] = {}
        # 2026-06-11 (per user): auto-re-entry-after-stop is now OFF by default
        # (cap 0). Once a position exits, the bot does NOT auto-re-open the same
        # side — it waits for a FRESH entry signal like any other trade. This
        # ends the cascade where, on a runaway adverse move, the bot re-entered
        # repeatedly into the trend and stopped out (cost ~-$2,901 on SPY). Can be
        # re-enabled via REENTRY_CAP>0 (and even then the no-chase guard applies).
        try:
            self._reentry_cap = int(os.getenv("REENTRY_CAP", "0"))
        except (TypeError, ValueError):
            self._reentry_cap = 0

        # ── MACD pipelines ────────────────────────────────────────────────
        self._pipelines:  Dict[str, IndicatorPipeline] = {}   # trading stocks (ENTRY resolution)
        self._idx_pipes:  Dict[str, IndicatorPipeline] = {}   # index pipelines
        # 2026-06-08: phantom-position safety net — tracks first-seen time of any
        # TWS position NOT in _open_positions (late-fill-after-cancel). Swept and
        # flattened if it persists past the grace period. See _sweep_unmanaged_positions.
        self._unmanaged_seen: Dict[str, float] = {}
        # 2026-06-03: separate EXIT pipelines — only present for a symbol whose
        # exit candle resolution differs from its entry resolution. Same MACD
        # params, different timeframe; drives EXIT logic for open positions.
        self._exit_pipes: Dict[str, IndicatorPipeline] = {}

        # ── TWS market data request IDs ───────────────────────────────────
        self._req_id_map:  Dict[str, int] = {}   # symbol → reqId (reqMktData live ticks)
        self._next_req_id: int = 10000
        # 2026-05-26 (two-subscription model): symbol → reqId for the OFFICIAL
        # keepUpToDate bar stream (committed MACD). Range 20000+.
        self._hist_req_map: Dict[str, int] = {}
        # 2026-06-03: symbol → reqId for the EXIT-resolution official-bar stream
        # (range 30000+, disjoint from entry hist 20000+ and mktData 10000+).
        self._exit_hist_req_map: Dict[str, int] = {}
        # 2026-06-10: symbol → reqId for the INDEX-resolution official-bar stream
        # (range 40000+). Only for DUAL stock+index symbols whose index candle
        # differs from their stock candle (e.g. QQQ stock 5min / index 3min), so
        # the index pipeline gets bars at its own resolution.
        self._idx_hist_req_map: Dict[str, int] = {}
        # 2026-07-08 (10s base-candle model): ONE 10-second keepUpToDate stream
        # per symbol (reqId reuses the 20000+ entry slot). A BarAggregator rolls
        # that 10s stream UP into the entry (5m), exit (3m) and index bars
        # locally — replacing the 2–3 separate per-resolution streams that broke
        # IBKR's ~50 concurrent-line cap and drove the exit-pipe reseed churn.
        self._base_hist_req_map: Dict[str, int] = {}     # symbol → 10s stream reqId
        self._aggregators: Dict[str, "BarAggregator"] = {}  # symbol → BarAggregator

        # ── Thread control ────────────────────────────────────────────────
        self._running = False

    # =========================================================================
    # STARTUP
    # =========================================================================

    def start(self):
        cfg = load_config()
        stocks  = list(cfg.get("Stocks",  {}).keys())
        indexes = list(cfg.get("Indexes", {}).keys())

        logger.info("Starting MACDBot — %d stocks  %d indexes", len(stocks), len(indexes))

        # ── Start the TWS engine ──────────────────────────────────────────
        self._engine = TWSEngine()
        if not self._engine.start():
            raise RuntimeError("Neither TWS nor CP API available — cannot start")

        # Sync internal _open_positions when an IBKR stop fills, eliminating
        # stale-state drift (the cause of repeated 10148 cancel-already-filled
        # errors and ghost positions).
        self._engine.set_position_clear_callback(self._on_stop_filled)

        # When TWS has been disconnected for ≥ 2.5 min, dump bot-side state
        # (positions, registered exits) so a post-mortem is possible even if
        # the gateway never recovers in this session.
        self._engine.set_long_disconnect_callback(self._on_long_disconnect)

        # ── Schedule EoD square-off ───────────────────────────────────────
        eod_time = cfg.get("EoDExitTime", "15:50")
        # BUG-004: Validate EoDExitTime is within market hours 13:00-16:00 ET
        try:
            _eh, _em = map(int, eod_time.split(":"))
            _emins   = _eh * 60 + _em
            if not (13 * 60 <= _emins <= 16 * 60):
                logger.error(
                    "*** CONFIG ERROR: EoDExitTime='%s' is OUTSIDE market hours "
                    "(13:00-16:00 ET). Bot will trade past market close! "
                    "Did you mean '%s'? Fix config.json immediately.",
                    eod_time,
                    f"{_eh + 12}:{_em:02d}" if _eh < 12 else eod_time)
            else:
                logger.info("EoDExitTime validated: %s ET ✓", eod_time)
        except Exception:
            logger.error("EoDExitTime '%s' is invalid format — expected HH:MM", eod_time)
        self._engine.eod_square_off(eod_time_et=eod_time)
        logger.info("EoD square-off scheduled for %s ET", eod_time)

        # ── Resolve conids ────────────────────────────────────────────────
        # Wait for IB Gateway to be fully ready after connect
        # nextValidId fires immediately but Gateway needs a few more seconds
        # before it can handle reqMatchingSymbols reliably
        # 2026-06-11: wait for the HMDS (historical-data) FARM to be READY before
        # we start seeding — instead of a blind 5s. After the 8:45 gateway restart
        # the farm is often still reconnecting at the 9:00 bot start, so seeds
        # fired too early came back empty (the daily cold-pipe problem). Gate on
        # the engine's farm-OK signal (error 2106), with a 60s cap so it never
        # blocks forever (seeding has its own retry path regardless).
        logger.info("Waiting for IB Gateway + HMDS data farm to be ready...")
        time.sleep(3)                                  # minimum gateway settle
        _tws_ref = getattr(self._engine, "_tws", None)
        _farm_deadline = time.time() + 60
        while time.time() < _farm_deadline:
            if getattr(_tws_ref, "_hmds_farm_ok", None) is True:
                logger.info("HMDS data farm READY — proceeding to seed")
                break
            time.sleep(1)
        else:
            logger.warning("HMDS farm-ready not confirmed within 60s — proceeding "
                           "anyway (seeds will retry via _retry_unseeded_pipes)")

        logger.info("Resolving conids for %d symbols...", len(stocks) + len(indexes))
        self._conid_cache: Dict[str, int] = {}
        for sym in stocks + indexes:
            conid = self._engine.resolve_conid(sym)
            if conid:
                self._conid_cache[sym] = conid
                logger.info("  %s → conid %d", sym, conid)
            else:
                logger.warning("  %s → conid NOT resolved — will be skipped", sym)
            time.sleep(1.5)          # Stagger requests — prevent IB Gateway overload

        # ── 2026-06-16: TEST-SEED GATE ────────────────────────────────────
        # The farm-OK (2106) signal can fire while the HMDS farm still returns
        # EMPTY bars at the open — that caused the daily error-162 storm + cold
        # pipes. So probe ONE conid until it actually returns bars before firing
        # the full seed burst. Fully guarded: it can never block/crash startup,
        # and any cold pipe that slips through still self-heals via the retry.
        try:
            _tws_p = getattr(self._engine, "_tws", None)
            _probe = next(iter(self._conid_cache.values()), None)
            if _tws_p is not None and _probe is not None and hasattr(_tws_p, "get_historical_bars"):
                _seed_deadline = time.time() + 240
                _pn = 0
                while time.time() < _seed_deadline:
                    _pn += 1
                    _tb = None
                    try:
                        _tb = _tws_p.get_historical_bars(conid=_probe, period="1 D", bar_size="5 mins")
                    except Exception:
                        _tb = None
                    if _tb:
                        logger.info("Test-seed OK (%d bars) — HMDS farm is serving data; seeding all symbols", len(_tb))
                        break
                    logger.warning("Test-seed attempt %d: farm not serving data yet — waiting 12s", _pn)
                    time.sleep(12)
                else:
                    logger.warning("Test-seed not confirmed within 4min — seeding anyway (cold pipes will self-heal via retry)")
        except Exception as _exc:
            logger.warning("test-seed gate skipped (%s) — proceeding to seed", _exc)

        # ── Build MACD pipelines ──────────────────────────────────────────
        for sym in stocks:
            if sym not in self._conid_cache:
                continue
            pipe = IndicatorPipeline(
                symbol=sym,
                broker=self,                          # bot acts as the broker proxy
                is_index=False,
                on_indicator_bar=self._on_bar,
            )
            pipe.seed()
            self._pipelines[sym] = pipe

        for sym in indexes:
            if sym not in self._conid_cache:
                continue
            pipe = IndicatorPipeline(
                symbol=sym,
                broker=self,
                is_index=True,
                on_indicator_bar=None,                # indexes don't trade
            )
            pipe.seed()
            self._idx_pipes[sym] = pipe

        # ── Retry cold-started seeds until success or pre-open deadline ──
        # HMDS error 162 ("session from different IP") makes seeds return 0
        # bars and leaves the pipeline cold-started. Retry every 30s until
        # either all pipelines have real bars or we reach T-2 min before
        # TimeToStartTrading. Runs BEFORE market data subscription so there
        # are no live ticks competing with pipe.reset().
        self._retry_failed_seeds(cfg)

        # ── Subscribe to TWS market data ──────────────────────────────────
        all_syms = list(self._conid_cache.keys())
        self._all_syms = all_syms          # shared list object — reconnect closure
                                           # AND runtime hot-add both append/read it
        self._subscribe_market_data(all_syms)

        # ── 2026-06-03: build EXIT pipelines for any stock whose exit candle
        #    resolution differs from its entry resolution (dedicated stream). ──
        for sym in list(self._pipelines.keys()):
            try:
                self._ensure_exit_pipe(sym)
            except Exception as exc:
                logger.warning("[%s] exit-pipe startup build failed: %s", sym, exc)

        # ── 2026-06-10: for DUAL stock+index symbols whose index candle differs
        #    from their stock candle, give the index pipeline its own stream. ──
        for sym in list(self._idx_pipes.keys()):
            try:
                self._ensure_idx_stream(sym)
            except Exception as exc:
                logger.warning("[%s] idx-stream startup build failed: %s", sym, exc)

        # ── Register reconnect callback — resubscribe on watchdog reconnect ──
        # When TWS drops and the watchdog reconnects, all reqTickByTickData
        # subscriptions are lost (they are tied to the socket, not the account).
        # This callback re-issues all subscriptions on the fresh TWSClient.
        def _on_reconnect(new_tws_client):
            logger.warning("Reconnect callback: resubscribing market data for %d symbols",
                           len(all_syms))
            # Patch callbacks onto the new TWSClient instance
            self._engine._tws = new_tws_client           # Engine already swapped it, but ensure local ref
            self._subscribe_market_data(all_syms)        # Re-issue all subscriptions
            for _s in list(self._idx_pipes.keys()):      # 2026-06-10: re-issue dual-symbol index streams
                try: self._ensure_idx_stream(_s)
                except Exception: pass
            logger.info("Reconnect callback: market data resubscription complete")

        self._engine.set_reconnect_callback(_on_reconnect)

        # ── Start direction monitor ───────────────────────────────────────
        # On 5s bars: monitor every 2s (faster than bar close)
        # On 30s bars: monitor every 5s (plenty of time)
        _bar_sec = int(cfg.get("MACD_SecBarSize", 30))
        _dm_interval = 2 if _bar_sec <= 5 else 5
        self._engine.start_direction_monitor(
            self._open_positions, self._pos_lock, interval=_dm_interval)
        logger.info("Direction monitor interval: %ds (bar size=%ds)",
                    _dm_interval, _bar_sec)

        self._running = True

        # ── Start dashboard (port 8080) ───────────────────────────────────
        # Reads bot state in-memory + cached TWS positions every 5s. No
        # impact on the trading loop. /api/data now includes per-symbol
        # MACD state (fast, slow, MACD, signal, diff, diff-means, intra-bar
        # armed flag) so the open-time behavior is visible without grep.
        try:
            from positions_server import start_dashboard
            start_dashboard(self)
            logger.info("Positions dashboard started on port 8080")
        except Exception as exc:
            logger.warning("Dashboard failed to start (continuing without it): %s", exc)

        # ── SHADOW TRADER (port 8090) — 2026-08-06 ───────────────────────────
        # Orders are simulated locally (see shadow_broker.py) instead of being
        # sent to the IBKR paper account. The Shadow Trader page — open shadow
        # positions, every stop-loss move, closed trades, net P&L and a date
        # picker that replays any past session — runs as a SEPARATE process
        # (scripts/start_shadow_ui.sh, kept alive by cron) rather than inside
        # this one, so that:
        #   • it stays up when the bot is down, which is exactly when you want
        #     to review yesterday's session, and
        #   • changing the UI never requires restarting the trading process
        #     (the lesson from the :8080 dashboard being embedded here).
        # The broker publishes shadow/state_today.json every second for it.
        logger.info("Shadow Trader UI is a separate service → http://<vm>:8090/")

        # ── Start the LIVE (tick-driven) TRAIL loop — 2026-08-06 ─────────────
        self._start_live_trail_loop()

        # ── Start periodic MACD STATUS log line ───────────────────────────
        self._start_macd_status_log()

        # ── Start runtime NEW-SYMBOL sync (2026-06-01) ────────────────────
        # Picks up stocks added to the Excel sheet mid-session (excel_sync
        # writes them to config.json) WITHOUT a restart: builds the pipeline,
        # seeds it, and subscribes market data — after which the symbol shows
        # on the dashboard automatically and is tradable. Fully fail-safe.
        self._start_new_symbol_sync()

        # 2026-07-04 (per user): ML entry-gate REMOVED. The bot no longer asks a
        # trained model whether to take a trade — every qualified signal is taken.

        logger.info("MACDBot started — monitoring %d stocks", len(self._pipelines))

    # =========================================================================
    # RUNTIME HOT-ADD OF NEW SYMBOLS  (added 2026-06-01)
    # =========================================================================
    # The bot builds pipelines only at startup. To make a stock added to the
    # Excel sheet mid-session appear on the dashboard (and trade) without a
    # restart, a daemon thread polls config.json and hot-adds any enabled
    # stock that has no pipeline yet. EVERYTHING here is wrapped so a failure
    # can never disturb the trading loop — worst case it logs and retries.

    def _start_new_symbol_sync(self, interval: int = 30):
        """Daemon loop: every `interval`s, hot-add any new enabled stocks AND
        auto-reseed any existing symbol whose flow params changed in Excel."""
        self._pipe_fp = {}          # sym -> last-seen flow fingerprint (baseline)
        def _loop():
            # small initial delay so startup finishes settling first
            time.sleep(interval)
            while self._running:
                try:
                    self._hotadd_new_symbols()
                except Exception as exc:
                    logger.warning("new-symbol sync error (ignored): %s", exc)
                try:
                    self._reseed_changed_symbols()
                except Exception as exc:
                    logger.warning("reseed sync error (ignored): %s", exc)
                try:
                    self._retry_unseeded_pipes()       # 2026-06-11: B2 — recover cold pipes
                except Exception as exc:
                    logger.warning("unseeded-pipe retry error (ignored): %s", exc)
                try:
                    self._reconcile_positions()        # 2026-06-11: C — clear phantom positions
                except Exception as exc:
                    logger.warning("position reconcile error (ignored): %s", exc)
                time.sleep(interval)
        threading.Thread(target=_loop, daemon=True, name="new-symbol-sync").start()
        logger.info("New-symbol sync started (every %ds) — Excel additions AND "
                    "flow-param changes go live without a restart", interval)

    def _hotadd_new_symbols(self):
        """Scan config.json for enabled stocks with no pipeline; hot-add them."""
        try:
            cfg = load_config()
        except Exception as exc:
            logger.warning("new-symbol sync: config read failed: %s", exc)
            return
        stocks_cfg = cfg.get("Stocks", {}) or {}
        for sym, entry in list(stocks_cfg.items()):
            if not isinstance(entry, dict):
                continue
            if str(entry.get("enabled", "N")).strip().upper() != "Y":
                continue
            if sym in self._pipelines or sym in self._idx_pipes:
                continue                              # already running
            try:
                self._hotadd_one(sym)
            except Exception as exc:
                logger.warning("[%s] hot-add failed (will retry next cycle): %s",
                               sym, exc)

    def _hotadd_one(self, sym: str):
        """Build + seed + subscribe a single new symbol, exactly like startup."""
        conid = self._conid_cache.get(sym) or self._engine.resolve_conid(sym)
        if not conid:
            logger.warning("[%s] hot-add: conid not resolved — skip this cycle", sym)
            return
        self._conid_cache[sym] = conid
        logger.info("[%s] HOT-ADD: new enabled stock detected — building pipeline", sym)
        pipe = IndicatorPipeline(
            symbol=sym,
            broker=self,
            is_index=False,
            on_indicator_bar=self._on_bar,
        )
        pipe.seed()                                   # blocking REST seed (~2-3s) — in daemon thread
        self._pipelines[sym] = pipe                   # now visible to the dashboard
        if sym not in self._all_syms:
            self._all_syms.append(sym)                # same list the reconnect closure uses
        self._subscribe_market_data([sym])            # live ticks + official bars
        try:
            self._ensure_exit_pipe(sym)               # exit pipe if exit candles ≠ entry
        except Exception as exc:
            logger.warning("[%s] hot-add exit-pipe build failed: %s", sym, exc)
        try:
            self._ensure_idx_stream(sym)              # index stream if dual & index candle ≠ stock
        except Exception as exc:
            logger.warning("[%s] hot-add idx-stream build failed: %s", sym, exc)
        logger.info("[%s] HOT-ADDED — pipeline seeded, market data subscribed; "
                    "live on dashboard and tradable", sym)

    # =========================================================================
    # AUTO-RESEED ON FLOW-PARAM CHANGE  (2026-06-03)
    # Excel edits to a symbol's MACD flow params (resolution, seed bars, fast/
    # slow/signal, price source, history depth) are picked up LIVE: a fresh
    # pipeline is rebuilt + reseeded and atomically swapped in, so the symbol
    # never runs on stale/unseeded MACD. No restart needed. Fixes the long-
    # standing "reseed gotcha" (resolution change mid-session left it cold).
    # Mirrors _hotadd_one; additive + per-symbol + fail-safe.
    # =========================================================================
    def _flow_fp(self, sym, cfg):
        """Hashable fingerprint of a symbol's flow-relevant MACD params.

        2026-06-11: for a DUAL stock+index symbol (in _idx_pipes), ALSO fold in
        the INDEX-sheet params (as_index=True) under distinct 'idx::' keys, so an
        edit to the symbol's Indexes-sheet row (resolution/fast/slow/signal/gates)
        is DETECTED and auto-reseeds the index pipeline. Without this, dual-symbol
        index edits only took effect after a full restart. Pure stocks (no
        _idx_pipes entry) are unchanged; pure indexes return the same dict twice
        (stable, harmless)."""
        from indicators import resolve_symbol_params
        items = dict(resolve_symbol_params(sym, cfg))
        if sym in self._idx_pipes:
            for k, v in resolve_symbol_params(sym, cfg, as_index=True).items():
                items[f"idx::{k}"] = v
        return tuple(sorted(items.items()))

    def _reseed_changed_symbols(self):
        """Detect Excel flow-param changes on existing pipelines and reseed them."""
        try:
            cfg = load_config()
        except Exception as exc:
            logger.warning("reseed sync: config read failed: %s", exc)
            return
        seen = set()
        for sym in list(self._pipelines.keys()) + list(self._idx_pipes.keys()):
            if sym in seen:
                continue
            seen.add(sym)
            try:
                fp = self._flow_fp(sym, cfg)
            except Exception as exc:
                logger.warning("[%s] reseed sync: fingerprint failed: %s", sym, exc)
                continue
            old = self._pipe_fp.get(sym)
            if old is None:
                self._pipe_fp[sym] = fp        # first sight — baseline, no reseed
                continue
            if fp != old:
                # Only rebuild the ENTRY pipeline when an entry-relevant param
                # changed; a change to exit_resolution ALONE just re-syncs the
                # exit pipeline (leaves the entry pipe — and any open entry
                # signal evaluation — untouched, so we never miss an entry).
                entry_changed = ({k: v for k, v in dict(old).items() if k != "exit_resolution"}
                                 != {k: v for k, v in dict(fp).items() if k != "exit_resolution"})
                if entry_changed:
                    try:
                        self._reseed_one(sym, fp, cfg)
                    except Exception as exc:
                        logger.warning("[%s] auto-reseed (entry) failed (retry next cycle): %s", sym, exc)
                try:
                    self._ensure_exit_pipe(sym)        # create/refresh/remove exit pipe per new config
                except Exception as exc:
                    logger.warning("[%s] auto-reseed (exit) failed (retry next cycle): %s", sym, exc)
                try:
                    self._ensure_idx_stream(sym)       # create/refresh/remove index stream (dual symbols)
                except Exception as exc:
                    logger.warning("[%s] auto-reseed (idx-stream) failed (retry next cycle): %s", sym, exc)
                self._pipe_fp[sym] = fp

    def _retry_unseeded_pipes(self):
        """2026-06-11 (B2 — fixed): recover a pipeline left NOT-READY (can't yet
        compute MACD), e.g. one whose seed returned 0 bars. Three guards prevent the
        thrash the first version caused (which BROKE trailing by tearing exit pipes
        down every cycle):
          1. Gate on is_ready() (functional), NOT is_real_seeded(): a pipe that is
             accumulating live bars becomes is_ready() and is LEFT ALONE — even if
             its initial seed was cold.
          2. Exit pipes are re-seeded IN PLACE (pipe.seed()) — NEVER torn down. A
             teardown cancels the exit stream + resets the bar count, so trail/BE
             (which runs on exit-bar closes) never gets to run.
          3. Hard per-pipe retry CAP so a symbol with no data (e.g. SOXL) or any
             churn stops after a few tries instead of hammering every cycle.
        All re-seeds route through the engine historical rate-limiter."""
        # 2026-07-07: BREAK THE RECEIVE-DEATH CHURN LOOP. During a receive-death
        # the HMDS farm still reports OK, so the cap-reset below fired fresh
        # re-seeds EVERY cycle — re-seeding each cold pipe ~14x/day, which only
        # added historical-request load and DEEPENED the receive-death (a self-
        # reinforcing loop). Re-seeding cannot succeed while the receive channel
        # is dead anyway (no bars flow). Skip the whole retry pass until the
        # channel is alive again — seeds resume automatically on recovery.
        try:
            _tws_recv = getattr(self._engine, "_tws", None)
            _recv_dead = _tws_recv is not None and _tws_recv.is_receive_broken()
            _hmds_dead = getattr(_tws_recv, "_hmds_farm_ok", None) is False
            # 2026-07-13 FIX: the comment below always claimed 162-throttle was
            # skipped, but the condition never checked it. _hmds_error is True
            # while the 162 pacing cooldown is active -> pause reseeds so the
            # watchdog stops feeding the pacing loop (the recurring death spiral).
            _hmds_162  = bool(getattr(_tws_recv, "_hmds_error", False))
            if _recv_dead or _hmds_dead or _hmds_162:
                # 2026-07-07: also skip when the HMDS (historical) farm is
                # 162-throttled — re-seeding then only fires MORE historical
                # requests → more 162/pacing → pipes stay cold → churns (3,816
                # reseeds on 07-07). No point retrying when seeds can't succeed.
                #
                # 2026-07-29: log ONCE per skip-episode instead of once per
                # cycle. This fired every 30s for as long as the condition held
                # — 73 identical INFO lines on 07-28, 60 of them after the
                # 15:55 post-close disconnect when nothing could be done anyway.
                # The state still gates the reseed exactly as before; only the
                # logging is deduplicated, and the resume is logged too so the
                # episode has a visible start and end.
                _why = ("receive channel dead" if _recv_dead
                        else "HMDS farm 162-throttled")
                if getattr(self, "_reseed_skip_reason", None) != _why:
                    self._reseed_skip_reason = _why
                    logger.info("unseeded-retry SKIPPED — %s; not re-seeding "
                                "(would worsen the pacing/162 loop); resumes on "
                                "recovery. Further identical skips suppressed "
                                "until this clears.", _why)
                return
            if getattr(self, "_reseed_skip_reason", None) is not None:
                logger.info("unseeded-retry RESUMED — %s cleared",
                            self._reseed_skip_reason)
                self._reseed_skip_reason = None
        except Exception:
            pass
        # 2026-06-16: cold pipes must SELF-HEAL, never permanently give up. The
        # per-pipe cap only existed to avoid pounding a genuinely dead symbol —
        # but a cold pipe at the open is almost always a TRANSIENT farm-not-ready
        # issue (the 9:00 cold start). So while the HMDS farm is NOT known-broken
        # we RESET the caps every cycle, letting cold pipes keep retrying
        # (pacing-cap-limited) until they seed. They now recover automatically
        # when the farm comes up — no restart needed. (This is the root fix for
        # the recurring "18 of 21 pipes stuck cold all day" problem.)
        MAX_RETRIES = 8
        if not hasattr(self, "_cold_retry"):
            self._cold_retry = {}
        if not hasattr(self, "_last_cap_reset_at"):
            self._last_cap_reset_at = 0.0
        # 2026-07-07: only RESET the retry caps at most once every 120s (was every
        # cycle). The every-cycle reset, combined with an HMDS farm that FLAPS
        # (162→broken→ok→broken), kept re-enabling unlimited reseeds → thousands
        # of them. A time-gated reset still lets cold pipes self-heal once the
        # farm is STABLY healthy, without the flap-driven churn.
        try:
            _tws_h = getattr(self._engine, "_tws", None)
            _now_cap = time.monotonic()
            if (getattr(_tws_h, "_hmds_farm_ok", None) is not False
                    and (_now_cap - self._last_cap_reset_at) > 120.0):
                self._cold_retry = {}      # farm stably healthy -> fresh retries
                self._last_cap_reset_at = _now_cap
        except Exception:
            pass
        def _not_ready(p):
            # 2026-07-14 FIX: only re-seed a GENUINELY COLD pipe (no real
            # historical data). A pipe that is real_seeded but not yet is_ready()
            # is just WAITING for its first live bar (the long pre-open gap after
            # the pre-market seed) — re-seeding it fires needless historical
            # requests and eventually kills the receive channel (216 reseeds +
            # RECEIVE CHANNEL DEAD on the 07-14 open). Leave it alone; it flips
            # ready on its own at the first live bar.
            try:
                if p is None or p.is_ready():
                    return False
                _real = getattr(p, "is_real_seeded", None)
                if callable(_real) and _real():
                    return False          # seeded, just waiting for a live bar
                return True               # genuinely cold seed -> re-seed
            except Exception:
                return False
        try:
            cfg = load_config()
        except Exception as exc:
            logger.warning("unseeded retry: config read failed: %s", exc)
            return
        # Entry + index pipes — atomic-swap rebuild via _reseed_one, capped.
        for sym in set(list(self._pipelines.keys()) + list(self._idx_pipes.keys())):
            if _not_ready(self._pipelines.get(sym)) or _not_ready(self._idx_pipes.get(sym)):
                k = ("ei", sym); n = self._cold_retry.get(k, 0)
                if n >= MAX_RETRIES:
                    continue
                self._cold_retry[k] = n + 1
                try:
                    logger.warning("[%s] NOT-READY pipe — re-seed %d/%d (rate-limited)", sym, n + 1, MAX_RETRIES)
                    self._reseed_one(sym, self._flow_fp(sym, cfg), cfg)
                except Exception as exc:
                    logger.warning("[%s] pipe re-seed failed: %s", sym, exc)
            else:
                self._cold_retry.pop(("ei", sym), None)        # recovered → reset cap
        # Exit pipes — re-seed IN PLACE (NO teardown — teardown breaks the trail). Capped.
        for sym in list(self._exit_pipes.keys()):
            ep = self._exit_pipes.get(sym)
            if _not_ready(ep):
                k = ("exit", sym); n = self._cold_retry.get(k, 0)
                if n >= MAX_RETRIES:
                    continue
                self._cold_retry[k] = n + 1
                try:
                    logger.warning("[%s] NOT-READY exit pipe — in-place re-seed %d/%d", sym, n + 1, MAX_RETRIES)
                    ep.seed()
                except Exception as exc:
                    logger.warning("[%s] exit-pipe in-place re-seed failed: %s", sym, exc)
            else:
                self._cold_retry.pop(("exit", sym), None)

    def _reseed_one(self, sym, new_fp, cfg):
        """Rebuild + reseed a symbol whose flow params changed; re-point its
        official-bar stream if the resolution changed. Handles a symbol that is
        both a traded stock and an index. Swap is atomic (dict set) so live
        callbacks see either the old or the new pipeline, never a broken one."""
        rebuilt = []
        for is_index, store in ((False, self._pipelines), (True, self._idx_pipes)):
            old_pipe = store.get(sym)
            if old_pipe is None:
                continue
            old_res = (old_pipe._params.get("resolution", "") or "").strip().lower()
            newpipe = IndicatorPipeline(
                symbol=sym,
                broker=self,
                is_index=is_index,
                on_indicator_bar=(None if is_index else self._on_bar),
            )
            newpipe.seed()                              # blocking REST seed (~2-3s) in daemon thread
            store[sym] = newpipe                        # atomic swap into the live dict
            new_res = (newpipe._params.get("resolution", "") or "").strip().lower()
            rebuilt.append((old_res, new_res, newpipe))
        if not rebuilt:
            return
        old_res, new_res, newpipe = rebuilt[-1]
        if new_res != old_res:
            self._resubscribe_official_bar(sym, new_res)
        self._pipe_fp[sym] = new_fp
        logger.warning("[%s] AUTO-RESEEDED %d pipe(s) — seeded=%s ready=%s  res %s -> %s",
                       sym, len(rebuilt), newpipe.is_seeded(), newpipe.is_ready(),
                       old_res, new_res)

    def _resubscribe_official_bar(self, sym, new_res):
        """Resolution changed (entry/exit/index candles edited in Excel): tear the
        symbol's single 10s base stream + aggregator down and rebuild them so the
        new target resolutions are picked up live. No per-resolution stream any
        more — one 10s stream aggregates locally (see _ensure_base_stream)."""
        tws = getattr(self._engine, "_tws", None)
        old = self._base_hist_req_map.pop(sym, None)
        if old is None:
            old = self._hist_req_map.get(sym)
        if tws is not None and old is not None:
            try:
                tws.cancelHistoricalData(old)
            except Exception as exc:
                logger.warning("[%s] cancel old base stream failed: %s", sym, exc)
        self._hist_req_map.pop(sym, None)
        self._aggregators.pop(sym, None)          # rebuild targets fresh
        self._ensure_base_stream(sym)

    def _ensure_base_stream(self, sym):
        """2026-07-08: subscribe ONE 10-second keepUpToDate stream per symbol and
        wire a BarAggregator that rolls it up into the symbol's ENTRY (5m), EXIT
        (3m) and INDEX bars locally. Idempotent: updates the aggregator targets
        on every call and opens the 10s stream at most once. This replaces the
        separate entry/exit/index streams — collapsing 2–3 IBKR historical lines
        per symbol down to 1 (staying under the ~50 concurrent-line cap and
        killing the exit-pipe reseed churn), while keeping entry ≠ exit ≠ index
        timeframes intact."""
        from indicators import NTickBarAggregator, resolution_to_seconds
        tws = getattr(self._engine, "_tws", None)
        if tws is None:
            return

        def _timebased(r):
            return bool(r) and r != "tick" and NTickBarAggregator.parse_resolution(r) is None

        try:
            entry_res, exit_res = self._exit_resolutions(sym)
        except Exception:
            entry_res, exit_res = "", ""

        agg = self._aggregators.get(sym)
        if agg is None:
            agg = BarAggregator()
            self._aggregators[sym] = agg

        # ENTRY target
        if _timebased(entry_res):
            agg.set_target("entry", resolution_to_seconds(entry_res))
        else:
            agg.remove_target("entry")
        # EXIT target (only when it differs from entry and is time-based)
        if _timebased(exit_res) and exit_res != entry_res:
            agg.set_target("exit", resolution_to_seconds(exit_res))
        else:
            agg.remove_target("exit")
        # INDEX target (dual stock+index symbol whose index candle differs)
        idx_pipe = self._idx_pipes.get(sym); stock_pipe = self._pipelines.get(sym)
        if idx_pipe is not None and stock_pipe is not None:
            idx_res = (idx_pipe._params.get("resolution", "") or "").strip().lower()
            if _timebased(idx_res) and idx_res != entry_res:
                agg.set_target("index", resolution_to_seconds(idx_res))
            else:
                agg.remove_target("index")

        # any legacy per-resolution stream routing is now dead
        self._exit_hist_req_map.pop(sym, None)
        self._idx_hist_req_map.pop(sym, None)

        if not agg.targets:
            return                                # pure tick/N-tick symbol → no base stream
        if self._base_hist_req_map.get(sym) is not None:
            return                                # 10s stream already open (targets updated above)

        conid = self._conid_cache.get(sym)
        if not conid:
            return
        from ibapi.contract import Contract
        c = Contract()
        c.conId = conid; c.exchange = "SMART"; c.currency = "USD"; c.secType = "STK"
        base_req_id = 20000 + (self._req_id_map.get(sym, 10000) - 10000)
        self._hist_req_map[sym] = base_req_id
        self._base_hist_req_map[sym] = base_req_id
        try:
            tws.reqHistoricalDataStream(
                base_req_id, sym, c, bar_size="10 secs", duration="900 S")
            logger.warning("[%s] 10s BASE stream → aggregates %s (reqId=%d)",
                           sym, dict(agg.targets), base_req_id)
        except Exception as exc:
            self._base_hist_req_map.pop(sym, None)
            logger.warning("[%s] 10s base-stream subscribe failed: %s", sym, exc)

    # =========================================================================
    # SEPARATE EXIT PIPELINE  (2026-06-03)
    # A symbol whose EXIT candle resolution differs from its ENTRY resolution
    # gets a second IndicatorPipeline (same MACD params, exit timeframe) with
    # its own official-bar stream (reqId 30000+). It drives EXIT logic only
    # (via _on_exit_bar). Created/refreshed/removed live by _ensure_exit_pipe,
    # which the startup, hot-add, and auto-reseed paths all call — so you can
    # change entry/exit candles in Excel mid-day with NO restart. Idempotent +
    # fail-safe; default (exit==entry) means no exit pipe and current behavior.
    # =========================================================================
    def _exit_resolutions(self, sym):
        """Return (entry_res, exit_res) for `sym` from current config."""
        from indicators import resolve_symbol_params
        p = resolve_symbol_params(sym, load_config())
        return ((p.get("entry_resolution") or p.get("resolution") or "").strip().lower(),
                (p.get("exit_resolution") or "").strip().lower())

    def _ensure_exit_pipe(self, sym):
        """Make the EXIT pipeline match config: build/refresh it when exit≠entry,
        remove it when exit==entry. Subscribes a dedicated exit-timeframe stream."""
        try:
            entry_res, exit_res = self._exit_resolutions(sym)
        except Exception as exc:
            logger.warning("[%s] exit-pipe resolve failed: %s", sym, exc)
            return
        differ = bool(exit_res) and exit_res != entry_res
        if not differ:
            if sym in self._exit_pipes:
                self._teardown_exit_pipe(sym)          # exit==entry now → drop exit pipe
            return
        cur = self._exit_pipes.get(sym)
        cur_res = (cur._params.get("resolution", "") if cur else "").strip().lower()
        if cur is not None and cur_res == exit_res:
            return                                      # already correct
        newpipe = IndicatorPipeline(
            symbol=sym, broker=self, is_index=False,
            on_indicator_bar=self._on_exit_bar, resolution_override=exit_res)
        newpipe.seed()
        self._exit_pipes[sym] = newpipe                 # atomic swap
        self._subscribe_exit_stream(sym, exit_res)
        logger.warning("[%s] EXIT pipeline @ %s candles (entry @ %s) — seeded=%s ready=%s",
                       sym, exit_res, entry_res, newpipe.is_seeded(), newpipe.is_ready())

    def _subscribe_exit_stream(self, sym, exit_res):
        """2026-07-08: the EXIT pipe is now fed by the symbol's shared 10s base
        stream, aggregated to the exit timeframe locally — no dedicated exit
        stream. Just (re)build the aggregator so its 'exit' target is set."""
        tws = getattr(self._engine, "_tws", None)
        old = self._exit_hist_req_map.pop(sym, None)     # drop any legacy exit stream
        if tws is not None and old is not None:
            try:
                tws.cancelHistoricalData(old)
            except Exception as exc:
                logger.warning("[%s] cancel old exit-stream failed: %s", sym, exc)
        self._ensure_base_stream(sym)

    def _teardown_exit_pipe(self, sym):
        """Remove the exit pipeline + cancel its stream (exit==entry again)."""
        tws = getattr(self._engine, "_tws", None)
        old = self._exit_hist_req_map.pop(sym, None)
        if tws is not None and old is not None:
            try:
                tws.cancelHistoricalData(old)
            except Exception:
                pass
        if self._exit_pipes.pop(sym, None) is not None:
            logger.warning("[%s] EXIT pipeline removed — exit candles now == entry", sym)

    def _ensure_idx_stream(self, sym):
        """2026-07-08: a DUAL stock+index symbol whose INDEX candle differs from
        its STOCK candle now gets its index bars from the shared 10s base stream,
        aggregated locally (the aggregator's 'index' target). No dedicated index
        stream. Idempotent + fail-safe — just drop any legacy stream and rebuild
        the base-stream targets."""
        try:
            tws = getattr(self._engine, "_tws", None)
            old = self._idx_hist_req_map.pop(sym, None)
            if tws is not None and old is not None:
                try: tws.cancelHistoricalData(old)
                except Exception: pass
            self._ensure_base_stream(sym)
        except Exception as exc:
            logger.warning("[%s] _ensure_idx_stream failed: %s", sym, exc)

    def stop(self):
        self._running = False
        # Dump bot-side state BEFORE engine.stop() so the engine teardown
        # log lines come AFTER the snapshot.
        try:
            self._dump_state(reason="shutdown")
        except Exception as exc:
            logger.error("Bot state dump on shutdown failed: %s", exc)
        if self._engine:
            self._engine.stop()
        logger.info("MACDBot stopped")

    # =========================================================================
    # MACD STATUS LOG  (periodic dump of indicator values to log file)
    # =========================================================================

    def _start_macd_status_log(self) -> None:
        """
        Spawn a daemon thread that emits one MACD STATUS line per active
        pipeline every MACDStatusLogIntervalSec seconds (default 30).

        Output is grep-able from log files — same values the dashboard shows,
        for sessions where the dashboard isn't reachable or for post-mortem
        analysis. Single fixed-width line per symbol per interval.
        """
        cfg = load_config()
        interval = int(cfg.get("MACDStatusLogIntervalSec", 30))
        if interval <= 0:
            logger.info("MACD STATUS log disabled (interval=%d)", interval)
            return

        def _log_loop():
            # Brief warm-up wait so the first line lands AFTER the open
            # README of the bot startup chatter.
            time.sleep(5)
            while self._running:
                try:
                    self._emit_macd_status_lines()
                except Exception as exc:
                    logger.debug("MACD status log emit failed: %s", exc)
                try:
                    self._sweep_unmanaged_positions()
                except Exception as exc:
                    logger.debug("unmanaged-position sweep failed: %s", exc)
                time.sleep(interval)

        t = threading.Thread(target=_log_loop, daemon=True, name="macd-status-log")
        t.start()
        logger.info("MACD STATUS log started (interval=%ds)", interval)

    def _sweep_unmanaged_positions(self) -> None:
        """2026-06-08 safety net: detect & flatten any TWS position NOT in the
        bot's _open_positions (e.g. a late fill after a 60s no-fill cancel that
        left an unprotected/unmanaged position). A 45s grace avoids racing the
        normal on_fill registration. Skips when receive is broken (stale data)."""
        GRACE = 45.0
        try:
            tws = getattr(self._engine, "_tws", None)
            if tws is not None and hasattr(tws, "is_receive_broken") and tws.is_receive_broken():
                return
            ibkr_pos = self._engine.get_positions(timeout=3) or {}
        except Exception:
            return
        now = time.time()
        for sym, info in list(ibkr_pos.items()):
            try:
                qty = abs(int(info.get("qty", 0) or 0))
            except Exception:
                qty = 0
            if qty == 0:
                self._unmanaged_seen.pop(sym, None)
                continue
            with self._pos_lock:
                managed = sym in self._open_positions
            if managed:
                self._unmanaged_seen.pop(sym, None)
                continue
            first = self._unmanaged_seen.setdefault(sym, now)
            if now - first < GRACE:
                continue
            side = str(info.get("side", "")).upper()
            close_side = "BUY" if side == "SHORT" else "SELL"
            conid = self._conid_cache.get(sym) or info.get("conid")
            logger.error("[%s] UNMANAGED/PHANTOM position (%s %d, unmanaged %.0fs, no bot stop) "
                         "— FLATTENING for safety", sym, side, qty, now - first)
            if conid:
                try:
                    self._engine.submit_market_close(symbol=sym, conid=int(conid),
                                                     side=close_side, quantity=qty,
                                                     order_ref=f"PHANTOM_FLAT_{sym}_{int(now)}")
                    self._unmanaged_seen.pop(sym, None)
                except Exception as exc:
                    logger.error("[%s] phantom flatten failed: %s — retry next sweep", sym, exc)
            else:
                logger.error("[%s] phantom flatten: no conid — cannot auto-close", sym)

    def _emit_macd_status_lines(self) -> None:
        """One log line per pipeline. Format greppable by 'MACD STATUS'."""
        pipes = {**self._pipelines, **self._idx_pipes}
        if not pipes:
            return
        for sym in sorted(pipes.keys()):
            pipe = pipes.get(sym)
            if pipe is None:
                continue
            try:
                eng = pipe._engine
                fast = eng._ef.value
                slow = eng._es.value
                if fast is None or slow is None:
                    continue
                macd_v = fast - slow
                sig_hist = list(eng._sig_hist or [])
                sig_v = sig_hist[0] if sig_hist else 0.0
                hist  = macd_v - sig_v
                ready = "READY" if eng.is_ready() else "WARM"
                armed = "ARMED" if not pipe._intrabar_fired_for_current_bar else "FIRED"
                side  = "F>S" if macd_v > 0 else ("F<S" if macd_v < 0 else "F=S")
                tag   = "IDX" if sym in self._idx_pipes else "STK"
                logger.info(
                    "MACD STATUS  %s  %-6s  res=%-6s  fast=%9.4f  slow=%9.4f  "
                    "MACD=%+.5f  Sig=%+.5f  Hist=%+.5f  "
                    "DM+=%+.5f  DM-=%+.5f  %s  %s  %s",
                    tag, sym, pipe._params.get("resolution", "?"),
                    fast, slow, macd_v, sig_v, hist,
                    eng._diff_means.pos_mean, eng._diff_means.neg_mean,
                    side, ready, armed,
                )
            except Exception:
                continue

    # =========================================================================
    # SEED RETRY  (defense against HMDS 162 "different IP" errors)
    # =========================================================================

    def _retry_failed_seeds(self, cfg: Dict) -> None:
        """
        Re-seed any pipeline that came back cold (0 bars) on first attempt.
        Retries every 30s until all pipelines have real bars or we hit the
        pre-open deadline (T-2 min before TimeToStartTrading).

        HMDS 162 sets a 60s cooldown on the TWSClient (see _hmds_error
        property). Once the cooldown expires the next attempt will actually
        hit IBKR again instead of short-circuiting. If the user closed the
        other IBKR session (mobile / web TWS / another Gateway) during the
        cooldown, the retry succeeds and the pipeline is properly warmed
        instead of needing 10 live bars after the open.

        Runs synchronously inside start() — no thread/lock complexity because
        market data subscription has not yet happened, so no live ticks are
        competing for pipeline state.
        """
        all_pipes = {**self._pipelines, **self._idx_pipes}
        cold = [sym for sym, p in all_pipes.items() if not p.is_real_seeded()]
        if not cold:
            logger.info("Seed retry: all %d pipelines seeded on first pass — skipping",
                        len(all_pipes))
            return

        # Compute deadline = today's TimeToStartTrading minus 2 minutes ET.
        # If we're already past it, skip retries — better to start trading
        # cold than to delay even further.
        try:
            start_str = cfg.get("TimeToStartTrading", "09:30")
            h, m = map(int, start_str.split(":"))
            now_est = datetime.now(EST)
            deadline = now_est.replace(hour=h, minute=m, second=0, microsecond=0)
            deadline = deadline - timedelta(minutes=2)
        except Exception as exc:
            logger.warning("Seed retry: deadline parse failed (%s) — using +5min", exc)
            deadline = datetime.now(EST) + timedelta(minutes=5)

        retry_interval_sec = 30
        max_rounds = 12              # 12 × 30s = 6 min max, in case deadline very far

        logger.warning("Seed retry: %d cold pipelines (%s) — retrying every %ds until %s ET",
                       len(cold), ", ".join(cold), retry_interval_sec,
                       deadline.strftime("%H:%M:%S"))

        for round_n in range(1, max_rounds + 1):
            now_est = datetime.now(EST)
            if now_est >= deadline:
                logger.error("Seed retry: deadline reached (%s ET) — giving up with %d "
                             "pipelines still cold: %s. Engine will need %d live bars to "
                             "warm naturally.",
                             deadline.strftime("%H:%M:%S"), len(cold), ", ".join(cold),
                             int(cfg.get("MACD_HistoryDepth", 10)))
                break

            seconds_to_deadline = (deadline - now_est).total_seconds()
            sleep_for = min(retry_interval_sec, max(1.0, seconds_to_deadline - 1.0))
            logger.info("Seed retry round %d: sleeping %.0fs before next attempt "
                        "(%d cold, deadline in %.0fs)",
                        round_n, sleep_for, len(cold), seconds_to_deadline)
            time.sleep(sleep_for)

            still_cold = []
            for sym in cold:
                pipe = all_pipes.get(sym)
                if pipe is None:
                    continue
                try:
                    pipe.reset()
                    pipe.seed()
                    if pipe.is_real_seeded():
                        logger.info("Seed retry: [%s] RECOVERED — pipeline now warm", sym)
                    else:
                        still_cold.append(sym)
                except Exception as exc:
                    logger.warning("Seed retry: [%s] re-seed raised %s — keeping cold", sym, exc)
                    still_cold.append(sym)
            cold = still_cold
            if not cold:
                logger.warning("Seed retry: ALL pipelines now warm after round %d", round_n)
                break

        # Pre-open READY CHECK — single clear line summarising what will and
        # won't trade out of the gate. Greppable: "READY CHECK".
        warm = [s for s, p in all_pipes.items() if p.is_real_seeded()]
        cold_final = [s for s, p in all_pipes.items() if not p.is_real_seeded()]
        logger.warning("READY CHECK: %d/%d pipelines seeded with bars.  warm=%s  cold=%s",
                       len(warm), len(all_pipes),
                       ",".join(sorted(warm)) or "(none)",
                       ",".join(sorted(cold_final)) or "(none)")

    # =========================================================================
    # DIAGNOSTIC STATE DUMP — fires on shutdown and on long disconnect (>2.5m)
    # =========================================================================

    def _dump_state(self, reason: str = "manual") -> None:
        """
        Log every piece of bot-side state useful for a post-mortem:
        running flag, conid cache, open positions (with direction / entry /
        stop / qty / be flag / max-high / min-low), pipeline counts.
        """
        try:
            now = datetime.now(EST).strftime("%Y-%m-%d %H:%M:%S %Z")
            logger.warning("=" * 78)
            logger.warning("BOT STATE DUMP — reason=%s  ts=%s", reason, now)
            logger.warning("-" * 78)
            logger.warning("running=%s  pipelines=%d  index_pipes=%d  conid_cache=%d",
                           self._running, len(self._pipelines),
                           len(self._idx_pipes), len(self._conid_cache))
            with self._pos_lock:
                positions = {s: dict(p) for s, p in self._open_positions.items()}
            if positions:
                logger.warning("Internal _open_positions: %d", len(positions))
                for sym in sorted(positions):
                    p = positions[sym]
                    logger.warning(
                        "  [%s] dir=%s  entry=%.4f  stop=%.4f  qty=%s  be=%s  "
                        "max_hi=%.4f  min_lo=%.4f",
                        sym,
                        p.get("direction", "?"),
                        float(p.get("entry_price", 0.0) or 0.0),
                        float(p.get("stop_price", 0.0) or 0.0),
                        p.get("qty", "?"),
                        bool(p.get("be_activated", False)),
                        float(p.get("max_high", 0.0) or 0.0),
                        float(p.get("min_low", 0.0) or 0.0),
                    )
            else:
                logger.warning("Internal _open_positions: (empty)")
            logger.warning("=" * 78)
        except Exception as exc:
            logger.error("Bot _dump_state error: %s", exc)

    def _on_long_disconnect(self, duration_sec: float) -> None:
        """
        Called by the engine watchdog the first time TWS has been
        disconnected for ≥ 2.5 min. Dump bot state immediately — the
        positions in IBKR are still alive; we just can't see them.
        """
        logger.error("BOT received long-disconnect notification: %.1fs", duration_sec)
        self._dump_state(reason=f"long_disconnect_{int(duration_sec)}s")

    # =========================================================================
    # MARKET DATA (TWS)
    # =========================================================================

    def _subscribe_market_data(self, symbols):
        """
        Subscribe to real-time ticks for all symbols via TWS.
        Overrides tickByTickAllLast on the TWSClient instance so TWS
        callbacks route to this bot's pipeline correctly.
        """
        if not (self._engine and self._engine._tws and
                self._engine._tws.is_connected):
            logger.warning("TWS not connected — market data subscription skipped")
            return

        _bar_sec = int(load_config().get("MACD_SecBarSize", 30))
        if _bar_sec <= 5:
            logger.warning(
                "5-SECOND BARS: Live market data permissions REQUIRED. "
                "Delayed data (10189 fallback) is too slow for 5s bars. "
                "Subscribe via IBKR Account Management → Market Data Subscriptions.")

        tws = self._engine._tws
        tws._conid_for_reqid = {}                        # reqId → conid map for 10189 fallback

        # FIX 3: Also handle tickPrice/tickSize callbacks from reqMktData fallback
        bot = self

        def _tick_callback(req_id, time_stamp, price, size,
                           tick_attrib_last, exchange, special_conds):
            """Called by TWS on each Last tick (reqTickByTickData)."""
            sym = next((s for s, r in bot._req_id_map.items() if r == req_id), None)
            if not sym:
                return
            # SHADOW (2026-08-06): the fill simulator prices off the same real
            # trade ticks the MACD pipeline uses — the data side is untouched.
            try:
                from shadow_broker import SHADOW
                SHADOW.on_price(sym, last=price)
            except Exception:
                pass
            tick = {
                "ts"      : datetime.fromtimestamp(time_stamp, tz=EST).replace(tzinfo=None),
                "price"   : price,
                "high"    : price,
                "low"     : price,
                "tick_vol": size,
            }
            # FIX: feed BOTH stock and index pipelines if both exist for this
            # symbol (when an ETF is in both Stocks and Indexes sheets, e.g.
            # QQQ traded as a stock AND used as the index filter for AAPL).
            # The old `or` short-circuit starved the index pipeline of ticks,
            # which made check_index_alignment return "MACD not ready yet" and
            # block every index-filtered signal.
            stock_pipe = bot._pipelines.get(sym)
            idx_pipe   = bot._idx_pipes.get(sym)
            if stock_pipe is not None:
                stock_pipe.on_raw_tick(tick)
            if idx_pipe is not None:
                idx_pipe.on_raw_tick(tick)

        def _tick_price_callback(req_id, tick_type, price, attrib):
            """Called by TWS for reqMktData live data (tickType 4=last, 1=bid, 2=ask)."""
            # 2026-05-26 (two-subscription model): feed ONLY last-trade ticks
            # (4=last, 68=delayed last) to the intra-bar path. Bid(1)/ask(2) used
            # to pollute the price series (and the old tick-built candles). The
            # committed MACD now comes from IBKR's official TRADES bars; intra-bar
            # crosses should trigger on the actual trade price, not the quote.
            #
            # SHADOW (2026-08-06): the shadow fill engine DOES want bid/ask —
            # a market order fills at the ask, not the last trade. So feed the
            # shadow book first, with the full book, then apply the last-trade
            # filter that protects the MACD pipeline.
            sym = next((s for s, r in bot._req_id_map.items() if r == req_id), None)
            if sym and price and price > 0:
                try:
                    from shadow_broker import SHADOW
                    SHADOW.on_tick_type(sym, tick_type, price)
                except Exception:
                    pass
            if tick_type not in (4, 68):
                return
            if not sym or price <= 0:
                return
            tick = {
                "ts"      : datetime.now(EST).replace(tzinfo=None),
                "price"   : price,
                "high"    : price,
                "low"     : price,
                "tick_vol": 0, "bar_vol": 0, "daily_vol": 0,
            }
            # FIX: feed both pipelines (see comment above in _tick_callback).
            stock_pipe = bot._pipelines.get(sym)
            idx_pipe   = bot._idx_pipes.get(sym)
            if stock_pipe is not None:
                stock_pipe.on_raw_tick(tick)
            if idx_pipe is not None:
                idx_pipe.on_raw_tick(tick)

        tws.tickByTickAllLast = _tick_callback           # tick-by-tick callback
        tws._on_tick_price    = _tick_price_callback     # live/delayed mktdata callback (proper EWrapper routing)
        # 2026-05-26 (two-subscription model): route IBKR's official keepUpToDate
        # bars to the pipelines for the committed MACD.
        tws._on_official_bar  = self._route_official_bar

        from ibapi.contract import Contract
        from indicators import _resolution_to_tws_bar_size, NTickBarAggregator
        for sym in symbols:
            conid = self._conid_cache.get(sym)
            if not conid:
                continue
            req_id = self._next_req_id
            self._next_req_id += 1
            self._req_id_map[sym] = req_id

            contract        = Contract()
            contract.conId    = conid
            contract.exchange = "SMART"
            contract.currency = "USD"
            contract.secType  = "STK"

            # Go straight to reqMktData(live) — skip reqTickByTickData which always gets 10189
            # This account (DUXXXXXXX) has live market data subscriptions
            tws.reqMarketDataType(1)                     # 1 = live data
            tws.reqMktData(req_id, contract, "", False, False, [])
            tws._conid_for_reqid[req_id] = conid         # Store for 10089 fallback
            logger.info("[%s] Subscribed reqId=%d conid=%d", sym, req_id, conid)
            time.sleep(0.5)          # Stagger subscriptions — 16 at once crashes IB Gateway

            # 2026-05-26 (two-subscription model): for TIME-BASED resolutions,
            # also subscribe IBKR's OFFICIAL keepUpToDate bars — these drive the
            # committed MACD (no more drift from tick-built candles). N-tick /
            # tick resolutions have no official-bar equivalent and keep the live
            # tick aggregator, so we skip the stream for them.
            pipe = self._pipelines.get(sym) or self._idx_pipes.get(sym)
            res = (pipe._params.get("resolution", "") if pipe else "").strip().lower()
            if res and res != "tick" and NTickBarAggregator.parse_resolution(res) is None:
                # 2026-07-08: ONE 10-second keepUpToDate stream, aggregated locally
                # to this symbol's entry/exit/index bars (see _ensure_base_stream).
                self._ensure_base_stream(sym)
                time.sleep(0.4)      # stagger the historical-stream subscriptions too

    def _route_official_bar(self, sym: str, bar: Dict, is_final: bool, reqId: int = None) -> None:
        """
        2026-05-26 (two-subscription model): engine → pipeline bridge. Called by
        TWSClient.historicalDataUpdate when an OFFICIAL keepUpToDate bar
        completes. Commits the bar into the stock and/or index pipeline. Feeds
        BOTH if the symbol is in each (e.g. QQQ traded AND used as an index).

        2026-06-03: if reqId matches the symbol's EXIT-resolution stream, route
        the bar to the EXIT pipeline only (different timeframe). Otherwise it's
        the entry-resolution stream → stock + index pipelines (current path).

        2026-07-08: if reqId is the symbol's single 10s BASE stream, feed the 10s
        bar to its BarAggregator and route each rolled-up (entry/exit/index) bar
        to the matching pipe. Legacy per-resolution branches below are kept as a
        fallback (unused once the base stream is active).
        """
        try:
            # SHADOW (2026-08-06): safety net for the fill engine. If tick flow
            # for a symbol pauses, the 10s bar keeps the shadow book priced —
            # and its high/low let a stop or limit that was touched INSIDE the
            # bar still fill instead of being silently missed.
            if is_final:
                try:
                    from shadow_broker import SHADOW
                    SHADOW.on_bar(sym, bar.get("close"), bar.get("high"),
                                  bar.get("low"))
                except Exception:
                    pass

            # 10s BASE stream → aggregate locally → entry / exit / index pipes
            if reqId is not None and self._base_hist_req_map.get(sym) == reqId:
                agg = self._aggregators.get(sym)
                if agg is not None and is_final:
                    ep = _bar_epoch(bar.get("date"))
                    if ep is not None:
                        for name, abar in agg.on_bar(
                                ep, bar.get("open"), bar.get("high"),
                                bar.get("low"), bar.get("close"), bar.get("volume", 0)):
                            target = (self._pipelines.get(sym)  if name == "entry" else
                                      self._exit_pipes.get(sym) if name == "exit"  else
                                      self._idx_pipes.get(sym))
                            if target is not None:
                                target.on_official_bar({
                                    "current_time": bar.get("current_time"),
                                    "open": abar["open"], "high": abar["high"],
                                    "low": abar["low"],   "close": abar["close"],
                                    "price": abar["close"], "volume": abar["volume"],
                                    "daily_volume": 0}, True)
                return
            # INDEX-resolution stream → index pipeline only (dual stock+index sym)
            if reqId is not None and self._idx_hist_req_map.get(sym) == reqId:
                ip = self._idx_pipes.get(sym)
                if ip is not None:
                    ip.on_official_bar(bar, is_final)
                return
            # EXIT-resolution stream → exit pipeline only
            if reqId is not None and self._exit_hist_req_map.get(sym) == reqId:
                exit_pipe = self._exit_pipes.get(sym)
                if exit_pipe is not None:
                    exit_pipe.on_official_bar(bar, is_final)
                return
            stock_pipe = self._pipelines.get(sym)
            if stock_pipe is not None:
                stock_pipe.on_official_bar(bar, is_final)
            idx_pipe = self._idx_pipes.get(sym)
            # 2026-06-10: a DUAL symbol with its own INDEX stream is fed by that
            # stream (above). Only feed the index pipe from the entry stream when
            # it has NO separate index stream — i.e. a pure index, or as a safe
            # fallback if the index stream wasn't subscribed. Prevents the entry
            # (stock-resolution) bars from overwriting the index pipe's own res.
            if idx_pipe is not None and sym not in self._idx_hist_req_map:
                idx_pipe.on_official_bar(bar, is_final)
        except Exception as exc:
            logger.error("[%s] _route_official_bar error: %s", sym, exc)

    def _on_exit_bar(self, symbol: str, bar: Dict):
        """2026-06-03: EXIT-pipeline bar close (the exit candle resolution).
        Runs EXIT logic ONLY, for an open position, using this bar's exit-
        resolution MACD/close. Real bar closes only — intra-bar synthetic bars
        are skipped. Entry logic is never run here (that's the entry pipeline)."""
        if not self._running:
            return
        if bool(bar.get("_intrabar", False)):
            return
        with self._pos_lock:
            pos = self._open_positions.get(symbol)
        if pos:
            try:
                self._handle_exit(symbol, pos, bar)
            except Exception as exc:
                logger.error("[%s] _on_exit_bar exit error: %s", symbol, exc)

    # =========================================================================
    # ON EACH COMPLETED BAR  (called by IndicatorPipeline._on_indicator_bar)
    # =========================================================================

    def _on_bar(self, symbol: str, bar: Dict):
        """
        Called by IndicatorPipeline. Now invoked in TWO situations:

          1. Real bar close (from _on_second_bar_closed) — bar has full
             OHLC, may have `_cross_event` if zero-cross happened at close.
          2. Intra-bar synthetic cross (from _check_intrabar_cross) — bar
             has `_intrabar=True` and `_cross_event` set. OHLC is the
             current tick price (no real high/low). Exit logic must NOT
             run on these — would corrupt high/low-based trail checks.

        Flow:
          - Position open + REAL bar  → exit logic
          - Position open + intra-bar → ignore (exits run on real closes)
          - No position               → entry check (works for both)
        """
        if not self._running:
            return

        is_intrabar = bool(bar.get("_intrabar", False))

        # ── 2026-05-27: per-bar price/MACD diagnostic CSV (real bars only) ──
        if not is_intrabar:
            try:
                pipe = self._pipelines.get(symbol) or self._idx_pipes.get(symbol)
                PriceLogger.get().log(
                    symbol, bar,
                    resolution=pipe.get_resolution() if pipe else None,
                    seeded=pipe.is_seeded() if pipe else None,
                    ready=pipe.is_ready() if pipe else None,
                )
            except Exception:
                pass

        # ── Exit logic for open positions (REAL bar closes only) ──────────
        with self._pos_lock:
            pos = self._open_positions.get(symbol)

        if pos:
            # 2026-06-03: if a separate EXIT pipeline owns this symbol (exit
            # timeframe ≠ entry), exits are driven by that pipeline's bar
            # closes via _on_exit_bar — skip the entry pipeline's exit pass so
            # exits evaluate on the configured exit candles only.
            if symbol not in self._exit_pipes:
                if not is_intrabar:
                    self._handle_exit(symbol, pos, bar)                  # full exit stack on real close
                else:
                    # 2026-06-04: live (forming-bar) delta/gamma tech exit only
                    self._handle_exit(symbol, pos, bar, intrabar=True)
            return                    # never enter and exit on same bar

        # ── 2026-06-12 (per user): CROSS fires INTRA-BAR, PATTERN waits for CLOSE ─
        # A signal-line CROSS should fire the MOMENT it happens (intra-bar) and not
        # wait for the bar to close. A PATTERN (fast-line turn) still waits for the
        # real bar close. So we no longer reject every intra-bar bar here; instead
        # we run the entry check below and, on an intra-bar provisional eval, only
        # allow the entry through when a CROSS is part of the trigger (the
        # `is_intrabar and not action["cross"]` gate further down). Pattern-only
        # intra-bar signals are dropped and re-evaluated at the bar close.
        # (Supersedes the 2026-06-04 "real bar close only" behaviour.)

        # ── Entry logic ───────────────────────────────────────────────────
        with self._pos_lock:
            current_positions = {s: True for s in self._open_positions}

        bar["symbol"] = symbol                       # entry_logic needs symbol for per-stock config

        # ── 2026-06-11 (D): fail-closed ENTRY freshness guard ──────────────
        # Never open a position on a COLD / not-ready / STALE entry pipe (e.g.
        # one just recovering from a pacing flap). Requires really-seeded + ready
        # + last COMMITTED bar within ~2x the resolution. Guard CODE errors
        # fail-open (a bug here must not halt trading); actual staleness blocks.
        epipe = self._pipelines.get(symbol)
        if epipe is not None:
            try:
                from indicators import resolution_to_seconds
                _stale_lim = 2.0 * resolution_to_seconds(epipe.get_resolution())
                _age = epipe.get_last_bar_age_sec()
                if (not epipe.is_real_seeded()) or (not epipe.is_ready()) \
                        or (_age is None) or (_age > _stale_lim):
                    logger.warning("[%s] ENTRY BLOCKED — pipe not fresh "
                                   "(real_seeded=%s ready=%s last_bar_age=%s limit=%.0fs)",
                                   symbol, epipe.is_real_seeded(), epipe.is_ready(),
                                   None if _age is None else round(_age, 1), _stale_lim)
                    return
            except Exception as exc:
                logger.warning("[%s] entry freshness guard error (allowing): %s", symbol, exc)

        action = check_entry_signal(bar, open_positions=current_positions)

        # 2026-07-14: on a REAL bar close, if this READY+fresh symbol did NOT
        # produce an entry, log WHY (the entry-logic reason) so "no trades" is
        # self-explaining. Skipped on intra-bar evals to avoid per-tick spam.
        if action["signal"] == "NONE" and not is_intrabar:
            logger.info("[%s] NO-ENTRY @ bar close — %s",
                        symbol, action.get("reason") or "no trigger/gate reason given")

        if action["signal"] in ("LONG", "SHORT") and action.get("auto_mode", True):
            # ── 2026-06-12 (per user): intra-bar entries are CROSS-ONLY ─────
            # Fire a CROSS immediately (intra-bar); make a PATTERN wait for the
            # bar close. On an intra-bar provisional eval, drop a pattern-only
            # signal (no cross) — it will fire at the bar close instead.
            if is_intrabar and not action.get("cross", False):
                return
            # 2026-07-04 (per user): ML entry-gate REMOVED — no model check,
            # every qualified signal is taken straight through to entry.
            self._handle_entry(symbol, action, bar)
            # 2026-06-01: if this entry fired from an INTRA-BAR provisional
            # evaluation, mark the bar so (a) no further intra-bar entry fires
            # this bar and (b) the bar-close path is suppressed — one entry
            # per bar max. Reset happens at the next bar close.
            if is_intrabar:
                pipe = self._pipelines.get(symbol)
                if pipe is not None:
                    pipe._intrabar_fired_for_current_bar = True

    # =========================================================================
    # ENTRY
    # =========================================================================
    # 2026-07-04 (per user): the ML entry-gate (_ml_allows_entry) was removed.
    # There is no longer any model-based "take this trade or not" check — every
    # qualified signal proceeds directly to _handle_entry.

    def _handle_entry(self, symbol: str, action: Dict, bar: Dict):
        """Submit a bracket order to the engine."""
        conid = self._conid_cache.get(symbol)
        if not conid:
            logger.error("[%s] No conid — cannot submit bracket", symbol)
            return

        # ── Guard: pending reconciliation? ────────────────────────────────
        # If a prior partial-fill / divergence triggered auto-flatten, the
        # engine blocks new entries on this symbol until IBKR confirms flat.
        # Skip this signal silently — the next bar will re-evaluate.
        if self._engine.is_pending_reconciliation(symbol):
            logger.info("[%s] ENTRY BLOCKED — pending reconciliation", symbol)
            return

        # ── Guard: bracket already in flight for this symbol (FAIL-CLOSED) ──
        # Added 2026-05-19 after over-fill incident where TWS receive was
        # broken and the bot couldn't see existing positions. This guard
        # tracks every bracket WE submit; only on_fill / on_cancel callbacks
        # remove the symbol. If those callbacks never fire (TWS broken),
        # the symbol stays here and prevents further over-fills.
        with self._brackets_lock:
            if symbol in self._brackets_in_flight:
                logger.warning("[%s] ENTRY BLOCKED — bracket already in flight for this symbol. "
                               "Either wait for fill/cancel callback or call "
                               "clear_bracket_in_flight('%s') manually.",
                               symbol, symbol)
                return

        # ── Guard: TWS receive-health check (added 2026-05-21) ─────────────
        # Refuse to place new orders when TWS receive socket appears stuck.
        # Symptom: orders go out fine, but no orderStatus / openOrder /
        # execDetails callbacks come back. Without callbacks the bot can't
        # see fills → can't update _open_positions → guards fail open →
        # over-fill cascade.
        # Detection: if we placed an order in the last 5 min and have NOT
        # received an order callback since that placement, receive is stale.
        # Block until callbacks resume.
        try:
            tws = getattr(self._engine, "_tws", None)
            last_cb = float(getattr(tws, "_last_order_callback_at", 0.0)) if tws else 0.0
            last_placed = float(getattr(self._engine, "_last_order_placed_at", 0.0))
            now_ts = time.time()
            if last_placed > 0 and (now_ts - last_placed) > 30 and last_cb < last_placed:
                stale_sec = now_ts - last_placed
                logger.error("[%s] ENTRY BLOCKED (receive-stale) — last order placed "
                             "%.0fs ago, no order callbacks since. TWS receive may be "
                             "broken; refusing new entries until receive recovers.",
                             symbol, stale_sec)
                return

            # 2026-05-22 (Fix #3): active liveness-probe gate. Independent of
            # whether any order was recently placed. If reqCurrentTime hasn't
            # answered within RECEIVE_DEAD_SEC, the receive socket is broken
            # and ALL new entries must be refused — not just blocked on lag
            # since last placement.
            if tws is not None and hasattr(tws, "is_receive_broken") \
               and tws.is_receive_broken():
                dead_sec = now_ts - float(getattr(tws, "_last_receive_at", 0.0))
                logger.error("[%s] ENTRY BLOCKED (receive-dead) — no reqCurrentTime "
                             "response for %.0fs. TWS receive channel broken; refusing "
                             "ALL entries until probe recovers.", symbol, dead_sec)
                return
        except Exception as exc:
            logger.warning("[%s] receive-health check error (proceeding): %s", symbol, exc)

        # ── Guard: check if IBKR already holds a position for this symbol ──
        # Prevents re-entry when internal _open_positions was cleared (stop-out)
        # but IBKR still holds shares from prior fills.
        # FAIL-CLOSED: on exception, block the entry rather than proceeding.
        #
        # 2026-05-22 (Fix #4): also fail-closed when get_positions returns an
        # empty dict while the receive channel is broken — an empty result
        # there is meaningless and was previously letting silent over-fills
        # through.
        try:
            ibkr_pos = self._engine.get_positions(timeout=3)
            if symbol in ibkr_pos:
                existing = ibkr_pos[symbol]
                qty_held = abs(existing.get("qty", 0))
                if qty_held > 0:
                    logger.warning("[%s] ENTRY BLOCKED — IBKR holds %s %d shares. "
                                   "Cannot open new position until flat.",
                                   symbol, existing.get("side", "?"), qty_held)
                    return
            else:
                # 2026-05-22 (Fix #4): if the snapshot is empty AND the receive
                # probe says broken, the snapshot is unreliable — fail closed.
                tws = getattr(self._engine, "_tws", None)
                if not ibkr_pos and tws is not None \
                   and hasattr(tws, "is_receive_broken") and tws.is_receive_broken():
                    logger.error("[%s] ENTRY BLOCKED (fail-closed) — IBKR snapshot "
                                 "empty AND receive-dead; cannot trust snapshot.",
                                 symbol)
                    return
        except Exception as exc:
            logger.error("[%s] ENTRY BLOCKED (fail-closed) — IBKR position check raised %s",
                         symbol, exc)
            return

        cfg       = load_config()
        direction = action["signal"]
        orders    = action["orders"]
        close     = bar["close"]
        # 2026-06-08 (per user): ENTRY = LIMIT order at the average of the signal
        # bar's HIGH and LOW (mid of the bar's range). Stop is set relative to
        # this limit price (entry ± MPL), consistent with the trail logic.
        # 2026-07-04: rounded to 2 dp (cents) — (H+L)/2 can land on a half-penny
        # (e.g. 183.445), which is not a valid US-equity tick and is submitted
        # to IBKR un-rounded (the engine does not re-round the parent LMT price).
        entry_limit_px = round((float(bar.get("high", close)) + float(bar.get("low", close))) / 2.0, 2)
        stock_cfg = cfg.get("Stocks", {}).get(symbol, {})
        mpl       = float(stock_cfg.get("mpl", cfg.get("MPL_dollar_Amount", 0.60)))
        qty       = int(orders.get("quantity", cfg.get("PositionSize", 500)))

        if direction == "SHORT":
            stop_px = round(entry_limit_px + mpl, 2)   # 2026-07-04: cents
        else:
            stop_px = round(entry_limit_px - mpl, 2)   # 2026-07-04: cents

        # 2026-06-05 (per user): capture the fast-line slope magnitude at entry
        # so the tech exit can require the rollover to be at least this steep
        # (in absolute value) before 3*MPL of profit.
        _em = bar.get("macd") or []
        try:
            entry_slope_abs = abs(float(_em[0]) - float(_em[1])) if len(_em) >= 2 else 0.0
        except Exception:
            entry_slope_abs = 0.0

        logger.info("*** [%s] %s SIGNAL  close=%.4f  lmt(H+L)/2=%.4f  qty=%d  stop=%.4f",
                    symbol, direction, close, entry_limit_px, qty, stop_px)

        # Reserve the in-flight slot BEFORE submit_bracket so a concurrent
        # entry check on this same symbol can't slip through during the
        # ~ms between the guard check above and the engine call below.
        with self._brackets_lock:
            self._brackets_in_flight.add(symbol)

        def on_fill(sym, fq, avg_price):
            """Called by engine fill-verify thread when parent fills."""
            logger.info("[%s] FILLED qty=%d avg=%.4f — registering position",
                        sym, fq, avg_price)
            with self._pos_lock:
                self._open_positions[sym] = {
                    "symbol"          : sym,
                    "direction"       : direction,
                    "entry_time"      : datetime.now(EST).strftime("%H:%M:%S"),
                    "entry_epoch"     : time.time(),   # 2026-06-11: for phantom-reconcile age guard
                    "entry_price"     : avg_price,
                    "stop_price"      : stop_px,
                    "stop_limit_price": stop_px,
                    "target_price"    : 0.0,      # no target — exits via trail/BE
                    "be_activated"    : False,
                    "max_high"        : avg_price,
                    "min_low"         : avg_price,
                    "high_history"    : [],
                    "low_history"     : [],
                    "qty"             : fq,
                    "conid"           : conid,
                    "mpl"             : mpl,
                    "entry_slope_abs" : entry_slope_abs,
                }
            # DELIBERATELY do NOT release _brackets_in_flight here.
            # Position is now REAL at IBKR. The slot stays LOCKED until
            # IBKR confirms the position is flat again (handled by
            # _on_stop_filled, or _release_after_ibkr_confirms_flat
            # invoked by the exit-handler thread). This prevents the
            # over-fill cascade where on_fill releases the slot, then
            # a subsequent bar's exit logic prematurely clears
            # _open_positions, then a new signal fires before the prior
            # position is actually closed in IBKR.

        def on_cancel(sym, reason):
            """
            Called if parent does not fill within 60s.

            FIX (2026-05-21 over-fill incident #2): we used to blindly clear
            the in-flight slot here. Problem: when TWS receive is broken,
            the fill-verify thread can't see actual fills and TIMES OUT
            even though the parent did fill silently at IBKR. Blind clear
            then lets a second bracket fire on the same symbol → over-fill.

            New behavior: spawn a background verifier that polls IBKR for
            the actual position. Only release the slot when IBKR reports
            qty == 0. If IBKR has shares, the symbol stays LOCKED in
            _brackets_in_flight forever (until manual clear) and the
            position is registered into _open_positions for exit handling.
            """
            logger.warning("[%s] Bracket cancelled — reason: %s. "
                           "Verifying IBKR position before releasing slot.",
                           sym, reason)

            def _verify_then_release(symbol_):
                try:
                    # 2026-05-26 (P0 #3): use the fail-closed read. A bare
                    # get_actual_position()==0 used to release the guards on a
                    # dead-socket empty snapshot — the false "qty=0" that drove
                    # the over-fill cascade. Now we release ONLY on a FRESH
                    # confirmed-flat; unknown keeps the slot LOCKED.
                    actual_signed, fresh = self._engine.get_position_confirmed(symbol_)
                    if fresh and actual_signed == 0:
                        # CONFIRMED flat — true cancel, no position opened. Release.
                        with self._pos_lock:
                            self._open_positions.pop(symbol_, None)
                        with self._brackets_lock:
                            self._brackets_in_flight.discard(symbol_)
                        logger.info("[%s] on_cancel: IBKR confirms flat — "
                                    "released both guards", symbol_)
                    elif fresh and actual_signed is not None and abs(actual_signed) > 0:
                        # Phantom cancel — IBKR DID fill the parent silently.
                        # Register the position so exit logic can take it,
                        # but KEEP the in-flight slot LOCKED to prevent
                        # another bracket from firing.
                        actual_abs = abs(actual_signed)
                        logger.error("[%s] on_cancel PHANTOM — IBKR holds "
                                     "%d shares (cancel was false). Registering "
                                     "position; keeping in-flight slot LOCKED.",
                                     symbol_, actual_signed)
                        side_long = actual_signed > 0
                        with self._pos_lock:
                            if symbol_ not in self._open_positions:
                                self._open_positions[symbol_] = {
                                    "symbol"          : symbol_,
                                    "direction"       : "LONG" if side_long else "SHORT",
                                    "entry_time"      : datetime.now(EST).strftime("%H:%M:%S"),
                                    "entry_epoch"     : time.time(),   # 2026-06-11: for phantom-reconcile age guard
                                    "entry_price"     : float(close),
                                    "stop_price"      : float(stop_px),
                                    "stop_limit_price": float(stop_px),
                                    "target_price"    : 0.0,
                                    "be_activated"    : False,
                                    "max_high"        : float(close),
                                    "min_low"         : float(close),
                                    "high_history"    : [],
                                    "low_history"     : [],
                                    "qty"             : int(actual_abs),
                                    "conid"           : conid,
                                }
                        # _brackets_in_flight intentionally NOT cleared
                    else:
                        # 2026-05-26 (P0 #3): position UNKNOWN — receive broken or
                        # snapshot timed out. Do NOT release; that false "flat" is
                        # exactly what let a second bracket fire and double the
                        # position. Keep the in-flight slot LOCKED until a fresh
                        # snapshot resolves it (manual clear if it never does).
                        logger.error("[%s] on_cancel: IBKR position UNKNOWN "
                                     "(snapshot not fresh) — keeping in-flight slot "
                                     "LOCKED (fail-closed). Manual "
                                     "clear_bracket_in_flight('%s') if confirmed flat.",
                                     symbol_, symbol_)
                except Exception as exc:
                    logger.exception("[%s] on_cancel verify error: %s — "
                                     "keeping slot LOCKED (fail-CLOSED)",
                                     symbol_, exc)
                    # On any exception, keep guards locked

            threading.Thread(
                target=_verify_then_release,
                args=(sym,),
                daemon=True,
                name=f"cancel-verify-{sym}",
            ).start()

        try:
            bracket_result = self._engine.submit_bracket(
                symbol              = symbol,
                conid               = conid,
                direction           = direction,
                quantity            = qty,
                entry_price         = entry_limit_px,
                stop_price          = stop_px,
                mpl                 = mpl,
                wide_stp_multiplier = float(cfg.get("WideStpMultiplier", 1.1)),
                on_fill             = on_fill,
                on_cancel           = on_cancel,
            )
        except Exception as exc:
            logger.error("[%s] submit_bracket raised %s — releasing in-flight slot",
                         symbol, exc)
            with self._brackets_lock:
                self._brackets_in_flight.discard(symbol)
            return

        # If Phase 1 parent placement failed (no fill-verify thread will run),
        # on_cancel won't fire — release the in-flight slot here so the symbol
        # isn't permanently blocked.
        if not (bracket_result and bracket_result.get("parent")):
            logger.warning("[%s] Bracket Phase 1 failed (no parent OID) — releasing in-flight slot",
                           symbol)
            with self._brackets_lock:
                self._brackets_in_flight.discard(symbol)

    def clear_bracket_in_flight(self, symbol: str) -> None:
        """
        Manual override: remove `symbol` from the in-flight set so new
        entries are allowed again. Use AFTER confirming the symbol is
        flat in IBKR (e.g. after running flatten_oneshot.py).
        """
        with self._brackets_lock:
            self._brackets_in_flight.discard(symbol)
        logger.warning("[%s] bracket-in-flight slot manually cleared", symbol)

    def _release_after_ibkr_confirms_flat(self, symbol: str, label: str,
                                          max_wait_sec: float = 60.0,
                                          poll_interval: float = 2.0) -> bool:
        """
        Background helper. Poll IBKR until the named symbol's actual
        position is 0, then release BOTH `_open_positions` and
        `_brackets_in_flight`. Returns True if confirmed flat in time,
        False if timeout (in which case BOTH guards stay LOCKED — symbol
        is blocked from new entries until manual clear).

        This is the only path that releases _brackets_in_flight after an
        entry has filled (other than on_cancel for un-filled entries).
        The release is ANCHORED to actual IBKR state — no premature
        clearing on bar-close exit speculation.
        """
        deadline = time.time() + max_wait_sec
        while time.time() < deadline:
            try:
                # 2026-05-26 (P0 #3): release ONLY on a FRESH confirmed-flat
                # snapshot. is_confirmed_flat is False when receive is broken /
                # the snapshot times out, so a dead socket can no longer fake
                # "flat" and free the in-flight slot for a re-entry.
                if self._engine.is_confirmed_flat(symbol):
                    with self._pos_lock:
                        self._open_positions.pop(symbol, None)
                    with self._brackets_lock:
                        self._brackets_in_flight.discard(symbol)
                    logger.info("[%s] %s — IBKR confirms flat; cleared both guards",
                                symbol, label)
                    return True
            except Exception as exc:
                logger.warning("[%s] %s — position check failed: %s",
                               symbol, label, exc)
            time.sleep(poll_interval)
        logger.error("[%s] %s — IBKR position still non-zero after %.0fs. "
                     "Symbol BLOCKED from new entries until manual "
                     "clear_bracket_in_flight('%s') call.",
                     symbol, label, max_wait_sec, symbol)
        return False

    def clear_all_brackets_in_flight(self) -> None:
        """Manual override: clear ALL in-flight slots. Use after a clean flatten."""
        with self._brackets_lock:
            n = len(self._brackets_in_flight)
            self._brackets_in_flight.clear()
        logger.warning("Cleared ALL %d bracket-in-flight slots", n)

    # =========================================================================
    # EXIT
    # =========================================================================

    def _on_stop_filled(self, symbol: str) -> None:
        """
        Called by the engine when an IBKR stop-type order (Phase 2 STP_MKT,
        BE STOP_PRICE / STOP_LIMIT, or surviving wide_stp) fills for `symbol`.
        Keeps internal _open_positions in sync with IBKR reality, then cancels
        any sibling stops in a background thread (so the IBAPI reader thread
        is not blocked by cancel I/O).
        """
        with self._pos_lock:
            if symbol not in self._open_positions:
                return                                   # already cleared elsewhere
            logger.info("[%s] Stop order filled in IBKR — clearing position state",
                        symbol)
            _pos = self._open_positions.pop(symbol, None)
        reenter_dir = (_pos or {}).get("direction")
        reenter_qty = int((_pos or {}).get("qty") or 0)
        reenter_entry = (_pos or {}).get("entry_price")   # 2026-06-11: for the no-chase guard
        # Also free the bracket-in-flight slot so the symbol can re-enter
        # after a stop-out (otherwise it stays blocked forever).
        with self._brackets_lock:
            self._brackets_in_flight.discard(symbol)

        def _cleanup(sym, eng, direction, qty, orig_entry):
            try:
                eng.handle_stop_hit(sym)                 # cancel sibling stops
            except Exception as exc:
                logger.warning("[%s] sibling-stop cleanup after stop-fill failed: %s",
                               sym, exc)
            # 2026-06-05 (per user): re-enter after the stop if the trend is
            # still intact (no delta/gamma exit and no reversal). Runs here on
            # the cleanup thread, never on the IBAPI reader thread. 2026-06-11:
            # now also guarded by the no-chase price check + REENTRY_CAP.
            try:
                time.sleep(1.0)                          # let IBKR settle flat
                self._maybe_reenter_after_stop(sym, direction, qty, orig_entry)
            except Exception as exc:
                logger.warning("[%s] re-entry after stop failed: %s", sym, exc)

        threading.Thread(
            target=_cleanup,
            args=(symbol, self._engine, reenter_dir, reenter_qty, reenter_entry),
            daemon=True,
            name=f"stop-fill-cleanup-{symbol}",
        ).start()

    def _clear_phantom(self, symbol: str, reason: str):
        """2026-06-11 (C): minimal removal of a symbol from internal position
        state — pop _open_positions + free the bracket-in-flight slot. Used by the
        phantom reconcile. Unlike _on_stop_filled it does NOT cancel siblings or
        re-enter (the position is already gone at the broker)."""
        with self._pos_lock:
            existed = self._open_positions.pop(symbol, None)
        with self._brackets_lock:
            self._brackets_in_flight.discard(symbol)
        if existed is not None:
            logger.warning("[%s] PHANTOM POSITION CLEARED — %s", symbol, reason)
        return existed

    def _reconcile_positions(self):
        """2026-06-11 (C): clear PHANTOM positions — symbols the bot still tracks
        in _open_positions but IBKR shows FLAT (e.g. a stop-out that didn't notify
        the bot, leaving it blocked from re-entry + a stale dashboard). Heavily
        guarded to NEVER race a fresh fill or a dead socket:
          (i)   IBKR positions snapshot is FRESH (positionEnd seen) — else skip;
          (ii)  engine.is_confirmed_flat(sym) (fail-closed) True for N consecutive
                cycles;
          (iii) entry age > MIN_AGE seconds;
          (iv)  not bracket-in-flight and not pending-reconciliation (mid-flatten).
        """
        N_CONSEC = 3
        MIN_AGE  = 120.0
        if not hasattr(self, "_flat_streak"):
            self._flat_streak = {}
        eng = self._engine
        try:
            _snap, fresh = eng.get_positions_snapshot()
        except Exception as exc:
            logger.warning("position reconcile: snapshot failed (skip): %s", exc)
            return
        if not fresh:
            return                                   # dead/stale socket → never infer 'flat'
        with self._pos_lock:
            syms = list(self._open_positions.keys())
        for sym in syms:
            try:
                if sym in self._brackets_in_flight or eng.is_pending_reconciliation(sym):
                    self._flat_streak.pop(sym, None)         # mid-flatten / in-flight → reset
                    continue
                if not eng.is_confirmed_flat(sym):
                    self._flat_streak.pop(sym, None)         # not confirmed flat → reset streak
                    continue
                self._flat_streak[sym] = self._flat_streak.get(sym, 0) + 1
                with self._pos_lock:
                    pos = self._open_positions.get(sym) or {}
                ep  = pos.get("entry_epoch")
                age = (time.time() - ep) if ep is not None else None
                if self._flat_streak[sym] >= N_CONSEC and age is not None and age > MIN_AGE:
                    self._clear_phantom(
                        sym, f"IBKR confirmed FLAT {self._flat_streak[sym]}x consecutive; "
                             f"entry_age={round(age)}s — bot held a stale position")
                    self._flat_streak.pop(sym, None)
            except Exception as exc:
                logger.warning("[%s] position reconcile error (ignored): %s", sym, exc)
                self._flat_streak.pop(sym, None)
        # prune streak counters for symbols no longer tracked
        for s in [s for s in self._flat_streak if s not in syms]:
            self._flat_streak.pop(s, None)

    def _maybe_reenter_after_stop(self, symbol: str, direction, qty: int, orig_entry: float = None) -> None:
        """
        2026-06-05 (per user): after a stop-loss fill, re-enter the SAME side if
        the trend is still intact. Trend intact = NO delta/gamma exit signal AND
        NO reversal of the fast line vs the slow line. Capped at self._reentry_cap
        per symbol per day. Interpreted "no delta or no reversal" as trend-intact
        (neither), since the literal OR would re-enter on almost every stop.
        """
        if not direction or qty <= 0:
            return
        pipe = self._pipelines.get(symbol)
        if pipe is None or not pipe.is_ready():
            return
        # 2026-06-05 fix: NEVER re-enter on a stale or unhealthy price feed. The
        # re-entry stop is computed from the last tick price; if the receive
        # channel is flapping or the tick is old, that price is wrong and the
        # bracket stop lands on the wrong side of the fill. Refuse until fresh.
        tws = getattr(self._engine, "_tws", None)
        if tws is not None and hasattr(tws, "is_receive_broken") and tws.is_receive_broken():
            logger.info("[%s] No re-entry after SL — receive channel unhealthy (price may be stale)", symbol)
            return
        last_ts = getattr(pipe, "_last_tick_ts", None)
        if last_ts is None:
            logger.info("[%s] No re-entry after SL — no live tick price yet", symbol)
            return
        try:
            age = (datetime.now(EST).replace(tzinfo=None) - last_ts).total_seconds()
        except Exception:
            age = 999.0
        if age > 8:
            logger.info("[%s] No re-entry after SL — tick price stale (%.0fs old)", symbol, age)
            return
        eng = pipe._engine
        macd_hist = list(getattr(eng, "_macd_hist", []))    # newest-first
        sig_hist  = list(getattr(eng, "_sig_hist", []))
        if len(macd_hist) < 3 or not sig_hist:
            return
        m0, m1, m2 = float(macd_hist[0]), float(macd_hist[1]), float(macd_hist[2])
        s0 = float(sig_hist[0])
        delta = m0 - m1
        gamma = delta - (m1 - m2)
        if str(direction).upper() == "LONG":
            tech_exit = (delta < 0) and (gamma < 0)         # fast line rolling over
            reversal  = (m0 < s0)                           # fast crossed below slow
        else:
            tech_exit = (delta > 0) and (gamma > 0)
            reversal  = (m0 > s0)
        if tech_exit or reversal:
            logger.info("[%s] No re-entry after SL — trend turned (%s%s)", symbol,
                        "delta/gamma exit " if tech_exit else "",
                        "reversal" if reversal else "")
            return
        n = self._reentry_count.get(symbol, 0)
        if n >= self._reentry_cap:
            logger.info("[%s] No re-entry after SL — cap %d reached", symbol, self._reentry_cap)
            return
        price = pipe._last_tick_price or s0
        if not price or price <= 0:
            return
        # 2026-06-11: NO-CHASE guard — never re-enter at a price WORSE than the
        # original entry (don't re-short HIGHER / re-long LOWER after being stopped
        # out into an adverse move). This is what compounded the SPY loss: it
        # re-shorted at 730.68 then 732.54, above the 729.25 entry, as price ran up.
        if orig_entry and orig_entry > 0:
            d = str(direction).upper()
            adverse = (d == "SHORT" and price > orig_entry) or (d == "LONG" and price < orig_entry)
            if adverse:
                logger.info("[%s] No re-entry after SL — price %.4f adverse vs entry %.4f "
                            "(would chase the move)", symbol, price, orig_entry)
                return
        self._reentry_count[symbol] = n + 1
        logger.info("[%s] RE-ENTRY after SL #%d %s (trend intact: no delta, no reversal)",
                    symbol, n + 1, direction)
        action = {"signal": str(direction).upper(), "orders": {"quantity": int(qty)},
                  "auto_mode": True}
        bar = {"symbol": symbol, "close": float(price), "high": float(price),
               "low": float(price),
               "current_time": datetime.now(EST).replace(tzinfo=None),
               "macd": macd_hist}
        self._handle_entry(symbol, action, bar)

    # =========================================================================
    # LIVE (TICK-DRIVEN) TRAIL — 2026-08-06
    # =========================================================================

    def _live_price_for(self, symbol: str):
        """
        Newest traded price for a symbol, straight off the streaming feed.

        Prefers the shadow broker's quote cache — that is the exact price the
        fill engine is testing resting stops against, so the trail and the fill
        can never be looking at different markets. Falls back to the entry /
        exit pipelines' last tick.
        """
        try:
            from shadow_broker import SHADOW
            q = (SHADOW._quotes or {}).get(symbol) or {}
            last = float(q.get("last") or 0.0)
            ts   = float(q.get("ts") or 0.0)
            if last > 0 and (time.time() - ts) <= 30:
                return last
        except Exception:
            pass
        for pipes in (self._exit_pipes, self._pipelines):
            pipe = pipes.get(symbol)
            last = float(getattr(pipe, "_last_tick_price", 0) or 0) if pipe else 0.0
            if last > 0:
                return last
        return None

    def _start_live_trail_loop(self) -> None:
        """
        Evaluate the live trail for every open position several times a second.

        Why a loop and not the tick callback itself: ticks arrive on the IB
        reader thread, thousands per second across ~21 symbols. Running exit
        logic there would block the socket that feeds the whole bot. This loop
        does the same work on its own thread at a fixed, configurable cadence —
        250 ms by default, which is real-time for a stop that is re-priced in
        whole cents.

        It only ever RATCHETS a stop (see _intrabar_trail + the monotonic guard
        in exit_logic); it never exits a position and never widens a stop. The
        exit-candle close still runs the full exit stack — 5-bar trail, target,
        stop-hit, EoD — exactly as before.
        """
        try:
            interval = float(os.getenv("LIVE_TRAIL_MS", "250")) / 1000.0
        except (TypeError, ValueError):
            interval = 0.25
        interval = max(0.05, min(interval, 5.0))

        def _loop():
            logger.info("LIVE TRAIL loop started — every %.0f ms, tick-driven "
                        "(set LIVE_TRAIL_MS to change, LiveTrail=N to disable)",
                        interval * 1000)
            while self._running:
                try:
                    time.sleep(interval)
                    with self._pos_lock:
                        open_syms = list(self._open_positions.items())
                    for sym, pos in open_syms:
                        price = self._live_price_for(sym)
                        if not price or price <= 0:
                            continue
                        # A tick has no bar — high/low ARE the tick price. The
                        # live trail only reads price/high/low, and deliberately
                        # never touches high_history / low_history, so the 5-bar
                        # trail's own state stays driven by real bars only.
                        bar = {
                            "symbol"      : sym,
                            "close"       : price,
                            "high"        : price,
                            "low"         : price,
                            "current_time": datetime.now(EST).replace(tzinfo=None),
                        }
                        try:
                            self._handle_exit(sym, pos, bar, intrabar=True)
                        except Exception as exc:
                            logger.error("[%s] live-trail evaluation error: %s", sym, exc)
                except Exception as exc:
                    logger.error("live-trail loop error: %s", exc)

        threading.Thread(target=_loop, daemon=True, name="live-trail").start()

    def _handle_exit(self, symbol: str, pos: Dict, bar: Dict, intrabar: bool = False):
        """
        Run process_exit() on the position and dispatch the result to the engine.
        2026-06-04: `intrabar=True` evaluates only the live delta/gamma tech exit
        (the trail / BE need real bar high/low, so they stay on real closes).
        """
        conid = self._conid_cache.get(symbol) or pos.get("conid")
        if not conid:
            logger.error("[%s] No conid for exit — cannot act", symbol)
            return

        direction   = pos["direction"]
        was_be      = pos.get("be_activated", False)

        tick = {
            "current_time": bar.get("current_time", datetime.now(EST).replace(tzinfo=None)),
            "price"       : bar["close"],
            "high"        : bar.get("high",  bar["close"]),
            "low"         : bar.get("low",   bar["close"]),
            "close"       : bar["close"],
            "prev_high"   : bar.get("prev_high", bar["close"]),
            "prev_low"    : bar.get("prev_low",  bar["close"]),
            "macd"        : bar.get("macd"),     # fast-line history (newest-first) for delta/gamma exit
        }

        action = process_exit(pos, tick, intrabar=intrabar)   # mutates pos in-place for stops
        action_type = action["action"]

        if action_type == "NONE":
            return

        elif action_type == "UPDATE_STOPS":
            updates   = action["updates"]
            new_stp   = updates.get("stop_price", pos["stop_price"])
            sls       = float(load_config().get("StopLimitSpread", 0.03))

            if not was_be and pos.get("be_activated"):
                # BE just activated this bar
                logger.info("[%s] BE activated — calling apply_breakeven(%.4f)",
                            symbol, new_stp)
                self._engine.apply_breakeven(
                    symbol    = symbol,
                    conid     = conid,
                    direction = direction,
                    new_stp_price = new_stp,
                    sls       = sls,
                )
            else:
                # Post-BE trailing
                logger.info("[%s] Trail stop — calling trail_stop(%.4f)",
                            symbol, new_stp)
                self._engine.trail_stop(
                    symbol    = symbol,
                    conid     = conid,
                    direction = direction,
                    new_stp_price = new_stp,
                    sls       = sls,
                )

        elif action_type == "CLOSE_TECH":
            # Technical (fast-line delta/gamma) exit. CANCEL the protective
            # stop / stop-limit FIRST, then MKT close — never leave a live
            # stop that could fire after we go flat (naked-position guard on
            # a fast reversion). The engine method enforces that ordering.
            close_side = "SELL" if direction == "LONG" else "BUY"
            qty = pos.get("qty", 0)
            logger.info("[%s] CLOSE_TECH — cancel stops FIRST then MKT close qty=%d  reason: %s",
                        symbol, qty, action.get("reason", ""))
            self._engine.square_off_technical(
                symbol   = symbol,
                conid    = conid,
                side     = close_side,
                quantity = qty,
            )
            # Release internal guards once IBKR confirms the symbol is flat
            # (mirrors the CLOSE_MARKET path).
            def _tech_release(bot, sym):
                time.sleep(10)
                bot._release_after_ibkr_confirms_flat(sym, "CLOSE_TECH", max_wait_sec=60.0)
            threading.Thread(
                target=_tech_release, args=(self, symbol), daemon=True,
                name=f"close-tech-release-{symbol}",
            ).start()

        elif action_type in ("CLOSE_LIMIT", "CLOSE_MARKET"):
            # CLOSE_MARKET = EoD square-off. CLOSE_LIMIT = v1.5 target hit
            # (re-enabled 2026-07-04). Both are executed as a MARKET close at
            # the moment they trigger — a deliberate, bug-informed choice: a
            # RESTING target limit order at IBKR reintroduced the orphan-target /
            # target-caught-in-sweep / non-fill-in-fast-market failures seen
            # earlier (see the QTI bug tracker), and a limit that fails to fill
            # leaves a stuck position. Detecting the target at the bar close and
            # closing at market is the reliable path. (The target price still
            # sets WHEN we exit; only the fill is at market, not a limit.)
            close_side = "SELL" if direction == "LONG" else "BUY"
            qty = pos.get("qty", 0)
            logger.info("[%s] %s triggered — sending MKT close qty=%d  reason: %s",
                        symbol, action_type, qty, action.get("reason", ""))
            self._engine.submit_market_close(
                symbol   = symbol,
                conid    = conid,
                side     = close_side,
                quantity = qty,
            )
            # FIX (2026-05-20 over-fill incident): do NOT unconditionally
            # clear _open_positions / _brackets_in_flight on a fixed timer.
            # Bot bar-close speculation that the close "happened" can be
            # wrong (partial fills, broken receive, etc.). Verify against
            # IBKR position == 0 before releasing the guards. While the
            # symbol stays in _open_positions, _handle_exit on subsequent
            # bars sees the same pos and skips re-submitting (since the
            # MKT close is already in flight). When IBKR confirms flat,
            # the symbol is removed from both guards.
            def _delayed_cleanup(bot, sym, eng):
                # Brief pause to let the MKT close start filling
                time.sleep(10)
                try: eng.handle_stop_hit(sym)        # cancel sibling stops
                except Exception as exc:
                    logger.warning("[%s] sibling-stop cleanup err: %s", sym, exc)
                bot._release_after_ibkr_confirms_flat(
                    sym, "CLOSE_MARKET", max_wait_sec=60.0)
            threading.Thread(
                target=_delayed_cleanup,
                args=(self, symbol, self._engine),
                daemon=True,
                name=f"close-mkt-cleanup-{symbol}",
            ).start()

        elif action_type == "STOP_HIT":
            # Bot's bar-close detection sees a stop breach. The IBKR stop
            # may or may not have fired. New unified flow (uses the engine's
            # auto-flatten + reconciliation watchdog instead of the prior
            # 5s-sleep defensive thread):
            #
            #   1. Wait 0.5s for the IBKR sync hook to fire if the broker
            #      did fill the stop on its own.
            #   2. If sync hook fired (symbol no longer in _open_positions),
            #      mop up siblings via handle_stop_hit and we're done.
            #   3. Otherwise, query IBKR's actual position. If position is
            #      still non-zero, treat it as a divergence — call the
            #      engine's auto-flatten which:
            #        - cancels all OCA siblings
            #        - sends MKT close for the actual non-zero qty
            #        - spawns the reconciliation watchdog (3 attempts)
            #        - blocks new entries on the symbol via
            #          is_pending_reconciliation until IBKR confirms flat
            #        - escalates to Discord on 3-attempt failure
            qty = pos.get("qty", 0)
            logger.warning("[%s] STOP_HIT detected by process_exit "
                           "(reason=%s) — defer 0.5s for sync hook then "
                           "verify with IBKR",
                           symbol, action.get("reason", ""))

            def _stop_hit_handler(sym, c_id, dir_, expected_qty, eng,
                                   pos_lock, open_pos, bot):
                try:
                    time.sleep(0.5)
                    with pos_lock:
                        still_open = sym in open_pos
                    if not still_open:
                        # Sync hook fired during the 0.5s wait — position
                        # already cleared by _on_stop_filled, which also
                        # released _brackets_in_flight. Just mop siblings.
                        try: eng.handle_stop_hit(sym)
                        except Exception as exc:
                            logger.warning("[%s] post-fill cleanup err: %s",
                                           sym, exc)
                        return

                    # Bot still thinks LONG/SHORT — verify with IBKR.
                    # 2026-05-26 (P0 #3): fail-closed read. A bare 0 used to be
                    # trusted as "flat" and released the guards even on a dead-
                    # socket empty snapshot, opening the door to a re-entry.
                    actual_signed, fresh = eng.get_position_confirmed(sym)
                    if fresh and actual_signed == 0:
                        logger.warning("[%s] STOP_HIT: bot state says open but "
                                       "IBKR confirms flat — releasing guards",
                                       sym)
                        with pos_lock: open_pos.pop(sym, None)
                        with bot._brackets_lock:
                            bot._brackets_in_flight.discard(sym)
                        try: eng.handle_stop_hit(sym)
                        except Exception: pass
                        return

                    if not fresh:
                        # Position UNKNOWN — receive broken / snapshot timed out.
                        # We can neither confirm flat nor size a flatten. Keep
                        # BOTH guards LOCKED and mark pending_reconciliation so no
                        # re-entry fires while we're blind; the reconcile
                        # heartbeat clears it once a fresh snapshot confirms flat.
                        logger.error("[%s] STOP_HIT: IBKR position UNKNOWN "
                                     "(snapshot not fresh) — keeping guards LOCKED, "
                                     "marking pending_reconciliation", sym)
                        try: eng.mark_pending_reconciliation(sym)
                        except Exception: pass
                        return

                    actual_abs = abs(actual_signed)
                    # FIX (2026-05-20 over-fill incident): do NOT clear
                    # _open_positions or _brackets_in_flight optimistically
                    # while auto-flatten is in flight. The previous code
                    # cleared _open_positions here, which let the next bar's
                    # signal slip through the position-open guard and fire
                    # a fresh bracket on top of an un-closed position. Now
                    # we trust the verification thread to release both
                    # guards only when IBKR actually reports qty=0.
                    logger.error("[%s] STOP_HIT divergence — IBKR position=%d "
                                 "(bot expected %d). Triggering auto-flatten + "
                                 "verified-clear watchdog.",
                                 sym, actual_signed, expected_qty)
                    eng._auto_flatten_gap(sym, actual_abs, dir_)
                    bot._release_after_ibkr_confirms_flat(
                        sym, "STOP_HIT auto-flatten", max_wait_sec=60.0)
                except Exception as exc:
                    logger.exception("[%s] STOP_HIT handler crashed: %s",
                                     sym, exc)

            threading.Thread(
                target=_stop_hit_handler,
                args=(symbol, conid, direction, qty, self._engine,
                      self._pos_lock, self._open_positions, self),
                daemon=True,
                name=f"stop-hit-handler-{symbol}",
            ).start()

    # =========================================================================
    # BROKER PROXY METHODS  (used by IndicatorPipeline.seed())
    # =========================================================================
    # indicators.py calls self._broker.session.* and self._broker._conid_cache
    # We provide a minimal session proxy so seed() works without ibkr_broker.

    class _SessionProxy:
        """
        Compatibility shim so indicators.py seed() can fetch historical bars.
        indicators.py calls session._get(path).
        Routes to TWSClient.get_historical_bars() — no CP API needed.
        """
        def __init__(self, tws_client, conid_cache: dict):
            self._tws         = tws_client               # TWSClient instance (not engine)
            self._conid_cache = conid_cache

        def _get(self, path: str, params: dict = None):
            """
            indicators.py calls session._get('/iserver/marketdata/history?conid=X&...')
            Parse conid and bar size from path then call TWSClient.get_historical_bars().
            """
            try:
                import urllib.parse as _up
                p = dict(params or {})
                if "conid" not in p:
                    qs = _up.parse_qs(_up.urlparse(path).query)
                    p  = {k: v[0] for k, v in qs.items()}

                conid    = int(p.get("conid", 0))
                period   = p.get("period", "1 D").replace("1d","1 D").replace("2d","2 D")
                bar_str  = p.get("bar", "30secs")
                bar_map  = {"30secs":"30 secs","5secs":"5 secs",
                            "1min":"1 min","5mins":"5 mins",
                            "1hour":"1 hour","1day":"1 day"}
                bar_size = bar_map.get(bar_str, "30 secs")

                if not conid or not self._tws:
                    return {"data": []}

                # Check if HMDS is broken (error 162 = different IP)
                # If so, return empty immediately — cold start is better than waiting 30s per symbol
                if getattr(self._tws, '_hmds_error', False):
                    logger.info("HMDS broken (error 162) — skipping seed for conid=%d (cold start)", conid)
                    return {"data": []}

                # Call get_historical_bars on TWSClient directly (not engine)
                bars = self._tws.get_historical_bars(
                    conid=conid, period=period, bar_size=bar_size)

                converted = [{"o":b["open"],"h":b["high"],"l":b["low"],
                               "c":b["close"],"v":b["volume"],"t":b["date"]}
                              for b in bars]
                return {"data": converted}

            except Exception as exc:
                logger.warning("_SessionProxy._get failed: %s", exc)
                return {"data": []}

    @property
    def session(self):
        """
        Compatibility shim for indicators.py which calls broker.session._get().
        Must pass the TWSClient instance (has get_historical_bars),
        NOT the TWSEngine (which does not).
        """
        if self._engine and self._engine._tws and self._engine._tws.is_connected:
            return self._SessionProxy(self._engine._tws, self._conid_cache)
        return None


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    bot = MACDBot()
    try:
        bot.start()
        logger.info("Bot running — press Ctrl+C to stop")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        bot.stop()
