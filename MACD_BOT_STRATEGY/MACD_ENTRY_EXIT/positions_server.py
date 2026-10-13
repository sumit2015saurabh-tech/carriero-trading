"""
positions_server.py — Read-only live positions dashboard for MACD bot.

Runs as a daemon thread inside macd_bot.py via start_dashboard(bot).
Serves positions_ui.html at GET / and JSON data at GET /api/data.

TWS socket calls (get_positions, get_live_orders_tws) are refreshed in a
background thread every 5 seconds — never triggered by HTTP requests.
All other reads are direct in-memory dict access with try/except guards.
"""
from __future__ import annotations

import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
    _DEPS_OK = True
except ImportError:
    _DEPS_OK = False

# ── Module-level state ────────────────────────────────────────────────────────

_bot = None          # MACDBot instance (set by start_dashboard)
_app = None          # FastAPI app

_cache_lock = threading.Lock()
_cache: Dict[str, Any] = {
    "tws_positions": {},   # from engine.get_positions() — cached 5s
    "live_orders"  : [],   # from tws.get_live_orders_tws() — cached 5s
    "refreshed_at" : 0.0,
    "positions_fresh_at": 0.0,  # last time tws_positions actually came back non-empty
    "orders_fresh_at": 0.0,     # 2026-06-23: last time the engine log grew (orders source)
    "pos_first_seen": {},  # 2026-06-09: sym -> "HH:MM:SS" first time we saw the position
    # 2026-06-23: orders panel is sourced from the engine LOG ONLY (instant,
    # no laggy reqAllOpenOrders poll). These tail today's log incrementally.
    "log_orders": {},   # oid(int) -> order record reconstructed from the log
    "log_path"  : "",   # path of the log file currently being tailed
    "log_offset": 0,    # byte offset read so far (incremental tail)
    "be_events" : {},   # 2026-07-31: sym -> {time, direction, stop} breakeven activation
    "exec_map"  : {},   # 2026-08-03: execId -> oid, joins COMMISSION lines onto orders
    "cleared_on": "",   # 2026-07-31: date-stamp of the last post-market clear
    "cleared_before": None,  # 2026-08-05: display cut-off "HH:MM:SS" (hide older
                             # rows). Non-destructive — parsed history is kept.
}

# 2026-07-31: daily post-market clean. After this time the panels are wiped once
# so the next session starts empty instead of carrying the day's dead orders.
# Default 16:05 = after the 15:50 EoD square-off and the 16:00 close, but BEFORE
# the 16:30 stop_bot cron — the dashboard lives inside the trading process, so a
# clear scheduled after 16:30 would never run. Override with DASHBOARD_CLEAR_TIME.
_CLEAR_TIME = os.getenv("DASHBOARD_CLEAR_TIME", "16:05")

_HTML_PATH = Path(__file__).parent / "positions_ui.html"
_LOG_DIR   = Path(__file__).parent / "logs"

# ── Engine-log order parsing (logs/tws_DD_MM_YY.txt) ──────────────────────────
# Per user directive 2026-06-23: build the dashboard orders/fills from the
# engine log ONLY. The engine writes the full order lifecycle to the log the
# instant each TWS callback fires, so this has none of the multi-second lag of
# the 5s reqAllOpenOrders poll the panel used to depend on.
_RE_TIME   = r"(\d\d:\d\d:\d\d),\d+"
# 2026-07-31: capture ref + parentId so an order's ROLE (entry / protective stop /
# square-off) is known and each stop can be linked back to the entry it guards.
# NOTE: the order-type group is (.*?) not (.+?) on purpose — a misconfigured
# EntryOrderType emits "SELL XBI 500  @ 0.0000" with an EMPTY type, and those
# orders are rejected by IBKR (code 321). They must still show on the dashboard.
_RE_PLACED = re.compile(_RE_TIME + r".*TWS placed: oid=(\d+)\s+(\w+)\s+(\w+)\s+(\d+)\s+(.*?)\s+@\s+([\d.]+)\s+ref=(\S+)(?:\s+parentId=(\S+))?")
_RE_OPEN   = re.compile(_RE_TIME + r".*TWS openOrder: oid=(\d+)\s+(\w+)\s+(\w+)\s+(.+?)\s+qty=([\d.]+)\s+lmt=([\d.]+)\s+aux=([\d.]+)\s+status=(\w+)")
_RE_STATUS = re.compile(_RE_TIME + r".*TWS orderStatus: oid=(\d+)\s+status=(\w+)\s+filled=([\d.]+)\s+remaining=([\d.]+)\s+avgPrice=([\d.]+)")
_RE_FILL   = re.compile(_RE_TIME + r".*TWS FILL: oid=(\d+)\s+symbol=(\w+)\s+side=(\w+)\s+qty=([\d.]+)\s+price=([\d.]+)(?:\s+execId=(\S+))?(?:.*?exchange=(\S+))?")
# 2026-08-03: IBKR reports its OWN realized P&L and commission per execution.
# Keyed by execId, so it is joined onto the order via the FILL line's execId.
# This is authoritative — better than reconstructing P&L from fill prices.
_RE_COMMISSION = re.compile(r"TWS COMMISSION: execId=(\S+)\s+commission=([\d.\-]+)(?:\s+currency=\S+)?(?:\s+realizedPNL=([\d.eE\-]+))?")
# 2026-07-31: a trailed / breakeven stop is re-priced with a MODIFY on the SAME
# oid — the only place the stop's movement is recorded. Without this the panel
# shows the stop's ORIGINAL price forever and the trail looks like it never ran.
_RE_MODIFY = re.compile(_RE_TIME + r".*TWS modify sent: oid=(\d+)\s+(\S+)\s+new_price=([\d.]+)\s+new_aux=([\d.]+)")
_RE_CANCEL = re.compile(_RE_TIME + r".*TWS cancel sent: oid=(\d+)")
# 2026-07-31: a rejected order (bad type, margin 201, ...) otherwise looks like
# an order that simply never filled. Surface the code + reason on the row.
_RE_ORDERR = re.compile(_RE_TIME + r".*TWS order error: oid=(\d+)\s+code=(\d+)\s+msg=(.*?)\s+reject=")
# Codes that are warnings, not failures — must not mark an order rejected.
# 10147 = "order not found to cancel", 10148 = "cannot be cancelled, state:
# Filled" — both are the engine's routine post-fill cleanup cancel racing the
# fill, NOT an order failure. Treating 10148 as an error made every stop that
# actually FILLED display as "Rejected" (2026-08-04).
_WARN_CODES = {2176, 2109, 10147, 10148, 202, 399, 2100, 2107, 2108}
_RE_BE     = re.compile(_RE_TIME + r".*\[(\w+)\]\s+(LONG|SHORT)\s+breakeven activated:\s+stop=([\d.]+)")

# ref prefix -> the role that order plays in the trade lifecycle
_ROLE_BY_REF = {
    "PARENT" : "ENTRY",       # the entry order itself (LMT or MKT)
    "WIDESTP": "STOP-WIDE",   # phase-1 safety stop, replaced once the fill price is known
    "STPMKT" : "STOP",        # phase-2 protective stop — this is the one that trails
    "CLOSE"  : "EXIT",        # square-off / reconciliation close
    "TECH"   : "EXIT-TECH",   # delta/gamma technical exit (currently disabled)
}

_log_lock = threading.Lock()


# 2026-08-04: the REAL (phase-2) protective stop is placed with parentId=none —
# its only link back to the entry is the parent oid embedded in its order_ref,
# e.g. "STPMKT_TWS_247_1785249630" guards entry oid 247. Matching on parentId
# alone linked each entry to the phase-1 WIDE stop (which IBKR cancels), so the
# panel showed a dead stop's original size instead of the live one.
_RE_STPMKT_PARENT = re.compile(r"^STPMKT_(?:[A-Z]+_)?(\d+)_", re.I)


def _parent_oid_from_ref(ref: str) -> Optional[int]:
    """Parent order id embedded in a phase-2 stop's order_ref, if present."""
    m = _RE_STPMKT_PARENT.match(str(ref or ""))
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def _role_for(ref: str) -> str:
    """Map an order_ref like 'STPMKT_TWS_1785421099_XLK' onto a lifecycle role."""
    if not ref:
        return ""
    up = str(ref).upper()
    if up.endswith("_EOD") or "_EOD_" in up:
        return "EOD-EXIT"
    for prefix, role in _ROLE_BY_REF.items():
        if up.startswith(prefix):
            return role
    return ""


# ── Orders from the engine log (logs only — no live IBKR poll) ───────────────

def _today_log_path() -> Path:
    """Today's engine log. VM tz = America/New_York; engine tags it the same."""
    return _LOG_DIR / f"tws_{datetime.now().strftime('%d_%m_%y')}.txt"


def _consume_log_chunk(chunk: str, recs: Dict[int, Dict],
                       be: Optional[Dict[str, Dict]] = None,
                       execmap: Optional[Dict[str, int]] = None) -> None:
    """Fold newly-appended log lines into the per-oid order records.

    2026-07-31: also records the stop's PRICE HISTORY (every modify on that oid)
    and breakeven activations, so the dashboard can show that a stop has MOVED
    rather than only its latest resting price.
    """
    if be is None:
        be = {}
    if execmap is None:
        execmap = {}
    for line in chunk.splitlines():
        # breakeven activation is an engine line, not a "TWS " line — test first
        m = _RE_BE.search(line)
        if m:
            t, sym, direction, stop = m.groups()
            be[sym] = {"time": t, "direction": direction, "stop": float(stop)}
            continue
        if "TWS " not in line:
            continue
        m = _RE_PLACED.search(line)
        if m:
            t, oid, side, sym, qty, otype, price, ref, parent = m.groups()
            r = recs.setdefault(int(oid), {"oid": int(oid)})
            r.update(symbol=sym, side=side, qty=float(qty),
                     order_type=otype.strip(), price=float(price), ref=ref, time=t)
            r["role"] = _role_for(ref)
            r["placed_time"] = t
            # first price this order ever carried — the baseline a trail moves from
            r.setdefault("orig_price", float(price))
            if parent and parent.lower() not in ("none", "0"):
                try:
                    r["parent_oid"] = int(parent)
                except ValueError:
                    pass
            r.setdefault("status", "PendingSubmit")
            continue
        # NOTE: modify / cancel / error lines only ANNOTATE an order we already
        # saw placed — they never create a record. IBKR emits warnings (e.g. 2176
        # fractional-share) against ids that are not orders at all; creating rows
        # from those put ~80 phantom entries a day into the panel.
        m = _RE_MODIFY.search(line)
        if m:
            t, oid, otype, new_price, new_aux = m.groups()
            r = recs.get(int(oid))
            if r is None:
                continue
            npx = float(new_price)
            hist = r.setdefault("stop_history", [])
            # ignore a no-op re-send so the move count reflects real movement
            if not hist or abs(hist[-1][1] - npx) > 1e-9:
                hist.append((t, npx))
            r["price"] = npx
            r["last_move_time"] = t
            if new_aux and float(new_aux) > 0:
                r["aux"] = float(new_aux)
            continue
        m = _RE_CANCEL.search(line)
        if m:
            t, oid = m.groups()
            r = recs.get(int(oid))
            if r is None:
                continue
            r["cancel_requested"] = True
            continue
        m = _RE_ORDERR.search(line)
        if m:
            t, oid, code, msg = m.groups()
            code_n = int(code)
            r = recs.get(int(oid))
            if r is None:
                continue
            if code_n in _WARN_CODES:
                r["warn"] = f"{code_n}: {msg}"
                continue
            # An error that arrives AFTER the order transacted can never un-do
            # it. The engine cancels a stop as cleanup the instant it fills, so
            # a cancel-race error routinely lands on an order that is already
            # Filled — downgrading it to "Rejected" would misreport a REAL
            # execution as a failure. Record the error, keep the true status.
            _already_done = (
                str(r.get("status") or "").lower() == "filled"
                or float(r.get("fill_qty") or 0.0) > 0
                or float(r.get("filled") or 0.0) > 0
            )
            r["error_code"] = code_n
            r["error_msg"] = msg
            if not _already_done:
                r["status"] = "Rejected"
            continue
        m = _RE_OPEN.search(line)
        if m:
            t, oid, action, sym, otype, qty, lmt, aux, status = m.groups()
            r = recs.setdefault(int(oid), {"oid": int(oid)})
            r.update(symbol=sym, side=action, qty=float(qty),
                     order_type=otype.strip(), lmt=float(lmt), aux=float(aux),
                     status=status, time=t)
            continue
        m = _RE_STATUS.search(line)
        if m:
            t, oid, status, filled, remaining, avg = m.groups()
            r = recs.setdefault(int(oid), {"oid": int(oid)})
            r.update(status=status, filled=float(filled),
                     remaining=float(remaining), avg_fill=float(avg), time=t)
            continue
        m = _RE_FILL.search(line)
        if m:
            t, oid, sym, fside, fqty, fprice, exec_id, exch = m.groups()
            r = recs.setdefault(int(oid), {"oid": int(oid)})
            # 2026-08-04: NEVER double-count an execution. IBKR re-sends
            # execDetails after a reconnect/reqExecutions, and the engine logs
            # each one — without this an order could report MORE shares than it
            # actually executed, which is the one error that must not happen.
            if exec_id:
                seen = r.setdefault("_exec_seen", set())
                if exec_id in seen:
                    continue
                seen.add(exec_id)
            r["symbol"]        = sym
            r["fill_side"]     = fside
            r["fill_qty"]      = r.get("fill_qty", 0.0) + float(fqty)
            r["fill_notional"] = r.get("fill_notional", 0.0) + float(fqty) * float(fprice)
            r["time"]          = t
            # partial fills are normal (S15.7) — keep first/last so the panel can
            # show how long a fill took to complete, plus the print count
            r.setdefault("first_fill_time", t)
            r["last_fill_time"] = t
            r["fill_prints"]    = r.get("fill_prints", 0) + 1
            if exch:
                r["exchange"] = exch
            # 2026-08-03: keep EVERY execution, not just the aggregate, so the
            # panel can show what actually executed print-by-print rather than
            # one rolled-up number that hides partial fills.
            r.setdefault("fills", []).append({
                "time": t, "qty": float(fqty), "price": float(fprice),
                "exchange": exch or "", "exec_id": exec_id or "",
            })
            if exec_id:
                execmap[exec_id] = int(oid)
            continue
        m = _RE_COMMISSION.search(line)
        if m:
            exec_id, comm, rpnl = m.groups()
            oid = execmap.get(exec_id)
            if oid is None or oid not in recs:
                continue
            r = recs[oid]
            if exec_id in (r.get("_comm_seen") or set()):
                continue                       # never double-count a re-sent report
            r.setdefault("_comm_seen", set()).add(exec_id)
            try:
                # accumulate at FULL precision and round only on output —
                # rounding each of ~200 reports as it arrives drifts the daily
                # total by several cents against the broker's own figure
                r["commission"] = r.get("commission", 0.0) + float(comm)
                if rpnl not in (None, ""):
                    val = float(rpnl)
                    # IBKR sends a sentinel ~1.8e308 when realized P&L is N/A
                    if abs(val) < 1e10:
                        r["ibkr_realized"] = r.get("ibkr_realized", 0.0) + val
            except (TypeError, ValueError):
                pass
            continue


def _maybe_daily_clear() -> bool:
    """Post-market daily clean (2026-07-31, user directive).

    2026-08-05 — REWRITTEN TO BE NON-DESTRUCTIVE. The original version wiped
    `log_orders` and jumped `log_offset` to EOF. That destroyed, in one shot:
    the orders panel, the Stop-Loss Movements panel, the "moved N×" annotation
    on every position, and the day's P&L — and, because the offset had skipped
    past them, none of it could be rebuilt without restarting the bot. A single
    stray request to /api/clear therefore blanked the dashboard for the rest of
    the session.

    Now a "clear" only records a CUT-OFF TIME. Parsing continues untouched and
    the display simply hides rows older than that mark, so the action is
    reversible (/api/clear?undo=1) and nothing can ever be lost.
    """
    now = datetime.now()                       # VM tz = America/New_York
    today = now.strftime("%Y-%m-%d")
    try:
        hh, mm = (int(x) for x in _CLEAR_TIME.split(":", 1))
    except (ValueError, AttributeError):
        hh, mm = 16, 5
    if (now.hour, now.minute) < (hh, mm):
        return False
    with _cache_lock:
        if _cache.get("cleared_on") == today:
            return False
        _cache["cleared_on"] = today
        _cache["cleared_before"] = now.strftime("%H:%M:%S")
        _cache["pos_first_seen"] = {}
    return True


def _visible(rec_time: Any, cutoff: Optional[str]) -> bool:
    """True when a row is at/after the current clear cut-off.

    Rows carry "HH:MM:SS" strings from the engine log, so a plain string
    compare is a correct chronological compare within one day.
    """
    if not cutoff:
        return True
    if not rec_time:
        return False                # undated rows are pre-existing -> hide
    return str(rec_time) >= str(cutoff)


def _refresh_log_orders() -> None:
    """Incrementally tail today's engine log and rebuild per-oid order records.
    Orders panel is sourced from this ONLY (instant; no 5s poll lag). Never
    raises. 2026-06-23."""
    with _log_lock:
        # 2026-08-05: the daily clear only sets a display cut-off now, so
        # parsing MUST continue in the same cycle — never return early here.
        _maybe_daily_clear()
        path = _today_log_path()
        with _cache_lock:
            cur  = _cache.get("log_path") or ""
            off  = int(_cache.get("log_offset") or 0)
            recs = _cache.get("log_orders") or {}
            bes  = _cache.get("be_events") or {}
            xmap = _cache.get("exec_map") or {}
        if str(path) != cur:            # new day / first run -> reset
            recs, off, bes, xmap = {}, 0, {}, {}
        if not path.exists():
            with _cache_lock:
                _cache["log_path"], _cache["log_offset"], _cache["log_orders"] = str(path), 0, recs
                _cache["be_events"], _cache["exec_map"] = bes, xmap
            return
        try:
            size = path.stat().st_size
            if size < off:              # truncated / rotated -> reparse
                recs, off, bes, xmap = {}, 0, {}, {}
            if size > off:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    f.seek(off)
                    chunk = f.read()
                    off = f.tell()
                _consume_log_chunk(chunk, recs, bes, xmap)
                with _cache_lock:
                    _cache["orders_fresh_at"] = time.time()
        except Exception:
            pass
        with _cache_lock:
            _cache["log_path"], _cache["log_offset"], _cache["log_orders"] = str(path), off, recs
            _cache["be_events"], _cache["exec_map"] = bes, xmap


def _log_loop() -> None:
    """Tail the engine log frequently in its own daemon thread so the orders
    panel stays near-instant. Incremental reads are cheap; the only larger read
    is a one-time full parse if the dashboard (re)starts mid-day — kept OFF the
    HTTP request path so no /api/data call ever blocks on it. 2026-06-23."""
    while True:
        try:
            _refresh_log_orders()
        except Exception:
            pass
        time.sleep(1.5)


# ── Background cache refresh (one reqPositions per 5s; orders come from log) ──

def _cache_loop() -> None:
    """Refresh TWS-sourced data every 5 seconds in a daemon thread."""
    while True:
        try:
            _refresh_tws_data()
        except Exception:
            pass
        time.sleep(5.0)


def _refresh_tws_data() -> None:
    """Inner refresh — all exceptions bubble up to _cache_loop.

    FIX (2026-05-21): when TWS receive is broken, `engine.get_positions()`
    returns an empty dict (positionEnd never fires, returns timeout-empty).
    Old code overwrote the cache with empty → dashboard lied "0 positions"
    while IBKR was actually holding shares. Now we PRESERVE last-known-good
    positions and surface a `positions_stale_sec` field so the dashboard
    can show a warning instead of zero.
    """
    if _bot is None:
        return
    engine = getattr(_bot, "_engine", None)
    if engine is None:
        return

    new_positions: Dict[str, Any] = {}
    new_orders = None                 # None = orders poll did NOT complete -> preserve last good
    positions_came_back: bool = False

    tws = getattr(engine, "_tws", None)
    # Feed health: only treat an EMPTY positions result as "truly flat" when the
    # receive channel is HEALTHY. When the feed is broken, an empty result is a
    # timeout artifact, so we preserve the last-known-good positions instead.
    try:
        recv_ok = bool(
            tws is not None and getattr(tws, "is_connected", False)
            and not (hasattr(tws, "is_receive_broken") and tws.is_receive_broken())
        )
    except Exception:
        recv_ok = False

    try:
        new_positions = engine.get_positions(timeout=3.0) or {}
        positions_came_back = bool(new_positions)
    except Exception:
        pass

    # 2026-06-23: the orders panel is now sourced from the engine LOG (see
    # _refresh_log_orders), so the 5s reqAllOpenOrders poll is no longer needed.
    # Skipping it removes a request/response round-trip from the IB reader thread
    # every 5s (less backlog → fewer receive-stale events). live_orders stays
    # empty; orders_stale_sec now reflects log freshness.
    new_orders = None

    with _cache_lock:
        # Trust positions when they came back non-empty, OR when the feed is
        # HEALTHY and returned empty (= genuinely flat -> clear the stale row).
        # Only PRESERVE last-known-good when the feed is broken AND empty (a
        # timeout artifact); the dashboard then flags positions_stale_sec. This
        # fixes the dashboard showing a CLOSED position as still open.
        if positions_came_back or recv_ok:
            _cache["tws_positions"]      = new_positions
            _cache["positions_fresh_at"] = time.time()
            # 2026-06-09: stamp first-seen so the dashboard can show an entry
            # Time even for positions the bot never registered in _open_positions.
            first_seen = _cache.get("pos_first_seen") or {}
            now_hms = datetime.now().strftime("%H:%M:%S")   # VM tz = America/New_York
            for _s in new_positions:
                first_seen.setdefault(_s, now_hms)
            for _s in list(first_seen):
                if _s not in new_positions:
                    first_seen.pop(_s, None)
            _cache["pos_first_seen"] = first_seen
        # Orders: only overwrite when the poll genuinely completed, so we never
        # replace real working orders with a spurious empty list on a failed/
        # disconnected poll; otherwise preserve last-good + flag staleness.
        if new_orders is not None:
            _cache["live_orders"]     = new_orders
            _cache["orders_fresh_at"] = time.time()
        _cache["refreshed_at"] = time.time()


# ── Data assembly ─────────────────────────────────────────────────────────────

def _safe_dict_items(d: Any) -> List:
    """Thread-safe snapshot of dict items — handles concurrent modification."""
    try:
        return list(d.items())
    except RuntimeError:
        return []
    except Exception:
        return []


def _build_macd_state() -> List[Dict[str, Any]]:
    """
    Per-symbol MACD snapshot for the dashboard panel. Reads pipeline
    state directly — no TWS calls, no locks held outside the snapshot.

    Output rows include:
      symbol, is_index, resolution, ready, seeded
      fast, slow, macd, signal, diff
      prev_macd (last committed bar's MACD — used to gauge proximity-to-zero)
      signal_pos_mean / signal_neg_mean (legacy Signal-line mean)
      diff_pos_mean   / diff_neg_mean   (mean of MACD Differential)
      intrabar_armed  (True if the one-shot guard is NOT set this bar)
    """
    rows: List[Dict[str, Any]] = []
    if _bot is None:
        return rows

    try:
        stocks  = dict(getattr(_bot, "_pipelines", {}) or {})
    except Exception:
        stocks = {}
    try:
        indexes = dict(getattr(_bot, "_idx_pipes", {}) or {})
    except Exception:
        indexes = {}

    # 2026-06-03: per-symbol mean multipliers so the dashboard can show the
    # means ALREADY MULTIPLIED by MeanMultiplier_L / _S (the actual gate
    # threshold), per user request.
    try:
        import json as _json
        _cfg = _json.load(open("config.json"))
    except Exception:
        _cfg = {}
    _g_ml = _cfg.get("MeanMultiplier_L", 1.0)
    _g_ms = _cfg.get("MeanMultiplier_S", 1.0)
    _stk_cfg = _cfg.get("Stocks", {}) or {}
    def _mult(sym):
        e = _stk_cfg.get(sym, {}) or {}
        ml = e.get("mean_multiplier_l")
        ms = e.get("mean_multiplier_s")
        ml = float(ml) if ml not in (None, "") else float(_g_ml or 1.0)
        ms = float(ms) if ms not in (None, "") else float(_g_ms or 1.0)
        return ml, ms

    for sym, pipe in list(stocks.items()) + list(indexes.items()):
        try:
            _ml, _ms = _mult(sym)          # 2026-06-03: per-symbol mean multipliers
            eng = getattr(pipe, "_engine", None)
            if eng is None:
                continue

            # EMA values — None until the engine has seen at least one price.
            fast = float(eng._ef.value) if getattr(eng._ef, "value", None) is not None else None
            slow = float(eng._es.value) if getattr(eng._es, "value", None) is not None else None
            macd_now = (fast - slow) if (fast is not None and slow is not None) else None

            # Signal line + history reads (newest first).
            sig_hist = list(getattr(eng, "_sig_hist", []) or [])
            signal_now = float(sig_hist[0]) if sig_hist else None
            diff_now = (macd_now - signal_now) if (macd_now is not None and signal_now is not None) else None

            # 2026-06-03: show the LIVE (forming-bar) MACD/signal so the dashboard
            # tracks the chart instead of lagging at the last CLOSED bar. Uses the
            # latest trade price, computed non-mutatingly (display only — the
            # bot still TRADES on committed bar closes).
            lp = getattr(pipe, "_last_tick_price", None)
            if lp is not None and hasattr(eng, "compute_provisional_all"):
                try:
                    live = eng.compute_provisional_all(float(lp))
                except Exception:
                    live = None
                if live:
                    fast, slow = live["fast"], live["slow"]
                    macd_now, signal_now = live["macd"], live["signal"]
                    diff_now = macd_now - signal_now

            means = getattr(eng, "_means", None)
            dmeans = getattr(eng, "_diff_means", None)
            sig_pos_mean = float(means.pos_mean) if means is not None else 0.0
            sig_neg_mean = float(means.neg_mean) if means is not None else 0.0
            diff_pos_mean = float(dmeans.pos_mean) if dmeans is not None else 0.0
            diff_neg_mean = float(dmeans.neg_mean) if dmeans is not None else 0.0

            # 2026-05-22: expose per-symbol mean-time settings so the user
            # can verify Excel changes have taken effect on the dashboard.
            mean_calc_start = getattr(pipe, "_mean_calc_start", None)
            mean_calc_str = mean_calc_start.strftime("%H:%M") if mean_calc_start else None

            # 2026-06-03: EXIT-pipeline values (different timeframe). When there's
            # no exit pipe (exit candles == entry), exit_* mirror the entry values
            # so the dashboard shows a single number (no slash).
            _params = getattr(pipe, "_params", {}) or {}
            entry_res = str(_params.get("resolution", "?"))
            exit_res  = str(_params.get("exit_resolution", entry_res))
            try:
                exit_pipe = dict(getattr(_bot, "_exit_pipes", {}) or {}).get(sym)
            except Exception:
                exit_pipe = None
            if exit_pipe is not None:
                eeng = getattr(exit_pipe, "_engine", None)
                ef2 = float(eeng._ef.value) if (eeng is not None and getattr(eeng._ef, "value", None) is not None) else None
                es2 = float(eeng._es.value) if (eeng is not None and getattr(eeng._es, "value", None) is not None) else None
                exit_macd = (ef2 - es2) if (ef2 is not None and es2 is not None) else None
                eh = list(getattr(eeng, "_sig_hist", []) or []) if eeng is not None else []
                exit_signal = float(eh[0]) if eh else None
                # live (forming-bar) exit values too, using the same last price
                if lp is not None and eeng is not None and hasattr(eeng, "compute_provisional_all"):
                    try:
                        elive = eeng.compute_provisional_all(float(lp))
                    except Exception:
                        elive = None
                    if elive:
                        exit_macd, exit_signal = elive["macd"], elive["signal"]
                exit_diff = (exit_macd - exit_signal) if (exit_macd is not None and exit_signal is not None) else None
                em = getattr(eeng, "_means", None); edm = getattr(eeng, "_diff_means", None)
                exit_sig_pos  = float(em.pos_mean) if em is not None else 0.0
                exit_sig_neg  = float(em.neg_mean) if em is not None else 0.0
                exit_diff_pos = float(edm.pos_mean) if edm is not None else 0.0
                exit_diff_neg = float(edm.neg_mean) if edm is not None else 0.0
                exit_res = str(getattr(exit_pipe, "_params", {}).get("resolution", exit_res))
            else:
                exit_macd, exit_signal, exit_diff = macd_now, signal_now, diff_now
                exit_sig_pos, exit_sig_neg   = sig_pos_mean, sig_neg_mean
                exit_diff_pos, exit_diff_neg = diff_pos_mean, diff_neg_mean

            rows.append({
                "symbol"         : sym,
                "is_index"       : sym in indexes,
                "resolution"     : entry_res,
                "entry_resolution": entry_res,
                "exit_resolution": exit_res,
                "exit_macd"      : round(exit_macd, 6) if exit_macd is not None else None,
                "exit_signal"    : round(exit_signal, 6) if exit_signal is not None else None,
                "exit_diff"      : round(exit_diff, 6) if exit_diff is not None else None,
                "exit_signal_pos_mean": round(exit_sig_pos * _ms, 6),
                "exit_signal_neg_mean": round(exit_sig_neg * _ml, 6),
                "exit_diff_pos_mean"  : round(exit_diff_pos, 6),
                "exit_diff_neg_mean"  : round(exit_diff_neg, 6),
                "ready"          : bool(eng.is_ready()) if hasattr(eng, "is_ready") else False,
                "seeded"         : bool(getattr(pipe, "is_real_seeded", lambda: False)()),
                "fast"           : round(fast, 4) if fast is not None else None,
                "slow"           : round(slow, 4) if slow is not None else None,
                "macd"           : round(macd_now, 6) if macd_now is not None else None,
                "signal"         : round(signal_now, 6) if signal_now is not None else None,
                "diff"           : round(diff_now, 6) if diff_now is not None else None,
                "prev_macd"      : (round(float(pipe._prev_macd), 6)
                                    if getattr(pipe, "_prev_macd", None) is not None else None),
                "signal_pos_mean": round(sig_pos_mean * _ms, 6),
                "signal_neg_mean": round(sig_neg_mean * _ml, 6),
                "diff_pos_mean"  : round(diff_pos_mean, 6),
                "diff_neg_mean"  : round(diff_neg_mean, 6),
                "intrabar_armed" : not bool(getattr(pipe, "_intrabar_fired_for_current_bar", False)),
                "mean_calc_start": mean_calc_str,
            })
        except Exception:
            # One bad pipeline must not break the rest of the snapshot.
            continue
    # Sort: stocks first (alphabetical), then indexes
    rows.sort(key=lambda r: (r["is_index"], r["symbol"]))
    return rows


def _build_round_trips(log_orders: Dict[int, Dict]) -> List[Dict[str, Any]]:
    """Reconstruct completed round trips from today's fills (logs only).

    Walks each symbol's filled orders in time order, signing BOT +qty / SLD
    -qty, and closes a trip whenever the running position returns to flat.
    Uses average cost on the open side — with the engine's one-entry-then-exit
    lifecycle this equals FIFO, and it degrades gracefully on partial fills.
    """
    by_sym: Dict[str, List[Dict]] = {}
    for r in log_orders.values():
        if not r.get("fill_qty"):
            continue
        sym = r.get("symbol") or ""
        if sym:
            by_sym.setdefault(sym, []).append(r)

    trips: List[Dict[str, Any]] = []
    for sym, orders in by_sym.items():
        orders.sort(key=lambda r: r.get("first_fill_time") or r.get("time") or "")
        pos = 0.0            # signed open quantity
        avg = 0.0            # average price of the open side
        opened_at = None
        entry_side = ""
        realized = 0.0
        # 2026-08-06: commission paid OPENING the position. Only the closing
        # execution's commission used to reach the round-trip row, so a trade's
        # net on this panel read better than it was by exactly the entry
        # commission — and disagreed with the Shadow Trader's net for the same
        # trade. Accumulate the open-side commission and net it off below.
        entry_comm = 0.0
        for r in orders:
            qty = float(r.get("fill_qty") or 0.0)
            if qty <= 0:
                continue
            px = float(r.get("fill_notional") or 0.0) / qty
            sign = 1.0 if str(r.get("fill_side", "")).upper() == "BOT" else -1.0
            signed = sign * qty
            if pos == 0 or (pos > 0) == (signed > 0):
                # opening or adding
                if pos == 0:
                    opened_at = r.get("first_fill_time") or r.get("time")
                    entry_side = "LONG" if signed > 0 else "SHORT"
                    avg = px
                    entry_comm = 0.0
                else:
                    avg = (avg * abs(pos) + px * qty) / (abs(pos) + qty)
                try:
                    entry_comm += float(r.get("commission") or 0.0)
                except (TypeError, ValueError):
                    pass
                pos += signed
            else:
                # reducing / closing
                closing = min(abs(pos), qty)
                realized += (px - avg) * closing * (1.0 if pos > 0 else -1.0)
                pos += signed
                if abs(pos) < 1e-9:
                    pos = 0.0
                    trips.append({
                        "symbol"    : sym,
                        "side"      : entry_side,
                        "qty"       : int(closing),
                        "entry_time": opened_at,
                        "entry_px"  : round(avg, 4),
                        "exit_time" : r.get("last_fill_time") or r.get("time"),
                        "exit_px"   : round(px, 4),
                        "pnl"       : round(realized, 2),
                        # Broker's OWN realized P&L for the closing execution,
                        # MINUS the commission paid to open — so this row is the
                        # true net for the trade and matches the Shadow Trader.
                        "ibkr_pnl"  : (round(r["ibkr_realized"] - entry_comm, 2)
                                       if r.get("ibkr_realized") is not None else None),
                        "commission": round((float(r.get("commission") or 0.0)
                                             + entry_comm), 2),
                        "entry_commission": round(entry_comm, 2),
                        "exit_role" : r.get("role") or "",
                    })
                    realized = 0.0
    trips.sort(key=lambda t: t.get("exit_time") or "")
    return trips


def _live_price(sym: str) -> Optional[float]:
    """
    Latest traded price for a symbol.

    Primary source is the shadow broker's quote cache — it is fed by the very
    ticks the fill engine prices against, so what the dashboard shows and what
    a stop would fill at can never disagree. Falls back to the entry pipeline's
    last tick if the shadow has no quote yet (e.g. shadow mode off).
    """
    try:
        from shadow_broker import SHADOW
        q = (SHADOW._quotes or {}).get(sym) or {}
        last = float(q.get("last") or 0.0)
        if last > 0:
            return last
    except Exception:
        pass
    try:
        pipe = (_bot._pipelines.get(sym) if _bot else None) or \
               (_bot._exit_pipes.get(sym) if _bot else None)
        last = float(getattr(pipe, "_last_tick_price", 0) or 0)
        return last if last > 0 else None
    except Exception:
        return None


def _build_response() -> Dict[str, Any]:
    """Assemble API response from in-memory state. Never raises."""
    result: Dict[str, Any] = {
        "tws_connected"  : False,
        "positions"      : [],
        "orders_working" : [],
        "orders_filled"  : [],
        "orders_cancelled": [],
        "orders_stale"   : [],
        "orders_all"     : [],
        "stop_moves"     : [],
        "breakeven_events": [],
        "round_trips"    : [],
        "realized_pnl"   : 0.0,
        "ibkr_realized_pnl": None,
        "commissions"    : 0.0,
        "shares_executed": 0,
        "qty_discrepancies": 0,
        "qty_discrepancy_orders": [],
        "stop_coverage_alerts": 0,
        "macd_state"     : [],
        "stats"          : {},
        "rates"          : {},
        "positions_stale_sec": None,   # seconds since reqPositions returned data
        "orders_stale_sec": None,      # seconds since the live-orders poll completed
        # 2026-08-06: SHADOW mode. Orders are simulated locally instead of being
        # sent to IBKR (see shadow_broker.py). The orders/fills/stop-move panels
        # need no change — the shadow broker emits the same log lines this file
        # parses — but the header must say so, and the shadow's own net P&L
        # (realized + open mark-to-market) is surfaced here too.
        "mode"           : "LIVE",
        "shadow"         : None,
        "timestamp"      : datetime.now(timezone.utc).isoformat(),
    }

    # ── Shadow-mode summary (safe if shadow_broker is absent) ────────────────
    try:
        from shadow_broker import SHADOW
        if SHADOW.enabled:
            result["mode"] = "SHADOW"
            result["shadow"] = {
                "realized"  : round(SHADOW._realized, 2),
                "unrealized": SHADOW.unrealized(),
                "net"       : SHADOW.net_pnl(),
                "commission": round(SHADOW._commission_total, 2),
                "trades"    : len(SHADOW._closed),
                "open"      : len(SHADOW._positions),
                "url"       : "http://%s:8090/" % (os.getenv("SHADOW_HOST", "<VM_HOST>")),
            }
    except Exception:
        pass

    if _bot is None:
        return result

    try:
        engine = getattr(_bot, "_engine", None)
        if engine is None:
            return result

        tws = getattr(engine, "_tws", None)
        result["tws_connected"] = bool(
            tws is not None and getattr(tws, "is_connected", False)
        )

        # ── Stats / rates (zero TWS calls) ───────────────────────────────
        try:
            result["stats"] = engine.get_stats()
        except Exception:
            pass
        try:
            result["rates"] = engine.get_rates()
        except Exception:
            pass

        # ── MACD state per symbol (visibility panel) ──────────────────────
        try:
            result["macd_state"] = _build_macd_state()
        except Exception:
            pass

        # ── Positions ─────────────────────────────────────────────────────
        with _cache_lock:
            tws_pos = dict(_cache.get("tws_positions") or {})
            fresh_at = float(_cache.get("positions_fresh_at") or 0.0)
            orders_fresh_at = float(_cache.get("orders_fresh_at") or 0.0)
        if fresh_at > 0:
            result["positions_stale_sec"] = round(time.time() - fresh_at, 1)
        if orders_fresh_at > 0:
            result["orders_stale_sec"] = round(time.time() - orders_fresh_at, 1)

        try:
            bot_pos = dict(getattr(_bot, "_open_positions", {}) or {})
        except Exception:
            bot_pos = {}

        with _cache_lock:
            first_seen = dict(_cache.get("pos_first_seen") or {})

        # 2026-06-09: SL fallback. When the bot isn't tracking a position in
        # _open_positions (adopted, or the parent filled while TWS receive was
        # stale so on_fill never registered it), bp.stop_price is blank. Recover
        # the REAL protective stop from the active working STP order at IBKR so
        # the Stop (SL) column is always populated for an open position.
        # ── 2026-08-04: LIVE protective stop per symbol, straight from the log ──
        # The position panel must show the stop that is ACTUALLY working at IBKR
        # right now — its price, its share count, and every move it has made —
        # not the phase-1 wide stop that gets cancelled seconds after the fill.
        # Log-sourced per the standing directive (orders/fills come from the log).
        live_stop_by_sym: Dict[str, Dict[str, Any]] = {}
        try:
            with _cache_lock:
                _lo = dict(_cache.get("log_orders") or {})
            _DEADS = {"cancelled", "apicancelled", "inactive", "filled",
                      "rejected", "apipending", "pendingcancel"}
            for _r in _lo.values():
                if "STP" not in str(_r.get("order_type") or "").upper():
                    continue
                if str(_r.get("status") or "").lower() in _DEADS:
                    continue          # only stops still working at IBKR
                _s = _r.get("symbol") or ""
                if not _s:
                    continue
                _prev = live_stop_by_sym.get(_s)
                if _prev is not None and int(_r.get("oid", 0)) < int(_prev["oid"]):
                    continue          # newest working stop wins (phase-2 > wide)
                _h = _r.get("stop_history") or []
                _px = _h[-1][1] if _h else _r.get("price")
                _rm = _r.get("remaining")
                try:
                    _sq = (int(float(_rm)) if _rm not in (None, "")
                           else int(float(_r.get("qty") or 0)) or None)
                except (TypeError, ValueError):
                    _sq = None
                live_stop_by_sym[_s] = {
                    "oid": int(_r.get("oid", 0)),
                    "price": round(float(_px), 2) if _px else None,
                    "orig": (round(float(_r["orig_price"]), 2)
                             if _r.get("orig_price") else None),
                    "qty": _sq,
                    "moves": len(_h),
                    "last_move": _r.get("last_move_time"),
                    "role": _r.get("role"),
                }
        except Exception:
            pass

        stop_by_sym: Dict[str, float] = {}
        stoplmt_by_sym: Dict[str, float] = {}
        try:
            _placed = _safe_dict_items(getattr(engine, "_placed_orders", {}) or {})
            _statuses = dict(getattr(tws, "_order_statuses", {}) or {}) if tws is not None else {}
            _DEAD = ("cancelled", "inactive", "apipending", "pendingcancel",
                     "apicancelled", "filled")
            _best_oid: Dict[str, int] = {}
            for _ref, _pinfo in _placed:
                _t = _pinfo.get("ticket")
                if _t is None:
                    continue
                _otype = (getattr(_t, "order_type", "") or "").upper()
                if "STP" not in _otype:
                    continue
                _psym = getattr(_t, "symbol", "") or ""
                # 2026-07-16: the STOP TRIGGER is ticket.price for BOTH plain STP
                # (stop-market) and STP LMT. ticket.aux_price is the LIMIT price,
                # which is 0 on a stop-market — the old code read aux_price and so
                # the fallback stop never populated for the stop-market-only config.
                _stp  = float(getattr(_t, "price", 0) or 0)
                _slmt = float(getattr(_t, "aux_price", 0) or 0) if "LMT" in _otype else 0.0
                if not _psym or _stp <= 0:
                    continue
                _oid = _pinfo.get("channel_oid")
                _st = (_statuses.get(_oid, {}) or {}).get("status", "") if _oid is not None else ""
                if str(_st).lower() in _DEAD:
                    continue
                _oid_n = _oid if isinstance(_oid, int) else -1
                if _oid_n >= _best_oid.get(_psym, -1):   # latest stop = current trail
                    _best_oid[_psym] = _oid_n
                    stop_by_sym[_psym] = round(_stp, 2)
                    if _slmt > 0:
                        stoplmt_by_sym[_psym] = round(_slmt, 2)
        except Exception:
            pass

        for sym, info in tws_pos.items():
            try:
                avg_cost = round(float(info.get("avg", 0) or 0), 4)
                qty      = float(info.get("qty", 0) or 0)
                side     = info.get("side", "")
                bp       = bot_pos.get(sym) or {}
                direction = bp.get("direction") if bp else None

                # 2026-06-04: current (trailed) stop-loss for the position.
                # 2026-06-09: fall back to the live working stop at IBKR, then
                # entry_time falls back to first-seen — so SL + Time are always
                # shown for an open position, even one the bot never registered.
                _sl = bp.get("stop_price")
                if _sl in (None, ""):
                    _sl = stop_by_sym.get(sym)
                # stop LIMIT price (companion to the stop trigger). Internal
                # tracking first, then the live STP-LMT working order at IBKR.
                _sll = bp.get("stop_limit_price")
                if _sll in (None, ""):
                    _sll = stoplmt_by_sym.get(sym)
                # 2026-07-16: RISK-SYSTEM RECONCILIATION — the quantity our bot
                # thinks it holds (_open_positions[sym].qty) MUST equal what IBKR
                # actually executed (tws_pos qty). Surface both + a match flag so
                # any divergence (the EoD direction-flip class of bug) is visible.
                _qi = bp.get("qty")
                try:
                    _qi_abs = int(abs(float(_qi))) if _qi not in (None, "") else None
                except (TypeError, ValueError):
                    _qi_abs = None
                _qmatch = (_qi_abs == int(abs(qty))) if _qi_abs is not None else None
                # 2026-06-11: trail visibility — be_activated = stop has started
                # trailing; locked_pnl = guaranteed $ if the stop fills now
                # (positive once the trail has moved the stop past entry into
                # profit). Lets the dashboard SHOW the trailing stop working.
                _be = bool(bp.get("be_activated"))
                _is_short = (qty < 0) or str(side).upper() == "SHORT" or str(direction or "").upper() == "SHORT"
                _locked = None
                try:
                    if _sl not in (None, "") and avg_cost:
                        _locked = ((avg_cost - float(_sl)) if _is_short else (float(_sl) - avg_cost)) * abs(qty)
                        _locked = round(_locked, 2)
                except (TypeError, ValueError):
                    _locked = None
                # 2026-08-04: prefer the LIVE stop from the log — price, share
                # count and move history — and flag when the stop does NOT cover
                # the position. An uncovered/oversized stop is the dangerous case
                # (a 500-share stop against an 80-share fill would flip the
                # position short on a stop-out), so it must be visible.
                _ls = live_stop_by_sym.get(sym) or {}
                if _ls.get("price") is not None:
                    _sl = _ls["price"]
                _pos_abs = int(abs(qty))
                _stop_qty = _ls.get("qty")
                _covers = (int(_stop_qty) == _pos_abs) if _stop_qty else None

                # 2026-08-06: LIVE PRICE + UNREALISED P&L. Until now this panel
                # showed avg_cost only and unrealized_pnl was hard-coded None —
                # there was no way to see a position move. The price comes from
                # the shadow broker's quote cache, which is fed by the same real
                # IBKR ticks the MACD pipelines consume, so it updates on every
                # tick. Falls back to the entry pipeline's last tick price.
                _last = _live_price(sym)
                _upl = _upl_pct = None
                if _last and avg_cost:
                    _sgn = -1.0 if _is_short else 1.0
                    _upl = round((_last - avg_cost) * abs(qty) * _sgn, 2)
                    _upl_pct = round((_last - avg_cost) / avg_cost * 100.0 * _sgn, 3)

                result["positions"].append({
                    "symbol"        : sym,
                    "side"          : side,
                    "qty"           : qty,
                    "avg_cost"      : avg_cost,
                    "last_price"    : round(_last, 4) if _last else None,
                    "unrealized_pnl": _upl,
                    "unrealized_pct": _upl_pct,
                    "direction"     : direction,
                    "entry_time"    : bp.get("entry_time") or first_seen.get(sym),
                    "stop_loss"     : round(float(_sl), 2) if _sl not in (None, "") else None,
                    "stop_limit"    : round(float(_sll), 2) if _sll not in (None, "") else None,
                    # live-stop detail
                    "stop_qty"      : _stop_qty,
                    "stop_covers"   : _covers,
                    "stop_oid"      : _ls.get("oid"),
                    "stop_orig"     : _ls.get("orig"),
                    "stop_moves"    : _ls.get("moves") or 0,
                    "stop_last_move": _ls.get("last_move"),
                    "qty_internal"  : _qi_abs,
                    "qty_match"     : _qmatch,
                    "be_activated"  : _be,
                    "locked_pnl"    : _locked,
                })
            except Exception:
                pass

        # ── Orders (LOGS ONLY — reconstructed from the engine log) ─────────
        # 2026-06-23 (user directive): the orders/fills panel is built ENTIRELY
        # from today's engine log (logs/tws_DD_MM_YY.txt), never from the laggy
        # reqAllOpenOrders poll or the reader-thread-backlogged in-memory dicts.
        # The _log_loop daemon tails the log (~1.5s); here we just classify.
        with _cache_lock:
            log_orders = dict(_cache.get("log_orders") or {})
            be_events  = dict(_cache.get("be_events") or {})
            _cutoff    = _cache.get("cleared_before")
        # 2026-08-05: hide, never delete. A cleared session simply stops showing
        # rows older than the cut-off; the parsed history stays intact so the
        # clear is reversible and stop/P&L derivation keeps working.
        if _cutoff:
            log_orders = {oid: r for oid, r in log_orders.items()
                          if _visible(r.get("time"), _cutoff)}
        result["cleared_before"] = _cutoff

        _SIDE_MAP    = {"BOT": "BUY", "SLD": "SELL"}
        _WORKING     = {"presubmitted", "submitted", "pendingsubmit"}
        _TERMINAL_CX = {"cancelled", "apicancelled", "inactive", "apipending",
                        "pendingcancel", "rejected"}

        # 2026-07-31: a stop is placed as a CHILD of its entry (parentId), so map
        # parent oid -> its protective stop. That lets an ENTRY row display the
        # stop that is guarding it, and show when that stop has moved.
        # 2026-08-04: link by parentId (phase-1 wide stop) AND by the parent oid
        # embedded in a phase-2 STPMKT ref. A LIVE stop always wins over a dead
        # one, so an entry shows the stop that is actually protecting it rather
        # than the cancelled wide stop it was originally bracketed with.
        _DEAD_ST = {"cancelled", "apicancelled", "inactive", "filled", "rejected",
                    "apipending", "pendingcancel"}

        def _is_live(rec: Dict) -> bool:
            return str(rec.get("status") or "").lower() not in _DEAD_ST

        stop_of_parent: Dict[int, Dict] = {}
        for r in log_orders.values():
            if "STP" not in str(r.get("order_type") or "").upper():
                continue
            p = r.get("parent_oid")
            if p is None:
                p = _parent_oid_from_ref(r.get("ref"))
            if p is None:
                continue
            prev = stop_of_parent.get(p)
            if prev is None:
                stop_of_parent[p] = r
                continue
            # live beats dead; among equals the newer oid (the phase-2 STPMKT
            # that superseded the wide stop) wins
            if _is_live(r) and not _is_live(prev):
                stop_of_parent[p] = r
            elif _is_live(r) == _is_live(prev) and \
                    int(r.get("oid", 0)) >= int(prev.get("oid", 0)):
                stop_of_parent[p] = r

        for oid, r in sorted(log_orders.items(),
                             key=lambda kv: kv[1].get("time", "")):
            try:
                status = str(r.get("status") or "Unknown")
                st_lc  = status.lower()
                otype  = r.get("order_type") or ""
                role   = r.get("role") or _role_for(r.get("ref", ""))
                is_stop = "STP" in otype.upper()

                _p = float(r.get("price") or r.get("lmt") or 0)
                if otype == "MKT":
                    price_disp = "MKT"
                elif otype in ("STP LMT", "STP_LMT"):
                    price_disp = f"{_p:.2f} / {float(r.get('aux') or 0):.2f}"
                elif _p:
                    price_disp = f"{_p:.2f}"
                else:
                    price_disp = "-"

                fill_qty = float(r.get("fill_qty") or 0.0)
                fill_avg = (r.get("fill_notional", 0.0) / fill_qty) if fill_qty \
                           else float(r.get("avg_fill") or 0.0)
                side     = r.get("side") or _SIDE_MAP.get(r.get("fill_side", ""),
                                                          r.get("fill_side", ""))
                filled_n = float(r.get("filled") or fill_qty or 0.0)
                remaining = r.get("remaining")
                qty_n    = int(r.get("qty") or round(fill_qty) or 0)

                # 2026-08-04: IBKR REDUCES a bracket child the moment the parent
                # partially fills — a stop placed for 500 against an entry that
                # filled 80 is cut to remaining=80 by IBKR itself. Displaying the
                # ORIGINAL 500 made it look as though an 80-share position was
                # protected by a 500-share stop. `remaining` is the live size.
                _rem = r.get("remaining")
                try:
                    live_qty = int(float(_rem)) if _rem not in (None, "") else qty_n
                except (TypeError, ValueError):
                    live_qty = qty_n
                if not is_stop:
                    live_qty = qty_n          # only stops get reduced this way

                # ── stop tracking: this order's own stop, or its child's ──────
                src = r if is_stop else stop_of_parent.get(oid, {})
                hist = src.get("stop_history") or []
                stop_now  = src.get("price")
                stop_orig = src.get("orig_price")
                if hist:
                    stop_now = hist[-1][1]
                moves = len(hist)
                stop_moved = bool(
                    moves and stop_orig is not None
                    and abs(float(stop_now) - float(stop_orig)) > 1e-9
                )
                # share count the linked stop actually covers, live
                _srem = src.get("remaining")
                try:
                    stop_qty = (int(float(_srem)) if _srem not in (None, "")
                                else int(float(src.get("qty") or 0)) or None)
                except (TypeError, ValueError):
                    stop_qty = None

                # ── slippage: fill vs the price we asked for (entries only) ───
                slippage = None
                if fill_avg and _p and otype == "LMT":
                    raw = fill_avg - _p
                    # a BUY filling below its limit is FAVOURABLE, and so is a
                    # SELL filling above — normalise so + always means better
                    slippage = round(-raw if str(side).upper() in ("BUY", "BOT")
                                     else raw, 4)

                # 2026-08-03: ORDERED and EXECUTED are separate facts and must
                # never be conflated. 41 of 91 orders on 07-31 never executed a
                # share yet displayed their ordered 500; three others executed
                # only 58 / 73 / 304 of 500 before being cancelled.
                #
                # 2026-08-04 — EXECUTED MUST ALWAYS BE RIGHT. Two independent
                # sources exist and each can fail in a different way:
                #   * sum of execDetails (TWS FILL) — can UNDER-report if a
                #     callback is lost in a receive backlog or a restart lands
                #     mid-fill (duplicates are already deduped by execId above)
                #   * orderStatus.filled — IBKR's cumulative figure, but it can
                #     briefly LAG the execution reports it is summarising
                # Taking the MAX can never understate the shares actually held,
                # which is the dangerous direction (an understated position
                # implies an undersized stop). Any disagreement is surfaced
                # rather than silently resolved.
                _status_filled = r.get("filled")
                executed_n = fill_qty
                qty_source = "fills"
                qty_disc = None
                if _status_filled is not None:
                    try:
                        _sf = float(_status_filled)
                        if abs(_sf - fill_qty) > 1e-6:
                            qty_disc = {"fills": fill_qty, "ibkr": _sf}
                        if _sf > executed_n:
                            executed_n = _sf
                            qty_source = "ibkr_orderstatus"
                    except (TypeError, ValueError):
                        pass
                entry = {
                    "order_id"  : f"TWS_{oid}",
                    "oid"       : oid,
                    "symbol"    : r.get("symbol") or "",
                    "role"      : role,
                    "order_type": otype,
                    "side"      : side,
                    "qty"       : qty_n,          # ORDERED
                    "ordered"   : qty_n,
                    "live_qty"  : live_qty,       # IBKR's CURRENT size for this order
                    "qty_reduced": bool(is_stop and live_qty != qty_n),
                    "stop_qty"  : stop_qty,       # shares the linked stop covers
                    "stop_oid"  : src.get("oid"),
                    "executed"  : executed_n,     # actually filled shares
                    "qty_source": qty_source,     # which source won
                    "qty_discrepancy": qty_disc,  # set only when sources disagree
                    "partial"   : bool(executed_n and executed_n < qty_n),
                    "unfilled"  : bool(not executed_n),
                    "fills"     : r.get("fills") or [],
                    "commission": (round(r["commission"], 2)
                                   if r.get("commission") is not None else None),
                    "ibkr_realized": (round(r["ibkr_realized"], 2)
                                      if r.get("ibkr_realized") is not None else None),
                    "price"     : price_disp,
                    "status"    : status,
                    "filled"    : filled_n,
                    "avg_fill"  : round(fill_avg, 4) if fill_avg else None,
                    "slippage"  : slippage,
                    "fill_prints": r.get("fill_prints"),
                    "exchange"  : r.get("exchange"),
                    "placed_time": r.get("placed_time"),
                    "fill_time" : r.get("last_fill_time"),
                    "ref"       : r.get("ref"),
                    "parent_oid": r.get("parent_oid"),
                    "time"      : r.get("time"),
                    # stop / SL detail
                    "stop_price": round(float(stop_now), 2) if stop_now else None,
                    "stop_orig" : round(float(stop_orig), 2) if stop_orig else None,
                    "stop_moved": stop_moved,
                    "stop_moves": moves,
                    "stop_last_move": src.get("last_move_time"),
                    "notional"  : round(fill_avg * fill_qty, 2) if fill_qty else None,
                    "error_code": r.get("error_code"),
                    "error_msg" : r.get("error_msg"),
                }

                result["orders_all"].append(entry)

                is_filled = (
                    st_lc == "filled"
                    or (remaining is not None and float(remaining) == 0 and filled_n > 0)
                    or (fill_qty > 0 and st_lc not in _WORKING and st_lc not in _TERMINAL_CX)
                )
                if is_filled:
                    result["orders_filled"].append(entry)
                elif st_lc in _TERMINAL_CX or r.get("cancel_requested"):
                    result["orders_cancelled"].append(entry)
                elif st_lc in _WORKING:
                    result["orders_working"].append(entry)
                else:
                    # 2026-07-31: indeterminate (stuck PendingSubmit / Unknown).
                    # Age it out so a dead row cannot masquerade as a live order.
                    result["orders_stale"].append(entry)

            except Exception:
                pass

        # ── Stop-movement feed: every SL move today, newest first ─────────────
        # Answers "has the stop moved?" directly rather than leaving the user to
        # compare a resting price against a number they no longer have.
        for r in log_orders.values():
            hist = r.get("stop_history") or []
            if len(hist) < 1:
                continue
            sym = r.get("symbol") or ""
            base = r.get("orig_price")
            prev = base
            step = 0
            for when, px in hist:
                if prev is not None and abs(px - float(prev)) > 1e-9:
                    step += 1
                    bev = be_events.get(sym) or {}
                    # The FIRST move off the original stop IS the breakeven
                    # activation (exit_logic moves the stop to the BE anchor,
                    # then every later move is a 5-bar trail step). Matching on
                    # the BE timestamp confirms it rather than assuming.
                    is_be = (step == 1) or bool(
                        bev.get("time") and bev["time"] == when)
                    result["stop_moves"].append({
                        "symbol": sym,
                        "oid"   : r.get("oid"),
                        "time"  : when,
                        "from"  : round(float(prev), 2),
                        "to"    : round(px, 2),
                        "delta" : round(px - float(prev), 2),
                        "step"  : step,
                        "kind"  : "BREAKEVEN" if is_be else "TRAIL",
                        "breakeven": is_be,
                    })
                prev = px
        result["stop_moves"].sort(key=lambda m: m.get("time") or "", reverse=True)
        result["breakeven_events"] = [
            {"symbol": s, **v} for s, v in be_events.items()
        ]

        # ── Completed round trips + realized P&L ──────────────────────────────
        trips = _build_round_trips(log_orders)
        result["round_trips"]  = trips
        result["realized_pnl"] = round(sum(t["pnl"] for t in trips), 2)
        # 2026-08-03: IBKR's own realized P&L, summed straight from the
        # COMMISSION reports. This is the figure that must agree with TWS —
        # the reconstructed one above is gross of commissions.
        ibkr_vals = [r.get("ibkr_realized") for r in log_orders.values()
                     if r.get("ibkr_realized") is not None]
        result["ibkr_realized_pnl"] = round(sum(ibkr_vals), 2) if ibkr_vals else None
        result["commissions"] = round(
            sum(r.get("commission") or 0.0 for r in log_orders.values()), 2)
        # totals the user can check against the broker at a glance
        result["shares_executed"] = int(sum(
            o.get("executed") or 0 for o in result["orders_all"]))
        # 2026-08-04: quantity-integrity summary. Non-zero means the two
        # independent share-count sources disagreed on at least one order —
        # the panel shows the larger (never understating), but the user must
        # be able to see that it happened rather than trust a silent guess.
        _disc = [o for o in result["orders_all"] if o.get("qty_discrepancy")]
        result["qty_discrepancies"] = len(_disc)
        result["qty_discrepancy_orders"] = [
            {"order_id": o["order_id"], "symbol": o["symbol"],
             **o["qty_discrepancy"]} for o in _disc[:20]
        ]
        # position-vs-stop coverage: any open position whose protective stop
        # does not cover it exactly is a live risk, counted here for the banner
        result["stop_coverage_alerts"] = sum(
            1 for p in result["positions"] if p.get("stop_covers") is False)

    except Exception:
        pass

    return result


# ── FastAPI app ───────────────────────────────────────────────────────────────

def _build_app() -> "FastAPI":
    app = FastAPI(title="MACD Dashboard", docs_url=None, redoc_url=None)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    @app.get("/", response_class=HTMLResponse)
    def serve_ui():
        try:
            return HTMLResponse(content=_HTML_PATH.read_text(encoding="utf-8"),
                                status_code=200)
        except Exception as exc:
            return HTMLResponse(
                content=f"<pre>positions_ui.html not found: {exc}</pre>",
                status_code=200,
            )

    @app.get("/api/clear")
    def clear_panels(undo: int = 0):
        """Post-market clean — NON-DESTRUCTIVE since 2026-08-05.

        Sets a display cut-off instead of deleting parsed history, so nothing
        is ever lost and the action can be undone with /api/clear?undo=1.
        (The previous version wiped the cache and skipped the read offset to
        EOF, which blanked the orders / stop-movement / P&L panels for the rest
        of the session with no way back short of restarting the bot.)
        Touches the display cache only — never trading state.
        """
        now = datetime.now()
        with _cache_lock:
            total = len(_cache.get("log_orders") or {})
            if undo:
                _cache["cleared_before"] = None
                _cache["cleared_on"] = ""
                return JSONResponse(
                    content={"cleared": False, "undo": True,
                             "orders_visible_again": total},
                    headers={"Cache-Control": "no-store"})
            cutoff = now.strftime("%H:%M:%S")
            _cache["cleared_before"] = cutoff
            _cache["cleared_on"] = now.strftime("%Y-%m-%d")
            hidden = sum(1 for r in (_cache.get("log_orders") or {}).values()
                         if not _visible(r.get("time"), cutoff))
        return JSONResponse(
            content={"cleared": True, "cutoff": cutoff, "orders_hidden": hidden,
                     "reversible": "/api/clear?undo=1"},
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/api/data")
    def get_data():
        # 2026-05-26: no-store so a browser / proxy can never serve a stale
        # snapshot — the dashboard must always reflect the live pipeline values.
        _no_cache = {"Cache-Control": "no-store, no-cache, must-revalidate"}
        try:
            return JSONResponse(content=_build_response(), headers=_no_cache)
        except Exception:
            return JSONResponse(
                content={
                    "tws_connected": False,
                    "positions": [],
                    "orders_working": [],
                    "orders_filled": [],
                    "orders_all": [],
                    "macd_state": [],
                    "stats": {},
                    "rates": {},
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                headers=_no_cache,
            )

    return app


# ── Public entry point ────────────────────────────────────────────────────────

def start_dashboard(bot) -> None:
    """
    Start the positions dashboard as a daemon thread inside macd_bot.py.
    If fastapi/uvicorn are missing or anything fails, returns silently.
    daemon=True ensures the thread dies when the bot process dies.
    """
    import logging as _logging
    _log = _logging.getLogger("macd_bot")

    if not _DEPS_OK:
        _log.warning("Dashboard not started: fastapi/uvicorn not installed in venv")
        return

    global _bot, _app
    _bot = bot

    try:
        _app = _build_app()
    except Exception as exc:
        _log.warning("Dashboard app build failed: %s", exc)
        return

    # Background TWS data refresh (5s interval)
    cache_thread = threading.Thread(
        target=_cache_loop,
        daemon=True,
        name="dashboard-cache",
    )
    cache_thread.start()

    # Background engine-log tail for the orders panel (1.5s interval) — orders
    # are sourced from the log only, so this owns the orders data. 2026-06-23.
    log_thread = threading.Thread(
        target=_log_loop,
        daemon=True,
        name="dashboard-logorders",
    )
    log_thread.start()

    # Uvicorn server thread
    def _run_uvicorn():
        try:
            cfg = uvicorn.Config(
                _app,
                host="0.0.0.0",
                port=8080,
                log_level="error",
                access_log=False,
            )
            srv = uvicorn.Server(cfg)
            srv.run()
        except Exception as exc:
            _log.warning("Dashboard uvicorn error: %s", exc)

    srv_thread = threading.Thread(
        target=_run_uvicorn,
        daemon=True,
        name="dashboard-uvicorn",
    )
    srv_thread.start()
