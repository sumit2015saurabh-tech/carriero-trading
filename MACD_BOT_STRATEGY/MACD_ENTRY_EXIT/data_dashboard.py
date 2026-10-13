#!/usr/bin/env python3
"""
QTI DATA DASHBOARD  (standalone, READ-ONLY)
===========================================
Signal-analytics view, sourced from a SEPARATE, READ-ONLY account.

  * Value calculation  -> account U391957 via IB Gateway on port 4003
                          (READ_ONLY_API=yes; this feed can NEVER trade).
  * Orders panel        -> the PAPER trading engine logs, parsed into a proper
                          order table (placed / filled / cancelled), NOT raw lines.
  * Zero dependency on the paper/demo account for anything except the orders panel.
  * This dashboard is DISPLAY-ONLY. Manual order entry / unwinds / stop edits are
    done in TWS (this account cannot place orders by design).

Per symbol it shows, from completed bars:
  MACD fast / slow / Diff (= fast - slow, the "value delta")
  delta (bar-to-bar slope) and gamma (acceleration) for BOTH the fast and slow lines
  MACD slow-line MEAN (mean-reversion reference: "slow line vs its mean")
  Cumulative Value Delta (CVD, running sum of the value-delta) on its OWN timeframe,
    its rate-of-change (CVD delta) and acceleration (CVD gamma = the rapid
    buyer<->seller flip)
  Cumulative MEAN Value Delta (kept — the original headline; smooths more)

Serves:  http://<vm-ip>:8081/
"""

import os, re, json, threading, time
from collections import defaultdict

import cvd_flow                                   # real order-flow CVD (tick-by-tick)
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

# ── Configuration ──────────────────────────────────────────────────────────
DASH_PORT   = 8081
GW_HOST     = os.getenv("DATA_GW_HOST", "127.0.0.1")
GW_PORT     = int(os.getenv("DATA_GW_PORT", "4003"))     # U391957 gateway (read-only)
# Second gateway used ONLY to cover symbols that overflow the primary gateway's
# 5-line tick-by-tick cap. Each IBKR login carries its own independent quota.
# READ-ONLY: this connection subscribes to market data and never sends an order.
FLOW2_PORT  = int(os.getenv("FLOW2_GW_PORT", "4002"))    # trading gateway (paper)
FLOW2_CID   = int(os.getenv("FLOW2_CLIENT_ID", "141"))   # must not collide: bot=1, flatten=99
FLOW2_ON    = os.getenv("FLOW2_ENABLE", "1") not in ("0", "false", "False")
CLIENT_ID   = int(os.getenv("DATA_CLIENT_ID", "20"))
BASE        = "/home/azureuser/MACD_ENTRY_EXIT"
CONFIG_PATH = os.path.join(BASE, "config.json")
LOG_DIR     = os.path.join(BASE, "logs")
ET          = timezone(timedelta(hours=-4))              # America/New_York (EDT)

RES_MAP = {"30s": "30 secs", "1min": "1 min", "2min": "2 mins", "3min": "3 mins",
           "5min": "5 mins", "10min": "10 mins", "15min": "15 mins", "30min": "30 mins"}


# ── math helpers ────────────────────────────────────────────────────────────
def _ema(vals, period):
    if not vals:
        return []
    k = 2.0 / (period + 1)
    out = [vals[0]]
    for v in vals[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out

def macd_lines(closes, fast, slow, sig):
    ef, es = _ema(closes, fast), _ema(closes, slow)
    macd   = [a - b for a, b in zip(ef, es)]
    signal = _ema(macd, sig)
    diff   = [m - s for m, s in zip(macd, signal)]
    return macd, signal, diff

def _r(x, dp=5):
    return round(x, dp) if x is not None else None

def _delta_gamma(seq):
    """(delta, gamma) at the tail of a series.
    delta = seq[-1]-seq[-2] (slope / momentum);
    gamma = delta - previous delta (acceleration)."""
    if len(seq) < 2:
        return None, None
    d1 = seq[-1] - seq[-2]
    if len(seq) < 3:
        return d1, None
    d2 = seq[-2] - seq[-3]
    return d1, d1 - d2


def load_symbols():
    """Enabled symbols + per-symbol MACD params from config.json (which
    excel_sync.py keeps in lockstep with strategy_config.xlsx — Excel is master).
    cvd_resolution lets the CVD run on its OWN timeframe (defaults to the MACD one)."""
    cfg = json.load(open(CONFIG_PATH))
    stocks = cfg.get("Stocks", {})
    out = {}
    for sym, v in stocks.items():
        if str(v.get("enabled", "Y")).upper() not in ("Y", "YES", "TRUE", "1"):
            continue
        res     = str(v.get("macd_resolution", "5min"))
        cvd_res = str(v.get("cvd_resolution", res))          # CVD own timeframe (default = MACD)
        out[sym] = {
            "fast": int(v.get("macd_fast", 12)),
            "slow": int(v.get("macd_slow", 26)),
            "sig":  int(v.get("macd_signal_period", 9)),
            "res":       RES_MAP.get(res, "5 mins"),
            "res_label": res,
            "cvd_res":       RES_MAP.get(cvd_res, RES_MAP.get(res, "5 mins")),
            "cvd_res_label": cvd_res,
        }
    return out


# ── IBKR data feed (bars only — no market-data / quotes) ───────────────────
class DataFeed(EWrapper, EClient):
    def __init__(self, params):
        EClient.__init__(self, self)
        self.params    = params
        self.symbols   = list(params.keys())
        self.lock      = threading.Lock()
        self.state     = {s: {} for s in self.symbols}
        self.bars      = defaultdict(list)   # sym -> [[date, close], ...]  MACD timeframe
        self.cvd_bars  = defaultdict(list)   # sym -> [[date, close], ...]  CVD timeframe
        self.account   = None
        self.connected_ok = False
        self.farm_ok   = True
        self.last_err  = ""
        self._rid = 100
        self.hist2sym    = {}   # reqId -> (sym, kind)  kind in {"macd","cvd"}
        self.sym_hist    = {}   # sym -> MACD-stream reqId
        self.sym_cvdhist = {}   # sym -> CVD-stream reqId (only when cvd_res != macd res)
        # ── REAL order-flow CVD (signed VOLUME from classified trades) ──
        self.flow      = cvd_flow.CVDEngine(self.symbols)
        self.tick2sym  = {}     # reqId -> sym  (reqTickByTickData AllLast)
        self.mkt2sym   = {}     # reqId -> sym  (reqMktData quote/volume)
        self.flow_note = ""
        self.flow2      = None
        self._last_mkt_px = {}

    def nextValidId(self, oid):
        self.connected_ok = True
        self.reqManagedAccts()
        for s in self.symbols:
            self._subscribe(s)                      # initial seed (one-time burst)
        self._subscribe_flow()                      # real CVD: trades + quotes
        threading.Thread(target=self._refresh_loop, daemon=True).start()
        threading.Thread(target=self._config_reload_loop, daemon=True).start()

    def _config_reload_loop(self):
        """Track the Excel file live (via config.json). Every 30s re-read and
        add/drop symbols + refresh MACD/CVD params so the dashboard mirrors the
        sheet without a restart."""
        while True:
            time.sleep(30)
            try:
                newp = load_symbols()
            except Exception:
                continue
            added   = [s for s in newp if s not in self.params]
            removed = [s for s in self.params if s not in newp]
            with self.lock:
                for s in removed:
                    self.params.pop(s, None); self.state.pop(s, None)
                    self.bars.pop(s, None); self.cvd_bars.pop(s, None)
                    if s in self.symbols:
                        self.symbols.remove(s)
                for s in added:
                    self.params[s] = newp[s]; self.state[s] = {}
                    self.symbols.append(s)
                for s in newp:
                    if s in self.params:
                        self.params[s] = newp[s]
            for s in added:
                self._subscribe(s)

    def _refresh_loop(self):
        """keepUpToDate streams updates; only re-subscribe a symbol that has no
        data yet (e.g. after a data-farm reconnect)."""
        while True:
            time.sleep(30)
            try:
                for s in list(self.symbols):
                    if not self.bars.get(s):
                        self._subscribe(s)
            except Exception:
                pass

    def _contract(self, sym):
        c = Contract(); c.symbol = sym; c.secType = "STK"
        c.exchange = "SMART"; c.currency = "USD"
        return c

    def _sub_one(self, sym, kind, res):
        hmap = self.sym_hist if kind == "macd" else self.sym_cvdhist
        old = hmap.get(sym)
        if old is not None:
            try: self.cancelHistoricalData(old)
            except Exception: pass
            self.hist2sym.pop(old, None)
        if kind == "macd":
            self.bars[sym] = []
        else:
            self.cvd_bars[sym] = []
        hid = self._rid; self._rid += 1
        self.hist2sym[hid] = (sym, kind)
        hmap[sym] = hid
        try:
            # keepUpToDate=True: initial bars via historicalData, then the forming
            # bar streams live via historicalDataUpdate.
            self.reqHistoricalData(hid, self._contract(sym), "", "2 D",
                                   res, "TRADES", 1, 1, True, [])
        except Exception:
            pass

    def _subscribe(self, sym):
        p = self.params[sym]
        self._sub_one(sym, "macd", p["res"])
        if p.get("cvd_res") and p["cvd_res"] != p["res"]:
            self._sub_one(sym, "cvd", p["cvd_res"])         # CVD has its own timeframe
        else:                                               # share the MACD stream for CVD
            old = self.sym_cvdhist.pop(sym, None)
            if old is not None:
                try: self.cancelHistoricalData(old)
                except Exception: pass
                self.hist2sym.pop(old, None)
            self.cvd_bars[sym] = []

    def _subscribe_flow(self):
        """Order-flow lines for the real CVD.

        reqTickByTickData is capped at 5 concurrent lines on this gateway
        (measured: the 6th returns error 10190), so only the first N symbols
        in config order get true CVD. Every symbol still gets a reqMktData
        quote line, which is what classifies each trade as buy or sell.
        """
        tick_syms, overflow = self.flow.plan_subscriptions(self.symbols)
        try:
            self.reqMarketDataType(1)               # live
        except Exception:
            pass
        for sym in self.symbols:                    # quotes for everyone
            rid = self._rid; self._rid += 1
            self.mkt2sym[rid] = sym
            try:
                self.reqMktData(rid, self._contract(sym), "", False, False, [])
            except Exception:
                pass
            time.sleep(0.15)                        # stagger — bursts upset the gateway
        for sym in tick_syms:                       # trades for the lucky 5
            rid = self._rid; self._rid += 1
            self.tick2sym[rid] = sym
            try:
                self.reqTickByTickData(rid, self._contract(sym), "AllLast", 0, False)
            except Exception:
                pass
            time.sleep(0.25)
        # Overflow symbols come off a SECOND gateway, which has its own quota.
        self.flow2 = start_secondary_flow(self.flow, overflow)
        note = f"CVD: {', '.join(tick_syms)} via data gw"
        if overflow:
            note += (f"  ·  {', '.join(overflow)} via trading gw"
                     if self.flow2 is not None
                     else f"  ·  no line for {', '.join(overflow)} (IBKR cap {cvd_flow.MAX_TICK_LINES}/login)")
        self.flow_note = note

    # ── order-flow callbacks ────────────────────────────────────────────
    def tickByTickAllLast(self, reqId, tickType, tim, price, size,
                          tickAttribLast, exchange, specialConditions):
        sym = self.tick2sym.get(reqId)
        if sym:
            try:
                self.flow.on_tick_trade(sym, float(price), float(size))
            except Exception:
                pass

    def tickPrice(self, reqId, field, price, attrib):
        sym = self.mkt2sym.get(reqId)
        if not sym:
            return
        try:
            if field == 1:                          # BID
                self.flow.on_quote(sym, bid=float(price))
            elif field == 2:                        # ASK
                self.flow.on_quote(sym, ask=float(price))
            elif field == 4 and cvd_flow.ENABLE_MKT_VOLUME_FALLBACK:
                self._last_mkt_px[sym] = float(price)
        except Exception:
            pass

    def tickSize(self, reqId, field, size):
        if not cvd_flow.ENABLE_MKT_VOLUME_FALLBACK:
            return                                  # field-8 scaling unverified
        sym = self.mkt2sym.get(reqId)
        if sym and field == 8:                      # cumulative day volume
            px = getattr(self, "_last_mkt_px", {}).get(sym)
            if px:
                try:
                    self.flow.on_mkt_volume(sym, px, float(size))
                except Exception:
                    pass

    def managedAccounts(self, accountsList):
        self.account = accountsList

    def error(self, reqId, code, msg, *a):
        if code in (2103, 2105, 2157, 2110, 1100, 1102):     # data farm / connectivity broken
            self.farm_ok = False; self.last_err = f"{code}: {msg[:80]}"
        elif code in (2104, 2106, 2158, 2107, 2108):          # farm connected OK
            self.farm_ok = True
        elif code not in (2119, 2100, 2150, 200, 2176, 10089, 10197):
            self.last_err = f"{code}: {msg}"

    def _buf(self, kind, sym):
        return self.bars[sym] if kind == "macd" else self.cvd_bars[sym]

    def historicalData(self, reqId, bar):
        t = self.hist2sym.get(reqId)
        if t is not None:
            self._buf(t[1], t[0]).append([bar.date, float(bar.close)])

    def historicalDataEnd(self, reqId, start, end):
        t = self.hist2sym.get(reqId)
        if t is not None:
            self._recompute(t[0])

    def historicalDataUpdate(self, reqId, bar):
        t = self.hist2sym.get(reqId)
        if t is None:
            return
        sym, kind = t
        rows = self._buf(kind, sym)
        if rows and rows[-1][0] == bar.date:
            rows[-1][1] = float(bar.close)          # update the forming bar (real-time)
        else:
            rows.append([bar.date, float(bar.close)])   # a new bar just committed
        self._recompute(sym)

    def _recompute(self, sym):
        p = self.params[sym]
        closes = [c for _, c in self.bars[sym]]
        if len(closes) < 2:
            return
        macd, signal, diff = macd_lines(closes, p["fast"], p["slow"], p["sig"])

        # delta / gamma of the fast (macd) and slow (signal) lines
        f_d, f_g = _delta_gamma(macd)
        s_d, s_g = _delta_gamma(signal)
        slow_mean = sum(signal) / len(signal)
        slow_vs_mean = (signal[-1] / slow_mean) if slow_mean else None    # "slow line x its mean"

        # CVD on its OWN timeframe (fall back to the MACD bars if no separate stream)
        cvd_src = [c for _, c in self.cvd_bars.get(sym, [])]
        if len(cvd_src) >= 2:
            _, _, cdiff = macd_lines(cvd_src, p["fast"], p["slow"], p["sig"])
        else:
            cdiff = diff
        # cumulative value-delta = running sum; roc = its slope; gamma = acceleration (the flip)
        cvd_series, run = [], 0.0
        for x in cdiff:
            run += x; cvd_series.append(run)
        cvd_cum = cvd_series[-1] if cvd_series else None
        cvd_roc, cvd_gamma = _delta_gamma(cvd_series)

        with self.lock:
            st = self.state[sym]
            st["macd"]        = _r(macd[-1])
            st["signal"]      = _r(signal[-1])
            st["diff"]        = _r(diff[-1])                       # value delta (latest)
            st["fast_delta"]  = _r(f_d); st["fast_gamma"] = _r(f_g)
            st["slow_delta"]  = _r(s_d); st["slow_gamma"] = _r(s_g)
            st["slow_mean"]   = _r(slow_mean)
            st["slow_vs_mean"] = _r(slow_vs_mean, 2)              # e.g. 0.67 => slow is below its mean
            st["cvd"]         = _r(cvd_cum)                        # cumulative value delta
            st["cvd_roc"]     = _r(cvd_roc)                        # rate of change (delta)
            st["cvd_gamma"]   = _r(cvd_gamma)                      # acceleration = rapid flip
            st["cum_value_delta"] = _r(sum(diff) / len(diff))     # original CVD MEAN (kept)
            st["bars"]        = len(closes)
            st["res"]         = p["res_label"]
            st["cvd_res"]     = p.get("cvd_res_label", p["res_label"])
            st["last_bar"]    = str(self.bars[sym][-1][0])[-8:]   # newest bar time (HH:MM:SS)


class SecondaryFlowFeed(EWrapper, EClient):
    """Tick-by-tick lines for symbols that overflow the primary gateway's cap.

    IBKR allows ~5 concurrent tick-by-tick subscriptions PER LOGIN (error 10190
    beyond that), so a second account covers what the first cannot. This client
    connects to the trading gateway purely to read market data — it subscribes
    to trades and quotes and never places, modifies, or cancels an order.

    If it cannot connect, or the connection drops, the symbols it serves are
    demoted back to "none" so the dashboard never shows a stale CVD as live.
    """

    def __init__(self, flow, symbols):
        EClient.__init__(self, self)
        self.flow = flow
        self.symbols = list(symbols)
        self.tick2sym = {}
        self.mkt2sym = {}
        self.ready = False
        self.err = ""
        self._rid = 30000

    def _contract(self, sym):
        c = Contract(); c.symbol = sym; c.secType = "STK"
        c.exchange = "SMART"; c.currency = "USD"
        return c

    def nextValidId(self, oid):
        self.ready = True
        # Subscribe off the reader thread — the staggering below must not block
        # the socket reader, and bursts upset the gateway.
        threading.Thread(target=self._subscribe, daemon=True).start()

    def _subscribe(self):
        try:
            self.reqMarketDataType(1)                    # live
        except Exception:
            pass
        for sym in self.symbols:                         # quotes classify trades
            rid = self._rid; self._rid += 1
            self.mkt2sym[rid] = sym
            try:
                self.reqMktData(rid, self._contract(sym), "", False, False, [])
            except Exception:
                pass
            time.sleep(0.4)
        for sym in self.symbols:                         # the trade prints
            rid = self._rid; self._rid += 1
            self.tick2sym[rid] = sym
            try:
                self.reqTickByTickData(rid, self._contract(sym), "AllLast", 0, False)
            except Exception:
                pass
            time.sleep(0.5)
        self.flow.enable_secondary(self.symbols)

    def tickByTickAllLast(self, reqId, tickType, tim, price, size,
                          tickAttribLast, exchange, specialConditions):
        sym = self.tick2sym.get(reqId)
        if sym:
            try:
                self.flow.on_tick_trade(sym, float(price), float(size))
            except Exception:
                pass

    def tickPrice(self, reqId, field, price, attrib):
        sym = self.mkt2sym.get(reqId)
        if not sym:
            return
        try:
            if field == 1:
                self.flow.on_quote(sym, bid=float(price))
            elif field == 2:
                self.flow.on_quote(sym, ask=float(price))
        except Exception:
            pass

    def error(self, reqId, code, msg, *a):
        if code == 10190:                                # cap hit on this login too
            self.err = "10190 tick-by-tick cap reached on secondary gateway"
        elif code in (1100, 1101, 1102, 502, 504):       # connection lost
            self.err = f"{code}: {str(msg)[:60]}"
            self.flow.demote_secondary()


def start_secondary_flow(flow, symbols):
    """Best-effort. A failure here must never take the dashboard down."""
    if not (FLOW2_ON and symbols):
        return None
    feed = SecondaryFlowFeed(flow, symbols)
    try:
        feed.connect(GW_HOST, FLOW2_PORT, FLOW2_CID)
    except Exception as e:
        feed.err = f"connect failed: {e}"
        return feed
    threading.Thread(target=feed.run, daemon=True).start()
    return feed


# ── Orders panel: parse the PAPER engine logs into a structured table ──────
FILL_RE   = re.compile(r"TWS FILL:\s+oid=(\d+)\s+symbol=(\S+)\s+side=(\S+)\s+qty=(\d+)\s+price=([\d.]+)")
STATUS_RE = re.compile(r"TWS orderStatus:\s+oid=(\d+)\s+status=(\S+)\s+filled=(\d+)\s+remaining=(\d+)\s+avgPrice=([\d.]+)")
# "TWS placed: oid=61980  BUY XBI 500 STP @ 136.5900  ref=... parentId=61979 ..."
PLACED_RE = re.compile(r"TWS placed:\s+oid=(\d+)\s+(\w+)\s+(\S+)\s+(\d+)\s+(\w+)\s+@\s+([\d.]+)")
PARENT_RE = re.compile(r"parentId=(\d+)")
TS_RE     = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
SIDE_MAP  = {"BOT": "BUY", "SLD": "SELL"}

def paper_orders(limit=50):
    """Return TODAY's orders (one row per IBKR order id) as structured records:
    time, symbol, side, qty, type, price, stop, status — parsed from the paper
    engine logs (TWS placed / FILL / orderStatus). A stop (type=STP) re-logs with
    a new price each time it trails, so latest-wins keeps the CURRENT stop; a
    stop's parentId links it back to its entry so the entry row shows its stop.

    ONLY the current-day log is read, so a fresh day starts with an empty panel —
    yesterday's orders are never shown (each trading day = its own tws_<date>.txt)."""
    today_log = os.path.join(LOG_DIR, "tws_" + datetime.now(ET).strftime("%d_%m_%y") + ".txt")
    if not os.path.exists(today_log):
        return []
    logs = [today_log]
    orders = {}      # oid -> record
    for lg in logs:
        try:
            with open(lg, errors="ignore") as fh:
                for line in fh:
                    ts = TS_RE.match(line)
                    t  = ts.group(1)[11:] if ts else ""    # HH:MM:SS
                    m = PLACED_RE.search(line)
                    if m:
                        oid, side, sym, qty, otype, price = m.groups()
                        r = orders.setdefault(oid, {"oid": oid})
                        r.update(symbol=sym, side=SIDE_MAP.get(side, side), qty=int(qty),
                                 otype=otype, time=t)
                        if r.get("status") != "Filled":        # keep the real fill price if filled
                            r["price"] = float(price)
                        r.setdefault("status", "Placed")
                        pm = PARENT_RE.search(line)
                        if pm:
                            r["parent"] = pm.group(1)
                        continue
                    m = FILL_RE.search(line)
                    if m:
                        oid, sym, side, qty, price = m.groups()
                        r = orders.setdefault(oid, {"oid": oid})
                        r.update(symbol=sym, side=SIDE_MAP.get(side, side),
                                 qty=int(qty), price=float(price), status="Filled", time=t)
                        continue
                    m = STATUS_RE.search(line)
                    if m:
                        oid, status, filled, remaining, avg = m.groups()
                        r = orders.setdefault(oid, {"oid": oid})
                        r["status"] = status; r["time"] = t
                        if float(avg) > 0 and r.get("status") == "Filled":
                            r["price"] = float(avg)
                        if int(filled) > 0:
                            r["qty"] = r.get("qty") or int(filled)
        except Exception:
            pass
    # link each stop (STP) back onto its parent entry so the entry row shows its stop
    for r in orders.values():
        if r.get("otype") == "STP" and r.get("parent") in orders:
            orders[r["parent"]]["stop"] = r.get("price")
    recs = sorted(orders.values(), key=lambda r: r.get("time", ""))
    return recs[-limit:][::-1]     # most recent first


# ── Web layer ──────────────────────────────────────────────────────────────
FEED = None

def build_payload():
    with FEED.lock:
        rows = []
        for s in FEED.symbols:
            st = FEED.state[s]
            rows.append({
                "symbol": s,
                "macd": st.get("macd"), "signal": st.get("signal"), "diff": st.get("diff"),
                "fast_delta": st.get("fast_delta"), "fast_gamma": st.get("fast_gamma"),
                "slow_delta": st.get("slow_delta"), "slow_gamma": st.get("slow_gamma"),
                "slow_mean": st.get("slow_mean"), "slow_vs_mean": st.get("slow_vs_mean"),
                "cvd": st.get("cvd"), "cvd_roc": st.get("cvd_roc"), "cvd_gamma": st.get("cvd_gamma"),
                "cum_value_delta": st.get("cum_value_delta"),
                "res": st.get("res"), "cvd_res": st.get("cvd_res"),
                "bars": st.get("bars"), "last_bar": st.get("last_bar"),
            })
            fl = FEED.flow.snapshot(s) or {}
            rows[-1].update({
                "flow_method":   fl.get("method"),
                "rcvd_5m":       fl.get("cvd_5m"),       # session cumulative (5m grid)
                "rcvd_5m_bar":   fl.get("cvd_5m_bar"),   # current 5m bar only
                "rcvd_1m_bar":   fl.get("cvd_1m_bar"),   # current 1m bar (early read)
                "rcvd_trades":   fl.get("trades"),
                "rcvd_div":      fl.get("divergence"),
                "rcvd_div_note": fl.get("divergence_note"),
                "rcvd_unclass":  fl.get("unclassified_pct"),
                "rcvd_bid":      fl.get("bid"),
                "rcvd_ask":      fl.get("ask"),
                "rcvd_lastpx":   fl.get("last_px"),
            })
        meta = {
            "account": FEED.account, "connected": FEED.connected_ok,
            "farm_ok": FEED.farm_ok, "last_err": FEED.last_err,
            "updated": datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET"),
            "flow_note": FEED.flow_note,
            "flow2_err": (FEED.flow2.err if getattr(FEED, "flow2", None) else ""),
        }
    return {"meta": meta, "symbols": rows, "orders": paper_orders()}


PAGE = """<!doctype html><html><head><meta charset=utf-8>
<title>QTI Data Dashboard — U391957</title>
<style>
 body{font-family:-apple-system,Segoe UI,Arial,sans-serif;margin:0;background:#0f1620;color:#e6edf3}
 header{padding:14px 20px;background:#111c2b;border-bottom:2px solid #1f5fbf}
 h1{margin:0;font-size:18px;color:#fff}
 .sub{color:#9fb3c8;font-size:12px;margin-top:4px}
 .banner{margin:10px 20px;padding:8px 12px;border-radius:6px;font-size:13px}
 .ok{background:#0e3a24;border:1px solid #1f9d55;color:#8ff0b8}
 .warn{background:#3a2a0e;border:1px solid #b8860b;color:#ffd97a}
 .wrap{overflow-x:auto}
 table{border-collapse:collapse;width:calc(100% - 40px);margin:10px 20px;font-size:12.5px}
 th,td{border:1px solid #24303f;padding:5px 7px;text-align:right;white-space:nowrap}
 th{background:#16273c;color:#cfe0f5;position:sticky;top:0}
 td.sym,th.sym,td.l,th.l{text-align:left}
 td.sym{font-weight:600;color:#fff}
 .grpF{background:#132033}.grpS{background:#15202e}
 .cvd{background:#10233a;color:#7ec8ff;font-weight:700}
 .flip{background:#1a1330;color:#d7b3ff;font-weight:700;font-size:13px}
 .of{background:#07231c;color:#6ff0c0;font-weight:700}
 .ofh{background:#0b3329;color:#8ff5d0}
 .divb{background:#3a1220;color:#ff9db0;font-weight:700}
 .divl{background:#123a1e;color:#8ff0b8;font-weight:700}
 .mut{color:#6b7c91;font-style:italic}
 .f{display:block;font-weight:400;color:#8fa6c0;font-size:9px;margin-top:3px;line-height:1.25;
    font-family:ui-monospace,Menlo,Consolas,monospace;white-space:nowrap}
 tr:nth-child(even) td{filter:brightness(1.06)}
 .neg{color:#ff7b72}.pos{color:#7ee787}
 h2{margin:18px 20px 4px;font-size:14px;color:#cfe0f5}
 .note{color:#9fb3c8;font-size:11px;margin:4px 20px 20px}
 .buy{color:#7ee787;font-weight:600}.sell{color:#ff7b72;font-weight:600}
 .st-Filled{color:#7ee787}.st-Cancelled,.st-ApiCancelled,.st-Inactive{color:#ff7b72}
 .st-Submitted,.st-PreSubmitted,.st-PendingSubmit{color:#ffd97a}
</style></head><body>
<header><h1>QTI Data Dashboard — MACD δ/γ · order-flow CVD</h1>
 <div class=sub id=meta>connecting…</div></header>
<div id=banner class=banner>…</div>

<div class=wrap><table id=vtbl><thead><tr>
 <th class=sym>Symbol<span class=f>ticker</span></th>
 <th>MACD<br>fast<span class=f>EMA12(C) − EMA26(C)</span></th>
 <th>Signal<br>slow<span class=f>EMA9(MACD)</span></th>
 <th>Diff<span class=f>MACD − Signal</span></th>
 <th class=grpF>Fast δ<span class=f>MACDt − MACDt-1</span></th>
 <th class=grpF>Fast γ<span class=f>δt − δt-1  (of MACD)</span></th>
 <th class=grpS>Slow δ<span class=f>SIGt − SIGt-1</span></th>
 <th class=grpS>Slow γ<span class=f>δt − δt-1  (of SIG)</span></th>
 <th>Slow<br>mean<span class=f>mean(SIG, all bars)</span></th>
 <th>Slow<br>÷mean<span class=f>SIGt ÷ Slow mean</span></th>
 <th class=cvd>Value&nbsp;CVD<span class=f>Σ(MACD − Signal) — NOT volume</span></th>
 <th class=cvd>Value&nbsp;CVD δ<span class=f>CVDt − CVDt-1</span></th>
 <th class=flip>Value&nbsp;CVD γ<br>(flip)<span class=f>CVDt−2·CVDt-1+CVDt-2</span></th>
 <th>Value&nbsp;CVD<br>mean<span class=f>mean(MACD − Signal)</span></th>
 <th class=ofh>Flow<span class=f>tick = real CVD</span></th>
 <th class=of>CVD 5m<br>session<span class=f>Σ signed VOLUME since 09:30</span></th>
 <th class=of>CVD 5m<br>bar<span class=f>signed volume, this 5m bar</span></th>
 <th class=of>CVD 1m<br>bar<span class=f>early read inside the 5m</span></th>
 <th class=ofh>Divergence<span class=f>price vs CVD swing</span></th>
 <th class=ofh>Trades<span class=f>classified prints</span></th>
 <th>MACD<br>res<span class=f>bar size</span></th>
 <th>CVD<br>res<span class=f>bar size</span></th>
 <th>Bars<span class=f># closes used</span></th>
 <th>Last&nbsp;bar<span class=f>newest bar (ET)</span></th>
</tr></thead><tbody></tbody></table></div>
<div class=note>Formula under each header is the EXACT math the dashboard computes (per-symbol periods from Excel; default
 Fast=12, Slow=26, Signal=9). C = bar CLOSE; EMAn seeded at the first close of the 2-day RTH window, k=2/(n+1).
 t = latest completed bar. δ = bar-to-bar slope (momentum), γ = change in δ (acceleration). CVD = cumulative
 value-delta (running sum of MACD−Signal) on its own timeframe; <b>CVD γ</b> = acceleration of the buyer↔seller
 flip. Read-only, from completed bars for account U391957 (Excel-driven). No Bid/Ask requested.
 Full spec + how to verify against live prices: QTI_Data_Dashboard_Spec PDF (Desktop).</div>

<h2>Orders (paper account — placed / filled / cancelled · stops shown &amp; trailed) — display only</h2>
<table id=otbl><thead><tr>
 <th class=l>Time</th><th class=l>OID</th><th class=l>Symbol</th><th class=l>Side</th>
 <th>Qty</th><th class=l>Type</th><th>Price</th><th>Stop</th><th class=l>Status</th>
</tr></thead><tbody></tbody></table>
<div class=note id=onote></div>

<script>
function fmt(v,dp){return (v===null||v===undefined)?'–':Number(v).toFixed(dp);}
function cls(v){return (v===null||v===undefined)?'':(v<0?'neg':(v>0?'pos':''));}
// signed share volume: thousands separators, explicit sign, dash when absent
function vol(v){if(v===null||v===undefined)return '–';
 const n=Number(v);return (n>0?'+':'')+n.toLocaleString('en-US');}
async function tick(){
 try{
  const r=await fetch('api/data?t='+Date.now(),{cache:'no-store'});const d=await r.json();
  const m=d.meta;
  document.getElementById('meta').textContent=
    'Account '+(m.account||'?')+'  •  updated '+m.updated+(m.last_err?('  •  '+m.last_err):'')
    +(m.flow_note?('  •  '+m.flow_note):'')+(m.flow2_err?('  •  ⚠ '+m.flow2_err):'');
  const b=document.getElementById('banner');
  if(m.farm_ok===false){b.className='banner warn';b.textContent='● IBKR data farm reconnecting — values repopulate automatically once the feed is back ('+(m.last_err||'')+').';}
  else{b.className='banner ok';b.textContent='● Connected — MACD δ/γ from bars; real CVD = signed trade VOLUME (5-min grid, resets 09:30 ET). "Value CVD" columns are Σ(MACD−Signal), not volume.';}
  const vt=document.querySelector('#vtbl tbody');vt.innerHTML='';
  for(const s of d.symbols){
   const tr=document.createElement('tr');
   tr.innerHTML='<td class=sym>'+s.symbol+'</td>'+
    '<td class="'+cls(s.macd)+'">'+fmt(s.macd,5)+'</td>'+
    '<td class="'+cls(s.signal)+'">'+fmt(s.signal,5)+'</td>'+
    '<td class="'+cls(s.diff)+'">'+fmt(s.diff,5)+'</td>'+
    '<td class="grpF '+cls(s.fast_delta)+'">'+fmt(s.fast_delta,5)+'</td>'+
    '<td class="grpF '+cls(s.fast_gamma)+'">'+fmt(s.fast_gamma,5)+'</td>'+
    '<td class="grpS '+cls(s.slow_delta)+'">'+fmt(s.slow_delta,5)+'</td>'+
    '<td class="grpS '+cls(s.slow_gamma)+'">'+fmt(s.slow_gamma,5)+'</td>'+
    '<td>'+fmt(s.slow_mean,5)+'</td>'+
    '<td class="'+cls((s.slow_vs_mean!=null)?s.slow_vs_mean-1:null)+'">'+fmt(s.slow_vs_mean,2)+'</td>'+
    '<td class="cvd '+cls(s.cvd)+'">'+fmt(s.cvd,4)+'</td>'+
    '<td class="cvd '+cls(s.cvd_roc)+'">'+fmt(s.cvd_roc,5)+'</td>'+
    '<td class="flip '+cls(s.cvd_gamma)+'">'+fmt(s.cvd_gamma,5)+'</td>'+
    '<td class="'+cls(s.cum_value_delta)+'">'+fmt(s.cum_value_delta,5)+'</td>'+
    '<td class="ofh'+(s.flow_method=='tick'?'':' mut')+'">'+(s.flow_method||'–')+'</td>'+
    '<td class="of '+cls(s.rcvd_5m)+'">'+vol(s.rcvd_5m)+'</td>'+
    '<td class="of '+cls(s.rcvd_5m_bar)+'">'+vol(s.rcvd_5m_bar)+'</td>'+
    '<td class="of '+cls(s.rcvd_1m_bar)+'">'+vol(s.rcvd_1m_bar)+'</td>'+
    '<td class="'+(s.rcvd_div=='bearish'?'divb':(s.rcvd_div=='bullish'?'divl':'mut'))+'" title="'+
       (s.rcvd_div_note||'')+'">'+(s.rcvd_div?s.rcvd_div.toUpperCase():'–')+'</td>'+
    '<td class=ofh>'+(s.rcvd_trades!=null&&s.rcvd_trades>0?s.rcvd_trades:'–')+'</td>'+
    '<td>'+(s.res||'–')+'</td><td>'+(s.cvd_res||'–')+'</td>'+
    '<td>'+(s.bars||'–')+'</td><td>'+(s.last_bar||'–')+'</td>';
   vt.appendChild(tr);
  }
  const ot=document.querySelector('#otbl tbody');ot.innerHTML='';
  const ords=d.orders||[];
  for(const o of ords){
   const sd=(o.side||'').toUpperCase();
   const tr=document.createElement('tr');
   tr.innerHTML='<td class=l>'+(o.time||'–')+'</td><td class=l>'+(o.oid||'–')+'</td>'+
    '<td class=l>'+(o.symbol||'–')+'</td>'+
    '<td class="l '+(sd==='BUY'?'buy':(sd==='SELL'?'sell':''))+'">'+(o.side||'–')+'</td>'+
    '<td>'+(o.qty!=null?o.qty:'–')+'</td>'+
    '<td class=l>'+(o.otype||'–')+'</td>'+
    '<td>'+(o.price!=null?Number(o.price).toFixed(2):'–')+'</td>'+
    '<td>'+(o.stop!=null?Number(o.stop).toFixed(2):(o.otype==='STP'?'—':'–'))+'</td>'+
    '<td class="l st-'+(o.status||'')+'">'+(o.status||'–')+'</td>';
   ot.appendChild(tr);
  }
  document.getElementById('onote').textContent = ords.length?('Showing '+ords.length+' most-recent orders from the paper engine (display only — trade in TWS).'):'(no orders in the paper engine logs yet)';
 }catch(e){document.getElementById('meta').textContent='fetch error: '+e;}
}
tick();setInterval(tick,3000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass
    def do_GET(self):
        if self.path.startswith("/api/data"):
            body = json.dumps(build_payload()).encode()
            ctype = "application/json"
        else:
            body = PAGE.encode(); ctype = "text/html; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        self.end_headers(); self.wfile.write(body)


def main():
    global FEED
    FEED = DataFeed(load_symbols())
    FEED.connect(GW_HOST, GW_PORT, CLIENT_ID)
    threading.Thread(target=FEED.run, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", DASH_PORT), Handler)
    print(f"QTI Data Dashboard on http://0.0.0.0:{DASH_PORT}  (gateway {GW_HOST}:{GW_PORT}, {len(FEED.symbols)} symbols)")
    srv.serve_forever()


if __name__ == "__main__":
    main()
