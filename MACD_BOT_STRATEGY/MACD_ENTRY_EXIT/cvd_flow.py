"""
cvd_flow.py — REAL order-flow Cumulative Volume Delta.

This is genuine CVD: every trade is classified as buyer- or seller-initiated
(the aggressor) and its SIZE is summed signed. It is NOT the MACD-histogram
running sum that the dashboard used to label "CVD" (that is a *value* delta and
has nothing to do with volume).

    signed_volume = +size   when the trade prints at/above the ask  (buyer lifted)
                    -size   when the trade prints at/below the bid  (seller hit)
                    tick-rule when it prints inside the spread

Design (per user spec, 2026-07-28):
  * PRIMARY  — 5-minute CVD, 1:1 with the 5-minute price series, reset to zero
               at the session open (09:30 ET) so each day starts flat.
  * OVERLAY  — 1-minute CVD for earlier reads on order-flow shifts *inside* the
               forming 5-minute candle.
  * DIVERGENCE — price higher-high vs CVD lower-high (buyers being absorbed →
               mean-reversion short) and the bullish mirror.

Feed budget (measured on this gateway, 2026-07-28):
  reqTickByTickData is capped at **5 concurrent lines** — the 6th request comes
  back IBKR error 10190 "Max number of tick-by-tick requests has been reached".
  reqMktData (quotes) served all 8 symbols fine.

  So only CVD_MAX_SYMBOLS symbols can carry true CVD. Every snapshot reports
  which method produced it, and unserved symbols report method="none" rather
  than a number that looks real:

    "tick"  — reqTickByTickData(AllLast): every individual trade, exact size,
              classified against the live reqMktData quote. This is real CVD.
    "none"  — no tick line available (over the cap). No CVD is reported.
    "mkt"   — OFF BY DEFAULT (ENABLE_MKT_VOLUME_FALLBACK). reqMktData field-8
              cumulative-volume differencing. Left disabled because a live
              comparison against tick-by-tick on SPY did not cleanly resolve
              whether field 8 is shares or round lots (ratio came out 16.9 on
              thin pre-market flow, i.e. neither 1x nor 100x). Shipping it
              would put a possibly 100x-wrong magnitude beside correct ones.
              Re-run that comparison on liquid regular-hours flow before
              enabling; until then unserved symbols honestly show nothing.

Thread-safety: all mutation happens under `self.lock`.
"""

from __future__ import annotations

import threading
from collections import defaultdict, OrderedDict
from datetime import datetime, timedelta, timezone

# America/New_York. The VM is set to ET and handles DST itself; we only need a
# fixed offset for bucket maths, and the caller passes ET-aware datetimes.
ET = timezone(timedelta(hours=-4))

SESSION_OPEN = (9, 30)      # CVD resets to zero here
SESSION_END = (16, 0)

MAX_TICK_LINES = 5          # measured: IBKR 10190 beyond this
ENABLE_MKT_VOLUME_FALLBACK = False   # see module docstring — scaling unverified


def _floor_bucket(dt: datetime, minutes: int) -> datetime:
    """Floor a timestamp onto the N-minute grid (bar OPEN, IBKR convention)."""
    return dt.replace(
        minute=(dt.minute // minutes) * minutes, second=0, microsecond=0
    )


def _session_date(dt: datetime):
    """The trading day a timestamp belongs to (pre-open ticks belong to that day)."""
    return dt.date()


class SymbolFlow:
    """Per-symbol order-flow accumulator."""

    __slots__ = (
        "symbol", "method", "bid", "ask", "last_price", "last_cum_vol",
        "b1", "b5", "px1", "px5", "session", "trades", "unclassified",
    )

    def __init__(self, symbol: str, method: str = "mkt"):
        self.symbol = symbol
        self.method = method            # "tick" | "mkt"
        self.bid = None
        self.ask = None
        self.last_price = None
        self.last_cum_vol = None        # for reqMktData volume differencing
        self.b1 = OrderedDict()         # bucket_open -> signed volume (1 min)
        self.b5 = OrderedDict()         # bucket_open -> signed volume (5 min)
        self.px1 = OrderedDict()        # bucket_open -> [open, high, low, close]
        self.px5 = OrderedDict()
        self.session = None             # date currently accumulating
        self.trades = 0
        self.unclassified = 0

    # ── session handling ────────────────────────────────────────────────
    def _roll_session(self, ts: datetime):
        """Reset everything at the session open so CVD starts each day at 0."""
        day = _session_date(ts)
        if self.session == day:
            return
        self.session = day
        self.b1.clear(); self.b5.clear()
        self.px1.clear(); self.px5.clear()
        self.trades = 0
        self.unclassified = 0

    def _in_session(self, ts: datetime) -> bool:
        t = ts.time()
        return (t.hour, t.minute) >= SESSION_OPEN and (t.hour, t.minute) < SESSION_END

    # ── classification ──────────────────────────────────────────────────
    def classify(self, price: float) -> int:
        """+1 buyer-initiated, -1 seller-initiated, 0 undecidable.

        Quote rule first (Lee-Ready): at/above ask = buy, at/below bid = sell.
        Inside the spread falls back to the tick rule (compare to previous
        trade price); a flat tick with no prior direction is left unclassified
        rather than guessed.
        """
        if self.ask is not None and price >= self.ask:
            return 1
        if self.bid is not None and price <= self.bid:
            return -1
        if self.last_price is not None:
            if price > self.last_price:
                return 1
            if price < self.last_price:
                return -1
        return 0

    # ── ingestion ───────────────────────────────────────────────────────
    def on_quote(self, bid, ask):
        if bid is not None and bid > 0:
            self.bid = bid
        if ask is not None and ask > 0:
            self.ask = ask

    def on_trade(self, ts: datetime, price: float, size: float):
        """Record one trade (or one volume increment) as signed flow."""
        if size is None or size <= 0 or price is None or price <= 0:
            return
        self._roll_session(ts)
        if not self._in_session(ts):
            self.last_price = price
            return

        side = self.classify(price)
        self.last_price = price
        self.trades += 1
        if side == 0:
            self.unclassified += 1
            signed = 0.0
        else:
            signed = side * float(size)

        for minutes, bucket, pxmap in ((1, self.b1, self.px1), (5, self.b5, self.px5)):
            key = _floor_bucket(ts, minutes)
            bucket[key] = bucket.get(key, 0.0) + signed
            o = pxmap.get(key)
            if o is None:
                pxmap[key] = [price, price, price, price]
            else:
                o[1] = max(o[1], price)
                o[2] = min(o[2], price)
                o[3] = price
            # keep memory bounded — one session is ~78 five-min / 390 one-min bars
            while len(bucket) > 500:
                bucket.popitem(last=False)
            while len(pxmap) > 500:
                pxmap.popitem(last=False)

    def on_cum_volume(self, ts: datetime, price: float, cum_vol: float):
        """reqMktData path: difference the exchange's running day volume."""
        if cum_vol is None or cum_vol <= 0:
            return
        prev = self.last_cum_vol
        self.last_cum_vol = cum_vol
        if prev is None or cum_vol < prev:      # first sample, or day rollover
            return
        delta = cum_vol - prev
        if delta > 0:
            self.on_trade(ts, price, delta)

    # ── derived series ──────────────────────────────────────────────────
    def cvd_series(self, minutes: int):
        """[(bucket_open, cumulative_cvd, close_price)] for the session."""
        bucket = self.b1 if minutes == 1 else self.b5
        pxmap = self.px1 if minutes == 1 else self.px5
        out, run = [], 0.0
        for k in sorted(bucket):
            run += bucket[k]
            px = pxmap.get(k)
            out.append((k, run, px[3] if px else None))
        return out


def _swings(series, lookback: int = 12):
    """Last two swing highs and lows of [(ts, cvd, price)] using a 1-bar pivot.

    Returns (price_highs, cvd_highs, price_lows, cvd_lows) as recent-last lists.
    A 1-bar pivot is deliberately loose: on 5-minute intraday data a stricter
    pivot rarely triggers inside one session.
    """
    tail = series[-lookback:] if len(series) > lookback else series
    ph, ch, pl, cl = [], [], [], []
    for i in range(1, len(tail) - 1):
        _, c_prev, p_prev = tail[i - 1]
        _, c_mid, p_mid = tail[i]
        _, c_next, p_next = tail[i + 1]
        if None in (p_prev, p_mid, p_next):
            continue
        if p_mid >= p_prev and p_mid >= p_next:
            ph.append(p_mid); ch.append(c_mid)
        if p_mid <= p_prev and p_mid <= p_next:
            pl.append(p_mid); cl.append(c_mid)
    return ph, ch, pl, cl


def detect_divergence(series):
    """Classic CVD divergence on the 5-minute series.

    bearish — price higher-high while CVD makes a lower-high: aggressive buyers
              are being absorbed by resting sell limits (mean-reversion short).
    bullish — price lower-low while CVD makes a higher-low (mirror).
    """
    if len(series) < 5:
        return None, ""
    ph, ch, pl, cl = _swings(series)
    if len(ph) >= 2 and ph[-1] > ph[-2] and ch[-1] < ch[-2]:
        return "bearish", (
            f"price HH {ph[-2]:.2f}→{ph[-1]:.2f} but CVD LH "
            f"{ch[-2]:+,.0f}→{ch[-1]:+,.0f} (buyers absorbed)"
        )
    if len(pl) >= 2 and pl[-1] < pl[-2] and cl[-1] > cl[-2]:
        return "bullish", (
            f"price LL {pl[-2]:.2f}→{pl[-1]:.2f} but CVD HL "
            f"{cl[-2]:+,.0f}→{cl[-1]:+,.0f} (sellers absorbed)"
        )
    return None, ""


class CVDEngine:
    """Owns per-symbol flow state. The IBKR client calls the on_* hooks."""

    def __init__(self, symbols):
        self.lock = threading.Lock()
        self.flows = {s: SymbolFlow(s) for s in symbols}
        self.tick_symbols = []      # served by reqTickByTickData (primary gw)
        self.secondary_symbols = [] # served by reqTickByTickData (second gw)
        self.mkt_symbols = []       # served by reqMktData volume differencing

    def plan_subscriptions(self, symbols):
        """Allocate the scarce tick-by-tick lines on the PRIMARY gateway.

        The first MAX_TICK_LINES symbols (config order — put the ones you care
        about first) get true tick-by-tick CVD there. The rest overflow: they
        are returned so the caller can serve them from a second gateway
        (see enable_secondary), and until something claims them they report
        "none" rather than a number that looks real.
        """
        syms = list(symbols)
        self.tick_symbols = syms[:MAX_TICK_LINES]
        overflow = syms[MAX_TICK_LINES:]
        self.mkt_symbols = overflow if ENABLE_MKT_VOLUME_FALLBACK else []
        with self.lock:
            for s in syms:
                f = self.flows.setdefault(s, SymbolFlow(s))
                if s in self.tick_symbols:
                    f.method = "tick"
                else:
                    f.method = "mkt" if ENABLE_MKT_VOLUME_FALLBACK else "none"
        return self.tick_symbols, overflow

    def enable_secondary(self, symbols):
        """Mark symbols as carrying true tick-by-tick from a SECOND gateway.

        Each IBKR login has its own independent tick-by-tick quota (measured:
        5 lines each), so a second account covers what the first cannot. These
        symbols get identical treatment to primary ones — same classification,
        same buckets — they just arrive over a different connection.
        """
        syms = list(symbols)
        self.secondary_symbols = syms
        with self.lock:
            for s in syms:
                f = self.flows.setdefault(s, SymbolFlow(s))
                f.method = "tick"
        return syms

    def demote_secondary(self):
        """Secondary gateway died — stop claiming real CVD for its symbols."""
        with self.lock:
            for s in getattr(self, "secondary_symbols", []):
                f = self.flows.get(s)
                if f is not None:
                    f.method = "none"

    # ── hooks ───────────────────────────────────────────────────────────
    def on_quote(self, sym, bid=None, ask=None):
        with self.lock:
            f = self.flows.get(sym)
            if f:
                f.on_quote(bid, ask)

    def on_tick_trade(self, sym, price, size, ts=None):
        """A real trade print. Valid for any symbol on a tick line, whichever
        gateway delivered it."""
        ts = ts or datetime.now(ET)
        with self.lock:
            f = self.flows.get(sym)
            if f and f.method == "tick":
                f.on_trade(ts, price, size)

    def on_mkt_volume(self, sym, price, cum_vol, ts=None):
        ts = ts or datetime.now(ET)
        with self.lock:
            f = self.flows.get(sym)
            if f and f.method == "mkt":
                f.on_cum_volume(ts, price, cum_vol)

    # ── output ──────────────────────────────────────────────────────────
    def snapshot(self, sym):
        """Dashboard payload for one symbol (None until flow has been seen)."""
        with self.lock:
            f = self.flows.get(sym)
            if f is None:
                return None
            if f.method == "none":
                return {
                    "method": "none", "trades": 0, "cvd_5m": None,
                    "bid": f.bid, "ask": f.ask, "last_px": f.last_price,
                    "cvd_5m_bar": None, "cvd_1m": None, "cvd_1m_bar": None,
                    "divergence": None,
                    "divergence_note": "no tick-by-tick line (IBKR cap 5)",
                    "unclassified_pct": None, "bars_5m": 0,
                }
            s5 = f.cvd_series(5)
            s1 = f.cvd_series(1)
            if not s5 and not s1:
                return {
                    "method": f.method, "trades": 0, "cvd_5m": None,
                    "bid": f.bid, "ask": f.ask, "last_px": f.last_price,
                    "cvd_5m_bar": None, "cvd_1m_bar": None, "cvd_1m": None,
                    "divergence": None, "divergence_note": "",
                    "unclassified_pct": None, "bars_5m": 0,
                }
            cum5 = s5[-1][1] if s5 else None
            cum1 = s1[-1][1] if s1 else None
            last5 = list(f.b5.values())[-1] if f.b5 else None
            last1 = list(f.b1.values())[-1] if f.b1 else None
            div, note = detect_divergence(s5)
            unclass = (100.0 * f.unclassified / f.trades) if f.trades else None
            return {
                "method": f.method,
                "bid": f.bid, "ask": f.ask, "last_px": f.last_price,
                "trades": f.trades,
                "cvd_5m": round(cum5) if cum5 is not None else None,
                "cvd_5m_bar": round(last5) if last5 is not None else None,
                "cvd_1m": round(cum1) if cum1 is not None else None,
                "cvd_1m_bar": round(last1) if last1 is not None else None,
                "divergence": div,
                "divergence_note": note,
                "unclassified_pct": round(unclass, 1) if unclass is not None else None,
                "bars_5m": len(s5),
            }
