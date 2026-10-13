"""
shadow_broker.py — SHADOW TRADING ENGINE  (added 2026-08-06)
================================================================================
WHY THIS EXISTS
--------------------------------------------------------------------------------
The IBKR *paper* account was unusable as a strategy test:
  • limit entries sat unfilled for a whole session (2026-08-05: zero fills),
  • the simulated exchange died / went silent and the dashboard stopped updating,
  • paper fills are midpoint-ish fantasies, so they proved nothing anyway.

So orders are no longer sent to IBKR. They are "punched" into this SHADOW
broker instead: a local, honest fill simulator driven by the SAME live market
data we already stream from the IBKR account. Market data still comes from
IBKR (that part works well) — only the ORDER side is simulated.

DESIGN RULE
--------------------------------------------------------------------------------
The shadow broker is a drop-in replacement for the *broker side* of TWSClient
and emits the EXACT SAME log lines the real TWS callbacks emit:

    TWS placed: oid=… …
    TWS orderStatus: oid=… status=… filled=… remaining=… avgPrice=…
    TWS FILL: oid=… symbol=… side=… qty=… price=… execId=… time=… exchange=…
    TWS modify sent: oid=… STP  new_price=… new_aux=…
    TWS cancel sent: oid=…
    TWS COMMISSION: execId=… commission=… currency=USD realizedPNL=…

That matters: positions_server.py (the :8080 dashboard) reconstructs the whole
orders panel — including every stop re-price — by tailing those lines out of
logs/tws_DD_MM_YY.txt. Because the shadow speaks the same language, the existing
dashboard reads shadow orders, shadow fills and shadow stop-moves with no
change to its parser. Everything ABOVE the broker (brackets, fill verification,
breakeven, trailing, EoD square-off, reconciliation) also runs unmodified.

WHAT IS SIMULATED — AND HOW HONESTLY
--------------------------------------------------------------------------------
  MKT      → fills immediately: BUY at ask + slippage, SELL at bid − slippage.
             (No midpoint gifts. If bid/ask are missing, last ± half-spread.)
  LMT      → fills ONLY when the market actually trades at/through the price:
             BUY needs last ≤ limit (or ask ≤ limit), SELL needs last ≥ limit
             (or bid ≥ limit). Fill price = the limit (never better) — the
             pessimistic assumption for a retail order at the back of the queue.
  STP      → arms when last crosses the trigger, then fills as a market order
             at trigger ∓ slippage (adverse — stops slip against you).
  STP LMT  → arms on the trigger, then behaves as a LMT at the limit price.

  Bracket semantics mirror IBKR: a child with a parentId stays PreSubmitted
  and cannot trigger until its parent fills, and it is auto-resized to the
  parent's filled quantity. OCA groups cancel their siblings on first fill.

  NOT simulated: partial fills, queue position, locate/borrow for shorts,
  auction/halt behaviour. Every fill is all-or-nothing. This is documented on
  purpose — see the PDF spec.

FILES WRITTEN  (all under  <project>/shadow/ )
--------------------------------------------------------------------------------
  shadow_YYYY-MM-DD.jsonl   append-only event ledger (the source of truth)
  trades_YYYY-MM-DD.csv     one row per closed round-trip, for Excel
  state_today.json          hot snapshot the Shadow Trader UI polls

Nothing is ever overwritten, so opening the Shadow Trader on any past date
replays that day exactly as it happened.

SWITCHES (env)
--------------------------------------------------------------------------------
  SHADOW_MODE=1|0             master switch (default 1 = shadow, no real orders)
  SHADOW_SLIPPAGE=0.01        $/share adverse slippage on MKT + stop fills
  SHADOW_SPREAD=0.02          assumed full spread when bid/ask are unavailable
  SHADOW_COMM_PER_SHARE=0.005 commission model (IBKR tiered-ish)
  SHADOW_COMM_MIN=1.00        minimum commission per execution
  SHADOW_LMT_TOUCH=0          1 = a touch fills a limit; 0 = must trade through
  SHADOW_DIR=<path>           override the ledger directory
"""
from __future__ import annotations

import csv
import json
import logging
import os
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:                                       # VM timezone is already America/New_York,
    import pytz                            # but be explicit so timestamps never drift.
    EST = pytz.timezone("America/New_York")
except Exception:                          # pragma: no cover — pytz is installed on the VM
    EST = None

logger = logging.getLogger("SHADOW")       # root handler adds the [TWS_ENGINE] tag + file


# ── Configuration ─────────────────────────────────────────────────────────────

def _envf(name: str, default: float) -> float:
    """Read a float from the environment, falling back on anything unparseable."""
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return float(default)


def _envb(name: str, default: bool) -> bool:
    """Read a 0/1-style boolean from the environment."""
    raw = str(os.getenv(name, "1" if default else "0")).strip().lower()
    return raw in ("1", "true", "yes", "on")


SHADOW_ENABLED   = _envb("SHADOW_MODE", True)          # default ON — no live orders
SLIPPAGE         = _envf("SHADOW_SLIPPAGE", 0.01)      # $/share, adverse
ASSUMED_SPREAD   = _envf("SHADOW_SPREAD", 0.02)        # used only when no bid/ask
COMM_PER_SHARE   = _envf("SHADOW_COMM_PER_SHARE", 0.005)
COMM_MIN         = _envf("SHADOW_COMM_MIN", 1.00)
LMT_TOUCH_FILLS  = _envb("SHADOW_LMT_TOUCH", False)    # False = must trade THROUGH
TICK_SECONDS     = _envf("SHADOW_TICK_SEC", 0.25)      # fill-engine cadence
PRICE_STALE_SEC  = _envf("SHADOW_PRICE_STALE_SEC", 90) # refuse to fill on old prices

_BASE_DIR   = Path(os.getenv("SHADOW_DIR",
                             Path(__file__).resolve().parent / "shadow"))
_TERMINAL   = ("Filled", "Cancelled", "Inactive")


def _now() -> datetime:
    """Current wall-clock in market time (the VM is already America/New_York)."""
    return datetime.now(EST).replace(tzinfo=None) if EST else datetime.now()


def _today_str() -> str:
    return _now().strftime("%Y-%m-%d")


def _hhmmss() -> str:
    return _now().strftime("%H:%M:%S")


def _commission(qty: int, price: float) -> float:
    """IBKR-ish tiered commission: $0.005/share, $1 minimum, capped at 1% notional."""
    if qty <= 0:
        return 0.0
    raw = max(COMM_MIN, qty * COMM_PER_SHARE)
    return round(min(raw, 0.01 * qty * max(price, 0.01)), 4)


def _guess_modify_reason() -> str:
    """
    Work out WHY a stop was re-priced, without touching any caller.

    apply_breakeven() and trail_stop() both funnel into _modify_order_safe() →
    modify_order_price(). Reading the call stack tells us which one we are in,
    so the Shadow Trader history can say "trailed" vs "moved to breakeven"
    instead of an anonymous price change. Modifies happen a handful of times per
    trade, so walking 12 stack frames costs nothing.
    """
    try:
        for frame in traceback.extract_stack(limit=14):
            fn = frame.name
            if fn == "apply_breakeven":
                return "BREAKEVEN"
            if fn == "trail_stop":
                return "TRAIL"
            if fn == "handle_stop_hit":
                return "STOP_HIT"
            if fn == "eod_square_off":
                return "EOD"
    except Exception:
        pass
    return "MODIFY"


# ── Ledger ────────────────────────────────────────────────────────────────────

class ShadowLedger:
    """
    Append-only per-day event log + closed-trade CSV.

    Every state change the Shadow Trader UI can ever need is written here as one
    JSON line. Re-reading the file for a past date reconstructs that day
    completely — that is what "open the shadow trader for any day" means.
    """

    def __init__(self, base_dir: Path = _BASE_DIR):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._day  = _today_str()

    # -- paths -----------------------------------------------------------------
    def events_path(self, day: Optional[str] = None) -> Path:
        return self.base_dir / f"shadow_{day or _today_str()}.jsonl"

    def trades_path(self, day: Optional[str] = None) -> Path:
        return self.base_dir / f"trades_{day or _today_str()}.csv"

    def state_path(self) -> Path:
        return self.base_dir / "state_today.json"

    # -- writes ----------------------------------------------------------------
    def append(self, kind: str, **payload) -> Dict[str, Any]:
        """Write one event line. Never raises — a ledger problem must not stop trading."""
        rec = {"t": _now().strftime("%Y-%m-%d %H:%M:%S"),
               "ts": time.time(), "type": kind}
        rec.update(payload)
        try:
            with self._lock:
                with self.events_path().open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(rec, default=str) + "\n")
        except Exception as exc:                       # pragma: no cover
            logger.error("SHADOW ledger write failed (%s): %s", kind, exc)
        return rec

    def append_trade_csv(self, trade: Dict[str, Any]) -> None:
        """Mirror a closed round-trip into a spreadsheet-friendly CSV."""
        cols = ["date", "symbol", "direction", "qty", "entry_time", "entry_price",
                "exit_time", "exit_price", "exit_reason", "gross_pnl",
                "commission", "net_pnl", "pnl_pct", "stop_initial", "stop_final",
                "stop_moves", "hold_sec", "entry_oid", "exit_oid"]
        path = self.trades_path()
        try:
            new = not path.exists()
            with self._lock:
                with path.open("a", newline="", encoding="utf-8") as fh:
                    w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
                    if new:
                        w.writeheader()
                    w.writerow(trade)
        except Exception as exc:                       # pragma: no cover
            logger.error("SHADOW trade CSV write failed: %s", exc)

    def write_state(self, state: Dict[str, Any]) -> None:
        """Atomically refresh the hot snapshot the UI polls."""
        try:
            tmp = self.state_path().with_suffix(".tmp")
            with tmp.open("w", encoding="utf-8") as fh:
                json.dump(state, fh, default=str)
            tmp.replace(self.state_path())
        except Exception as exc:                       # pragma: no cover
            logger.debug("SHADOW state snapshot failed: %s", exc)

    # -- reads -----------------------------------------------------------------
    def read_events(self, day: str) -> List[Dict[str, Any]]:
        path = self.events_path(day)
        out: List[Dict[str, Any]] = []
        if not path.exists():
            return out
        try:
            with path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        out.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except Exception as exc:                       # pragma: no cover
            logger.error("SHADOW ledger read failed for %s: %s", day, exc)
        return out

    def available_days(self) -> List[str]:
        days = []
        for p in sorted(self.base_dir.glob("shadow_*.jsonl"), reverse=True):
            days.append(p.stem.replace("shadow_", ""))
        return days


# ── The broker ────────────────────────────────────────────────────────────────

class ShadowBroker:
    """
    A local order book + fill simulator that impersonates the IBKR order side.

    Thread-safety: one lock guards orders/positions/quotes. The fill engine runs
    on its own daemon thread; TWSClient calls in from the API thread; the UI
    reads through snapshot() which copies under the lock.
    """

    def __init__(self) -> None:
        self.enabled   = SHADOW_ENABLED
        self.ledger    = ShadowLedger()
        self._lock     = threading.RLock()
        self._client   = None                 # TWSClient — for status/callbacks

        self._orders:    Dict[int, Dict[str, Any]] = {}     # oid → order record
        self._positions: Dict[str, Dict[str, Any]] = {}     # sym → IBKR-shaped position
        self._lots:      Dict[str, Dict[str, Any]] = {}     # sym → open round-trip
        self._quotes:    Dict[str, Dict[str, float]] = {}   # sym → {bid,ask,last,ts}
        self._fills:     List[Dict[str, Any]] = []
        self._closed:    List[Dict[str, Any]] = []          # closed round-trips today
        self._sl_events: List[Dict[str, Any]] = []          # every stop re-price today
        self._exec_seq  = 0
        self._realized  = 0.0
        self._commission_total = 0.0
        self._day       = _today_str()

        self._engine_thread: Optional[threading.Thread] = None
        self._stop_engine = threading.Event()
        # 2026-08-06: the fill engine is EVENT-DRIVEN. Every quote sets _wake, so
        # a resting stop or limit is tested the moment its price prints rather
        # than up to TICK_SECONDS later. The timeout on the wait is only a
        # safety net (stale-quote checks, bar-driven fills, state publishing).
        self._wake = threading.Event()
        self._listeners: List[Any] = []          # push targets (the live UI)

    # ── wiring ────────────────────────────────────────────────────────────────

    def attach(self, client) -> None:
        """Bind the TWSClient so synthetic callbacks reach the engine + bot."""
        self._client = client
        self.start()

    def start(self) -> None:
        """Idempotently start the fill-engine thread."""
        if not self.enabled or (self._engine_thread and self._engine_thread.is_alive()):
            return
        self._stop_engine.clear()
        self._engine_thread = threading.Thread(
            target=self._engine_loop, daemon=True, name="shadow-fill-engine")
        self._engine_thread.start()
        logger.warning(
            "SHADOW MODE ACTIVE — orders are simulated locally, NOTHING is sent to "
            "IBKR. slippage=$%.3f/sh  assumed_spread=$%.3f  commission=$%.4f/sh "
            "(min $%.2f)  limit_fill=%s  ledger=%s",
            SLIPPAGE, ASSUMED_SPREAD, COMM_PER_SHARE, COMM_MIN,
            "on touch" if LMT_TOUCH_FILLS else "must trade through",
            self.ledger.events_path())
        self.ledger.append("SESSION_START", slippage=SLIPPAGE,
                           spread=ASSUMED_SPREAD, comm_per_share=COMM_PER_SHARE,
                           comm_min=COMM_MIN, limit_touch=LMT_TOUCH_FILLS)

    def stop(self) -> None:
        self._stop_engine.set()

    # ── market data in ────────────────────────────────────────────────────────

    def on_price(self, symbol: str, last: Optional[float] = None,
                 bid: Optional[float] = None, ask: Optional[float] = None) -> None:
        """
        Feed a live quote. Called from the bot's tick callbacks, which already
        receive real IBKR market data — the data side is untouched by shadow mode.
        """
        if not self.enabled or not symbol:
            return
        with self._lock:
            q = self._quotes.setdefault(symbol, {"bid": 0.0, "ask": 0.0,
                                                 "last": 0.0, "ts": 0.0})
            if last and last > 0:
                q["last"] = float(last)
            if bid and bid > 0:
                q["bid"] = float(bid)
            if ask and ask > 0:
                q["ask"] = float(ask)
            q["ts"] = time.time()
        # Wake the fill engine NOW — this is what makes a stop fire on the tick
        # that breaches it instead of on the next timer sweep.
        self._wake.set()

    def on_tick_type(self, symbol: str, tick_type: int, price: float) -> None:
        """Convenience shim: map raw IBKR tick types onto on_price()."""
        if price is None or price <= 0:
            return
        if tick_type in (1, 66):
            self.on_price(symbol, bid=price)
        elif tick_type in (2, 67):
            self.on_price(symbol, ask=price)
        elif tick_type in (4, 68):
            self.on_price(symbol, last=price)

    def on_bar(self, symbol: str, close: float, high: float = 0.0,
               low: float = 0.0) -> None:
        """
        Feed a completed bar. Used as a safety net: if tick flow for a symbol
        pauses, bar closes keep the shadow book priced. High/low let a stop or
        limit that was touched INSIDE the bar still fill, instead of being
        silently skipped because we only ever saw closes.
        """
        if not self.enabled or not close or close <= 0:
            return
        with self._lock:
            q = self._quotes.setdefault(symbol, {"bid": 0.0, "ask": 0.0,
                                                 "last": 0.0, "ts": 0.0})
            q["bar_high"] = float(high or close)
            q["bar_low"]  = float(low or close)
            q["last"]     = float(close)
            q["ts"]       = time.time()
        self._wake.set()

    def _quote(self, symbol: str) -> Tuple[float, float, float, bool]:
        """Return (bid, ask, last, fresh) with a synthetic spread when needed."""
        q = self._quotes.get(symbol) or {}
        last = float(q.get("last") or 0.0)
        bid  = float(q.get("bid") or 0.0)
        ask  = float(q.get("ask") or 0.0)
        ts   = float(q.get("ts") or 0.0)
        fresh = bool(ts) and (time.time() - ts) <= PRICE_STALE_SEC
        if last <= 0 and bid > 0 and ask > 0:
            last = round((bid + ask) / 2.0, 4)
        if last > 0:                                   # synthesise a book if needed
            if bid <= 0:
                bid = round(last - ASSUMED_SPREAD / 2.0, 4)
            if ask <= 0:
                ask = round(last + ASSUMED_SPREAD / 2.0, 4)
        return bid, ask, last, fresh

    # ── order entry (called by TWSClient) ─────────────────────────────────────

    def place(self, order_id: int, ticket, transmit: bool = True) -> Optional[int]:
        """
        Accept an order into the shadow book. Mirrors place_order_ticket()'s
        contract: returns the order id on success, None on rejection.
        """
        if not self.enabled:
            return None
        try:
            parent_oid = None
            if getattr(ticket, "parent_id", ""):
                parent_oid = self._resolve_parent(ticket.parent_id)

            rec = {
                "oid":        int(order_id),
                "symbol":     ticket.symbol,
                "conid":      int(getattr(ticket, "conid", 0) or 0),
                "side":       ticket.side,
                "qty":        int(ticket.quantity),
                "order_type": ticket.order_type,
                "price":      float(ticket.price or 0.0),
                "aux_price":  float(getattr(ticket, "aux_price", 0.0) or 0.0),
                "tif":        getattr(ticket, "tif", "DAY"),
                "ref":        getattr(ticket, "order_ref", ""),
                "parent_id":  parent_oid,
                "oca_group":  getattr(ticket, "oca_group", "") or "",
                "transmit":   bool(transmit),
                # A child waits for its parent; an untransmitted leg waits for the
                # transmitting leg of its bracket — exactly like IBKR.
                "status":     "PreSubmitted" if (parent_oid or not transmit) else "Submitted",
                "filled":     0.0,
                "remaining":  float(ticket.quantity),
                "avg_fill_price": 0.0,
                "armed":      False,          # stop orders: trigger already crossed
                "placed_at":  time.time(),
                "placed_hhmmss": _hhmmss(),
                "price_history": [],
                "reject":     "",
            }
            with self._lock:
                self._orders[int(order_id)] = rec
                # transmit=True releases every held leg of the same bracket
                if transmit:
                    self._release_bracket(int(order_id))

            # Same wording the real client logs — the :8080 dashboard parses this.
            logger.info("TWS placed: oid=%d  %s %s %d %s @ %.4f  ref=%s  parentId=%s  transmit=%s",
                        order_id, rec["side"], rec["symbol"], rec["qty"],
                        rec["order_type"], rec["price"], rec["ref"],
                        parent_oid if parent_oid else "none", transmit)
            self._emit_status(int(order_id), rec["status"], 0.0, float(rec["qty"]), 0.0)
            self.ledger.append("ORDER_PLACED", **{k: rec[k] for k in
                               ("oid", "symbol", "side", "qty", "order_type",
                                "price", "aux_price", "ref", "parent_id",
                                "oca_group", "status")})
            self._publish_state(force=True)      # a new order must reach the UI at once
            return int(order_id)
        except Exception as exc:
            logger.error("SHADOW place failed for %s: %s", getattr(ticket, "symbol", "?"), exc)
            return None

    def _resolve_parent(self, parent_ref: str) -> Optional[int]:
        """Map a parent order_ref string onto the shadow oid that carries it."""
        with self._lock:
            for oid, rec in self._orders.items():
                if rec.get("ref") == parent_ref:
                    return oid
        # Fall back to the client's own ref→id map (the engine registers there too)
        try:
            if self._client:
                return self._client.resolve_parent_id(parent_ref)
        except Exception:
            pass
        return None

    def _release_bracket(self, transmit_oid: int) -> None:
        """
        A transmit=True leg tells IBKR to send everything staged before it.
        Here that means: any parentless order still held for transmission goes
        Submitted. Children stay PreSubmitted until their parent fills.
        Caller must hold the lock.
        """
        for oid, rec in self._orders.items():
            if rec["status"] != "PreSubmitted" or rec.get("parent_id"):
                continue
            if not rec.get("transmit") and oid <= transmit_oid:
                rec["status"] = "Submitted"
                self._emit_status_locked(oid, "Submitted", rec["filled"],
                                         rec["remaining"], rec["avg_fill_price"])

    # ── modify / cancel ───────────────────────────────────────────────────────

    def modify(self, order_id: int, ticket, new_price: float,
               new_aux_price: float = 0.0) -> bool:
        """
        Re-price a resting order. This is how the stop-loss moves (breakeven and
        the 5-bar trail), so every change is recorded with old → new, the reason
        inferred from the caller, and the price at the moment it moved.
        """
        if not self.enabled:
            return False
        with self._lock:
            rec = self._orders.get(int(order_id))
            if not rec:
                logger.warning("SHADOW modify: unknown oid=%d — ignored", order_id)
                return False
            if rec["status"] in _TERMINAL:
                logger.warning("SHADOW modify: oid=%d already %s — ignored",
                               order_id, rec["status"])
                return False
            old_price = rec["price"]
            old_aux   = rec["aux_price"]
            rec["price"] = float(new_price)
            if rec["order_type"] == "STP LMT":
                rec["aux_price"] = float(new_aux_price)
            # A re-priced stop must re-evaluate its trigger from scratch.
            rec["armed"] = False
            reason = _guess_modify_reason()
            _b, _a, last, _f = self._quote(rec["symbol"])
            entry = (self._lots.get(rec["symbol"]) or {}).get("entry_price", 0.0)
            hop = {
                "t": _hhmmss(), "ts": time.time(), "oid": int(order_id),
                "symbol": rec["symbol"], "ref": rec["ref"],
                "order_type": rec["order_type"],
                "from": round(old_price, 4), "to": round(float(new_price), 4),
                "delta": round(float(new_price) - old_price, 4),
                "aux_from": round(old_aux, 4), "aux_to": round(float(new_aux_price), 4),
                "reason": reason, "price_at_move": round(last, 4),
                "entry_price": round(entry, 4),
                "locked_pnl": (round((float(new_price) - entry) *
                                     (1 if (self._lots.get(rec["symbol"]) or {})
                                      .get("direction") == "LONG" else -1) *
                                     (self._lots.get(rec["symbol"]) or {}).get("qty", 0), 2)
                               if entry else 0.0),
            }
            rec["price_history"].append(hop)
            self._sl_events.append(hop)
            # keep the open round-trip's stop view current
            lot = self._lots.get(rec["symbol"])
            if lot is not None and rec["order_type"] in ("STP", "STP LMT"):
                if lot.get("stop_initial") in (None, 0, 0.0):
                    lot["stop_initial"] = old_price or float(new_price)
                lot["stop_current"] = float(new_price)
                lot["stop_moves"] = int(lot.get("stop_moves", 0)) + 1
                lot.setdefault("stop_history", []).append(hop)

        # Identical wording to the real path — the :8080 dashboard's _RE_MODIFY
        # reads this to show that a stop has MOVED rather than its original price.
        logger.info("TWS modify sent: oid=%d  %s  new_price=%.4f  new_aux=%.4f",
                    order_id, rec["order_type"], float(new_price), float(new_aux_price))

        # A stop re-priced to the WRONG SIDE of the market is already breached and
        # will fire on the next evaluation — IBKR would reject it outright. That
        # is nearly always a trail bug, so say it plainly rather than let the
        # position quietly close a moment later for no visible reason.
        if rec["order_type"] in ("STP", "STP LMT") and last > 0:
            breached = ((rec["side"] == "SELL" and float(new_price) >= last) or
                        (rec["side"] == "BUY" and float(new_price) <= last))
            if breached:
                logger.warning("SHADOW STOP PLACED THROUGH THE MARKET: [%s] oid=%d "
                               "%s stop moved to %.4f while the market is %.4f — it "
                               "is already breached and will fill immediately.",
                               rec["symbol"], order_id, rec["side"],
                               float(new_price), last)
        logger.info("SHADOW SL UPDATE: [%s] oid=%d  %s  %.4f → %.4f  (%s)  price_now=%.4f",
                    rec["symbol"], order_id, rec["order_type"], hop["from"],
                    hop["to"], reason, hop["price_at_move"])
        self.ledger.append("SL_UPDATE", **hop)
        self._publish_state(force=True)      # a stop move must reach the UI at once
        return True

    def cancel(self, order_id: int, reason: str = "cancel") -> bool:
        """Cancel a resting shadow order."""
        if not self.enabled:
            return False
        with self._lock:
            rec = self._orders.get(int(order_id))
            if not rec:
                logger.info("TWS cancel sent: oid=%d", order_id)   # keep panel in sync
                return True
            if rec["status"] in _TERMINAL:
                logger.info("TWS cancel sent: oid=%d", order_id)
                return True
            rec["status"] = "Cancelled"
            rec["remaining"] = 0.0
            self._emit_status_locked(order_id, "Cancelled", rec["filled"],
                                     0.0, rec["avg_fill_price"])
        logger.info("TWS cancel sent: oid=%d", order_id)
        self.ledger.append("ORDER_CANCELLED", oid=int(order_id),
                           symbol=rec["symbol"], ref=rec["ref"], reason=reason)
        return True

    def global_cancel(self) -> None:
        """reqGlobalCancel equivalent — kill every working shadow order."""
        with self._lock:
            live = [oid for oid, r in self._orders.items()
                    if r["status"] not in _TERMINAL]
        for oid in live:
            self.cancel(oid, reason="global_cancel")
        logger.warning("SHADOW global cancel — %d working orders killed", len(live))

    # ── broker-state reads (called by TWSClient) ──────────────────────────────

    def positions_snapshot(self) -> Tuple[Dict[str, Dict], bool]:
        """IBKR-shaped positions. fresh=True — the shadow book is always known."""
        with self._lock:
            return {s: dict(p) for s, p in self._positions.items()}, True

    def live_orders(self) -> List[Dict[str, Any]]:
        """Working orders in the shape get_live_orders_tws() returns."""
        with self._lock:
            out = []
            for oid, r in self._orders.items():
                if r["status"] in _TERMINAL:
                    continue
                out.append({
                    "orderId": oid, "symbol": r["symbol"], "action": r["side"],
                    "orderType": r["order_type"], "totalQuantity": r["qty"],
                    "lmtPrice": r["price"] if r["order_type"] == "LMT" else 0.0,
                    "auxPrice": r["price"] if r["order_type"] in ("STP", "STP LMT") else 0.0,
                    "status": r["status"], "orderRef": r["ref"],
                })
            return out

    def get_symbol_qty(self, symbol: str) -> int:
        with self._lock:
            p = self._positions.get(symbol)
            if not p:
                return 0
            return int(p["qty"]) * (1 if p["side"] == "LONG" else -1)

    # ── the fill engine ───────────────────────────────────────────────────────

    def _engine_loop(self) -> None:
        """
        Price every working order — woken by each incoming quote.

        The wait() returns immediately when a tick arrives, so the latency from
        "price prints" to "stop fires" is sub-millisecond rather than a polling
        interval. The TICK_SECONDS timeout still fires when the market is quiet,
        which keeps stale-quote handling and the UI snapshot ticking over.
        """
        while not self._stop_engine.is_set():
            try:
                self._wake.wait(timeout=TICK_SECONDS)
                self._wake.clear()
                self._roll_day_if_needed()
                self._evaluate_all()
                self._publish_state()
            except Exception as exc:                    # pragma: no cover
                logger.error("SHADOW engine loop error: %s", exc)

    def _roll_day_if_needed(self) -> None:
        """New trading day → reset the in-memory day view (the ledger rolls by filename)."""
        today = _today_str()
        if today == self._day:
            return
        with self._lock:
            self._day = today
            self._fills.clear()
            self._closed.clear()
            self._sl_events.clear()
            self._realized = 0.0
            self._commission_total = 0.0
        logger.warning("SHADOW day rolled → %s", today)
        self.ledger.append("SESSION_START", rolled=True)

    def _evaluate_all(self) -> None:
        """One pass over the book. Fills are executed outside the scan to keep the lock short."""
        to_fill: List[Tuple[int, float, str]] = []
        with self._lock:
            for oid, rec in list(self._orders.items()):
                if rec["status"] not in ("Submitted", "PreSubmitted"):
                    continue
                # A child cannot work until its parent has filled.
                if rec.get("parent_id"):
                    parent = self._orders.get(rec["parent_id"])
                    if not parent or parent["status"] != "Filled":
                        continue
                    if rec["status"] == "PreSubmitted":
                        rec["status"] = "Submitted"
                        # IBKR resizes a bracket child to what the parent actually filled
                        pf = int(parent["filled"])
                        if pf and pf != rec["qty"]:
                            rec["qty"] = pf
                            rec["remaining"] = float(pf)
                        self._emit_status_locked(oid, "Submitted", 0.0,
                                                 rec["remaining"], 0.0)
                elif rec["status"] == "PreSubmitted":
                    continue                            # still held for transmission

                price, why = self._fill_price_for(rec)
                if price:
                    to_fill.append((oid, price, why))

        for oid, price, why in to_fill:
            self._execute(oid, price, why)

    def _fill_price_for(self, rec: Dict[str, Any]) -> Tuple[Optional[float], str]:
        """
        The honest fill model. Returns (fill_price, reason) or (None, "").
        Caller must hold the lock.
        """
        sym = rec["symbol"]
        bid, ask, last, fresh = self._quote(sym)
        if last <= 0 or not fresh:
            return None, ""                            # never invent a fill on stale data

        otype = rec["order_type"]
        buy   = rec["side"] == "BUY"
        px    = rec["price"]
        q     = self._quotes.get(sym) or {}
        hi    = float(q.get("bar_high") or last)
        lo    = float(q.get("bar_low") or last)

        if otype == "MKT":
            return (round(ask + SLIPPAGE, 4) if buy
                    else round(max(bid - SLIPPAGE, 0.01), 4)), "market"

        if otype == "LMT":
            if buy:
                touched = (last <= px) or (ask <= px) or (lo <= px)
                through = (last < px) or (ask <= px) or (lo < px)
            else:
                touched = (last >= px) or (bid >= px) or (hi >= px)
                through = (last > px) or (bid >= px) or (hi > px)
            if touched if LMT_TOUCH_FILLS else through:
                return round(px, 4), "limit"           # never better than the limit
            return None, ""

        if otype in ("STP", "STP LMT"):
            # BUY stop sits ABOVE the market (protects a short); SELL stop BELOW.
            if not rec["armed"]:
                hit = (last >= px or hi >= px) if buy else (last <= px or lo <= px)
                if not hit:
                    return None, ""
                rec["armed"] = True
                logger.info("SHADOW stop ARMED: oid=%d [%s] %s stop=%.4f  last=%.4f",
                            rec["oid"], sym, rec["side"], px, last)
            if otype == "STP":
                # Triggered stop becomes a market order — slippage works against us.
                base = max(px, last) if buy else min(px, last)
                return (round(base + SLIPPAGE, 4) if buy
                        else round(max(base - SLIPPAGE, 0.01), 4)), "stop"
            lmt = rec["aux_price"] or px
            if (buy and (last <= lmt or ask <= lmt)) or \
               (not buy and (last >= lmt or bid >= lmt)):
                return round(lmt, 4), "stop_limit"
            return None, ""

        return None, ""

    def _execute(self, oid: int, fill_price: float, why: str) -> None:
        """Book a full fill: position, P&L, OCA, callbacks, log lines, ledger."""
        with self._lock:
            rec = self._orders.get(oid)
            if not rec or rec["status"] in _TERMINAL:
                return
            qty = int(rec["remaining"] or rec["qty"])
            if qty <= 0:
                return

            # ── Over-cover guard ────────────────────────────────────────────
            # An EXIT order (protective stop, square-off, EoD close) must never
            # fill for more than the position it is closing. If two protective
            # orders survive together — a wide stop whose cancel failed plus the
            # phase-2 stop, say — a gap through both levels would otherwise close
            # 500 shares twice and leave the book SHORT 500. That is the
            # over-cover failure this bot has fought before, so the shadow refuses
            # it and says so loudly rather than quietly booking a phantom trade.
            if _is_exit_ref(rec["ref"]):
                pos = self._positions.get(rec["symbol"])
                cur = (int(pos["qty"]) * (1 if pos["side"] == "LONG" else -1)) if pos else 0
                avail = abs(cur) if ((cur > 0 and rec["side"] == "SELL") or
                                     (cur < 0 and rec["side"] == "BUY")) else 0
                if avail <= 0:
                    rec["status"] = "Cancelled"
                    rec["remaining"] = 0.0
                    self._emit_status_locked(oid, "Cancelled", 0.0, 0.0, 0.0)
                    logger.warning("SHADOW OVER-COVER BLOCKED: [%s] exit oid=%d "
                                   "(%s %d, ref=%s) triggered with NO open position "
                                   "— order cancelled instead of opening a new one. "
                                   "A stale protective order outlived its position.",
                                   rec["symbol"], oid, rec["side"], qty, rec["ref"])
                    self.ledger.append("OVER_COVER_BLOCKED", oid=oid,
                                       symbol=rec["symbol"], side=rec["side"],
                                       qty=qty, ref=rec["ref"], position=cur,
                                       action="cancelled")
                    return
                if qty > avail:
                    logger.warning("SHADOW OVER-COVER CAPPED: [%s] exit oid=%d wanted "
                                   "%d but only %d shares are open — filling %d.",
                                   rec["symbol"], oid, qty, avail, avail)
                    self.ledger.append("OVER_COVER_BLOCKED", oid=oid,
                                       symbol=rec["symbol"], side=rec["side"],
                                       qty=qty, ref=rec["ref"], position=cur,
                                       action="capped", filled=avail)
                    qty = avail

            rec["status"] = "Filled"
            rec["filled"] = float(qty)
            rec["remaining"] = 0.0
            rec["avg_fill_price"] = float(fill_price)
            rec["filled_at"] = time.time()
            rec["filled_hhmmss"] = _hhmmss()
            rec["fill_reason"] = why

            self._exec_seq += 1
            exec_id = f"SHDW.{oid}.{self._exec_seq:04d}"
            comm = _commission(qty, fill_price)
            realized = self._apply_to_position(rec, qty, fill_price, comm, exec_id)
            self._commission_total += comm

            fill = {"t": _hhmmss(), "ts": time.time(), "oid": oid,
                    "symbol": rec["symbol"], "side": rec["side"], "qty": qty,
                    "price": round(fill_price, 4), "exec_id": exec_id,
                    "order_type": rec["order_type"], "ref": rec["ref"],
                    "reason": why, "commission": comm,
                    "realized_pnl": round(realized, 2)}
            self._fills.append(fill)
            siblings = self._oca_siblings(oid, rec.get("oca_group"))

        # ── log in the exact TWS dialect the dashboard parses ────────────────
        logger.info("TWS orderStatus: oid=%d  status=Filled  filled=%.0f  remaining=0  avgPrice=%.4f",
                    oid, qty, fill_price)
        logger.info("TWS FILL: oid=%d  symbol=%s  side=%s  qty=%.0f  price=%.4f  "
                    "execId=%s  time=%s  exchange=%s",
                    oid, rec["symbol"], "BOT" if rec["side"] == "BUY" else "SLD",
                    qty, fill_price, exec_id,
                    _now().strftime("%Y%m%d %H:%M:%S"), "SHADOW")
        logger.info("TWS COMMISSION: execId=%s  commission=%.4f  currency=USD  realizedPNL=%.4f",
                    exec_id, comm, realized)
        logger.info("SHADOW FILL [%s] %s %d @ %.4f (%s, %s) — realized=%.2f  net_day=%.2f",
                    rec["symbol"], rec["side"], qty, fill_price, rec["order_type"],
                    why, realized, self.net_pnl())

        self.ledger.append("FILL", oid=oid, symbol=rec["symbol"], side=rec["side"],
                           qty=qty, price=round(fill_price, 4), exec_id=exec_id,
                           order_type=rec["order_type"], ref=rec["ref"],
                           reason=why, commission=comm,
                           realized_pnl=round(realized, 2))

        # Notify the engine + any per-order callback, exactly like orderStatus would.
        self._fire_callbacks(oid, "Filled", float(qty), 0.0, float(fill_price))

        # OCA: first fill kills the siblings (that is how a stop cancels a target).
        for sib in siblings:
            self.cancel(sib, reason=f"oca_sibling_of_{oid}")

        self._publish_state(force=True)      # a fill must reach the UI at once

    def _oca_siblings(self, oid: int, group: str) -> List[int]:
        """Working orders sharing an OCA group with oid. Caller holds the lock."""
        if not group:
            return []
        return [o for o, r in self._orders.items()
                if o != oid and r.get("oca_group") == group
                and r["status"] not in _TERMINAL]

    # ── positions + P&L ───────────────────────────────────────────────────────

    def _apply_to_position(self, rec: Dict[str, Any], qty: int, price: float,
                           comm: float, exec_id: str) -> float:
        """
        Fold a fill into the position book and return the realized P&L it produced.
        Opening/adding → weighted-average cost. Reducing/closing → realized P&L
        and, when flat, a completed round-trip written to the ledger + CSV.
        Caller must hold the lock.
        """
        sym    = rec["symbol"]
        signed = qty if rec["side"] == "BUY" else -qty
        pos    = self._positions.get(sym)
        cur    = (int(pos["qty"]) * (1 if pos["side"] == "LONG" else -1)) if pos else 0
        avg    = float(pos["avg"]) if pos else 0.0
        realized = 0.0
        new = cur + signed

        if cur == 0 or (cur > 0) == (signed > 0):
            # Opening or adding — weighted average cost. No price P&L is realized,
            # but the commission IS paid now, so book it as a negative realization.
            # That keeps the day's realized total identical to the sum of the
            # closed trades' net P&L (gross − entry comm − exit comm).
            realized = -comm
            total = abs(cur) + qty
            avg = ((abs(cur) * avg) + (qty * price)) / total if total else price
            if cur == 0:
                self._lots[sym] = {
                    "symbol": sym, "direction": "LONG" if signed > 0 else "SHORT",
                    "qty": qty, "entry_price": price, "entry_time": _hhmmss(),
                    "entry_ts": time.time(), "entry_oid": rec["oid"],
                    "entry_ref": rec["ref"], "commission": comm,
                    "stop_initial": 0.0, "stop_current": 0.0, "stop_moves": 0,
                    "stop_history": [],
                }
                self.ledger.append("POSITION_OPEN", symbol=sym,
                                   direction=self._lots[sym]["direction"],
                                   qty=qty, price=round(price, 4),
                                   oid=rec["oid"], ref=rec["ref"])
            else:
                lot = self._lots.setdefault(sym, {})
                lot["qty"] = abs(new)
                lot["entry_price"] = avg
                lot["commission"] = float(lot.get("commission", 0.0)) + comm
        else:
            # reducing or closing — realize against the average cost
            closed_qty = min(abs(cur), qty)
            direction  = 1 if cur > 0 else -1
            realized   = (price - avg) * closed_qty * direction - comm
            lot = self._lots.get(sym) or {}
            lot["commission"] = float(lot.get("commission", 0.0)) + comm
            if abs(new) == 0:
                self._close_round_trip(sym, rec, closed_qty, price, realized, lot)
                self._lots.pop(sym, None)
            else:
                lot["qty"] = abs(new)
            if abs(new) > 0 and (new > 0) != (cur > 0):
                # Flipped straight through zero (the strategy never does this,
                # but a reconciliation over-cover could): the old lot closed
                # above, so start a fresh one on the new side.
                avg = price
                self._lots[sym] = {
                    "symbol": sym, "direction": "LONG" if new > 0 else "SHORT",
                    "qty": abs(new), "entry_price": price, "entry_time": _hhmmss(),
                    "entry_ts": time.time(), "entry_oid": rec["oid"],
                    "entry_ref": rec["ref"], "commission": 0.0,
                    "stop_initial": 0.0, "stop_current": 0.0, "stop_moves": 0,
                    "stop_history": [],
                }

        # One accumulation point for the day's realized total: price P&L on
        # reductions, minus every commission at the moment it is paid.
        self._realized += realized

        if new == 0:
            self._positions.pop(sym, None)
        else:
            self._positions[sym] = {
                "side": "LONG" if new > 0 else "SHORT",
                "qty": float(abs(new)), "avg": float(avg),
                "conid": rec.get("conid", 0),
            }
        return realized

    def _close_round_trip(self, sym: str, rec: Dict[str, Any], qty: int,
                          exit_price: float, realized: float,
                          lot: Dict[str, Any]) -> None:
        """Record a completed trade — this is what the history table shows."""
        entry = float(lot.get("entry_price", 0.0))
        direction = lot.get("direction", "LONG")
        gross = (exit_price - entry) * qty * (1 if direction == "LONG" else -1)
        comm  = float(lot.get("commission", 0.0))
        trade = {
            "date": _today_str(), "symbol": sym, "direction": direction,
            "qty": qty, "entry_time": lot.get("entry_time", ""),
            "entry_price": round(entry, 4), "exit_time": _hhmmss(),
            "exit_price": round(exit_price, 4),
            "exit_reason": _exit_reason(rec["ref"], rec["order_type"]),
            "gross_pnl": round(gross, 2), "commission": round(comm, 2),
            "net_pnl": round(gross - comm, 2),
            "pnl_pct": round((gross / (entry * qty) * 100.0) if entry and qty else 0.0, 3),
            "stop_initial": round(float(lot.get("stop_initial") or 0.0), 4),
            "stop_final": round(float(lot.get("stop_current") or 0.0), 4),
            "stop_moves": int(lot.get("stop_moves", 0)),
            "hold_sec": int(time.time() - float(lot.get("entry_ts", time.time()))),
            "entry_oid": lot.get("entry_oid"), "exit_oid": rec["oid"],
            "stop_history": lot.get("stop_history", []),
        }
        self._closed.append(trade)
        self.ledger.append("TRADE_CLOSED", **trade)
        self.ledger.append_trade_csv(trade)
        logger.warning("SHADOW TRADE CLOSED [%s] %s %d  %.4f → %.4f  net=%.2f  "
                       "(%s, stop moved %d×)  DAY NET=%.2f",
                       sym, direction, qty, entry, exit_price, trade["net_pnl"],
                       trade["exit_reason"], trade["stop_moves"], self.net_pnl())

    def unrealized(self) -> float:
        """Mark open positions to the live market."""
        total = 0.0
        with self._lock:
            for sym, p in self._positions.items():
                _b, _a, last, _f = self._quote(sym)
                if last <= 0:
                    continue
                sign = 1 if p["side"] == "LONG" else -1
                total += (last - float(p["avg"])) * float(p["qty"]) * sign
        return round(total, 2)

    def net_pnl(self) -> float:
        """Realized (net of commission) + open mark-to-market."""
        return round(self._realized + self.unrealized(), 2)

    # ── callbacks back into the engine ────────────────────────────────────────

    def _emit_status(self, oid: int, status: str, filled: float,
                     remaining: float, avg: float) -> None:
        with self._lock:
            self._emit_status_locked(oid, status, filled, remaining, avg)

    def _emit_status_locked(self, oid: int, status: str, filled: float,
                            remaining: float, avg: float) -> None:
        """Write the status where get_order_status() reads it, then log it."""
        client = self._client
        if client is not None:
            try:
                info = {"status": status, "filled": filled, "remaining": remaining,
                        "avg_fill_price": avg, "last_fill_price": avg, "parent_id": 0}
                with client._status_lock:
                    client._order_statuses[oid] = info
                client._last_order_callback_at = time.time()
                client._last_receive_at = time.time()
            except Exception as exc:                    # pragma: no cover
                logger.debug("SHADOW status write failed oid=%d: %s", oid, exc)
        if status != "Filled":                          # Filled is logged by _execute
            logger.info("TWS orderStatus: oid=%d  status=%s  filled=%.0f  remaining=%.0f  "
                        "avgPrice=%.4f", oid, status, filled, remaining, avg)

    def _fire_callbacks(self, oid: int, status: str, filled: float,
                        remaining: float, avg: float) -> None:
        """
        Publish the status, then run the engine hooks OFF the fill-engine thread.

        The status dict is written synchronously — fill verification polls it and
        must see the fill immediately. The callbacks are dispatched on a worker
        thread because the engine's stop-fill hook can block (reconciliation,
        auto-flatten, sleeps), and a blocked fill engine would stop pricing every
        OTHER symbol's stop. Real TWS callbacks arrive on their own reader thread
        for the same reason.
        """
        self._emit_status(oid, status, filled, remaining, avg)
        client = self._client
        if client is None:
            return

        def _dispatch() -> None:
            cb = client._fill_callbacks.get(oid)
            if cb:
                try:
                    cb(oid, status, filled, avg)
                except Exception as exc:
                    logger.error("SHADOW fill callback error oid=%d: %s", oid, exc)
            engine_cb = getattr(client, "_engine_on_stop_callback", None)
            if engine_cb:
                try:
                    engine_cb(oid, status, filled, remaining)
                except Exception as exc:
                    logger.error("SHADOW engine status forward error oid=%d: %s",
                                 oid, exc)

        threading.Thread(target=_dispatch, daemon=True,
                         name=f"shadow-cb-{oid}").start()

    # ── snapshot for the UI ───────────────────────────────────────────────────

    def snapshot(self) -> Dict[str, Any]:
        """Everything the Shadow Trader page shows for TODAY, in one dict."""
        with self._lock:
            positions = []
            for sym, p in self._positions.items():
                _b, _a, last, fresh = self._quote(sym)
                lot  = self._lots.get(sym, {})
                sign = 1 if p["side"] == "LONG" else -1
                upl  = (last - float(p["avg"])) * float(p["qty"]) * sign if last else 0.0
                stop = float(lot.get("stop_current") or 0.0)
                positions.append({
                    "symbol": sym, "side": p["side"], "qty": int(p["qty"]),
                    "avg": round(float(p["avg"]), 4), "last": round(last, 4),
                    "price_fresh": fresh,
                    "entry_time": lot.get("entry_time", ""),
                    "stop": round(stop, 4),
                    "stop_initial": round(float(lot.get("stop_initial") or 0.0), 4),
                    "stop_moves": int(lot.get("stop_moves", 0)),
                    # Money still at risk if the stop fills now. Once the stop is
                    # trailed past entry this goes NEGATIVE — which is the good
                    # case, so it is also surfaced as `locked` (guaranteed profit)
                    # rather than being shown as a confusing negative risk.
                    "risk": round((float(p["avg"]) - stop) * float(p["qty"]) * sign, 2)
                            if stop else 0.0,
                    "locked": round((stop - float(p["avg"])) * float(p["qty"]) * sign, 2)
                              if stop else 0.0,
                    "unrealized": round(upl, 2),
                    "unrealized_pct": round((upl / (float(p["avg"]) * float(p["qty"])) * 100.0)
                                            if p["avg"] and p["qty"] else 0.0, 3),
                    "stop_history": lot.get("stop_history", []),
                })
            orders = []
            for oid, r in sorted(self._orders.items()):
                orders.append({
                    "oid": oid, "symbol": r["symbol"], "side": r["side"],
                    "qty": r["qty"], "type": r["order_type"],
                    "price": round(r["price"], 4), "status": r["status"],
                    "ref": r["ref"], "role": _role_for_ref(r["ref"]),
                    "placed": r["placed_hhmmss"],
                    "filled_at": r.get("filled_hhmmss", ""),
                    "avg_fill": round(r["avg_fill_price"], 4),
                    "moves": len(r["price_history"]),
                    "parent": r.get("parent_id"),
                })
            realized = round(self._realized, 2)
            state = {
                "mode": "SHADOW" if self.enabled else "LIVE",
                "day": self._day,
                "generated": _hhmmss(),
                "positions": positions,
                "orders": orders,
                "fills": list(self._fills),
                "trades": list(self._closed),
                "sl_events": list(self._sl_events),
                "quotes": {s: dict(q) for s, q in self._quotes.items()},
                "pnl": {
                    "realized": realized,
                    "unrealized": self.unrealized(),
                    "net": round(realized + self.unrealized(), 2),
                    "commission": round(self._commission_total, 2),
                    "trades": len(self._closed),
                    "wins": sum(1 for t in self._closed if t["net_pnl"] > 0),
                    "losses": sum(1 for t in self._closed if t["net_pnl"] <= 0),
                    "open_positions": len(positions),
                },
                "config": {
                    "slippage": SLIPPAGE, "spread": ASSUMED_SPREAD,
                    "comm_per_share": COMM_PER_SHARE, "comm_min": COMM_MIN,
                    "limit_rule": "touch" if LMT_TOUCH_FILLS else "trade-through",
                },
            }
        return state

    def _publish_state(self, force: bool = False) -> None:
        """
        Refresh state_today.json — the file the Shadow Trader process watches.

        Price-only changes are rate-limited to 5/s (the UI cannot use more than
        that, and it keeps the write cheap). Anything material — a fill, a stop
        move, a cancel — publishes IMMEDIATELY with force=True, so the page
        reacts to a trade in well under a second even though the UI runs in its
        own process.
        """
        now = time.time()
        if not force and now - getattr(self, "_last_publish", 0.0) < 0.2:
            return
        self._last_publish = now
        self.ledger.write_state(self.snapshot())


def _exit_reason(ref: str, otype: str) -> str:
    """Human label for why a position closed, derived from the order ref."""
    up = str(ref or "").upper()
    if "_EOD" in up:
        return "EOD SQUARE-OFF"
    if up.startswith("STPMKT") or up.startswith("WIDESTP"):
        return "STOP HIT"
    if up.startswith("TECH"):
        return "TECHNICAL EXIT"
    if up.startswith("CLOSE"):
        return "MANUAL/RECONCILE CLOSE"
    if up.startswith("TARGET"):
        return "TARGET HIT"
    return otype or "CLOSE"


def _is_exit_ref(ref: str) -> bool:
    """True when an order_ref belongs to a position-CLOSING order."""
    up = str(ref or "").upper()
    if not up:
        return False
    if up.endswith("_EOD") or "_EOD_" in up:
        return True
    return up.startswith(("WIDESTP", "STPMKT", "STPPRICE", "STPLMT", "CLOSE",
                          "TECH", "TARGET"))


def _role_for_ref(ref: str) -> str:
    """Same role mapping the :8080 dashboard uses, so both panels agree."""
    up = str(ref or "").upper()
    if not up:
        return ""
    if up.endswith("_EOD") or "_EOD_" in up:
        return "EOD-EXIT"
    for prefix, role in (("PARENT", "ENTRY"), ("WIDESTP", "STOP-WIDE"),
                         ("STPMKT", "STOP"), ("CLOSE", "EXIT"),
                         ("TECH", "EXIT-TECH"), ("TARGET", "TARGET")):
        if up.startswith(prefix):
            return role
    return ""


# ── Module singleton ──────────────────────────────────────────────────────────
SHADOW = ShadowBroker()


# ── Day rebuild (used by the Shadow Trader UI for historical dates) ───────────

def rebuild_day(day: str, base_dir: Optional[Path] = None) -> Dict[str, Any]:
    """
    Replay a day's ledger into the same shape snapshot() returns.

    This is what makes "open the shadow trader for any day" work: the UI asks
    for 2026-07-14, we read shadow_2026-07-14.jsonl and rebuild that session —
    orders, fills, every stop move, each closed trade and the day's net P&L —
    without needing the bot to be running.
    """
    ledger = ShadowLedger(base_dir) if base_dir else SHADOW.ledger
    events = ledger.read_events(day)
    orders: Dict[int, Dict[str, Any]] = {}
    fills:  List[Dict[str, Any]] = []
    trades: List[Dict[str, Any]] = []
    sl:     List[Dict[str, Any]] = []
    realized = 0.0
    commission = 0.0

    for ev in events:
        kind = ev.get("type")
        if kind == "ORDER_PLACED":
            orders[int(ev.get("oid", 0))] = {
                "oid": ev.get("oid"), "symbol": ev.get("symbol"),
                "side": ev.get("side"), "qty": ev.get("qty"),
                "type": ev.get("order_type"), "price": ev.get("price"),
                "status": ev.get("status", "Submitted"), "ref": ev.get("ref", ""),
                "role": _role_for_ref(ev.get("ref", "")),
                "placed": str(ev.get("t", ""))[-8:], "filled_at": "",
                "avg_fill": 0.0, "moves": 0, "parent": ev.get("parent_id"),
            }
        elif kind == "FILL":
            oid = int(ev.get("oid", 0))
            fills.append({
                "t": str(ev.get("t", ""))[-8:], "oid": oid,
                "symbol": ev.get("symbol"), "side": ev.get("side"),
                "qty": ev.get("qty"), "price": ev.get("price"),
                "order_type": ev.get("order_type"), "ref": ev.get("ref"),
                "reason": ev.get("reason"), "commission": ev.get("commission", 0.0),
                "realized_pnl": ev.get("realized_pnl", 0.0),
                "exec_id": ev.get("exec_id", ""),
            })
            commission += float(ev.get("commission") or 0.0)
            if oid in orders:
                orders[oid].update(status="Filled",
                                   filled_at=str(ev.get("t", ""))[-8:],
                                   avg_fill=ev.get("price", 0.0))
        elif kind == "SL_UPDATE":
            sl.append(ev)
            oid = int(ev.get("oid", 0))
            if oid in orders:
                orders[oid]["price"] = ev.get("to")
                orders[oid]["moves"] = int(orders[oid].get("moves", 0)) + 1
        elif kind == "ORDER_CANCELLED":
            oid = int(ev.get("oid", 0))
            if oid in orders and orders[oid]["status"] != "Filled":
                orders[oid]["status"] = "Cancelled"
        elif kind == "TRADE_CLOSED":
            trades.append(ev)
            realized += float(ev.get("net_pnl") or 0.0)

    wins = sum(1 for t in trades if float(t.get("net_pnl") or 0) > 0)
    return {
        "mode": "SHADOW", "day": day, "historical": True,
        "generated": _hhmmss(),
        "positions": [],                                # a past day is closed out
        "orders": [orders[o] for o in sorted(orders)],
        "fills": fills, "trades": trades, "sl_events": sl,
        "pnl": {
            "realized": round(realized, 2), "unrealized": 0.0,
            "net": round(realized, 2), "commission": round(commission, 2),
            "trades": len(trades), "wins": wins, "losses": len(trades) - wins,
            "open_positions": 0,
        },
        "config": {"slippage": SLIPPAGE, "spread": ASSUMED_SPREAD,
                   "comm_per_share": COMM_PER_SHARE, "comm_min": COMM_MIN,
                   "limit_rule": "touch" if LMT_TOUCH_FILLS else "trade-through"},
    }


def available_days(base_dir: Optional[Path] = None) -> List[str]:
    """Dates that have a shadow ledger, newest first — populates the date picker."""
    ledger = ShadowLedger(base_dir) if base_dir else SHADOW.ledger
    return ledger.available_days()
