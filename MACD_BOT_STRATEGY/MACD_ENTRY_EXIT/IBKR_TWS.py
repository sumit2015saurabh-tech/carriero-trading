"""
IBKR_TWS.py — Single-channel order execution engine (TWS / IB Gateway)

Architecture:
─────────────────────────────────────────────────────────────────────────────
Channel : TWS API socket to IB Gateway (port 4002) at 45 msg/sec safety cap.

NOTE: an earlier version of this engine had a Client Portal REST API
fallback channel and a queue spill-over path. That fallback has been
REMOVED. The `channel` parameter on internal helpers is kept for interface
compatibility but always routes to TWS.

When TWS rate is saturated, new orders are queued by priority:

  Priority 1 (highest): STP        — stop market orders (protection first)
  Priority 2          : STP_PRICE  & STP_LIMIT — stop-limit orders
  Priority 3 (lowest) : NEW        — new entry orders

Queue is drained as soon as the TWS rate-tracker has capacity again.
─────────────────────────────────────────────────────────────────────────────

FIXES APPLIED (v2 — Apr 23, 2026):
  FIX 1: Bracket legs now PINNED to a single channel — never split.
  FIX 2: TWS parentId resolved to integer orderId (not string cOID).
  FIX 3: Fill verification added — 60s poll, cancel on no-fill.
  FIX 4: TWS reconnection watchdog with exponential backoff.
  FIX 6: Queue staleness — orders older than 30s are discarded.

FIXES APPLIED (v3 — Apr 22, 2026 — from 51-bug production register):
  FIX A (B-46,B-41): TWS bracket transmit=False atomic assembly for Phase 1.
  FIX B (B-41,B-42,B-17,B-18): Two-phase bracket architecture.
  FIX C (B-29): Cancel with 3x retry and 0.5s delays.
  FIX D (B-26,B-33): Placement semaphore(3) to limit concurrency.
  FIX E (B-30): Duplicate fill guard via _filling_syms set.
  FIX F (B-44,B-45): Phase 2 orders use actual filled qty, not ordered qty.
  FIX G: Fill verify receives dict copy, not mutable reference.
  FIX H (B-50): Stability gate on fill verify — wait for filled_qty to settle
                for ≥3 polls before snapshotting Phase 2 size.
  FIX I (B-02): 50ms inter-order throttle delay for Phase 2 orders.
  FIX X       : Cancel parent's remaining unfilled qty when Phase 2 fires
                with fq < ordered_qty — prevents post-Phase-2 naked tail.
  FIX Z1      : OCA grouping on STP_MKT / STOP_PRICE / STOP_LIMIT — IBKR
                auto-cancels siblings when one fills (no over-cover).
─────────────────────────────────────────────────────────────────────────────

Uses multithreading:
  - Main thread: accepts order requests via submit_order()
  - TWS thread: runs the TWS API event loop (EClient.run)
  - Queue drain thread: processes queued orders when rate frees up
  - TWS watchdog thread: reconnects TWS on disconnect
  - Fill verify threads: one per bracket, confirms parent fill
"""

from __future__ import annotations                      # Allow forward references in type hints

import json                        # JSON parsing for config and API responses
import logging                     # Logging framework
import os                          # File path operations
import threading                   # Multithreading for TWS event loop + queue drainer
import time                        # Timestamps and sleep for rate limiting
from collections import deque      # Fast append/pop for rate tracking windows
from dataclasses import dataclass, field  # Structured order representation
from datetime import datetime      # Timestamps in logs
from enum import IntEnum           # Priority enum with numeric comparison
from queue import PriorityQueue    # Thread-safe priority queue for order backlog
from typing import Any, Callable, Dict, List, Optional, Tuple  # Type hints
from zoneinfo import ZoneInfo      # Timezone-aware timestamps

# ── Timezone ──────────────────────────────────────────────────────────────
EST = ZoneInfo("America/New_York")  # All timestamps in Eastern for consistency with market hours

# ── Logger setup ──────────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")  # Log directory next to this script
os.makedirs(LOG_DIR, exist_ok=True)  # Create logs/ directory if it doesn't exist

def _setup_logging():
    """Configure dual logging: terminal output + daily rotating file."""
    root = logging.getLogger()                           # Get root logger
    root.setLevel(logging.INFO)                          # Capture INFO and above
    root.handlers.clear()                                # Prevent duplicate handlers on reimport

    fmt = logging.Formatter("%(asctime)s [TWS_ENGINE] %(levelname)s - %(message)s")  # Uniform log format

    # Console handler — prints to terminal
    ch = logging.StreamHandler()                         # Stream to stderr by default
    ch.setLevel(logging.INFO)                            # Show INFO and above on console
    ch.setFormatter(fmt)                                 # Apply uniform format
    root.addHandler(ch)                                  # Register console handler

    # File handler — one file per day, named logs/tws_DD_MM_YY.txt
    tag = datetime.now(EST).strftime("%d_%m_%y")         # Date tag for log filename
    log_path = os.path.join(LOG_DIR, f"tws_{tag}.txt")   # Full path to today's log file
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")  # Append mode, UTF-8
    fh.setLevel(logging.INFO)                            # Same level as console
    fh.setFormatter(fmt)                                 # Same format as console
    root.addHandler(fh)                                  # Register file handler

    logging.getLogger("IBKR_TWS").info("Log file → %s", log_path)  # Log the log file path

_setup_logging()                                         # Run logging setup at module load
logger = logging.getLogger("IBKR_TWS")                  # Module-level logger instance

# ── SHADOW TRADING (2026-08-06) ───────────────────────────────────────────────
# The IBKR paper account was useless for testing: limit entries never filled,
# the simulated exchange went silent and the dashboard stopped updating. Orders
# are therefore no longer sent to IBKR — they are punched into a local shadow
# broker that fills them honestly against the SAME live market data we already
# stream from IBKR. Market data, conids, historical bars and account values all
# still come from the real account; only the ORDER side is intercepted.
#
# Every live-order call site below is kept intact and commented "LIVE PATH" so
# it can be switched back on by setting SHADOW_MODE=0 — nothing was deleted.
try:
    from shadow_broker import SHADOW                  # local fill simulator + ledger
    SHADOW_ON = SHADOW.enabled                        # env SHADOW_MODE, default 1
except Exception as _sh_exc:                          # never let this break the engine
    SHADOW = None                                     # type: ignore[assignment]
    SHADOW_ON = False
    logger.error("shadow_broker unavailable (%s) — orders would go LIVE to IBKR", _sh_exc)


# =============================================================================
# CONFIGURATION — loaded from environment or defaults
# =============================================================================

# TWS API connection settings
TWS_HOST     = os.getenv("TWS_HOST", "127.0.0.1")     # TWS/IB Gateway host (localhost)
TWS_PORT     = int(os.getenv("TWS_PORT", "4002"))      # 7497=paper, 7496=live
TWS_CLIENT_ID = int(os.getenv("TWS_CLIENT_ID", "1"))   # Unique client ID for this connection

ACCOUNT_ID   = os.getenv("IBKR_ACCOUNT_ID", "DUXXXXXXX")  # IBKR account ID

# Rate limits — conservative thresholds to avoid disconnection
TWS_MAX_MSG_PER_SEC = int(os.getenv("TWS_MAX_MSG_PER_SEC", "45"))   # TWS hard limit is 50; we use 45 as safety margin

# Soft threshold (90% of cap) used by RateTracker to flag near-saturation in
# logs. Originally this was also the spill-over trigger to the CP API
# fallback; CP API has been removed but the threshold is still a useful
# saturation warning.
TWS_SPILLOVER_THRESHOLD = float(os.getenv("TWS_SPILLOVER_PCT", "0.90"))

# Fill verification timeout — how long to wait for parent fill before cancelling bracket
FILL_VERIFY_TIMEOUT  = int(os.getenv("FILL_VERIFY_TIMEOUT",  "60"))   # seconds to wait for fill
FILL_VERIFY_INTERVAL = float(os.getenv("FILL_VERIFY_INTERVAL", "0.5")) # poll every 0.5s — fast enough for 5s bars


def _entry_fill_timeout(symbol, _default=FILL_VERIFY_TIMEOUT):
    """2026-06-09: keep the entry LIMIT working for ~2 candles (the signal
    candle + the next) before the no-fill cancel, instead of a flat 60s.
    Returns 2 x the symbol's entry-candle size in seconds; falls back to
    FILL_VERIFY_TIMEOUT if the resolution is unknown."""
    try:
        import json as _json
        with open("config.json") as _f:
            _st = (_json.load(_f).get("Stocks") or {}).get(symbol, {}) or {}
        _r = str(_st.get("entry_candles") or _st.get("macd_resolution") or "").strip().lower()
        _secs = None
        if _r.endswith("min"):
            _secs = int(_r[:-3] or "1") * 60
        elif _r.endswith("s") and not _r.endswith("tick"):
            _secs = int(_r[:-1])
        if _secs and _secs > 0:
            return max(2 * _secs, 20)
    except Exception:
        pass
    return _default


# Queue staleness — orders older than this are discarded (stale stop prices are dangerous)
QUEUE_MAX_AGE_SEC = int(os.getenv("QUEUE_MAX_AGE_SEC", "30"))  # 30 seconds max queue age


# =============================================================================
# ORDER PRIORITY — determines queue drain order when both channels are full
# =============================================================================

class OrderPriority(IntEnum):
    """
    Lower numeric value = higher priority (PriorityQueue is min-heap).

    Rationale:
      STP (stop market) is the most critical — it protects against loss.
      STP_LIMIT (stop price + stop limit) is next — secondary protection.
      NEW orders (entries) are lowest — missing an entry costs opportunity,
      but missing a stop costs real money.
    """
    STP       = 1   # Stop Market — HIGHEST priority (loss protection)
    STP_LIMIT = 2   # Stop Price & Stop Limit — secondary protection
    NEW       = 3   # New entry orders — LOWEST priority (opportunity cost only)


# =============================================================================
# ORDER DATA CLASS — represents a single order to be placed
# =============================================================================

@dataclass(order=True)
class OrderTicket:
    """
    A single order ticket that flows through the dual-channel system.

    The @dataclass(order=True) makes it comparable by field order,
    and PriorityQueue uses the `priority` field (first) for ordering.
    The `seq` field breaks ties within the same priority (FIFO within tier).
    """
    priority: OrderPriority                       # Queue drain order (STP > STP_LIMIT > NEW)
    seq: int                                      # Monotonic counter — FIFO within same priority
    symbol: str            = field(compare=False)  # Ticker symbol (e.g., "AAPL")
    conid: int             = field(compare=False)  # IBKR contract ID
    side: str              = field(compare=False)  # "BUY" or "SELL"
    order_type: str        = field(compare=False)  # "MKT", "LMT", "STP", "STP LMT"
    quantity: int          = field(compare=False)  # Number of shares
    price: float           = field(compare=False, default=0.0)     # Limit price or stop trigger price
    aux_price: float       = field(compare=False, default=0.0)     # Stop limit price (for STP LMT orders)
    tif: str               = field(compare=False, default="DAY")   # Time in force: DAY, GTC
    parent_id: str         = field(compare=False, default="")      # Parent order ref for bracket children
    order_ref: str         = field(compare=False, default="")      # Our custom reference ID (cOID)
    callback: Optional[Callable] = field(compare=False, default=None)  # Called with (order_id, status) after placement
    oca_group: str         = field(compare=False, default="")      # FIX Z1: OCA group id (links sibling stops)
    oca_type: int          = field(compare=False, default=0)       # FIX Z1: 1=cancel-on-fill, 2=reduce-on-fill
    _channel_used: str     = field(compare=False, default="")      # Filled after placement: "TWS" or "API"
    _created_at: float     = field(compare=False, default=0.0)     # FIX 6: timestamp for staleness check


# =============================================================================
# RATE TRACKER — sliding window counter for messages per second
# =============================================================================

class RateTracker:
    """
    Thread-safe sliding-window rate tracker.

    Counts messages sent in the last 1-second window.
    Used to decide whether a channel is "about to hit" its rate limit.

    Implementation: deque of timestamps. On each check, expired entries
    (older than 1 second) are pruned. Count of remaining = current rate.
    """

    def __init__(self, max_per_sec: int, spillover_pct: float = 0.90):
        self._max = max_per_sec                     # Hard limit (e.g., 45 for TWS)
        self._spillover = int(max_per_sec * spillover_pct)  # Threshold to trigger spillover
        self._timestamps: deque = deque()            # Timestamps of recent messages
        self._lock = threading.Lock()                # Thread safety for concurrent access

    def record(self):
        """Record that one message was just sent. Called after every successful send."""
        now = time.monotonic()                       # Monotonic clock — immune to system time changes
        with self._lock:                             # Lock for thread safety
            self._timestamps.append(now)             # Add current timestamp
            self._prune(now)                         # Remove entries older than 1 second

    def current_rate(self) -> int:
        """Return number of messages sent in the last 1 second."""
        now = time.monotonic()                       # Get current monotonic time
        with self._lock:                             # Lock for thread safety
            self._prune(now)                         # Remove stale entries first
            return len(self._timestamps)             # Remaining = messages in last 1s

    def is_near_limit(self) -> bool:
        """True if current rate >= spillover threshold (about to hit limit)."""
        return self.current_rate() >= self._spillover  # Check against spillover threshold

    def is_at_limit(self) -> bool:
        """True if current rate >= hard max (will be rejected/disconnected)."""
        return self.current_rate() >= self._max      # Check against hard limit

    def headroom(self) -> int:
        """How many more messages can be sent before hitting the hard limit."""
        return max(0, self._max - self.current_rate())  # Non-negative remaining capacity

    def _prune(self, now: float):
        """Remove timestamps older than 1 second from the left side of the deque."""
        cutoff = now - 1.0                           # 1-second sliding window
        while self._timestamps and self._timestamps[0] < cutoff:  # While oldest entry is expired
            self._timestamps.popleft()               # Oldest entry expired — remove it


# =============================================================================
# TWS API WRAPPER — socket-based connection to TWS/IB Gateway
# =============================================================================

try:
    from ibapi.client import EClient      # TWS API client (sends requests)
    from ibapi.wrapper import EWrapper     # TWS API wrapper (receives callbacks)
    from ibapi.contract import Contract    # Contract definition object
    from ibapi.order import Order          # Order definition object
    TWS_AVAILABLE = True                   # ibapi package is installed
except ImportError:
    TWS_AVAILABLE = False                  # ibapi not installed — TWS channel disabled
    logger.warning("ibapi not installed — TWS channel will be unavailable. "  # Warn at startup
                   "Install from IB's download page (not pip).")


# Conditional base classes: if ibapi is installed, inherit from EWrapper+EClient;
# otherwise inherit from object so the class definition doesn't crash on import.
_TWS_BASES = (EWrapper, EClient) if TWS_AVAILABLE else (object,)  # Dynamic base class selection


class TWSClient(*_TWS_BASES):
    """
    Thin TWS API wrapper that handles:
      - Connection to TWS/IB Gateway
      - Order ID management (nextValidId)
      - Order placement via placeOrder()
      - Order status tracking via orderStatus() callback
      - Fill detection via execDetails() callback

    Runs the EClient event loop in a dedicated daemon thread so the main
    thread is never blocked by TWS message processing.

    FIX 2 (v2): Maintains order_ref → TWS integer orderId mapping so
    bracket children can resolve string parentId refs to integer IDs.

    FIX A (v3): place_order_ticket accepts transmit parameter for atomic
    bracket assembly on TWS (transmit=False on parent/early children,
    transmit=True on last child to send all atomically).

    FIX C (v3): cancel_order_by_id retries 3 times with 0.5s delays.
    """

    def __init__(self):
        if not TWS_AVAILABLE:                            # Guard against missing ibapi
            raise RuntimeError("ibapi package not installed — cannot use TWS channel")

        EClient.__init__(self, self)                     # Initialize the API client
        EWrapper.__init__(self)                          # Initialize the callback handler

        self._next_order_id: Optional[int] = None        # Set by nextValidId callback
        self._order_id_event = threading.Event()         # Signaled when first order ID arrives
        self._order_id_lock = threading.Lock()           # Protects _next_order_id increment

        self._connected = False
        # HMDS 162 ("session from different IP") auto-clears after this ts.
        # Property _hmds_error below returns True while now() < this.
        self._hmds_error_until: float = 0.0
        self._hmds_block_duration: float = 60.0          # seconds — HMDS lockout window
        self._run_thread: Optional[threading.Thread] = None  # Thread running EClient.run()

        # Order status tracking: order_id → latest status dict
        self._order_statuses: Dict[int, Dict] = {}       # Populated by orderStatus callback
        self._status_lock = threading.Lock()              # Protects _order_statuses

        # Receive-health heartbeat (added 2026-05-21). Updated on EVERY
        # order-related callback (orderStatus, openOrder, execDetails).
        # MACDBot._handle_entry reads this to detect a stuck receive socket
        # and refuse new entries until callbacks resume.
        self._last_order_callback_at: float = 0.0

        # 2026-05-22 (Fix #3): receive-channel liveness probe. Active poll
        # via reqCurrentTime — the bot's _last_order_callback_at only fires
        # when there's order activity, so a quiet bot cannot detect a broken
        # receive socket until the next order is placed (too late).
        # The probe sends reqCurrentTime every PROBE_INTERVAL_SEC and the
        # currentTime() callback updates _last_receive_at. If
        # (now - _last_receive_at) > RECEIVE_DEAD_SEC the receive socket is
        # classified as broken and is_receive_broken() returns True.
        self._last_receive_at: float = time.time()
        self._receive_probe_started = False
        self._receive_probe_lock = threading.Lock()
        self._receive_probe_interval = 10.0     # send probe every 10s
        # 2026-05-26 (probe fix): raised 30→60s. This gateway drops reqCurrentTime
        # responses under heavy market-data load (observed 0/201 answered while 22
        # symbols streamed), so liveness is now driven by ALL inbound messages
        # (ticks + control-channel pushes), not just the probe — see the
        # _last_receive_at updates in the tick / account / position / pnl
        # callbacks below. The over-fill guard is independent (_last_order_callback_at
        # + the per-order entry gate), so this broadening does not weaken it.
        # 2026-07-29: 60s was tripping ~11x every morning at the open while the
        # socket was demonstrably healthy (log busy, 0 connectionClosed during
        # 09:30-15:30, 53k tickString/day arriving). A genuine disconnect is NOT
        # detected by this timer anyway — error codes 1100/1102 clear _connected
        # and is_receive_broken() returns True immediately on that path — so this
        # threshold only needs to be long enough to outlast the open's callback
        # latency. Override with RECEIVE_DEAD_SEC if a real outage must be caught
        # faster.
        try:
            self._receive_dead_sec   = float(os.getenv("RECEIVE_DEAD_SEC", "180"))
        except (TypeError, ValueError):
            self._receive_dead_sec   = 180.0
        self._receive_alert_sent     = False    # 2026-05-26 (P0 #4): one alert per outage
        self._last_connect_at        = 0.0      # 2026-06-11: set in nextValidId; gates the DEAD-alert warm-up suppression
        self._hmds_farm_ok           = None     # 2026-06-11: None=unknown, True=2106 OK, False=2105/2107 down (startup seed-gate)

        # FIX 2: order_ref (string) → TWS order_id (int) mapping.
        # When a parent is placed via TWS, we store its ref→id here.
        # When a child carries parent_id="PARENT_...", we look up the
        # integer TWS orderId from this map instead of parsing the string.
        self._ref_to_tws_id: Dict[str, int] = {}         # e.g. "PARENT_12345_AAPL" → 500
        self._ref_lock = threading.Lock()                 # Protects the ref mapping

        # Callbacks registered by the engine for fill notifications
        self._fill_callbacks: Dict[int, Callable] = {}   # order_id → callback(order_id, status, filled, avg_price)

        # FIX 4: reconnection state
        self._host = TWS_HOST                            # Stored for reconnection
        self._port = TWS_PORT                            # Stored for reconnection
        self._client_id = TWS_CLIENT_ID                  # Stored for reconnection

        # 2026-05-26 (two-subscription model): keepUpToDate OFFICIAL-bar streams.
        # These reqIds live in a dedicated range (>=20000) so they never collide
        # with the seed path (get_historical_bars uses _pending_hist_reqid, and
        # historicalData/historicalDataEnd filter on it — so they IGNORE the
        # stream's initial history burst; the engine is seeded separately).
        # historicalDataUpdate watches the forming bar; when its date rolls, the
        # previous bar is final and is pushed to _on_official_bar(sym, bar, True).
        self._hist_stream_syms: Dict[int, str] = {}      # reqId → symbol
        self._hist_stream_forming: Dict[int, Tuple] = {} # reqId → (bar_date, BarData)
        self._on_official_bar: Optional[Callable] = None # set by the bot

    @property
    def _hmds_error(self) -> bool:
        """
        Dynamic HMDS-blocked flag — True while the 162 cooldown is active.
        Auto-clears once time.time() >= _hmds_error_until, so a second seed
        attempt after the cooldown will actually try the broker again
        instead of short-circuiting to cold start. Read-only on purpose:
        callers set _hmds_error_until directly (see error() handler).
        """
        return time.time() < self._hmds_error_until

    # ── Connection ────────────────────────────────────────────────────────

    def connect_and_start(self, host: str = TWS_HOST, port: int = TWS_PORT,
                          client_id: int = TWS_CLIENT_ID) -> bool:
        """
        Connect to TWS/IB Gateway and start the message processing thread.

        Returns True if connection + order ID handshake succeeded.
        Returns False if TWS is unreachable or handshake timed out.
        """
        # Store connection params for reconnection (FIX 4)
        self._host = host                                # Save host for watchdog reconnect
        self._port = port                                # Save port for watchdog reconnect
        self._client_id = client_id                      # Save client ID for watchdog reconnect

        try:
            self.connect(host, port, client_id)          # Open socket to TWS
        except Exception as exc:
            logger.error("TWS connect failed (host=%s port=%d): %s", host, port, exc)  # Log failure
            return False                                 # Signal failure to caller

        # Reset the event so we wait for a fresh nextValidId
        self._order_id_event.clear()                     # Clear any previous signal

        # Start the event loop in a daemon thread — processes all TWS callbacks
        self._run_thread = threading.Thread(
            target=self.run,                             # EClient.run() blocks forever
            daemon=True,                                 # Dies when main thread exits
            name="tws-event-loop",                       # Descriptive thread name
        )
        self._run_thread.start()                         # Begin processing TWS messages

        # Wait for nextValidId callback — confirms TWS is ready for orders
        got_id = self._order_id_event.wait(timeout=10)   # 10 second timeout
        if not got_id:                                   # Timeout expired without getting ID
            logger.error("TWS connected but nextValidId not received within 10s")  # Log timeout
            self.disconnect()                            # Clean up socket
            return False                                 # Signal failure

        self._connected = True                           # Mark connection as active
        logger.info("TWS connected: host=%s port=%d clientId=%d  nextOrderId=%d",
                    host, port, client_id, self._next_order_id)  # Log successful connection

        # 2026-05-22 (Fix #3): start receive-channel liveness probe
        self._last_receive_at = time.time()
        self._start_receive_probe()
        return True                                      # Signal success

    @property
    def is_connected(self) -> bool:
        """True if TWS connection is active and we have a valid order ID."""
        return self._connected and self.isConnected()    # Both flags must be true

    # ── EWrapper callbacks ────────────────────────────────────────────────

    def nextValidId(self, orderId: int):
        """
        Called by TWS when connection is established (or after reconnect).
        Provides the next valid order ID we can use.
        All subsequent order IDs must be >= this value.
        """
        # 2026-06-11: stamp the (re)connect time. The DEAD-channel Discord alert
        # is suppressed for a warm-up window after this, because the bot re-seeds
        # on (re)connect and ticks legitimately pause during seeding — that was
        # "crying wolf" and spamming DEAD texts on every startup/reconnect.
        self._last_connect_at = time.time()
        with self._order_id_lock:                        # Thread-safe update
            # On reconnect, only update if the new ID is higher to avoid collisions
            if self._next_order_id is None or orderId > self._next_order_id:
                self._next_order_id = orderId            # Update to new valid ID
        self._order_id_event.set()                       # Signal that we're ready
        self._last_receive_at = time.time()              # 2026-05-26: control-channel liveness
        logger.info("TWS nextValidId: %d", orderId)      # Log the ID for debugging

    def orderStatus(self, orderId, status, filled, remaining,
                    avgFillPrice, permId, parentId, lastFillPrice,
                    clientId, whyHeld, mktCapPrice):
        """
        Called by TWS whenever an order's status changes.
        We track this for fill detection and logging.

        Note: TWS sends DUPLICATE orderStatus messages — this is normal.
        We always overwrite with the latest data.
        """
        info = {
            "status": status,                            # e.g., "Submitted", "Filled", "Cancelled"
            "filled": filled,                            # Number of shares filled so far
            "remaining": remaining,                      # Shares still to fill
            "avg_fill_price": avgFillPrice,              # Average fill price across all fills
            "last_fill_price": lastFillPrice,            # Price of the most recent fill
            "parent_id": parentId,                       # Parent order ID (for bracket children)
        }

        with self._status_lock:                          # Thread-safe status update
            self._order_statuses[orderId] = info         # Update/overwrite latest status
        # Heartbeat for the receive-health gate in MACDBot._handle_entry.
        self._last_order_callback_at = time.time()
        self._last_receive_at        = time.time()    # 2026-05-22 (Fix #3)

        logger.info("TWS orderStatus: oid=%d  status=%s  filled=%.0f  remaining=%.0f  "
                    "avgPrice=%.4f", orderId, status, filled, remaining, avgFillPrice)  # Log status change

        # Fire registered callback if exists
        cb = self._fill_callbacks.get(orderId)           # Look up callback for this order
        if cb:                                           # Callback registered
            try:
                cb(orderId, status, filled, avgFillPrice)  # Invoke with fill data
            except Exception as exc:
                logger.error("TWS fill callback error for oid=%d: %s", orderId, exc)  # Log callback error

        # Engine-level forward — registered by TWSEngine to detect
        # stop-type order fills and notify the bot. Wrapped so any error here
        # never breaks the existing per-order callback path above.
        engine_cb = getattr(self, "_engine_on_stop_callback", None)
        if engine_cb:
            try:
                engine_cb(orderId, status, filled, remaining)
            except Exception as exc:
                logger.error("Engine status forward error oid=%d: %s", orderId, exc)

    def openOrder(self, orderId, contract, order, orderState):
        """
        Called by TWS when an order is acknowledged (open, submitted, etc.).
        Logs full order details: symbol, side, type, qty, price, status.
        """
        self._last_order_callback_at = time.time()    # receive-health heartbeat
        self._last_receive_at        = time.time()    # 2026-05-22 (Fix #3)
        commission = orderState.commission if orderState.commission < 1e9 else 0.0
        logger.info("TWS openOrder: oid=%d  %s %s %s qty=%.0f  lmt=%.4f  aux=%.4f  "
                    "status=%s  commission=%.4f",
                    orderId, order.action, contract.symbol, order.orderType,
                    order.totalQuantity, order.lmtPrice if order.lmtPrice < 1e9 else 0.0,
                    order.auxPrice if order.auxPrice < 1e9 else 0.0,
                    orderState.status, commission)

    def execDetails(self, reqId, contract, execution):
        """
        Called by TWS when an order execution (fill) occurs.
        Logs every fill with price, quantity, side, and time.
        """
        self._last_order_callback_at = time.time()    # receive-health heartbeat
        self._last_receive_at        = time.time()    # 2026-05-22 (Fix #3)
        logger.info("TWS FILL: oid=%d  symbol=%s  side=%s  qty=%.0f  price=%.4f  "
                    "execId=%s  time=%s  exchange=%s",
                    execution.orderId, contract.symbol, execution.side,
                    execution.shares, execution.price,
                    execution.execId, execution.time, execution.exchange)

    def currentTime(self, time_val: int):
        """
        2026-05-22 (Fix #3): liveness-probe response. reqCurrentTime is sent
        every _receive_probe_interval seconds; this callback fires when the
        response arrives. If responses stop, the receive channel is broken.
        """
        self._last_receive_at = time.time()

    def is_receive_broken(self) -> bool:
        """
        2026-05-22 (Fix #3): True if the receive channel hasn't responded
        within _receive_dead_sec. Used by _handle_entry to refuse new orders
        when TWS callbacks have stopped flowing.
        """
        if not self._connected:
            return True
        return (time.time() - self._last_receive_at) > self._receive_dead_sec

    def _start_receive_probe(self) -> None:
        """2026-05-22 (Fix #3): idempotent — starts the liveness-probe thread."""
        with self._receive_probe_lock:
            if self._receive_probe_started:
                return
            self._receive_probe_started = True
        threading.Thread(
            target=self._receive_probe_loop,
            daemon=True,
            name="tws-receive-probe",
        ).start()
        logger.info("TWS receive-probe started (interval=%.0fs, dead=%.0fs)",
                    self._receive_probe_interval, self._receive_dead_sec)

    def _receive_probe_loop(self) -> None:
        """Send reqCurrentTime periodically. Sleep + send. Exit on disconnect."""
        while True:
            try:
                time.sleep(self._receive_probe_interval)
                if not self._connected:
                    return
                try:
                    self.reqCurrentTime()
                except Exception as exc:
                    logger.warning("receive-probe reqCurrentTime failed: %s", exc)

                # 2026-05-26 (P0 #4): account-wide alert. Entry placement is
                # already refused account-wide while is_receive_broken() (the
                # shared socket gates every symbol). This adds a one-shot
                # operator alert so a human knows trading has halted, and resets
                # when the channel recovers so the next outage re-alerts.
                if self.is_receive_broken():
                    if not self._receive_alert_sent:
                        self._receive_alert_sent = True
                        dead = time.time() - self._last_receive_at
                        # 2026-07-14: enrich with request-load DIAG so the log
                        # explains WHY the socket died even on a light data day —
                        # coincides with a historical-request burst / 162 pacing
                        # throttle, or genuine external silence?
                        try:
                            _win   = getattr(self, "_hist_window_sec", 600.0)
                            _reqs  = getattr(self, "_hist_req_times", []) or []
                            _cut   = time.monotonic() - _win
                            _inwin = sum(1 for _t in _reqs if _t > _cut)
                            _cap   = getattr(self, "_hist_max_per_window", 50)
                            _sconn = time.time() - getattr(self, "_last_connect_at", 0.0)
                            _diag  = ("hist_req_in_window=%d/%s, hmds_162_throttled=%s, "
                                      "%.0fs_since_connect%s" % (
                                          _inwin, _cap, self._hmds_error, _sconn,
                                          " (warm-up seeding — expected)" if _sconn < 420 else ""))
                        except Exception:
                            _diag = "diag_unavailable"
                        logger.error("RECEIVE CHANNEL DEAD (%.0fs silent) — TWS "
                                     "callbacks stopped; new entries halted "
                                     "account-wide until recovery. WHY: %s", dead, _diag)
                        # 2026-06-11: suppress the Discord push during the warm-up
                        # window after a (re)connect — the bot re-seeds then and
                        # ticks pause legitimately, so a DEAD here is expected, not
                        # an outage. Still logged above; just don't spam Discord.
                        try:
                            _supp = float(os.getenv("DEAD_ALERT_SUPPRESS_SEC", "420"))
                        except (TypeError, ValueError):
                            _supp = 420.0
                        _since_conn = time.time() - getattr(self, "_last_connect_at", 0.0)
                        if _since_conn >= _supp:
                            try:
                                from notifier import notify_critical
                                notify_critical(
                                    "TWS receive channel DEAD — trading halted",
                                    f"No reqCurrentTime response for {dead:.0f}s. "
                                    "Order/position callbacks have stopped; the bot is "
                                    "refusing ALL new entries until the receive socket "
                                    "recovers. Check IB Gateway.")
                            except Exception:
                                pass
                        else:
                            logger.warning("RECEIVE DEAD Discord alert SUPPRESSED "
                                           "(%.0fs into warm-up window, < %.0fs) — "
                                           "seeding/reconnect, not an outage", _since_conn, _supp)
                elif self._receive_alert_sent:
                    self._receive_alert_sent = False
                    logger.warning("RECEIVE CHANNEL RECOVERED — entries re-enabled")
            except Exception as exc:
                logger.error("receive-probe loop error: %s", exc)

    def commissionReport(self, commissionReport):
        """
        Called by TWS after execDetails with commission info.
        Logs the commission for each fill.
        """
        logger.info("TWS COMMISSION: execId=%s  commission=%.4f  currency=%s  realizedPNL=%.4f",
                    commissionReport.execId, commissionReport.commission,
                    commissionReport.currency,
                    commissionReport.realizedPNL if commissionReport.realizedPNL < 1e9 else 0.0)

    # ── Market data callbacks ─────────────────────────────────────────────

    def tickPrice(self, reqId, tickType, price, attrib):
        """
        Called by TWS/IB Gateway when a price tick arrives.
        Routes to the registered _on_tick_price callback if set.
        Tick types: 1=bid, 2=ask, 4=last, 6=high, 7=low, 9=close,
                    66=delayed bid, 67=delayed ask, 68=delayed last.
        """
        self._last_receive_at = time.time()    # 2026-05-26: ticks count as receive-liveness
        cb = getattr(self, '_on_tick_price', None)
        if cb:
            try:
                cb(reqId, tickType, price, attrib)
            except Exception as exc:
                logger.error("tickPrice callback error reqId=%d: %s", reqId, exc)

    def tickSize(self, reqId, tickType, size):
        """
        Called by TWS/IB Gateway when a size tick arrives.
        Routes to the registered _on_tick_size callback if set.
        """
        self._last_receive_at = time.time()    # 2026-05-26: ticks count as receive-liveness
        cb = getattr(self, '_on_tick_size', None)
        if cb:
            try:
                cb(reqId, tickType, size)
            except Exception as exc:
                logger.error("tickSize callback error reqId=%d: %s", reqId, exc)

    def tickString(self, reqId, tickType, value):
        """
        Called by TWS for string-valued ticks (tickType 45 = last-trade
        timestamp, 48 = RTVolume, 84 = last exchange).

        2026-07-29: this is the HIGHEST-VOLUME inbound market-data callback on
        this setup — 53,139 arrivals on 2026-07-28 — and until now it was the
        only one NOT stamping _last_receive_at (it was never overridden, so it
        fell through to the ibapi default). That is why the watchdog reported
        "69s silent" while the log was plainly busy: real traffic was arriving
        continuously through a callback that did not count as liveness.
        Every inbound message is proof the receive socket is alive.
        """
        self._last_receive_at = time.time()
        cb = getattr(self, '_on_tick_string', None)
        if cb:
            try:
                cb(reqId, tickType, value)
            except Exception as exc:
                logger.error("tickString callback error reqId=%d: %s", reqId, exc)

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        """
        Called by TWS on errors and informational messages.
        Error codes < 2000 are warnings, >= 2000 are errors.
        Some codes are informational (e.g., 2104 = market data farm connected).

        FIX 4: On connection-lost codes, set _connected = False.
        The watchdog thread detects this and triggers reconnection.
        """
        # Informational messages — log at debug level
        if errorCode in (2104, 2106, 2158, 2119):        # Market data farm / connection info codes
            if errorCode == 2106:                        # 2026-06-11: HMDS (historical-data) farm OK
                self._hmds_farm_ok = True
            logger.debug("TWS info [%d]: %s", errorCode, errorString)  # Debug-level for info codes
            return                                       # No action needed

        # 2026-06-11: HMDS / market-data farm INACTIVE — historical seeds will
        # return empty until it recovers. Track so the startup seed-gate waits.
        if errorCode in (2103, 2105, 2107):
            if errorCode in (2105, 2107):                # HMDS (historical) farm down
                self._hmds_farm_ok = False
            logger.warning("TWS farm status [%d]: %s", errorCode, errorString)
            return

        # Connection lost — FIX 4: watchdog will handle reconnection
        if errorCode in (1100, 1101, 1102):              # Connection lost/restored codes
            self._connected = False                      # Watchdog thread will detect this
            logger.error("TWS connection lost [%d]: %s — watchdog will reconnect",
                         errorCode, errorString)         # Log connection loss
            return                                       # Watchdog handles reconnection

        # Order-specific errors
        if errorCode == 10189:
            # FIX 3: No market data permissions for reqTickByTickData.
            # Fall back to reqMktData with marketDataType=3 (delayed free data).
            # This lets the bot receive prices without a paid data subscription.
            logger.warning("TWS 10189 for reqId=%d: no tick-by-tick permission — "
                           "falling back to reqMktData (live streaming)", reqId)
            conid = getattr(self, '_conid_for_reqid', {}).get(reqId)
            if conid:
                try:
                    from ibapi.contract import Contract as _C
                    c = _C()
                    c.conId    = conid
                    c.secType  = "STK"
                    c.exchange = "SMART"
                    c.currency = "USD"
                    self.reqMarketDataType(1)             # 1 = live data (paper accounts get free live)
                    self.reqMktData(reqId, c, "", False, False, [])
                    logger.info("reqMktData(live) sent for reqId=%d conid=%d",
                                reqId, conid)
                except Exception as _fe:
                    logger.warning("reqMktData fallback failed: %s", _fe)
        elif errorCode == 162:
            # HMDS error — typically "Trading TWS session is connected from a
            # different IP address" when the same IBKR user is also logged in
            # on another machine (mobile app, web TWS, another Gateway). The
            # _hmds_error property short-circuits seed calls during the
            # cooldown so seeding logic can skip immediately instead of
            # waiting 30s per symbol. Cooldown lets the bot retry later in
            # case the other session disconnects on its own.
            self._hmds_error_until = time.time() + self._hmds_block_duration
            logger.warning("TWS HMDS error 162 for reqId=%d: %s — HMDS blocked for %.0fs",
                           reqId, errorString, self._hmds_block_duration)
        elif errorCode == 10089:
            # No live market data subscription for API.
            # Fall back to reqMarketDataType(3) = delayed (15-min lag) data.
            # This is the last resort — delayed prices are better than no prices.
            logger.warning("TWS 10089 for reqId=%d: no live API market data subscription — "
                           "falling back to reqMktData (delayed 15-min)", reqId)
            conid = getattr(self, '_conid_for_reqid', {}).get(reqId)
            if conid:
                try:
                    from ibapi.contract import Contract as _C
                    c = _C()
                    c.conId    = conid
                    c.secType  = "STK"
                    c.exchange = "SMART"
                    c.currency = "USD"
                    self.reqMarketDataType(3)             # 3 = delayed free data (no subscription needed)
                    self.reqMktData(reqId, c, "", False, False, [])
                    logger.info("reqMktData(delayed) sent for reqId=%d conid=%d",
                                reqId, conid)
                except Exception as _fe:
                    logger.warning("reqMktData delayed fallback failed: %s", _fe)
        elif reqId > 0:                                  # Error tied to a specific order
            logger.error("TWS order error: oid=%d  code=%d  msg=%s  reject=%s",
                         reqId, errorCode, errorString,
                         advancedOrderRejectJson[:200] if advancedOrderRejectJson else "")
        else:                                            # General system error
            logger.warning("TWS error [%d]: %s", errorCode, errorString)

    # ── Order placement ───────────────────────────────────────────────────

    def get_next_order_id(self) -> int:
        """
        Return the next valid order ID and increment the counter.
        Thread-safe — multiple threads can call this concurrently.
        """
        with self._order_id_lock:                        # Lock for atomic read-and-increment
            oid = self._next_order_id                    # Read current ID
            self._next_order_id += 1                     # Increment for next call
        return oid                                       # Return the reserved ID

    def register_ref(self, order_ref: str, tws_order_id: int):
        """
        FIX 2: Store a mapping from our string order_ref to TWS integer orderId.
        Called immediately after placing a parent so children can look up the
        integer parentId they need for TWS bracket linkage.
        """
        with self._ref_lock:                             # Thread-safe map update
            self._ref_to_tws_id[order_ref] = tws_order_id  # Store ref→id mapping
        logger.debug("TWS ref registered: %s → %d", order_ref, tws_order_id)  # Log mapping

    def resolve_parent_id(self, parent_ref: str) -> Optional[int]:
        """
        FIX 2: Look up the TWS integer orderId for a parent order_ref string.
        Returns the integer orderId, or None if the parent hasn't been placed
        via TWS (or was placed via API where string refs work natively).
        """
        with self._ref_lock:                             # Thread-safe map read
            return self._ref_to_tws_id.get(parent_ref)   # Return integer ID or None

    def get_order_status(self, order_id: int) -> Optional[Dict]:
        """
        FIX 3: Return the latest status dict for a TWS order.
        Used by fill verification to check if an order has filled.
        """
        with self._status_lock:                          # Thread-safe read
            return self._order_statuses.get(order_id, {}).copy() if order_id in self._order_statuses else None  # Return copy or None

    def place_order_ticket(self, ticket: OrderTicket, transmit: bool = True) -> Optional[int]:
        """
        Convert an OrderTicket to TWS Contract + Order objects and submit.

        FIX 1 & 2: For bracket children, resolves string parent_id to integer
        TWS orderId via the ref mapping. If resolution fails, logs error
        and places as standalone (safer than crashing).

        FIX A (v3): transmit parameter controls whether the order is sent to
        the exchange immediately. For Phase 1 bracket assembly on TWS:
          parent(transmit=False) → child STP(transmit=True)
        This makes IBKR receive both legs atomically. The last order with
        transmit=True triggers transmission of all preceding orders in the
        bracket that had transmit=False.

        Returns the TWS order ID on success, None on failure.
        """
        if not self.is_connected:                        # Check connection before placing
            logger.warning("TWS not connected — cannot place order for %s", ticket.symbol)  # Log warning
            return None                                  # Cannot place without connection

        # Build the Contract object — defines WHAT we're trading
        contract = Contract()                            # Create empty contract
        contract.conId   = ticket.conid                  # IBKR contract ID (already resolved)
        contract.symbol  = ticket.symbol                 # Ticker symbol
        contract.secType = "STK"                         # Stock (could be extended for options/futures)
        contract.exchange = "SMART"                      # IBKR smart routing — best execution
        contract.currency = "USD"                        # US dollar denomination

        # Build the Order object — defines HOW we're trading
        order = Order()                                  # Create empty order
        order.action        = ticket.side                # "BUY" or "SELL"
        order.totalQuantity = ticket.quantity             # Number of shares
        order.orderType     = self._map_order_type(ticket.order_type)  # TWS order type string
        order.tif           = ticket.tif                 # Time in force: "DAY", "GTC"
        order.orderRef      = ticket.order_ref           # Our custom reference for tracking
        order.transmit      = transmit                   # FIX A: control atomic bracket assembly
        order.eTradeOnly    = False                      # Deprecated in IB Gateway 10.37+
        order.firmQuoteOnly = False                      # Deprecated in IB Gateway 10.37+

        # FIX Z1: OCA grouping — when one sibling fills, IBKR auto-cancels the rest
        if ticket.oca_group:                             # Ticket asked to be in an OCA group
            order.ocaGroup = ticket.oca_group            # Group identifier (string)
            order.ocaType  = ticket.oca_type or 1        # Default: cancel siblings on first fill

        # Set price fields based on order type
        if ticket.order_type == "LMT":                   # Limit order
            order.lmtPrice = ticket.price                # Limit price for LMT orders

        elif ticket.order_type == "STP":                 # Stop market order
            order.auxPrice = ticket.price                # Stop trigger price for STP orders

        elif ticket.order_type == "STP LMT":             # Stop limit order
            order.lmtPrice = ticket.aux_price            # Limit price (worst acceptable fill)
            order.auxPrice = ticket.price                # Stop trigger price (activates the order)

        elif ticket.order_type == "MKT":                 # Market order
            pass                                         # Market orders need no price fields

        # FIX 2: Resolve string parent_id to integer TWS orderId
        # TWS requires integer parentId — string cOID refs only work on CP API.
        # Look up the parent's TWS orderId from our ref→id mapping.
        if ticket.parent_id:                             # This ticket is a bracket child
            resolved_parent = self.resolve_parent_id(ticket.parent_id)  # Look up integer ID
            if resolved_parent is not None:              # Found the parent's TWS ID
                order.parentId = resolved_parent         # Integer TWS orderId — correct for TWS
                logger.debug("TWS parentId resolved: %s → %d", ticket.parent_id, resolved_parent)  # Log resolution
            else:
                # Parent ref not found — this means parent was placed on API or hasn't been
                # registered yet. This should NOT happen if bracket pinning (FIX 1) is working.
                logger.error("TWS: Cannot resolve parentId '%s' to integer orderId for %s. "
                             "Parent may have been placed on API (bracket channel split?). "
                             "Placing as STANDALONE — this child has NO parent protection.",
                             ticket.parent_id, ticket.symbol)  # Log error for investigation
                # Do NOT set parentId — order will be standalone.
                # This is safer than setting a garbage integer.

        # Get the next order ID and place the order
        order_id = self.get_next_order_id()              # Reserve unique TWS order ID

        # Register callback before placing so we don't miss fast fills
        if ticket.callback:                              # Caller provided a callback
            self._fill_callbacks[order_id] = ticket.callback  # Register before placement

        # ── SHADOW BRANCH (2026-08-06) ───────────────────────────────────────
        # In shadow mode the order is booked locally instead of being sent to
        # IBKR. The shadow broker logs the identical "TWS placed:" line, fires
        # the same orderStatus / FILL callbacks, and fills against live prices —
        # so the engine above (bracket assembly, fill verification, Phase 2,
        # breakeven, trailing) and the :8080 dashboard both work unchanged.
        if SHADOW_ON and SHADOW is not None:
            shadow_oid = SHADOW.place(order_id, ticket, transmit=transmit)
            if shadow_oid is not None and ticket.order_ref:
                self.register_ref(ticket.order_ref, order_id)  # children still resolve
            if shadow_oid is None:
                self._fill_callbacks.pop(order_id, None)
            return shadow_oid

        try:
            # LIVE PATH — reactivated by SHADOW_MODE=0. Kept, not deleted.
            self.placeOrder(order_id, contract, order)   # TWS API call — sends via socket
            logger.info("TWS placed: oid=%d  %s %s %d %s @ %.4f  ref=%s  parentId=%s  transmit=%s",
                        order_id, ticket.side, ticket.symbol, ticket.quantity,
                        ticket.order_type, ticket.price, ticket.order_ref,
                        order.parentId if order.parentId else "none",
                        transmit)                        # FIX A: log transmit flag for debugging

            # FIX 2: Register this order's ref→id mapping for child resolution
            if ticket.order_ref:                         # Has a reference string
                self.register_ref(ticket.order_ref, order_id)  # Store ref→id for children

            return order_id                              # Return the TWS order ID
        except Exception as exc:
            logger.error("TWS placeOrder failed for %s: %s", ticket.symbol, exc)  # Log placement error
            self._fill_callbacks.pop(order_id, None)     # Clean up callback on failure
            return None                                  # Signal failure

    def cancel_order_by_id(self, order_id: int) -> bool:
        """
        FIX C (B-29): Cancel a specific order by its TWS order ID.
        Retries up to 3 times with 0.5s delays between attempts.
        Returns True if cancel was successfully sent (no exception), False otherwise.
        OID should only be cleared from tracking after confirmed success.
        """
        # ── SHADOW BRANCH (2026-08-06) — cancel in the local book ────────────
        if SHADOW_ON and SHADOW is not None:
            return SHADOW.cancel(order_id)

        max_retries = 3                                  # FIX C: retry 3 times total
        for attempt in range(1, max_retries + 1):        # Attempts 1, 2, 3
            try:
                # LIVE PATH — reactivated by SHADOW_MODE=0. Kept, not deleted.
                self.cancelOrder(order_id)               # TWS API cancel call
                logger.info("TWS cancel sent: oid=%d (attempt %d/%d)",
                            order_id, attempt, max_retries)  # Log successful send
                return True                              # Cancel request accepted by TWS
            except Exception as exc:
                logger.warning("TWS cancel attempt %d/%d failed for oid=%d: %s",
                               attempt, max_retries, order_id, exc)  # Log retry
                if attempt < max_retries:                # More retries remaining
                    time.sleep(0.5)                      # FIX C: 0.5s delay between retries
        logger.error("TWS cancel EXHAUSTED all %d retries for oid=%d", max_retries, order_id)  # All retries failed
        return False                                     # Could not cancel after all retries

    def modify_order_price(self, order_id: int, ticket: 'OrderTicket',
                           new_price: float, new_aux_price: float = 0.0) -> bool:
        """
        Modify an existing TWS order in-place. On TWS, calling placeOrder()
        with the SAME orderId updates the order instead of creating a new one.

        Rebuilds the full Contract + Order objects from the original ticket,
        overrides only the price fields, and resubmits with the same orderId.

        Args:
            order_id:      The existing TWS integer order ID to modify.
            ticket:        The original OrderTicket (for symbol, conid, side, qty, type).
            new_price:     New price (auxPrice for STP, lmtPrice for LMT).
            new_aux_price: New auxPrice for STP LMT (limit price). 0.0 if not STP LMT.

        Returns True if modify was accepted by TWS, False on failure.
        """
        # ── SHADOW BRANCH (2026-08-06) ───────────────────────────────────────
        # This is the ONLY path a stop-loss ever moves through (breakeven and
        # the 5-bar trail both land here), so the shadow broker records the
        # old → new price, the reason, and the market price at that instant.
        # It also emits the same "TWS modify sent:" line the :8080 dashboard
        # parses to show that a stop has moved.
        if SHADOW_ON and SHADOW is not None:
            ok = SHADOW.modify(order_id, ticket, new_price, new_aux_price)
            if ok:                                       # keep the stored ticket in sync
                if ticket.order_type == "STP LMT":
                    ticket.price     = new_price
                    ticket.aux_price = new_aux_price
                else:
                    ticket.price     = new_price
            return ok

        if not self.is_connected:                        # Check connection before modifying
            logger.warning("TWS not connected — cannot modify oid=%d", order_id)
            return False                                 # Cannot modify without connection

        # Rebuild Contract from the original ticket
        contract = Contract()                            # Create fresh Contract object
        contract.conId   = ticket.conid                  # Same contract ID
        contract.symbol  = ticket.symbol                 # Same ticker symbol
        contract.secType = "STK"                         # Stock type (same as original)
        contract.exchange = "SMART"                      # IBKR smart routing
        contract.currency = "USD"                        # USD denomination

        # Rebuild Order from the original ticket with updated price
        order = Order()                                  # Create fresh Order object
        order.action        = ticket.side                # Same side (BUY/SELL)
        order.totalQuantity = ticket.quantity            # Same quantity
        order.orderType     = self._map_order_type(ticket.order_type)  # Same order type
        order.tif           = ticket.tif                 # Same time in force
        order.orderRef      = ticket.order_ref           # Same reference
        order.transmit      = True                       # Always transmit on modify
        order.eTradeOnly    = False                      # Deprecated in IB Gateway 10.37+
        order.firmQuoteOnly = False                      # Deprecated in IB Gateway 10.37+

        # FIX Z2: preserve OCA group identity on a modify. If we leave ocaGroup
        # / ocaType blank on the rebuilt Order, IBKR treats this as an attempt
        # to *change* the OCA membership/type and rejects with code=10327
        # ("OCA group type revision is not allowed"). The async rejection
        # arrives AFTER placeOrder() returns, so the previous code path
        # silently believed the modify succeeded — that's how STP_MKT prices
        # were diverging from IBKR's real values during BE / trail.
        if getattr(ticket, "oca_group", ""):
            order.ocaGroup = ticket.oca_group
            order.ocaType  = ticket.oca_type or 1

        # Set the NEW price fields based on order type
        if ticket.order_type == "STP":                   # Stop market: price is auxPrice
            order.auxPrice = new_price                   # Updated stop trigger price
        elif ticket.order_type == "LMT":                 # Limit: price is lmtPrice
            order.lmtPrice = new_price                   # Updated limit price
        elif ticket.order_type == "STP LMT":             # Stop limit: both prices
            order.auxPrice = new_price                   # Updated stop trigger price
            order.lmtPrice = new_aux_price               # Updated limit price

        try:
            self.placeOrder(order_id, contract, order)   # Same orderId = modify (not new order)
            # 2026-07-16: keep the STORED ticket in sync with what we just sent.
            # Previously only the outgoing Order carried the new price; the
            # OrderTicket in _placed_orders kept its ORIGINAL price, so anything
            # reading _placed_orders (the dashboard SL fallback, a later modify)
            # saw a STALE stop after every trail — "the stop never updates".
            if ticket.order_type == "STP LMT":
                ticket.price     = new_price             # stop trigger price
                ticket.aux_price = new_aux_price         # stop limit price
            else:
                ticket.price     = new_price             # STP trigger / LMT price
            logger.info("TWS modify sent: oid=%d  %s  new_price=%.4f  new_aux=%.4f",
                        order_id, ticket.order_type, new_price, new_aux_price)
            return True                                  # Modification accepted by TWS
        except Exception as exc:
            logger.error("TWS modify failed for oid=%d: %s", order_id, exc)
            return False                                 # Modification failed

    @staticmethod
    def _map_order_type(otype: str) -> str:
        """
        Map our internal order type strings to TWS API order type strings.
        TWS uses slightly different naming for some order types.
        """
        mapping = {
            "MKT"     : "MKT",        # Market order — fill at current price
            "LMT"     : "LMT",        # Limit order — fill at price or better
            "STP"     : "STP",         # Stop market — triggers market order at stop price
            "STP LMT" : "STP LMT",    # Stop limit — triggers limit order at stop price
            "STP_LMT" : "STP LMT",    # Alternative naming used in some code paths
        }
        return mapping.get(otype, otype)                 # Pass through unknown types unchanged

    # ── Position tracking (reqPositions) ─────────────────────────────────

    def req_positions(self):
        """
        Subscribe to position updates from TWS.
        TWS will push all current positions via the position() callback,
        then signal completion with positionEnd().
        Safe to call at startup and periodically for a full refresh.
        """
        self._positions: Dict[str, Dict] = {}           # symbol → {side, qty, avg_cost}
        self._positions_lock = threading.Lock()          # Protects _positions dict
        self._positions_ready = threading.Event()        # Set when positionEnd() fires
        self._positions_ready.clear()                    # Reset ready flag
        self.reqPositions()                              # TWS EClient call — triggers callbacks
        logger.info("reqPositions() called — waiting for positionEnd()")

    def position(self, account: str, contract, pos: float, avgCost: float):
        """
        TWS callback: fires once per position held in the account.
        Stores position by symbol for later lookup.
        Safe to call before req_positions() — lazily initialises state.
        """
        self._last_receive_at = time.time()    # 2026-05-26: control-channel liveness
        # FIX 1: lazily initialise in case callback fires before req_positions()
        if not hasattr(self, '_positions'):
            self._positions      = {}
            self._positions_lock = threading.Lock()
            self._positions_ready = threading.Event()
        sym = contract.symbol                            # e.g. 'AAPL'
        # FIX: IBKR sends position() callback with pos=0 to notify that a
        # previously-held position has been fully closed. Storing qty=0 entries
        # causes the EoD MKT-close loop to submit invalid "size=0" orders.
        # Treat pos=0 as REMOVAL from the tracker, not insertion.
        with self._positions_lock:
            if pos == 0:
                self._positions.pop(sym, None)
                logger.debug("position() callback: %s pos=0 — removed from tracker", sym)
                return
            side = "LONG" if pos > 0 else "SHORT"       # Determine direction from sign
            self._positions[sym] = {
                "side": side,
                "qty" : abs(pos),
                "avg" : avgCost,
                "conid": contract.conId,
            }
        logger.debug("position() callback: %s %s %.0f @ %.4f", sym, side, abs(pos), avgCost)

    def positionEnd(self):
        """TWS callback: fires after all position() callbacks are delivered."""
        self._last_receive_at = time.time()    # 2026-05-26: control-channel liveness
        if not hasattr(self, '_positions_ready'):
            self._positions_ready = threading.Event()
        self._positions_ready.set()                      # Signal snapshot complete
        self.cancelPositions()                           # Stop streaming — one-shot request
        logger.debug("positionEnd() — position snapshot complete, streaming cancelled")

    # ── Account values (P&L, cash, equity) — streamed via reqAccountUpdates ──
    # IBKR pushes updateAccountValue() callbacks for every key in the account
    # summary (NetLiquidation, TotalCashValue, RealizedPnL, UnrealizedPnL, …)
    # whenever they change. We cache the latest snapshot and let the engine
    # read it during _dump_state() and the periodic P&L log.

    # Keys we care about — IBKR pushes these whenever they change broker-side.
    # Logging them directly from the WS callback gives realtime P&L without
    # any polling loop. The set is small so log volume stays low.
    # NOTE: RealizedPnL and UnrealizedPnL are NOT in this set on purpose —
    # the faster reqPnL push (see pnl() callback) already logs those values
    # at sub-second cadence; we only keep the slow updateAccountValue copies
    # cached for state dumps.
    _PNL_LOG_KEYS = {
        "NetLiquidation",       # current portfolio value (≈ "principal")
        "TotalCashValue",       # total cash across currencies
        "AvailableFunds",       # cash free for new orders
        "GrossPositionValue",   # MV of all open positions
        "BuyingPower",          # leveraged purchasing power
    }

    def updateAccountValue(self, key: str, value: str, currency: str, accountName: str):
        """TWS callback: one per account-value key, fires on subscribe and on change.

        Cache every key for state dumps; emit a one-line log only when one of
        the P&L-relevant keys has actually changed. No polling, no throttle —
        IBKR's natural push cadence handles the rate.
        """
        self._last_receive_at = time.time()    # 2026-05-26: control-channel liveness
        if not hasattr(self, '_account_values'):
            self._account_values = {}
            self._account_values_lock = threading.Lock()
        changed = False
        with self._account_values_lock:
            prev = self._account_values.get(key)
            self._account_values[key] = (value, currency)
            if key in self._PNL_LOG_KEYS and (prev is None or prev[0] != value):
                changed = True
        if changed:
            logger.info("ACCOUNT %s = %s %s", key, value, currency)

    def updateAccountTime(self, timeStamp: str):
        """TWS callback: timestamp of latest account value update."""
        if not hasattr(self, '_account_values'):
            self._account_values = {}
            self._account_values_lock = threading.Lock()
        with self._account_values_lock:
            self._account_values["__timestamp__"] = (timeStamp, "")

    def accountDownloadEnd(self, accountName: str):
        """TWS callback: fires when initial account download finishes."""
        if not hasattr(self, '_account_ready'):
            self._account_ready = threading.Event()
        self._account_ready.set()
        logger.info("accountDownloadEnd() — account values streaming for %s", accountName)

    def start_account_updates(self, account: str):
        """Subscribe to streaming account-value updates. Idempotent."""
        if not hasattr(self, '_account_values'):
            self._account_values = {}
            self._account_values_lock = threading.Lock()
            self._account_ready = threading.Event()
        try:
            self.reqAccountUpdates(True, account)        # subscribe=True
            logger.info("Subscribed to account updates for %s", account)
        except Exception as exc:
            logger.warning("reqAccountUpdates(True, %s) failed: %s", account, exc)

    def stop_account_updates(self, account: str):
        """Unsubscribe from streaming account updates."""
        try:
            self.reqAccountUpdates(False, account)
        except Exception:
            pass

    def get_account_values(self) -> Dict[str, Tuple[str, str]]:
        """Return a copy of the latest cached account values dict."""
        if not hasattr(self, '_account_values'):
            return {}
        with self._account_values_lock:
            return dict(self._account_values)

    # ── reqPnL — fast push of account-aggregate P&L ────────────────────────
    # Sub-second push cadence whenever IBKR re-marks the portfolio. Strictly
    # push: one subscribe call, then pnl() fires for every change. We log
    # every change directly from the callback — no polling.

    PNL_REQ_ID = 9_000_001                               # fixed, won't collide

    def pnl(self, reqId: int, dailyPnL: float,
            unrealizedPnL: float, realizedPnL: float):
        """TWS callback: realtime account-aggregate P&L push."""
        self._last_receive_at = time.time()    # 2026-05-26: control-channel liveness
        if not hasattr(self, '_pnl_lock'):
            self._pnl_lock = threading.Lock()
            self._pnl_data: Dict[str, float] = {}
        with self._pnl_lock:
            prev = dict(self._pnl_data)
            self._pnl_data = {
                "dailyPnL": dailyPnL,
                "unrealizedPnL": unrealizedPnL,
                "realizedPnL": realizedPnL,
            }
        # log only when at least one number actually moved
        if (prev.get("dailyPnL") != dailyPnL or
            prev.get("unrealizedPnL") != unrealizedPnL or
            prev.get("realizedPnL") != realizedPnL):
            logger.info("PNL  daily=%+.2f  realized=%+.2f  unrealized=%+.2f",
                        dailyPnL, realizedPnL, unrealizedPnL)

    def start_pnl_subscription(self, account: str):
        """Subscribe to streaming account-aggregate P&L push. Idempotent."""
        if not hasattr(self, '_pnl_lock'):
            self._pnl_lock = threading.Lock()
            self._pnl_data = {}
        try:
            self.reqPnL(self.PNL_REQ_ID, account, "")    # modelCode="" = default
            logger.info("Subscribed to reqPnL push for %s (reqId=%d)",
                        account, self.PNL_REQ_ID)
        except Exception as exc:
            logger.warning("reqPnL(%s) failed: %s", account, exc)

    def stop_pnl_subscription(self):
        """Cancel the streaming P&L subscription."""
        try:
            self.cancelPnL(self.PNL_REQ_ID)
        except Exception:
            pass

    def get_pnl(self) -> Dict[str, float]:
        """Return latest realtime P&L snapshot from reqPnL push."""
        if not hasattr(self, '_pnl_data'):
            return {}
        with self._pnl_lock:
            return dict(self._pnl_data)

    def get_positions_snapshot(self, timeout: float = 5.0) -> Tuple[Dict[str, Dict], bool]:
        """
        Request a fresh position snapshot from TWS.

        Returns (positions, fresh):
          positions — dict: symbol → {side, qty, avg, conid}
          fresh     — True ONLY if positionEnd() actually fired within the
                      timeout. False means the request TIMED OUT (e.g. the
                      receive socket is dead) and `positions` is unreliable —
                      it may be empty simply because no reply came back, NOT
                      because the account is flat. Callers making a
                      release / clear / flatten decision MUST treat fresh=False
                      as 'position UNKNOWN', never as 'flat'. (2026-05-26)

        FIX 2b: Guards against rapid repeat calls — if a request is already
        in flight, waits for it to complete rather than sending another.
        """
        # ── SHADOW BRANCH (2026-08-06) ───────────────────────────────────────
        # IBKR holds no shadow position, so reqPositions would report flat and
        # the reconciler would "flatten" trades that only exist locally. Serve
        # the shadow book instead — always fresh=True, because it is authoritative.
        if SHADOW_ON and SHADOW is not None:
            return SHADOW.positions_snapshot()

        if not hasattr(self, '_positions'):
            self._positions       = {}
            self._positions_lock  = threading.Lock()
            self._positions_ready = threading.Event()
            self._pos_req_lock    = threading.Lock()    # Prevents concurrent reqPositions

        # Only one reqPositions request at a time
        if not self._pos_req_lock.acquire(blocking=False):
            # Another thread already fetching — wait for its result
            fresh = self._positions_ready.wait(timeout=timeout)
            with self._positions_lock:
                return dict(self._positions), fresh

        try:
            self._positions_ready.clear()
            with self._positions_lock:
                self._positions.clear()
            self.reqPositions()                          # One-shot — positionEnd cancels it
            fresh = self._positions_ready.wait(timeout=timeout)
            with self._positions_lock:
                return dict(self._positions), fresh
        finally:
            self._pos_req_lock.release()

    def resolve_conid(self, symbol: str, exchange: str = "SMART",
                      currency: str = "USD", timeout: float = 5.0) -> Optional[int]:
        """
        Resolve a symbol string to its IBKR conid via reqMatchingSymbols().
        Returns the conid int or None if not found within timeout.
        Thread-safe: uses a per-call lock so concurrent resolutions don't clobber each other.
        """
        # FIX 3: per-call lock prevents concurrent resolve_conid() calls
        # from overwriting _pending_symbol / _conid_event each other
        if not hasattr(self, '_resolve_lock'):
            self._resolve_lock = threading.Lock()
        with self._resolve_lock:
            req_id = self.get_next_order_id()           # Unique reqId per call
            self._conid_result: Optional[int] = None
            self._conid_event = threading.Event()
            self._conid_event.clear()
            self._pending_symbol = symbol
            self.reqMatchingSymbols(req_id, symbol)     # TWS EClient call
            self._conid_event.wait(timeout=timeout)
            return self._conid_result

    def symbolSamples(self, reqId: int, contractDescriptions):
        """
        TWS callback for reqMatchingSymbols().
        Picks the first US stock match.
        """
        for cd in contractDescriptions:
            c = cd.contract
            if (c.symbol == getattr(self, '_pending_symbol', '') and
                    c.secType == "STK" and c.currency == "USD"):
                self._conid_result = c.conId
                break
        self._conid_event.set()                         # Signal result ready

    # ── Live orders (replaces CP API get_live_orders) ─────────────────────

    def get_live_orders_tws(self, timeout: float = 5.0) -> list:
        """
        Fetch all open orders from IB Gateway via reqAllOpenOrders().
        Returns list of dicts: [{orderId, conid, symbol, orderType, side, qty}, ...]
        Replaces CP API _api.get_live_orders() entirely.
        """
        # ── SHADOW BRANCH (2026-08-06) — working orders live in the local book ──
        if SHADOW_ON and SHADOW is not None:
            return SHADOW.live_orders()

        if not hasattr(self, '_live_orders_lock'):
            self._live_orders_lock  = threading.Lock()
            self._live_orders_event = threading.Event()
            self._live_orders_buf   = []

        with self._live_orders_lock:
            self._live_orders_buf   = []
            self._live_orders_event.clear()
            self._live_orders_done  = False

        self.reqAllOpenOrders()                          # TWS pushes all open orders

        # Wait for openOrderEnd() callback
        self._live_orders_event.wait(timeout=timeout)

        with self._live_orders_lock:
            return list(self._live_orders_buf)

    def openOrder(self, orderId: int, contract, order, orderState):
        """TWS callback: fires once per open order from reqAllOpenOrders."""
        if not hasattr(self, '_live_orders_buf'):
            return
        with self._live_orders_lock:
            self._live_orders_buf.append({
                "orderId"   : orderId,
                "order_id"  : str(orderId),
                "conid"     : contract.conId,
                "symbol"    : contract.symbol,
                "orderType" : order.orderType,
                "side"      : order.action,
                "qty"       : order.totalQuantity,
                "order_ref" : order.orderRef,
                "local_order_id": order.orderRef,
            })

    def openOrderEnd(self):
        """TWS callback: fires after all openOrder() callbacks are delivered."""
        if hasattr(self, '_live_orders_event'):
            self._live_orders_event.set()

    # ── Historical data (replaces CP API _get for seed bars) ─────────────

    def _pace_historical(self):
        """2026-06-11: throttle EVERY historical-data request (startup seeds,
        reseeds, failed-seed retries, and entry/exit/index keepUpToDate stream
        subscribes) to <= 1 per HIST_MIN_INTERVAL seconds. A (re)start's seed
        burst otherwise fires ~85-100 reqHistoricalData in ~2 min, breaching
        IBKR's ~60-per-10-min pacing limit → HMDS error 162 → the whole API
        receive gets throttled (the recurring 'RECEIVE CHANNEL DEAD' flap).
        Default 13s ≈ 46 req / 10 min, safely under the limit. Shared by both
        get_historical_bars() and reqHistoricalDataStream()."""
        import time as _t
        if not hasattr(self, '_hist_pace_lock'):
            self._hist_pace_lock = threading.Lock()
            self._hist_last_req_at = 0.0
            self._hist_req_times = []          # 2026-06-11: monotonic ts in the rolling window
            try:
                self._hist_min_interval = float(os.getenv("HIST_MIN_INTERVAL", "13"))
            except (TypeError, ValueError):
                self._hist_min_interval = 13.0
            # 2026-06-11: HARD cap — never more than N historical requests per
            # window (default 50 / 600s = under IBKR's ~60/10min), regardless of
            # timing or symbol count. This GUARANTEES the pacing limit can't be
            # breached even on a big restart, so the 162-throttle flap can't recur.
            try:
                self._hist_max_per_window = int(os.getenv("HIST_MAX_PER_WINDOW", "50"))
            except (TypeError, ValueError):
                self._hist_max_per_window = 50
            self._hist_window_sec = 600.0
        with self._hist_pace_lock:
            now = _t.monotonic()
            # (a) min-interval spacing between consecutive requests
            wait = (self._hist_last_req_at + self._hist_min_interval) - now
            if wait > 0:
                _t.sleep(wait)
                now = _t.monotonic()
            # (b) HARD rolling-window cap: if the window is full, hold until the
            #     oldest request ages out so we never exceed the per-window limit.
            cutoff = now - self._hist_window_sec
            self._hist_req_times = [t for t in self._hist_req_times if t > cutoff]
            if len(self._hist_req_times) >= self._hist_max_per_window:
                wait2 = (self._hist_req_times[0] + self._hist_window_sec) - now + 0.1
                if wait2 > 0:
                    logger.warning("Historical pacing CAP reached (%d in %ds) — holding %.0fs "
                                   "to stay under IBKR's limit",
                                   len(self._hist_req_times), int(self._hist_window_sec), wait2)
                    _t.sleep(wait2)
                    now = _t.monotonic()
                    cutoff = now - self._hist_window_sec
                    self._hist_req_times = [t for t in self._hist_req_times if t > cutoff]
            self._hist_last_req_at = now
            self._hist_req_times.append(now)

    def get_historical_bars(self, conid: int, period: str = "1 D",
                             bar_size: str = "30 secs",
                             timeout: float = 30.0) -> list:
        """
        Fetch historical bars via reqHistoricalData().
        Returns list of bar dicts: [{open, high, low, close, volume, date}, ...]
        Replaces CP API /iserver/marketdata/history endpoint.

        period:   TWS duration string e.g. "1 D", "2 D", "1 W"
        bar_size: TWS bar size string e.g. "30 secs", "5 secs", "1 min"
        """
        from ibapi.contract import Contract as _C
        import datetime

        if not hasattr(self, '_hist_lock'):
            self._hist_lock  = threading.Lock()
            self._hist_event = threading.Event()
            self._hist_buf   = []
            self._hist_reqid = threading.local()
        if not hasattr(self, '_hist_serial_lock'):
            # FIX: serialize concurrent reqHistoricalData calls. Without this,
            # parallel pipeline reseeds collide — _pending_hist_reqid only
            # tracks ONE request, so the second caller stomps the first and
            # the first's bars are silently dropped by the historicalData
            # callback. _hist_serial_lock is SEPARATE from _hist_lock (the
            # buffer mutex) to avoid deadlock with the callback.
            self._hist_serial_lock = threading.Lock()

        # Serialize the ENTIRE request lifecycle (set, send, wait, read).
        with self._hist_serial_lock:
            self._pace_historical()          # 2026-06-11: respect IBKR historical pacing
            req_id = self.get_next_order_id()

            with self._hist_lock:
                self._hist_buf   = []
                self._hist_event.clear()
                self._pending_hist_reqid = req_id

            c = _C()
            c.conId    = conid
            c.secType  = "STK"
            c.exchange = "SMART"
            c.currency = "USD"

            end_dt = ""                                      # Empty = now
            self.reqHistoricalData(
                req_id, c, end_dt, period, bar_size,
                "TRADES", 1, 1, False, []
            )

            self._hist_event.wait(timeout=timeout)

            with self._hist_lock:
                return list(self._hist_buf)

    def historicalData(self, reqId: int, bar):
        """TWS callback: fires once per historical bar."""
        # 2026-07-10: EVERY historical bar received is proof the API channel is
        # alive — stamp liveness BEFORE the guards. Startup seeds ~20 pipes ×200
        # bars each; those thousands of historicalData messages queue ahead of
        # the currentTime/tick replies on the single dispatch thread. Without
        # this stamp, the dispatch thread spends >60s draining the seed backlog
        # while the liveness clock (only ticked by ticks/updates) goes stale →
        # false 'RECEIVE CHANNEL DEAD' + entry halt during every startup. Counting
        # seed bars as liveness makes the received data actually keep us "alive".
        self._last_receive_at = time.time()
        if not hasattr(self, '_hist_buf'):
            return
        if getattr(self, '_pending_hist_reqid', None) != reqId:
            return
        with self._hist_lock:
            # Parse TWS date string to datetime for indicators.py compatibility
            # TWS format: "20260429  09:30:00" (with double space) or "20260429 09:30:00"
            from datetime import datetime as _dt
            from zoneinfo import ZoneInfo as _ZI
            try:
                _ts = _dt.strptime(bar.date.strip().replace("  ", " "), "%Y%m%d %H:%M:%S")
                _ts = _ts.replace(tzinfo=_ZI("America/New_York"))
            except Exception:
                _ts = _dt.now(_ZI("America/New_York"))
            self._hist_buf.append({
                "open"  : bar.open,
                "high"  : bar.high,
                "low"   : bar.low,
                "close" : bar.close,
                "volume": bar.volume,
                "date"  : bar.date,
                "ts"    : _ts,
            })

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """TWS callback: fires after all historicalData() callbacks."""
        self._last_receive_at = time.time()    # 2026-07-10: seed completion = liveness too
        if getattr(self, '_pending_hist_reqid', None) == reqId:
            if hasattr(self, '_hist_event'):
                self._hist_event.set()

    # ── Official-bar stream (keepUpToDate) — 2026-05-26 two-subscription model ──

    def reqHistoricalDataStream(self, reqId: int, symbol: str, contract,
                                bar_size: str = "3 mins", duration: str = "2 D",
                                use_rth: int = 1) -> None:
        """
        Subscribe to IBKR's OFFICIAL bars with keepUpToDate=True. IBKR first
        sends the initial history (ignored here — the engine is seeded by the
        separate seed() path), then streams the forming bar via
        historicalDataUpdate(). reqId must be in the dedicated >=20000 range.
        """
        with self._ref_lock:                                   # reuse a lock for the map write
            self._hist_stream_syms[reqId] = symbol
            self._hist_stream_forming.pop(reqId, None)
        self._pace_historical()            # 2026-06-11: respect IBKR historical pacing (shared with seeds)
        try:
            self.reqHistoricalData(reqId, contract, "", duration, bar_size,
                                   "TRADES", use_rth, 1, True, [])   # keepUpToDate=True
            logger.info("Official-bar stream: %s reqId=%d bar=%s (keepUpToDate)",
                        symbol, reqId, bar_size)
        except Exception as exc:
            logger.warning("reqHistoricalDataStream(%s) failed: %s", symbol, exc)

    def historicalDataUpdate(self, reqId: int, bar) -> None:
        """
        keepUpToDate live update: IBKR re-sends the CURRENTLY FORMING bar. When
        its date advances to a new period, the PREVIOUS bar is final and is
        committed via _on_official_bar(symbol, bar_dict, is_final=True).
        """
        self._last_receive_at = time.time()    # control-channel liveness
        sym = self._hist_stream_syms.get(reqId)
        if sym is None:
            return
        prev = self._hist_stream_forming.get(reqId)
        if prev is not None and prev[0] != bar.date:
            cb = self._on_official_bar
            if cb is not None:
                try:
                    cb(sym, self._bar_to_dict(prev[1]), True, reqId)   # commit completed bar (reqId → entry/exit stream)
                except Exception as exc:
                    logger.error("on_official_bar(%s) error: %s", sym, exc)
        self._hist_stream_forming[reqId] = (bar.date, bar)

    @staticmethod
    def _bar_to_dict(bar) -> Dict:
        """
        Convert an ibapi BarData into the dict shape IndicatorPipeline expects.
        current_time is set to now(EST): a just-completed live bar closed within
        the last couple of seconds, so wall-clock now is its effective close
        time — and this sidesteps the gateway returning bar.date in UTC.
        """
        return {
            "current_time": datetime.now(EST),
            # 2026-07-08: pass the broker bar timestamp through so macd_bot's 10s
            # BarAggregator can align 10-second bars into 5-min / 3-min windows.
            "date"  : getattr(bar, "date", None),
            "open"  : float(bar.open),  "high": float(bar.high),
            "low"   : float(bar.low),   "close": float(bar.close),
            "price" : float(bar.close),
            "volume": float(getattr(bar, "volume", 0) or 0),
            "daily_volume": 0,
        }





# =============================================================================
# DUAL-CHANNEL ORDER ENGINE — the main orchestrator
# =============================================================================

class TWSEngine:
    """
    Orchestrates order flow over the TWS / IB Gateway socket. (CP API
    fallback was removed; all orders go via TWS.)

    FIX 1: Bracket orders are pinned to a single channel — all legs go
           through the same channel in sequence, never split.
    FIX 2: TWS parentId resolved from string ref → integer orderId.
    FIX 3: Fill verification spawns a background thread per bracket.
    FIX 4: TWS watchdog thread reconnects on disconnect.
    FIX 6: Queue staleness check discards orders older than 30s.

    v3 additions:
    FIX A: TWS Phase 1 bracket uses transmit=False for atomic assembly.
    FIX B: Two-phase bracket: Phase 1 (entry+STP) at submission,
           Phase 2 (STP Market) after fill confirmation.
    FIX C: Cancel with 3x retry.
    FIX D: Placement semaphore(3) limits concurrent brackets.
    FIX E: Duplicate fill guard prevents double fill-verify threads.
    FIX F: Phase 2 uses actual filled qty, not ordered qty.
    FIX G: Fill verify receives dict snapshot, not mutable ref.
    FIX H: API Phase 1 batches parent+STP in one POST.
    FIX I: 50ms inter-order throttle for Phase 2 orders.
    """

    def __init__(self):
        # ── TWS client (the only channel — CP API fallback was removed) ───
        self._tws: Optional[TWSClient] = None            # TWS socket client
        self._on_reconnect: Optional[Callable] = None    # Called after watchdog reconnect

        # ── Rate tracking ─────────────────────────────────────────────────
        self._tws_rate = RateTracker(TWS_MAX_MSG_PER_SEC, TWS_SPILLOVER_THRESHOLD)  # TWS rate tracker

        # ── Priority queue for overflow ───────────────────────────────────
        self._queue: PriorityQueue = PriorityQueue()     # Thread-safe priority queue
        self._seq_counter = 0                            # Monotonic counter for FIFO within priority
        self._seq_lock = threading.Lock()                # Protects _seq_counter

        # ── Queue drain thread ────────────────────────────────────────────
        self._drain_thread: Optional[threading.Thread] = None  # Background drain thread
        self._stop_drain = threading.Event()             # Signal to stop the drain thread

        # ── FIX 4: TWS watchdog thread ────────────────────────────────────
        self._watchdog_thread: Optional[threading.Thread] = None  # Background watchdog thread
        self._stop_watchdog = threading.Event()          # Signal to stop the watchdog

        # ── Order tracking ────────────────────────────────────────────────
        # Maps our order_ref → {channel, channel_oid, ticket}
        self._placed_orders: Dict[str, Dict] = {}       # Track all placed orders
        self._placed_lock = threading.Lock()             # Protects _placed_orders

        # ── FIX D (B-26, B-33): Placement semaphore ──────────────────────
        # Limits concurrent bracket submissions to 3. When 13 brackets fire
        # simultaneously, this prevents overwhelming the gateway.
        # Acquired before Phase 1, released after Phase 2 completes (or fails).
        self._placement_sem = threading.Semaphore(3)     # FIX D: max 3 concurrent brackets

        # ── FIX E (B-30): Duplicate fill guard ────────────────────────────
        # Prevents spawning duplicate fill verification threads for the same symbol.
        # Check before spawning; add on start; remove on completion.
        self._filling_syms: set = set()                  # FIX E: symbols currently being fill-verified
        self._filling_lock = threading.Lock()            # FIX E: protects _filling_syms

        # ── Per-symbol order tracking ─────────────────────────────────────
        # Tracks ALL live order IDs for each symbol. Used by stop management
        # to cancel related orders when any stop hits.
        # Key: symbol → Dict with keys: parent, wide_stp, stp_mkt, stop_price, stop_lmt
        # Values are order ID strings (e.g., "TWS_500" or "API_123") or None.
        self._symbol_orders: Dict[str, Dict[str, Optional[str]]] = {}  # Per-symbol order ID map
        self._symbol_orders_lock = threading.Lock()      # Protects _symbol_orders

        # Position-clear hook: fired when a stop-type order (stp_mkt /
        # stop_price / stop_lmt / wide_stp) for a tracked symbol fills in IBKR.
        # Set by the bot via set_position_clear_callback() so it can keep its
        # internal _open_positions dict in sync with IBKR reality.
        self._on_stop_filled: Optional[Callable[[str], None]] = None

        # Long-disconnect hook: fired when the TWS socket has been down for
        # ≥ LONG_DISCONNECT_SEC. Bot uses this to dump its own position state.
        self._on_long_disconnect: Optional[Callable[[float], None]] = None
        self._long_disconnect_sec = 150.0    # 2.5 min threshold

        # ── Position-reconciliation state ─────────────────────────────────
        # Tracks symbols that are currently being auto-flattened due to a
        # partial-fill gap (Issue 1) or a STOP_HIT divergence (Issue 2). The
        # bot consults is_pending_reconciliation(sym) before placing any new
        # entry — blocks re-entries until IBKR confirms position is flat.
        self._pending_reconciliation: set = set()
        self._pending_reconciliation_lock = threading.Lock()

        # 2026-05-22 (Fix #4): heartbeat thread to auto-clear stale reconcile
        # flags. If IBKR reports position=0 for a symbol that's still in the
        # pending set, the close completed but the reconcile thread already
        # gave up — unblock it here.
        self._reconcile_heartbeat_interval = 30.0   # seconds
        self._reconcile_heartbeat_started = False
        self._reconcile_heartbeat_lock = threading.Lock()

        # 2026-05-26 (P2 #8): single flatten authority per symbol. EoD square-
        # off, partial-fill-gap auto-flatten and the direction monitor can all
        # fire a close on the same symbol at once (the 15:51 over-cover storm).
        # A symbol claimed here is being flattened by exactly one path; the
        # others skip until the reconcile thread releases it.
        self._flattening_syms: set = set()
        self._flattening_lock = threading.Lock()

        # 2026-05-26 (P2 #7): suppress the partial-fill-gap detector until this
        # wall-clock time. Set during EoD: a reqGlobalCancel turns every OCA
        # sibling terminal with filled=0, which is NOT a real gap. Without this
        # the cancel storm tripped a gap auto-flatten for every open symbol on
        # top of the EoD MKT closes.
        self._suppress_gap_until: float = 0.0

        # Per-symbol auxiliary state for partial-fill detection. Populated
        # when Phase 2 completes; cleared in handle_stop_hit. Tracks:
        #   "position_qty"        — total shares of the open position
        #   "siblings"            — { oid: {"key": str, "filled": int,
        #                                   "remaining": int, "status": str} }
        #   "gap_handled"         — idempotent flag, prevents double-flatten
        self._oca_tracking: Dict[str, Dict] = {}
        self._oca_tracking_lock = threading.Lock()

        # Receive-health: timestamp of last successful order placement.
        # Bot reads this + TWSClient._last_order_callback_at to detect a
        # stuck receive socket and refuse new entries.
        self._last_order_placed_at: float = 0.0

        # ── Statistics ────────────────────────────────────────────────────
        self._stats = {
            "tws_placed": 0,                             # Orders successfully placed via TWS
            "queued": 0,                                 # Orders that went through the queue
            "queue_drained_tws": 0,                      # Queued orders drained via TWS
            "queue_stale_discarded": 0,                  # FIX 6: stale orders discarded
            "fill_verified": 0,                          # FIX 3: brackets with confirmed fills
            "fill_cancelled": 0,                         # FIX 3: brackets cancelled due to no fill
            "tws_reconnects": 0,                         # FIX 4: successful TWS reconnections
            "failed": 0,                                 # Orders that failed (TWS unavailable)
        }
        self._stats_lock = threading.Lock()              # Protects _stats

    # ── Initialization ────────────────────────────────────────────────────

    def start(self) -> bool:
        """
        Initialize both channels and start background threads.
        Returns True if at least one channel is available.
        """
        tws_ok = False                                   # TWS connection status
        # Try to connect TWS (IB Gateway — only channel)
        if TWS_AVAILABLE:                                # ibapi package installed
            try:
                self._tws = TWSClient()                  # Create TWS client instance
                tws_ok = self._tws.connect_and_start()   # Connect and start event loop
                if tws_ok:
                    # Hook engine into TWS orderStatus so we can detect stop fills
                    # and notify the bot (see _on_order_status_event).
                    self._tws._engine_on_stop_callback = self._on_order_status_event
                    # SHADOW (2026-08-06): bind the local fill simulator to this
                    # client so its synthetic orderStatus/FILL callbacks reach the
                    # engine exactly as the real ones would, and start its engine.
                    if SHADOW_ON and SHADOW is not None:
                        SHADOW.attach(self._tws)
                    # Subscribe to streaming account values (NetLiquidation,
                    # TotalCashValue, AvailableFunds, ...) — these push at
                    # IBKR's natural slower cadence (~3 min when stable).
                    try:
                        self._tws.start_account_updates(ACCOUNT_ID)
                    except Exception as exc:
                        logger.warning("start_account_updates failed: %s", exc)
                    # Subscribe to fast push of account-aggregate P&L
                    # (sub-second cadence on MTM changes). pnl() callback
                    # logs each push directly — strictly push, no polling.
                    try:
                        self._tws.start_pnl_subscription(ACCOUNT_ID)
                    except Exception as exc:
                        logger.warning("start_pnl_subscription failed: %s", exc)
                    logger.info("TWS channel: ONLINE (primary)")
                else:
                    logger.warning("TWS channel: FAILED to connect")
                    self._tws = None
            except Exception as exc:
                logger.error("TWS initialization error: %s", exc)
                self._tws = None
        else:
            logger.info("TWS channel: UNAVAILABLE (ibapi not installed)")

        if not tws_ok:
            logger.error("FATAL: TWS (IB Gateway) not available — cannot trade. "
                         "Ensure IB Gateway is running on port %d", TWS_PORT)
            return False

        # Start the queue drain thread — processes overflow orders
        self._start_drain_thread()

        # Start the TWS watchdog thread — reconnects on disconnect
        if self._tws is not None:
            self._start_watchdog_thread()

        logger.info("TWSEngine started: TWS=ON (IB Gateway only)")
        return True                                      # At least one channel available

    def stop(self):
        """Shut down all background threads and dump diagnostic state."""
        # Dump full engine state BEFORE tearing things down — captures the
        # picture as it was at shutdown moment (orders, queue, stats, ...).
        try:
            self._dump_state(reason="shutdown")
        except Exception as exc:
            logger.error("Engine state dump on shutdown failed: %s", exc)

        # Stop drain thread
        self._stop_drain.set()                           # Signal drain thread to exit
        if self._drain_thread and self._drain_thread.is_alive():  # Thread is running
            self._drain_thread.join(timeout=5)           # Wait up to 5s for clean exit

        # FIX 4: Stop watchdog thread
        self._stop_watchdog.set()                        # Signal watchdog to exit
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=5)

        # Stop direction monitor thread
        self.stop_direction_monitor()

        # Disconnect TWS
        if self._tws and self._tws.is_connected:
            self._tws.disconnect()
            logger.info("TWS disconnected")

        logger.info("TWSEngine stopped. Stats: %s", self._stats)  # Log final stats

    # ── FIX 4: TWS watchdog thread ────────────────────────────────────────

    def _start_watchdog_thread(self):
        """
        FIX 4: Start a background thread that monitors TWS connection health.
        On disconnect, attempts reconnection with exponential backoff:
          5s → 10s → 20s → 40s → 60s (capped)
        """
        self._stop_watchdog.clear()                      # Reset stop signal

        def _watchdog_loop():
            backoff = 5                                  # Start with 5 second delay
            max_backoff = 60                             # Cap at 60 seconds

            # Long-disconnect tracking — when did the current outage start,
            # and have we already emitted the state-dump for this episode?
            disconnect_start_ts: Optional[float] = None
            long_dump_done = False

            while not self._stop_watchdog.is_set():      # Loop until stop signal
                self._stop_watchdog.wait(10)             # Check every 10 seconds
                if self._stop_watchdog.is_set():         # Check if stopped during sleep
                    break                                # Exit loop cleanly

                # If TWS is connected, reset backoff and continue
                if self._tws and self._tws.is_connected:  # TWS is healthy
                    if disconnect_start_ts is not None:
                        outage = time.time() - disconnect_start_ts
                        logger.warning("TWS watchdog: outage ended after %.1fs", outage)
                    disconnect_start_ts = None           # reset outage tracking
                    long_dump_done = False
                    backoff = 5                          # Reset backoff on healthy connection
                    continue                             # Keep monitoring

                # TWS is disconnected — attempt reconnection
                if self._tws is None:                    # TWS was never created (ibapi missing)
                    continue                             # Nothing to reconnect

                # Outage tracking: stamp first detection + dump state once at threshold
                if disconnect_start_ts is None:
                    disconnect_start_ts = time.time()
                outage_sec = time.time() - disconnect_start_ts
                if outage_sec >= self._long_disconnect_sec and not long_dump_done:
                    logger.error("TWS DISCONNECTED for %.1fs (>= %.0fs threshold) — "
                                 "dumping diagnostic state",
                                 outage_sec, self._long_disconnect_sec)
                    try:
                        self._dump_state(reason=f"long_disconnect_{int(outage_sec)}s")
                    except Exception as exc:
                        logger.error("State dump on long disconnect failed: %s", exc)
                    if self._on_long_disconnect is not None:
                        try:
                            self._on_long_disconnect(outage_sec)
                        except Exception as exc:
                            logger.error("on_long_disconnect callback error: %s", exc)
                    long_dump_done = True                # only dump once per outage

                logger.warning("TWS watchdog: connection lost — attempting reconnect "
                               "in %ds...", backoff)     # Log reconnect plan
                self._stop_watchdog.wait(backoff)        # Wait before reconnect attempt
                if self._stop_watchdog.is_set():         # Check if stopped during wait
                    break                                # Exit loop cleanly

                try:
                    # Create a fresh TWSClient instance for clean reconnection
                    # The old socket is dead — reusing it causes protocol errors
                    # FIX: ensure old socket is fully closed before new connect
                    # Error 326 (clientId in use) happens when old socket lingers
                    try:
                        self._tws.disconnect()           # Force close old socket
                    except Exception:
                        pass
                    time.sleep(2)                        # Give IB Gateway 2s to release clientId
                    new_tws = TWSClient()                # Fresh client instance
                    success = new_tws.connect_and_start(
                        host=self._tws._host,            # Reuse saved host
                        port=self._tws._port,            # Reuse saved port
                        client_id=self._tws._client_id,  # Reuse saved client ID
                    )
                    if success:                          # Reconnection succeeded
                        # Transfer the ref mapping from old client to new one
                        # so bracket children can still resolve parent IDs
                        with self._tws._ref_lock:        # Lock old client's ref map
                            old_refs = dict(self._tws._ref_to_tws_id)  # Copy all refs
                        with new_tws._ref_lock:          # Lock new client's ref map
                            new_tws._ref_to_tws_id.update(old_refs)  # Transfer refs

                        self._tws = new_tws              # Swap in the new client
                        # Re-attach engine status hook so stop-fill detection
                        # survives a gateway reconnect.
                        self._tws._engine_on_stop_callback = self._on_order_status_event
                        # Re-subscribe to account-value stream — subscriptions
                        # are tied to the socket, lost on reconnect.
                        try:
                            self._tws.start_account_updates(ACCOUNT_ID)
                        except Exception as exc:
                            logger.warning("start_account_updates after reconnect failed: %s", exc)
                        try:
                            self._tws.start_pnl_subscription(ACCOUNT_ID)
                        except Exception as exc:
                            logger.warning("start_pnl_subscription after reconnect failed: %s", exc)
                        backoff = 5                      # Reset backoff on success
                        with self._stats_lock:           # Thread-safe stats update
                            self._stats["tws_reconnects"] += 1  # Increment reconnect counter
                        logger.info("TWS watchdog: reconnected successfully "
                                    "(total reconnects: %d)",
                                    self._stats["tws_reconnects"])  # Log success

                        # FIX: invoke registered callback so macd_bot re-issues
                        # market-data subscriptions (reqMktData/reqTickByTickData)
                        # on the new socket. Without this, the socket is alive
                        # but no ticks flow → pipelines starve → bot freezes
                        # silently after every watchdog reconnect.
                        if self._on_reconnect is not None:
                            try:
                                self._on_reconnect(new_tws)
                            except Exception as exc:
                                logger.error("Reconnect callback failed: %s", exc)
                    else:                                # Reconnection failed
                        # Increase backoff (exponential, capped at max_backoff)
                        backoff = min(backoff * 2, max_backoff)  # Double backoff
                        logger.warning("TWS watchdog: reconnect failed — "
                                       "next attempt in %ds", backoff)  # Log failure
                except Exception as exc:
                    backoff = min(backoff * 2, max_backoff)  # Double backoff on exception
                    logger.error("TWS watchdog: reconnect exception: %s — "
                                 "next attempt in %ds", exc, backoff)  # Log exception

        self._watchdog_thread = threading.Thread(
            target=_watchdog_loop,                       # Run the watchdog loop
            daemon=True,                                 # Dies when main thread exits
            name="tws-watchdog",                         # Descriptive thread name
        )
        self._watchdog_thread.start()                    # Begin health monitoring
        logger.info("TWS watchdog thread started (10s health check interval)")  # Log thread start

    # ── Order submission (main entry point) ───────────────────────────────

    def submit_order(self, ticket: OrderTicket) -> str:
        """
        Submit an order via TWS (IB Gateway — only channel).
        Returns order ID string on success, "QUEUED" if TWS is saturated,
        or "FAILED" if TWS is unavailable.
        """
        tws_ok = self._tws and self._tws.is_connected

        if tws_ok and not self._tws_rate.is_at_limit():  # TWS available and below hard limit
            result = self._send_via_tws(ticket)           # Send via TWS
            if result:
                return result

        if tws_ok:                                        # TWS connected but at limit — queue
            self._queue.put(ticket)
            with self._stats_lock:
                self._stats["queued"] += 1
            logger.warning("QUEUED [pri=%s]: %s %s %d %s @ %.4f  (TWS_rate=%d)",
                           ticket.priority.name, ticket.side, ticket.symbol,
                           ticket.quantity, ticket.order_type, ticket.price,
                           self._tws_rate.current_rate())
            return "QUEUED"

        logger.error("FAILED [%s]: TWS not connected — cannot submit %s %d %s",
                     ticket.symbol, ticket.side, ticket.quantity, ticket.order_type)
        return "FAILED"


    def _send_via_tws(self, ticket: OrderTicket, transmit: bool = True) -> Optional[str]:
        """
        Attempt to place an order via TWS.
        Records the message in the rate tracker on success.
        Returns a prefixed order ID string or None on failure.

        FIX A: transmit parameter forwarded to TWSClient.place_order_ticket
        for atomic bracket assembly.
        """
        if not self._tws or not self._tws.is_connected:  # TWS not available
            return None                                  # Cannot place

        oid = self._tws.place_order_ticket(ticket, transmit=transmit)  # FIX A: pass transmit flag
        if oid is not None:                              # Placement succeeded
            self._tws_rate.record()                      # Count this message toward rate limit
            ticket._channel_used = "TWS"                 # Mark which channel was used
            result_id = f"TWS_{oid}"                     # Prefix for identification

            with self._placed_lock:                      # Thread-safe tracking update
                self._placed_orders[ticket.order_ref] = {
                    "channel": "TWS",                    # Record channel used
                    "channel_oid": oid,                  # Record channel-specific order ID
                    "ticket": ticket,                    # Record the ticket
                }

            with self._stats_lock:                       # Thread-safe stats update
                self._stats["tws_placed"] += 1           # Increment TWS placement counter

            # Stamp last-placement time for the bot's receive-health gate.
            self._last_order_placed_at = time.time()

            return result_id                             # Return prefixed order ID
        return None                                      # Placement failed


    def _pick_bracket_channel(self) -> str:
        """
        Always returns TWS. CP API removed — IB Gateway is the only channel.
        """
        if self._tws and self._tws.is_connected:
            return "TWS"
        logger.warning("_pick_bracket_channel: TWS not connected")
        return "TWS"  # Best effort — let TWS handle the error


    def _send_via_channel(self, ticket: OrderTicket, channel: str, transmit: bool = True) -> Optional[str]:
        """
        Send an order via TWS. CP API removed — only TWS channel exists.
        channel parameter kept for interface compatibility but always routes to TWS.
        """
        if self._tws and self._tws.is_connected:
            return self._send_via_tws(ticket, transmit=transmit)
        logger.error("[%s] _send_via_channel: TWS not connected — cannot send %s",
                     ticket.symbol, ticket.order_type)
        return None

    # ── Queue drain thread ────────────────────────────────────────────────

    def _start_drain_thread(self):
        """
        Start the background thread that drains queued orders.

        FIX 6: Before sending a queued order, checks if it's stale
        (older than QUEUE_MAX_AGE_SEC). Stale orders are discarded
        because their prices are no longer valid.
        """
        self._stop_drain.clear()                         # Reset stop signal

        def _drain_loop():
            while not self._stop_drain.is_set():         # Loop until stop signal
                self._stop_drain.wait(0.1)               # 100ms poll interval
                if self._stop_drain.is_set():            # Check if stopped during sleep
                    break                                # Exit loop cleanly
                if self._queue.empty():                  # No orders waiting
                    continue                             # Keep polling

                tws_ok = (self._tws is not None and      # TWS is up
                          self._tws.is_connected and     # TWS is connected
                          not self._tws_rate.is_near_limit())  # TWS has capacity
                if not tws_ok:                           # TWS still saturated — queue anyway
                    continue                             # Wait for capacity

                try:
                    ticket = self._queue.get_nowait()     # Non-blocking dequeue
                except Exception:                        # Queue was empty (race condition)
                    continue                             # Keep polling

                # FIX 6: Staleness check — discard orders older than QUEUE_MAX_AGE_SEC
                age = time.time() - ticket._created_at   # Calculate age in seconds
                if age > QUEUE_MAX_AGE_SEC:              # Order is stale
                    with self._stats_lock:               # Thread-safe stats update
                        self._stats["queue_stale_discarded"] += 1  # Increment discard counter
                    logger.warning("DRAIN DISCARDED (stale %.1fs): %s %s %d %s @ %.4f — "
                                   "price no longer valid",
                                   age, ticket.side, ticket.symbol, ticket.quantity,
                                   ticket.order_type, ticket.price)  # Log discard
                    continue                             # Drop the order — do NOT re-queue

                # Try TWS first (preferred), then API
                placed = False                           # Track if placement succeeded
                if tws_ok:                               # TWS has capacity
                    result = self._send_via_tws(ticket)  # Try TWS
                    if result:                           # TWS succeeded
                        placed = True                    # Mark as placed
                        with self._stats_lock:           # Thread-safe stats update
                            self._stats["queue_drained_tws"] += 1  # Increment TWS drain counter
                        logger.info("DRAIN via TWS [pri=%s]: %s %s → %s",
                                   ticket.priority.name, ticket.symbol,
                                   ticket.order_type, result)  # Log drain event

                if not placed:                           # TWS failed — re-queue
                    self._queue.put(ticket)
                    with self._stats_lock:
                        self._stats["failed"] += 1
                    logger.warning("DRAIN failed for %s %s — re-queued",
                                   ticket.symbol, ticket.order_type)

        self._drain_thread = threading.Thread(
            target=_drain_loop,                          # Run the drain loop
            daemon=True,                                 # Dies when main thread exits
            name="order-drain",                          # Descriptive thread name
        )
        self._drain_thread.start()                       # Begin draining
        logger.info("Queue drain thread started (100ms poll interval)")  # Log thread start

    # ── Convenience methods ───────────────────────────────────────────────

    def submit_stop_market(self, symbol: str, conid: int, side: str,
                           quantity: int, stop_price: float,
                           order_ref: str = "", parent_id: str = "",
                           callback: Callable = None) -> Optional[str]:
        """Submit a Stop Market order with HIGHEST priority."""
        ticket = OrderTicket(
            priority   = OrderPriority.STP,              # Highest priority for loss protection
            seq        = 0,                              # Will be assigned in submit_order
            symbol     = symbol,                         # Ticker symbol
            conid      = conid,                          # IBKR contract ID
            side       = side,                           # "BUY" or "SELL"
            order_type = "STP",                          # Stop market order type
            quantity   = quantity,                        # Number of shares
            price      = stop_price,                     # Stop trigger price
            order_ref  = order_ref or f"STP_{symbol}_{int(time.time())}",  # Auto-generate ref if empty
            parent_id  = parent_id,                      # Parent order ref for bracket linkage
            callback   = callback,                       # Optional fill callback
            _created_at = time.time(),                   # FIX 6: stamp for staleness
        )
        return self.submit_order(ticket)                 # Route through dual-channel system

    def submit_stop_limit(self, symbol: str, conid: int, side: str,
                          quantity: int, stop_price: float, limit_price: float,
                          order_ref: str = "", parent_id: str = "",
                          callback: Callable = None) -> Optional[str]:
        """Submit a Stop Limit order with MEDIUM priority."""
        ticket = OrderTicket(
            priority   = OrderPriority.STP_LIMIT,        # Medium priority
            seq        = 0,                              # Will be assigned in submit_order
            symbol     = symbol,                         # Ticker symbol
            conid      = conid,                          # IBKR contract ID
            side       = side,                           # "BUY" or "SELL"
            order_type = "STP LMT",                      # Stop limit order type
            quantity   = quantity,                        # Number of shares
            price      = stop_price,                     # Stop trigger price
            aux_price  = limit_price,                    # Limit price (worst fill)
            order_ref  = order_ref or f"STPLMT_{symbol}_{int(time.time())}",  # Auto-generate ref
            parent_id  = parent_id,                      # Parent order ref for bracket linkage
            callback   = callback,                       # Optional fill callback
            _created_at = time.time(),                   # FIX 6: stamp for staleness
        )
        return self.submit_order(ticket)                 # Route through dual-channel system

    def submit_entry_limit(self, symbol: str, conid: int, side: str,
                           quantity: int, limit_price: float,
                           order_ref: str = "",
                           callback: Callable = None) -> Optional[str]:
        """Submit a new entry Limit order with LOWEST priority."""
        ticket = OrderTicket(
            priority   = OrderPriority.NEW,              # Lowest priority for entries
            seq        = 0,                              # Will be assigned in submit_order
            symbol     = symbol,                         # Ticker symbol
            conid      = conid,                          # IBKR contract ID
            side       = side,                           # "BUY" or "SELL"
            order_type = "LMT",                          # Limit order type
            quantity   = quantity,                        # Number of shares
            price      = limit_price,                    # Limit price
            order_ref  = order_ref or f"ENTRY_{symbol}_{int(time.time())}",  # Auto-generate ref
            callback   = callback,                       # Optional fill callback
            _created_at = time.time(),                   # FIX 6: stamp for staleness
        )
        return self.submit_order(ticket)                 # Route through dual-channel system

    def submit_market_close(self, symbol: str, conid: int, side: str,
                            quantity: int, order_ref: str = "",
                            callback: Callable = None) -> Optional[str]:
        """Submit a market close order with high priority (protective)."""
        ticket = OrderTicket(
            priority   = OrderPriority.STP,              # High priority — protective close
            seq        = 0,                              # Will be assigned in submit_order
            symbol     = symbol,                         # Ticker symbol
            conid      = conid,                          # IBKR contract ID
            side       = side,                           # "BUY" or "SELL"
            order_type = "MKT",                          # Market order type
            quantity   = quantity,                        # Number of shares
            order_ref  = order_ref or f"CLOSE_{symbol}_{int(time.time())}",  # Auto-generate ref
            callback   = callback,                       # Optional fill callback
            _created_at = time.time(),                   # FIX 6: stamp for staleness
        )
        return self.submit_order(ticket)                 # Route through dual-channel system

    def square_off_technical(self, symbol: str, conid: int, side: str,
                             quantity: int, order_ref: str = "") -> Optional[str]:
        """
        TECHNICAL (delta/gamma) exit close — CANCEL-FIRST, THEN MARKET CLOSE.
        (2026-05-30)

        Used by the fast-line momentum-reversal exit. Order matters:
          1. Cancel ALL working protective orders for this symbol (stop,
             stop-limit, wide-stop) via handle_stop_hit() — 3x retry + conid
             sweep — so no resting stop can fire AFTER we go flat.
          2. Brief pause to let the cancels register at IBKR.
          3. Send the MKT close for the live quantity.

        This prevents a fast reversion from triggering a stale stop right
        after the technical close, which would otherwise open a fresh
        (naked) position in the opposite direction.
        """
        logger.info("[%s] TECHNICAL square-off: cancelling protective orders "
                    "FIRST (side=%s qty=%d)", symbol, side, quantity)
        # 1. Cancel protective siblings first
        try:
            self.handle_stop_hit(symbol)
        except Exception as exc:
            logger.warning("[%s] cancel-before-close error (continuing to close): %s",
                           symbol, exc)
        # 2. Let cancels register before sending the close
        time.sleep(0.5)
        # 3. Now flatten at market — no live stop remains to double-fill
        return self.submit_market_close(
            symbol=symbol, conid=conid, side=side, quantity=quantity,
            order_ref=order_ref or f"TECH_{symbol}_{int(time.time())}",
        )

    # ══════════════════════════════════════════════════════════════════════
    # FIX B: TWO-PHASE BRACKET ORDER SUBMISSION
    # ══════════════════════════════════════════════════════════════════════
    #
    # ARCHITECTURE (learned from 51 production bugs):
    #
    # Phase 1 (AT ENTRY): Parent LMT + STP child — 2 orders only
    #   - On TWS: parent(transmit=False) + STP(transmit=True) → atomic
    #   - On API: single POST with 2 orders in the array (FIX H)
    #   - STP has parentId → IBKR holds it PreSubmitted until parent fills
    #   - If parent cancelled → STP auto-cancelled (no naked position)
    #
    # Phase 2 (AFTER FILL CONFIRMED): STP LMT + Target
    #   - Only placed after fill verification confirms fq > 0
    #   - Uses actual filled qty (fq), NOT ordered qty (FIX F: B-44, B-45)
    #   - NO parentId on these — parent is already filled (FIX B: B-42)
    #   - Each sent as individual order, not batched (B-18)
    #   - 50ms inter-order throttle (FIX I: B-02)
    #
    # FIX D: Semaphore(3) limits concurrent bracket submissions.
    # FIX E: _filling_syms prevents duplicate fill-verify threads.
    # FIX G: Fill verify receives dict(results) snapshot, not mutable ref.
    # ══════════════════════════════════════════════════════════════════════

    def submit_bracket(self, symbol: str, conid: int, direction: str,
                       quantity: int, entry_price: float,
                       stop_price: float, mpl: float,
                       wide_stp_multiplier: float = 1.1,
                       on_fill: Callable = None,
                       on_cancel: Callable = None,
                       ) -> Dict[str, Optional[str]]:
        """
        FIX B: Two-phase bracket order submission.

        Phase 1 (immediate): Parent LMT + Wide STP child — 2 orders only.
          Wide STP = entry ± (mpl * wide_stp_multiplier) — safety net until Phase 2.
        Phase 2 (after fill): STP Market at fill_price ± mpl — uses actual filled qty.
        No target order — exits handled by trailing stops and BE logic only.

        Args:
            mpl:                MPL dollar amount from config (max per-share loss).
            stop_price:         Actual STP Market price (entry ± mpl), used in Phase 2.
            wide_stp_multiplier: Multiplier for wide STP distance (default 1.1).
            on_fill:            Called with (symbol, filled_qty, avg_price) on confirmed fill.
            on_cancel:          Called with (symbol, reason) if bracket is cancelled (no fill).

        Returns dict: {parent, wide_stp, stp_mkt} → order IDs or None.
        stp_mkt is None at return time — placed in Phase 2 after fill.
        """
        ts = f"{int(time.time())}_{symbol}"              # Unique timestamp+symbol tag for order refs
        side = "BUY" if direction == "LONG" else "SELL"  # Entry side
        close_side = "SELL" if direction == "LONG" else "BUY"  # Exit side (opposite of entry)

        # Compute Wide STP price — wider than actual stop, acts as safety net during Phase 1
        if direction == "LONG":                           # Long trade: wide STP is below entry
            wide_stp_price = round(entry_price - (mpl * wide_stp_multiplier), 2)  # e.g., entry - 1.1*mpl (rounded to cents)
        else:                                            # Short trade: wide STP is above entry
            wide_stp_price = round(entry_price + (mpl * wide_stp_multiplier), 2)  # e.g., entry + 1.1*mpl (rounded to cents)

        # Initialize results dict — Phase 2 orders start as None
        # No target order — exits handled by trailing stops and BE logic only
        results: Dict[str, Optional[str]] = {
            "parent": None,                              # Will be set in Phase 1
            "wide_stp": None,                            # Will be set in Phase 1 (safety net)
            "stp_mkt": None,                             # Will be set in Phase 2 (actual stop)
        }

        # FIX 1: Pick ONE channel for all legs — never split a bracket
        channel = self._pick_bracket_channel()           # Decide channel once for all phases
        logger.info("[%s] Bracket pinned to channel: %s (two-phase architecture)", symbol, channel)  # Log channel choice

        # FIX D (B-26, B-33): Acquire placement semaphore — limits to 3 concurrent brackets
        # This is acquired before Phase 1 and released after Phase 2 completes (or fails).
        # The semaphore prevents 13+ simultaneous brackets from overwhelming the gateway.
        acquired = self._placement_sem.acquire(timeout=30)  # Wait up to 30s for a slot
        if not acquired:                                 # Could not acquire within 30s
            logger.error("[%s] Bracket ABORTED — placement semaphore timeout (3 brackets already in flight)",
                         symbol)                         # Log semaphore timeout
            return results                               # Return all-None results

        try:
            # ══════════════════════════════════════════════════════════════
            # PHASE 1: Parent LMT + STP child — 2 orders only
            # ══════════════════════════════════════════════════════════════

            parent_ref = f"PARENT_{ts}"                  # Unique ref for parent order
            wide_stp_ref = f"WIDESTP_{ts}"               # Unique ref for wide stop order

            # Build parent ticket
            # FIX: config-driven entry type (LMT vs MKT). MKT guarantees fill
            # but pays the spread; LMT is passive and may miss on momentum.
            _entry_type = "LMT"
            try:
                import json as _json
                with open("config.json") as _f:
                    _entry_type = str(_json.load(_f).get("EntryOrderType", "LMT")).upper()
            except Exception:
                pass
            parent_ticket = OrderTicket(
                priority=OrderPriority.NEW,              # Entry order — lowest priority
                seq=0,                                   # Will be assigned below
                symbol=symbol,                           # Ticker symbol
                conid=conid,                             # IBKR contract ID
                side=side,                               # Entry side (BUY or SELL)
                order_type=_entry_type,                  # "LMT" or "MKT" (config-driven)
                quantity=quantity,                        # Number of shares
                price=(entry_price if _entry_type == "LMT" else 0.0),  # MKT ignores price
                tif="DAY",                               # Day order
                order_ref=parent_ref,                    # Unique reference for tracking
                _created_at=time.time(),                 # Timestamp for staleness
            )
            with self._seq_lock:                         # Thread-safe sequence assignment
                parent_ticket.seq = self._seq_counter    # Assign sequence number
                self._seq_counter += 1                   # Increment for next

            # Build Wide STP child ticket (with parentId for IBKR bracket linkage)
            # Wide STP is a safety net at 1.1x MPL — replaced by actual STP Market in Phase 2
            wide_stp_ticket = OrderTicket(
                priority=OrderPriority.STP,              # Highest priority — loss protection
                seq=0,                                   # Will be assigned below
                symbol=symbol,                           # Same ticker
                conid=conid,                             # Same contract
                side=close_side,                         # Exit side (opposite of entry)
                order_type="STP",                        # Stop market order
                quantity=quantity,                        # Same quantity as parent
                price=wide_stp_price,                    # Wide STP trigger price (1.1x MPL from entry)
                tif="DAY",                               # Day order
                parent_id=parent_ref,                    # FIX B: parentId links to parent for Phase 1
                order_ref=wide_stp_ref,                  # Unique reference for tracking
                _created_at=time.time(),                 # Timestamp for staleness
            )
            with self._seq_lock:                         # Thread-safe sequence assignment
                wide_stp_ticket.seq = self._seq_counter  # Assign sequence number
                self._seq_counter += 1                   # Increment for next

            # ── Place Phase 1 based on channel ────────────────────────────
            if channel == "TWS":
                # FIX A (B-46): TWS atomic bracket assembly
                # parent(transmit=False) + Wide STP(transmit=True)
                # IBKR TWS holds parent until last child has transmit=True,
                # then sends all orders atomically. This prevents children
                # from firing before parent is acknowledged.
                results["parent"] = self._send_via_channel(parent_ticket, channel, transmit=False)  # FIX A: hold parent
                if results["parent"]:                    # Parent accepted by TWS
                    results["wide_stp"] = self._send_via_channel(wide_stp_ticket, channel, transmit=True)  # FIX A: transmit=True triggers atomic send
                    if not results["wide_stp"]:          # Wide STP failed — cancel parent
                        logger.error("[%s] Phase 1 Wide STP failed on TWS — cancelling parent %s",
                                     symbol, results["parent"])  # Log failure
                        raw_pid = results["parent"].split("_", 1)[1] if "_" in results["parent"] else results["parent"]  # Extract raw ID
                        self._tws.cancel_order_by_id(int(raw_pid))  # Cancel orphaned parent
                        results["parent"] = None         # Clear parent since bracket is broken
                else:                                    # Parent failed
                    logger.error("[%s] Phase 1 parent failed on TWS", symbol)  # Log failure

            # ── Check Phase 1 success ─────────────────────────────────────
            if not results["parent"]:                    # Parent placement failed
                logger.error("[%s] Bracket ABORTED — Phase 1 parent placement failed on %s",
                             symbol, channel)            # Log abort
                return results                           # Return with all-None or partial results

            logger.info("[%s] Phase 1 submitted on %s: parent=%s  wide_stp=%s  wide_stp_price=%.4f",
                        symbol, channel, results["parent"], results["wide_stp"],
                        wide_stp_price)                  # Log Phase 1 success with wide STP price

            # ── Store order IDs in per-symbol tracker ─────────────────────
            with self._symbol_orders_lock:                # Thread-safe write to symbol orders
                self._symbol_orders[symbol] = {
                    "parent": results["parent"],         # Parent order ID
                    "wide_stp": results["wide_stp"],     # Wide STP order ID (safety net)
                    "stp_mkt": None,                     # Will be set in Phase 2
                    "stop_price": None,                  # Will be set in apply_breakeven
                    "stop_lmt": None,                    # Will be set in apply_breakeven
                }

            # ══════════════════════════════════════════════════════════════
            # SPAWN FILL VERIFICATION → Phase 2 placed after fill
            # ══════════════════════════════════════════════════════════════
            # FIX G: Pass dict(results) snapshot, not the mutable reference
            # FIX E: Duplicate fill guard checked inside _spawn_fill_verify
            self._spawn_fill_verify(
                symbol=symbol,                           # Ticker for logging and guard
                conid=conid,                             # Contract ID for Phase 2 orders
                channel=channel,                         # Pinned channel for all phases
                parent_oid=results["parent"],            # Parent order ID for fill check
                all_oids=dict(results),                  # FIX G: snapshot copy of results dict
                close_side=close_side,                   # Exit side for Phase 2 orders
                ordered_qty=quantity,                    # Original ordered quantity (for logging)
                direction=direction,                     # LONG or SHORT — needed for Phase 2 stop calc
                mpl=mpl,                                 # MPL dollar amount for Phase 2 STP Market price
                on_fill=on_fill,                         # Fill callback
                on_cancel=on_cancel,                     # Cancel callback
            )

        except Exception as exc:
            logger.error("[%s] Bracket submission exception: %s", symbol, exc)  # Log unexpected error
        finally:
            # FIX D: Semaphore is released inside _spawn_fill_verify after Phase 2
            # completes (or fails). If we never got to spawn fill verify (e.g., parent
            # failed), we must release here.
            if not results["parent"]:                    # Phase 1 failed — never spawned fill verify
                self._placement_sem.release()            # FIX D: release semaphore on early exit
                logger.debug("[%s] Placement semaphore released (Phase 1 failed)", symbol)  # Log release

        return results                                   # Return Phase 1 results (Phase 2 pending)

    # ── FIX B + FIX E + FIX G: Fill verification → Phase 2 ───────────────

    def _spawn_fill_verify(self, symbol: str, conid: int, channel: str,
                           parent_oid: str, all_oids: Dict[str, Optional[str]],
                           close_side: str, ordered_qty: int,
                           direction: str, mpl: float,
                           on_fill: Callable = None,
                           on_cancel: Callable = None):
        """
        FIX B: Spawn a background thread that:
          1. Verifies the parent order filled (polling for FILL_VERIFY_TIMEOUT seconds)
          2. On fill: places Phase 2 orders (STP Market only) using actual filled qty
          3. On timeout/cancel: cancels all bracket legs

        FIX E: Duplicate fill guard — if symbol already has a fill-verify thread,
        skip spawning another one. This prevents double Phase 2 placement.

        FIX F: Phase 2 orders use actual filled quantity (fq), NOT ordered_qty.
        FIX G: all_oids is already a dict snapshot (caller passes dict(results)).
        FIX D: Releases placement semaphore after Phase 2 completes (or fails).
        """
        # FIX E (B-30): Duplicate fill guard — check before spawning
        with self._filling_lock:                         # Thread-safe check-and-add
            if symbol in self._filling_syms:             # Already has a fill-verify thread
                logger.warning("[%s] Fill verify SKIPPED — already in progress (duplicate guard)",
                               symbol)                   # Log duplicate prevention
                self._placement_sem.release()            # FIX D: release sem since we won't run
                return                                   # Exit without spawning
            self._filling_syms.add(symbol)               # FIX E: mark symbol as being verified

        def _verify():
            try:
                _verify_timeout = _entry_fill_timeout(symbol)  # 2026-06-09: ~2 candles, not flat 60s
                deadline = time.time() + _verify_timeout  # Calculate timeout deadline
                logger.info("[%s] Fill verification started (max %ds = 2 candles, channel=%s, parent=%s)",
                            symbol, _verify_timeout, channel, parent_oid)  # Log start

                # Extract the raw order ID from the prefixed string "TWS_123" or "API_456"
                raw_parent_id = parent_oid.split("_", 1)[1] if "_" in parent_oid else parent_oid  # Strip channel prefix

                filled_qty = 0.0                         # Actual filled quantity (for FIX F)
                avg_price = 0.0                          # Average fill price
                fill_confirmed = False                   # Track if fill was confirmed
                parent_dead = False                      # Track if parent was cancelled/inactive

                # FIX H (B-50): wait for filled_qty to STABILISE before snapshotting,
                # otherwise Phase 2 stops are sized to a partial early-fill and the
                # remaining shares end up naked. Stable = 3 consecutive polls (~1.5s)
                # with no growth, OR filled_qty == ordered_qty (fully done), OR
                # parent status flips to a terminal "Filled".
                last_filled_qty = 0.0                    # Last observed filled qty
                stable_polls = 0                         # Consecutive polls with no growth
                STABLE_POLLS_REQUIRED = 3                # ~1.5s with default 0.5s interval

                # 2026-05-22 (Fix #1): parallel position-snapshot verification.
                # The status-callback path can stop flowing if receive is broken,
                # so we also poll IBKR's actual position. If a non-zero position
                # appears matching our direction, that's a confirmed silent fill.
                # Snapshot every POS_SNAPSHOT_EVERY polls (≈ every 5s with 0.5s
                # interval) to avoid hammering reqPositions.
                POS_SNAPSHOT_EVERY = 10
                poll_count         = 0
                pre_position       = 0
                try:
                    pre_position = int(self.get_actual_position(symbol))
                except Exception:
                    pre_position = 0

                while time.time() < deadline:            # Poll until timeout
                    time.sleep(FILL_VERIFY_INTERVAL)     # Poll interval — 0.5s for 5s bars, configurable
                    poll_count += 1

                    # 2026-05-22 (Fix #1): every ~5s, snapshot IBKR position.
                    # If the position increased by >= ordered_qty in our direction,
                    # the parent silently filled (receive callbacks not flowing).
                    if poll_count % POS_SNAPSHOT_EVERY == 0:
                        try:
                            cur_pos = int(self.get_actual_position(symbol))
                            delta   = cur_pos - pre_position
                            wanted  = ordered_qty if direction == "LONG" else -ordered_qty
                            # Sign-matching delta of at least ordered_qty
                            if (wanted > 0 and delta >= wanted) or \
                               (wanted < 0 and delta <= wanted):
                                if filled_qty == 0:
                                    filled_qty = float(abs(delta))
                                    # Synthesise avg_price = parent's order avg if we
                                    # have it from any partial status, else use the
                                    # ordered price proxy. Phase 2 stop will be sized
                                    # by mpl regardless.
                                    if avg_price == 0:
                                        try:
                                            sd = self._tws.get_order_status(int(raw_parent_id))
                                            avg_price = float(sd.get("avg_fill_price", 0)) \
                                                if sd else 0
                                        except Exception:
                                            pass
                                    logger.warning(
                                        "[%s] Fill DETECTED via position snapshot — "
                                        "pre=%d cur=%d delta=%d (receive-callback "
                                        "path missed it)",
                                        symbol, pre_position, cur_pos, delta)
                                    fill_confirmed = True
                                    break
                        except Exception as exc:
                            logger.debug("[%s] fill-verify position snapshot failed: %s",
                                         symbol, exc)

                    parent_status_str = ""               # Track terminal-Filled state

                    if channel == "TWS" and self._tws:   # Check via TWS orderStatus data
                        try:
                            status = self._tws.get_order_status(int(raw_parent_id))  # Look up status
                            if status:                   # Status exists for this order
                                st = str(status.get("status", "")).lower()  # Normalize status string
                                parent_status_str = st   # Save for stability check
                                # If cancelled/inactive by IBKR — stop waiting immediately
                                if "cancel" in st or "inactive" in st:  # Parent was killed by IBKR
                                    logger.warning("[%s] Parent %s was %s by IBKR — "
                                                   "cancelling bracket",
                                                   symbol, parent_oid, st.upper())  # Log cancellation
                                    parent_dead = True   # Mark parent as dead
                                    break                # Exit poll loop
                                filled_qty = float(status.get("filled", 0))  # Get filled quantity
                                avg_price = float(status.get("avg_fill_price", 0))  # Get average price
                        except (ValueError, TypeError):  # Parsing error
                            pass                         # Continue polling

                    # FIX H: stability gate — only confirm once fills have settled
                    if filled_qty > 0 and avg_price > 0:
                        # Fully filled OR broker reports terminal "filled" state → done
                        if filled_qty >= ordered_qty or parent_status_str == "filled":
                            fill_confirmed = True
                            break
                        # Same qty seen this poll? Increment stability counter.
                        if filled_qty == last_filled_qty:
                            stable_polls += 1
                            if stable_polls >= STABLE_POLLS_REQUIRED:
                                fill_confirmed = True
                                break
                        else:                            # Qty grew — reset stability counter
                            stable_polls = 0
                            last_filled_qty = filled_qty

                # ── Handle fill result ────────────────────────────────────
                # FIX H: also accept partial fills that timed out before stabilising —
                # the position IS open in IBKR, so Phase 2 must protect what was filled.
                if not fill_confirmed and filled_qty > 0 and avg_price > 0 and not parent_dead:
                    logger.warning("[%s] Fill verify timed out with partial fill (filled=%d/%d) — "
                                   "proceeding to Phase 2 to avoid naked position",
                                   symbol, int(filled_qty), ordered_qty)
                    fill_confirmed = True

                if fill_confirmed and filled_qty > 0:    # Parent filled successfully
                    # FIX F: Use actual filled quantity, NOT ordered quantity
                    fq = int(filled_qty)                  # Convert to int for order quantity
                    logger.info("[%s] Fill CONFIRMED: parent=%s  fq=%d (ordered=%d)  avg=%.4f",
                                symbol, parent_oid, fq, ordered_qty, avg_price)  # Log fill with both quantities
                    with self._stats_lock:                # Thread-safe stats update
                        self._stats["fill_verified"] += 1  # Increment fill counter

                    # FIX X: lock parent at fq — cancel its remaining unfilled qty
                    # so it cannot resume filling AFTER Phase 2 sized stops to fq.
                    # Without this, slow-routing parents continue filling past fq
                    # and the excess shares end up naked.
                    if fq < ordered_qty and not parent_dead and channel == "TWS" and self._tws:
                        try:
                            self._tws.cancel_order_by_id(int(raw_parent_id))  # cancel remaining
                            logger.info("[%s] Parent %s capped at fq=%d (cancelled remaining %d to "
                                        "prevent post-Phase-2 fills)",
                                        symbol, parent_oid, fq, ordered_qty - fq)
                            # 2026-08-04: WAIT FOR THE CANCEL TO ACTUALLY LAND
                            # before sizing Phase 2. The old code slept a fixed
                            # 0.3s and re-read once — far too short. On XLK
                            # (04-08 10:25) the cancel was sent at :17 and a
                            # further 100 shares filled at :21, i.e. 4s later,
                            # so Phase 2 sized the stop to 305 while the real
                            # position was 405 and 100 shares sat unprotected.
                            #
                            # Poll until the parent reaches a TERMINAL state
                            # (Cancelled/Filled/Inactive) or the budget expires,
                            # then size the stop from the FINAL filled qty.
                            # Waiting is safe: the Phase-1 WIDE stop is still
                            # live and IBKR keeps it sized to the parent's
                            # filled quantity, so the position stays protected
                            # for the whole wait.
                            _settle_budget = float(
                                os.getenv("PARENT_CAP_SETTLE_SEC", "8"))
                            _TERMINAL = ("cancelled", "filled", "inactive",
                                         "apicancelled")
                            _deadline = time.time() + _settle_budget
                            _stable_since = None
                            while time.time() < _deadline:
                                time.sleep(0.25)
                                try:
                                    status_after = self._tws.get_order_status(
                                        int(raw_parent_id))
                                except Exception:
                                    continue             # status read is best-effort
                                if not status_after:
                                    continue
                                new_filled = float(
                                    status_after.get("filled", filled_qty))
                                if new_filled > filled_qty:
                                    logger.info(
                                        "[%s] Race-fill captured during cancel: "
                                        "filled %d → %d", symbol,
                                        int(filled_qty), int(new_filled))
                                    filled_qty = new_filled
                                    avg_price = float(
                                        status_after.get("avg_fill_price",
                                                         avg_price))
                                    fq = int(filled_qty)
                                    _stable_since = None   # restart the settle
                                    continue
                                _st = str(status_after.get("status") or "").lower()
                                if _st in _TERMINAL:
                                    # terminal AND quantity unchanged for one
                                    # extra tick => no more fills can arrive
                                    if _stable_since is None:
                                        _stable_since = time.time()
                                    elif time.time() - _stable_since >= 0.5:
                                        break
                            else:
                                logger.warning(
                                    "[%s] Parent %s did not reach a terminal state "
                                    "within %.1fs of the cancel — sizing Phase 2 to "
                                    "fq=%d; a later fill could leave shares uncovered "
                                    "(watch the dashboard stop-coverage alert)",
                                    symbol, parent_oid, _settle_budget, fq)
                        except Exception as exc:
                            logger.warning("[%s] Parent cancel-remaining failed: %s — "
                                           "stop will only cover fq=%d", symbol, exc, fq)

                    # ══════════════════════════════════════════════════════
                    # PHASE 2: Check Wide STP, place STP Market (no target)
                    # ══════════════════════════════════════════════════════
                    self._place_phase2_orders(
                        symbol=symbol,                   # Ticker for logging
                        conid=conid,                     # Contract ID
                        channel=channel,                 # Pinned channel
                        close_side=close_side,           # Exit side
                        fq=fq,                           # FIX F: actual filled quantity
                        direction=direction,             # LONG or SHORT for stop price calc
                        mpl=mpl,                         # MPL dollar amount for STP Market
                        fill_price=avg_price,            # Actual fill price for STP Market calc
                        ts=parent_oid,                   # Use parent OID for unique refs
                    )

                    # Call on_fill callback
                    if on_fill:                          # Caller provided fill callback
                        try:
                            on_fill(symbol, fq, avg_price)  # FIX F: pass actual filled qty
                        except Exception as exc:
                            logger.error("[%s] on_fill callback error: %s", symbol, exc)  # Log callback error
                    return                               # Fill verified + Phase 2 placed — done

                # ── Timeout or parent cancelled — cancel all bracket legs ─
                reason = "parent_cancelled" if parent_dead else "no_fill_timeout"  # Determine reason

                # 2026-05-22 (Fix #2): SNAPSHOT-BEFORE-CANCEL.
                # Before issuing the cancel, query IBKR's actual position. If
                # the position moved by ordered_qty in our direction, the
                # parent silently filled and the receive callbacks just got
                # lost. Adopt the position via Phase 2 instead of cancelling.
                # Without this guard, the cancel kills the protective stop
                # leg while leaving a silently-filled position naked at IBKR —
                # the exact pattern that caused the PLTR / SMCI / GOOGL stack
                # incidents on 2026-05-21 and 2026-05-22.
                try:
                    cur_pos = int(self.get_actual_position(symbol))
                    delta   = cur_pos - pre_position
                    wanted  = ordered_qty if direction == "LONG" else -ordered_qty
                    if (wanted > 0 and delta >= wanted) or \
                       (wanted < 0 and delta <= wanted):
                        logger.warning(
                            "[%s] Pre-cancel snapshot shows silent fill — "
                            "pre=%d cur=%d delta=%d. ADOPTING position via "
                            "Phase 2 instead of cancelling.",
                            symbol, pre_position, cur_pos, delta)
                        fq = abs(delta)
                        # Use last-known avg_price if available, else best estimate
                        snap_avg = avg_price
                        if snap_avg == 0:
                            try:
                                sd = self._tws.get_order_status(int(raw_parent_id))
                                snap_avg = float(sd.get("avg_fill_price", 0)) \
                                    if sd else 0
                            except Exception:
                                snap_avg = 0
                        # If we still don't know the fill price, cancel anyway —
                        # Phase 2 needs a price to size the stop.
                        if snap_avg > 0:
                            with self._stats_lock:
                                self._stats["fill_verified"] += 1
                            self._place_phase2_orders(
                                symbol=symbol, conid=conid, channel=channel,
                                close_side=close_side, fq=fq,
                                direction=direction, mpl=mpl,
                                fill_price=snap_avg, ts=parent_oid,
                            )
                            if on_fill:
                                try:
                                    on_fill(symbol, fq, snap_avg)
                                except Exception as exc:
                                    logger.error("[%s] on_fill callback error: %s",
                                                 symbol, exc)
                            return  # Adopted — do NOT cancel
                        else:
                            # 2026-05-26 (P1 #5): silent fill, no price to size a
                            # stop. Do NOT fall through to cancel (that left the
                            # adopted shares NAKED — the PLTR pattern). Route to
                            # Phase 2; its fill_price<=0 guard flattens immediately.
                            logger.error(
                                "[%s] Silent fill (fq=%d) with unknown avg_price — "
                                "routing to Phase 2 emergency flatten instead of "
                                "leaving naked.", symbol, fq)
                            with self._stats_lock:
                                self._stats["fill_verified"] += 1
                            self._place_phase2_orders(
                                symbol=symbol, conid=conid, channel=channel,
                                close_side=close_side, fq=fq,
                                direction=direction, mpl=mpl,
                                fill_price=0.0, ts=parent_oid,
                            )
                            return  # handled — do NOT fall through to cancel
                except Exception as exc:
                    logger.warning("[%s] Pre-cancel snapshot failed: %s — "
                                   "proceeding with cancel", symbol, exc)

                logger.warning("[%s] Parent %s did NOT fill within %ds (reason=%s) — "
                               "cancelling all bracket legs to prevent phantom position",
                               symbol, parent_oid, FILL_VERIFY_TIMEOUT, reason)  # Log cancellation

                for label, oid_str in all_oids.items():  # Iterate all bracket legs
                    if not oid_str or oid_str == "QUEUED":  # Skip missing or queued orders
                        continue                         # Nothing to cancel
                    raw_id = oid_str.split("_", 1)[1] if "_" in oid_str else oid_str  # Strip prefix
                    try:
                        if channel == "TWS" and self._tws:  # Cancel via TWS
                            self._tws.cancel_order_by_id(int(raw_id))  # FIX C: 3x retry inside
                        logger.info("[%s] Cancelled %s order %s (no-fill cleanup)",
                                    symbol, label, oid_str)  # Log successful cancel
                    except Exception as exc:
                        logger.debug("[%s] Cancel %s %s failed (may already be gone): %s",
                                     symbol, label, oid_str, exc)  # Log cancel failure (may be normal)

                with self._stats_lock:                   # Thread-safe stats update
                    self._stats["fill_cancelled"] += 1   # Increment cancel counter
                if on_cancel:                            # Caller provided cancel callback
                    try:
                        on_cancel(symbol, reason)        # Invoke cancel callback
                    except Exception as exc:
                        logger.error("[%s] on_cancel callback error: %s", symbol, exc)  # Log callback error

            finally:
                # FIX E: Remove symbol from filling set on completion (success or failure)
                with self._filling_lock:                 # Thread-safe removal
                    self._filling_syms.discard(symbol)   # Remove symbol from guard set
                # FIX D: Release placement semaphore after Phase 2 completes (or fails)
                self._placement_sem.release()            # Allow next bracket to proceed
                logger.debug("[%s] Fill verify complete — semaphore released, fill guard cleared",
                             symbol)                     # Log cleanup

        # Launch the verification thread
        t = threading.Thread(
            target=_verify,                              # Run the verification loop
            daemon=True,                                 # Dies when main thread exits
            name=f"fill-verify-{symbol}",                # Descriptive thread name
        )
        t.start()                                        # Begin fill verification

    # ── FIX B: Phase 2 order placement (after fill) ──────────────────────

    def _place_phase2_orders(self, symbol: str, conid: int, channel: str,
                              close_side: str, fq: int,
                              direction: str, mpl: float,
                              fill_price: float, ts: str):
        """
        Phase 2 (after parent fill confirmed):
          1. Check if Wide STP already fired (position stopped out → abort)
          2. Place STP Market at fill_price ± mpl (standalone, no parentId)
          3. Cancel Wide STP (replaced by tighter STP Market)

        No target order — exits are managed by trailing stops and BE logic.
        All orders are standalone (no parentId) because parent is already filled.
        Uses actual filled qty (fq), NOT ordered qty (FIX F).
        """
        logger.info("[%s] Phase 2: checking Wide STP then placing STP_MKT with fq=%d (channel=%s)",
                     symbol, fq, channel)                # Log Phase 2 start

        # ── 0. Guard: never place a stop at a garbage price (2026-05-26, P1 #5) ──
        # A silent-fill adoption can reach Phase 2 with no real fill price. Sizing
        # the stop as fill_price ± mpl then yields a nonsensical (e.g. negative)
        # trigger, so the position would sit NAKED. A flat book beats an
        # unprotected one — flatten immediately and let reconciliation verify.
        if not fill_price or fill_price <= 0:
            logger.error("[%s] Phase 2: fill_price unknown (%.4f) — cannot size a "
                         "protective stop. FLATTENING %d shares via MKT close "
                         "instead of leaving the position naked.",
                         symbol, fill_price or 0.0, fq)
            try:
                self.mark_pending_reconciliation(symbol)
                self._ensure_reconcile_heartbeat()
                self.submit_market_close(symbol=symbol, conid=conid,
                                         side=close_side, quantity=int(fq))
                threading.Thread(
                    target=self._reconcile_until_flat, args=(symbol,),
                    kwargs={"conid": conid, "direction": direction},
                    daemon=True, name=f"reconcile-{symbol}").start()
            except Exception as _fe:
                logger.error("[%s] Phase 2 emergency flatten failed: %s", symbol, _fe)
                try:
                    from notifier import notify_critical
                    notify_critical(
                        "Naked position — manual flatten needed",
                        f"[{symbol}] ~{fq} shares with no fill price; auto MKT-close "
                        "failed. Flatten manually NOW.")
                except Exception:
                    pass
            return

        # ── 1. Check if Wide STP already fired ───────────────────────────
        # If Wide STP filled, position was stopped out before we could place Phase 2.
        wide_stp_oid = None                              # Will hold the Wide STP order ID string
        with self._symbol_orders_lock:                   # Thread-safe read of symbol orders
            sym_orders = self._symbol_orders.get(symbol, {})  # Get order map for this symbol
            wide_stp_oid = sym_orders.get("wide_stp")    # Get Wide STP order ID

        wide_stp_fired = False                           # Track if Wide STP filled
        if wide_stp_oid:                                 # Wide STP was placed in Phase 1
            raw_wide_id = wide_stp_oid.split("_", 1)[1] if "_" in wide_stp_oid else wide_stp_oid  # Strip channel prefix
            if channel == "TWS" and self._tws:           # Check via TWS orderStatus data
                try:
                    status = self._tws.get_order_status(int(raw_wide_id))  # Look up Wide STP status
                    if status:                           # Status exists for this order
                        st = str(status.get("status", "")).lower()  # Normalize status string
                        if "filled" in st:               # Wide STP has filled — position stopped out
                            wide_stp_fired = True        # Mark as fired
                except (ValueError, TypeError):          # Parsing error — assume not fired
                    pass                                 # Continue with Phase 2
        if wide_stp_fired:                               # Wide STP already triggered — position is flat
            logger.warning("[%s] Wide STP already fired — position stopped out before Phase 2. "
                           "Skipping all post-fill orders.", symbol)  # Log abort
            # Cancel any remaining orders for this symbol (parent is filled, wide_stp is filled)
            with self._symbol_orders_lock:               # Thread-safe read
                remaining = dict(self._symbol_orders.get(symbol, {}))  # Copy order map
            for label, oid_str in remaining.items():     # Iterate all tracked orders
                if not oid_str or label in ("parent", "wide_stp"):  # Skip filled orders and None
                    continue                             # Nothing to cancel
                self._cancel_order_safe(oid_str, channel, label, symbol)  # Cancel remaining order
            with self._symbol_orders_lock:               # Thread-safe clear
                self._symbol_orders.pop(symbol, None)    # Remove symbol from tracking
            return                                       # Abort Phase 2 — position already stopped out

        # ── 2. Place STP Market at fill_price ± mpl (standalone) ─────────
        if direction == "LONG":                          # Long trade: stop is below fill price
            stp_mkt_price = round(fill_price - mpl, 2)   # STP Market trigger = fill - mpl (rounded to cents)
        else:                                            # Short trade: stop is above fill price
            stp_mkt_price = round(fill_price + mpl, 2)   # STP Market trigger = fill + mpl (rounded to cents)

        stp_mkt_ref = f"STPMKT_{ts}_{int(time.time())}"  # Unique ref for STP Market
        # FIX Z1: assign a stable OCA group string for this position. STOP_PRICE
        # and STOP_LIMIT placed later by apply_breakeven will join this same group
        # so that any one of them filling auto-cancels the others on the broker side.
        oca_group = f"BRK_{symbol}_{ts}"
        stp_mkt_ticket = OrderTicket(
            priority=OrderPriority.STP,                  # Highest priority — loss protection
            seq=0,                                       # Will be assigned below
            symbol=symbol,                               # Ticker symbol
            conid=conid,                                 # IBKR contract ID
            side=close_side,                             # Exit side
            order_type="STP",                            # Stop market order type
            quantity=fq,                                 # FIX F: actual filled quantity, NOT ordered qty
            price=stp_mkt_price,                         # STP Market trigger at fill ± mpl
            tif="DAY",                                   # Day order
            parent_id="",                                # No parentId — parent already filled
            order_ref=stp_mkt_ref,                       # Unique reference
            oca_group=oca_group,                         # FIX Z1: link siblings on the broker
            oca_type=1,                                  # FIX Z1: cancel siblings when one fills
            _created_at=time.time(),                     # Timestamp for staleness
        )
        with self._seq_lock:                             # Thread-safe sequence assignment
            stp_mkt_ticket.seq = self._seq_counter       # Assign sequence number
            self._seq_counter += 1                       # Increment for next

        # Place STP Market via the pinned channel (standalone — transmit=True always)
        stp_mkt_result = self._send_via_channel(stp_mkt_ticket, channel, transmit=True)  # Standalone order
        if stp_mkt_result:                               # Placement succeeded
            logger.info("[%s] Phase 2 STP_MKT placed: %s @ %.4f (fq=%d)",
                        symbol, stp_mkt_result, stp_mkt_price, fq)  # Log success
        else:                                            # Placement failed
            logger.error("[%s] Phase 2 STP_MKT FAILED — keeping Wide STP alive as fallback!",
                         symbol)                         # Log failure — Wide STP remains as protection

        # ── 3. Cancel Wide STP (replaced by tighter STP Market) ──────────
        if stp_mkt_result and wide_stp_oid:              # STP Market placed OK — safe to cancel Wide STP
            cancelled = self._cancel_order_safe(wide_stp_oid, channel, "wide_stp", symbol)  # Cancel with retry
            if cancelled:                                # Wide STP cancelled successfully
                logger.info("[%s] Wide STP %s cancelled (replaced by STP_MKT %s)",
                            symbol, wide_stp_oid, stp_mkt_result)  # Log replacement
            else:                                        # Cancel failed — both stops may coexist briefly
                logger.warning("[%s] Wide STP %s cancel failed — may coexist with STP_MKT",
                               symbol, wide_stp_oid)     # Log warning
        elif not stp_mkt_result:                         # STP Market failed — keep Wide STP alive
            logger.warning("[%s] Keeping Wide STP %s alive — STP_MKT placement failed",
                           symbol, wide_stp_oid)         # Log fallback decision

        # ── 4. Update per-symbol order tracking with Phase 2 IDs ─────────
        # No target order — exits are handled entirely by trailing stops and BE logic
        with self._symbol_orders_lock:                   # Thread-safe update to symbol orders
            if symbol in self._symbol_orders:            # Symbol still tracked (should always be)
                self._symbol_orders[symbol]["stp_mkt"] = stp_mkt_result  # Store STP Market order ID
                # FIX Z1: remember OCA group so apply_breakeven can attach siblings.
                self._symbol_orders[symbol]["oca_group"] = oca_group
                # Clear wide_stp if it was cancelled successfully
                if stp_mkt_result and wide_stp_oid:      # STP Market replaced Wide STP
                    self._symbol_orders[symbol]["wide_stp"] = None  # Wide STP no longer active

        # Initialize OCA-tracking entry for partial-fill gap detection.
        # `direction` lets _auto_flatten_gap pick the correct close side.
        # `conid` is stored so auto-flatten can build the close contract
        # WITHOUT depending on _placed_orders (which handle_stop_hit clears
        # before auto-flatten gets a chance to read it).
        try:
            stp_mkt_oid_raw = (int(str(stp_mkt_result).split("_", 1)[1])
                               if stp_mkt_result and "_" in str(stp_mkt_result)
                               else int(stp_mkt_result) if stp_mkt_result else None)
        except (ValueError, IndexError):
            stp_mkt_oid_raw = None
        with self._oca_tracking_lock:
            self._oca_tracking[symbol] = {
                "position_qty": int(fq),
                "direction":    direction,
                "conid":        int(conid),
                "siblings":     {},
                "gap_handled":  False,
            }
            if stp_mkt_oid_raw is not None:
                self._oca_tracking[symbol]["siblings"][stp_mkt_oid_raw] = {
                    "key": "stp_mkt", "filled": 0, "remaining": int(fq),
                    "status": "Submitted",
                }

        logger.info("[%s] Phase 2 complete: stp_mkt=%s  fq=%d  (no target — exits via stops/BE only)",
                     symbol, stp_mkt_result, fq)          # Log Phase 2 summary

    # ── Helper: safe order cancellation with retry ──────────────────────

    def _cancel_order_safe(self, oid_str: str, channel: str, label: str, symbol: str) -> bool:
        """
        Cancel an order with 3x retry. Returns True if cancel was sent.

        Extracts the raw ID from prefixed strings like "TWS_500" and asks
        TWS to cancel it. CP API removed — TWS is the only channel.
        """
        if not oid_str:                                  # No order ID provided — nothing to cancel
            return False                                 # Cannot cancel what doesn't exist
        raw_id = oid_str.split("_", 1)[1] if "_" in oid_str else oid_str  # Strip channel prefix to get raw ID

        for attempt in range(1, 4):                      # Try up to 3 times (FIX C: 3x retry)
            try:
                if self._tws:                            # TWS available
                    self._tws.cancel_order_by_id(int(raw_id))
                    logger.info("[%s] Cancelled %s order %s via TWS (attempt %d)",
                                symbol, label, oid_str, attempt)
                    return True                          # Cancel succeeded
                else:                                    # TWS not available
                    logger.warning("[%s] Cannot cancel %s %s — TWS unavailable",
                                   symbol, label, oid_str)
                    return False
            except Exception as exc:                     # Cancel attempt failed
                logger.warning("[%s] Cancel %s %s attempt %d failed: %s",
                               symbol, label, oid_str, attempt, exc)
                if attempt < 3:
                    time.sleep(0.5)                      # FIX C: 0.5s delay between retry attempts
        logger.error("[%s] Cancel %s %s FAILED after 3 attempts", symbol, label, oid_str)
        return False

    def _modify_order_safe(self, oid_str: str, channel: str, label: str,
                           symbol: str, new_price: float,
                           new_aux_price: float = 0.0) -> bool:
        """
        Modify an existing order's price in-place. No cancel/replace needed.

        On TWS: calls placeOrder with the same orderId (TWS treats this as modify).
        On API: calls POST /iserver/account/{id}/order/{orderId} with new price.

        Falls back to cancel+replace if modify fails (some IBKR states reject modify).

        Args:
            oid_str:       Order ID string with channel prefix (e.g., "TWS_500" or "API_123").
            channel:       "TWS" or "API" — which channel the order lives on.
            label:         Human-readable label for logging (e.g., "stp_mkt").
            symbol:        Ticker symbol for logging.
            new_price:     New price (auxPrice for STP, lmtPrice for LMT).
            new_aux_price: New limit price for STP LMT orders. 0.0 otherwise.

        Returns True if modify succeeded, False if it failed.
        """
        # Round prices to 2 decimal places (IBKR min tick for stocks)
        new_price = round(new_price, 2)
        new_aux_price = round(new_aux_price, 2)

        if not oid_str:                                  # No order ID — nothing to modify
            return False                                 # Cannot modify non-existent order

        # Extract raw order ID from prefixed string
        raw_id = oid_str.split("_", 1)[1] if "_" in oid_str else oid_str

        if channel == "TWS" and self._tws:               # Modify via TWS
            # Look up the original ticket from _placed_orders for contract/order rebuild
            original_ticket = None                       # Will hold original OrderTicket
            with self._placed_lock:                      # Thread-safe read
                for ref, info in self._placed_orders.items():  # Search for this order
                    if info.get("channel_oid") == int(raw_id) or str(info.get("channel_oid")) == raw_id:
                        original_ticket = info.get("ticket")  # Found original ticket
                        break

            if not original_ticket:                      # Original ticket not found in tracking
                logger.warning("[%s] Modify %s: original ticket not found for oid=%s — "
                               "cannot rebuild order for TWS modify", symbol, label, oid_str)
                return False                             # Cannot modify without original details

            try:
                success = self._tws.modify_order_price(  # TWS modify = placeOrder with same ID
                    order_id=int(raw_id),                 # Same integer order ID
                    ticket=original_ticket,               # Original ticket for contract rebuild
                    new_price=new_price,                  # Updated price
                    new_aux_price=new_aux_price,          # Updated aux price (STP LMT only)
                )
                if success:                              # Modify accepted by TWS
                    logger.info("[%s] Modified %s %s: new_price=%.4f",
                                symbol, label, oid_str, new_price)
                return success                           # Return modify result
            except Exception as exc:
                logger.error("[%s] TWS modify %s %s failed: %s", symbol, label, oid_str, exc)
                return False                             # Modify failed

        return False                                     # No channel available

    # ── Breakeven stop adjustment ─────────────────────────────────────────

    def apply_breakeven(self, symbol: str, conid: int, direction: str,
                        new_stp_price: float, sls: float = 0.03):
        """
        Called when price reaches breakeven threshold.

        Actions:
          1. Place STOP PRICE order (STP) at new_stp_price + sls (LONG) or - sls (SHORT)
          2. Place STOP LIMIT order (STP LMT) at new_stp_price + 2*sls (LONG) or - 2*sls (SHORT)
             with trigger at STOP PRICE level
          3. Cancel old STP Market and place new one at new_stp_price
             (cancel-then-place, since CP API modify is unreliable)

        All three orders protect the position. STOP PRICE and STOP LIMIT are tighter
        (hit first on orderly decline), STP Market is the backstop (guaranteed fill).
        """
        close_side = "SELL" if direction == "LONG" else "BUY"  # Exit side based on direction
        ts = f"{int(time.time())}_{symbol}"              # Unique timestamp+symbol tag for order refs

        # Compute STOP PRICE and STOP LIMIT levels based on direction
        if direction == "LONG":                          # Long: stops are below current price
            stop_price_level = round(new_stp_price + sls, 2)   # STOP PRICE (rounded to cents)
            stop_lmt_level = round(new_stp_price + (2 * sls), 2)   # STOP LIMIT (rounded to cents)
        else:                                            # Short: stops are above current price
            stop_price_level = round(new_stp_price - sls, 2)   # STOP PRICE (rounded to cents)
            stop_lmt_level = round(new_stp_price - (2 * sls), 2)   # STOP LIMIT (rounded to cents)

        # Look up current orders and channel for this symbol
        with self._symbol_orders_lock:                   # Thread-safe read of symbol orders
            sym_orders = dict(self._symbol_orders.get(symbol, {}))  # Copy order map for this symbol

        old_stp_mkt_oid = sym_orders.get("stp_mkt")     # Current STP Market order ID (to be replaced)
        # FIX Z1: same OCA group as the existing STP_MKT — links all 3 stops on the broker.
        # Falls back to a fresh group if Phase 2 didn't record one (defensive).
        oca_group = sym_orders.get("oca_group") or f"BRK_{symbol}_{ts}"

        # Determine the channel from any existing placed order
        channel = "TWS"                                  # Default channel
        with self._placed_lock:                          # Thread-safe read of placed orders
            for ref, info in self._placed_orders.items():  # Search placed orders for this symbol
                if symbol in ref:                        # Found an order for this symbol
                    channel = info.get("channel", "TWS")  # Use its channel
                    break                                # Found channel — stop searching

        # ── 1 & 2: STOP-PRICE + STOP-LIMIT siblings — DISABLED 2026-06-01 ──
        # Per user: the protective/trailing stop is now a SINGLE STOP-MARKET.
        # We no longer place the STOP_PRICE / STOP_LIMIT siblings — only the
        # STP Market backstop (step 3 below). One protective order per symbol
        # instead of three = far less OCA / partial-fill tracking that can
        # drift out of sync (the root of today's over-fill divergence).
        # Pull the close quantity from the existing STP Market so the new one matches.
        stp_price_result = None                          # sibling disabled
        stp_lmt_result   = None                          # sibling disabled
        be_qty = 1                                       # fallback; overwritten below
        if old_stp_mkt_oid:                              # Old STP Market exists — get its qty
            with self._placed_lock:                      # Thread-safe read
                for ref, info in self._placed_orders.items():
                    if info.get("channel_oid") and old_stp_mkt_oid and str(info["channel_oid"]) in old_stp_mkt_oid:
                        be_qty = info["ticket"].quantity  # Reuse its quantity
                        break

        # ── 3. Move/place the STP Market (the ONLY protective stop now) ───
        # On TWS: modify in place (same orderId, placeOrder with new price) — reliable.
        # On API: cancel + replace (CP API modify is unreliable for STP orders).
        new_stp_mkt_result = old_stp_mkt_oid             # Assume OID stays same (TWS modify)

        if old_stp_mkt_oid:
            if channel == "TWS" and self._tws:           # TWS: modify in place — no cancel needed
                modified = self._modify_order_safe(
                    oid_str=old_stp_mkt_oid,
                    channel=channel,
                    label="stp_mkt",
                    symbol=symbol,
                    new_price=new_stp_price,
                )
                if modified:
                    logger.info("[%s] BE STP_MKT modified in place: %s → %.4f",
                                symbol, old_stp_mkt_oid, new_stp_price)
                    new_stp_mkt_result = old_stp_mkt_oid  # Same OID — modify kept it
                else:
                    logger.warning("[%s] BE STP_MKT TWS modify failed — falling back to cancel+replace",
                                   symbol)
                    new_stp_mkt_result = None             # Will place fresh below

            else:                                        # API: cancel + replace (modify unreliable)
                cancelled = self._cancel_order_safe(old_stp_mkt_oid, channel, "stp_mkt", symbol)
                if cancelled:
                    logger.info("[%s] BE STP_MKT %s cancelled (API path)", symbol, old_stp_mkt_oid)
                else:
                    logger.warning("[%s] BE STP_MKT %s cancel failed — placing new anyway",
                                   symbol, old_stp_mkt_oid)
                new_stp_mkt_result = None                 # Must place fresh below

        # Place fresh STP Market if modify failed or API path
        if new_stp_mkt_result is None or new_stp_mkt_result != old_stp_mkt_oid:
            new_stp_ref = f"STPMKT_BE_{ts}"
            new_stp_ticket = OrderTicket(
                priority=OrderPriority.STP,
                seq=0,
                symbol=symbol,
                conid=conid,
                side=close_side,
                order_type="STP",
                quantity=be_qty,
                price=new_stp_price,
                tif="DAY",
                parent_id="",
                order_ref=new_stp_ref,
                oca_group=oca_group,                      # FIX Z1: same group as siblings
                oca_type=1,
                _created_at=time.time(),
            )
            with self._seq_lock:
                new_stp_ticket.seq = self._seq_counter
                self._seq_counter += 1
            new_stp_mkt_result = self._send_via_channel(new_stp_ticket, channel, transmit=True)
            if new_stp_mkt_result:
                logger.info("[%s] BE STP_MKT placed fresh: %s @ %.4f",
                            symbol, new_stp_mkt_result, new_stp_price)
            else:
                logger.error("[%s] BE STP_MKT FAILED @ %.4f — position may lack backstop!",
                             symbol, new_stp_price)

        # ── 4. Update per-symbol order tracking ──────────────────────────
        with self._symbol_orders_lock:                   # Thread-safe update to symbol orders
            if symbol in self._symbol_orders:            # Symbol still tracked
                self._symbol_orders[symbol]["stop_price"] = stp_price_result  # Store STOP PRICE order ID
                self._symbol_orders[symbol]["stop_lmt"] = stp_lmt_result  # Store STOP LIMIT order ID
                self._symbol_orders[symbol]["stp_mkt"] = new_stp_mkt_result  # Store new STP Market order ID

        # Refresh OCA-tracking siblings to reflect the BE-installed orders.
        # The fresh oid set replaces the prior single stp_mkt entry. Reset
        # gap_handled so a future partial-fill on these new siblings can
        # trigger auto-flatten.
        def _raw_oid(s):
            try:
                return int(str(s).split("_", 1)[1]) if s and "_" in str(s) else (int(s) if s else None)
            except (ValueError, IndexError):
                return None
        with self._oca_tracking_lock:
            track = self._oca_tracking.get(symbol)
            if track is not None:
                track["siblings"] = {}
                track["gap_handled"] = False
                qty = int(track.get("position_qty", 0))
                for raw, key in (
                    (_raw_oid(new_stp_mkt_result), "stp_mkt"),
                    (_raw_oid(stp_price_result),   "stop_price"),
                    (_raw_oid(stp_lmt_result),     "stop_lmt"),
                ):
                    if raw is not None:
                        track["siblings"][raw] = {
                            "key": key, "filled": 0, "remaining": qty,
                            "status": "Submitted",
                        }

        logger.info("[%s] Breakeven applied: stop_price=%s  stop_lmt=%s  stp_mkt=%s",
                    symbol, stp_price_result, stp_lmt_result, new_stp_mkt_result)  # Log BE summary

    # ── Trailing stop adjustment ──────────────────────────────────────────

    def trail_stop(self, symbol: str, conid: int, direction: str,
                   new_stp_price: float, sls: float = 0.03):
        """
        Called on each bar after breakeven to trail all three stop orders.

        Uses MODIFY (not cancel+replace) — 3 API calls instead of 6.
        On TWS: placeOrder with same orderId = modify in place.
        On API: POST /iserver/account/{id}/order/{orderId} = modify in place.

        If modify fails for any order, falls back to cancel+replace for that order only.

        Parameters:
            symbol:        Ticker symbol (e.g., "AAPL")
            conid:         IBKR contract ID
            direction:     "LONG" or "SHORT"
            new_stp_price: New STP Market trigger price (the base trailing level)
            sls:           StopLimitSpread — offset for STOP PRICE and STOP LIMIT
                          STOP PRICE = new_stp_price + sls (LONG) / - sls (SHORT)
                          STOP LIMIT = new_stp_price + 2*sls (LONG) / - 2*sls (SHORT)

        Actions (3 modify calls total):
          1. Modify STP Market   → new_stp_price
          2. Modify STOP PRICE   → new_stp_price + sls (LONG) / - sls (SHORT)
          3. Modify STOP LIMIT   → trigger=STOP_PRICE level, limit=STOP_LIMIT level
        """
        close_side = "SELL" if direction == "LONG" else "BUY"  # Exit side based on direction

        # Compute new STOP PRICE and STOP LIMIT levels from the base trailing price
        if direction == "LONG":                          # Long: stops trail below price
            new_stop_price_level = round(new_stp_price + sls, 2)       # STOP PRICE above STP Market
            new_stop_lmt_level = round(new_stp_price + (2 * sls), 2)   # STOP LIMIT above STOP PRICE
        else:                                            # Short: stops trail above price
            new_stop_price_level = round(new_stp_price - sls, 2)       # STOP PRICE below STP Market
            new_stop_lmt_level = round(new_stp_price - (2 * sls), 2)   # STOP LIMIT below STOP PRICE

        new_stp_price = round(new_stp_price, 2)          # Round to cents (IBKR min tick for stocks)

        # Look up current order IDs for this symbol
        with self._symbol_orders_lock:                   # Thread-safe read
            sym_orders = dict(self._symbol_orders.get(symbol, {}))  # Copy order map

        stp_mkt_oid = sym_orders.get("stp_mkt")         # Current STP Market order ID
        # FIX Z1: keep replacement orders bound to the same OCA group as siblings.
        oca_group = sym_orders.get("oca_group", "")
        stop_price_oid = sym_orders.get("stop_price")    # Current STOP PRICE order ID
        stop_lmt_oid = sym_orders.get("stop_lmt")        # Current STOP LIMIT order ID

        # Determine the channel from placed orders
        channel = "TWS"                                  # Default to TWS
        with self._placed_lock:                          # Thread-safe read
            for ref, info in self._placed_orders.items():  # Search for this symbol's orders
                if symbol in ref:                        # Found an order for this symbol
                    channel = info.get("channel", "TWS") # Use its channel
                    break                                # Stop searching

        logger.info("[%s] TRAIL: modifying 3 stops → stp=%.4f  stp_price=%.4f  stp_lmt=%.4f",
                    symbol, new_stp_price, new_stop_price_level, new_stop_lmt_level)

        # ── 1. Modify STP Market → new_stp_price ────────────────────────
        stp_ok = False                                   # Track if modify succeeded
        if stp_mkt_oid:                                  # STP Market order exists
            stp_ok = self._modify_order_safe(             # Modify in place (no cancel needed)
                oid_str=stp_mkt_oid,                     # Existing order ID
                channel=channel,                         # Channel it lives on
                label="stp_mkt",                         # Label for logging
                symbol=symbol,                           # Ticker for logging
                new_price=new_stp_price,                 # New stop trigger price
            )
            if stp_ok:                                   # Modify accepted
                logger.info("[%s] TRAIL STP_MKT modified: %s → %.4f",
                            symbol, stp_mkt_oid, new_stp_price)
            else:                                        # Modify failed — fall back to cancel+replace
                logger.warning("[%s] TRAIL STP_MKT modify failed — falling back to cancel+replace",
                               symbol)
                self._cancel_order_safe(stp_mkt_oid, channel, "stp_mkt", symbol)  # Cancel old
                # Place new STP Market as replacement
                ts = f"{int(time.time())}_{symbol}"
                qty = self._get_symbol_qty(symbol)       # Look up position quantity
                new_ticket = OrderTicket(
                    priority=OrderPriority.STP, seq=0, symbol=symbol, conid=conid,
                    side=close_side, order_type="STP", quantity=qty,
                    price=new_stp_price, tif="DAY", order_ref=f"STPMKT_TRAIL_{ts}",
                    oca_group=oca_group, oca_type=1 if oca_group else 0,  # FIX Z1
                    _created_at=time.time(),
                )
                with self._seq_lock:
                    new_ticket.seq = self._seq_counter
                    self._seq_counter += 1
                new_oid = self._send_via_channel(new_ticket, channel, transmit=True)
                if new_oid:                              # Replacement placed OK
                    with self._symbol_orders_lock:
                        if symbol in self._symbol_orders:
                            self._symbol_orders[symbol]["stp_mkt"] = new_oid
                    logger.info("[%s] TRAIL STP_MKT replaced: %s → %.4f", symbol, new_oid, new_stp_price)
        else:                                            # No STP Market to modify
            logger.warning("[%s] TRAIL: no stp_mkt to modify", symbol)

        time.sleep(0.05)                                 # 50ms throttle between modifications

        # ── 2. Modify STOP PRICE → new_stop_price_level ─────────────────
        sp_ok = False                                    # Track if modify succeeded
        if stop_price_oid:                               # STOP PRICE order exists
            sp_ok = self._modify_order_safe(              # Modify in place
                oid_str=stop_price_oid, channel=channel,
                label="stop_price", symbol=symbol,
                new_price=new_stop_price_level,          # New STOP PRICE trigger
            )
            if sp_ok:                                    # Modify accepted
                logger.info("[%s] TRAIL STOP_PRICE modified: %s → %.4f",
                            symbol, stop_price_oid, new_stop_price_level)
            else:                                        # Modify failed — fall back
                logger.warning("[%s] TRAIL STOP_PRICE modify failed — falling back to cancel+replace",
                               symbol)
                self._cancel_order_safe(stop_price_oid, channel, "stop_price", symbol)
                ts = f"{int(time.time())}_{symbol}"
                qty = self._get_symbol_qty(symbol)
                new_ticket = OrderTicket(
                    priority=OrderPriority.STP, seq=0, symbol=symbol, conid=conid,
                    side=close_side, order_type="STP", quantity=qty,
                    price=new_stop_price_level, tif="DAY", order_ref=f"STPPRICE_TRAIL_{ts}",
                    oca_group=oca_group, oca_type=1 if oca_group else 0,  # FIX Z1
                    _created_at=time.time(),
                )
                with self._seq_lock:
                    new_ticket.seq = self._seq_counter
                    self._seq_counter += 1
                new_oid = self._send_via_channel(new_ticket, channel, transmit=True)
                if new_oid:
                    with self._symbol_orders_lock:
                        if symbol in self._symbol_orders:
                            self._symbol_orders[symbol]["stop_price"] = new_oid
                    logger.info("[%s] TRAIL STOP_PRICE replaced: %s → %.4f", symbol, new_oid, new_stop_price_level)
        else:
            logger.warning("[%s] TRAIL: no stop_price to modify", symbol)

        time.sleep(0.05)                                 # 50ms throttle between modifications

        # ── 3. Modify STOP LIMIT → new trigger + limit levels ───────────
        sl_ok = False                                    # Track if modify succeeded
        if stop_lmt_oid:                                 # STOP LIMIT order exists
            sl_ok = self._modify_order_safe(              # Modify in place
                oid_str=stop_lmt_oid, channel=channel,
                label="stop_lmt", symbol=symbol,
                new_price=new_stop_price_level,          # Trigger at STOP PRICE level
                new_aux_price=new_stop_lmt_level,        # Limit at STOP LIMIT level
            )
            if sl_ok:                                    # Modify accepted
                logger.info("[%s] TRAIL STOP_LMT modified: %s → trigger=%.4f limit=%.4f",
                            symbol, stop_lmt_oid, new_stop_price_level, new_stop_lmt_level)
            else:                                        # Modify failed — fall back
                logger.warning("[%s] TRAIL STOP_LMT modify failed — falling back to cancel+replace",
                               symbol)
                self._cancel_order_safe(stop_lmt_oid, channel, "stop_lmt", symbol)
                ts = f"{int(time.time())}_{symbol}"
                qty = self._get_symbol_qty(symbol)
                new_ticket = OrderTicket(
                    priority=OrderPriority.STP_LIMIT, seq=0, symbol=symbol, conid=conid,
                    side=close_side, order_type="STP LMT", quantity=qty,
                    price=new_stop_price_level, aux_price=new_stop_lmt_level,
                    tif="DAY", order_ref=f"STPLMT_TRAIL_{ts}",
                    oca_group=oca_group, oca_type=1 if oca_group else 0,  # FIX Z1
                    _created_at=time.time(),
                )
                with self._seq_lock:
                    new_ticket.seq = self._seq_counter
                    self._seq_counter += 1
                new_oid = self._send_via_channel(new_ticket, channel, transmit=True)
                if new_oid:
                    with self._symbol_orders_lock:
                        if symbol in self._symbol_orders:
                            self._symbol_orders[symbol]["stop_lmt"] = new_oid
                    logger.info("[%s] TRAIL STOP_LMT replaced: %s → trigger=%.4f limit=%.4f",
                                symbol, new_oid, new_stop_price_level, new_stop_lmt_level)
        else:
            logger.warning("[%s] TRAIL: no stop_lmt to modify", symbol)

        logger.info("[%s] Trail complete: stp_ok=%s  sp_ok=%s  sl_ok=%s  (modify-in-place)",
                    symbol, stp_ok, sp_ok, sl_ok)         # Log trail summary with modify results

    def _get_symbol_qty(self, symbol: str) -> int:
        """Look up the position quantity for a symbol from placed orders."""
        with self._placed_lock:                          # Thread-safe read
            for ref, info in self._placed_orders.items():  # Search placed orders
                if symbol in ref and info.get("ticket"):  # Found order for this symbol
                    if info["ticket"].order_type == "STP":  # Stop order has the correct qty
                        return info["ticket"].quantity    # Return its quantity
        return 1                                         # Fallback default (should never happen)

    # ── Stop-fill notification hook (keeps bot's _open_positions in sync) ──

    def set_position_clear_callback(self, fn: Callable[[str], None]) -> None:
        """
        Register a callback fired when a stop-type order for a tracked symbol
        fills in IBKR. Used by the bot to clear internal _open_positions[symbol]
        the moment the broker confirms the exit, eliminating stale-state drift.
        """
        self._on_stop_filled = fn                        # Store callback reference

    def set_long_disconnect_callback(self, fn: Callable[[float], None]) -> None:
        """
        Register a callback fired ONCE when TWS has been disconnected for
        more than the long-disconnect threshold (default 150s = 2.5 min).
        The callback receives the disconnect duration in seconds. Used by the
        bot to dump its own position state so a post-mortem is possible if
        the connection never recovers.
        """
        self._on_long_disconnect = fn

    # ── P&L / account snapshot helpers ─────────────────────────────────────

    def _account_snapshot(self) -> Dict[str, str]:
        """Return the most useful subset of account-value keys as a flat dict.

        Merges two streams:
          - reqAccountUpdates   (slow push: cash, equity, available funds, …)
          - reqPnL              (fast push: realized / unrealized / daily P&L)
        Realtime P&L from reqPnL takes precedence over the slower copies.
        """
        if not self._tws:
            return {}
        try:
            raw = self._tws.get_account_values()
        except Exception:
            raw = {}
        try:
            pnl = self._tws.get_pnl()
        except Exception:
            pnl = {}
        wanted = (
            "NetLiquidation",          # current total portfolio value (= "principal")
            "TotalCashValue",          # total cash across currencies
            "AvailableFunds",          # cash available to open new positions
            "BuyingPower",             # leveraged purchasing power
            "GrossPositionValue",      # sum of position market values
            "EquityWithLoanValue",     # equity for margin purposes
        )
        out: Dict[str, str] = {}
        for k in wanted:
            v = raw.get(k)
            if v is not None:
                value, currency = v
                out[k] = f"{value} {currency}".strip()
        # Prefer realtime P&L from reqPnL, fall back to slow updateAccountValue
        if "realizedPnL" in pnl:
            out["RealizedPnL"]   = f"{pnl['realizedPnL']:+.2f} USD (live)"
            out["UnrealizedPnL"] = f"{pnl['unrealizedPnL']:+.2f} USD (live)"
            out["DailyPnL"]      = f"{pnl['dailyPnL']:+.2f} USD (live)"
        else:
            for k in ("RealizedPnL", "UnrealizedPnL"):
                v = raw.get(k)
                if v is not None:
                    out[k] = f"{v[0]} {v[1]}".strip() + " (slow)"
        ts = raw.get("__timestamp__")
        if ts:
            out["timestamp"] = ts[0]
        return out

    def log_pnl_snapshot(self, prefix: str = "P&L SNAPSHOT") -> None:
        """Log the current P&L / cash / equity snapshot in one line per key."""
        snap = self._account_snapshot()
        if not snap:
            logger.warning("%s: account values not yet streamed (TWS likely just connected)",
                           prefix)
            return
        logger.warning("─── %s ───", prefix)
        order = ("NetLiquidation", "TotalCashValue", "AvailableFunds",
                 "BuyingPower", "GrossPositionValue",
                 "RealizedPnL", "UnrealizedPnL", "DailyPnL",
                 "EquityWithLoanValue", "timestamp")
        for k in order:
            if k in snap:
                logger.warning("  %-22s %s", k, snap[k])

    # ── Diagnostic state dump (called on shutdown + on long disconnect) ────

    def _dump_state(self, reason: str = "manual") -> None:
        """
        Log the full engine state — every tracked symbol's order IDs, queue
        depth, fill-verify backlog, connection status, and stats. Triggered
        on shutdown and on disconnects exceeding `_long_disconnect_sec`.
        Wraps each section in try/except so a partial failure can't suppress
        the rest of the dump.
        """
        try:
            now = datetime.now(EST).strftime("%Y-%m-%d %H:%M:%S %Z")
            logger.warning("=" * 78)
            logger.warning("ENGINE STATE DUMP — reason=%s  ts=%s", reason, now)
            logger.warning("-" * 78)
        except Exception:
            pass
        # Connection
        try:
            if self._tws is not None:
                logger.warning("TWS: connected=%s  host=%s  port=%s  clientId=%s",
                               getattr(self._tws, "is_connected", False),
                               getattr(self._tws, "_host", "?"),
                               getattr(self._tws, "_port", "?"),
                               getattr(self._tws, "_client_id", "?"))
            else:
                logger.warning("TWS: client object is None")
        except Exception as exc:
            logger.warning("TWS state read failed: %s", exc)
        # Per-symbol order tracking — the most important rebuild data
        try:
            with self._symbol_orders_lock:
                sym_orders = {s: dict(o) for s, o in self._symbol_orders.items()}
            if sym_orders:
                logger.warning("Tracked symbols: %d", len(sym_orders))
                for sym in sorted(sym_orders):
                    omap = sym_orders[sym]
                    pieces = [f"{k}={v}" for k, v in omap.items() if v]
                    logger.warning("  [%s] %s", sym, "  ".join(pieces) if pieces else "(empty)")
            else:
                logger.warning("Tracked symbols: (none)")
        except Exception as exc:
            logger.warning("symbol_orders dump failed: %s", exc)
        # Active fill-verify threads
        try:
            with self._filling_lock:
                filling = sorted(self._filling_syms)
            logger.warning("Fill-verify in progress: %s",
                           filling if filling else "(none)")
        except Exception as exc:
            logger.warning("filling_syms dump failed: %s", exc)
        # Queue depth
        try:
            qsize = self._queue.qsize() if hasattr(self, "_queue") and self._queue else 0
            logger.warning("Order queue depth: %d", qsize)
        except Exception as exc:
            logger.warning("queue size read failed: %s", exc)
        # Stats
        try:
            with self._stats_lock:
                stats_copy = dict(self._stats)
            logger.warning("Stats: %s", stats_copy)
        except Exception as exc:
            logger.warning("stats dump failed: %s", exc)
        # P&L / account snapshot — realized + unrealized, cash, equity
        try:
            self.log_pnl_snapshot(prefix=f"P&L SNAPSHOT (dump:{reason})")
        except Exception as exc:
            logger.warning("P&L snapshot in dump failed: %s", exc)
        logger.warning("=" * 78)

    def _on_order_status_event(self, orderId: int, status: str,
                               filled: float, remaining: float) -> None:
        """
        Forwarded from TWSClient.orderStatus on every status update.

        Two responsibilities:
          1. On full stop fill, fire the position-clear callback (existing).
          2. On ANY stop status update (filled/partial/cancelled), update
             the OCA-tracking state and check for partial-fill gaps. When
             all OCA siblings reach a terminal state and the cumulative
             fill < expected position qty, trigger _auto_flatten_gap().
        """
        # Find which symbol + which sibling-key this orderId belongs to
        target_sym = None
        sibling_key = None
        with self._symbol_orders_lock:
            for sym, omap in self._symbol_orders.items():
                for key in ("stp_mkt", "stop_price", "stop_lmt", "wide_stp"):
                    v = omap.get(key)
                    if v is None:
                        continue
                    try:
                        v_oid = int(str(v).split("_", 1)[1]) if "_" in str(v) else int(v)
                    except (ValueError, IndexError):
                        continue
                    if v_oid == orderId:
                        target_sym = sym
                        sibling_key = key
                        break
                if target_sym:
                    break

        if not target_sym:
            return                                       # Order not in tracking — nothing to do

        # ── Update OCA-tracking state for this sibling ─────────────────
        with self._oca_tracking_lock:
            track = self._oca_tracking.get(target_sym)
            if track is not None:
                sib = track["siblings"].setdefault(orderId, {
                    "key": sibling_key, "filled": 0,
                    "remaining": 0, "status": "Submitted",
                })
                sib["filled"]    = int(filled)
                sib["remaining"] = int(remaining)
                sib["status"]    = status

        # ── Existing behavior: notify bot on FULL fill ─────────────────
        # (Full fill = status=Filled and remaining=0 — sync hook fires)
        if status == "Filled" and filled > 0 and remaining <= 0:
            if self._on_stop_filled is not None:
                try:
                    self._on_stop_filled(target_sym)
                except Exception as exc:
                    logger.error("[%s] _on_stop_filled error: %s", target_sym, exc)

        # ── NEW: Partial-fill gap detection ─────────────────────────────
        # Fires when all OCA siblings have reached terminal state but the
        # cumulative fill is less than the expected position qty.
        self._check_partial_fill_gap(target_sym)

    # ── Partial-fill gap detection + auto-flatten ─────────────────────────

    def _check_partial_fill_gap(self, symbol: str) -> None:
        """
        Idempotent: when all OCA siblings reach terminal state, sum their
        fills and compare to position_qty. If gap exists and gap_handled
        flag is False, trigger _auto_flatten_gap.
        """
        # 2026-05-26 (P2 #7): suppress during the EoD / post-global-cancel
        # window. reqGlobalCancel turns every OCA sibling terminal with
        # filled=0, which is NOT a real partial-fill gap. Without this guard the
        # cancel storm tripped a gap auto-flatten for every open symbol on top
        # of the EoD MKT closes → mass over-cover (the 15:50→15:51 flip storm).
        if time.time() < self._suppress_gap_until:
            logger.info("[%s] partial-fill gap check suppressed — EoD / "
                        "post-global-cancel window active", symbol)
            return

        with self._oca_tracking_lock:
            track = self._oca_tracking.get(symbol)
            if track is None or track.get("gap_handled"):
                return
            siblings = track["siblings"]
            if not siblings:
                return
            terminal = {"Filled", "Cancelled", "ApiCancelled", "Inactive"}
            all_done = all(s["status"] in terminal for s in siblings.values())
            if not all_done:
                return                                   # Wait for all siblings to finalize
            total_filled = sum(s["filled"] for s in siblings.values())
            expected     = int(track.get("position_qty", 0))
            if expected <= 0:
                return                                   # No tracked qty — nothing to compare
            if total_filled >= expected:
                return                                   # No gap — fully closed
            # Gap detected — mark handled (idempotent) and capture gap qty
            track["gap_handled"] = True
            gap_qty   = expected - total_filled
            direction = track.get("direction", "LONG")

        # Outside lock — log and trigger flatten
        logger.error(
            "[%s] PARTIAL-FILL GAP detected — expected=%d total_filled=%d gap=%d  "
            "auto-flattening in 500ms",
            symbol, expected, total_filled, gap_qty)
        # Spawn flatten on a daemon thread so the API reader thread is not blocked
        threading.Thread(
            target=self._auto_flatten_gap,
            args=(symbol, gap_qty, direction),
            daemon=True,
            name=f"auto-flatten-{symbol}",
        ).start()

    def _auto_flatten_gap(self, symbol: str, gap_qty: int, direction: str) -> None:
        """
        Cancel any working OCA siblings, wait 500ms, send MKT close for the
        gap, then spawn the reconciliation watchdog.

        Order matters here: the conid lookup MUST happen BEFORE
        handle_stop_hit() runs, because handle_stop_hit clears the symbol
        from both _placed_orders and _oca_tracking. We read from
        _oca_tracking first (preferred — set at Phase 2 with explicit conid),
        then fall back to _placed_orders for any legacy/edge case.
        """
        # 0. Claim single flatten authority (2026-05-26, P2 #8). If another
        #    path (EoD, a prior gap, a stop-hit handler) is already flattening
        #    this symbol, skip — a second MKT close on top is what over-covered
        #    shorts into longs in the 15:51 flip storm. The claim is released by
        #    _reconcile_until_flat once the position is confirmed flat/escalated.
        if not self._begin_flatten(symbol):
            logger.warning("[%s] auto-flatten SKIPPED — flatten already in "
                           "progress for this symbol (duplicate guard)", symbol)
            return

        # 1. Resolve conid FIRST — read from oca tracking (cleared by step 3).
        conid = None
        with self._oca_tracking_lock:
            track = self._oca_tracking.get(symbol)
            if track and track.get("conid"):
                conid = int(track["conid"])
        # Fallback: scan _placed_orders if oca-tracking didn't have it
        if conid is None:
            with self._placed_lock:
                for ref, info in list(self._placed_orders.items()):
                    if symbol in ref and info.get("ticket"):
                        conid = info["ticket"].conid
                        break

        # 2. Mark symbol as pending reconciliation IMMEDIATELY so any signal
        #    fired in the next ~milliseconds is blocked at the entry gate.
        self.mark_pending_reconciliation(symbol)

        # 2026-05-22 (Fix #4): start the heartbeat thread the first time we
        # mark anything pending. Self-heals any reconcile thread that gives up
        # while IBKR's position cache is still catching up.
        self._ensure_reconcile_heartbeat()

        # 3. Cancel any working siblings (belt-and-suspenders — OCA usually
        #    handles this, but a STOP_LIMIT working remainder may still be live).
        try:
            self.handle_stop_hit(symbol)
        except Exception as exc:
            logger.warning("[%s] handle_stop_hit during auto-flatten: %s",
                           symbol, exc)

        if conid is None:
            logger.error("[%s] auto-flatten: no conid found — cannot send close",
                         symbol)
            try:
                from notifier import notify_critical
                notify_critical(
                    "Auto-flatten failed",
                    f"[{symbol}] No conid resolvable. Manual intervention required.",
                )
            except Exception:
                pass
            self._end_flatten(symbol)
            return

        # 4. 500ms grace before MKT close (lets OCA cancellation propagate)
        time.sleep(0.5)

        # 2026-05-22 (Fix #3): use IBKR's actual position as source-of-truth
        # for the close quantity instead of the passed-in gap_qty derived from
        # filled-vs-expected accounting. The accounting path produces off-by-
        # one residuals (e.g., 499 close for a 500-share short → -1 left over,
        # which the reconciler then mis-flags as "STALLED"). Falls back to the
        # accounting value if the snapshot fails.
        try:
            actual_signed, fresh = self.get_position_confirmed(symbol)
            if fresh and actual_signed is not None and abs(actual_signed) > 0:
                close_qty   = abs(actual_signed)
                close_side  = "SELL" if actual_signed > 0 else "BUY"
                if close_qty != int(gap_qty) or close_side != ("SELL" if direction == "LONG" else "BUY"):
                    logger.warning(
                        "[%s] auto-flatten close-qty override: accounting=%d/%s "
                        "→ snapshot=%d/%s (using snapshot)",
                        symbol, int(gap_qty),
                        "SELL" if direction == "LONG" else "BUY",
                        close_qty, close_side)
            elif fresh and actual_signed == 0:
                # CONFIRMED flat (fresh snapshot) — nothing to close.
                logger.info("[%s] auto-flatten: IBKR confirms flat; "
                            "clearing pending_reconciliation", symbol)
                self.clear_pending_reconciliation(symbol)
                self._end_flatten(symbol)
                return
            else:
                # 2026-05-26 (P0 #3): position UNKNOWN (receive broken / snapshot
                # timed out). Do NOT assume flat — that false "0" was exactly how
                # naked positions slipped through. Close the accounting gap_qty
                # instead so the reconcile watchdog keeps working it.
                logger.warning(
                    "[%s] auto-flatten: position UNKNOWN (snapshot not fresh) — "
                    "NOT assuming flat; closing accounting gap_qty=%d / %s",
                    symbol, int(gap_qty), direction)
                close_qty  = int(gap_qty)
                close_side = "SELL" if direction == "LONG" else "BUY"
        except Exception as exc:
            logger.warning(
                "[%s] auto-flatten: snapshot failed (%s) — falling back to "
                "accounting gap_qty=%d / direction=%s",
                symbol, exc, int(gap_qty), direction)
            close_qty  = int(gap_qty)
            close_side = "SELL" if direction == "LONG" else "BUY"

        # 5. Send MKT close for the gap qty
        try:
            oid = self.submit_market_close(
                symbol=symbol, conid=conid,
                side=close_side, quantity=close_qty,
            )
            logger.warning(
                "[%s] auto-flatten MKT close placed: oid=%s side=%s qty=%d",
                symbol, oid, close_side, close_qty)
        except Exception as exc:
            logger.error("[%s] auto-flatten close failed: %s", symbol, exc)
            try:
                from notifier import notify_critical
                notify_critical(
                    "Auto-flatten close failed",
                    f"[{symbol}] submit_market_close raised: {exc}. "
                    "Position may still be open. Manual intervention required.",
                )
            except Exception:
                pass
            self._end_flatten(symbol)
            return

        # 6. Spawn reconciliation watchdog. 2026-05-22 (Fix #2): pass conid +
        # direction so the watchdog can send a TOP-UP close on stall instead
        # of giving up.
        threading.Thread(
            target=self._reconcile_until_flat,
            args=(symbol,),
            kwargs={"conid": conid, "direction": direction},
            daemon=True,
            name=f"reconcile-{symbol}",
        ).start()

    def _reconcile_until_flat(self, symbol: str,
                              max_attempts: int = 20,
                              poll_interval: float = 1.0,
                              max_stalls: int = 12,
                              conid: Optional[int] = None,
                              direction: Optional[str] = None,
                              max_topups: int = 3) -> None:
        """
        Polls IBKR's actual position for `symbol` up to max_attempts times.
        On each poll: if position is 0, clears pending_reconciliation and
        returns.

        Adaptive escalation:
          - We keep polling AS LONG AS the position is REDUCING toward 0.
            A large MKT close fills across multiple exchanges and may take
            many seconds for IBKR's position cache to fully settle. While
            it's actively reducing, the auto-flatten is working — don't
            falsely declare failure.
          - On stall (no reduction for `max_stalls` polls): if conid +
            direction are known and we have top-ups left, send a top-up MKT
            close for the residual and keep polling. Otherwise escalate.
          - Only escalate when:
              (a) max_attempts exhausted with non-zero position, OR
              (b) stall exceeded max_topups attempts.

        Defaults (2026-05-22 — Fix #1: raised from 10/3 to 20/12 because
        IBKR's position cache routinely lags 5-15s behind a MKT close
        routed across multiple exchanges; old defaults misread settlement
        lag as a real failure and locked symbols for the rest of the day):
          - max_attempts=20, poll_interval=1.0s → up to 20 s window
          - max_stalls=12 consecutive non-reducing polls → top-up or escalate
          - max_topups=3 retries with a top-up MKT close on stall
        """
        last_abs   = None
        stall_run  = 0
        topups_sent = 0
        for attempt in range(1, max_attempts + 1):
            time.sleep(poll_interval)
            try:
                actual, fresh = self.get_position_confirmed(symbol)
            except Exception as exc:
                logger.warning("[%s] reconciliation poll %d failed: %s",
                               symbol, attempt, exc)
                continue
            if not fresh:
                # 2026-05-26 (P0 #3): position UNKNOWN — receive broken / snapshot
                # timed out. NEVER declare flat on unknown (that false "0" was the
                # bug). Count it as a stall so the loop eventually escalates and
                # leaves the symbol LOCKED + alerted rather than silently "flat".
                stall_run += 1
                logger.warning("[%s] reconciliation poll %d/%d: position UNKNOWN "
                               "(snapshot not fresh) — not flat (stall=%d)",
                               symbol, attempt, max_attempts, stall_run)
                if stall_run >= max_stalls:
                    logger.warning("[%s] reconciliation STALLED on UNKNOWN position "
                                   "for %d polls — escalating", symbol, stall_run)
                    break
                continue
            actual_abs = abs(int(actual))
            logger.info("[%s] reconciliation poll %d/%d: actual position=%d",
                        symbol, attempt, max_attempts, actual)
            if actual == 0:
                self.clear_pending_reconciliation(symbol)
                logger.info("[%s] reconciliation SUCCESS — position flat after %d "
                            "attempt(s); new entries unblocked", symbol, attempt)
                self._end_flatten(symbol)
                return
            # Track progress: if position reduced vs. last poll, it's still
            # filling — reset stall counter and keep going.
            if last_abs is not None and actual_abs < last_abs:
                stall_run = 0
            else:
                stall_run += 1
                if stall_run >= max_stalls:
                    # 2026-05-22 (Fix #2): on stall, try a top-up MKT close
                    # for the residual instead of giving up. IBKR routinely
                    # leaves small dust remainders on multi-exchange MKT
                    # closes; the right response is another close for the
                    # remainder, not to lock the symbol.
                    if conid is not None and direction is not None \
                       and topups_sent < max_topups:
                        topup_side = "SELL" if int(actual) > 0 else "BUY"
                        topups_sent += 1
                        try:
                            oid = self.submit_market_close(
                                symbol=symbol, conid=int(conid),
                                side=topup_side, quantity=actual_abs,
                            )
                            logger.warning(
                                "[%s] reconciliation TOP-UP %d/%d: stall at "
                                "%d shares — sending MKT %s %d (oid=%s)",
                                symbol, topups_sent, max_topups,
                                actual_abs, topup_side, actual_abs, oid)
                        except Exception as exc:
                            logger.error(
                                "[%s] reconciliation top-up failed: %s",
                                symbol, exc)
                            break
                        # Reset stall tracking and continue polling
                        stall_run = 0
                        last_abs  = None
                        continue
                    logger.warning(
                        "[%s] reconciliation STALLED at %d shares for %d "
                        "consecutive polls — escalating (topups_sent=%d)",
                        symbol, actual_abs, stall_run, topups_sent)
                    break
            last_abs = actual_abs

        # ── Escalate (either max_attempts exhausted or stalled) ────────
        try:
            actual = self.get_actual_position(symbol)
        except Exception:
            actual = "unknown"
        logger.error(
            "[%s] reconciliation FAILED after %d attempt(s) (stall=%d) — "
            "IBKR position=%s. Symbol BLOCKED from new entries until manual "
            "intervention.",
            symbol, attempt, stall_run, actual)
        try:
            from notifier import notify_critical
            notify_critical(
                "Reconciliation FAILED — manual intervention required",
                f"[{symbol}] Auto-flatten attempted but IBKR position is still "
                f"`{actual}` after {attempt} reconciliation attempt(s) "
                f"(stall_run={stall_run}). The symbol is now BLOCKED from new "
                "entries until you manually resolve and call "
                "clear_pending_reconciliation('" + symbol + "') or restart the bot.",
                fields=[
                    {"name": "Symbol",         "value": symbol,            "inline": True},
                    {"name": "IBKR Position",  "value": str(actual),       "inline": True},
                    {"name": "Attempts",       "value": f"{attempt}/{max_attempts}", "inline": True},
                    {"name": "Stall run",      "value": str(stall_run),    "inline": True},
                ],
            )
        except Exception as exc:
            logger.warning("Discord notification failed: %s", exc)
        # Release the flatten claim — this symbol is now escalated/locked, and a
        # later manual clear or the reconcile heartbeat will resolve it.
        self._end_flatten(symbol)

    def get_position_confirmed(self, symbol: str) -> Tuple[Optional[int], bool]:
        """
        Fail-closed position read (2026-05-26). Returns (signed_qty, fresh):

          fresh=True  → a snapshot completed (positionEnd received) so
                        signed_qty is reliable. Positive=LONG, negative=SHORT,
                        0=flat.
          fresh=False → receive socket broken OR snapshot timed out. signed_qty
                        is None and the caller MUST treat the position as
                        UNKNOWN — never as flat. This is the guard that stops
                        the 'empty snapshot == flat' false-release that drove
                        the over-fill / phantom-cancel incidents on 05-22.
        """
        if self._tws is None or not self._tws.is_connected:
            return None, False
        # If the receive channel is known-broken, don't even pretend to know.
        if hasattr(self._tws, "is_receive_broken") and self._tws.is_receive_broken():
            return None, False
        try:
            positions, fresh = self._tws.get_positions_snapshot(timeout=2.0)
        except Exception as exc:
            logger.warning("get_position_confirmed(%s) — snapshot failed: %s",
                           symbol, exc)
            return None, False
        if not fresh:
            return None, False                  # timed out → UNKNOWN, never flat
        sym_up = symbol.upper()
        for sym, info in positions.items():
            if str(sym).upper() != sym_up:
                continue
            try:
                qty  = int(info.get("qty", 0))
                side = str(info.get("side", "")).upper()
                return (qty if side == "LONG" else -qty), True
            except (TypeError, ValueError):
                return None, False
        return 0, True                          # fresh snapshot, symbol absent → flat

    def is_confirmed_flat(self, symbol: str) -> bool:
        """
        True ONLY when a FRESH snapshot confirms zero shares. Unknown
        (receive broken / timeout) and non-zero both return False — i.e.
        'unknown' is treated as 'NOT flat' (fail-closed). Use this for every
        release / clear decision instead of `get_actual_position() == 0`.
        """
        qty, fresh = self.get_position_confirmed(symbol)
        return fresh and qty == 0

    def get_actual_position(self, symbol: str) -> int:
        """
        Best-effort SIGNED position qty for `symbol` (positive=LONG,
        negative=SHORT, 0=flat). Returns 0 when the position is unknown.

        WARNING: a 0 from this method does NOT prove the position is flat — it
        can also mean the snapshot failed / receive is dead. For any decision
        that releases a guard, frees an in-flight slot, or clears
        reconciliation, use get_position_confirmed() / is_confirmed_flat()
        instead so 'unknown' is never mistaken for 'flat'.
        """
        qty, fresh = self.get_position_confirmed(symbol)
        return qty if (fresh and qty is not None) else 0

    # ── Pending-reconciliation public API ─────────────────────────────────

    def is_pending_reconciliation(self, symbol: str) -> bool:
        """True if `symbol` is currently being auto-flattened — block entries."""
        with self._pending_reconciliation_lock:
            return symbol in self._pending_reconciliation

    def mark_pending_reconciliation(self, symbol: str) -> None:
        """Add `symbol` to the pending set. Idempotent."""
        with self._pending_reconciliation_lock:
            self._pending_reconciliation.add(symbol)
        logger.warning("[%s] marked pending_reconciliation — entries blocked", symbol)

    def clear_pending_reconciliation(self, symbol: str) -> None:
        """Remove `symbol` from the pending set. Called after IBKR confirms flat."""
        with self._pending_reconciliation_lock:
            self._pending_reconciliation.discard(symbol)
        logger.info("[%s] cleared pending_reconciliation", symbol)

    # ── Single flatten authority (2026-05-26, P2 #8) ──────────────────────

    def _begin_flatten(self, symbol: str) -> bool:
        """
        Claim exclusive flatten rights for `symbol`. Returns True if the claim
        was granted, False if a flatten is already in progress (caller must
        skip). Released by _end_flatten — held across the reconcile thread so a
        second close path can't pile on mid-flatten.
        """
        with self._flattening_lock:
            if symbol in self._flattening_syms:
                return False
            self._flattening_syms.add(symbol)
            return True

    def _end_flatten(self, symbol: str) -> None:
        """Release the flatten claim. Safe to call even if no claim was held."""
        with self._flattening_lock:
            self._flattening_syms.discard(symbol)

    def is_flattening(self, symbol: str) -> bool:
        """True if a flatten is currently in progress for `symbol`."""
        with self._flattening_lock:
            return symbol in self._flattening_syms

    # 2026-05-22 (Fix #4): heartbeat that auto-clears stale reconcile flags.
    # If IBKR reports position=0 for a pending symbol, the close already
    # finished but the reconcile thread either gave up or never observed
    # the final settle — unblock the symbol so it can resume taking entries.
    def _ensure_reconcile_heartbeat(self) -> None:
        """Idempotent: start the heartbeat thread on first call."""
        with self._reconcile_heartbeat_lock:
            if self._reconcile_heartbeat_started:
                return
            self._reconcile_heartbeat_started = True
        threading.Thread(
            target=self._reconcile_heartbeat_loop,
            daemon=True,
            name="reconcile-heartbeat",
        ).start()
        logger.info("reconcile-heartbeat thread started (interval=%.1fs)",
                    self._reconcile_heartbeat_interval)

    def _reconcile_heartbeat_loop(self) -> None:
        while True:
            try:
                time.sleep(self._reconcile_heartbeat_interval)
                with self._pending_reconciliation_lock:
                    pending = list(self._pending_reconciliation)
                if not pending:
                    continue
                if self._tws is None or not self._tws.is_connected:
                    continue
                for sym in pending:
                    try:
                        # 2026-05-26 (P0 #3): only clear on a FRESH confirmed-flat
                        # snapshot. is_confirmed_flat returns False when receive is
                        # broken / the snapshot times out, so a dead socket can no
                        # longer auto-clear a still-open position's guard.
                        flat = self.is_confirmed_flat(sym)
                    except Exception as exc:
                        logger.warning(
                            "reconcile-heartbeat: is_confirmed_flat(%s) "
                            "failed: %s", sym, exc)
                        continue
                    if flat:
                        self.clear_pending_reconciliation(sym)
                        logger.warning(
                            "[%s] reconcile-heartbeat: IBKR confirms flat — "
                            "auto-clearing stale pending_reconciliation flag",
                            sym)
            except Exception as exc:
                logger.error("reconcile-heartbeat loop error: %s", exc)

    # ── Stop hit handler — cancel all orders for symbol ───────────────────

    def handle_stop_hit(self, symbol: str):
        """
        Called when ANY stop order fills (STOP PRICE or STOP LIMIT or STP Market).

        Immediately cancels ALL other live orders for this symbol:
          - stp_mkt (if not the one that filled)
          - stop_price (if not the one that filled)
          - stop_lmt (if not the one that filled)
          - wide_stp (if somehow still alive)

        Then clears self._symbol_orders[symbol].
        """
        logger.info("[%s] STOP HIT — cancelling all remaining orders for symbol", symbol)  # Log stop hit event

        # Get all tracked order IDs for this symbol
        with self._symbol_orders_lock:                   # Thread-safe read of symbol orders
            sym_orders = dict(self._symbol_orders.get(symbol, {}))  # Copy order map

        if not sym_orders:                               # No orders tracked for this symbol
            logger.warning("[%s] handle_stop_hit called but no orders tracked", symbol)  # Log missing symbol
            return                                       # Nothing to cancel

        # Determine the channel from any existing placed order
        channel = "TWS"                                  # Default channel
        with self._placed_lock:                          # Thread-safe read of placed orders
            for ref, info in self._placed_orders.items():  # Search placed orders for this symbol
                if symbol in ref:                        # Found an order for this symbol
                    channel = info.get("channel", "TWS")  # Use its channel
                    break                                # Found channel — stop searching

        # Cancel every tracked order for this symbol (3x retry per cancel via _cancel_order_safe)
        cancel_labels = ["stp_mkt", "stop_price", "stop_lmt", "wide_stp", "parent"]  # All order keys (no target — not used)
        for label in cancel_labels:                      # Iterate each order type
            oid_str = sym_orders.get(label)              # Get order ID for this type
            if oid_str:                                  # Order ID exists — attempt cancel
                self._cancel_order_safe(oid_str, channel, label, symbol)  # Cancel with 3x retry

        # Clear the symbol from per-symbol tracking
        with self._symbol_orders_lock:                   # Thread-safe removal
            self._symbol_orders.pop(symbol, None)        # Remove symbol entirely from tracking
        with self._oca_tracking_lock:                    # Also clear OCA tracking
            self._oca_tracking.pop(symbol, None)

        # ── Conid sweep: cancel ALL live orders for this symbol's conid ──
        # Catches orphan orders not in _symbol_orders (reconnect gaps, tracking failures)
        conid = None
        for ref, info in list(self._placed_orders.items()):
            if symbol in ref and info.get("ticket"):
                conid = info["ticket"].conid
                break

        if conid and self._tws and self._tws.is_connected:
            try:
                # Get all open orders and cancel any matching this conid
                import time as _t
                _t.sleep(0.3)                            # Let IBKR process the individual cancels first
                all_orders = self._tws.get_all_open_orders() if hasattr(self._tws, 'get_all_open_orders') else []
                swept = 0
                for o in all_orders:
                    if o.get("conid") == conid:
                        oid = o.get("oid")
                        if oid:
                            self._tws.cancel_order_by_id(oid)
                            swept += 1
                if swept:
                    logger.info("[%s] Conid sweep: cancelled %d orphan order(s) for conid=%d",
                                symbol, swept, conid)
            except Exception as exc:
                logger.debug("[%s] Conid sweep failed: %s", symbol, exc)

        # Clean up _placed_orders for this symbol
        with self._placed_lock:
            to_remove = [ref for ref in self._placed_orders if symbol in ref]
            for ref in to_remove:
                del self._placed_orders[ref]

        logger.info("[%s] STOP HIT cleanup complete — all orders cancelled, symbol cleared",
                    symbol)                              # Log cleanup completion

        # 2026-06-10: ALSO clear the bot's internal _open_positions. The clean
        # orderStatus=Filled hook (which calls _on_stop_filled) is bypassed on
        # this technical/cancel stop-hit path, which left a PHANTOM position (bot
        # thought it was still in the symbol → blocked re-entry, stale dashboard).
        # Idempotent (bot-side pop uses default None), so a double-call is safe.
        if self._on_stop_filled is not None:
            try:
                self._on_stop_filled(symbol)
            except Exception as exc:
                logger.error("[%s] stop-hit position-clear notify failed: %s", symbol, exc)

    # ── Status and diagnostics ────────────────────────────────────────────

    # ── Position queries ──────────────────────────────────────────────────

    def get_positions(self, timeout: float = 5.0) -> Dict[str, Dict]:
        """
        Return a fresh snapshot of all open positions from IB Gateway via TWS.
        Uses reqPositions() — push-based, accurate, no CP API needed.
        Returns dict: symbol → {side: 'LONG'|'SHORT', qty: float, avg: float}
        """
        if self._tws and self._tws.is_connected:
            try:
                positions, _fresh = self._tws.get_positions_snapshot(timeout=timeout)
                return positions
            except Exception as exc:
                logger.warning("get_positions via TWS failed: %s", exc)

        logger.warning("get_positions: TWS not connected — returning empty")
        return {}

    def get_positions_snapshot(self, timeout: float = 5.0):
        """
        Engine-level delegator to TWSClient.get_positions_snapshot so callers
        holding the ENGINE (e.g. the reconcile-to-flat heartbeat in macd_bot)
        receive the (positions, fresh) tuple. Returns ({}, False) when the
        socket is not connected — fresh=False means position UNKNOWN and MUST
        never be treated as flat. Mirrors get_positions() but preserves fresh.
        """
        if self._tws and self._tws.is_connected:
            try:
                return self._tws.get_positions_snapshot(timeout=timeout)
            except Exception as exc:
                logger.warning("get_positions_snapshot via TWS failed: %s", exc)
        return {}, False

    # ── Conid resolution ──────────────────────────────────────────────────

    def resolve_conid(self, symbol: str, timeout: float = 5.0) -> Optional[int]:
        """
        Resolve symbol → IBKR conid via TWS reqMatchingSymbols().
        Cached after first lookup. Returns None if lookup fails.
        """
        if not hasattr(self, '_conid_cache'):
            self._conid_cache: Dict[str, int] = {}

        if symbol in self._conid_cache:                  # Cache hit
            return self._conid_cache[symbol]

        if self._tws and self._tws.is_connected:
            try:
                conid = self._tws.resolve_conid(symbol, timeout=timeout)
                if conid:
                    self._conid_cache[symbol] = conid
                    logger.info("Resolved conid: %s → %d", symbol, conid)
                    return conid
            except Exception as exc:
                logger.warning("resolve_conid(%s) failed: %s", symbol, exc)

        logger.error("Could not resolve conid for %s", symbol)
        return None

    # ── EoD square-off ────────────────────────────────────────────────────

    def eod_square_off(self, eod_time_et: str = "15:50"):
        """
        BUG-001 Fix: 60s grace wait + skip PreSubmitted + protect EOD_CLOSE orders.
        BUG-002 Fix: unique cOID per MKT close (millisecond timestamp).
        BUG-003 Fix: _eod_fired flag prevents re-fire; reset at 20:00 EST.
        BUG-004 Fix: validate EoDExitTime at call time — warn if outside 13:00-16:00.
        """
        import pytz
        from datetime import datetime as dt

        # BUG-004: Validate EoDExitTime at startup
        try:
            _h, _m = map(int, eod_time_et.split(":"))
            _mins  = _h * 60 + _m
            if not (13 * 60 <= _mins <= 16 * 60):
                logger.error(
                    "EoDExitTime '%s' is OUTSIDE US market hours (13:00-16:00 ET). "
                    "Check config.json — did you mean '%s' (PM)?",
                    eod_time_et,
                    f"{_h + 12}:{_m:02d}" if _h < 12 else eod_time_et)
        except Exception:
            logger.error("EoDExitTime '%s' is invalid — expected HH:MM", eod_time_et)

        def _eod_loop():
            est          = pytz.timezone("America/New_York")
            h, m         = map(int, eod_time_et.split(":"))
            _eod_fired   = False                         # BUG-003: prevents re-fire
            logger.info("EoD square-off scheduled for %s ET", eod_time_et)

            while True:
                now_et  = dt.now(est)
                target  = now_et.replace(hour=h, minute=m,  second=0, microsecond=0)
                reset_t = now_et.replace(hour=20, minute=0, second=0, microsecond=0)

                # BUG-003: Reset flag at 20:00 EST — well after all EoD activity
                if now_et >= reset_t and _eod_fired:
                    _eod_fired = False
                    logger.info("EoD flag reset at 20:00 ET — ready for next session")

                if now_et >= target and not _eod_fired:
                    _eod_fired = True                    # Mark fired immediately — no re-fire
                    break
                elif now_et < target:
                    time.sleep(min((target - now_et).total_seconds(), 30))
                else:
                    time.sleep(30)                       # Past target, already fired — wait for reset

            logger.warning("EoD TRIGGERED at %s ET — cancelling brackets, then flattening",
                           eod_time_et)

            # 2026-05-26 (P2 #7): suppress the partial-fill-gap detector for the
            # whole EoD flatten window. The reqGlobalCancel below makes every OCA
            # sibling terminal with filled=0; without this the gap detector would
            # fire an auto-flatten for every open symbol on top of the EoD MKT
            # closes and over-cover the book (the 15:50→15:51 flip storm).
            self._suppress_gap_until = time.time() + 600   # 10 min covers cancel+grace+closes
            logger.info("EoD: partial-fill-gap detector suppressed for 600s")

            # ── Step 1: Cancel bracket orders only, skip PreSubmitted (in-flight) ──
            # BUG-001b/c: Skip PreSubmitted orders and protect EOD_CLOSE MKT orders
            with self._symbol_orders_lock:
                all_sym_orders = {s: dict(v) for s, v in self._symbol_orders.items()}

            for sym, sym_orders in all_sym_orders.items():
                for label, oid_str in sym_orders.items():
                    if not oid_str:
                        continue
                    # BUG-001c: Skip orders that are EOD_CLOSE (in-flight MKT closes)
                    if "EOD_" in str(oid_str):
                        logger.info("EoD: skipping EOD_CLOSE order %s %s", sym, oid_str)
                        continue
                    # BUG-001b: Skip PreSubmitted/Submitted — IBKR is routing them
                    if self._tws and self._tws.is_connected:
                        raw_id = oid_str.split("_", 1)[1] if "_" in oid_str else oid_str
                        try:
                            status_d = self._tws.get_order_status(int(raw_id))
                            if status_d:
                                st = str(status_d.get("status", "")).upper()
                                if st in ("PRESUBMITTED", "SUBMITTED"):
                                    logger.info("EoD: skipping %s %s (status=%s)",
                                                sym, label, st)
                                    continue
                        except Exception:
                            pass
                    self._cancel_order_safe(oid_str, "TWS", label, sym)

            # ── Step 2a: reqGlobalCancel — kill ALL remaining orders on the account ──
            # This catches orphan orders not in _symbol_orders (reconnect gaps, tracking failures).
            # Must happen BEFORE MKT close orders — orphan BUY/SELL orders consume margin.
            # Without this, MKT close orders get rejected with REG T margin errors.
            # SHADOW (2026-08-06): kill the local book instead of the IBKR one.
            if SHADOW_ON and SHADOW is not None:
                logger.info("EoD: shadow global cancel — killing all working shadow orders")
                SHADOW.global_cancel()
                time.sleep(1)
            elif self._tws and self._tws.is_connected:
                logger.info("EoD: reqGlobalCancel — killing all orphan orders to free margin")
                self._tws.reqGlobalCancel()              # LIVE PATH (SHADOW_MODE=0)
                time.sleep(3)                            # Let IBKR process the global cancel

            # ── Step 2b: BUG-001a: 10s grace wait — let cancels settle ──
            logger.info("EoD: 10s grace wait — allowing cancels to settle")
            time.sleep(10)

            # ── Step 3: Get remaining positions and send MKT closes ────────────
            positions = self.get_positions()
            if not positions:
                logger.info("EoD: all positions flat after grace wait ✓")
                return

            logger.warning("EoD: %d positions remain — sending MKT closes", len(positions))

            for sym, pos in positions.items():
                conid = pos.get("conid") or (self._conid_cache.get(sym)
                                              if hasattr(self, "_conid_cache") else None)
                if not conid:
                    logger.warning("EoD: no conid for %s — skipping", sym)
                    continue

                close_side = "SELL" if pos["side"] == "LONG" else "BUY"
                qty        = int(pos["qty"])
                # FIX: defensive guard — skip 0-qty ghost entries that may slip
                # through despite the position() callback filter. Submitting a
                # 0-size MKT order causes "size value cannot be zero" rejection.
                if qty <= 0:
                    logger.info("EoD: skipping %s — qty=%d (ghost/closed)", sym, qty)
                    continue
                # BUG-002: unique cOID — millisecond timestamp guarantees no duplicate rejection
                unique_coid = f"{sym}_EOD_{int(time.time() * 1000)}"

                try:
                    self.submit_market_close(
                        symbol=sym, conid=conid,
                        side=close_side, quantity=qty,
                        order_ref=unique_coid)
                    logger.info("EoD MKT %s %s %d (cOID=%s)", close_side, sym, qty, unique_coid)
                except Exception as exc:
                    logger.error("EoD MKT close failed for %s: %s", sym, exc)

            logger.info("EoD square-off complete")

        threading.Thread(target=_eod_loop, daemon=True, name="eod-squareoff").start()


    # ── Direction monitor (5s flip detector) ─────────────────────────────

    def start_direction_monitor(self, open_positions: Dict,
                                 pos_lock: threading.Lock,
                                 interval: int = 5):
        """
        Background thread that checks every `interval` seconds whether any
        open position has flipped direction on IBKR (e.g. internal=SHORT
        but IBKR=LONG — means wide STP or another order fired silently).

        On flip detected:
          1. Cancel all tracked orders (handle_stop_hit)
          2. Conid sweep — cancel any remaining live orders
          3. Send MKT flatten order
          4. Clear internal state for that symbol

        open_positions: the bot's internal position dict (shared reference)
        pos_lock:       threading.Lock protecting open_positions
        interval:       poll frequency in seconds (default 5)
        """
        self._stop_dir_monitor = threading.Event()

        def _loop():
            logger.info("Direction monitor started (every %ds)", interval)
            while not self._stop_dir_monitor.wait(timeout=interval):
                try:
                    # Skip reqPositions entirely if no positions open — prevents socket flooding
                    with pos_lock:
                        if not open_positions:
                            continue                      # Nothing to monitor — skip API call
                    ibkr_actual = self.get_positions()
                except Exception:
                    continue

                with pos_lock:
                    snapshot = dict(open_positions)

                for sym, pos in snapshot.items():
                    int_dir = pos.get("direction", "")
                    if sym not in ibkr_actual:
                        continue                          # Not on IBKR — handled by reconcile
                    ibkr_info = ibkr_actual[sym]
                    ibkr_side = ibkr_info.get("side", "")
                    ibkr_qty  = int(ibkr_info.get("qty", 0))

                    flip = ((int_dir == "SHORT" and ibkr_side == "LONG") or
                            (int_dir == "LONG"  and ibkr_side == "SHORT"))
                    if not flip:
                        continue

                    # 2026-05-26 (P2 #8): if a flatten / reconciliation is already
                    # working this symbol, the "flip" is just the close in flight
                    # — do NOT fire a second flatten on top of it. Piling on is
                    # what over-covered shorts into longs in the 15:51 storm.
                    if self.is_flattening(sym) or self.is_pending_reconciliation(sym):
                        logger.info("[%s] Dir monitor: flip seen but a flatten / "
                                    "reconciliation is already in progress — "
                                    "skipping duplicate flatten", sym)
                        continue

                    logger.warning(
                        "[%s] DIRECTION FLIP — internal=%s IBKR=%s %d. "
                        "Cancelling all orders and flattening.",
                        sym, int_dir, ibkr_side, ibkr_qty)

                    # Step 1: cancel all tracked orders
                    self.handle_stop_hit(sym)

                    # Step 2: conid sweep
                    conid = (ibkr_info.get("conid") or
                             (self._conid_cache.get(sym)
                              if hasattr(self, '_conid_cache') else None))
                    if conid and self._tws and self._tws.is_connected:
                        try:
                            live = self._tws.get_live_orders_tws(timeout=3.0)
                            for o in live:
                                if str(o.get("conid","")) == str(conid):
                                    oid = o.get("orderId", 0)
                                    if oid:
                                        try:
                                            self._tws.cancelOrder(int(oid))
                                            logger.info("[%s] Dir monitor sweep cancelled oid=%s",
                                                        sym, oid)
                                        except Exception:
                                            pass
                        except Exception as _se:
                            logger.debug("[%s] Dir monitor sweep: %s", sym, _se)

                    # Step 3: MKT flatten
                    close_side = "SELL" if ibkr_side == "LONG" else "BUY"
                    if conid and ibkr_qty > 0:
                        try:
                            self.submit_market_close(
                                symbol=sym, conid=conid,
                                side=close_side, quantity=ibkr_qty)
                            logger.warning("[%s] Dir monitor: MKT %s %d sent",
                                           sym, close_side, ibkr_qty)
                        except Exception as _me:
                            logger.error("[%s] Dir monitor: MKT flatten failed: %s", sym, _me)

                    # Step 4: clear internal state
                    with pos_lock:
                        open_positions.pop(sym, None)
                    with self._symbol_orders_lock:
                        self._symbol_orders.pop(sym, None)
                    logger.info("[%s] Dir monitor: internal state cleared", sym)

        self._dir_monitor_thread = threading.Thread(
            target=_loop, daemon=True, name="direction-monitor")
        self._dir_monitor_thread.start()

    def stop_direction_monitor(self):
        """Stop the direction monitor thread cleanly."""
        if hasattr(self, '_stop_dir_monitor'):
            self._stop_dir_monitor.set()
        if hasattr(self, '_dir_monitor_thread') and self._dir_monitor_thread.is_alive():
            self._dir_monitor_thread.join(timeout=3)

    def set_reconnect_callback(self, callback: Callable):
        """
        Register a callback fired after watchdog successfully reconnects.
        Called with (new_tws_client) so caller can resubscribe market data.
        macd_bot registers this at startup to restore tick subscriptions on reconnect.
        """
        self._on_reconnect = callback
        logger.info("Reconnect callback registered")

    def get_historical_bars(self, conid: int, bar_size: str = "30 secs",
                             duration: str = "2 D", timeout: float = 30.0) -> list:
        """
        Delegate to TWSClient.get_historical_bars().
        Called by indicators.py seed() via tws_engine.get_historical_bars().
        Fetches last N bars from IBKR regardless of date/time.
        duration="2 D" covers yesterday + today so we always get enough bars.
        """
        if self._tws and self._tws.is_connected:
            return self._tws.get_historical_bars(
                conid=conid, period=duration,
                bar_size=bar_size, timeout=timeout)
        return []

    def get_stats(self) -> Dict:
        """Return a copy of placement statistics for monitoring."""
        with self._stats_lock:                           # Thread-safe read
            return dict(self._stats)                     # Return snapshot copy

    def get_queue_depth(self) -> int:
        """Number of orders currently waiting in the queue."""
        return self._queue.qsize()                       # Thread-safe queue size

    def get_rates(self) -> Dict[str, int]:
        """Current messages-per-second for each channel."""
        return {
            "tws_rate": self._tws_rate.current_rate(),   # TWS current rate
            "tws_headroom": self._tws_rate.headroom(),   # TWS remaining capacity

        }


# =============================================================================
# STANDALONE USAGE — demonstrates the engine with a sample bracket order
# =============================================================================

if __name__ == "__main__":
    """
    Example usage showing how to integrate the TWSEngine
    with the existing trading bot.
    """
    import threading

    # ── Shared position state (owned by the main bot) ─────────────────────
    open_positions: Dict[str, Dict] = {}
    pos_lock = threading.Lock()

    def on_fill(symbol, qty, avg_price):
        """Called when parent fill is confirmed by verification thread."""
        logger.info("CALLBACK: %s filled  qty=%.0f  avg=%.4f", symbol, qty, avg_price)
        with pos_lock:
            open_positions[symbol] = {
                "direction": "SHORT",
                "qty"      : qty,
                "entry"    : avg_price,
            }

    def on_cancel(symbol, reason):
        """Called when bracket is cancelled due to no fill."""
        logger.info("CALLBACK: %s cancelled  reason=%s", symbol, reason)

    # ── Initialize engine ─────────────────────────────────────────────────
    engine = TWSEngine()
    if not engine.start():
        logger.error("Failed to start — neither TWS nor API available")
        exit(1)

    # ── Resolve conids ────────────────────────────────────────────────────
    AAPL_CONID = engine.resolve_conid("AAPL") or 265598

    # ── Start direction monitor ───────────────────────────────────────────
    engine.start_direction_monitor(open_positions, pos_lock, interval=5)

    # ── Schedule EoD square-off ───────────────────────────────────────────
    engine.eod_square_off(eod_time_et="15:50")

    # ── Submit bracket ────────────────────────────────────────────────────
    bracket_result = engine.submit_bracket(
        symbol="AAPL",
        conid=AAPL_CONID,
        direction="SHORT",
        quantity=500,
        entry_price=272.05,
        stop_price=272.65,
        mpl=0.60,
        wide_stp_multiplier=1.1,
        on_fill=on_fill,
        on_cancel=on_cancel,
    )
    logger.info("Bracket result: %s", bracket_result)
    logger.info("Positions: %s", engine.get_positions())
    logger.info("Stats: %s", engine.get_stats())

    try:
        time.sleep(30)
    except KeyboardInterrupt:
        pass

    engine.stop()
