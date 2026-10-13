"""
=============================================================================
QTI Fund  |  Model: !JFMX3X_NONCORR_MACD_V3.x
FILE: ibkr_broker.py
DESCRIPTION:
    Interactive Brokers Web API integration layer.

    Responsibilities
    ─────────────────
    • Session management  – authenticate via Client Portal Gateway (CP Gateway)
      and keep the brokerage session alive with /tickle heartbeats.
    • Contract lookup     – resolve a ticker symbol to an IBKR conid.
    • Live market data    – WebSocket stream for real-time bar data.
    • Order management    – place / modify / cancel orders via REST.
    • Position tracking   – query open positions and balances.

    Architecture
    ─────────────
    • IBKRSession   – low-level HTTP + WebSocket client
    • IBKRBroker    – high-level facade called by the strategy loop
    • run_strategy  – example event loop wiring entry_logic + exit_logic

    Prerequisites
    ─────────────
    1. Download the CP Gateway from IBKR and run it locally:
          java -jar root/ibgroup.web.core.iblink.router.clientportal.gw.jar
               --conf root/conf.yaml
       By default it listens on https://localhost:5000
       (self-signed cert — set IBKR_VERIFY_SSL=false in .env for local dev)

    2. Log in once via the browser at https://localhost:5000 to create a session.

    3. Set environment variables (or use a .env file):
          IBKR_BASE_URL   = https://localhost:5000/v1/api   (default)
          IBKR_ACCOUNT_ID = DU123456
          IBKR_VERIFY_SSL = false                           (local Gateway only)

    IBKR Web API docs:
    https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-trading/
=============================================================================
"""

from __future__ import annotations
import contextlib

import asyncio
import json
import logging
import os
import time
import threading
from collections import deque
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

EST = ZoneInfo("America/New_York")
from typing import Any, Callable, Dict, List, Optional

import requests
import requests.cookies
import urllib3
import websocket  # pip install websocket-client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from indicators import (
    IndicatorPipeline,
    IndexMACDStore,
    is_tick_resolution,
    is_second_resolution,
    is_minute_resolution,
    resolution_is_tick,
    resolution_is_second,
    resolution_is_minute,
    _load_cfg as _load_ind_cfg,
)

# ─── Logger ───────────────────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

def _setup_logging():
    """Configure dual logging: terminal + daily file (logs/logs_DD_MM_YY.txt)."""
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Clear any handlers added by other modules (indicators.py, entry_logic.py)
    root.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [MACD] %(levelname)s - %(message)s")

    # Console handler (terminal)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # File handler (daily rotating by name)
    tag = datetime.now(EST).strftime("%d_%m_%y")       # DD_MM_YY  (EST)
    log_path = os.path.join(LOG_DIR, f"logs_{tag}.txt")
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Suppress duplicate root handlers from other modules
    for name in ("indicators", "entry_logic", "exit_logic"):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True

    logging.getLogger("ibkr_broker").info("Log file → %s", log_path)

_setup_logging()
logger = logging.getLogger("ibkr_broker")

# ─── Environment / Defaults ──────────────────────────────────────────────────
BASE_URL   = os.getenv("IBKR_BASE_URL",   "https://localhost:5000/v1/api")
ACCOUNT_ID = "DUXXXXXXX"
VERIFY_SSL = os.getenv("IBKR_VERIFY_SSL", "false").lower() not in ("false", "0", "no")

# Shared session cookie file — written by any script after successful auth,
# read by any script that needs to reuse the authenticated session.
SESSION_FILE = os.path.join(os.path.expanduser("~"), ".ibkr_session.json")

# Field tags used when subscribing to WebSocket market-data streams
# 31=last price, 84=bid, 86=ask, 85=ask size, 88=bid size,
# 7059=last size, 70=high, 71=low, 6119=server_id
# 7762=bar volume (shares traded this bar), 87=volume (daily cumulative)
MD_FIELDS = ["31", "84", "85", "86", "88", "7059", "70", "71", "7762", "87"]


# =============================================================================
# LOW-LEVEL SESSION
# =============================================================================

class IBKRSession:
    """
    Thin wrapper around the IBKR CP Gateway REST API.

    All requests go to the CP Gateway running on localhost.  The Gateway
    handles OAuth / cookie management; we only need to keep the session
    alive by calling /tickle at least once per minute.
    """

    def __init__(
        self,
        base_url: str = BASE_URL,
        account_id: str = ACCOUNT_ID,
        verify_ssl: bool = VERIFY_SSL,
    ):
        self.base_url   = base_url.rstrip("/")
        self.account_id = account_id
        self.verify_ssl = verify_ssl
        self._session   = requests.Session()
        self._session.verify = verify_ssl
        self._tickle_thread: Optional[threading.Thread] = None
        self._stop_tickle   = threading.Event()
        self._http_lock = threading.Lock()   # serialises all HTTP calls

        # Attempt to load a previously saved session cookie so that
        # scripts launched AFTER the initial browser login can reuse
        # the same authenticated session without re-logging in.
        self._load_cookies()

    # ── Cookie persistence ────────────────────────────────────────────

    def _save_cookies(self):
        """Persist session cookies to disk so other scripts can reuse them."""
        try:
            cookies = {}
            for c in self._session.cookies:
                cookies[c.name] = {
                    "value":  c.value,
                    "domain": c.domain,
                    "path":   c.path,
                }
            with open(SESSION_FILE, "w") as f:
                json.dump(cookies, f)
            logger.debug("Session cookies saved → %s", SESSION_FILE)
        except Exception as exc:
            logger.warning("Could not save session cookies: %s", exc)

    def _load_cookies(self):
        """Load previously saved cookies from disk (if the file exists)."""
        if not os.path.exists(SESSION_FILE):
            return
        try:
            with open(SESSION_FILE) as f:
                cookies = json.load(f)
            for name, info in cookies.items():
                self._session.cookies.set(
                    name,
                    info["value"],
                    domain=info.get("domain", "localhost"),
                    path=info.get("path", "/"),
                )
            logger.info("Loaded saved session cookies from %s", SESSION_FILE)
        except Exception as exc:
            logger.warning("Could not load session cookies: %s", exc)

    # ── Internal helpers ──────────────────────────────────────────────────

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def _get(self, path: str, params: Dict = None) -> Any:
        resp = self._session.get(self._url(path), params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: Any = None, _retries: int = 3) -> Any:
        """POST with exponential backoff on 429 (throttle) responses."""
        import time as _t
        for attempt in range(_retries):
            resp = self._session.post(
                self._url(path),
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=20,   # increased from 10s — CP Gateway needs more time during mass fills
            )
            if resp.status_code == 429:
                wait = 0.5 * (2 ** attempt)
                logger.warning("IBKR throttle (429) on POST %s — waiting %.1fs (attempt %d/%d)",
                                path, wait, attempt+1, _retries)
                _t.sleep(wait)
                continue
            if not resp.ok:
                logger.warning("IBKR %d on POST %s — body: %s",
                               resp.status_code, path, resp.text[:300])
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str, _retries: int = 3) -> Any:
        """DELETE with exponential backoff on 429 (throttle) responses.
        Logs full 400 response body per IBKR API team request — helps diagnose
        cancel failures that return 400 Bad Request."""
        import time as _t
        for attempt in range(_retries):
            resp = self._session.delete(self._url(path), timeout=10)
            if resp.status_code == 429:
                wait = 0.5 * (2 ** attempt)
                logger.warning("IBKR throttle (429) on DELETE %s — waiting %.1fs (attempt %d/%d)",
                                path, wait, attempt+1, _retries)
                _t.sleep(wait)
                continue
            if resp.status_code == 400:
                # Log full body so we can report exact error to IBKR API team
                try:
                    body = resp.json()
                except Exception:
                    body = resp.text
                logger.warning("IBKR 400 on DELETE %s — body: %s", path, body)
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()
        return resp.json()

    # ── Session management ────────────────────────────────────────────────

    def tickle(self) -> Dict:
        """
        Keep the brokerage session alive.  Must be called at least once
        per minute, otherwise the Gateway will close the session.
        Returns the session status payload.
        """
        result = self._get("/tickle")
        self._save_cookies()
        return result

    def auth_status(self) -> Dict:
        """Query authentication and brokerage-session status."""
        result = self._get("/iserver/auth/status")
        self._save_cookies()
        return result

    def reauthenticate(self) -> Dict:
        """Trigger re-authentication if the session has expired."""
        return self._post("/iserver/reauthenticate")

    def start_tickle_thread(self, interval: int = 55):
        """
        Spawn a background thread that calls /tickle every `interval` seconds
        so the brokerage session does not time out.
        """
        if self._tickle_thread and self._tickle_thread.is_alive():
            return  # already running

        self._stop_tickle.clear()

        def _loop():
            _consecutive_401 = 0
            while not self._stop_tickle.is_set():
                try:
                    self.tickle()
                    _consecutive_401 = 0
                    logger.debug("tickle OK")
                except Exception as exc:
                    err_str = str(exc)
                    if "401" in err_str:
                        _consecutive_401 += 1
                        logger.warning("tickle 401 (attempt %d) — session expired, "
                                       "trying reauthenticate...", _consecutive_401)
                        try:
                            self.reauthenticate()
                            logger.info("Reauthenticate call sent — waiting for session refresh")
                        except Exception as re_exc:
                            logger.warning("Reauthenticate failed: %s", re_exc)
                        if _consecutive_401 >= 5:
                            logger.error("Session lost after %d attempts. "
                                         "Please re-login at https://localhost:5000",
                                         _consecutive_401)
                    else:
                        logger.warning("tickle failed: %s", exc)
                self._stop_tickle.wait(interval)

        self._tickle_thread = threading.Thread(target=_loop, daemon=True, name="ibkr-tickle")
        self._tickle_thread.start()
        logger.info("Tickle thread started (every %ds)", interval)

    def stop_tickle_thread(self):
        self._stop_tickle.set()

    def logout(self):
        """End the brokerage session cleanly."""
        self.stop_tickle_thread()
        try:
            self._get("/logout")
            logger.info("Logged out from IBKR Web API")
        except Exception as exc:
            logger.warning("Logout error: %s", exc)

    # ── Account ───────────────────────────────────────────────────────────

    def get_accounts(self) -> List[Dict]:
        """Return a list of accounts available to this session."""
        return self._get("/portfolio/accounts")

    def get_account_summary(self, account_id: str) -> Dict:
        return self._get(f"/portfolio/{account_id}/summary")

    def get_positions(self, account_id: str) -> List[Dict]:
        # IBKR confirmed: /portfolio2/{accountId}/positions has no cache lag
        # unlike /portfolio/{accountId}/positions/0 which can lag 2-8 minutes
        return self._get(f"/portfolio2/{account_id}/positions")

    # ── Contract search ───────────────────────────────────────────────────

    def search_conid(self, symbol: str, sec_type: str = "STK", exchange: str = "SMART") -> Optional[int]:
        """
        Resolve a symbol to an IBKR contract ID (conid).

        Uses /iserver/secdef/search which returns matching contracts.
        We pick the first STK contract on the given exchange.
        """
        try:
            results = self._post("/iserver/secdef/search", {"symbol": symbol, "name": False, "secType": sec_type})
            for r in results:
                # Each result has a 'contracts' list with exchange/conid pairs
                for contract in r.get("contracts", []):
                    if exchange.upper() in contract.get("exchange", "").upper() or exchange == "SMART":
                        return int(r["conid"])
            # Fallback: return first result's conid
            if results:
                return int(results[0]["conid"])
        except Exception as exc:
            logger.error("conid lookup failed for %s: %s", symbol, exc)
        return None

    # ── Market data snapshots ─────────────────────────────────────────────

    def snapshot(self, conids: List[int], fields: List[str] = None) -> List[Dict]:
        """
        Request a top-of-book market data snapshot.
        The FIRST call for a conid is a "pre-flight" that opens the stream;
        data will be returned on subsequent calls.
        """
        fields = fields or MD_FIELDS
        conids_str = ",".join(str(c) for c in conids)
        fields_str = ",".join(fields)
        return self._get(
            "/iserver/marketdata/snapshot",
            params={"conids": conids_str, "fields": fields_str},
        )

    # ── Orders ────────────────────────────────────────────────────────────

    def place_order(self, account_id: str, orders: List[Dict]) -> List[Dict]:
        """
        Submit one or more order tickets to IBKR.

        `orders` is a list of order dicts.  For a bracket order, include
        the parent and child dicts in the same list (IBKR handles linkage
        via cOID/parentId fields — see _build_ibkr_bracket below).

        Returns the raw IBKR response, which may include order reply
        messages requiring confirmation.
        """
        return self._post(f"/iserver/account/{account_id}/orders", {"orders": orders})

    def reply_confirm(self, message_id: str) -> Dict:
        """
        Confirm an order reply message (IBKR 'fat-finger' check).
        Pass the `id` field from the order reply response.
        """
        return self._post(f"/iserver/reply/{message_id}", {"confirmed": True})

    def suppress_order_messages(self, message_ids: List[str]) -> Dict:
        """
        Suppress specific order reply message types for the current session.
        Recommended to call at session start with common message IDs.
        """
        return self._post("/iserver/questions/suppress", {"messageIds": message_ids})

    def modify_order(self, account_id: str, order_id: str, order: Dict) -> Dict:
        """Modify an existing live order."""
        return self._post(f"/iserver/account/{account_id}/order/{order_id}", order)

    def cancel_order(self, account_id: str, order_id: str) -> Dict:
        """Cancel an open order."""
        return self._delete(f"/iserver/account/{account_id}/order/{order_id}")

    def get_live_orders(self, account_id: str, filters: str = "") -> Dict:
        """Return current open / recently filled orders."""
        params = {"accountId": account_id, "force": "true"}
        if filters:
            params["filters"] = filters
        return self._get("/iserver/account/orders", params=params)


# =============================================================================
# VOLUME PARSER  —  handles IBKR formatted strings like "11.4M", "6.28K"
# =============================================================================

def _parse_vol(v) -> float:
    """
    Convert IBKR volume values to plain float.

    IBKR sometimes sends volume as a formatted string:
        "11.4M"  →  11_400_000.0
        "6.28K"  →  6_280.0
        "1.2B"   →  1_200_000_000.0
        "12345"  →  12345.0
        12345    →  12345.0
        None/""  →  0.0
    """
    if v is None:
        return 0.0
    s = str(v).strip().upper()
    if s == "" or s == "0":
        return 0.0
    try:
        if s.endswith("B"):
            return float(s[:-1]) * 1_000_000_000
        if s.endswith("M"):
            return float(s[:-1]) * 1_000_000
        if s.endswith("K"):
            return float(s[:-1]) * 1_000
        return float(s)
    except (ValueError, TypeError):
        return 0.0


# =============================================================================
# EMBEDDED PRICE LOGGER — zero-lag tick capture for selected symbols
# =============================================================================

class PriceLogger:
    """
    Captures every WebSocket tick for ALL active trading symbols into memory.
    Saves to Excel on demand (called from IBKRBroker.stop()).

    Symbols loaded dynamically from config.json so the logger always matches
    what the bot is actually trading. Used to measure exact tick-level
    entry slippage: compare tick price at signal time vs actual fill price.

    Runs INSIDE the same WebSocket — zero lag, no second connection needed.
    """

    # Populated at runtime from config.json Stocks section — not hard-coded
    PRICE_LOG_SYMBOLS: set = set()

    @classmethod
    def _load_symbols(cls):
        """Load all stocks from config.json (enabled or not — log everything)."""
        try:
            import json as _j, os as _o
            _cfg = _o.path.join(_o.path.dirname(_o.path.abspath(__file__)), "config.json")
            with open(_cfg) as _fh:
                _data = _j.load(_fh)
            cls.PRICE_LOG_SYMBOLS = set(_data.get("Stocks", {}).keys())
        except Exception:
            cls.PRICE_LOG_SYMBOLS = {"AAPL", "TSLA", "NVDA"}   # safe fallback

    def __init__(self):
        PriceLogger._load_symbols()
        self._data: Dict[str, List[Dict]] = {s: [] for s in self.PRICE_LOG_SYMBOLS}
        self._last_price: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._enabled = True
        logger.info("PriceLogger active for %d stocks: %s",
                    len(self.PRICE_LOG_SYMBOLS), sorted(self.PRICE_LOG_SYMBOLS))

    def on_tick(self, symbol: str, data: Dict):
        """Called from _handle_tick on every WebSocket message."""
        if not self._enabled:
            return
        # Add symbol dynamically if it wasn't in config at startup
        if symbol not in self.PRICE_LOG_SYMBOLS:
            self.PRICE_LOG_SYMBOLS.add(symbol)
            with self._lock:
                if symbol not in self._data:
                    self._data[symbol] = []

        price = float(data.get("31", 0) or 0)
        if price == 0:
            return

        # Skip duplicate prices
        if self._last_price.get(symbol) == price:
            return
        self._last_price[symbol] = price

        now = datetime.now(EST)
        bid       = float(data.get("84", 0) or 0)
        ask       = float(data.get("86", 0) or 0)
        high      = float(data.get("70", 0) or 0)
        low       = float(data.get("71", 0) or 0)
        last_size = _parse_vol(data.get("7059", 0))
        bar_vol   = _parse_vol(data.get("7762", 0))
        daily_vol = _parse_vol(data.get("87", 0))

        row = {
            "timestamp"   : now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "last_price"  : price,
            "bid"         : bid,
            "ask"         : ask,
            "spread"      : round(ask - bid, 4) if bid > 0 and ask > 0 else 0,
            "high"        : high,
            "low"         : low,
            "last_size"   : last_size,
            "bar_volume"  : bar_vol,
            "daily_volume": daily_vol,
        }

        with self._lock:
            self._data[symbol].append(row)

        count = len(self._data[symbol])
        if count == 1:
            logger.info("[PriceLog] %s first tick: %.2f  bid=%.2f  ask=%.2f",
                        symbol, price, bid, ask)
        elif count % 500 == 0:
            logger.info("[PriceLog] %s %d ticks  last=%.2f", symbol, count, price)

    def save_excel(self):
        """Save all collected price data to Excel files. Called on shutdown."""
        try:
            from openpyxl import Workbook
            from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
            from openpyxl.utils import get_column_letter
        except ImportError:
            logger.warning("PriceLogger: openpyxl not installed — skipping Excel save")
            return

        tag = datetime.now(EST).strftime("%d_%m_%y")
        script_dir = os.path.dirname(os.path.abspath(__file__))

        headers = [
            "Timestamp", "Last Price", "Bid", "Ask", "Spread",
            "High", "Low", "Last Size", "Bar Volume", "Daily Volume",
        ]
        col_widths = [24, 14, 14, 14, 10, 14, 14, 12, 14, 16]

        header_font  = Font(name="Arial", bold=True, color="FFFFFF", size=11)
        header_fill  = PatternFill("solid", fgColor="2F5496")
        header_align = Alignment(horizontal="center", vertical="center")
        data_font    = Font(name="Arial", size=10)
        thin_border  = Border(
            left=Side(style="thin", color="D9D9D9"),
            right=Side(style="thin", color="D9D9D9"),
            top=Side(style="thin", color="D9D9D9"),
            bottom=Side(style="thin", color="D9D9D9"),
        )
        alt_fill = PatternFill("solid", fgColor="F2F7FB")

        with self._lock:
            snapshot = {s: list(rows) for s, rows in self._data.items()}

        for symbol, rows in snapshot.items():
            if not rows:
                logger.info("[PriceLog] %s — no ticks collected, skipping", symbol)
                continue

            wb = Workbook()
            ws = wb.active
            ws.title = f"{symbol} Prices"

            # Title
            ws.merge_cells("A1:J1")
            c = ws["A1"]
            c.value = (f"{symbol} — Price Log (WebSocket)  |  "
                       f"{datetime.now(EST).strftime('%B %d, %Y')}  |  "
                       f"{len(rows)} ticks")
            c.font = Font(name="Arial", bold=True, size=13, color="2F5496")
            c.alignment = Alignment(horizontal="left", vertical="center")
            ws.row_dimensions[1].height = 30

            # Summary
            prices = [r["last_price"] for r in rows]
            ws.merge_cells("A2:J2")
            s = ws["A2"]
            s.value = (
                f"Open: {prices[0]:.4f}  |  Close: {prices[-1]:.4f}  |  "
                f"High: {max(prices):.4f}  |  Low: {min(prices):.4f}  |  "
                f"Range: {max(prices)-min(prices):.4f}  |  "
                f"Start: {rows[0]['timestamp']}  |  End: {rows[-1]['timestamp']}"
            )
            s.font = Font(name="Arial", size=10, color="666666")
            s.alignment = Alignment(horizontal="left", vertical="center")
            ws.row_dimensions[2].height = 22
            ws.row_dimensions[3].height = 8

            # Headers (row 4)
            hr = 4
            for ci, h in enumerate(headers, 1):
                cell = ws.cell(row=hr, column=ci, value=h)
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = header_align
                cell.border = thin_border
            ws.row_dimensions[hr].height = 26

            # Data
            for ri, rd in enumerate(rows, hr + 1):
                vals = [
                    rd["timestamp"], rd["last_price"], rd["bid"], rd["ask"],
                    rd["spread"], rd["high"], rd["low"], rd["last_size"],
                    rd["bar_volume"], rd["daily_volume"],
                ]
                use_alt = (ri - hr) % 2 == 0
                for ci, val in enumerate(vals, 1):
                    cell = ws.cell(row=ri, column=ci, value=val)
                    cell.font = data_font
                    cell.border = thin_border
                    if use_alt:
                        cell.fill = alt_fill
                    if ci in (2, 3, 4, 6, 7):
                        cell.number_format = '#,##0.0000'
                    elif ci == 5:
                        cell.number_format = '#,##0.0000'
                    elif ci in (8, 9, 10):
                        cell.number_format = '#,##0'

            for i, w in enumerate(col_widths, 1):
                ws.column_dimensions[get_column_letter(i)].width = w
            ws.freeze_panes = f"A{hr + 1}"
            ws.auto_filter.ref = f"A{hr}:J{hr + len(rows)}"

            filename = f"prices_{symbol}_{tag}.xlsx"
            filepath = os.path.join(script_dir, filename)
            wb.save(filepath)
            logger.info("[PriceLog] %s — saved %d ticks → %s",
                        symbol, len(rows), filepath)

        total = sum(len(r) for r in snapshot.values())
        logger.info("[PriceLog] === SAVE COMPLETE — %d total ticks ===", total)


# =============================================================================
# WEBSOCKET MARKET DATA STREAMER
# =============================================================================

class IBKRMarketDataStream:
    """
    Maintains a single WebSocket connection to the IBKR CP Gateway for
    real-time top-of-book streaming market data.

    Data is stored in self.data[conid] and a user-supplied callback is
    called on every update.
    """

    WS_URL = BASE_URL.replace("https://", "wss://").replace("http://", "ws://") \
             .replace("/v1/api", "") + "/v1/api/ws"

    def __init__(
        self,
        base_url: str = BASE_URL,
        on_bar: Callable[[int, Dict], None] = None,
        verify_ssl: bool = VERIFY_SSL,
        on_reconnect: Callable = None,
    ):
        ws_base = base_url.replace("https://", "wss://").replace("http://", "ws://")
        self.ws_url        = ws_base + "/ws"
        self.on_bar        = on_bar           # callback(conid: int, data: dict)
        self.on_reconnect  = on_reconnect     # called after reconnect to reset pipelines
        self.verify_ssl    = verify_ssl
        self.data: Dict[int, Dict] = {}
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._subscriptions: List[int] = []
        self._last_msg_time: float = time.time()   # tracks last WS message (incl heartbeats)
        self._last_bar_time: float = time.time()   # tracks last actual market data message
        self._order_fills: dict = {}             # oid → fill info from sor WS topic
        self._connect_count: int   = 0              # counts connects, >1 = reconnect

    def connect(self):
        """Open the WebSocket connection in a background thread."""
        ssl_opts = {} if self.verify_ssl else {"cert_reqs": 0}

        self._ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._thread = threading.Thread(
            target=lambda: self._ws.run_forever(
                sslopt       = ssl_opts,
                ping_interval= 20,   # send a ping every 20s
                ping_timeout = 10,   # if no pong in 10s → connection is dead
            ),
            daemon=True,
            name="ibkr-ws",
        )
        self._thread.start()
        logger.info("WebSocket thread started → %s", self.ws_url)
        time.sleep(1)   # give the socket a moment to open

    def subscribe_order_updates(self):
        """Subscribe to live order updates via sor+{} topic.
        Per IBKR guidance, this provides real-time fill notifications
        without needing to poll /iserver/account/orders."""
        self._send("sor+{}")
        logger.info("Subscribed to live order updates (sor+{})")

    def subscribe(self, conid: int, fields: List[str] = None):
        """Subscribe to live top-of-book data for a contract."""
        fields = fields or MD_FIELDS
        fields_json = json.dumps(fields)
        msg = f"smd+{conid}+{{\"fields\":{fields_json}}}"
        if conid not in self._subscriptions:
            self._subscriptions.append(conid)
        self._send(msg)
        logger.info("Subscribed to conid=%d fields=%s", conid, fields)

    def unsubscribe(self, conid: int):
        """Cancel a market data subscription."""
        self._send(f"umd+{conid}+{{}}")
        if conid in self._subscriptions:
            self._subscriptions.remove(conid)
        logger.info("Unsubscribed conid=%d", conid)

    def _send(self, msg: str):
        if self._ws and self._ws.sock and self._ws.sock.connected:
            self._ws.send(msg)
        else:
            logger.warning("WebSocket not connected, cannot send: %s", msg[:60])

    def _on_open(self, ws):
        self._connect_count += 1
        is_reconnect = self._connect_count > 1
        logger.info("WebSocket connected (connect #%d%s)",
                    self._connect_count, " — RECONNECT" if is_reconnect else "")
        # Subscribe to live order updates (sor topic) per IBKR recommendation
        try:
            self._send("sor+{}")
            logger.info("Subscribed to live order updates (sor+{})")
        except Exception as _se:
            logger.warning("Could not subscribe to sor: %s", _se)
        # Re-subscribe to all conids
        for conid in self._subscriptions:
            self.subscribe(conid)
        # On reconnect: reset all bar accumulators so fresh bars start immediately.
        # Without this, the 30s bar timer is broken after reconnect — ticks arrive
        # but bars never close and on_bar never fires (seen Apr-16-2026).
        if is_reconnect and self.on_reconnect:
            try:
                self.on_reconnect()
                logger.info("WebSocket reconnect: bar accumulators reset")
            except Exception as exc:
                logger.error("WebSocket reconnect callback failed: %s", exc, exc_info=True)

    def _on_message(self, ws, raw: str):
        self._last_msg_time = time.time()   # stamp every WS message (incl heartbeats)
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        # ── Live order updates via sor topic ─────────────────────────────
        topic = str(data.get("topic", ""))
        if topic == "sor" or topic.startswith("sor"):
            orders = data.get("args", data.get("orders", []))
            if isinstance(orders, list):
                for o in orders:
                    oid = str(o.get("orderId", o.get("order_id", "")))
                    status = str(o.get("status", o.get("order_status", ""))).lower()
                    filled = float(o.get("filledQuantity",
                                   o.get("filled_quantity", 0)) or 0)
                    avg_px = float(o.get("avgPrice",
                                   o.get("average_price", 0)) or 0)
                    if oid:
                        self._order_fills[oid] = {
                            "status": status, "filled": filled, "avg_price": avg_px
                        }
                        if "fill" in status or filled > 0:
                            logger.info("WS order fill [oid=%s]: status=%s qty=%.0f avg=%.4f",
                                        oid, status, filled, avg_px)
            return  # sor has no conid — skip market data path

        # Market data messages have a 'conid' field
        conid = data.get("conid")
        if conid is None:
            return   # heartbeat / non-market message — does NOT update _last_bar_time

        self._last_bar_time = time.time()   # real market data arrived
        self.data[conid] = data

        if self.on_bar:
            try:
                self.on_bar(conid, data)
            except Exception as exc:
                logger.error("on_bar callback error: %s", exc)

    def _on_error(self, ws, error):
        logger.error("WebSocket error: %s", error)

    def _on_close(self, ws, code, msg):
        logger.warning("WebSocket closed (code=%s msg=%s) – will reconnect", code, msg)
        time.sleep(5)
        self.connect()

    def get_latest(self, conid: int) -> Optional[Dict]:
        return self.data.get(conid)

    def disconnect(self):
        if self._ws:
            self._ws.close()


# =============================================================================
# ORDER BUILDER  —  translate entry_logic output → IBKR order tickets
# =============================================================================

def build_ibkr_bracket(
    conid: int,
    direction: str,
    orders_dict: Dict,
    symbol: str = "",
) -> List[Dict]:
    """
    Build the ENTRY bracket — parent + Stop Market only.

    ARCHITECTURE CHANGE (Apr-2026):
    ─────────────────────────────────────────────────────────────────────────
    IBKR CP Gateway only reliably returns 2 order acknowledgements per call,
    regardless of how many tickets are submitted.  Sending 4 tickets in one
    call causes stop_lmt and target to be silently dropped — they never reach
    the exchange.

    New design — two-phase order placement:

    Phase 1 (AT ENTRY — this function):
        Call 1: [parent LMT]         → capture parent_oid
        Call 2: [Stop Market STP]    → capture stop_oid  (only protection at entry)

    Phase 2 (ONCE IN PROFIT — apply_exit_action UPDATE_STOPS BREAKEVEN):
        Place Stop Limit (STP LMT) and move Stop Market to breakeven zone.
        These are standalone orders (not bracket children) managed by our
        own OCA logic: when any one fills, the bot cancels the others via
        the reconcile/STOP_HIT handlers.

    This guarantees every order is acknowledged and tracked.
    ─────────────────────────────────────────────────────────────────────────
    Returns only the parent ticket.  submit_entry() places it first,
    captures the real parent_oid, then places the Stop Market separately.
    """
    side       = "BUY"  if direction == "LONG" else "SELL"
    qty        = orders_dict["quantity"]
    parent     = orders_dict["parent_order"]
    ts         = f"{int(time.time())}_{symbol}"
    parent_ref = f"PARENT_{ts}"

    parent_ticket = {
        "conid"    : conid,
        "side"     : side,
        "orderType": "LMT",
        "price"    : parent["limit_price"],
        "quantity" : qty,
        "tif"      : "DAY",
        "cOID"     : parent_ref,
        "order_ref": parent_ref,
    }
    return [parent_ticket]


# =============================================================================
# HIGH-LEVEL BROKER FACADE
# =============================================================================

class IBKRBroker:
    """
    High-level broker interface.  Called by the strategy loop.

    Handles:
      • Session initialisation and keepalive
      • Suppression of common order reply messages
      • Symbol → conid resolution (cached)
      • Market data subscription and bar aggregation
      • Order submission with automatic reply confirmation
      • Stop/target modification via entry IDs
      • End-of-Day market close
    """

    # Message IDs to suppress at session start (IBKR 'fat-finger' prompts)
    SUPPRESS_IDS = ["o163", "o354", "o451", "o10331", "o2137", "o383"]

    def __init__(
        self,
        base_url:   str  = BASE_URL,
        account_id: str  = ACCOUNT_ID,
        verify_ssl: bool = VERIFY_SSL,
    ):
        if not account_id:
            raise ValueError("IBKR_ACCOUNT_ID environment variable is not set.")

        self.account_id = account_id
        self.session    = IBKRSession(base_url, account_id, verify_ssl)
        self._conid_cache: Dict[str, int]  = {}
        self._order_ids:   Dict[str, Dict] = {}   # symbol → {parent, stop, stop_lmt, target}
        # Load recently_closed from disk — survives bot restarts
        _rc_path = os.path.join(os.path.dirname(__file__), ".recently_closed.json")
        try:
            if os.path.exists(_rc_path):
                import json as _json
                _rc_raw  = _json.loads(open(_rc_path).read())
                _rc_now0 = time.time()
                self._recently_closed: Dict[str, float] = {
                    s: t for s, t in _rc_raw.items() if _rc_now0 - t < 900
                }
                if self._recently_closed:
                    logger.info("Loaded %d recently-closed symbol(s) from disk: %s",
                                len(self._recently_closed), list(self._recently_closed.keys()))
            else:
                self._recently_closed: Dict[str, float] = {}
        except Exception as _rce:
            logger.warning("Could not load recently_closed: %s", _rce)
            self._recently_closed: Dict[str, float] = {}
        self.stream: Optional[IBKRMarketDataStream] = None

        # ── Live Order Registry ──────────────────────────────────────────
        # Tracks EVERY order ID placed by this code that hasn't been
        # confirmed filled or cancelled.  Keyed by order_id (str).
        #
        # Value: {"symbol": str, "side": str, "type": str, "placed_at": float}
        #
        # WHY: IBKR's get_live_orders API can miss orders in certain
        # states (PreSubmitted, edge cases). By maintaining our OWN
        # registry, EoD can cancel every order by its specific ID
        # instead of relying on a potentially incomplete API response.
        self._live_order_registry: Dict[str, Dict] = {}
        self._registry_lock = threading.Lock()

        # One-minute bar builder: buffers ticks → OHLCV bars
        self._bar_buffer: Dict[int, List[Dict]] = {}

        # Indicator pipelines — one per subscribed symbol
        # Keyed by symbol string; created in subscribe()
        self._pipelines: Dict[str, IndicatorPipeline] = {}

        # Embedded price logger — captures ticks for AAPL/TSLA/NVDA
        self.price_logger = PriceLogger()

        # ── Position reconciliation ──────────────────────────────────────
        # Shared reference to run_strategy's open_positions dict.
        # Set by run_strategy() so the reconciliation thread can update it.
        self._open_positions: Optional[Dict[str, Dict]] = None
        # Lock protecting _open_positions against concurrent writes from
        # _verify_fill_async background threads and the reconcile thread.
        self._pos_lock = threading.Lock()
        self._order_ids_lock = threading.Lock()   # protects _order_ids dict
        self._entering_lock  = threading.Lock()   # protects _currently_entering set
        self._pos_rate_lock  = threading.Lock()   # rate-limits position endpoint calls
        self._last_pos_call  = 0.0                # timestamp of last position call
        self._reconcile_thread: Optional[threading.Thread] = None
        self._stop_reconcile = threading.Event()
        self._watchdog_thread: Optional[threading.Thread] = None
        self._stop_watchdog   = threading.Event()

        # ── EoD timer ────────────────────────────────────────────────────
        # Independent timer thread that fires at EoDExitTime regardless of
        # whether open_positions has entries. Queries IBKR for actual
        # positions and closes them all.
        self._eod_thread: Optional[threading.Thread] = None
        self._stop_eod = threading.Event()
        self._eod_fired = False  # prevent double-fire
        self._eod_safety_fired = False  # prevent double-fire of safety net

    def start(self, on_bar: Callable[[str, Dict], None] = None):
        """
        Initialise the session, start the WebSocket stream, and set up
        the order reply message suppression.

        `on_bar` is called with (symbol, bar_dict) on each 1-minute bar close.
        """
        # Verify the Gateway is reachable
        try:
            status = self.session.auth_status()
            logger.info("Auth status: %s", status)
        except Exception as exc:
            raise RuntimeError(
                f"Cannot reach IBKR CP Gateway at {self.session.base_url}.\n"
                f"Make sure the Gateway is running and you are logged in.\n"
                f"Error: {exc}"
            )

        # Suppress common order confirmation prompts
        try:
            self.session.suppress_order_messages(self.SUPPRESS_IDS)
        except Exception as exc:
            logger.warning("Could not suppress order messages: %s", exc)

        # Start keepalive thread
        self.session.start_tickle_thread()

        # Start WebSocket with a tick-level callback
        def _on_ws_tick(conid: int, data: Dict):
            self._handle_tick(conid, data)

        self.stream = IBKRMarketDataStream(
            base_url     = self.session.base_url,
            on_bar       = _on_ws_tick,
            verify_ssl   = self.session.verify_ssl,
            on_reconnect = self._reset_pipelines,
        )
        self.stream.connect()

        self._on_bar_callback = on_bar

        # Start position reconciliation thread (every 3 minutes)
        self._start_reconciliation_thread(interval=180)
        self._start_direction_monitor(interval=5)

        # Start EoD timer thread (checks every 10 seconds)
        self._start_eod_timer_thread()

        # Start WebSocket watchdog — detects silent disconnects and reconnects
        self._start_ws_watchdog()

        logger.info("IBKRBroker started (account=%s)", self.account_id)

    def _reset_pipelines(self):
        """
        Reset all bar accumulators after a WS reconnect.
        Called by IBKRMarketDataStream._on_open on every reconnect.

        The CP Gateway closes the WS every ~22 minutes (code 1000).
        On reconnect the SecondBarAggregator inside each pipeline is mid-bar.
        New ticks arrive but the bar boundary timer never fires — bars never
        close and on_bar never fires for the rest of the session.
        Calling reset() clears _buf and _bucket so first tick starts a fresh bar.
        """
        reset_count = 0
        for sym, pipeline in self._pipelines.items():
            try:
                pipeline.reset()
                reset_count += 1
            except Exception as exc:
                logger.warning("[%s] Pipeline reset failed: %s", sym, exc)
        self._bar_buffer.clear()
        logger.info("Reconnect reset: %d pipeline(s) + bar buffer cleared — "
                    "fresh bars start on next tick.", reset_count)

    def stop(self):
        """Clean shutdown — saves price log Excel files before stopping."""
        # Stop reconciliation thread
        self._stop_reconciliation_thread()
        self._stop_direction_monitor_thread()

        # Stop EoD timer thread
        self._stop_eod_timer_thread()

        # Save price logger data to Excel before disconnecting
        try:
            self.price_logger.save_excel()
        except Exception as exc:
            logger.error("PriceLogger save failed: %s", exc)

        # Stop watchdog
        self._stop_ws_watchdog()

        if self.stream:
            self.stream.disconnect()
        self.session.stop_tickle_thread()
        logger.info("IBKRBroker stopped")

    # ── Contract resolution ────────────────────────────────────────────────

    def resolve_conid(self, symbol: str) -> int:
        """Return the cached conid for a symbol, or look it up."""
        if symbol not in self._conid_cache:
            conid = self.session.search_conid(symbol)
            if conid is None:
                raise ValueError(f"Could not resolve conid for symbol: {symbol}")
            self._conid_cache[symbol] = conid
            logger.info("Resolved %s → conid=%d", symbol, conid)
        return self._conid_cache[symbol]

    def subscribe(self, symbol: str, is_index: bool = False):
        """
        Subscribe to live market data for a symbol (stock or index).

        is_index=True  → pipeline updates IndexMACDStore, never fires on_bar_callback
        is_index=False → pipeline fires on_bar_callback when MACD is ready
        """
        conid = self.resolve_conid(symbol)
        # Pre-flight snapshot to open the stream
        try:
            self.session.snapshot([conid], MD_FIELDS)
        except Exception:
            pass
        if self.stream:
            self.stream.subscribe(conid)
        # Initialize bar buffer
        if conid not in self._bar_buffer:
            self._bar_buffer[conid] = []

        # Create indicator pipeline for this symbol if not yet done.
        if symbol not in self._pipelines:
            # Indexes never fire the strategy on_bar callback.
            # Stocks fire it when MACD is ready.
            # Per-symbol resolution is read inside IndicatorPipeline from config.
            pipeline = IndicatorPipeline(
                symbol,
                broker           = self,
                is_index         = is_index,
                on_indicator_bar = None if is_index else self._on_bar_callback,
            )

            self._pipelines[symbol] = pipeline
            logger.info("[%s] IndicatorPipeline created (is_index=%s) — seeding EMAs...",
                        symbol, is_index)

            seeded = pipeline.seed()
            if seeded:
                logger.info("[%s] EMA seeding complete — MACD ready at first bar", symbol)
            else:
                logger.warning(
                    "[%s] Could not seed from prior session — will warm up from live bars",
                    symbol,
                )

            # ── Log resolution mode clearly for debugging ─────────────────
            res = pipeline.get_resolution()
            from indicators import resolution_is_tick, resolution_is_second, NTickBarAggregator
            if resolution_is_tick(res):
                logger.info(
                    "[%s] RESOLUTION MODE: TICK — MACD computed on every trade tick. "
                    "Set macd_resolution='tick' in config. "
                    "Per-tick data logged at DEBUG level only.",
                    symbol)
            elif resolution_is_second(res):
                logger.info("[%s] RESOLUTION MODE: %s second bars", symbol, res)
            elif NTickBarAggregator.parse_resolution(res) is not None:
                logger.info("[%s] RESOLUTION MODE: %s (N-tick bars)", symbol, res)
            else:
                logger.info("[%s] RESOLUTION MODE: %s (minute bars)", symbol, res)

    # ── Tick → bar aggregation ─────────────────────────────────────────────

    def _handle_tick(self, conid: int, data: Dict):
        """
        Process one raw WebSocket tick.

        Dual role depending on MACD_Resolution in config.json:

        ① Minute resolution ("1min", "5min", …):
           Accumulates ticks into OHLCV minute bars.  When the minute
           boundary is crossed, _close_bar() is called which passes the
           completed bar through IndicatorPipeline.on_minute_bar() to
           compute MACD, then fires _on_bar_callback with the enriched bar.

        ② Second resolution ("30s", "10s", …):
           Passes each tick to IndicatorPipeline.on_raw_tick() which
           aggregates into second bars, computes MACD, and fires
           _on_bar_callback with enriched second bars autonomously.

        ③ Tick resolution ("tick"):
           Passes each tick directly to IndicatorPipeline.on_raw_tick()
           which computes MACD on every trade and fires _on_bar_callback.
        """
        price = float(data.get("31", 0) or 0)
        if price == 0:
            return

        now      = datetime.now(EST)
        symbol   = self._conid_to_symbol(conid)

        # ── Embedded price logger (zero-lag capture) ──────────────────────
        if symbol:
            self.price_logger.on_tick(symbol, data)

        # Volume fields
        bar_vol   = _parse_vol(data.get("7762", 0))  # IBKR bar volume
        tick_vol  = _parse_vol(data.get("7059", 0))  # this trade size
        daily_vol = _parse_vol(data.get("87",   0))  # cumulative daily

        # ── Route by resolution ───────────────────────────────────────────
        # Resolution is now per-symbol (stored in the pipeline).
        # tick, Xs (second), and NNtick (N-tick) all go straight to the
        # pipeline which handles aggregation internally.
        # Only minute resolution accumulates here in the minute buffer.
        if symbol:
            pipeline = self._pipelines.get(symbol)
            if pipeline:
                res = pipeline.get_resolution()
                from indicators import NTickBarAggregator
                is_ntick = NTickBarAggregator.parse_resolution(res) is not None
                if resolution_is_tick(res) or resolution_is_second(res) or is_ntick:
                    raw_tick = {
                        "ts"       : now,
                        "price"    : price,
                        "high"     : float(data.get("70", price) or price),
                        "low"      : float(data.get("71", price) or price),
                        "tick_vol" : tick_vol,
                        "bar_vol"  : bar_vol,
                        "daily_vol": daily_vol,
                    }
                    # FIX 6 — Clear tick-mode logging so debugs are readable.
                    # For pure tick resolution ("tick"), log every tick at DEBUG
                    # level so it does not flood INFO logs but is visible with
                    # --log-level DEBUG. For second/ntick bars, only log when
                    # a completed bar is emitted (done inside indicators.py).
                    if resolution_is_tick(res):
                        logger.debug("[%s] TICK  price=%.4f  vol=%s  ts=%s",
                                     symbol, price, tick_vol,
                                     now.strftime("%H:%M:%S.%f")[:-3])
                    pipeline.on_raw_tick(raw_tick)
                    return  # do NOT accumulate into minute buffer

        # ── Minute resolution: accumulate into minute buffer ──────────────
        buf = self._bar_buffer.setdefault(conid, [])

        if buf:
            last_min = buf[-1]["_minute"]
            cur_min  = now.replace(second=0, microsecond=0)
            if cur_min > last_min:
                bar = self._close_bar(buf, symbol)
                buf.clear()
                # bar already has MACD merged in (done inside _close_bar)
                if bar and symbol and self._on_bar_callback:
                    self._on_bar_callback(symbol, bar)

        tick = {
            "_minute"  : now.replace(second=0, microsecond=0),
            "price"    : price,
            "bid"      : float(data.get("84", price) or price),
            "ask"      : float(data.get("86", price) or price),
            "high"     : float(data.get("70", price) or price),
            "low"      : float(data.get("71", price) or price),
            "bar_vol"  : bar_vol,
            "tick_vol" : tick_vol,
            "daily_vol": daily_vol,
        }
        buf.append(tick)

    def _close_bar(self, buf: List[Dict], symbol: Optional[str] = None) -> Optional[Dict]:
        """
        Build a completed OHLCV bar from the tick buffer, then pass it
        through the IndicatorPipeline to compute MACD fields.

        Returns the enriched bar dict (with all 6 MACD keys added), or
        None if the pipeline is still warming up (entry logic is blocked).
        """
        if not buf:
            return None

        prices = [t["price"] for t in buf]

        # Volume — prefer IBKR's cumulative bar_vol; fall back to sum of tick sizes
        bar_vol_series   = [t["bar_vol"]  for t in buf if t["bar_vol"]  > 0]
        tick_vol_sum     = sum(t["tick_vol"] for t in buf)
        bar_volume       = bar_vol_series[-1] if bar_vol_series else tick_vol_sum

        daily_vol_series = [t["daily_vol"] for t in buf if t["daily_vol"] > 0]
        daily_volume     = daily_vol_series[-1] if daily_vol_series else 0

        bar = {
            "current_time": buf[-1]["_minute"],
            "open"        : prices[0],
            "high"        : max(t["high"] for t in buf),
            "low"         : min(t["low"]  for t in buf),
            "close"       : prices[-1],
            "price"       : prices[-1],
            "prev_high"   : max(t["high"] for t in buf[:-1]) if len(buf) > 1 else prices[-1],
            "prev_low"    : min(t["low"]  for t in buf[:-1]) if len(buf) > 1 else prices[-1],
            "volume"      : bar_volume,
            "daily_volume": daily_volume,
        }

        # Pass through IndicatorPipeline to compute and merge MACD fields
        if symbol:
            pipeline = self._pipelines.get(symbol)
            if pipeline:
                enriched = pipeline.on_minute_bar(bar)
                if enriched is None:
                    # Still warming up — log handled inside pipeline, block entry
                    return None
                return enriched

        return bar  # no pipeline yet — return raw bar (MACD fields absent)

    def _conid_to_symbol(self, conid: int) -> Optional[str]:
        for sym, cid in self._conid_cache.items():
            if cid == conid:
                return sym
        return None

    # ── Live Order Registry helpers ───────────────────────────────────────

    def _register_order(self, order_id: str, symbol: str, side: str = "",
                        order_type: str = ""):
        """Track a live order ID so EoD can cancel it explicitly."""
        if not order_id or order_id == "-1":
            return
        with self._registry_lock:
            self._live_order_registry[str(order_id)] = {
                "symbol"   : symbol,
                "side"     : side,
                "type"     : order_type,
                "placed_at": time.time(),
            }
        logger.debug("Registry: tracked order %s for %s (%s %s)",
                     order_id, symbol, side, order_type)

    def _unregister_order(self, order_id: str):
        """Remove an order from the registry (filled, cancelled, or replaced)."""
        if not order_id:
            return
        with self._registry_lock:
            removed = self._live_order_registry.pop(str(order_id), None)
        if removed:
            logger.debug("Registry: removed order %s (%s)", order_id, removed["symbol"])

    def _unregister_symbol(self, symbol: str):
        """Remove ALL tracked orders for a symbol (after position closed)."""
        with self._registry_lock:
            to_remove = [oid for oid, info in self._live_order_registry.items()
                         if info["symbol"] == symbol]
            for oid in to_remove:
                del self._live_order_registry[oid]
        if to_remove:
            logger.debug("Registry: removed %d orders for %s", len(to_remove), symbol)

    def _cancel_all_registered_orders(self, skip_eod_close: bool = False):
        """
        Cancel every order in our registry by its specific ID.

        This is more reliable than IBKR's get_live_orders API,
        which can miss orders in certain states.

        Args:
            skip_eod_close: If True, do NOT cancel orders of type EOD_CLOSE.
                            Use this in the safety-net pass so we don't cancel
                            the very close orders we just submitted (March 31
                            cancel-loop fix).
        """
        with self._registry_lock:
            all_orders = dict(self._live_order_registry)

        if not all_orders:
            logger.info("EoD Registry: no tracked orders to cancel")
            return

        # FIX (March-31 cancel-loop): When called from the safety net, skip any
        # EOD_CLOSE orders that are already in-flight (PreSubmitted/Submitted).
        # Cancelling them before IBKR fills them caused the infinite loop where
        # every close attempt was cancelled before it could execute.
        if skip_eod_close:
            eod_orders  = {oid: info for oid, info in all_orders.items()
                           if info.get("type") == "EOD_CLOSE"}
            all_orders  = {oid: info for oid, info in all_orders.items()
                           if info.get("type") != "EOD_CLOSE"}
            if eod_orders:
                logger.info(
                    "EoD Registry: skipping %d in-flight EOD_CLOSE order(s): %s "
                    "— letting them fill before safety-net acts",
                    len(eod_orders),
                    {oid: info["symbol"] for oid, info in eod_orders.items()})

        if not all_orders:
            logger.info("EoD Registry: no non-EOD_CLOSE orders to cancel")
            return

        logger.info("EoD Registry: cancelling %d tracked order(s): %s",
                    len(all_orders),
                    {oid: info["symbol"] for oid, info in all_orders.items()})

        for oid, info in all_orders.items():
            try:
                self.session.cancel_order(self.account_id, oid)
                logger.info("EoD Registry: cancelled order %s  symbol=%s  type=%s",
                            oid, info["symbol"], info["type"])
            except Exception as exc:
                # Order may already be filled/cancelled — that's fine
                logger.debug("EoD Registry: cancel order %s failed (likely already gone): %s",
                             oid, exc)

        # Clear only the cancelled entries from the registry
        with self._registry_lock:
            for oid in all_orders:
                self._live_order_registry.pop(oid, None)

    # ── Order submission ───────────────────────────────────────────────────


    def batch_submit_entries(self, pending: list) -> dict:
        """
        Submit bracket orders for N symbols in exactly 2 HTTP calls.

        ── Architecture ──────────────────────────────────────────────────────
        Each bracket = 4 orders:
          parent LMT  → entry order
          child STP   → stop market  (parentId → held until parent fills)
          child STPLMT→ stop limit   (parentId → held until parent fills)
          target LMT  → profit taker (standalone — only fills if price moves to it)

        Call 1: [parent_A, stp_A, parent_B, stp_B, ...]   ← N×2 tickets
        Call 2: [stplmt_A, target_A, stplmt_B, target_B, ...]  ← N×2 tickets

        Total = 2 HTTP calls regardless of N (13, 50, 100 — does not matter).

        ── ID integrity ──────────────────────────────────────────────────────
        Every ticket carries a unique cOID built as:
            PARENT_{timestamp}_{SYMBOL}   e.g. PARENT_1776270128_PLTR
            STOP_{timestamp}_{SYMBOL}
            STPLMT_{timestamp}_{SYMBOL}
            TARGET_{timestamp}_{SYMBOL}

        The parentId on STP and STPLMT references the parent's cOID.
        IBKR holds both children in PreSubmitted state until the parent fills.
        If the parent is cancelled (by us or by IBKR), both children are
        auto-cancelled — no orphan STP can fire against a non-existent position.

        ── ID recovery ────────────────────────────────────────────────────────
        When encrypt_message:1 fires (fat-finger check), IBKR only returns
        the parent OID in the response — children are on IBKR but withheld.
        We recover ALL missing OIDs by:
          1. Building a ref→slot map from the cOIDs WE sent (not from response)
          2. Querying /iserver/account/orders after a 300ms settle
          3. Matching each live order's orderRef back to our cOID

        ── Non-fill cancellation ──────────────────────────────────────────────
        The batch_entry_loop spawns one _verify thread per stock after this
        returns. Each thread polls for fill for up to 60s. If no fill:
          → cancel parent + STP + STPLMT + target for that symbol only
          → remove from open_positions
          → _currently_entering.discard(symbol)
        All other stocks in the batch are unaffected.

        Args:
            pending: list of dicts (one per symbol), each containing:
                symbol, direction, qty, bar_close, conid,
                close_side, target_side,
                stop_price, stop_lmt_trigger, stop_lmt_price, target_price,
                parent_ref, stop_ref, stoplmt_ref, target_ref

        Returns:
            dict: {symbol → {parent_oid, stop_oid, stoplmt_oid, target_oid}}
            Missing OIDs are None (logged as warnings).
        """
        if not pending:
            return {}

        syms = [p["symbol"] for p in pending]
        logger.info("BATCH ENTRY: %d stock(s) → 2 HTTP calls: %s", len(pending), syms)

        # ── Build all tickets ─────────────────────────────────────────────────
        # Call 1: parent + STP per stock (N×2 tickets)
        # STP carries parentId → IBKR holds it PreSubmitted until parent fills.
        # Auto-cancelled by IBKR if parent is cancelled — no naked short possible.
        call1_tickets = []
        for p in pending:
            side = "BUY" if p["direction"] == "LONG" else "SELL"
            call1_tickets.append({
                "conid"    : p["conid"],
                "side"     : side,
                "orderType": "LMT",
                "price"    : p["bar_close"],
                "quantity" : p["qty"],
                "tif"      : "DAY",
                "cOID"     : p["parent_ref"],
                "order_ref": p["parent_ref"],
            })
            call1_tickets.append({
                "conid"    : p["conid"],
                "side"     : p["close_side"],
                "orderType": "STP",
                "price"    : p["stop_price"],
                "quantity" : p["qty"],
                "tif"      : "DAY",
                "parentId" : p["parent_ref"],   # held until parent fills
                "cOID"     : p["stop_ref"],
                "order_ref": p["stop_ref"],
            })

        # Call 2: STPLMT + target per stock (N×2 tickets)
        # STPLMT carries parentId → also held until parent fills.
        # Target is standalone LMT — cannot create naked position.
        call2_tickets = []
        for p in pending:
            call2_tickets.append({
                "conid"    : p["conid"],
                "side"     : p["close_side"],
                "orderType": "STP LMT",
                "price"    : p["stop_lmt_price"],
                "auxPrice" : p["stop_lmt_trigger"],
                "quantity" : p["qty"],
                "tif"      : "DAY",
                "parentId" : p["parent_ref"],   # held until parent fills
                "cOID"     : p["stoplmt_ref"],
                "order_ref": p["stoplmt_ref"],
            })
            call2_tickets.append({
                "conid"    : p["conid"],
                "side"     : p["target_side"],
                "orderType": "LMT",
                "price"    : p["target_price"],
                "quantity" : p["qty"],
                "tif"      : "DAY",
                "parentId" : p["parent_ref"],   # auto-cancelled if parent cancels
                "cOID"     : p["target_ref"],
                "order_ref": p["target_ref"],
            })

        # ── Submit Call 1: parents + stops ────────────────────────────────────
        try:
            resp1 = self.session.place_order(self.account_id, call1_tickets)
        except Exception as exc:
            logger.error("BATCH Call 1 failed: %s", exc)
            return {p["symbol"]: {"parent_oid":None,"stop_oid":None,
                                   "stoplmt_oid":None,"target_oid":None}
                    for p in pending}
        logger.info("BATCH Call 1 response (%d tickets, %d stocks): %s",
                    len(call1_tickets), len(pending), resp1)

        # ── Submit Call 2: STPLMT + targets ──────────────────────────────────
        try:
            resp2 = self.session.place_order(self.account_id, call2_tickets)
        except Exception as exc:
            logger.error("BATCH Call 2 failed: %s", exc)
            resp2 = []
        logger.info("BATCH Call 2 response (%d tickets, %d stocks): %s",
                    len(call2_tickets), len(pending), resp2)

        # ── Build ref→OID map from responses ─────────────────────────────────
        # Primary: scan both responses for any items with recognisable refs.
        ref_to_oid: Dict[str, str] = {}
        for item in (resp1 if isinstance(resp1, list) else []) +                     (resp2 if isinstance(resp2, list) else []):
            if not isinstance(item, dict):
                continue
            oid = str(item.get("order_id", ""))
            if not oid or oid in ("-1", "0"):
                continue
            # Use order_ref first, fall back to local_order_id
            ref = (item.get("order_ref", "") or
                   item.get("local_order_id", "") or
                   item.get("orderRef", ""))
            if ref:
                ref_to_oid[ref] = oid
                logger.debug("BATCH mapped ref=%s → oid=%s status=%s",
                             ref, oid, item.get("order_status","?"))
            # Confirm fat-finger prompts immediately
            if "id" in item and "message" in item:
                try:
                    self.session.reply_confirm(item["id"])
                    logger.info("BATCH confirmed fat-finger msg id=%s", item["id"])
                except Exception:
                    pass

        # ── Recovery: find OIDs missing from response via live orders ─────────
        # Happens when encrypt_message:1 withholds child acknowledgements.
        # We built each ref with the symbol embedded so matching is exact.
        all_sent_refs = set()
        for p in pending:
            all_sent_refs.update([p["parent_ref"], p["stop_ref"],
                                   p["stoplmt_ref"], p["target_ref"]])
        missing_refs = all_sent_refs - set(ref_to_oid.keys())

        if missing_refs:
            logger.info("BATCH: %d ref(s) missing from response — "
                        "querying live orders (encrypt_message recovery)",
                        len(missing_refs))
            try:
                time.sleep(0.3)   # let gateway register all orders
                live_resp   = self.session.get_live_orders(self.account_id)
                live_orders = (live_resp.get("orders", [])
                               if isinstance(live_resp, dict) else
                               live_resp if isinstance(live_resp, list) else [])
                for o in live_orders:
                    if not isinstance(o, dict):
                        continue
                    o_ref = (o.get("order_ref","") or o.get("orderRef","") or
                             o.get("local_order_id",""))
                    o_id  = str(o.get("orderId", o.get("order_id","")))
                    if o_ref and o_id and o_id not in ("0","") and o_ref in missing_refs:
                        ref_to_oid[o_ref] = o_id
                        logger.info("BATCH recovered: ref=%s → oid=%s", o_ref, o_id)
                        missing_refs.discard(o_ref)
                if missing_refs:
                    logger.warning("BATCH: still missing after recovery: %s", missing_refs)
            except Exception as exc:
                logger.warning("BATCH live-orders recovery failed: %s", exc)

        # ── Map OIDs back to each symbol ──────────────────────────────────────
        results: Dict[str, Dict] = {}
        for p in pending:
            sym = p["symbol"]
            parent_oid  = ref_to_oid.get(p["parent_ref"])
            stop_oid    = ref_to_oid.get(p["stop_ref"])
            stoplmt_oid = ref_to_oid.get(p["stoplmt_ref"])
            target_oid  = ref_to_oid.get(p["target_ref"])

            results[sym] = {
                "parent_oid" : parent_oid,
                "stop_oid"   : stop_oid,
                "stoplmt_oid": stoplmt_oid,
                "target_oid" : target_oid,
            }

            # Log per-stock summary
            if parent_oid:
                logger.info("[%s] BATCH parent=%-12s stp=%-12s stplmt=%-12s target=%s",
                            sym, parent_oid, stop_oid or "MISSING",
                            stoplmt_oid or "MISSING", target_oid or "MISSING")
            else:
                logger.error("[%s] BATCH: parent not acknowledged — entry failed", sym)

            if not stop_oid:
                logger.warning("[%s] BATCH: STP not acknowledged — no stop market", sym)
            if not stoplmt_oid:
                logger.warning("[%s] BATCH: STPLMT not acknowledged", sym)
            if not target_oid:
                logger.warning("[%s] BATCH: target not acknowledged", sym)

        return results

    def submit_entry(self, symbol: str, signal: Dict) -> bool:
        """
        Submit entry order using two separate API calls.

        NEW ARCHITECTURE (Apr-2026):
        ────────────────────────────────────────────────────────────────────
        IBKR CP Gateway only reliably acknowledges 2 orders per call.
        Sending all 4 bracket legs in one call silently drops stop_lmt
        and target — they never reach the exchange.

        New two-call approach:
          Call 1 → Parent LMT only         → capture parent_oid
          Call 2 → Stop Market (STP) only  → capture stop_oid

        The Stop Market is the ONLY protection at entry. Stop Limit and
        target are added later by compute_stop_update() when the position
        reaches the breakeven threshold (BREAKEVEN phase).

        OCA management:
          Since stop orders are now standalone (not bracket children),
          our code acts as the OCA group. When any stop fills (detected
          by reconcile finding position gone), STOP_HIT handler cancels
          all remaining stop orders. This is reliable because reconcile
          runs every 3 minutes and STOP_HIT fires on every bar.
        ────────────────────────────────────────────────────────────────────
        Returns True if both parent and stop were acknowledged.
        """
        if not signal.get("orders") or signal["signal"] == "NONE":
            return False
        if not signal["orders"].get("send_to_market", True):
            logger.info("[%s] Manual mode — signal logged but order NOT sent", symbol)
            return False

        try:
            conid      = self.resolve_conid(symbol)
            direction  = signal["signal"]
            orders     = signal["orders"]
            qty        = orders.get("quantity", 100)
            close_side = "SELL" if direction == "LONG" else "BUY"
            side       = "BUY"  if direction == "LONG" else "SELL"
            ts         = f"{int(time.time())}_{symbol}"
            # Bar close used as fallback price reference if fill avg not yet available
            bar_close  = orders["parent_order"].get("limit_price", 0.0)

            # ── Load config for price levels ──────────────────────────────
            try:
                _cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
                with open(_cfg_path) as _fh:
                    cfg = json.load(_fh)
            except Exception:
                cfg = {}
            mpl      = float(cfg.get("MPL_dollar_Amount", 0.60))
            slm      = float(cfg.get("SLMultiplier",      1.0))
            sls      = float(cfg.get("StopLimitSpread",   0.03))
            tgt_mult = float(cfg.get("TargetMultiplier",  3.0))

            # ── Price levels ──────────────────────────────────────────────
            if direction == "LONG":
                stop_price       = round(bar_close - mpl,             4)
                stop_lmt_trigger = round(bar_close - mpl * slm,       4)
                stop_lmt_price   = round(bar_close - mpl * slm - sls, 4)
                target_price     = round(bar_close + mpl * tgt_mult,  4)
                target_side      = "SELL"
            else:
                stop_price       = round(bar_close + mpl,             4)
                stop_lmt_trigger = round(bar_close + mpl * slm,       4)
                stop_lmt_price   = round(bar_close + mpl * slm + sls, 4)
                target_price     = round(bar_close - mpl * tgt_mult,  4)
                target_side      = "BUY"

            # ── Order reference IDs ───────────────────────────────────────
            parent_ref  = f"PARENT_{ts}"
            stop_ref    = f"STOP_{ts}"
            stoplmt_ref = f"STPLMT_{ts}"
            target_ref  = f"TARGET_{ts}"

            # ══════════════════════════════════════════════════════════════
            # FIX (Apr-15-2026): 2-TICKET BRACKET — parent + STP only
            #
            # ROOT CAUSE of naked-short bug (MSFT/PLTR/TSLA Apr-15-2026):
            #   We sent 3 tickets in Call 1: [parent, STP, STPLMT].
            #   IBKR CP Gateway silently dropped ticket #2 (STP) every time.
            #   Only [STPLMT, parent] came back — giving IBKR a 2-order bracket
            #   of parent + STPLMT (no stop market at all).
            #   When IBKR internally cancelled/rejected the parent (PendingSubmit
            #   resolved to rejection), the STPLMT child was released from
            #   PreSubmitted → became active → price hit its trigger →
            #   fired as a naked SELL → SHORT 500 with no position behind it.
            #
            # FIX: Send exactly 2 tickets: [parent LMT + STP child].
            #   2-ticket calls are reliable on CP Gateway — always acknowledged.
            #   STP has parentId → IBKR holds it PreSubmitted until parent fills.
            #   If parent is cancelled (by us or IBKR) → STP auto-cancelled.
            #   No child can fire without a real position behind it.
            #
            # STPLMT and target are placed in separate subsequent calls.
            # ══════════════════════════════════════════════════════════════

            # ── CALL 1: parent LMT + STP child (2 tickets) ──────────────
            # CP Gateway reliably returns exactly 2 acknowledgements per call.
            # STP has parentId → IBKR holds it PreSubmitted until parent fills.
            # If parent is cancelled (by us or IBKR) → STP auto-cancelled.
            parent_ticket = {
                "conid"    : conid,
                "side"     : side,
                "orderType": "LMT",
                "price"    : bar_close,
                "quantity" : qty,
                "tif"      : "DAY",
                "cOID"     : parent_ref,
                "order_ref": parent_ref,
            }
            stop_ticket = {
                "conid"    : conid,
                "side"     : close_side,
                "orderType": "STP",
                "price"    : stop_price,
                "quantity" : qty,
                "tif"      : "DAY",
                "parentId" : parent_ref,
                "cOID"     : stop_ref,
                "order_ref": stop_ref,
            }

            resp1 = self.session.place_order(self.account_id,
                                             [parent_ticket, stop_ticket])
            logger.info("[%s] bracket Call 1 response: %s", symbol, resp1)
            ids1 = self._handle_order_response(resp1, symbol, known_refs={
                "parent": parent_ref,
                "stop"  : stop_ref,
            })

            parent_oid  = ids1.get("parent") if ids1 else None
            stop_oid    = ids1.get("stop")   if ids1 else None
            stoplmt_oid = None

            if not parent_oid:
                logger.error("[%s] submit_entry: bracket Call 1 not acknowledged — aborting",
                             symbol)
                return False

            logger.info("[%s] Parent LMT placed   entry=%.4f  oid=%s",
                        symbol, bar_close, parent_oid)
            self._register_order(parent_oid, symbol, side, "ENTRY")

            if stop_oid:
                logger.info("[%s] Child STP placed    stop=%.4f  oid=%s  [PreSubmitted]",
                            symbol, stop_price, stop_oid)
                self._register_order(stop_oid, symbol, close_side, "STOP")
            else:
                logger.warning("[%s] Child STP not acknowledged — NO stop protection.",
                               symbol)

            # ── CALL 2: STPLMT + target (2 tickets) ──────────────────────
            # Both placed in one call — 2 tickets, always reliable.
            # STPLMT uses parentId → also auto-cancelled if parent cancels.
            # Target is standalone LMT — only fills if price reaches it,
            # so it cannot create a naked position.
            stoplmt_ticket = {
                "conid"    : conid,
                "side"     : close_side,
                "orderType": "STP LMT",
                "price"    : stop_lmt_price,
                "auxPrice" : stop_lmt_trigger,
                "quantity" : qty,
                "tif"      : "DAY",
                "parentId" : parent_ref,
                "cOID"     : stoplmt_ref,
                "order_ref": stoplmt_ref,
            }
            target_ticket_early = {
                "conid"    : conid,
                "side"     : target_side,
                "orderType": "LMT",
                "price"    : target_price,
                "quantity" : qty,
                "tif"      : "DAY",
                "cOID"     : target_ref,
                "order_ref": target_ref,
            }

            resp2 = self.session.place_order(self.account_id,
                                             [stoplmt_ticket, target_ticket_early])
            logger.info("[%s] bracket Call 2 response: %s", symbol, resp2)
            ids2 = self._handle_order_response(resp2, symbol, known_refs={
                "stop_lmt": stoplmt_ref,
                "target"  : target_ref,
            })

            # After ref-matching fix: stop_lmt slot = STPLMT, target slot = TARGET
            stoplmt_oid       = (ids2.get("stop_lmt") or ids2.get("stop")) if ids2 else None
            _target_oid_early = ids2.get("target") if ids2 else None

            if stoplmt_oid:
                logger.info("[%s] Child STP LMT placed trigger=%.4f lmt=%.4f oid=%s  [PreSubmitted]",
                            symbol, stop_lmt_trigger, stop_lmt_price, stoplmt_oid)
                self._register_order(stoplmt_oid, symbol, close_side, "STOP_LMT")
            else:
                logger.warning("[%s] Child STP LMT not acknowledged — no stop limit at entry.",
                               symbol)

            # target_oid comes from Call 2 (placed alongside STPLMT above)
            target_oid = _target_oid_early
            if target_oid:
                logger.info("[%s] Target LMT placed   target=%.4f oid=%s",
                            symbol, target_price, target_oid)
                self._register_order(target_oid, symbol, target_side, "TARGET")
            else:
                logger.warning("[%s] Target LMT not acknowledged — "
                               "exit_logic bar-close monitoring as fallback.",
                               symbol)

                        # ── Store all order IDs ───────────────────────────────────────
            order_ids = {"parent": parent_oid}
            if stop_oid:    order_ids["stop"]     = stop_oid
            if stoplmt_oid: order_ids["stop_lmt"] = stoplmt_oid
            if target_oid:  order_ids["target"]   = target_oid
            self._order_ids[symbol]          = order_ids
            self._order_ids[symbol + "_qty"] = qty
            logger.info("[%s] Order IDs captured: %s", symbol, order_ids)

            # ── Non-blocking fill verification (background thread) ────────
            #
            # ARCHITECTURE NOTE — why we do NOT block here:
            #   The bracket uses parentId on all children. IBKR holds children
            #   in PreSubmitted until the parent fills. If the parent is cancelled
            #   (by us or by IBKR at EOD), children are cancelled automatically.
            #   Blocking 60s killed every entry in after-hours (Apr 13 log 3):
            #   price moved away before poll finished → bot cancelled everything.
            #
            # WHAT WE DO INSTEAD:
            #   1. Return True immediately so the position is tracked internally.
            #   2. Spawn a background thread that polls for up to 60s.
            #   3. If fill confirmed → update actual qty (handles partial fills).
            #   4. If no fill in 60s → cancel parent + all children + clean state.
            #      This is safe because IBKR will also cancel DAY children at EOD,
            #      but we do it sooner to prevent any gap between parent cancel
            #      and child cancel during the session.

            def _verify_fill_async(sym, p_oid, s_oid, sl_oid, t_oid,
                                   cfg_qty, sig, open_pos_ref,
                                   expected_side=""):
                filled_qty, avg_fill = self._poll_for_fill(
                    p_oid, sym, timeout=60, expected_side=expected_side)
                if filled_qty == 0:
                    logger.warning(
                        "[%s] Parent LMT %s did not fill within 60s — "
                        "cancelling parent + all children to prevent orphan STP.",
                        sym, p_oid)
                    # Before cancelling, do one final position check.
                    # If IBKR holds the position already, the order DID fill —
                    # the orders endpoint just lagged. Treat this as a fill.
                    # DIRECTION CHECK: must match our intended direction.
                    # A stale orphan in the opposite direction (e.g. SHORT 500
                    # while we just placed a LONG) must NOT be treated as our fill.
                    _expected_side = open_pos_ref.get(sym, {}).get("direction", "")                         if open_pos_ref else ""
                    try:
                        final_check = self.get_ibkr_actual_positions()
                        if sym in final_check:
                            fc_pos    = final_check[sym]
                            actual_qty = fc_pos["qty"]
                            actual_side = fc_pos.get("side", "")
                            if actual_qty > 0:
                                # Reject if wrong direction
                                if _expected_side and actual_side and                                         actual_side != _expected_side:
                                    logger.warning(
                                        "[%s] Pre-cancel check: IBKR holds %s %d "
                                        "but we entered %s — stale orphan, "
                                        "proceeding with cancel.",
                                        sym, actual_side, actual_qty, _expected_side)
                                else:
                                    logger.warning(
                                        "[%s] No fill on orders endpoint but IBKR holds "
                                        "%s %d — orders endpoint lagged. "
                                        "Treating as FILLED, keeping position.",
                                        sym, actual_side, actual_qty)
                                    if actual_qty != cfg_qty:
                                        sig["_actual_fill_qty"] = actual_qty
                                        self._order_ids[sym + "_qty"] = actual_qty
                                        with self._pos_lock:
                                            if open_pos_ref is not None and sym in open_pos_ref:
                                                open_pos_ref[sym]["quantity"] = actual_qty
                                    return  # position is real — do NOT cancel
                    except Exception:
                        pass

                    # No position on IBKR either — genuinely no fill.
                    # Cancel remaining children. If cancel returns 400, the
                    # order already filled/expired on IBKR — still safe to continue.
                    got_400 = False
                    for label, oid in [("parent",   p_oid),
                                       ("stop",     s_oid),
                                       ("stop_lmt", sl_oid),
                                       ("target",   t_oid)]:
                        if oid:
                            try:
                                self.session.cancel_order(self.account_id, oid)
                                logger.info("[%s] Cancelled %s order %s",
                                            sym, label, oid)
                            except Exception as ce:
                                if "400" in str(ce):
                                    got_400 = True
                                    logger.warning(
                                        "[%s] Could not cancel %s %s (400 — already "
                                        "filled/gone on IBKR): %s",
                                        sym, label, oid, ce)
                                else:
                                    logger.warning("[%s] Could not cancel %s %s: %s",
                                                   sym, label, oid, ce)

                    # If ANY cancel returned 400, re-check positions one more time.
                    # 400 on a cancel = the order filled after our poll window closed.
                    if got_400:
                        try:
                            recheck = self.get_ibkr_actual_positions()
                            if sym in recheck and recheck[sym]["qty"] > 0:
                                rpos  = recheck[sym]
                                rqty  = rpos["qty"]
                                rside = rpos.get("side", "")
                                # Direction check — reject orphan in opposite direction
                                if _expected_side and rside and rside != _expected_side:
                                    logger.warning(
                                        "[%s] 400 cancel + IBKR holds %s %d "
                                        "but we entered %s — stale orphan. "
                                        "Proceeding with state cleanup.",
                                        sym, rside, rqty, _expected_side)
                                else:
                                    logger.warning(
                                        "[%s] 400 cancel + position exists "
                                        "(%s %d) — order filled AFTER poll timeout. "
                                        "Keeping position.",
                                        sym, rside, rqty)
                                    if rqty != cfg_qty:
                                        sig["_actual_fill_qty"] = rqty
                                        self._order_ids[sym + "_qty"] = rqty
                                        with self._pos_lock:
                                            if open_pos_ref is not None and sym in open_pos_ref:
                                                open_pos_ref[sym]["quantity"] = rqty
                                    return  # position is real — do NOT cancel
                        except Exception:
                            pass

                    # Remove from open_positions — no fill, no position
                    if open_pos_ref is not None:
                        open_pos_ref.pop(sym, None)
                    self._order_ids.pop(sym, None)
                    self._order_ids.pop(sym + "_qty", None)
                    self._unregister_symbol(sym)
                    logger.info("[%s] Entry cancelled — position state cleaned up", sym)
                else:
                    # Fill confirmed — update actual qty for partial fills
                    actual = int(filled_qty)
                    if actual != cfg_qty:
                        logger.warning(
                            "[%s] PARTIAL FILL — ordered %d, filled %d. "
                            "Updating tracked qty.", sym, cfg_qty, actual)
                        self._order_ids[sym + "_qty"] = actual
                        sig["_actual_fill_qty"] = actual
                        with self._pos_lock:
                            if open_pos_ref is not None and sym in open_pos_ref:
                                open_pos_ref[sym]["quantity"] = actual
                    if avg_fill > 0:
                        sig["_actual_fill_price"] = avg_fill
                        if open_pos_ref is not None and sym in open_pos_ref:
                            open_pos_ref[sym]["entry_price"] = avg_fill
                    logger.info("[%s] Fill verified — qty=%d  avg=%.4f",
                                sym, actual, avg_fill or bar_close)

            # Pass open_positions reference so the thread can clean up if needed
            _open_pos = getattr(self, "_open_positions", None)
            # Pass expected direction so position fallback checks don't confuse
            # a stale orphan in the opposite direction for our fill.
            _entry_direction = signal.get("signal", "")  # "LONG" or "SHORT"
            _t = threading.Thread(
                target=_verify_fill_async,
                args=(symbol, parent_oid, stop_oid, stoplmt_oid, target_oid,
                      qty, signal, _open_pos),
                kwargs={"expected_side": _entry_direction},
                daemon=True,
                name=f"fill-verify-{symbol}",
            )
            _t.start()
            logger.info("[%s] Fill verification started in background (max 60s)",
                        symbol)

            signal["_actual_fill_price"]   = bar_close
            signal["_actual_fill_qty"]     = qty
            signal["_actual_stop_price"]   = stop_price
            signal["_actual_target_price"] = target_price

            return True

        except Exception as exc:
            resp_text = ""
            if hasattr(exc, "response") and exc.response is not None:
                try:
                    resp_text = f" | IBKR response: {exc.response.text}"
                except Exception:
                    pass
            logger.error("[%s] submit_entry failed: %s%s", symbol, exc, resp_text)

        return False

    def _handle_order_response(self, response: Any, symbol: str,
                                known_refs: dict = None) -> Optional[Dict]:
        """
        Process raw IBKR order response.
        Automatically confirms any order reply messages.
        Returns a dict of order IDs {parent, stop, stop_lmt, target} or None.

        known_refs: dict of {slot: cOID_string} we submitted — e.g.
            {"parent": "PARENT_...", "stop": "STOP_...", "stop_lmt": "STPLMT_...",
             "target": "TARGET_..."}
        When encrypt_message:1 fires, IBKR only returns the parent in the response.
        The children ARE on IBKR but their OIDs are withheld. By pre-loading
        known_refs, the Priority 4 recovery can find every child by its cOID
        via the live-orders endpoint — regardless of what the response contains.
        """
        if isinstance(response, dict):
            if "error" in response:
                logger.error("[%s] Order error: %s", symbol, response["error"])
                return None
            response = [response]
        elif not isinstance(response, list):
            response = [response]

        # Child slots filled in sequence as children are detected
        _CHILD_SLOTS  = ["stop", "stop_lmt", "target"]
        _SLOT_ORDER   = ["parent"] + _CHILD_SLOTS   # for pure positional fallback
        confirmed_ids : Dict[str, str] = {}
        _child_idx    = 0   # next child slot to fill
        _pos_idx      = 0   # positional counter (fallback only)

        for item in response:
            if not isinstance(item, dict):
                continue

            # ── Order reply message (requires confirmation) ───────────────
            if "id" in item and "message" in item:
                msg_id = item["id"]
                logger.info("[%s] Confirming order message id=%s msg=%s",
                            symbol, msg_id, item.get("messageIds", []))
                try:
                    confirm_resp = self.session.reply_confirm(msg_id)
                    if isinstance(confirm_resp, list):
                        for cr in confirm_resp:
                            if isinstance(cr, dict):
                                oid = cr.get("order_id")
                                if oid:
                                    confirmed_ids.setdefault("parent", str(oid))
                                    logger.info("[%s] Order confirmed: order_id=%s status=%s",
                                                symbol, oid, cr.get("order_status", ""))
                                if "id" in cr and "message" in cr:
                                    try:
                                        self.session.reply_confirm(cr["id"])
                                    except Exception:
                                        pass
                    elif isinstance(confirm_resp, dict):
                        oid = confirm_resp.get("order_id")
                        if oid:
                            confirmed_ids.setdefault("parent", str(oid))
                            logger.info("[%s] Order confirmed: order_id=%s", symbol, oid)
                except Exception as exc:
                    logger.error("[%s] Reply confirm failed: %s", symbol, exc)
                continue   # reply messages don't count as positional slots

            # ── Normal acknowledgement ────────────────────────────────────
            if "order_id" not in item:
                continue

            oid = str(item["order_id"])
            if oid == "-1":
                logger.warning("[%s] Order component failed: %s",
                               symbol, item.get("text", ""))
                _pos_idx += 1
                continue

            ref          = item.get("order_ref", "")
            local_oid    = item.get("local_order_id", "")
            # IBKR sometimes returns the ref in local_order_id but not order_ref.
            # Use whichever is non-empty for matching.
            ref_key      = ref or local_oid
            has_parent   = "parent_order_id" in item   # this item IS a child
            is_parent_id = "PARENT" in local_oid or "PARENT" in ref

            # ── Priority 1: ref-based matching ────────────────────────────
            # Fires when IBKR returns our order_ref or local_order_id string.
            if "STOP_" in ref_key and "LMT" not in ref_key and "STPLMT" not in ref_key:
                confirmed_ids["stop"]     = oid
                logger.debug("[%s] Captured stop     %s (ref-match)", symbol, oid)
            elif "STPLMT" in ref_key:
                confirmed_ids["stop_lmt"] = oid
                logger.debug("[%s] Captured stop_lmt %s (ref-match)", symbol, oid)
            elif "TARGET" in ref_key:
                confirmed_ids["target"]   = oid
                logger.debug("[%s] Captured target   %s (ref-match)", symbol, oid)
            elif "PARENT" in ref_key or is_parent_id:
                confirmed_ids["parent"]   = oid
                logger.debug("[%s] Captured parent   %s (ref-match)", symbol, oid)

            # ── Priority 2: field-based detection ────────────────────────
            # Fires when encrypt_message:1 hides the order_ref.
            # Detect parent vs child by which fields are present.
            elif has_parent:
                # This item has parent_order_id → it is a child order
                if _child_idx < len(_CHILD_SLOTS):
                    slot = _CHILD_SLOTS[_child_idx]
                    confirmed_ids[slot] = oid
                    logger.debug("[%s] Captured %s %s (field-detect child, slot %d)",
                                 symbol, slot, oid, _child_idx)
                    _child_idx += 1
                else:
                    logger.debug("[%s] Extra child %s beyond expected slots — ignored", symbol, oid)
            elif is_parent_id:
                # local_order_id contains PARENT_ → this is the parent
                confirmed_ids["parent"] = oid
                logger.debug("[%s] Captured parent %s (field-detect parent)", symbol, oid)

            # ── Priority 3: pure positional fallback ─────────────────────
            # Last resort when none of the above signals are present.
            else:
                if _pos_idx < len(_SLOT_ORDER):
                    slot = _SLOT_ORDER[_pos_idx]
                    confirmed_ids[slot] = oid
                    logger.debug("[%s] Captured %s %s (positional slot %d)",
                                 symbol, slot, oid, _pos_idx)
                else:
                    logger.debug("[%s] Extra item %s beyond expected slots — ignored", symbol, oid)

            _pos_idx += 1

        # ── Priority 4: live-orders recovery ─────────────────────────────────
        # If IBKR returned encrypt_message:1 with no identifying fields and
        # positional fallback also failed (e.g. empty response, all -1 IDs),
        # query the live orders endpoint and match by our cOID refs.
        # This prevents any ID from being ghosted after a bracket submission.
        needs_recovery = not confirmed_ids.get("parent") or not confirmed_ids.get("stop")
        if needs_recovery:
            try:
                live_resp = self.session.get_live_orders(self.account_id)
                live_orders = []
                if isinstance(live_resp, dict):
                    live_orders = live_resp.get("orders", [])
                elif isinstance(live_resp, list):
                    live_orders = live_resp

                # Build ref→slot map from known_refs we SENT (not from response).
                # When encrypt_message:1 fires, child refs never appear in the
                # response — building from response would miss them entirely.
                # Using our own sent refs guarantees we find every child.
                _ref_to_slot = {}
                if known_refs:
                    # Invert: slot → ref  becomes  ref → slot
                    for slot, ref in known_refs.items():
                        if ref:
                            _ref_to_slot[ref] = slot
                else:
                    # Fallback: scan response items for any refs we can find
                    for item in response:
                        if not isinstance(item, dict):
                            continue
                        ref = item.get("order_ref", "") or item.get("cOID", "")
                        if "PARENT" in ref:
                            _ref_to_slot[ref] = "parent"
                        elif "STPLMT" in ref:
                            _ref_to_slot[ref] = "stop_lmt"
                        elif "STOP_" in ref and "LMT" not in ref:
                            _ref_to_slot[ref] = "stop"
                        elif "TARGET" in ref:
                            _ref_to_slot[ref] = "target"

                for o in live_orders:
                    if not isinstance(o, dict):
                        continue
                    o_ref = o.get("order_ref", "") or o.get("orderRef", "")
                    o_id  = str(o.get("orderId", o.get("order_id", "")))
                    if not o_id or o_id == "0":
                        continue
                    # Match by order_ref string patterns
                    for ref_key, slot in _ref_to_slot.items():
                        if ref_key and ref_key in o_ref and slot not in confirmed_ids:
                            confirmed_ids[slot] = o_id
                            logger.info("[%s] Recovered %s=%s via live-orders query (ref=%s)",
                                        symbol, slot, o_id, o_ref)
            except Exception as _exc:
                logger.debug("[%s] live-orders recovery query failed: %s", symbol, _exc)

        if confirmed_ids:
            logger.info("[%s] Order IDs captured: %s", symbol, confirmed_ids)
        else:
            logger.warning("[%s] No order IDs captured from response: %s", symbol, response)
        return confirmed_ids if confirmed_ids else None



    # ── Single-order placement helper ─────────────────────────────────────────

    def _place_single_order(self, ticket: Dict, symbol: str,
                            label: str) -> Optional[str]:
        """
        Submit one order ticket and return its order_id, or None on failure.
        Handles both plain response and reply-confirm (encrypt_message) shapes.
        """
        try:
            resp  = self.session.place_order(self.account_id, [ticket])
            items = resp if isinstance(resp, list) else [resp]
            for item in items:
                if not isinstance(item, dict):
                    continue
                if "id" in item and "message" in item:
                    try:
                        cr = self.session.reply_confirm(item["id"])
                        crs = cr if isinstance(cr, list) else [cr]
                        for c in crs:
                            if isinstance(c, dict) and c.get("order_id"):
                                oid = str(c["order_id"])
                                logger.info("[%s] %s placed  order_id=%s",
                                            symbol, label, oid)
                                return oid
                    except Exception:
                        pass
                elif "order_id" in item and str(item["order_id"]) != "-1":
                    oid = str(item["order_id"])
                    logger.info("[%s] %s placed  order_id=%s", symbol, label, oid)
                    return oid
            logger.warning("[%s] %s placed but no order_id returned: %s",
                           symbol, label, resp)
            return None
        except Exception as exc:
            logger.error("[%s] %s FAILED: %s", symbol, label, exc)
            return None


    # ── Fill-confirmation polling ─────────────────────────────────────────────

    def _poll_for_fill(self, order_id: str, symbol: str,
                       timeout: int = 60, interval: int = 2,
                       expected_side: str = "") -> tuple:
        """
        Poll IBKR for fill confirmation before placing the Stop Market.

        BUG 1 FIX (wrong stop price):
            Stop was previously calculated from bar close (signal price).
            When parent fills at a different avg price (partial fills in lots),
            the stop was anchored to the wrong reference.
            e.g. QCOM 09-Apr: bar close 127.35, avg fill 127.42
                 Stop placed 127.95 but should have been 128.02.
            Fix: return actual avg_fill_price → caller uses it for stop calc.

        BUG 2 FIX (stop fires before parent fills):
            Stop was placed 200ms after parent, while parent still PendingSubmit.
            If price moved against us in those 200ms, stop triggered against
            zero/partial position → net SHORT.
            e.g. PLTR 09-Apr: LONG parent pending, SELL STP triggered first.
            Fix: do not place stop until this function confirms a fill.

        Returns (filled_qty, avg_fill_price).
        Returns (0, 0.0) if order cancelled or timed out with no fill.
        """
        deadline = time.time() + timeout
        logger.info("[%s] Waiting for fill on order %s (max %ds)...",
                    symbol, order_id, timeout)

        while time.time() < deadline:
            try:
                # ── Primary: check live orders for fill ───────────────────
                orders_resp = self.session._get("/iserver/account/orders",
                                                params={"filters": "filled,partially_filled"})
                orders_list = []
                if isinstance(orders_resp, dict):
                    orders_list = orders_resp.get("orders", [])
                elif isinstance(orders_resp, list):
                    orders_list = orders_resp

                for o in orders_list:
                    oid = str(o.get("orderId", o.get("order_id", "")))
                    if oid != str(order_id):
                        continue
                    status    = str(o.get("status", o.get("order_status", ""))).lower()
                    filled    = float(o.get("filledQuantity",
                                     o.get("filled_quantity", 0)) or 0)
                    avg_price = float(o.get("avgPrice",
                                     o.get("average_price", 0)) or 0)

                    if "cancel" in status or "inactive" in status:
                        # Parent was cancelled externally by IBKR (rejected,
                        # expired, or manually cancelled). Return 0 immediately
                        # so the caller cancels all children right away —
                        # no need to wait the full 60s.
                        logger.warning("[%s] Parent order %s was CANCELLED by IBKR "
                                       "(status=%s) — cancelling all children now.",
                                       symbol, order_id, status)
                        return 0, 0.0

                    if filled > 0 and avg_price > 0:
                        logger.info(
                            "[%s] Fill confirmed: order=%s  qty=%.0f  avg=%.4f",
                            symbol, order_id, filled, avg_price)
                        return filled, avg_price

                # ── Fallback: check actual IBKR positions ─────────────────
                # Position endpoint updates faster than order endpoint on some
                # IBKR gateway versions.
                actual = self.get_ibkr_actual_positions()
                if symbol in actual:
                    pos = actual[symbol]
                    qty_held = pos["qty"]
                    side_held = pos.get("side", "")
                    if qty_held > 0:
                        # DIRECTION CHECK: if we expected LONG but IBKR holds
                        # SHORT (or vice versa), this is a stale orphan — NOT
                        # our fill. Skip it to avoid false confirmation.
                        if expected_side and side_held and side_held != expected_side:
                            logger.warning(
                                "[%s] Position fallback: IBKR holds %s %d but "
                                "we entered %s — this is a stale orphan, "
                                "NOT our fill. Ignoring.",
                                symbol, side_held, qty_held, expected_side)
                        else:
                            # Try to get actual avg fill price from orders endpoint
                            _avg_px = 0.0
                            try:
                                _ord_resp = self.session._get(
                                    "/iserver/account/orders",
                                    params={"filters": "filled,partially_filled"})
                                _ord_list = (_ord_resp.get("orders", [])
                                             if isinstance(_ord_resp, dict) else _ord_resp or [])
                                for _o in _ord_list:
                                    if str(_o.get("orderId", _o.get("order_id",""))) == str(order_id):
                                        _avg_px = float(_o.get("avgPrice",
                                                        _o.get("average_price", 0)) or 0)
                                        break
                            except Exception:
                                pass
                            logger.info(
                                "[%s] Fill confirmed via position endpoint: "
                                "qty=%.0f  side=%s  avg=%.4f%s",
                                symbol, qty_held, side_held, _avg_px,
                                " (using bar_close as fallback)" if _avg_px == 0 else "")
                            return qty_held, _avg_px  # caller uses bar_close if 0.0

            except Exception as exc:
                logger.debug("[%s] _poll_for_fill query error: %s", symbol, exc)

            time.sleep(interval)

        logger.warning("[%s] _poll_for_fill: no fill in %ds", symbol, timeout)
        # ── Final fallback before declaring no-fill ────────────────────────
        # IBKR CP Gateway orders endpoint can lag 60-90s behind the exchange
        # during high-activity bursts (11 simultaneous fills on 14-Apr).
        # Before giving up, do one last check of the positions endpoint.
        # DIRECTION CHECK is mandatory: if IBKR holds the OPPOSITE side,
        # it is a stale orphan (e.g. prior SHORT still open while LONG was
        # just placed). Confirming that as a fill causes trail stops to fire
        # in the wrong direction, compounding the orphan. (AVGO 14-Apr:
        # SHORT 500 orphan → confirmed as LONG fill → 12 trail SELL stops
        # fired → SHORT grew from 500 to 6500 shares.)
        try:
            actual_final = self.get_ibkr_actual_positions()
            if symbol in actual_final:
                pos_final   = actual_final[symbol]
                qty_held    = pos_final["qty"]
                side_held   = pos_final.get("side", "")
                if qty_held > 0:
                    if expected_side and side_held and side_held != expected_side:
                        logger.warning(
                            "[%s] _poll_for_fill final check: IBKR holds %s %d "
                            "but we entered %s — stale orphan, NOT our fill. "
                            "Declaring no-fill.",
                            symbol, side_held, qty_held, expected_side)
                        # Fall through to return (0, 0.0) below
                    else:
                        # Try to get actual avg fill price before returning
                        _avg_px2 = 0.0
                        try:
                            _ord_resp2 = self.session._get(
                                "/iserver/account/orders",
                                params={"filters": "filled,partially_filled"})
                            _ord_list2 = (_ord_resp2.get("orders", [])
                                          if isinstance(_ord_resp2, dict) else _ord_resp2 or [])
                            for _o2 in _ord_list2:
                                if str(_o2.get("orderId", _o2.get("order_id",""))) == str(order_id):
                                    _avg_px2 = float(_o2.get("avgPrice",
                                                     _o2.get("average_price", 0)) or 0)
                                    break
                        except Exception:
                            pass
                        logger.warning(
                            "[%s] _poll_for_fill: orders endpoint showed no fill "
                            "but IBKR DOES hold %s %d — treating as filled. avg=%.4f%s",
                            symbol, side_held, qty_held, _avg_px2,
                            " (using bar_close as fallback)" if _avg_px2 == 0 else "")
                        return qty_held, _avg_px2
        except Exception as exc:
            logger.debug("[%s] _poll_for_fill final position check failed: %s",
                         symbol, exc)
        return 0, 0.0

    # ── Config reader for stop-management params ──────────────────────────────

    @staticmethod
    def _read_stop_cfg(symbol: str) -> Dict:
        """Read MPL, BEMultiplier, TrailingPerc, TrailingStop_Perc, SLS for symbol."""
        cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
        try:
            with open(cfg_path) as fh:
                cfg = json.load(fh)
        except Exception:
            cfg = {}
        stock_cfg  = cfg.get("Stocks", {}).get(symbol.upper(), {})
        mpl        = float(stock_cfg.get("mpl",  cfg.get("MPL_dollar_Amount", 0.60)))
        bem        = float(cfg.get("BEMultiplier",    1.0))
        trail_perc = float(cfg.get("TrailingPerc",    0.5))
        use_perc   = str(cfg.get("TrailingStop_Perc", "N")).upper() == "Y"
        sls        = float(cfg.get("StopLimitSpread", 0.03))
        slm        = float(cfg.get("SLMultiplier",    1.0))
        return {
            "mpl": mpl, "bem": bem, "trail_perc": trail_perc,
            "use_perc": use_perc, "sls": sls, "slm": slm,
        }

    # ── Trailing stop calculator ───────────────────────────────────────────────

    def compute_stop_update(self, symbol: str, pos: Dict, bar: Dict) -> Optional[Dict]:
        """
        Compute whether the three-layer stop stack needs to move this bar.
        Returns an UPDATE_STOPS action dict, or None if no movement needed.

        Called on EVERY bar while a position is open, before process_exit().

        ── Stop stack layout ───────────────────────────────────────────────
        The three IBKR orders that protect the position at all times:

          AT ENTRY (bracket placed by submit_entry, SLM=1 from config):
            Stop Market (STP)         = entry - MPL          PRIMARY exit
            Stop Limit  (STP LMT)     = entry - MPL*2 / entry - MPL*2 - SLS
            (STP LMT sits BELOW STP — if STP triggers, OCA cancels STP LMT)

          AFTER BREAKEVEN (price >= entry + MPL * BEMultiplier):
            First move — cancel original bracket stops, place:
            Stop Market (STP)         = entry + (MPL * BEM / 2)
            Stop Price  (STP trigger) = STP + SLS               (above STP)
            Stop Limit  (STP LMT lmt) = STP + SLS*2             (above STP_P)
            phase flag → "BREAKEVEN"

          TRAILING (every subsequent bar after BE activates):
            5-bar mode (TrailingStop_Perc=N):
              new_STP = min(last 5 closes) - MPL    (LONG)
              new_STP = max(last 5 closes) + MPL    (SHORT)
            Perc mode (TrailingStop_Perc=Y):
              new_STP = close * (1 - TrailingPerc/100)   (LONG)
              new_STP = close * (1 + TrailingPerc/100)   (SHORT)
            Stop only moves in profitable direction — never reverses.
            phase flag → "TRAIL"

        The phase flag is included in the returned action so apply_exit_action
        knows whether to cancel original bracket stops (BREAKEVEN) or just
        replace the existing trail stops (TRAIL).
        """
        scfg       = self._read_stop_cfg(symbol)
        mpl        = scfg["mpl"]
        bem        = scfg["bem"]
        use_perc   = scfg["use_perc"]
        trail_pct  = scfg["trail_perc"]

        direction    = pos.get("direction", "LONG")
        entry_price  = pos.get("entry_price", 0.0)
        current_stp  = pos.get("stop_price",  0.0)
        be_activated = pos.get("be_activated", False)
        close        = bar.get("close", 0.0)

        # Maintain a rolling deque of the last 5 closes in the position dict
        if not isinstance(pos.get("close_history"), deque):
            pos["close_history"] = deque(maxlen=5)
        pos["close_history"].append(close)

        # Cannot trail if entry/stop are unknown (adopted position with zero prices)
        if entry_price == 0.0 or current_stp == 0.0:
            return None

        be_dist = mpl * bem   # price distance above entry that triggers breakeven

        # ── Phase 2: Breakeven ─────────────────────────────────────────────
        if not be_activated:
            if direction == "LONG"  and close >= entry_price + be_dist:
                new_stp = round(entry_price + be_dist / 2.0, 4)
            elif direction == "SHORT" and close <= entry_price - be_dist:
                new_stp = round(entry_price - be_dist / 2.0, 4)
            else:
                return None   # not yet at breakeven threshold

            # Only move if improvement
            if (direction == "LONG"  and new_stp <= current_stp) or                (direction == "SHORT" and new_stp >= current_stp):
                return None

            pos["be_activated"] = True
            pos["stop_price"]   = new_stp
            logger.info("[%s] BE activated  close=%.4f  new_STP=%.4f  "
                        "(entry=%.4f BEM_dist=%.4f)",
                        symbol, close, new_stp, entry_price, be_dist)
            return {
                "action"   : "UPDATE_STOPS",
                "direction": direction,
                "updates"  : {"stop_price": new_stp, "phase": "BREAKEVEN",
                              "be_activated": False},   # False = first time
            }

        # ── Phase 3: Trailing ──────────────────────────────────────────────
        history = list(pos["close_history"])

        if use_perc:
            if direction == "LONG":
                new_stp = round(close * (1.0 - trail_pct / 100.0), 4)
            else:
                new_stp = round(close * (1.0 + trail_pct / 100.0), 4)
        else:  # 5-bar mode
            if len(history) < 1:
                return None
            if direction == "LONG":
                new_stp = round(min(history) - mpl, 4)
            else:
                new_stp = round(max(history) + mpl, 4)

        # Stops only move in the profitable direction
        if direction == "LONG"  and new_stp <= current_stp:
            return None
        if direction == "SHORT" and new_stp >= current_stp:
            return None

        pos["stop_price"] = new_stp
        logger.info("[%s] Trail %s  close=%.4f  new_STP=%.4f  prev_STP=%.4f  "
                    "(BE already active — trailing all 3 stop orders)",
                    symbol, "PERC" if use_perc else "5BAR",
                    close, new_stp, current_stp)
        return {
            "action"   : "UPDATE_STOPS",
            "direction": direction,
            "updates"  : {"stop_price": new_stp, "phase": "TRAIL",
                          "be_activated": True},
        }

    # ── Exit order management ──────────────────────────────────────────────

    def apply_exit_action(self, symbol: str, action: Dict):
        """
        Translate an exit_logic action into IBKR API calls.

        action types:
            UPDATE_STOPS   → modify stop and stop-limit orders
            CLOSE_LIMIT    → cancel stops, place closing limit order
            CLOSE_MARKET   → cancel all open orders, place market close
            STOP_HIT       → clean up position state (IBKR executed the stop)
            NONE           → do nothing
        """
        action_type = action.get("action", "NONE")
        if action_type == "NONE":
            return

        ids        = self._order_ids.get(symbol, {})
        direction  = action.get("direction", "LONG")
        conid      = self._conid_cache.get(symbol)
        close_side = "SELL" if direction == "LONG" else "BUY"   # side to CLOSE position

        if action_type == "UPDATE_STOPS":
            # ═══════════════════════════════════════════════════════════════
            # STOP MANAGEMENT STACK
            # ═══════════════════════════════════════════════════════════════
            #
            # Three-order protection stack at all times after entry:
            #
            #   Phase 1 — INITIAL (entry to breakeven trigger)
            #   ─────────────────────────────────────────────
            #   [Entry]          ← filled parent limit order
            #   Stop Market STP  ← entry − MPL  (guaranteed market fill)
            #   Stop Price STP   ← entry − MPL*SLM*2   (backstop below STP)
            #   Stop Limit LMT   ← entry − MPL*SLM*2 − SLS
            #
            #   These are the original bracket child orders placed at entry.
            #   They stay in place until breakeven activates.
            #
            #   Phase 2 — BREAKEVEN (price > entry + MPL * BEMultiplier)
            #   ─────────────────────────────────────────────────────────
            #   Cancel original bracket hard stops (stop, stop_lmt).
            #   Place new stack ABOVE entry:
            #     Trail Stop Market STP  = entry + (MPL * BEM / 2)
            #     Trail Stop Price STP   = Trail_STP + SLS      (above STP)
            #     Trail Stop Limit LMT   = Trail_STP + SLS*2    (above STP_PRICE)
            #
            #   Phase 3 — TRAILING (ongoing, every bar after breakeven)
            #   ──────────────────────────────────────────────────────
            #   On each bar, compute new_stp from trailing method:
            #     5-bar:  new_stp = min(last 5 closes) − MPL     (LONG)
            #             new_stp = max(last 5 closes) + MPL     (SHORT)
            #     Perc:   new_stp = max_high * (1 − TrailingPerc/100) (LONG)
            #             new_stp = min_low  * (1 + TrailingPerc/100) (SHORT)
            #
            #   Only move stops UP (LONG) or DOWN (SHORT) — never reverse.
            #   Update all three orders:
            #     Trail STP   = new_stp
            #     Trail STP_P = new_stp + SLS      (LONG) / new_stp − SLS (SHORT)
            #     Trail LMT   = new_stp + SLS*2    (LONG) / new_stp − SLS*2 (SHORT)
            #
            #   Implementation: place new orders FIRST, cancel old SECOND
            #   so the position is never unprotected during the swap.
            # ═══════════════════════════════════════════════════════════════

            updates   = action.get("updates", {})
            qty       = self._order_ids.get(symbol + "_qty", 100)
            side      = "SELL" if direction == "LONG" else "BUY"
            ts        = f"{int(time.time())}_{symbol}"

            # Read live config values (re-read each bar so edits take effect)
            try:
                _cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
                with open(_cfg_path) as _fh:
                    _cfg = json.load(_fh)
                sls = float(_cfg.get("StopLimitSpread", 0.03))
            except Exception:
                sls = 0.03

            phase              = updates.get("phase", "TRAIL")   # "BREAKEVEN" or "TRAIL"
            new_stp_price      = updates.get("stop_price")       # Stop Market trigger
            be_activated       = updates.get("be_activated", False)

            if not new_stp_price or not conid:
                return

            new_stp_price = round(new_stp_price, 4)

            # Compute the Stop-Price and Stop-Limit from the Stop Market
            # Rule: once in trailing phase, STP_PRICE and STP_LMT sit ABOVE
            # the Stop Market (LONG) so a slow/orderly decline hits the limit
            # first; if price gaps through all three, STP guarantees a fill.
            if direction == "LONG":
                new_stp_p_trigger = round(new_stp_price + sls,   4)   # Stop Price trigger (above STP)
                new_stp_l_limit   = round(new_stp_price + sls*2, 4)   # Stop Limit limit  (above STP_P)
            else:  # SHORT
                new_stp_p_trigger = round(new_stp_price - sls,   4)
                new_stp_l_limit   = round(new_stp_price - sls*2, 4)

            # ── Helper: place one standalone order, return order_id ───────
            def _place_single(ticket: Dict) -> Optional[str]:
                try:
                    resp = self.session.place_order(self.account_id, [ticket])
                    items = resp if isinstance(resp, list) else [resp]
                    for it in items:
                        if not isinstance(it, dict):
                            continue
                        if "id" in it and "message" in it:
                            try:
                                cr = self.session.reply_confirm(it["id"])
                                crs = cr if isinstance(cr, list) else [cr]
                                for c in crs:
                                    if isinstance(c, dict) and c.get("order_id"):
                                        return str(c["order_id"])
                            except Exception:
                                pass
                        elif "order_id" in it and str(it["order_id"]) != "-1":
                            return str(it["order_id"])
                except Exception as exc:
                    logger.error("[%s] Place order FAILED (%s): %s",
                                 symbol, ticket.get("order_ref", "?"), exc)
                return None

            # ── Helper: cancel order safely ───────────────────────────────
            def _cancel_safe(oid: Optional[str], label: str):
                if not oid:
                    return
                try:
                    self.session.cancel_order(self.account_id, oid)
                    logger.info("[%s] Cancelled old %s  oid=%s", symbol, label, oid)
                except Exception as exc:
                    # Log at WARNING so throttle/failure is visible in logs
                    logger.warning("[%s] Cancel %s %s failed: %s",
                                   symbol, label, oid, exc)
                self._unregister_order(oid)

            # ═══════════════════════════════════════════════════════════════
            # BREAKEVEN: first time position is in the money
            # ───────────────────────────────────────────────────────────────
            # AT ENTRY the bracket has only 2 orders (John's design):
            #   ids["parent"]  — filled LMT (entry)
            #   ids["stop"]    — STP Market only (entry + MPL for SHORT)
            #   No STPLMT at entry — Stop Price and Stop Limit are
            #   only placed here at breakeven and onwards.
            #
            # On BREAKEVEN:
            #   1. Place NEW Stop Market     → trail_stop       (BELOW for SHORT)
            #      This is the guaranteed-fill stop, deeper in profit.
            #   2. Place NEW Stop Price STP  → trail_stop_price (ABOVE Stop Market)
            #      Triggered first on an orderly move — better exit price.
            #   3. Place NEW Stop Limit      → trail_stop_lmt   (ABOVE Stop Price)
            #      Limit on the fill — prevents worst-case slippage.
            #   4. Cancel ids["stop"] (original entry STP Market)
            #   Order: place new ones FIRST, cancel old AFTER — never unprotected.
            #   SLMultiplier now controls the Stop Market ↔ Stop Price gap,
            #   as intended — it only applies once in the money.
            # ═══════════════════════════════════════════════════════════════
            if phase == "BREAKEVEN" and not be_activated:
                logger.info(
                    "[%s] Breakeven activated — moving STP to %.4f",
                    symbol, new_stp_price)

                # Only the STP moves at BE — modify in place, no new orders.
                _stp_oid = ids.get("stop")
                if _stp_oid:
                    try:
                        self.session.modify_order(self.account_id, _stp_oid, {
                            "conid"    : conid,
                            "side"     : close_side,
                            "orderType": "STP",
                            "price"    : new_stp_price,
                            "quantity" : qty,
                            "tif"      : "DAY",
                        })
                        logger.info("[%s] BE Stop moved to %.4f  oid=%s",
                                    symbol, new_stp_price, _stp_oid)
                    except Exception as _me:
                        logger.warning("[%s] BE Stop move failed: %s", symbol, _me)
                else:
                    logger.warning("[%s] BE: no STP OID to move", symbol)

                ids["trail_stop"] = _stp_oid   # still the same OID, just moved
                ids["be_activated"] = True
                self._order_ids[symbol] = ids

# ═══════════════════════════════════════════════════════════════
            # TRAIL: move all three standalone stops in profitable direction
            # ───────────────────────────────────────────────────────────────
            # Place new first, cancel old second — position never unprotected.
            # ═══════════════════════════════════════════════════════════════
            elif phase == "TRAIL":
                old_stp_oid   = ids.get("trail_stop")
                old_stp_p_oid = ids.get("trail_stop_price")
                old_stp_l_oid = ids.get("trail_stop_lmt")

                logger.info(
                    "[%s] Trail update  stp=%.4f  stp_p=%.4f  stp_l=%.4f",
                    symbol, new_stp_price, new_stp_p_trigger, new_stp_l_limit)

                # Place new Stop Market FIRST
                new_stp_oid = _place_single({
                    "conid"    : conid,
                    "side"     : close_side,
                    "orderType": "STP",
                    "price"    : new_stp_price,
                    "quantity" : qty,
                    "tif"      : "DAY",
                    "cOID"     : f"{symbol}_TRSTP_{ts}",
                    "order_ref": f"TRSTP_{ts}",
                })
                if new_stp_oid:
                    logger.info("[%s] Trail STP placed  stp=%.4f  oid=%s",
                                symbol, new_stp_price, new_stp_oid)
                else:
                    logger.warning("[%s] Trail STP placement failed — old STP kept", symbol)
                time.sleep(0.05)   # throttle guard between place calls

                # Place new Stop Price
                new_stp_p_oid = _place_single({
                    "conid"    : conid,
                    "side"     : close_side,
                    "orderType": "STP",
                    "price"    : new_stp_p_trigger,
                    "quantity" : qty,
                    "tif"      : "DAY",
                    "cOID"     : f"{symbol}_TRSTPP_{ts}",
                    "order_ref": f"TRSTPP_{ts}",
                })
                if new_stp_p_oid:
                    logger.info("[%s] Trail STP-Price placed  trigger=%.4f  oid=%s",
                                symbol, new_stp_p_trigger, new_stp_p_oid)

                # Place new Stop Limit
                new_stp_l_oid = _place_single({
                    "conid"    : conid,
                    "side"     : close_side,
                    "orderType": "STP LMT",
                    "auxPrice" : new_stp_p_trigger,
                    "price"    : new_stp_l_limit,
                    "quantity" : qty,
                    "tif"      : "DAY",
                    "cOID"     : f"{symbol}_TRSTPL_{ts}",
                    "order_ref": f"TRSTPL_{ts}",
                })
                if new_stp_l_oid:
                    logger.info("[%s] Trail STP-Limit placed  stop=%.4f  lmt=%.4f  oid=%s",
                                symbol, new_stp_p_trigger, new_stp_l_limit, new_stp_l_oid)

                # Cancel OLD orders AFTER new ones confirmed.
                # Small sleep between each cancel to avoid IBKR throttle (429)
                # when 3 positions each fire 3 cancels simultaneously = 9 calls/bar.
                _cancel_safe(old_stp_oid,   "trail STP");   time.sleep(0.05)
                _cancel_safe(old_stp_p_oid, "trail STP-Price"); time.sleep(0.05)
                _cancel_safe(old_stp_l_oid, "trail STP-Limit")

                time.sleep(0.15)   # let IBKR register new orders before sweep
                # Belt-and-suspenders: if any cancel was silently dropped by
                # IBKR (throttle / race), old stops survive and each triggers
                # a new position in the opposite direction, growing the book
                # (AVGO SHORT 500 → LONG 2501 → LONG 5501 incident).
                # Sweep ALL live IBKR orders for this conid and cancel any
                # survivors that are NOT the new orders we just placed.
                # Protected set = new trail orders + ALL other tracked orders
                # that are legitimately live (e.g. target LMT stays live during trailing).
                # Cancelling the target order here would remove the profit exit — bug.
                _protected_oids = {str(o) for o in [new_stp_oid, new_stp_p_oid, new_stp_l_oid] if o}
                for _key in ("target", "stop", "stop_lmt"):
                    _existing = ids.get(_key)
                    if _existing:
                        _protected_oids.add(str(_existing))
                _new_oids = _protected_oids
                try:
                    _live = self.session.get_live_orders(self.account_id, filters="open")
                    for _o in (_live or {}).get("orders", []):
                        if str(_o.get("conid","")) == str(conid):
                            _oid = str(_o.get("orderId", _o.get("order_id","")))
                            if _oid and _oid not in _new_oids:
                                try:
                                    self.session.cancel_order(self.account_id, _oid)
                                    logger.info("[%s] Trail sweep: cancelled stale order %s",
                                                symbol, _oid)
                                except Exception:
                                    pass
                except Exception as _exc:
                    logger.debug("[%s] Trail sweep failed: %s", symbol, _exc)

                # Store new IDs
                if new_stp_oid:
                    ids["trail_stop"]       = new_stp_oid
                    self._register_order(new_stp_oid, symbol, close_side, "TRAIL_STP")
                if new_stp_p_oid:
                    ids["trail_stop_price"] = new_stp_p_oid
                    self._register_order(new_stp_p_oid, symbol, close_side, "TRAIL_STP_P")
                if new_stp_l_oid:
                    ids["trail_stop_lmt"]   = new_stp_l_oid
                    self._register_order(new_stp_l_oid, symbol, close_side, "TRAIL_STP_L")
                self._order_ids[symbol] = ids

        elif action_type in ("CLOSE_MARKET", "CLOSE_LIMIT"):
            # Cancel all child orders (all three trailing stop keys)
            for key in ("stop", "stop_lmt", "target",
                        "trail_stop", "trail_stop_price", "trail_stop_lmt"):
                oid = ids.get(key)
                if oid:
                    try:
                        self.session.cancel_order(self.account_id, oid)
                        self._unregister_order(oid)
                    except Exception:
                        self._unregister_order(oid)

            close_coid = f"{symbol}_CLOSE_{int(time.time())}"
            if action_type == "CLOSE_MARKET":
                ticket = {
                    "conid"    : conid,
                    "side"     : close_side,
                    "orderType": "MKT",
                    "quantity" : self._order_ids.get(symbol + "_qty", 100),
                    "tif"      : "DAY",
                    "cOID"     : close_coid,
                }
            else:
                ticket = {
                    "conid"    : conid,
                    "side"     : close_side,
                    "orderType": "LMT",
                    "price"    : action.get("orders", {}).get("limit_price"),
                    "quantity" : self._order_ids.get(symbol + "_qty", 100),
                    "tif"      : "DAY",
                    "cOID"     : close_coid,
                }

            try:
                resp = self.session.place_order(self.account_id, [ticket])
                logger.info("[%s] Close order placed: %s", symbol, resp)
                close_ids = self._handle_order_response(resp, symbol)
                # Register the close order so EoD can cancel it if it lingers
                if close_ids:
                    for key, oid in close_ids.items():
                        self._register_order(oid, symbol, close_side, "EXIT_CLOSE")
                self._order_ids.pop(symbol, None)
                self._order_ids.pop(symbol + "_qty", None)
            except Exception as exc:
                logger.error("[%s] close order failed: %s", symbol, exc)

        elif action_type == "STOP_HIT":
            # Cancel ALL known order IDs from internal tracking first,
            # then do a belt-and-suspenders sweep of ALL live IBKR orders
            # for this symbol's conid.
            #
            # WHY THE SWEEP IS NEEDED (META 13-Apr-2026):
            #   BE STP market order partially filled 282 of 500 shares.
            #   Bot cleared internal state (tracking 500 closed).
            #   But 218 shares remained LONG on IBKR — unmanaged orphan.
            #   The sweep catches any remaining open orders that survived
            #   a partial fill, preventing this orphan from being re-opened.
            logger.info("[%s] Stop hit – cancelling all live orders then "
                        "cleaning up position state", symbol)
            ids = self._order_ids.get(symbol, {})
            for key in ("stop", "stop_lmt", "stop_price", "wide_stop", "target",
                        "trail_stop", "trail_stop_price", "trail_stop_lmt"):
                oid = ids.get(key)
                if oid:
                    try:
                        self.session.cancel_order(self.account_id, oid)
                        logger.info("[%s] STOP_HIT: cancelled %s  oid=%s",
                                    symbol, key, oid)
                    except Exception as exc:
                        logger.debug("[%s] STOP_HIT: cancel %s %s skipped "
                                     "(already gone): %s", symbol, key, oid, exc)
                    self._unregister_order(oid)

            # Belt-and-suspenders: cancel ALL live IBKR orders for this conid
            # 3 attempts with increasing delays — 400 errors are common during
            # mass fills when CP Gateway is under load.
            conid = self._conid_cache.get(symbol)
            if conid:
                for _sweep_attempt in range(3):
                    try:
                        _sw_delay = [0, 1.0, 2.0][_sweep_attempt]
                        if _sw_delay:
                            time.sleep(_sw_delay)
                        live_resp   = self.session.get_live_orders(self.account_id, filters="open")
                        live_orders = (live_resp or {}).get("orders", [])
                        swept = []
                        for o in live_orders:
                            if str(o.get("conid", "")) == str(conid):
                                oid = str(o.get("orderId", o.get("order_id", "")))
                                if oid:
                                    for _cancel_attempt in range(2):
                                        try:
                                            self.session.cancel_order(self.account_id, oid)
                                            swept.append(oid)
                                            break
                                        except Exception as _ce:
                                            if _cancel_attempt == 0:
                                                time.sleep(0.5)
                                            else:
                                                logger.warning("[%s] STOP_HIT: could not cancel "
                                                               "oid=%s: %s", symbol, oid, _ce)
                        if swept:
                            logger.info("[%s] STOP_HIT sweep: cancelled %d additional "
                                        "live IBKR order(s): %s", symbol, len(swept), swept)
                        break  # success
                    except Exception as exc:
                        if _sweep_attempt < 2:
                            logger.info("[%s] STOP_HIT sweep attempt %d failed (%s) — retrying",
                                        symbol, _sweep_attempt+1, exc)
                        else:
                            logger.warning("[%s] STOP_HIT sweep failed after 3 attempts: %s",
                                           symbol, exc)

            self._unregister_symbol(symbol)
            self._order_ids.pop(symbol, None)
            self._order_ids.pop(symbol + "_qty", None)
            # Brief pause before state cleanup — gives IBKR time to process
            # the individual OID cancels above before the sweep fires.
            # Without this, the sweep sees orders still "open" that were
            # just cancelled milliseconds ago, causing 400 errors.
            time.sleep(0.3)
            logger.info("[%s] Stop hit – position state cleaned up", symbol)

    # ── EoD square-off ────────────────────────────────────────────────────

    def eod_square_off(self, open_positions: Dict[str, Dict], skip_eod_close: bool = False):
        """
        End-of-Day full square-off.  Called once when EoDExitTime is reached.

        Step 1 — Cancel ALL tracked orders from our registry (by specific ID)
        Step 1b — Also cancel via IBKR API (belt + suspenders)
        Step 2 — Query IBKR for ACTUAL open positions (not internal state)
        Step 3 — Market close every position with a UNIQUE order ID

        FIXES applied (26-Mar-2026 incident):
        ─────────────────────────────────────
        • Uses _cancel_all_registered_orders() FIRST — this catches ghost
          orders that IBKR's get_live_orders API may miss.
        • Uses a unique cOID (timestamp-based) for every close attempt,
          preventing "Local order ID already registered" rejections.
        """
        logger.info("=== EoD SQUARE-OFF STARTED ===")

        # ── Step 1: Cancel ALL tracked orders from our registry ───────────
        # This is MORE RELIABLE than the API check below because it
        # catches orders in states that get_live_orders may not return
        # (e.g. PreSubmitted, PendingCancel, etc.)
        # Pass skip_eod_close through: safety-net calls preserve in-flight EOD_CLOSE orders
        self._cancel_all_registered_orders(skip_eod_close=skip_eod_close)

        # ── Step 1b: Also cancel via IBKR API (belt + suspenders) ─────────
        try:
            live_resp = self.session.get_live_orders(self.account_id)
            orders_list = []
            if isinstance(live_resp, dict):
                orders_list = live_resp.get("orders", [])
            elif isinstance(live_resp, list):
                orders_list = live_resp

            if not orders_list:
                logger.info("EoD: No open orders found on account")
            else:
                logger.info("EoD: Found %d open order(s) via API — cancelling all",
                            len(orders_list))
                for order in orders_list:
                    oid = str(order.get("orderId") or order.get("order_id", ""))
                    sym = order.get("ticker", order.get("symbol", "?"))
                    status = str(order.get("status", "")).upper()
                    # Only cancel orders that are still live
                    if status in ("FILLED", "CANCELLED", "INACTIVE"):
                        continue
                    # FIX (March-31 cancel-loop): Skip in-flight orders.
                    # Cancelling PreSubmitted/Submitted orders before IBKR
                    # routes them to the exchange caused the safety-net loop.
                    if status in ("PRESUBMITTED", "SUBMITTED"):
                        logger.info(
                            "EoD: Skipping cancel of order %s (%s) — "
                            "status=%s, letting it fill naturally", oid, sym, status)
                        continue
                    if not oid:
                        continue
                    try:
                        self.session.cancel_order(self.account_id, oid)
                        logger.info("EoD: Cancelled order %s  symbol=%s  status=%s",
                                    oid, sym, status)
                    except Exception as exc:
                        logger.warning("EoD: Cancel order %s failed: %s", oid, exc)

        except Exception as exc:
            logger.error("EoD: Failed to fetch live orders: %s", exc)

        # Small pause to let cancellations propagate before placing closes
        import time as _time
        _time.sleep(1.0)

        # ── Step 2: Query IBKR for ACTUAL positions ──────────────────────
        ibkr_actual = self.get_ibkr_actual_positions()

        if not ibkr_actual:
            logger.info("EoD: No actual positions on IBKR — nothing to close")
            # Clean up internal state since IBKR confirms we're flat
            if open_positions:
                logger.info("EoD: Clearing %d stale internal positions: %s",
                            len(open_positions), list(open_positions.keys()))
            logger.info("=== EoD SQUARE-OFF COMPLETE ===")
            return

        # ── Step 3: Market close every ACTUAL position ───────────────────
        # CRITICAL: Use a UNIQUE cOID for every attempt.  The old code
        # used f"{sym}_EOD_CLOSE" which caused the 26-Mar-2026 incident:
        # once IBKR registered that ID (even if the order filled),
        # every retry was rejected with "already registered".
        logger.info("EoD: Closing %d actual IBKR position(s): %s",
                    len(ibkr_actual), list(ibkr_actual.keys()))

        for sym, pos_info in ibkr_actual.items():
            actual_qty  = pos_info["qty"]
            actual_side = pos_info["side"]
            close_side  = "SELL" if actual_side == "LONG" else "BUY"
            conid       = self._conid_cache.get(sym)

            if not conid:
                logger.error("EoD: No conid for %s — cannot place close order", sym)
                continue

            # Unique cOID per attempt — timestamp ensures no collisions
            unique_coid = f"{sym}_EOD_{int(time.time() * 1000)}"

            ticket = {
                "conid"    : conid,
                "side"     : close_side,
                "orderType": "MKT",
                "quantity" : actual_qty,
                "tif"      : "DAY",
                "cOID"     : unique_coid,
            }
            try:
                resp = self.session.place_order(self.account_id, [ticket])
                # Check for rejection in response
                if isinstance(resp, dict) and "error" in resp:
                    logger.error("EoD: REJECTED %s  %s  qty=%d  error=%s",
                                 sym, actual_side, actual_qty, resp["error"][:200])
                else:
                    logger.info("EoD: Market close placed for %s  %s  qty=%d  resp=%s",
                                sym, actual_side, actual_qty, resp)
                    # Register the EoD close order in case we need to cancel it later
                    if isinstance(resp, list):
                        for item in resp:
                            if isinstance(item, dict) and item.get("order_id"):
                                self._register_order(
                                    str(item["order_id"]), sym, close_side, "EOD_CLOSE")
                self._order_ids.pop(sym, None)
                self._order_ids.pop(sym + "_qty", None)
            except Exception as exc:
                logger.error("EoD: Market close for %s failed: %s", sym, exc)

        logger.info("=== EoD SQUARE-OFF COMPLETE ===")

    # ── Position query ────────────────────────────────────────────────────

    def get_open_positions(self) -> Dict[str, bool]:
        """Return {symbol: True} for all symbols with open positions."""
        try:
            positions = self.session.get_positions(self.account_id)
            return {
                p.get("contractDesc", p.get("ticker", "?")): True
                for p in positions
                if float(p.get("position", 0)) != 0
            }
        except Exception as exc:
            logger.error("get_positions failed: %s", exc)
            return {}

    # ── IBKR actual position query (with qty and side) ────────────────────

    def get_ibkr_actual_positions(self) -> Dict[str, Dict]:
        """
        Query IBKR for real positions on the account.

        Returns {symbol: {"qty": abs_qty, "side": "LONG"|"SHORT"}}
        Only includes symbols with non-zero positions.
        """
        result = {}
        try:
            positions = self.session.get_positions(self.account_id)
            for p in positions:
                qty_raw = float(p.get("position", 0))
                if qty_raw == 0:
                    continue

                # portfolio2 uses 'ticker' as primary field; fall back to
                # contractDesc, symbol, then conid reverse-lookup from cache.
                sym = (p.get("ticker") or
                       p.get("contractDesc") or
                       p.get("symbol") or "")

                # Clean up — IBKR sometimes returns "AAPL\nNASDAQ.NMS"
                if sym and "\n" in sym:
                    sym = sym.split("\n")[0]
                sym = sym.strip().upper()

                # If still blank, reverse-lookup via conid cache
                if not sym or sym == "?":
                    pos_conid = str(p.get("conid", ""))
                    for _s, _c in self._conid_cache.items():
                        if str(_c) == pos_conid:
                            sym = _s
                            break

                if not sym or sym == "?":
                    logger.warning("get_ibkr_actual_positions: cannot resolve "
                                   "symbol for entry %s — skipping", p)
                    continue  # skip rather than polluting result with '?'

                result[sym] = {
                    "qty":      abs(int(qty_raw)),
                    "side":     "LONG" if qty_raw > 0 else "SHORT",
                    "avg_cost": float(p.get("avgCost", p.get("avgPrice", 0)) or 0),
                    "conid":    str(p.get("conid", "")),
                }
            logger.info("IBKR actual positions: %s",
                        {s: f"{v['side']} {v['qty']}" for s, v in result.items()} or "FLAT")
        except Exception as exc:
            logger.error("get_ibkr_actual_positions failed: %s", exc)
        return result

    # ── Position reconciliation (every 3 min) ────────────────────────────

    def reconcile_positions(self):
        """
        Compare internal open_positions with actual IBKR positions.

        Two directions of reconciliation:

        Direction 1 — Internal has it, IBKR does not:
            Stop or target was filled by IBKR. Cancel orphan orders and
            remove from internal tracking.

        Direction 2 — IBKR has it, internal does not:
            *** This is the doubling bug source. ***
            Bot closed internally (STOP_HIT cleared state) but the actual
            IBKR position survived (e.g. parent limit partially filled and
            bot never sent a close order to IBKR). This orphan position sits
            unmanaged. When a new signal fires for the same symbol, the IBKR
            position check may race and miss it — a new order goes through,
            adding to the existing position and doubling the quantity.

            FIX: When reconcile finds a symbol on IBKR that is NOT in
            open_positions, immediately send a market close order to flatten
            it. This prevents the position from doubling on the next signal.
        """
        if self._open_positions is None:
            return

        ibkr_actual = self.get_ibkr_actual_positions()

        # ── Direction 2: IBKR has position, bot does not know about it ───────
        # Check FIRST before Direction 1, so we always have a fresh ibkr_actual
        our_symbols = set(self._open_positions.keys())
        # Purge stale recently-closed entries (older than 5 minutes)
        _rc_now = time.time()
        self._recently_closed = {s: t for s, t in self._recently_closed.items()
                                  if _rc_now - t < 900}  # 15 min window
        for sym, pos_info in ibkr_actual.items():
            if sym in our_symbols:
                continue  # bot knows about it — handled in Direction 1 below
            # Skip recently-closed symbols — IBKR position cache lags up to 5 min
            # after a stop or target fills, the position still shows on IBKR
            if sym in self._recently_closed:
                logger.info("Reconcile: [%s] skipping — position closed %.0fs ago, "
                            "IBKR cache still shows it (clears in ~%.0fs)",
                            sym, _rc_now - self._recently_closed[sym],
                            900 - (_rc_now - self._recently_closed[sym]))
                continue
            # Orphan position — bot has no record of this
            conid = self._conid_cache.get(sym)
            if not conid:
                logger.warning(
                    "Reconcile: [%s] ORPHAN on IBKR (%s %d) but no conid cached "
                    "— cannot auto-close. Close manually.",
                    sym, pos_info["side"], pos_info["qty"])
                continue
            close_side = "SELL" if pos_info["side"] == "LONG" else "BUY"
            unique_coid = f"{sym}_RECONCILE_CLOSE_{int(time.time() * 1000)}"

            # ── Choose TIF based on whether exchange is currently open ────────
            # During regular session (09:30–16:00 EST) use DAY MKT.
            # After 16:00 EST the exchange is closed — DAY orders are rejected.
            # Instead place a MKT + GTC so it executes at the next open.
            # This handles the case where a parent LMT fills during the closing
            # auction (e.g. 15:59–16:02) and reconcile fires after 16:00.
            try:
                _now_est = datetime.datetime.now(
                    datetime.timezone(datetime.timedelta(hours=-4)))  # EDT
                _market_open  = _now_est.replace(hour=9,  minute=30, second=0, microsecond=0)
                _market_close = _now_est.replace(hour=16, minute=0,  second=0, microsecond=0)
                _in_session = _market_open <= _now_est <= _market_close
            except Exception:
                _in_session = True  # safe default — try DAY first

            if _in_session:
                # Regular session — DAY MKT closes immediately
                ticket = {
                    "conid"    : conid,
                    "side"     : close_side,
                    "orderType": "MKT",
                    "quantity" : pos_info["qty"],
                    "tif"      : "DAY",
                    "cOID"     : unique_coid,
                }
                tif_desc = "DAY MKT (regular session)"
            else:
                # Exchange closed — use GTC so it fires at next open
                # outsideRTH=False ensures it only fills during regular hours,
                # preventing an unintended pre-market fill.
                ticket = {
                    "conid"       : conid,
                    "side"        : close_side,
                    "orderType"   : "MKT",
                    "quantity"    : pos_info["qty"],
                    "tif"         : "GTC",
                    "outsideRTH"  : False,
                    "cOID"        : unique_coid,
                }
                tif_desc = "GTC MKT (exchange closed — will fill at next open)"

            # ── Check if this orphan could have a stop already ───────────────
            # If the bot just placed a stop (e.g. via emergency bracket) but
            # hasn't registered the position internally yet, closing at market
            # would double-close. We first check if there are live orders
            # protecting this position before sending a market close.
            # If stop+target are already live — adopt the position instead.

            # ── STEP 1: Cancel ALL live IBKR orders for this symbol first ────
            # ROOT CAUSE OF THE POSITION-FLIP LOOP (10-Apr-2026):
            #   When a bracket parent LMT fails to fill, the child STP order
            #   can survive into after-hours and execute there, creating a real
            #   position.  If the STP is still live when reconcile sends a MKT
            #   close, both orders are live simultaneously:
            #     - MKT close fills → position is flat
            #     - STP immediately re-opens the position in the opposite direction
            #   This creates a 3-hour flip loop where every reconcile close
            #   triggers another position.  The fix is to kill ALL open orders
            #   for this conid BEFORE placing the close, so nothing can reopen it.
            try:
                live_resp = self.session.get_live_orders(
                    self.account_id, filters="open")
                live_orders = (live_resp or {}).get("orders", [])
                cancelled_ids = []
                for o in live_orders:
                    o_conid = str(o.get("conid", ""))
                    if o_conid == str(conid):
                        oid = str(o.get("orderId", o.get("order_id", "")))
                        if oid:
                            try:
                                self.session.cancel_order(self.account_id, oid)
                                cancelled_ids.append(oid)
                            except Exception as ce:
                                logger.warning(
                                    "Reconcile: [%s] Could not cancel live order "
                                    "%s before orphan close: %s",
                                    sym, oid, ce)
                if cancelled_ids:
                    logger.info(
                        "Reconcile: [%s] Cancelled %d live IBKR order(s) before "
                        "orphan close: %s",
                        sym, len(cancelled_ids), cancelled_ids)
                    # Brief pause to let IBKR process the cancellations
                    time.sleep(0.5)
            except Exception as exc:
                logger.warning(
                    "Reconcile: [%s] Could not fetch live orders before orphan "
                    "close: %s — proceeding with close anyway", sym, exc)

            # ── STEP 2: Adopt or close the orphan ────────────────────────────
            # If live protective orders already exist (stop + target placed by
            # emergency bracket), adopt the position internally rather than
            # closing it at market and wiping out the protection.
            try:
                live_check = self.session.get_live_orders(
                    self.account_id, filters="open")
                live_orders = (live_check or {}).get("orders", [])
                protective = [o for o in live_orders
                              if str(o.get("conid","")) == str(conid)
                              and o.get("orderType","") in ("STP","LMT","STOP_LIMIT")]
            except Exception:
                protective = []

            if protective:
                # Live stop/target already in place — adopt internally
                direction = pos_info["side"]  # "LONG" or "SHORT"
                qty_a     = pos_info["qty"]
                logger.warning(
                    "Reconcile: [%s] ORPHAN POSITION — adopting internally "
                    "(found %d protective order(s) already live). "
                    "side=%s qty=%s",
                    sym, len(protective), direction, qty_a)
                with self._positions_lock if hasattr(self, "_positions_lock") else contextlib.nullcontext():
                    if sym not in self._open_positions:
                        self._open_positions[sym] = {
                            "symbol"          : sym,
                            "direction"       : direction,
                            "entry_price"     : 0.0,
                            "stop_price"      : 0.0,
                            "stop_limit_price": 0.0,
                            "target_price"    : 0.0,
                            "be_activated"    : False,
                            "max_high"        : 0.0,
                            "min_low"         : 0.0,
                            "high_history"    : [],
                            "low_history"     : [],
                            "quantity"        : qty_a,
                        }
            else:
                # No protective orders — close at market
                try:
                    resp = self.session.place_order(self.account_id, [ticket])
                    if isinstance(resp, dict) and resp.get("error"):
                        raise RuntimeError(resp["error"])
                    logger.warning(
                        "Reconcile: [%s] ORPHAN POSITION DETECTED — %s %d shares "
                        "found on IBKR with no internal record and no live orders. "
                        "Sending %s CLOSE. resp=%s",
                        sym, pos_info["side"], pos_info["qty"], tif_desc, resp)
                except Exception as exc:
                    logger.error(
                        "Reconcile: [%s] Failed to close orphan position (%s): %s — "
                        "MANUAL CLOSE REQUIRED.",
                        sym, tif_desc, exc)

        # ── Direction 1: Internal has it, IBKR does not ───────────────────────
        if not self._open_positions:
            return

        stale_symbols = [
            sym for sym in list(self._open_positions.keys())
            if sym not in ibkr_actual
        ]

        # ── Direction mismatch check: internal direction ≠ IBKR direction ────────
        # Scenario: WS feed dies, stop STP fires silently on IBKR, target LMT stays
        # live. When stop BUY 500 fills, position goes FLAT then target BUY partially
        # fills → LONG 157 (AMD/NVDA incident, Apr 14 2026).
        # Reconcile sees AMD=LONG 157 but internal thinks SHORT 500 → "confirmed".
        # Must detect direction mismatch and cancel ALL orders for that symbol NOW.
        for sym, int_pos in list(self._open_positions.items()):
            if sym not in ibkr_actual:
                continue
            ibkr_info  = ibkr_actual[sym]
            int_dir    = int_pos.get("direction", "")
            ibkr_side  = ibkr_info.get("side", "")
            if not int_dir or not ibkr_side:
                continue
            if int_dir != ibkr_side:
                logger.error(
                    "Reconcile DIRECTION MISMATCH: [%s] internal=%s but IBKR=%s %d. "
                    "Stop fired silently (WS dead?) and target order created orphan. "
                    "Cancelling ALL orders for %s and clearing internal state.",
                    sym, int_dir, ibkr_side, ibkr_info["qty"], sym)
                # ── Step 1: Cancel every tracked order individually ────────
                # Log failures explicitly — silent pass was hiding cancels that
                # left orders live on IBKR (caused LONG 1000 on Apr-17-2026).
                ids = self._order_ids.get(sym, {})
                for key in ("parent", "stop", "stop_lmt", "target",
                            "trail_stop", "trail_stop_price", "trail_stop_lmt"):
                    oid = ids.get(key)
                    if oid:
                        try:
                            self.session.cancel_order(self.account_id, oid)
                            logger.info("Reconcile mismatch: cancelled %s oid=%s for [%s]",
                                        key, oid, sym)
                        except Exception as exc:
                            logger.warning("Reconcile mismatch: cancel %s oid=%s FAILED: %s",
                                           key, oid, exc)

                # ── Step 2: Sweep all live orders for this conid (with retry) ──
                # If the sweep endpoint returns 400, retry up to 3 times.
                # A failed sweep leaves orphan orders live that can fire again
                # (Trail STP 225330709 + 225330710 caused LONG 1000 on Apr-17-2026).
                conid_mm = self._conid_cache.get(sym)
                if conid_mm:
                    _sweep_delays = [0, 0.5, 1.0]
                    for _sw_attempt, _sw_delay in enumerate(_sweep_delays, start=1):
                        if _sw_delay:
                            time.sleep(_sw_delay)
                        try:
                            mm_live = self.session.get_live_orders(
                                self.account_id, filters="open")
                            _swept = 0
                            for o in (mm_live or {}).get("orders", []):
                                if str(o.get("conid", "")) == str(conid_mm):
                                    oid = str(o.get("orderId", o.get("order_id", "")))
                                    if oid:
                                        try:
                                            self.session.cancel_order(self.account_id, oid)
                                            logger.info(
                                                "Reconcile mismatch sweep: [%s] cancelled %s",
                                                sym, oid)
                                            _swept += 1
                                        except Exception as exc:
                                            logger.warning(
                                                "Reconcile mismatch sweep: cancel %s FAILED: %s",
                                                oid, exc)
                            logger.info("Reconcile mismatch sweep [%s]: %d order(s) cancelled "
                                        "(attempt %d)", sym, _swept, _sw_attempt)
                            break   # sweep succeeded — stop retrying
                        except Exception as exc:
                            logger.warning("Reconcile mismatch sweep failed [%s] "
                                           "(attempt %d/%d): %s",
                                           sym, _sw_attempt, len(_sweep_delays), exc)
                self._unregister_symbol(sym)
                self._order_ids.pop(sym, None)
                self._order_ids.pop(sym + "_qty", None)
                with self._pos_lock:
                    self._open_positions.pop(sym, None)

        if not stale_symbols:
            logger.info("Reconcile: all %d internal positions confirmed on IBKR",
                        len(self._open_positions))
            return

        for sym in stale_symbols:
            logger.warning(
                "Reconcile: [%s] NOT found on IBKR — stop/target was filled. "
                "Removing from internal tracking and cancelling orphan orders.", sym)

            # Step A: Cancel all known order IDs from internal tracking
            ids = self._order_ids.get(sym, {})
            for key in ("parent", "stop", "stop_lmt", "target",
                        "trail_stop", "trail_stop_price", "trail_stop_lmt"):
                oid = ids.get(key)
                if oid:
                    try:
                        self.session.cancel_order(self.account_id, oid)
                        logger.info("Reconcile: Cancelled orphan order %s for %s (%s)",
                                    oid, sym, key)
                    except Exception:
                        pass  # may already be filled/cancelled

            # Step B: Belt-and-suspenders sweep — cancel ANY remaining live
            # IBKR orders for this conid that our internal dict may have missed.
            # This catches cases where IBKR returned children in unexpected order
            # during bracket placement and the OID was mis-assigned.
            conid_d1 = self._conid_cache.get(sym)
            if conid_d1:
                try:
                    live_resp_d1  = self.session.get_live_orders(
                        self.account_id, filters="open")
                    live_orders_d1 = (live_resp_d1 or {}).get("orders", [])
                    swept_d1 = []
                    for o in live_orders_d1:
                        if str(o.get("conid", "")) == str(conid_d1):
                            oid = str(o.get("orderId", o.get("order_id", "")))
                            if oid:
                                try:
                                    self.session.cancel_order(self.account_id, oid)
                                    swept_d1.append(oid)
                                except Exception:
                                    pass
                    if swept_d1:
                        logger.info(
                            "Reconcile D1 sweep: [%s] cancelled %d additional live "
                            "IBKR order(s) not in internal tracking: %s",
                            sym, len(swept_d1), swept_d1)
                except Exception as exc:
                    logger.warning("Reconcile D1 sweep failed for %s: %s", sym, exc)

            # Clean up the live order registry for this symbol
            self._unregister_symbol(sym)
            self._order_ids.pop(sym, None)
            self._order_ids.pop(sym + "_qty", None)
            with self._pos_lock:
                self._open_positions.pop(sym, None)
            logger.info("Reconcile: [%s] position removed from internal state", sym)

    # ── WebSocket watchdog ────────────────────────────────────────────────────

    def _start_ws_watchdog(self, timeout: int = 60, check_interval: int = 15,
                           resub_interval: int = 720):
        """
        Background thread that detects a silently dead WebSocket and reconnects.

        Root cause this fixes (10-Apr-2026):
          IBKR dropped the WebSocket connection without sending a close frame.
          The websocket-client library did NOT detect the disconnect (on_close
          was never called), so the reconnect logic never fired. The bot sat
          silently receiving no bar data for hours while the reconcile thread
          (on HTTP) continued working. Strategy was effectively blind.

        How it works:
          Every `check_interval` seconds, check how long ago the last WebSocket
          message was received. During market hours (09:25–16:05 EST), if no
          message has arrived for `timeout` seconds, the connection is dead.
          Force-close the old WebSocket, then reconnect and re-subscribe.
          This does NOT disturb any open positions — all position state,
          order IDs, and reconcile thread are unaffected by a reconnect.
        """
        self._stop_watchdog = threading.Event()

        def _watchdog_loop():
            logger.info("WS watchdog started (timeout=%ds, check every %ds)",
                        timeout, check_interval)
            while not self._stop_watchdog.wait(timeout=check_interval):
                # Outer try/except: catches ANY unhandled exception so the
                # watchdog thread can NEVER die silently.  Without this, a
                # single AttributeError (e.g. stream is None, or _last_msg_time
                # missing) would kill the thread permanently — the WS feed would
                # go dead and no reconnect would ever fire.
                try:  # outer — keeps thread alive no matter what
                    # ── Periodic umd+smd refresh per IBKR guidance ──────────
                    # IBKR confirmed subscriptions time out after ~15 minutes.
                    # Workflow: send umd (unsubscribe) then smd (re-subscribe)
                    # every resub_interval seconds to keep data flowing.
                    _now_ts = time.time()
                    if not hasattr(self, "_last_resub_time"):
                        self._last_resub_time = _now_ts
                    if (_now_ts - self._last_resub_time) >= resub_interval and self.stream:
                        logger.info("WS WATCHDOG: Periodic umd+smd refresh "
                                    "(%.0fs elapsed) for %d conids",
                                    _now_ts - self._last_resub_time,
                                    len(self._conid_cache))
                        for _sym, _cid in list(self._conid_cache.items()):
                            try:
                                self.stream.unsubscribe(_cid)
                                time.sleep(0.05)
                                self.stream.subscribe(_cid)
                                logger.info("WS WATCHDOG: Refreshed [%s] conid=%s", _sym, _cid)
                            except Exception as _re:
                                logger.warning("WS WATCHDOG: Refresh [%s] failed: %s", _sym, _re)
                        self._last_resub_time = _now_ts

                    # Only enforce during extended market hours
                    now_est = datetime.now(EST)
                    market_open  = now_est.replace(hour=9,  minute=25, second=0, microsecond=0)
                    market_close = now_est.replace(hour=16, minute=5,  second=0, microsecond=0)
                    in_market = market_open <= now_est <= market_close

                    if not in_market:
                        continue  # outside hours — silence is normal

                    if not self.stream:
                        continue

                    # Use _last_bar_time (real market data) NOT _last_msg_time
                    # _last_msg_time includes IBKR heartbeats which keep arriving
                    # even when market data subscriptions have silently died.
                    # _last_bar_time is only updated when a conid message arrives.
                    last_bar = getattr(self.stream, "_last_bar_time", None)
                    if last_bar is None:
                        continue   # stream not yet initialised
                    elapsed = time.time() - last_bar

                    if elapsed > timeout:
                        logger.warning(
                            "WS WATCHDOG: No WebSocket message for %.0fs "
                            "(threshold=%ds) — forcing reconnect",
                            elapsed, timeout)

                        # ── BUG 1 FIX (AMD/NVDA Apr-14-2026) ────────────────
                        # When the WS feed dies with open positions, the stop
                        # orders can fire silently on IBKR while the bot is
                        # blind. The target LMT stays live and partially fills
                        # (AMD → LONG 157, NVDA → LONG 33).
                        # Safety action: before reconnecting, cancel ALL child
                        # orders (stop, target, trails) for every open position
                        # so nothing can execute unsupervised while we are dark.
                        if self._open_positions:
                            # NOTE: We do NOT cancel stops/targets when WS dies.
                            # IBKR executes stop and limit orders independently
                            # of the WebSocket feed — they are live on the exchange
                            # and will protect positions even while we are blind.
                            # Cancelling them (old behaviour) LEFT positions naked
                            # which caused unprotected MKT closes via reconcile.
                            logger.warning(
                                "WS WATCHDOG: Feed dead with %d open position(s) %s — "
                                "stops/targets remain live on IBKR during reconnect.",
                                len(self._open_positions),
                                list(self._open_positions.keys()))

                        try:
                            # Close the dead socket (triggers on_close which
                            # calls connect(), but we call it explicitly too
                            # in case on_close is also blocked)
                            if self.stream._ws:
                                self.stream._ws.close()
                        except Exception:
                            pass

                        time.sleep(2)

                        # Force a fresh connection + resubscribe all symbols
                        try:
                            self.stream.connect()
                            logger.info("WS WATCHDOG: Reconnected successfully")
                            # Resubscribe every symbol — the WS session loses
                            # all subscriptions on disconnect. Without this,
                            # the bot reconnects but receives no market data.
                            for _sym in list(self._conid_cache.keys()):
                                _cid = self._conid_cache[_sym]
                                try:
                                    self.stream.subscribe(_cid)
                                    logger.info(
                                        "WS WATCHDOG: Resubscribed [%s] conid=%s",
                                        _sym, _cid)
                                except Exception as _se:
                                    logger.warning(
                                        "WS WATCHDOG: Resubscribe [%s] failed: %s",
                                        _sym, _se)
                        except Exception as exc:
                            logger.error("WS WATCHDOG: Reconnect failed: %s", exc)

                except Exception as exc:
                    logger.error("WS watchdog iteration error (thread will continue): %s",
                                 exc, exc_info=True)
                    # Do NOT re-raise — let the loop continue on the next interval.
                    # A crash here would kill reconnect capability for the rest of
                    # the session (as seen in Apr-15-2026 where 40 min of bars
                    # were lost because the watchdog died silently).

        self._watchdog_thread = threading.Thread(
            target=_watchdog_loop, daemon=True, name="ws-watchdog")
        self._watchdog_thread.start()

    def _stop_ws_watchdog(self):
        """Stop the WebSocket watchdog thread."""
        if hasattr(self, "_stop_watchdog"):
            self._stop_watchdog.set()
        if hasattr(self, "_watchdog_thread") and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=3)

    def _start_direction_monitor(self, interval: int = 5):
        """
        Fast direction-flip monitor. Runs every `interval` seconds on all open
        positions. If IBKR reports the opposite direction to what the bot expects
        (e.g. internal=SHORT but IBKR=LONG), it means the wide STP or another
        order fired silently. Immediately cancel all live orders for that symbol
        via conid sweep and send a MKT close to flatten.

        This runs much faster than the full reconcile (5s vs 180s) so it catches
        the AMZN/AMD LONG 1500/1000 scenario before multiple stops pile up.
        """
        self._stop_direction_monitor = threading.Event()

        def _loop():
            logger.info("Direction monitor started (every %ds)", interval)
            while not self._stop_direction_monitor.wait(timeout=interval):
                if self._open_positions is None:
                    continue
                try:
                    ibkr_actual = self.get_ibkr_actual_positions()
                except Exception:
                    continue

                with self._pos_lock:
                    positions_snapshot = dict(self._open_positions)

                for sym, pos in positions_snapshot.items():
                    int_dir = pos.get("direction", "")
                    if sym not in ibkr_actual:
                        continue  # handled by full reconcile
                    ibkr_info = ibkr_actual[sym]
                    ibkr_side = ibkr_info.get("side", "")
                    ibkr_qty  = ibkr_info.get("qty", 0)

                    # Detect direction flip
                    flip = ((int_dir == "SHORT" and ibkr_side == "LONG") or
                            (int_dir == "LONG"  and ibkr_side == "SHORT"))
                    if not flip:
                        continue

                    logger.warning(
                        "[%s] DIRECTION FLIP detected — internal=%s IBKR=%s %d. "
                        "Cancelling all orders and flattening immediately.",
                        sym, int_dir, ibkr_side, ibkr_qty)

                    # Step 1: Cancel all tracked orders
                    ids = self._order_ids.get(sym, {})
                    for key in ("stop", "stop_lmt", "stop_price", "wide_stop",
                                "target", "trail_stop", "trail_stop_price",
                                "trail_stop_lmt"):
                        oid = ids.get(key)
                        if oid:
                            try:
                                self.session.cancel_order(self.account_id, oid)
                                logger.info("[%s] Direction monitor cancelled %s oid=%s",
                                            sym, key, oid)
                            except Exception as _ce:
                                logger.debug("[%s] Cancel %s failed: %s", sym, key, _ce)

                    # Step 2: Conid sweep — cancel all live orders for this symbol
                    conid = self._conid_cache.get(sym)
                    if conid:
                        try:
                            _lo = self.session.get_live_orders(self.account_id)
                            for _o in (_lo.get("orders", []) if isinstance(_lo, dict) else []):
                                if str(_o.get("conid", "")) == str(conid):
                                    _oid = str(_o.get("orderId", _o.get("order_id", "")))
                                    if _oid:
                                        try:
                                            self.session.cancel_order(self.account_id, _oid)
                                            logger.info("[%s] Direction monitor sweep "
                                                        "cancelled oid=%s", sym, _oid)
                                        except Exception:
                                            pass
                        except Exception as _se:
                            logger.debug("[%s] Direction monitor sweep failed: %s", sym, _se)

                    # Step 3: Flatten the IBKR position with MKT order
                    close_side = "SELL" if ibkr_side == "LONG" else "BUY"
                    try:
                        self.session.place_order(self.account_id, [{
                            "conid"    : conid,
                            "side"     : close_side,
                            "orderType": "MKT",
                            "quantity" : ibkr_qty,
                            "tif"      : "DAY",
                            "cOID"     : f"{sym}_DIRFLIP_{int(time.time()*1000)}",
                        }])
                        logger.warning("[%s] Direction monitor: MKT %s %d sent to flatten",
                                       sym, close_side, ibkr_qty)
                    except Exception as _me:
                        logger.error("[%s] Direction monitor: MKT flatten failed: %s",
                                     sym, _me)

                    # Step 4: Clear internal state
                    with self._pos_lock:
                        self._open_positions.pop(sym, None)
                    with self._order_ids_lock:
                        self._order_ids.pop(sym, None)
                    self._recently_closed[sym] = time.time()
                    logger.info("[%s] Direction monitor: internal state cleared", sym)

        self._direction_monitor_thread = threading.Thread(
            target=_loop, daemon=True, name="direction-monitor")
        self._direction_monitor_thread.start()

    def _stop_direction_monitor_thread(self):
        if hasattr(self, "_stop_direction_monitor"):
            self._stop_direction_monitor.set()
        if hasattr(self, "_direction_monitor_thread") and                 self._direction_monitor_thread.is_alive():
            self._direction_monitor_thread.join(timeout=3)

    def _start_reconciliation_thread(self, interval: int = 180):
        """Start a background thread that reconciles positions every `interval` seconds."""
        self._stop_reconcile.clear()

        def _loop():
            logger.info("Reconciliation thread started (every %ds)", interval)
            while not self._stop_reconcile.wait(timeout=interval):
                try:
                    self.reconcile_positions()
                except Exception as exc:
                    logger.error("Reconciliation thread error: %s", exc)

        self._reconcile_thread = threading.Thread(
            target=_loop, daemon=True, name="position-reconcile")
        self._reconcile_thread.start()

    def _stop_reconciliation_thread(self):
        """Stop the reconciliation thread."""
        self._stop_reconcile.set()
        if self._reconcile_thread and self._reconcile_thread.is_alive():
            self._reconcile_thread.join(timeout=5)
            logger.info("Reconciliation thread stopped")

    # ── EoD timer thread ─────────────────────────────────────────────────

    def _start_eod_timer_thread(self):
        """
        Start a background thread that fires eod_square_off at EoDExitTime
        AND a safety-net check 5 minutes later.

        Pass 1 (EoDExitTime, e.g. 15:50): Cancel all orders, close all
        actual IBKR positions.

        Pass 2 (EoDExitTime + 5 min, e.g. 15:55): Check again. If ANY
        positions still exist on IBKR (order rejected, API glitch, partial
        fill, etc.), cancel all orders again and force-close everything
        at market price. This is the last safety net before market close.
        """
        self._stop_eod.clear()
        self._eod_fired = False
        self._eod_safety_fired = False

        def _eod_loop():
            logger.info("EoD timer thread started — will fire at EoDExitTime from config.json + safety net 5 min later")

            # Sentinel: date on which flags were last reset. Initialise to
            # None so the first 20:00+ tick always triggers a reset check,
            # and subsequent ticks within the same calendar day are skipped.
            _eod_last_reset_date = None

            while not self._stop_eod.is_set():
                try:
                    # Re-read config on every check so live edits work
                    cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
                    with open(cfg_path) as fh:
                        cfg = json.load(fh)

                    eod_str = cfg.get("EoDExitTime", "15:50")
                    eod_h, eod_m = map(int, eod_str.split(":"))
                    eod_time = dtime(eod_h, eod_m)

                    # ISSUE A FIX: Hard-clamp EoDExitTime to [13:00, 16:00] EST.
                    # A wrong value (e.g. "05:50" AM or UTC "19:50") would fire EoD
                    # before market opens or miss it entirely — close positions at the
                    # wrong time. Clamp to safe default "15:50" and log ERROR.
                    if not (dtime(13, 0) <= eod_time <= dtime(16, 0)):
                        logger.error(
                            "EoD Timer: EoDExitTime=%s is OUTSIDE valid range 13:00-16:00 EST. "
                            "Clamping to safe default 15:50. Fix config.json immediately.",
                            eod_str)
                        eod_h, eod_m = 15, 50
                        eod_time = dtime(15, 50)

                    # Safety net time = EoDExitTime + 5 minutes
                    safety_m = eod_m + 5
                    safety_h = eod_h
                    if safety_m >= 60:
                        safety_m -= 60
                        safety_h += 1
                    safety_time = dtime(safety_h, safety_m)

                    now_est = datetime.now(EST).time()

                    # ── Pass 1: Primary EoD square-off ────────────────────
                    if now_est >= eod_time and not self._eod_fired:
                        self._eod_fired = True

                        internal_count = len(self._open_positions) if self._open_positions else 0
                        ibkr_actual = self.get_ibkr_actual_positions()

                        logger.info(
                            "EoD Timer PASS 1 fired at %s EST — "
                            "internal positions: %d, IBKR actual positions: %d",
                            now_est.strftime("%H:%M:%S"), internal_count, len(ibkr_actual))

                        self.eod_square_off(self._open_positions or {})

                        # Clear internal positions after EoD
                        if self._open_positions is not None:
                            symbols_cleared = list(self._open_positions.keys())
                            self._open_positions.clear()
                            if symbols_cleared:
                                logger.info("EoD Timer: cleared %d internal positions: %s",
                                            len(symbols_cleared), symbols_cleared)
                            else:
                                logger.info("EoD Timer: no internal positions to clear")
                        logger.info("EoD Timer PASS 1 complete")

                    # ── Pass 2: Safety net — catch anything left ──────────
                    if now_est >= safety_time and not self._eod_safety_fired:
                        self._eod_safety_fired = True

                        # FIX (March-31 cancel-loop): Wait 60 seconds before
                        # acting.  Pass 1 places close orders as PreSubmitted;
                        # they need time to route and fill.  Firing immediately
                        # caused the safety net to cancel in-flight close orders,
                        # creating an infinite cancel-resubmit loop.
                        logger.info(
                            "EoD SAFETY NET check at %s EST — "
                            "waiting 60s for in-flight orders to fill before acting...",
                            now_est.strftime("%H:%M:%S"))
                        self._stop_eod.wait(timeout=60)

                        ibkr_actual = self.get_ibkr_actual_positions()

                        if ibkr_actual:
                            logger.warning(
                                "EoD SAFETY NET fired at %s EST — "
                                "%d position(s) STILL OPEN on IBKR: %s — "
                                "force-closing everything!",
                                now_est.strftime("%H:%M:%S"),
                                len(ibkr_actual), list(ibkr_actual.keys()))

                            # FIX: Pass skip_eod_close=True so we don't cancel
                            # EOD_CLOSE orders that are already in-flight.
                            self.eod_square_off(self._open_positions or {}, skip_eod_close=True)

                            # Clear internal state again
                            if self._open_positions is not None:
                                self._open_positions.clear()

                            logger.info("EoD SAFETY NET complete — all positions should be flat")
                        else:
                            logger.info(
                                "EoD SAFETY NET check at %s EST — "
                                "IBKR confirms FLAT. No action needed.",
                                now_est.strftime("%H:%M:%S"))

                    # Reset both flags well after market close so the EoD can
                    # fire again the next day if this process stays alive.
                    #
                    # BUG FIXED (10-Apr-2026): The old code used `if now_est >= reset_time`
                    # which fires on EVERY 10-second tick from 20:00 onwards forever.
                    # After resetting the flags, the next tick still sees
                    # `now_est >= eod_time` (15:50) as True, so EoD re-fires
                    # immediately and the square-off loop repeats all night.
                    # The log showed hundreds of "daily flags reset" lines cycling
                    # every ~70 seconds from midnight until 04:15 AM.
                    #
                    # FIX: Track the calendar date when we last reset so we only
                    # reset once per day, not on every loop tick.
                    reset_time = dtime(20, 0)
                    today_date = datetime.now(EST).date()
                    if now_est >= reset_time and _eod_last_reset_date != today_date:
                        _eod_last_reset_date = today_date
                        if self._eod_fired or self._eod_safety_fired:
                            logger.info("EoD Timer: daily flags reset at %s EST — "
                                        "ready for next session", now_est.strftime("%H:%M:%S"))
                        self._eod_fired = False
                        self._eod_safety_fired = False

                except Exception as exc:
                    logger.error("EoD timer thread error: %s", exc)

                # Check every 10 seconds
                self._stop_eod.wait(timeout=10)

        self._eod_thread = threading.Thread(
            target=_eod_loop, daemon=True, name="eod-timer")
        self._eod_thread.start()

    def _stop_eod_timer_thread(self):
        """Stop the EoD timer thread."""
        self._stop_eod.set()
        if self._eod_thread and self._eod_thread.is_alive():
            self._eod_thread.join(timeout=5)
            logger.info("EoD timer thread stopped")


# =============================================================================
# STOCK LIST  —  reads Stocks + Indexes from config.json on every bar
# =============================================================================

class StockList:
    """
    Reads the "Stocks" and "Indexes" sections from config.json live.
    All methods re-read the file on every call so mid-session edits
    take effect on the very next bar — no restart needed.

    Stocks config.json format:
        "Stocks": {
            "AAPL": {
                "enabled":      "Y",        Y=trade, N=monitor only
                "quantity":     100,        shares per trade
                "index_filter": "Y",        Y=require index MACD agreement
                "index":        "SPY",      which index to compare against
                "macd_fast":    12,         per-stock MACD overrides (optional)
                "macd_slow":    26,
                "macd_signal_period": 9,
                "macd_resolution":    "30s",
                "macd_price_source":  "close"
            }
        }

    Indexes config.json format:
        "Indexes": {
            "SPY": {
                "enabled": "Y",             Y=subscribe + calculate MACD
                "macd_resolution": "30s",   per-index MACD overrides (optional)
                ...
            }
        }

    Rules:
      - Symbol lookup is NOT case-sensitive
      - enabled="Y"  → signal evaluated and orders sent
      - enabled="N"  → MACD calculated, no orders placed
      - index_filter="Y" → signal only fires if named index MACD agrees
      - index_filter="N" or missing → signal independent of any index
      - Indexes are NEVER traded — reference instruments only
    """

    CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

    @classmethod
    def _read_cfg(cls) -> Dict:
        try:
            with open(cls.CONFIG_PATH) as fh:
                return json.load(fh)
        except Exception as exc:
            logger.error("StockList: config read error — %s", exc)
            return {}

    @classmethod
    def _read(cls) -> Dict:
        """Raw Stocks dict with keys normalised to UPPERCASE."""
        raw = cls._read_cfg().get("Stocks", {})
        return {k.upper(): v for k, v in raw.items()}

    @classmethod
    def _read_indexes(cls) -> Dict:
        """Raw Indexes dict with keys normalised to UPPERCASE."""
        raw = cls._read_cfg().get("Indexes", {})
        return {k.upper(): v for k, v in raw.items()}

    @classmethod
    def _fallback_qty(cls) -> int:
        try:
            return int(cls._read_cfg().get("PositionSize", 100))
        except Exception:
            return 100

    # ── Stock queries ──────────────────────────────────────────────────────

    @classmethod
    def is_enabled(cls, symbol: str) -> bool:
        """True if this stock has enabled=Y. Re-read from file every call."""
        entry = cls._read().get(symbol.upper())
        if entry is None:
            return False
        return str(entry.get("enabled", "N")).strip().upper() == "Y"

    @classmethod
    def get_quantity(cls, symbol: str) -> int:
        """Per-stock quantity, falling back to PositionSize."""
        entry = cls._read().get(symbol.upper())
        if entry and "quantity" in entry:
            try:
                return int(entry["quantity"])
            except (ValueError, TypeError):
                pass
        return cls._fallback_qty()

    @classmethod
    def get_index_filter(cls, symbol: str) -> tuple:
        """
        Returns (use_filter: bool, index_symbol: str).
        use_filter=True means this stock requires index MACD alignment.
        index_symbol is the key to look up in IndexMACDStore (uppercase).
        """
        entry = cls._read().get(symbol.upper())
        if entry is None:
            return False, ""
        use_filter = str(entry.get("index_filter", "N")).strip().upper() == "Y"
        index_sym  = str(entry.get("index", "")).strip().upper()
        return use_filter, index_sym

    @classmethod
    def all_symbols(cls) -> List[str]:
        """All stock symbols defined in Stocks section (uppercase)."""
        return list(cls._read().keys())

    # ── Index queries ──────────────────────────────────────────────────────

    @classmethod
    def all_index_symbols(cls) -> List[str]:
        """All index symbols in the Indexes section where enabled=Y (uppercase)."""
        indexes = cls._read_indexes()
        return [
            sym for sym, entry in indexes.items()
            if str(entry.get("enabled", "N")).strip().upper() == "Y"
        ]

    # ── Logging ───────────────────────────────────────────────────────────

    @classmethod
    def log_status(cls):
        """Log full stock + index list at startup."""
        indexes = cls._read_indexes()
        stocks  = cls._read()

        logger.info("─── Index List ──────────────────────────────────────────")
        if indexes:
            for sym, entry in sorted(indexes.items()):
                enabled = str(entry.get("enabled", "N")).strip().upper()
                res     = entry.get("macd_resolution", "global default")
                logger.info("  %-8s  res=%-6s  %s",
                            sym, res, "ACTIVE" if enabled == "Y" else "disabled")
        else:
            logger.info("  (no indexes defined)")

        logger.info("─── Stock List ──────────────────────────────────────────")
        if stocks:
            for sym, entry in sorted(stocks.items()):
                enabled   = str(entry.get("enabled", "N")).strip().upper()
                qty       = entry.get("quantity", cls._fallback_qty())
                use_idx   = str(entry.get("index_filter", "N")).strip().upper() == "Y"
                idx_name  = entry.get("index", "")
                res       = entry.get("macd_resolution", "global default")
                idx_str   = f"  index_filter={idx_name}" if use_idx else "  no index filter"
                status    = "TRADE ENABLED" if enabled == "Y" else "disabled"
                logger.info("  %-8s  qty=%-5s  res=%-6s  %-14s  %s",
                            sym, qty, res, idx_str, status)
        else:
            logger.warning("  (no stocks defined — nothing to trade)")

        logger.info("─────────────────────────────────────────────────────────")


# =============================================================================
# STRATEGY LOOP  —  wires IBKRBroker + entry_logic + exit_logic + StockList
# =============================================================================

def run_strategy(broker: IBKRBroker):
    """
    Strategy event loop.

    STARTUP SEQUENCE
    ─────────────────
    1. Subscribe all active INDEXES first (enabled=Y in Indexes section).
       Each index gets its own IndicatorPipeline seeded from prior session.
       Index pipelines update IndexMACDStore on every ready bar — they
       never fire the on_bar trading callback.

    2. Subscribe all STOCKS (enabled=Y or N alike, so MACD is always
       warm and ready the moment a stock is re-enabled mid-session).

    PER-BAR FLOW
    ─────────────
    1. Bar arrives enriched with MACD fields (IndicatorPipeline handles it).
       Index bars silently update IndexMACDStore and stop here.

    2. Stock bar: if a position is open → run exit logic regardless of
       enabled flag.  Open positions are always managed.

    3. No open position + enabled=Y:
       a. Call entry_logic.check_entry_signal() for stock MACD conditions.
       b. If signal returned → apply index correlation gate if index_filter=Y:
              LONG  requires INDEX_MACD[0] > INDEX_MACDSignal[0]
              SHORT requires INDEX_MACD[0] < INDEX_MACDSignal[0]
          Turn ON/OFF per stock: set index_filter=Y or N in config.json.
       c. If all checks pass → submit bracket order.

    4. No open position + enabled=N → skip silently.

    ALL FIELDS LIVE-RELOADED (no restart needed):
    ─────────────────────────────────────────────
    enabled, quantity, index_filter, index      → next bar
    IndexCorrelation                            → next bar (global ON/OFF)
    MeanMultiplier_L, MeanMultiplier_S          → next bar
    macd_fast, macd_slow, macd_resolution       → auto-reseed (~3 sec)
                                                   (detected by IndicatorPipeline._check_param_change
                                                    on the next bar close — no restart needed)
    MACD_SecBarSize                              → auto-reseed (linked to macd_resolution)
    """
    from entry_logic import check_entry_signal
    from exit_logic  import process_exit

    StockList.log_status()

    open_positions:     Dict[str, Dict] = {}
    _last_enabled_log:  Dict[str, bool] = {}   # to suppress repeat log lines
    # Symbols currently mid-entry — guards against duplicate signal on next bar.
    _currently_entering: set = set()
    _entering_lock = threading.Lock()   # protects _currently_entering set
    # Queue for batch entry: on_bar pushes signal dicts here.
    # batch_entry_thread drains it every 1s and submits all in 2 HTTP calls.
    import queue as _queue_mod
    _pending_signals: _queue_mod.Queue = _queue_mod.Queue()

    # Share the open_positions dict with the broker so the
    # reconciliation thread can update it when IBKR fills arrive.
    broker._open_positions = open_positions

    # ── FIX (Apr-2026): Startup position audit ────────────────────────────
    # Query IBKR at launch for any positions inherited from a prior session.
    # Without this, stale shorts/longs silently block all new entry signals.
    logger.info("=== STARTUP POSITION AUDIT ===")
    try:
        _startup_pos = broker.get_ibkr_actual_positions()
        if not _startup_pos:
            logger.info("Startup audit: IBKR is FLAT — no inherited positions")
        else:
            logger.warning(
                "Startup audit: %d inherited position(s) found: %s — "
                "adopting into internal state. Review manually if unexpected.",
                len(_startup_pos),
                {s: f"{v['side']} {v['qty']}" for s, v in _startup_pos.items()})
            for _sym, _pi in _startup_pos.items():
                if _sym not in open_positions:
                    open_positions[_sym] = {
                        "symbol"          : _sym,
                        "direction"       : _pi["side"],
                        "entry_price"     : 0,
                        "stop_price"      : 0,
                        "stop_limit_price": 0,
                        "target_price"    : 0,
                        "be_activated"    : False,
                        "max_high"        : 0,
                        "min_low"         : 0,
                        "high_history"    : [],
                        "low_history"     : [],
                        "quantity"        : _pi["qty"],
                    }
                    logger.warning(
                        "Startup audit: [%s] adopted as %s %d — "
                        "EoD will close if not managed before exit time.",
                        _sym, _pi["side"], _pi["qty"])
    except Exception as _exc:
        logger.error("Startup audit failed: %s — proceeding blind", _exc)

    logger.info("=== STARTUP AUDIT COMPLETE ===")
    # ─────────────────────────────────────────────────────────────────────

    def on_bar(symbol: str, bar: Dict):
        nonlocal open_positions, _last_enabled_log

        bar["symbol"] = symbol

        # ── Manage open positions — always, regardless of enabled flag ─────
        if symbol in open_positions:
            pos = open_positions[symbol]

            # ── Step 1: EoD check via process_exit ────────────────────────
            # GUARD: if adopted position has stop_price=0.0, skip process_exit
            # to prevent immediate false STOP_HIT (any close >= 0 for SHORT).
            # These positions are managed by reconcile direction checks only.
            if pos.get("stop_price", 0.0) == 0.0 and pos.get("entry_price", 0.0) == 0.0:
                logger.info("[%s] Adopted position (entry=0, stop=0) — skipping exit logic "
                            "until prices are known.", symbol)
                return
            action = process_exit(pos, bar)

            if action["action"] == "CLOSE_MARKET":
                broker.eod_square_off(open_positions)
                open_positions.clear()
                logger.info("EoD square-off complete — all positions cleared")
                return

            # ── Step 2: Stop management — compute trailing stop update ────
            # compute_stop_update() reads config, applies breakeven/trail
            # logic, and returns an UPDATE_STOPS action if a move is due.
            # It handles all three phases:
            #   Phase 1 — Initial: no movement, original bracket stops protect
            #   Phase 2 — Breakeven: fires once when price > entry + BEM*MPL
            #                        cancels bracket stops, places new stack
            #   Phase 3 — Trailing: 5-bar or % method, stops only move in
            #                       profitable direction
            stop_action = broker.compute_stop_update(symbol, pos, bar)
            if stop_action:
                broker.apply_exit_action(symbol, stop_action)

            # ── Step 3: process_exit STOP_HIT / CLOSE_LIMIT detection ─────
            # process_exit() monitors the bar close vs current stop_price.
            # If stop is hit internally, apply_exit_action(STOP_HIT) cancels
            # all live orders and clears state.
            if action["action"] in ("STOP_HIT", "CLOSE_LIMIT"):
                broker.apply_exit_action(symbol, action)
                open_positions.pop(symbol, None)
                broker._recently_closed[symbol] = time.time()
                try:
                    import json as _json2
                    _rc_path2 = os.path.join(os.path.dirname(__file__), ".recently_closed.json")
                    open(_rc_path2, 'w').write(_json2.dumps(broker._recently_closed))
                except Exception:
                    pass
                logger.info("[%s] Position closed via %s", symbol, action["action"])
            return

        # ── Check enabled flag (live from config.json) ────────────────────
        enabled = StockList.is_enabled(symbol)
        if _last_enabled_log.get(symbol) != enabled:
            if enabled:
                logger.info("[%s] Trading ENABLED  qty=%d — acting on signals",
                            symbol, StockList.get_quantity(symbol))
            else:
                logger.info("[%s] Trading DISABLED — monitoring only", symbol)
            _last_enabled_log[symbol] = enabled

        if not enabled:
            return

        # ── Entry signal ──────────────────────────────────────────────────
        pos_map = {sym: True for sym in open_positions}
        with _entering_lock:
            if symbol in _currently_entering:
                return   # mid-entry — skip until open_positions updated
        signal  = check_entry_signal(bar, pos_map)
        if signal["signal"] == "NONE":
            return

        direction = signal["signal"]   # "LONG" or "SHORT"

        # ── Index Correlation gate (per-stock switch) ──────────────────────
        # Each stock controls this individually via:
        #   index_filter = Y/N   (the ON/OFF switch for this stock)
        #   index        = "SPY" / "QQQ" etc. (which index to compare)
        #
        # Condition:
        #   LONG  -> INDEX_MACD[0] > INDEX_MACDSignal[0]
        #   SHORT -> INDEX_MACD[0] < INDEX_MACDSignal[0]
        #
        # To turn OFF for a stock: set index_filter=N in config.json or Excel.
        use_filter, index_sym = StockList.get_index_filter(symbol)

        if use_filter:
            if not index_sym:
                logger.warning("[%s] index_filter=Y but no index assigned"
                               " — blocking signal", symbol)
                return

            idx_fields = IndexMACDStore.get(index_sym)
            if idx_fields is None:
                logger.info("[%s] Signal BLOCKED — %s MACD not ready yet",
                            symbol, index_sym)
                return

            idx_macd   = idx_fields["macd"][0]
            idx_signal = idx_fields["macd_signal"][0]

            if direction == "LONG":
                idx_aligned = idx_macd > idx_signal
            else:  # SHORT
                idx_aligned = idx_macd < idx_signal

            if not idx_aligned:
                logger.info(
                    "[%s] Signal BLOCKED by index filter — "
                    "%s MACD(%.5f) vs Signal(%.5f) not aligned for %s",
                    symbol, index_sym, idx_macd, idx_signal, direction,
                )
                return

            logger.info(
                "[%s] Index filter PASSED — "
                "%s MACD(%.5f) vs Signal(%.5f) aligned for %s",
                symbol, index_sym, idx_macd, idx_signal, direction,
            )

        # ── Submit order ──────────────────────────────────────────────────
        qty = StockList.get_quantity(symbol)
        if signal.get("orders"):
            signal["orders"]["quantity"] = qty

        # ── Safety: verify no position already exists on IBKR ─────────
        # This catches cases where IBKR filled a stop/target but the
        # reconciliation thread hasn't synced yet, or the position was
        # opened externally.
        #
        # FIX (Apr-2026): Previously, an existing IBKR position was re-adopted
        # into open_positions with entry_price=0 and stop_price=0. This caused
        # process_exit to immediately fire STOP_HIT (stop=0 means any bar close
        # is "above" it for a short), which cleared the position from state again
        # without sending any close order to IBKR. The orphan sat unmanaged and
        # was eventually doubled on the next signal.
        #
        # New behaviour:
        #   If IBKR has a position we do NOT know about (not in open_positions)
        #   → send a market close immediately to flatten it.
        #   If IBKR has a position we DO know about (already in open_positions)
        #   → it is being managed normally, just block the new entry signal.
        try:
            ibkr_actual = broker.get_ibkr_actual_positions()
            if symbol in ibkr_actual:
                existing = ibkr_actual[symbol]
                if symbol in open_positions:
                    # Already tracked — just block the new entry
                    logger.warning(
                        "[%s] ENTRY BLOCKED — IBKR shows existing %s position of %d shares "
                        "(already tracked internally).",
                        symbol, existing["side"], existing["qty"])
                else:
                    # ORPHAN — bot has no record of this position.
                    # Send market close immediately to prevent doubling.
                    conid_sym = broker._conid_cache.get(symbol)
                    if conid_sym:
                        close_side = "SELL" if existing["side"] == "LONG" else "BUY"
                        unique_coid = f"{symbol}_ORPHAN_CLOSE_{int(time.time() * 1000)}"
                        ticket = {
                            "conid"    : conid_sym,
                            "side"     : close_side,
                            "orderType": "MKT",
                            "quantity" : existing["qty"],
                            "tif"      : "DAY",
                            "cOID"     : unique_coid,
                        }
                        try:
                            resp = broker.session.place_order(broker.account_id, [ticket])
                            logger.warning(
                                "[%s] ENTRY BLOCKED — ORPHAN position %s %d found on IBKR "
                                "with no internal record. Sending market CLOSE to flatten. "
                                "resp=%s",
                                symbol, existing["side"], existing["qty"], resp)
                        except Exception as _exc:
                            logger.error(
                                "[%s] ENTRY BLOCKED — Failed to close orphan position: %s",
                                symbol, _exc)
                    else:
                        logger.warning(
                            "[%s] ENTRY BLOCKED — ORPHAN %s %d on IBKR, no conid cached "
                            "— cannot auto-close. Close manually.",
                            symbol, existing["side"], existing["qty"])
                return
        except Exception as exc:
            logger.warning("[%s] Could not verify IBKR positions: %s — proceeding with entry", symbol, exc)

        logger.info("[%s] *** %s SIGNAL ***  qty=%d  close=%.4f",
                    symbol, direction, qty, bar["close"])

        # ── Push signal to batch queue (NOT per-stock thread) ───────────────
        # The batch_entry_thread collects ALL signals that arrive within a
        # 1-second window (one 30s bar burst) and submits them ALL in 2 HTTP
        # calls instead of N×2 calls. This eliminates CP Gateway congestion
        # that was causing STP drops and STPLMT failures when 13 stocks
        # signal simultaneously (seen Apr-15-2026: all 13 SHORT at 12:22).
        with _entering_lock:
            _currently_entering.add(symbol)
        logger.info("[%s] *** %s SIGNAL ***  qty=%d  close=%.4f",
                    symbol, direction, qty, bar["close"])

        # Build the price levels and refs here (on WS thread, fast — pure math)
        try:
            _cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
            with open(_cfg_path) as _fh:
                _cfg = json.load(_fh)
        except Exception:
            _cfg = {}
        _mpl      = float(_cfg.get("MPL_dollar_Amount", 0.60))
        _slm      = float(_cfg.get("SLMultiplier",      1.0))
        _sls      = float(_cfg.get("StopLimitSpread",   0.03))
        _tgt_mult = float(_cfg.get("TargetMultiplier",  3.0))
        _bc       = bar["close"]
        _conid    = broker.resolve_conid(symbol)
        _ts       = f"{int(time.time())}_{symbol}"
        _cside    = "SELL" if direction == "LONG" else "BUY"
        _tside    = "SELL" if direction == "LONG" else "BUY"
        if direction == "LONG":
            _sp  = round(_bc - _mpl,             4)
            _slt = round(_bc - _mpl * _slm,      4)
            _slp = round(_bc - _mpl*_slm - _sls, 4)
            _tp  = round(_bc + _mpl * _tgt_mult, 4)
            _tside = "SELL"
        else:
            _sp  = round(_bc + _mpl,             4)
            _slt = round(_bc + _mpl * _slm,      4)
            _slp = round(_bc + _mpl*_slm + _sls, 4)
            _tp  = round(_bc - _mpl * _tgt_mult, 4)
            _tside = "BUY"

        _pending_signals.put({
            "symbol"          : symbol,
            "signal"          : signal,
            "direction"       : direction,
            "qty"             : qty,
            "bar_close"       : _bc,
            "conid"           : _conid,
            "close_side"      : _cside,
            "target_side"     : _tside,
            "stop_price"      : _sp,         # reference only — actual STP placed after fill at fill_px ± MPL
            "stop_lmt_trigger": _slt,        # reference only — BE/trail uses fill_px based levels
            "stop_lmt_price"  : _slp,        # reference only — BE/trail uses fill_px based levels
            "target_price"    : _tp,
            "mpl"             : _mpl,
            "tgt_mult"        : _tgt_mult,
            "parent_ref"      : f"PARENT_{_ts}",
            "wide_stop_ref"   : f"WIDE_STP_{_ts}",
            "stop_ref"        : f"STOP_{_ts}",
            "stoplmt_ref"     : f"STPLMT_{_ts}",  # used as ref ID in BE phase
            "target_ref"      : f"TARGET_{_ts}",
        })

    # ── Batch entry thread ───────────────────────────────────────────────────
    # Collects all signals queued by on_bar within a 1-second window,
    # then submits them ALL in exactly 2 HTTP calls (batch_submit_entries).
    # This replaces 13 × 2 = 26 sequential HTTP calls with just 2 calls,
    # eliminating CP Gateway congestion and STP drop issues.
    # ── Gateway concurrency semaphore ────────────────────────────────────────
    # Controls how many stocks submit orders simultaneously.
    # CP Gateway returns 400 Bad Request when multiple different conids hit
    # the /orders endpoint at the same time (confirmed Apr-16-2026).
    # A semaphore of 3 allows 3 stocks in parallel — fast enough while safe.
    #
    # This value is intentionally a named constant so future tuning is trivial:
    #   - Set to 1 → fully sequential (safest, slowest)
    #   - Set to 3 → 3 parallel (current setting, ~5s for 13 stocks)
    #   - Set to 5 → 5 parallel (faster, test before using with many stocks)
    # No other code needs to change when this number changes.
    # ── Entry system: concurrent bracket placement + single verify thread ───────
    #
    # The CP Gateway returns 400 only when multiple DIFFERENT conids are bundled
    # in ONE JSON body. Separate HTTP calls each with ONE conid arriving
    # simultaneously are processed independently — no 400 error.
    #
    # PLACEMENT_CONCURRENCY = N means N brackets are placed simultaneously.
    # Each runs in its own thread, each HTTP call has exactly 1 conid.
    #
    #   N=1  → sequential,   30 stocks = 30 × 150ms = 4.5s
    #   N=5  → 5 at a time,  30 stocks =  6 × 150ms = 0.9s
    #   N=10 → 10 at a time, 30 stocks =  3 × 150ms = 0.45s
    #   N=30 → all at once,  30 stocks =  1 × 150ms = 0.15s
    #
    # Start at 5, test on paper, increase if no 429s.
    # Only this one number needs to change — nothing else.
    #
    # Phase 1: _entry_loop spawns one thread per stock. Each thread acquires
    #   _placement_sem (max N slots) then places its bracket and returns.
    # Phase 2: _verify_loop polls all pending fills simultaneously every 2s.
    #   Places target after fill confirmed using actual fill price.

    PLACEMENT_CONCURRENCY = 1          # Sequential — prevents CP Gateway overload on simultaneous signals
    _placement_sem = threading.Semaphore(PLACEMENT_CONCURRENCY)

    _pending_fills: list = []          # [(p, parent_oid, stop_oid, stoplmt_oid), ...]
    _pending_lock  = threading.Lock()  # protects _pending_fills list
    _deadlines: dict = {}              # sym → deadline timestamp

    def _entry_loop():
        """
        Reads _pending_signals and spawns one lightweight thread per stock.
        Each thread acquires _placement_sem before placing its bracket —
        at most PLACEMENT_CONCURRENCY brackets in-flight simultaneously.
        Never waits for fills — that is the verify thread's job.
        """
        logger.info("Entry loop started — concurrent placement "
                    "(max %d simultaneous)", PLACEMENT_CONCURRENCY)
        while True:
            p = _pending_signals.get()
            threading.Thread(
                target=_place_bracket, args=(p,),
                daemon=True, name=f"bracket-{p['symbol']}",
            ).start()

    def _place_bracket(p: dict):
        """
        Bracket placement in two phases:

        Phase 1 (semaphore held — ~150ms):
          HTTP POST to CP Gateway: parent LMT + STP Market.
          Parse initial OID response.
          Semaphore released here — next stock can start immediately.

        Phase 2 (semaphore FREE — up to 2.5s):
          Recovery retries for missing OIDs (encrypt_message lag).
          Register OIDs, hand off to verify thread.

        Before this fix all 2.5s of recovery ran inside the semaphore slot,
        blocking other stocks and causing HTTP read timeouts on the last batch
        when all 13 stocks signalled simultaneously (seen Apr-17-2026: TSM timed out).
        """
        sym = p["symbol"]
        _placement_sem.acquire()
        try:
            resp1, ref_to_oid = _gateway_post(p)   # Phase 1 — fast HTTP only
        finally:
            _placement_sem.release()               # release BEFORE recovery

        if resp1 is None:
            return   # bracket failed — already logged and guard released in _gateway_post
        _recover_and_register(p, resp1, ref_to_oid)   # Phase 2 — recovery outside slot

    def _gateway_post(p: dict):
        """
        Phase 1: send the 2-ticket bracket to CP Gateway and parse the raw response.
        Returns (resp1, ref_to_oid) on success, (None, {}) on failure.
        Called while the placement semaphore is held — must be fast (no sleeps).
        """
        sym  = p["symbol"]
        side = "BUY" if p["direction"] == "LONG" else "SELL"

        # Bracket 1: Parent LMT + Wide STP (2 orders, parentId links them)
        # If parent unfilled → cancel parent → IBKR auto-cancels Wide STP.
        close_side = "BUY" if p["direction"] == "SHORT" else "SELL"
        _mpl = p.get("mpl", 0.60)
        _bc  = p["bar_close"]
        _wide_px = round(_bc + _mpl * 1.1, 4) if p["direction"] == "SHORT"                    else round(_bc - _mpl * 1.1, 4)

        call1 = [
            {
                "conid"    : p["conid"],
                "side"     : side,
                "orderType": "LMT",
                "price"    : _bc,
                "quantity" : p["qty"],
                "tif"      : "DAY",
                "cOID"     : p["parent_ref"],
                "order_ref": p["parent_ref"],
            },
            {
                "conid"    : p["conid"],
                "side"     : close_side,
                "orderType": "STP",
                "price"    : _wide_px,
                "quantity" : p["qty"],
                "tif"      : "DAY",
                "cOID"     : p["wide_stop_ref"],
                "order_ref": p["wide_stop_ref"],
                "parentId" : p["parent_ref"],
            },
        ]
        try:
            resp1 = broker.session.place_order(broker.account_id, call1)
            logger.info("[%s] Bracket placed: %s", sym, resp1)
        except Exception as exc:
            logger.error("[%s] Bracket failed: %s — checking if IBKR received it anyway",
                         sym, exc)
            # CP Gateway can time out while still processing the order.
            # Wait 2 seconds then check if IBKR opened a position for this symbol.
            time.sleep(2)
            try:
                _actual = broker.get_ibkr_actual_positions()
                if sym in _actual:
                    logger.warning(
                        "[%s] Bracket timed out but IBKR DOES hold %s — "
                        "adding to recently_closed so reconcile skips it, "
                        "then registering for emergency stop placement.",
                        sym, _actual[sym]["side"])
                    # Hand off to verify loop via pending_fills with bar_close as fill price
                    import time as _time
                    _deadline = _time.time() + 65
                    with _pending_lock:
                        _pending_fills.append((p, None, None, None))
                        _deadlines[sym] = _deadline
                    with _entering_lock:
                        _currently_entering.discard(sym)
                    return None, {}
                else:
                    logger.error("[%s] Bracket failed and IBKR is FLAT — order not placed.",
                                 sym)
            except Exception as chk:
                logger.error("[%s] Post-timeout position check failed: %s", sym, chk)
            with _entering_lock:
                _currently_entering.discard(sym)
            return None, {}

        # ── Parse OIDs from initial response (fast — no sleeps) ──────────
        ref_to_oid = {}
        for item in (resp1 if isinstance(resp1, list) else []):
            if not isinstance(item, dict):
                continue
            # Fat-finger confirm FIRST — before oid check
            if "id" in item and "message" in item:
                try:
                    # Capture confirm response — IBKR may return child OIDs here
                    _confirm_resp = broker.session.reply_confirm(item["id"])
                    # Parse child OIDs from confirm response
                    for _cr in (_confirm_resp if isinstance(_confirm_resp, list)
                                else [_confirm_resp] if isinstance(_confirm_resp, dict)
                                else []):
                        if not isinstance(_cr, dict):
                            continue
                        _cr_oid = str(_cr.get("order_id", ""))
                        _cr_ref = (_cr.get("order_ref","") or
                                   _cr.get("local_order_id",""))
                        if _cr_oid and _cr_oid not in ("-1","0") and _cr_ref:
                            ref_to_oid[_cr_ref] = _cr_oid
                            logger.info("[%s] Child OID from confirm: %s → %s",
                                        sym, _cr_ref, _cr_oid)
                except Exception:
                    pass
            oid = str(item.get("order_id",""))
            if not oid or oid in ("-1","0"):
                continue
            ref = item.get("order_ref","") or item.get("local_order_id","")
            if ref:
                ref_to_oid[ref] = oid

        return resp1, ref_to_oid
        # ── End of Phase 1 — semaphore released by caller ────────────────

    def _recover_and_register(p: dict, resp1, ref_to_oid: dict):
        """
        Phase 2: recovery retries + OID registration + verify hand-off.
        Runs AFTER the placement semaphore is released — sleeps here are safe.
        """
        sym = p["symbol"]

        # ── Recovery: find missing OIDs via live orders (retry with backoff) ──
        #
        # B44 ROOT CAUSE: IBKR's encrypt_message:1 withholds the STP OID from
        # the initial response. The STP IS on IBKR — it just isn't returned.
        # A single 300ms query is too fast — IBKR hasn't indexed the new order yet.
        # Fix: retry up to 3 times with increasing delays (0.3s → 0.7s → 1.5s).
        # Total max wait: ~2.5s — still well within the fill polling window.
        # On April 16 every single STP was missing. With retries, later attempts
        # reliably find the order once IBKR's internal index catches up.
        # Recover parent OID + wide_stop OID at placement time.
        # Wide STP is a bracket child — IBKR may return its OID in the
        # reply_confirm response above. If not, retry via live orders.
        # Longer delays needed: 13 stocks fire simultaneously → gateway lag.
        all_refs = {p["parent_ref"], p["wide_stop_ref"]}
        missing  = all_refs - set(ref_to_oid.keys())
        if missing:
            _retry_delays = [0.5, 1.0, 2.0]  # wider gaps for 13-stock burst
            for _attempt, _delay in enumerate(_retry_delays, start=1):
                if not missing:
                    break
                try:
                    time.sleep(_delay)
                    live = broker.session.get_live_orders(broker.account_id)
                    orders = (live.get("orders",[]) if isinstance(live,dict)
                              else live if isinstance(live,list) else [])
                    for o in orders:
                        if not isinstance(o, dict):
                            continue
                        o_ref = (o.get("order_ref","") or o.get("orderRef","")
                                 or o.get("local_order_id",""))
                        o_id  = str(o.get("orderId", o.get("order_id","")))
                        if not o_id or o_id in ("0",""):
                            continue
                        if o_ref and o_ref in missing:
                            ref_to_oid[o_ref] = o_id
                            missing.discard(o_ref)
                    if missing:
                        logger.warning("[%s] Parent OID recovery attempt %d/%d",
                                       sym, _attempt, len(_retry_delays))
                    else:
                        logger.info("[%s] Parent OID recovered on attempt %d", sym, _attempt)
                except Exception as exc:
                    logger.warning("[%s] OID recovery attempt %d failed: %s", sym, _attempt, exc)
            if missing:
                logger.error("[%s] Parent OID recovery FAILED — bracket orphaned", sym)

        parent_oid   = ref_to_oid.get(p["parent_ref"])
        wide_stp_oid = None   # fetched from live orders after fill

        logger.info("[%s] OIDs: parent=%s", sym, parent_oid or "MISSING")

        if not parent_oid:
            logger.error("[%s] No parent OID — cannot monitor fill", sym)
            with _entering_lock:
                _currently_entering.discard(sym)
            return

        # ── Phase 1b: Wide STP as separate POST ──────────────────────────────
        # Separate POST → IBKR returns OID directly (no encrypt_message lag).
        _close_side_reg = "BUY" if p["direction"] == "SHORT" else "SELL"
        _mpl_ws = p.get("mpl", 0.60)
        _wide_px = round(
            p["bar_close"] + _mpl_ws * 1.1 if p["direction"] == "SHORT"
            else p["bar_close"] - _mpl_ws * 1.1, 4)
        wide_stp_oid = None
        for _ws_attempt in range(3):
            try:
                _ws_resp = broker.session.place_order(broker.account_id, [{
                    "conid"    : p["conid"],
                    "side"     : _close_side_reg,
                    "orderType": "STP",
                    "price"    : _wide_px,
                    "quantity" : p["qty"],
                    "tif"      : "DAY",
                    "cOID"     : p["wide_stop_ref"],
                    "order_ref": p["wide_stop_ref"],
                }])
                for _wi in (_ws_resp if isinstance(_ws_resp, list) else []):
                    if not isinstance(_wi, dict): continue
                    if "id" in _wi and "message" in _wi:
                        try: broker.session.reply_confirm(_wi["id"])
                        except Exception: pass
                    _w_oid = str(_wi.get("order_id", ""))
                    if _w_oid and _w_oid not in ("-1", "0"):
                        wide_stp_oid = _w_oid
                if wide_stp_oid:
                    logger.info("[%s] Wide STP placed at %.4f oid=%s",
                                sym, _wide_px, wide_stp_oid)
                    break
                else:
                    logger.warning("[%s] Wide STP OID missing", sym); break
            except Exception as _wse:
                if _ws_attempt < 2 and "503" in str(_wse):
                    time.sleep(0.5)
                else:
                    logger.warning("[%s] Wide STP failed: %s", sym, _wse); break

        bracket_tgt_oid = ref_to_oid.get(p["target_ref"])

        with broker._order_ids_lock:
            broker._order_ids[sym] = {
                "parent"    : parent_oid,
                "wide_stop" : wide_stp_oid,
                "target"    : bracket_tgt_oid,
            }
            broker._order_ids[sym + "_qty"] = p["qty"]
        if wide_stp_oid:
            broker._register_order(wide_stp_oid, sym, _close_side_reg, "WIDE_STOP")
        if bracket_tgt_oid:
            broker._register_order(bracket_tgt_oid, sym, _close_side_reg, "TARGET")

        # ── Register parent OID — wide STP and Bracket 2 OIDs fetched after fill
        with broker._order_ids_lock:
            broker._order_ids[sym] = {
                "parent"   : parent_oid,
                "wide_stop": None,  # fetched after fill, then updated to 1.0×MPL
                "stop"     : None,  # set to wide_stop OID after update
                "stop_price": None,
                "stop_lmt" : None,
                "target"   : None,
            }
            broker._order_ids[sym + "_qty"] = p["qty"]

        # ── Hand off to verify thread ─────────────────────────────────────
        with _pending_lock:
            _pending_fills.append((p, parent_oid, None, None))
            _deadlines[sym] = time.time() + 62
        logger.info("[%s] Bracket registered — parent=%s", sym, parent_oid or "MISSING")

    def _verify_loop():
        """
        Single thread that monitors all pending fills simultaneously.
        Runs forever alongside _entry_loop.

        Every 2 seconds:
          - Checks fill status for every stock in _pending_fills (one HTTP each)
          - On fill: places target LMT using actual fill price, registers position
          - On 60s timeout: cancels parent (IBKR auto-cancels STP+STPLMT via parentId)

        The 2s sleep is lock-free — all stocks are checked in the same loop pass.
        For 40 stocks: 40 × 50ms GET = 2s per pass, same as 1 stock per pass.
        """
        logger.info("Verify loop started")
        # Track symbols whose fill has already been handled this session.
        # Prevents the "treating as filled" path from firing twice when
        # the orders endpoint lags and reports no fill on a second poll.
        _filled_syms: set = set()

        while True:
            time.sleep(0.5)   # reduced from 2s — catches fills during OID recovery

            with _pending_lock:
                pending = list(_pending_fills)

            if not pending:
                continue

            to_remove = []
            for (p, p_oid, s_oid, sl_oid) in pending:
                sym = p["symbol"]
                # Already handled this fill — skip immediately
                if sym in _filled_syms:
                    to_remove.append((p, p_oid, s_oid, sl_oid))
                    continue
                try:
                    fq, avg = broker._poll_for_fill(
                        p_oid, sym, timeout=2, expected_side=p["direction"])
                except Exception:
                    fq, avg = 0, 0.0

                if fq > 0:
                    # ── PARENT FILLED ─────────────────────────────────────
                    # fq = actual filled qty (never p["qty"] — partial fills)
                    fill_px = avg if avg > 0 else p["bar_close"]
                    mpl     = p.get("mpl", 0.60)
                    slm     = p.get("sl_multiplier", 1.0)
                    sls     = p.get("sl_spread",     0.03)
                    tgt     = p.get("tgt_mult",      3.0)
                    _cs     = "BUY" if p["direction"] == "SHORT" else "SELL"

                    if p["direction"] == "SHORT":
                        sp  = round(fill_px + mpl,              4)
                        slt = round(fill_px + mpl * slm,        4)
                        slp = round(fill_px + mpl * slm + sls,  4)
                        tp  = round(fill_px - mpl * tgt,        4)
                    else:
                        sp  = round(fill_px - mpl,              4)
                        slt = round(fill_px - mpl * slm,        4)
                        slp = round(fill_px - mpl * slm - sls,  4)
                        tp  = round(fill_px + mpl * tgt,        4)

                    stop_oid   = None
                    target_oid = None

                    # ── Guard: re-check position before placing orders ────
                    # Wide STP may have fired immediately after parent fill,
                    # making the position flat before we get here.
                    # If already flat → skip all order placement entirely.
                    _recheck = broker.get_ibkr_actual_positions()
                    if sym not in _recheck:
                        logger.warning("[%s] Position already flat after fill "
                                       "— wide STP likely fired. Skipping "
                                       "order placement.", sym)
                        # Cancel any orphan orders via conid sweep
                        _conid_flat = broker._conid_cache.get(sym)
                        if _conid_flat:
                            try:
                                _lo_f = broker.session.get_live_orders(broker.account_id)
                                for _of in (_lo_f.get("orders",[])
                                             if isinstance(_lo_f,dict) else []):
                                    if str(_of.get("conid","")) == str(_conid_flat):
                                        _of_id = str(_of.get("orderId",
                                                      _of.get("order_id","")))
                                        if _of_id:
                                            try:
                                                broker.session.cancel_order(
                                                    broker.account_id, _of_id)
                                                logger.info("[%s] Cancelled orphan "
                                                            "oid=%s", sym, _of_id)
                                            except Exception:
                                                pass
                            except Exception as _gse:
                                logger.debug("[%s] Orphan sweep: %s", sym, _gse)
                        # Skip to position registration with fq=0 equivalent
                        fq = 0
                    
                    if fq > 0:
                        # ── POST A: STP Price, STP Limit, Target ──────────
                        # 3 separate POSTs — IBKR rejects multi-order batches
                        # without parentId, and parentId on filled parent also
                        # rejected. Separate POSTs is the only reliable path.
                        for _ticket, _key, _side, _otype in [
                            ({"conid": p["conid"], "side": _cs, "orderType": "STP",
                              "price": slt, "quantity": fq, "tif": "DAY",
                              "cOID": p["stoplmt_ref"]+"_P",
                              "order_ref": p["stoplmt_ref"]+"_P"},
                             "stop_price", _cs, "STOP_PRICE"),
                            ({"conid": p["conid"], "side": _cs,
                              "orderType": "STP LMT",
                              "price": slp, "auxPrice": slt, "quantity": fq,
                              "tif": "DAY",
                              "cOID": p["stoplmt_ref"],
                              "order_ref": p["stoplmt_ref"]},
                             "stop_lmt", _cs, "STOP_LMT"),
                            ({"conid": p["conid"], "side": p["target_side"],
                              "orderType": "LMT", "price": tp, "quantity": fq,
                              "tif": "DAY",
                              "cOID": p["target_ref"],
                              "order_ref": p["target_ref"]},
                             "target", p["target_side"], "TARGET"),
                        ]:
                            try:
                                _r = broker.session.place_order(
                                    broker.account_id, [_ticket])
                                for _i in (_r if isinstance(_r, list) else []):
                                    if not isinstance(_i, dict):
                                        continue
                                    if "id" in _i and "message" in _i:
                                        try:
                                            broker.session.reply_confirm(_i["id"])
                                        except Exception:
                                            pass
                                    _o = str(_i.get("order_id", ""))
                                    if _o and _o not in ("-1", "0"):
                                        with broker._order_ids_lock:
                                            broker._order_ids.setdefault(
                                                sym, {})[_key] = _o
                                        broker._register_order(_o, sym, _side, _otype)
                                        if _key == "target":
                                            target_oid = _o
                                        logger.info("[%s] %s oid=%s",
                                                    sym, _otype, _o)
                            except Exception as _e:
                                logger.warning("[%s] %s failed: %s",
                                               sym, _otype, _e)

                        # ── POST B: Find Wide STP, cancel it, place new STP ─
                        # Try to find wide STP OID from cached or live orders.
                        # If found: cancel it. If not found: conid sweep to
                        # cancel any unrecognised STP on this conid (= wide STP).
                        wide_oid = (broker._order_ids.get(sym) or {}).get("wide_stop")
                        if not wide_oid:
                            try:
                                _lo = broker.session.get_live_orders(broker.account_id)
                                for _o in (_lo.get("orders", [])
                                           if isinstance(_lo, dict) else []):
                                    _ref = str(_o.get("order_ref", "") or
                                               _o.get("local_order_id", ""))
                                    _id  = str(_o.get("orderId",
                                                _o.get("order_id", "")))
                                    if (p["wide_stop_ref"] in _ref and
                                            _id not in ("", "0", "-1")):
                                        wide_oid = _id
                                        with broker._order_ids_lock:
                                            broker._order_ids.setdefault(
                                                sym, {})["wide_stop"] = _id
                                        logger.info("[%s] Wide STP OID found: %s",
                                                    sym, _id)
                                        break
                            except Exception as _we:
                                logger.debug("[%s] Wide STP fetch: %s", sym, _we)

                        if wide_oid:
                            # Found it — cancel directly
                            try:
                                broker.session.cancel_order(
                                    broker.account_id, wide_oid)
                                broker._unregister_order(wide_oid)
                                with broker._order_ids_lock:
                                    broker._order_ids.get(sym, {}).pop(
                                        "wide_stop", None)
                                logger.info("[%s] Wide STP cancelled oid=%s",
                                            sym, wide_oid)
                            except Exception as _ce:
                                logger.warning("[%s] Wide STP cancel failed: %s",
                                               sym, _ce)
                        else:
                            # OID not found — conid sweep to cancel all unrecognised
                            # STP orders on this conid (wide STP is the only one
                            # we did not place ourselves at this point).
                            logger.warning("[%s] Wide STP OID not found — "
                                           "running conid sweep to cancel it", sym)
                            _conid = broker._conid_cache.get(sym)
                            _known_oids = {
                                v for v in
                                (broker._order_ids.get(sym) or {}).values()
                                if v
                            }
                            if _conid:
                                try:
                                    _lo2 = broker.session.get_live_orders(
                                        broker.account_id)
                                    for _o2 in (_lo2.get("orders", [])
                                                if isinstance(_lo2, dict) else []):
                                        if str(_o2.get("conid","")) != str(_conid):
                                            continue
                                        _id2 = str(_o2.get("orderId",
                                                    _o2.get("order_id", "")))
                                        _otype2 = str(_o2.get("orderType",
                                                       _o2.get("order_type", "")))
                                        # Cancel any STP we don't recognise
                                        if (_id2 and _id2 not in _known_oids
                                                and "STP" in _otype2.upper()
                                                and "LMT" not in _otype2.upper()):
                                            try:
                                                broker.session.cancel_order(
                                                    broker.account_id, _id2)
                                                logger.info(
                                                    "[%s] Wide STP sweep cancelled "
                                                    "oid=%s", sym, _id2)
                                            except Exception:
                                                pass
                                except Exception as _s2e:
                                    logger.debug("[%s] Wide STP sweep: %s",
                                                 sym, _s2e)

                        # Place new primary STP at fill_price ± 1.0×MPL
                        try:
                            _sr = broker.session.place_order(broker.account_id, [{
                                "conid"    : p["conid"],
                                "side"     : _cs,
                                "orderType": "STP",
                                "price"    : sp,
                                "quantity" : fq,
                                "tif"      : "DAY",
                                "cOID"     : p["stop_ref"],
                                "order_ref": p["stop_ref"],
                            }])
                            for _si in (_sr if isinstance(_sr, list) else []):
                                if not isinstance(_si, dict):
                                    continue
                                if "id" in _si and "message" in _si:
                                    try:
                                        broker.session.reply_confirm(_si["id"])
                                    except Exception:
                                        pass
                                _so = str(_si.get("order_id", ""))
                                if _so and _so not in ("-1", "0"):
                                    stop_oid = _so
                            if stop_oid:
                                broker._register_order(stop_oid, sym, _cs, "STOP")
                                with broker._order_ids_lock:
                                    broker._order_ids.setdefault(
                                        sym, {})["stop"] = stop_oid
                                logger.info("[%s] STP placed at %.4f "
                                            "(fill=%.4f qty=%d ± MPL=%.2f) oid=%s",
                                            sym, sp, fill_px, fq, mpl, stop_oid)
                            else:
                                logger.warning("[%s] STP placed but OID missing",
                                               sym)
                        except Exception as exc:
                            logger.warning("[%s] STP failed: %s", sym, exc)

                    # ── Register position ─────────────────────────────────
                    with broker._pos_lock:
                        open_positions[sym] = {
                            "symbol"          : sym,
                            "direction"       : p["direction"],
                            "entry_price"     : fill_px,
                            "stop_price"      : sp,
                            "stop_limit_price": p["stop_lmt_price"],
                            "target_price"    : tp,
                            "be_activated"    : False,
                            "max_high"        : fill_px,
                            "min_low"         : fill_px,
                            "high_history"    : [],
                            "low_history"     : [],
                            "quantity"        : fq,
                        }
                    # Correct target price if fill_px differs from bar_close
                    _bar_tp = p.get("target_price", tp)
                    if abs(tp - _bar_tp) > 0.01 and target_oid:
                        try:
                            broker.session.modify_order(broker.account_id, target_oid, {
                                "conid"    : p["conid"],
                                "side"     : p["target_side"],
                                "orderType": "LMT",
                                "price"    : tp,
                                "quantity" : fq,
                                "tif"      : "DAY",
                            })
                            logger.info("[%s] Target modified to fill-based level "
                                        "%.4f → %.4f", sym, _bar_tp, tp)
                        except Exception as _me:
                            logger.debug("[%s] Target modify skipped: %s", sym, _me)

                    logger.info("[%s] Position opened  %s  entry=%.4f  "
                                "stop=%.4f  target=%.4f  qty=%d  "
                                "stop_oid=%s  target_oid=%s",
                                sym, p["direction"], fill_px, sp, tp, fq,
                                stop_oid or "MISSING",
                                target_oid or "MISSING")
                    _filled_syms.add(sym)   # guard against double-fire
                    with _entering_lock:
                        _currently_entering.discard(sym)
                    to_remove.append((p, p_oid, s_oid, sl_oid))

                elif time.time() > _deadlines.get(sym, float("inf")):
                    # ── TIMEOUT — cancel parent (IBKR auto-cancels STP via parentId) ──
                    logger.warning("[%s] Unfilled after 60s — cancelling parent "
                                   "(IBKR auto-cancels STP via parentId)", sym)
                    cancel_failed = False
                    try:
                        broker.session.cancel_order(broker.account_id, p_oid)
                    except Exception as exc:
                        logger.warning("[%s] Cancel failed: %s", sym, exc)
                        cancel_failed = True

                    if cancel_failed:
                        # Cancel returned 400 — two possible meanings:
                        # A) Order filled (position open) → place emergency stop
                        # B) Order filled AND immediately stopped out (FLAT) → just clean up
                        # IMPORTANT: IBKR positions endpoint has a caching delay (up to 60s).
                        # We must wait and recheck before concluding FLAT.
                        try:
                            actual = broker.get_ibkr_actual_positions()
                            if not actual or sym not in actual:
                                # Recheck after 4 seconds — IBKR cache may be stale
                                logger.info("[%s] Cancel 400 — IBKR shows FLAT, "
                                            "waiting 4s for position cache to update...", sym)
                                time.sleep(4)
                                actual = broker.get_ibkr_actual_positions()

                            if not actual or sym not in actual:
                                logger.info("[%s] Cancel 400 + FLAT after recheck — "
                                            "order filled and stopped out immediately. "
                                            "Cancelling any live child orders.", sym)
                                # Cancel ALL bracket children — OIDs may be None since
                                # child recovery runs inside "if fq > 0" which hasn't
                                # fired yet. Belt-and-suspenders conid sweep catches all.
                                for _key in ("wide_stop", "stop_price", "stop_lmt",
                                             "target", "stop"):
                                    _child_oid = (broker._order_ids.get(sym) or {}).get(_key)
                                    if _child_oid:
                                        try:
                                            broker.session.cancel_order(
                                                broker.account_id, _child_oid)
                                            logger.info("[%s] Cancelled orphan %s oid=%s",
                                                        sym, _key, _child_oid)
                                        except Exception:
                                            pass
                                # Conid sweep — catches children even if OIDs are None
                                _conid_flat = broker._conid_cache.get(sym)
                                if _conid_flat:
                                    try:
                                        _lo_flat = broker.session.get_live_orders(
                                            broker.account_id, filters="open")
                                        for _of in (_lo_flat or {}).get("orders", []):
                                            if str(_of.get("conid","")) == str(_conid_flat):
                                                _of_id = str(_of.get("orderId",
                                                              _of.get("order_id","")))
                                                if _of_id:
                                                    try:
                                                        broker.session.cancel_order(
                                                            broker.account_id, _of_id)
                                                        logger.info(
                                                            "[%s] Conid sweep cancelled "
                                                            "orphan oid=%s", sym, _of_id)
                                                    except Exception:
                                                        pass
                                    except Exception as _cse:
                                        logger.debug("[%s] Conid sweep failed: %s",
                                                     sym, _cse)
                                # Fall through to normal cleanup below
                            elif sym in actual:
                                # Position IS open on IBKR — treat as fill
                                logger.warning(
                                    "[%s] Cancel failed but IBKR holds %s — "
                                    "treating as fill, placing STP+target now",
                                    sym, actual[sym])
                                fill_px  = p.get("limit_price", 0.0)
                                fq       = broker._order_ids.get(sym + "_qty",
                                             StockList.get_quantity(sym))
                                direction = p["direction"]
                                cfg       = load_config()
                                mpl       = float(p.get("mpl",
                                             cfg.get("MPL_dollar_Amount", 0.60)))
                                tgt_mult  = float(cfg.get("TargetMultiplier", 3.0))
                                sls       = float(cfg.get("StopLimitSpread", 0.03))

                                if direction == "SHORT":
                                    sp = round(fill_px + mpl, 2)
                                    tp = round(fill_px - mpl * tgt_mult, 2)
                                else:
                                    sp = round(fill_px - mpl, 2)
                                    tp = round(fill_px + mpl * tgt_mult, 2)

                                stop_oid = target_oid = None
                                try:
                                    stop_oid = broker._place_stop_order(
                                        sym, direction, fq, sp, sls)
                                except Exception as se:
                                    logger.error("[%s] Emergency STP failed: %s", sym, se)
                                try:
                                    target_oid = broker._place_target_order(
                                        sym, direction, fq, tp)
                                except Exception as te:
                                    logger.error("[%s] Emergency TGT failed: %s", sym, te)

                                open_positions[sym] = {
                                    "symbol"          : sym,
                                    "direction"       : direction,
                                    "entry_price"     : fill_px,
                                    "stop_price"      : sp,
                                    "stop_limit_price": round(sp - sls if direction == "SHORT" else sp + sls, 2),
                                    "target_price"    : tp,
                                    "be_activated"    : False,
                                    "max_high"        : fill_px,
                                    "min_low"         : fill_px,
                                    "high_history"    : [],
                                    "low_history"     : [],
                                    "quantity"        : fq,
                                }
                                with broker._order_ids_lock:
                                    broker._order_ids[sym] = {
                                        "parent": p_oid,
                                        "stop"  : stop_oid,
                                        "target": target_oid,
                                    }
                                logger.warning(
                                    "[%s] Emergency bracket registered: "
                                    "entry=%.4f stop=%.4f target=%.4f "
                                    "stop_oid=%s target_oid=%s",
                                    sym, fill_px, sp, tp,
                                    stop_oid or "MISSING",
                                    target_oid or "MISSING")
                                _filled_syms.add(sym)
                                with _entering_lock:
                                    _currently_entering.discard(sym)
                                to_remove.append((p, p_oid, s_oid, sl_oid))
                                continue
                        except Exception as chk_exc:
                            logger.error("[%s] Post-cancel-failure position check failed: %s",
                                         sym, chk_exc)

                    # Cancel wide STP — independent order, NOT auto-cancelled with parent
                    _w_oid_to = (broker._order_ids.get(sym) or {}).get("wide_stop")
                    if _w_oid_to:
                        try:
                            broker.session.cancel_order(broker.account_id, _w_oid_to)
                            broker._unregister_order(_w_oid_to)
                            logger.info("[%s] Wide STP cancelled on 60s timeout (oid=%s)",
                                        sym, _w_oid_to)
                        except Exception as _wce:
                            logger.warning("[%s] Wide STP cancel on timeout failed: %s",
                                           sym, _wce)

                    with broker._order_ids_lock:
                        broker._order_ids.pop(sym, None)
                        broker._order_ids.pop(sym + "_qty", None)
                    _filled_syms.discard(sym)   # allow re-entry later
                    with _entering_lock:
                        _currently_entering.discard(sym)
                    to_remove.append((p, p_oid, s_oid, sl_oid))

            if to_remove:
                with _pending_lock:
                    for item in to_remove:
                        try:
                            _pending_fills.remove(item)
                            _deadlines.pop(item[0]["symbol"], None)
                        except ValueError:
                            pass

    threading.Thread(target=_entry_loop,  daemon=True, name="entry-loop").start()
    threading.Thread(target=_verify_loop, daemon=True, name="verify-loop").start()
    logger.info("Entry loop + verify loop started — "
                "brackets placed sequentially, fills verified in parallel")

    broker.start(on_bar=on_bar)

    # ── Step 1: Subscribe indexes FIRST so their MACD is ready before ─────
    # stocks start generating signals.
    index_symbols = StockList.all_index_symbols()
    if index_symbols:
        logger.info("Subscribing %d index(es): %s", len(index_symbols), index_symbols)
        for idx in index_symbols:
            try:
                broker.subscribe(idx, is_index=True)
            except Exception as exc:
                logger.error("[%s] index subscribe failed: %s — index filter will block",
                             idx, exc)
    else:
        logger.info("No active indexes defined — all stocks run without index filter")

    # ── Step 2: Subscribe all stocks (Y and N) ────────────────────────────
    stock_symbols = StockList.all_symbols()
    if not stock_symbols:
        logger.error("No stocks in config.json Stocks section — nothing to do")
        broker.stop()
        return

    logger.info("Subscribing %d stock(s): %s", len(stock_symbols), stock_symbols)
    for sym in stock_symbols:
        try:
            broker.subscribe(sym, is_index=False)
        except Exception as exc:
            logger.error("[%s] subscribe failed: %s — skipping", sym, exc)

    logger.info(
        "Strategy LIVE.  Edit config.json at any time:\n"
        "  • enabled Y/N          → takes effect next bar\n"
        "  • index_filter Y/N     → takes effect next bar\n"
        "  • quantity             → takes effect next bar\n"
        "  • macd_fast/slow/res   → auto-reseeds that symbol (~3 sec)\n"
        "  Ctrl-C to stop."
    )

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Strategy interrupted by user")
    finally:
        broker.stop()


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    # No command-line args needed — stock list is in config.json
    broker = IBKRBroker(
        base_url   = BASE_URL,
        account_id = ACCOUNT_ID,
        verify_ssl = VERIFY_SSL,
    )
    run_strategy(broker)
