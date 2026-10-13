#!/usr/bin/env python3
"""
monitor.py — independent safety watchdog for the MACD bot.

What it does (and ONLY what it does):
─────────────────────────────────────────────────────────────────────────────
 1. Tails the bot's latest tws_DD_MM_YY.txt log file in real time.
 2. Tracks two values pushed by IBKR over the bot's WebSocket:
       • dailyPnL            (from reqPnL → "PNL daily=… realized=… unrealized=…")
       • NetLiquidation      (from reqAccountUpdates → "ACCOUNT NetLiquidation = …")
 3. Computes drawdown:        dd_pct = -min(0, dailyPnL) / NetLiquidation × 100
 4. Tracks the timestamp of the last log line as a heartbeat.
 5. Fires EMERGENCY_EXIT when ANY of these happen:
       • dd_pct > DD_THRESHOLD_PCT (default 1.2)
       • no new log lines for STALE_LOG_SEC (default 150 = 2.5 min)
       • bot logs "TWS DISCONNECTED for Xs" or "outage ended after" with X≥150
 6. EMERGENCY_EXIT:
       • opens its OWN IBKR socket on a fresh clientId (does not collide
         with the bot's clientId=1)
       • reqGlobalCancel — kills every working order in the account
       • reqPositions   — for each non-zero, fires a MKT close order
       • waits up to 30 s for fills
       • SIGINT the bot's python macd_bot.py process
       • optionally `docker stop ib-gateway` (--stop-gateway flag)

Design rules:
  - PUSH ONLY: monitor never polls IBKR; the values it acts on are pushed
    over the WS by the bot's running subscriptions. Monitor reads them out
    of the log.
  - BLAST-RADIUS LIMITED: monitor's IBKR connection only fires cancels and
    MKT closes. It cannot place new entries.
  - IDEMPOTENT: if the monitor loops fire EMERGENCY twice, the second
    pass sees an already-flat account and exits cleanly.
  - SELF-KILL ON SUCCESS: after a successful flatten + bot kill, monitor
    exits with rc=0 so a supervising shell can decide whether to restart.

Usage:
    python3 monitor.py
    python3 monitor.py --threshold 1.5 --log-dir ~/MACD_ENTRY_EXIT/logs
    python3 monitor.py --stop-gateway     # also docker stop ib-gateway
    python3 monitor.py --dry-run          # do everything except fire orders
"""
from __future__ import annotations

import argparse
import glob
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from typing import Dict, Optional, Tuple

# ── ibapi (same package the bot uses) ───────────────────────────────────────
try:
    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper
    from ibapi.contract import Contract
    from ibapi.order import Order
except ImportError:
    print("FATAL: ibapi not installed. Run with the bot's venv:")
    print("  ~/MACD_ENTRY_EXIT/venv/bin/python monitor.py")
    sys.exit(1)

# =============================================================================
# CONFIG (overridable via CLI)
# =============================================================================
DEFAULT_LOG_DIR        = os.path.expanduser("~/MACD_ENTRY_EXIT/logs")
DEFAULT_DD_PCT         = 1.2          # drawdown threshold % of NetLiquidation
DEFAULT_STALE_LOG_SEC  = 150.0        # 2.5 min — log heartbeat threshold
DEFAULT_TWS_HOST       = "127.0.0.1"
DEFAULT_TWS_PORT       = 4002
DEFAULT_MONITOR_CLIENT = 11           # bot uses 1; flatten script used 2-3-10
DEFAULT_BOT_PROC_NAME  = "macd_bot.py"
DEFAULT_DOCKER_NAME    = "ib-gateway"
CHECK_INTERVAL_SEC     = 3            # how often the watchdog thread evaluates

# =============================================================================
# LOGGER (own file — does not pollute the bot's log)
# =============================================================================
LOG_PATH = os.path.expanduser("~/MACD_ENTRY_EXIT/logs/monitor.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MONITOR] %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger("monitor")

# =============================================================================
# REGEXES — match the bot's exact log formats
# =============================================================================
PNL_RE      = re.compile(r"PNL\s+daily=([+-]?[\d.]+)\s+realized=([+-]?[\d.]+)\s+unrealized=([+-]?[\d.]+)")
NETLIQ_RE   = re.compile(r"ACCOUNT NetLiquidation = ([\d.]+)\s*USD")
LONGDISC_RE = re.compile(r"TWS DISCONNECTED for ([\d.]+)s")
LINE_TS_RE  = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")

# =============================================================================
# SHARED STATE (written by tail thread, read by watchdog thread)
# =============================================================================
class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.daily_pnl: Optional[float]  = None
        self.realized: Optional[float]   = None
        self.unrealized: Optional[float] = None
        self.netliq: Optional[float]     = None
        self.last_log_ts: Optional[float] = None   # epoch seconds derived from log line
        self.long_disconnect_logged: bool = False  # set when bot itself logged 2.5m+ outage
        self.fired: bool = False                   # set after EMERGENCY_EXIT runs
        self.fired_reason: str = ""

STATE = State()

# =============================================================================
# LOG TAILER — survives file rotation
# =============================================================================

def find_latest_log(log_dir: str) -> Optional[str]:
    """Return path of newest tws_*.txt in log_dir, or None."""
    pattern = os.path.join(log_dir, "tws_*.txt")
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None


def tail_log(log_dir: str):
    """
    Generator yielding (line, log_ts_epoch) as they appear in the newest log.
    Survives:
      - log file rotation (bot rolls a new tws_DD_MM_YY.txt at midnight or restart)
      - log file truncation / replacement (uses inode tracking)
      - log dir not yet existing
    """
    current_path: Optional[str] = None
    fh = None
    inode = None
    while True:
        # discover current file
        latest = find_latest_log(log_dir)
        if latest is None:
            time.sleep(2)
            continue
        # rotation? open fresh
        if latest != current_path or fh is None:
            if fh is not None:
                try: fh.close()
                except Exception: pass
            try:
                fh = open(latest, "r", encoding="utf-8", errors="replace")
                fh.seek(0, os.SEEK_END)   # start from end — only NEW lines
                inode = os.fstat(fh.fileno()).st_ino
                current_path = latest
                log.info("Tailing log: %s", current_path)
            except Exception as exc:
                log.error("Failed to open %s: %s", latest, exc)
                time.sleep(2); continue
        # read available lines
        line = fh.readline()
        if line:
            ts_epoch = _parse_line_ts(line)
            yield line, ts_epoch
            continue
        # no new line — check rotation (different inode at same path)
        try:
            st = os.stat(current_path)
            if st.st_ino != inode:
                log.info("Log rotated (inode change) — reopening")
                fh.close()
                fh = None
                continue
        except FileNotFoundError:
            log.warning("Log file vanished — re-discovering")
            fh.close(); fh = None
            continue
        time.sleep(0.25)


def _parse_line_ts(line: str) -> Optional[float]:
    m = LINE_TS_RE.match(line)
    if not m: return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        return None


def tail_thread(log_dir: str):
    """Run the tailer; update STATE on every relevant line."""
    try:
        for line, ts in tail_log(log_dir):
            with STATE.lock:
                if ts is not None:
                    STATE.last_log_ts = ts
                m = PNL_RE.search(line)
                if m:
                    STATE.daily_pnl  = float(m.group(1))
                    STATE.realized   = float(m.group(2))
                    STATE.unrealized = float(m.group(3))
                m = NETLIQ_RE.search(line)
                if m:
                    STATE.netliq = float(m.group(1))
                m = LONGDISC_RE.search(line)
                if m:
                    STATE.long_disconnect_logged = True
                    log.error("Bot reported long disconnect: %ss", m.group(1))
    except Exception as exc:
        log.exception("Tail thread crashed: %s", exc)

# =============================================================================
# EMERGENCY EXIT — flatten + cancel via own IBKR socket
# =============================================================================

class FlattenApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.ready = threading.Event()
        self.next_id: Optional[int] = None
        self.positions = []                                   # list[(contract, qty, avg)]
        self.pos_done = threading.Event()
        self.fills_seen: set = set()
    def nextValidId(self, oid):
        self.next_id = oid; self.ready.set()
    def position(self, account, contract, position, avgCost):
        if abs(position) > 0.0:
            self.positions.append((contract, position, avgCost))
    def positionEnd(self):
        self.pos_done.set()
    def orderStatus(self, orderId, status, filled, remaining, *a, **k):
        if status == "Filled" and remaining == 0:
            if orderId not in self.fills_seen:
                self.fills_seen.add(orderId)
                log.info("FLATTEN fill confirmed oid=%d", orderId)
    def error(self, reqId, code, msg, *a):
        if code in (2104, 2106, 2107, 2119, 2158, 2169, 2100, 2150, 2168, 2109):
            return                                            # informational
        log.warning("IBKR err code=%s msg=%s", code, msg[:140])


def emergency_exit(host: str, port: int, client_id: int,
                   stop_gateway: bool, dry_run: bool, reason: str) -> bool:
    """
    Flatten EVERYTHING:
      1. own IBKR socket on `client_id` (must NOT be 1; the bot uses 1)
      2. reqGlobalCancel — kills every order placed by any client
      3. reqPositions; MKT close every non-zero
      4. wait ≤30s for fills
      5. SIGINT bot
      6. (optional) docker stop ib-gateway
    Returns True if completed, False on connection failure.
    """
    log.error("=" * 78)
    log.error("EMERGENCY_EXIT triggered — reason=%s", reason)
    log.error("=" * 78)

    if dry_run:
        log.warning("--dry-run set: would flatten + kill bot but skipping actual API calls")
        _kill_bot(dry_run=True)
        if stop_gateway:
            _stop_docker(dry_run=True)
        return True

    app = FlattenApp()
    try:
        log.info("Connecting to IB Gateway %s:%d clientId=%d ...", host, port, client_id)
        app.connect(host, port, client_id)
        threading.Thread(target=app.run, daemon=True).start()
        if not app.ready.wait(15):
            log.error("Connect timed out — IBKR socket unreachable")
            _kill_bot()
            if stop_gateway: _stop_docker()
            return False
        log.info("Connected. server-given nextValidId=%s", app.next_id)
        # bump above any plausible bot-issued id
        app.next_id = max(app.next_id or 0, 200000)

        # 1. cancel everything
        try:
            log.info("reqGlobalCancel ...")
            app.reqGlobalCancel()
            time.sleep(3)
        except Exception as exc:
            log.warning("reqGlobalCancel error: %s", exc)

        # 2. positions
        log.info("reqPositions ...")
        app.pos_done.clear(); app.positions = []
        try:
            app.reqPositions()
            app.pos_done.wait(15)
        except Exception as exc:
            log.warning("reqPositions error: %s", exc)

        if not app.positions:
            log.info("No open positions — account already flat")
        else:
            log.error("Found %d open position(s) — flattening at MKT", len(app.positions))
            target = []
            for contract, qty, avg in app.positions:
                side = "SELL" if qty > 0 else "BUY"
                c = Contract()
                c.conId    = contract.conId
                c.symbol   = contract.symbol
                c.secType  = contract.secType
                c.exchange = "SMART"
                c.currency = contract.currency or "USD"
                o = Order()
                o.action         = side
                o.totalQuantity  = abs(int(qty))
                o.orderType      = "MKT"
                o.tif            = "DAY"
                o.transmit       = True
                o.outsideRth     = True
                o.eTradeOnly     = False
                o.firmQuoteOnly  = False
                oid = app.next_id; app.next_id += 1
                try:
                    app.placeOrder(oid, c, o)
                    target.append((oid, contract.symbol, abs(int(qty))))
                    log.error("  FLATTEN oid=%d %s %s qty=%d", oid, side,
                              contract.symbol, abs(int(qty)))
                except Exception as exc:
                    log.error("  placeOrder failed for %s: %s", contract.symbol, exc)
            # wait for fills
            t0 = time.time()
            while time.time() - t0 < 30 and len(app.fills_seen) < len(target):
                time.sleep(0.5)
            log.info("Fills confirmed: %d / %d", len(app.fills_seen), len(target))

        # 3. final flat check
        try:
            app.pos_done.clear(); app.positions = []
            app.reqPositions()
            app.pos_done.wait(8)
            if app.positions:
                log.error("Positions REMAINING after flatten: %s",
                          [(c.symbol, q) for c, q, _ in app.positions])
            else:
                log.info("Final state: account FLAT")
        except Exception as exc:
            log.warning("Final position check failed: %s", exc)

    except Exception as exc:
        log.exception("Emergency exit crashed: %s", exc)
    finally:
        try: app.disconnect()
        except Exception: pass

    # 4. kill bot
    _kill_bot()
    # 5. stop gateway (optional)
    if stop_gateway:
        _stop_docker()
    return True


def _kill_bot(dry_run: bool = False):
    """SIGINT then SIGTERM the bot if still alive."""
    log.warning("Killing bot process (pkill -INT -f %s)", DEFAULT_BOT_PROC_NAME)
    if dry_run:
        log.info("[dry-run] would: pkill -INT -f %s", DEFAULT_BOT_PROC_NAME)
        return
    try:
        subprocess.run(["pkill", "-INT", "-f", DEFAULT_BOT_PROC_NAME], check=False)
    except Exception as exc:
        log.error("pkill -INT failed: %s", exc)
    time.sleep(8)
    try:
        rc = subprocess.run(["pgrep", "-f", DEFAULT_BOT_PROC_NAME],
                            stdout=subprocess.DEVNULL).returncode
        if rc == 0:
            log.warning("Bot still alive after SIGINT — sending SIGTERM")
            subprocess.run(["pkill", "-TERM", "-f", DEFAULT_BOT_PROC_NAME], check=False)
            time.sleep(3)
            rc = subprocess.run(["pgrep", "-f", DEFAULT_BOT_PROC_NAME],
                                stdout=subprocess.DEVNULL).returncode
            if rc == 0:
                log.error("Bot STILL alive — sending SIGKILL as last resort")
                subprocess.run(["pkill", "-KILL", "-f", DEFAULT_BOT_PROC_NAME], check=False)
        else:
            log.info("Bot process gone.")
    except Exception as exc:
        log.error("Bot kill verification failed: %s", exc)


def _stop_docker(dry_run: bool = False):
    log.warning("Stopping docker container '%s'", DEFAULT_DOCKER_NAME)
    if dry_run:
        log.info("[dry-run] would: docker stop %s", DEFAULT_DOCKER_NAME)
        return
    try:
        subprocess.run(["docker", "stop", DEFAULT_DOCKER_NAME],
                       check=False, timeout=30)
    except Exception as exc:
        log.error("docker stop failed: %s", exc)

# =============================================================================
# WATCHDOG — periodic check on STATE
# =============================================================================

def watchdog(args):
    while True:
        time.sleep(CHECK_INTERVAL_SEC)
        with STATE.lock:
            if STATE.fired:
                return                                        # one-shot
            daily   = STATE.daily_pnl
            netliq  = STATE.netliq
            last_ts = STATE.last_log_ts
            long_disc = STATE.long_disconnect_logged

        now = time.time()
        triggers = []

        # 1. drawdown breach
        if daily is not None and netliq is not None and netliq > 0:
            loss = -min(0.0, daily)
            dd_pct = (loss / netliq) * 100.0
            if dd_pct >= args.threshold:
                triggers.append(
                    f"drawdown={dd_pct:.3f}%  (daily={daily:+.2f} netliq={netliq:.2f}) "
                    f">= threshold={args.threshold:.2f}%"
                )

        # 2. log heartbeat staleness
        if last_ts is not None:
            stale = now - last_ts
            if stale >= args.stale_sec:
                triggers.append(f"log stale: {stale:.0f}s since last line "
                                f">= {args.stale_sec:.0f}s")

        # 3. bot already detected a long disconnect
        if long_disc:
            triggers.append("bot logged TWS DISCONNECTED >= 150s")

        if triggers:
            with STATE.lock:
                if STATE.fired: return
                STATE.fired = True
                STATE.fired_reason = "; ".join(triggers)
            log.error("WATCHDOG fired: %s", STATE.fired_reason)
            emergency_exit(
                host=args.host, port=args.port,
                client_id=args.client_id,
                stop_gateway=args.stop_gateway,
                dry_run=args.dry_run,
                reason=STATE.fired_reason,
            )
            log.warning("EMERGENCY_EXIT complete — monitor exiting")
            return

# =============================================================================
# MAIN
# =============================================================================

def main():
    p = argparse.ArgumentParser(description="MACD bot safety monitor")
    p.add_argument("--threshold",     type=float, default=DEFAULT_DD_PCT,
                   help=f"Drawdown %% of NetLiquidation that triggers EXIT (default {DEFAULT_DD_PCT})")
    p.add_argument("--stale-sec",     type=float, default=DEFAULT_STALE_LOG_SEC,
                   help=f"Seconds without new log line that triggers EXIT (default {DEFAULT_STALE_LOG_SEC})")
    p.add_argument("--log-dir",       type=str,   default=DEFAULT_LOG_DIR,
                   help="Directory containing the bot's tws_*.txt files")
    p.add_argument("--host",          type=str,   default=DEFAULT_TWS_HOST)
    p.add_argument("--port",          type=int,   default=DEFAULT_TWS_PORT)
    p.add_argument("--client-id",     type=int,   default=DEFAULT_MONITOR_CLIENT,
                   help=f"IBKR clientId for monitor's own connection (default {DEFAULT_MONITOR_CLIENT}; bot uses 1)")
    p.add_argument("--stop-gateway",  action="store_true",
                   help="Also `docker stop ib-gateway` after flatten")
    p.add_argument("--dry-run",       action="store_true",
                   help="Run watchdog logic but skip actual flatten + kill")
    args = p.parse_args()

    log.info("Monitor started")
    log.info("  log-dir       = %s", args.log_dir)
    log.info("  threshold     = %.2f%% drawdown of NetLiquidation", args.threshold)
    log.info("  stale-sec     = %.0fs (log heartbeat)", args.stale_sec)
    log.info("  client-id     = %d (bot uses 1; do NOT collide)", args.client_id)
    log.info("  stop-gateway  = %s", args.stop_gateway)
    log.info("  dry-run       = %s", args.dry_run)
    log.info("  monitor.log   = %s", LOG_PATH)

    # graceful SIGINT for the monitor itself
    def _on_sigint(*_):
        log.warning("Monitor received SIGINT — exiting (no flatten triggered)")
        sys.exit(0)
    signal.signal(signal.SIGINT, _on_sigint)
    signal.signal(signal.SIGTERM, _on_sigint)

    # tailer + watchdog
    t1 = threading.Thread(target=tail_thread, args=(args.log_dir,), daemon=True, name="tailer")
    t1.start()
    try:
        watchdog(args)
    except Exception as exc:
        log.exception("Watchdog crashed: %s", exc)
        sys.exit(2)
    sys.exit(0)


if __name__ == "__main__":
    main()
