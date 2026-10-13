"""
=============================================================================
IBKR DISCORD MONITOR  —  DUXXXXXXX
=============================================================================
Features:
  1. Rejected order detection  — checks every 30s, sends Discord alert with
                                  full order details on any rejection found.
  2. Drawdown alert            — if portfolio drawdown > 1% from day-start NAV,
                                  sends alert every 20 seconds until drawdown
                                  recovers below 1% or script is stopped.
  3. EoD open position warning — at 15:52 EST checks for open positions and
                                  alerts. At 15:57 EST re-checks and sends
                                  alert every 30 seconds until flat or 16:00.

Usage:
    python3 discord_monitor.py

    Runs alongside ibkr_broker.py — shares the same session cookie,
    never calls /tickle, never writes cookies. Zero interference.
=============================================================================
"""

import json
import os
import time
import threading
from datetime import datetime, date
from zoneinfo import ZoneInfo

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_URL      = "https://localhost:5000/v1/api"
ACCOUNT_ID    = "DUXXXXXXX"
SESSION_FILE  = os.path.join(os.path.expanduser("~"), ".ibkr_session.json")
DISCORD_WEBHOOK = __import__("os").environ.get("DISCORD_WEBHOOK_URL", "")

EST = ZoneInfo("America/New_York")

# Drawdown threshold (1%)
DRAWDOWN_THRESHOLD_PCT = 1.0

# How often to re-alert during drawdown (seconds)
DRAWDOWN_ALERT_INTERVAL = 20

# How often to poll for rejected orders (seconds)
REJECTED_POLL_INTERVAL = 30

# EoD times (EST)
EOD_FIRST_CHECK  = (15, 52)   # 3:52 PM
EOD_SECOND_CHECK = (15, 57)   # 3:57 PM
EOD_CUTOFF       = (16,  0)   # 4:00 PM

# ─── Shared state ─────────────────────────────────────────────────────────────
_day_start_nav       = None          # NAV captured at script start
_alerted_rejected    = set()         # order IDs already alerted
_drawdown_alerting   = False         # True while drawdown > threshold
_eod_first_done      = False         # first EoD check sent
_eod_loop_running    = False         # EoD 30-second loop active
_stop_event          = threading.Event()


# =============================================================================
# SESSION  (read-only — never writes cookies, never tickles)
# =============================================================================

def _load_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE) as f:
                cookies = json.load(f)
            for name, info in cookies.items():
                s.cookies.set(
                    name, info["value"],
                    domain=info.get("domain", "localhost"),
                    path=info.get("path", "/"),
                )
            print(f"[Monitor] Loaded session cookies from {SESSION_FILE}")
        except Exception as e:
            print(f"[Monitor] Warning: could not load cookies: {e}")
    return s


def _get(session: requests.Session, path: str, params: dict = None):
    resp = session.get(
        f"{BASE_URL}/{path.lstrip('/')}",
        params=params,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


# =============================================================================
# DISCORD
# =============================================================================

def _send_discord(message: str, color: int = None, title: str = None,
                  fields: list = None):
    """
    Send a message to Discord via webhook.
    Supports plain text OR rich embed (when title/fields provided).
    color: Discord embed colour as integer (e.g. 0xFF0000 = red)
    """
    try:
        if title or fields:
            embed = {
                "title"      : title or "",
                "description": message,
                "color"      : color or 0x2F3136,
                "timestamp"  : datetime.utcnow().isoformat(),
                "footer"     : {"text": f"IBKR Monitor  |  {ACCOUNT_ID}"},
            }
            if fields:
                embed["fields"] = fields
            payload = {"embeds": [embed]}
        else:
            payload = {"content": message}

        resp = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"[Discord] Send failed: {resp.status_code} — {resp.text[:200]}")
        else:
            print(f"[Discord] Sent: {title or message[:60]}")
    except Exception as e:
        print(f"[Discord] Exception: {e}")


def _fmt_time() -> str:
    return datetime.now(EST).strftime("%H:%M:%S EST")


# =============================================================================
# FEATURE 1 — Rejected order detection
# =============================================================================

def _fetch_orders(session: requests.Session) -> list:
    try:
        resp = _get(session, "/iserver/account/orders",
                    params={"accountId": ACCOUNT_ID, "force": "true"})
        if isinstance(resp, dict):
            return resp.get("orders", [])
        if isinstance(resp, list):
            return resp
    except Exception as e:
        print(f"[RejectedChecker] Could not fetch orders: {e}")
    return []


def _check_rejected_orders(session: requests.Session):
    global _alerted_rejected

    orders = _fetch_orders(session)
    today  = date.today().strftime("%Y%m%d")

    for o in orders:
        status = str(o.get("status", "")).upper()
        if status not in ("CANCELLED", "INACTIVE", "REJECTED"):
            continue

        oid = str(o.get("orderId", o.get("order_id", "")))
        if not oid or oid in _alerted_rejected:
            continue

        # Filter to today's orders only
        exec_time = str(o.get("lastExecutionTime",
                               o.get("lastFillTime",
                               o.get("time", ""))))
        if len(exec_time) >= 8 and today not in exec_time.replace("-","").replace("/",""):
            continue

        _alerted_rejected.add(oid)

        sym    = o.get("ticker", o.get("symbol", "?"))
        side   = o.get("side", "?")
        otype  = o.get("orderType", "?")
        qty    = o.get("totalSize", o.get("size", "?"))
        price  = o.get("price", "")
        reason = o.get("text", o.get("warningText", o.get("reason", "No reason provided")))

        price_str = f"${float(price):,.2f}" if price else "MARKET"

        fields = [
            {"name": "Symbol",     "value": str(sym),    "inline": True},
            {"name": "Side",       "value": str(side),   "inline": True},
            {"name": "Order Type", "value": str(otype),  "inline": True},
            {"name": "Quantity",   "value": str(qty),    "inline": True},
            {"name": "Price",      "value": price_str,   "inline": True},
            {"name": "Order ID",   "value": str(oid),    "inline": True},
            {"name": "Status",     "value": str(status), "inline": True},
            {"name": "Reason",     "value": str(reason), "inline": False},
            {"name": "Time",       "value": _fmt_time(), "inline": True},
        ]

        _send_discord(
            message=f"An order was **{status}** for `{sym}`.",
            title="🚫 Order Rejected / Cancelled",
            color=0xFF4444,
            fields=fields,
        )


def rejected_order_loop(session: requests.Session):
    print("[Monitor] Rejected order checker started")
    while not _stop_event.is_set():
        try:
            _check_rejected_orders(session)
        except Exception as e:
            print(f"[RejectedChecker] Error: {e}")
        _stop_event.wait(timeout=REJECTED_POLL_INTERVAL)
    print("[Monitor] Rejected order checker stopped")


# =============================================================================
# FEATURE 2 — Drawdown alert
# =============================================================================

def _fetch_nav(session: requests.Session) -> float:
    try:
        summary = _get(session, f"/portfolio/{ACCOUNT_ID}/summary")
        nav = summary.get("netliquidationvalue", {}).get("amount", 0)
        return float(nav)
    except Exception as e:
        print(f"[DrawdownChecker] Could not fetch NAV: {e}")
        return 0.0


def drawdown_loop(session: requests.Session):
    global _day_start_nav, _drawdown_alerting

    print("[Monitor] Drawdown monitor started")

    # Capture starting NAV
    while _day_start_nav is None and not _stop_event.is_set():
        nav = _fetch_nav(session)
        if nav > 0:
            _day_start_nav = nav
            print(f"[DrawdownChecker] Day-start NAV captured: ${_day_start_nav:,.2f}")
        else:
            _stop_event.wait(timeout=5)

    while not _stop_event.is_set():
        try:
            current_nav = _fetch_nav(session)
            if current_nav <= 0 or _day_start_nav is None:
                _stop_event.wait(timeout=20)
                continue

            drawdown_amt = _day_start_nav - current_nav
            drawdown_pct = (drawdown_amt / _day_start_nav) * 100

            if drawdown_pct >= DRAWDOWN_THRESHOLD_PCT:
                # In drawdown — alert and keep alerting every 20s
                if not _drawdown_alerting:
                    _drawdown_alerting = True
                    print(f"[DrawdownChecker] Drawdown {drawdown_pct:.2f}% — alerting")

                fields = [
                    {"name": "Starting NAV",   "value": f"${_day_start_nav:,.2f}",  "inline": True},
                    {"name": "Current NAV",    "value": f"${current_nav:,.2f}",      "inline": True},
                    {"name": "Drawdown ($)",   "value": f"${drawdown_amt:,.2f}",     "inline": True},
                    {"name": "Drawdown (%)",   "value": f"{drawdown_pct:.2f}%",      "inline": True},
                    {"name": "Threshold",      "value": f"{DRAWDOWN_THRESHOLD_PCT}%","inline": True},
                    {"name": "Time",           "value": _fmt_time(),                 "inline": True},
                    {"name": "Action Required","value": "Monitor open positions. Consider reducing exposure.", "inline": False},
                ]

                _send_discord(
                    message=(
                        f"⚠️ Portfolio drawdown has exceeded **{DRAWDOWN_THRESHOLD_PCT}%**!\n"
                        f"Current drawdown: **{drawdown_pct:.2f}%**  (${drawdown_amt:,.2f})\n"
                        f"Alerts repeat every {DRAWDOWN_ALERT_INTERVAL}s until recovered."
                    ),
                    title="⚠️ Drawdown Alert",
                    color=0xFF8C00,
                    fields=fields,
                )
                _stop_event.wait(timeout=DRAWDOWN_ALERT_INTERVAL)

            else:
                # Recovered
                if _drawdown_alerting:
                    _drawdown_alerting = False
                    _send_discord(
                        message=(
                            f"✅ Portfolio drawdown has recovered below "
                            f"**{DRAWDOWN_THRESHOLD_PCT}%**.\n"
                            f"Current NAV: **${current_nav:,.2f}**\n"
                            f"Drawdown: **{drawdown_pct:.2f}%**"
                        ),
                        title="✅ Drawdown Recovered",
                        color=0x00CC66,
                        fields=[
                            {"name": "Current NAV",  "value": f"${current_nav:,.2f}", "inline": True},
                            {"name": "Drawdown",     "value": f"{drawdown_pct:.2f}%", "inline": True},
                            {"name": "Time",         "value": _fmt_time(),             "inline": True},
                        ],
                    )
                    print("[DrawdownChecker] Drawdown recovered — alert cleared")
                _stop_event.wait(timeout=20)

        except Exception as e:
            print(f"[DrawdownChecker] Error: {e}")
            _stop_event.wait(timeout=20)

    print("[Monitor] Drawdown monitor stopped")


# =============================================================================
# FEATURE 3 — EoD open position warnings
# =============================================================================

def _fetch_open_positions(session: requests.Session) -> list:
    try:
        positions = _get(session, f"/portfolio/{ACCOUNT_ID}/positions/0")
        return [p for p in positions if float(p.get("position", 0)) != 0]
    except Exception as e:
        print(f"[EoDChecker] Could not fetch positions: {e}")
        return []


def _build_position_fields(open_pos: list) -> list:
    fields = []
    for p in open_pos:
        sym     = p.get("contractDesc", p.get("ticker", "?"))
        if "\n" in sym:
            sym = sym.split("\n")[0]
        qty     = float(p.get("position", 0))
        side    = "LONG" if qty > 0 else "SHORT"
        mkt_px  = float(p.get("mktPrice", 0))
        ur      = float(p.get("unrealizedPnl", 0))
        fields.append({
            "name" : f"{sym.strip()}  ({side}  {abs(int(qty))} shares)",
            "value": f"Mkt Price: ${mkt_px:,.2f}  |  Unrealized P&L: ${ur:,.2f}",
            "inline": False,
        })
    return fields


def _send_eod_alert(session: requests.Session, check_label: str):
    open_pos = _fetch_open_positions(session)
    if not open_pos:
        return False   # flat — no alert needed

    count = len(open_pos)
    fields = _build_position_fields(open_pos)
    fields.append({"name": "Check", "value": check_label, "inline": True})
    fields.append({"name": "Time",  "value": _fmt_time(), "inline": True})

    _send_discord(
        message=(
            f"🕒 **{count} open position(s)** still active near market close!\n"
            f"These will be force-closed by the EoD routine if not closed manually."
        ),
        title="🕒 EoD Open Position Warning",
        color=0xFF4500,
        fields=fields,
    )
    print(f"[EoDChecker] {check_label} — alerted {count} open positions")
    return True


def eod_loop(session: requests.Session):
    global _eod_first_done, _eod_loop_running

    print("[Monitor] EoD position watcher started")

    while not _stop_event.is_set():
        now_est = datetime.now(EST)
        h, m    = now_est.hour, now_est.minute

        # ── 3:52 PM check ──────────────────────────────────────────────
        if (h, m) == EOD_FIRST_CHECK and not _eod_first_done:
            _eod_first_done = True
            _send_eod_alert(session, "3:52 PM check")

        # ── 3:57 PM — start 30-second loop until 4:00 PM or flat ──────
        if (h, m) >= EOD_SECOND_CHECK and (h, m) < EOD_CUTOFF:
            if not _eod_loop_running:
                _eod_loop_running = True
                print("[EoDChecker] 3:57 PM reached — starting 30-second loop")

            still_open = _send_eod_alert(session, "3:57–4:00 PM loop")
            if not still_open:
                print("[EoDChecker] Account flat — stopping EoD loop")
                _eod_loop_running = False
                _stop_event.wait(timeout=30)
                continue
            _stop_event.wait(timeout=30)
            continue

        # ── Past 4:00 PM — reset flags for next day ────────────────────
        if (h, m) >= EOD_CUTOFF:
            if _eod_first_done or _eod_loop_running:
                print("[EoDChecker] Past 4:00 PM — resetting EoD flags for tomorrow")
                _eod_first_done   = False
                _eod_loop_running = False

        _stop_event.wait(timeout=10)

    print("[Monitor] EoD watcher stopped")


# =============================================================================
# STARTUP CHECK
# =============================================================================

def _check_auth(session: requests.Session) -> bool:
    try:
        status = _get(session, "/iserver/auth/status")
        if status.get("authenticated"):
            print(f"[Monitor] Auth OK — connected to IBKR (account={ACCOUNT_ID})")
            return True
        else:
            print("[Monitor] ✗ NOT AUTHENTICATED — start ibkr_broker.py first")
            return False
    except Exception as e:
        print(f"[Monitor] ✗ Cannot reach CP Gateway: {e}")
        return False


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("  IBKR Discord Monitor")
    print(f"  Account : {ACCOUNT_ID}")
    print(f"  Webhook : ...{DISCORD_WEBHOOK[-30:]}")
    print("=" * 60)

    session = _load_session()

    if not _check_auth(session):
        print("\n  Make sure ibkr_broker.py is running first, then retry.\n")
        return

    # Send startup notification
    _send_discord(
        message=(
            f"Discord monitor started for account **{ACCOUNT_ID}**.\n"
            f"Monitoring:\n"
            f"• Rejected orders (every {REJECTED_POLL_INTERVAL}s)\n"
            f"• Portfolio drawdown > {DRAWDOWN_THRESHOLD_PCT}% "
            f"(alerts every {DRAWDOWN_ALERT_INTERVAL}s while in drawdown)\n"
            f"• Open positions at 3:52 PM and 3:57–4:00 PM EST (every 30s)"
        ),
        title="🟢 Monitor Started",
        color=0x00CC66,
    )

    # Launch all three monitors as daemon threads
    threads = [
        threading.Thread(target=rejected_order_loop, args=(session,),
                         daemon=True, name="rejected-checker"),
        threading.Thread(target=drawdown_loop,        args=(session,),
                         daemon=True, name="drawdown-monitor"),
        threading.Thread(target=eod_loop,             args=(session,),
                         daemon=True, name="eod-watcher"),
    ]

    for t in threads:
        t.start()

    print("\n  All monitors running. Press Ctrl-C to stop.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[Monitor] Shutting down...")
        _stop_event.set()
        for t in threads:
            t.join(timeout=5)

        _send_discord(
            message=f"Discord monitor stopped for account **{ACCOUNT_ID}**.",
            title="🔴 Monitor Stopped",
            color=0xFF4444,
        )
        print("[Monitor] Stopped cleanly.\n")


if __name__ == "__main__":
    main()
