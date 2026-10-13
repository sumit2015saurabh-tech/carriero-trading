"""
=============================================================================
POSITION CHECKER  —  Open positions + Closed trades + Net Daily P&L
=============================================================================
Usage:
    cd ~/Desktop/System_Trades/MACD_ENTRY_EXIT
    python3 check_positions.py

    Auto-refreshes every 10 seconds. Press Ctrl-C to exit.

    Shows:
      1. Account overview (NAV, cash, margin, buying power)
      2. Net Daily P&L  = Realized (closed trades) + Unrealized (open positions)
      3. Open positions  with unrealized P&L per position
      4. Closed trades today  with realized P&L per trade
      5. Live orders

    Does NOT call /tickle or write any cookies — zero interference with
    ibkr_broker.py. It only reads the shared session cookie file written
    by ibkr_broker.py.
=============================================================================
"""

import json
import os
import time
from datetime import datetime

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL     = "https://localhost:5000/v1/api"
ACCOUNT_ID   = "DUXXXXXXX"
SESSION_FILE = os.path.join(os.path.expanduser("~"), ".ibkr_session.json")

# ANSI colour helpers
GREEN  = "\033[32m"
RED    = "\033[31m"
YELLOW = "\033[33m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def _colour(value, text):
    """Return green text if value >= 0, red if negative."""
    return f"{GREEN}{text}{RESET}" if value >= 0 else f"{RED}{text}{RESET}"


def _load_session():
    """
    Load the shared session cookie written by ibkr_broker.py.
    We NEVER write back to this file — read-only so the running
    strategy session is never disturbed.
    """
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
        except Exception:
            pass   # session file missing or malformed — will try anyway
    return s


def _get(session, path, params=None):
    resp = session.get(
        f"{BASE_URL}/{path.lstrip('/')}",
        params=params,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def _fetch_closed_trades(session):
    """
    Fetch today's filled orders from IBKR and compute realized P&L
    per symbol from the fills.

    IBKR CP Gateway endpoint:
        GET /iserver/account/orders?accountId=...&force=true
    Filled orders have status = "Filled".

    We pair BUY fills against SELL fills (or vice versa for shorts)
    to compute realized P&L per completed round-trip.

    Returns a list of dicts:
        {symbol, side, qty, entry_price, exit_price, realized_pnl, fill_time}
    """
    today = datetime.now().strftime("%Y%m%d")
    closed = []

    try:
        resp = _get(session, "/iserver/account/orders",
                    params={"accountId": ACCOUNT_ID, "force": "true"})
        if isinstance(resp, dict):
            orders_list = resp.get("orders", [])
        elif isinstance(resp, list):
            orders_list = resp
        else:
            orders_list = []
    except Exception:
        return closed

    # ── Collect all FILLED orders for today ──────────────────────────
    # IBKR order object keys we use:
    #   ticker / symbol, side (BUY/SELL), orderType, status,
    #   filledQuantity / totalSize, avgPrice / price, lastExecutionTime
    filled = []
    for o in orders_list:
        status = str(o.get("status", "")).upper()
        if status != "FILLED":
            continue
        # Filter to today — lastExecutionTime format: "YYYYMMDD HH:MM:SS"
        exec_time = str(o.get("lastExecutionTime", o.get("lastFillTime", "")))
        if today not in exec_time.replace("-", "").replace("/", ""):
            # If no date in the string at all, include it (intraday only)
            if len(exec_time) > 6:
                continue
        filled.append(o)

    if not filled:
        return closed

    # ── Group fills by symbol ─────────────────────────────────────────
    from collections import defaultdict
    by_sym = defaultdict(list)
    for o in filled:
        sym = o.get("ticker", o.get("symbol", "?"))
        if "\n" in sym:
            sym = sym.split("\n")[0]
        sym = sym.strip().upper()
        by_sym[sym].append(o)

    # ── For each symbol, match entry fills vs exit fills ─────────────
    # Strategy only trades stocks (not options/futures), so we treat:
    #   SHORT trade: opening SELL fill + closing BUY fill
    #   LONG  trade: opening BUY  fill + closing SELL fill
    #
    # We reconstruct P&L as:
    #   SHORT: (sell_price - buy_price) * qty
    #   LONG:  (sell_price - buy_price) * qty
    # which is the same formula in both cases.

    for sym, fills in by_sym.items():
        buy_fills  = [f for f in fills if str(f.get("side", "")).upper() == "BUY"]
        sell_fills = [f for f in fills if str(f.get("side", "")).upper() == "SELL"]

        # Aggregate qty and weighted avg price for buys and sells
        total_buy_qty  = sum(float(f.get("filledQuantity", f.get("totalSize", 0))) for f in buy_fills)
        total_sell_qty = sum(float(f.get("filledQuantity", f.get("totalSize", 0))) for f in sell_fills)

        def _wavg(fs):
            total_qty = sum(float(f.get("filledQuantity", f.get("totalSize", 0))) for f in fs)
            if total_qty == 0:
                return 0.0
            total_val = sum(
                float(f.get("filledQuantity", f.get("totalSize", 0))) *
                float(f.get("avgPrice", f.get("price", 0)))
                for f in fs
            )
            return total_val / total_qty

        avg_buy  = _wavg(buy_fills)
        avg_sell = _wavg(sell_fills)

        # Matched qty = the smaller of buy and sell (completed round-trips)
        matched_qty = min(total_buy_qty, total_sell_qty)
        if matched_qty == 0:
            continue

        realized = (avg_sell - avg_buy) * matched_qty

        # Determine direction from which side opened first
        # Use the earliest fill time to decide
        def _fill_time(fs):
            times = [str(f.get("lastExecutionTime", f.get("lastFillTime", ""))) for f in fs]
            return min(t for t in times if t) if times else ""

        buy_time  = _fill_time(buy_fills)
        sell_time = _fill_time(sell_fills)

        if sell_time and buy_time and sell_time < buy_time:
            direction = "SHORT"   # sold first → short trade
        elif buy_time and sell_time and buy_time < sell_time:
            direction = "LONG"
        elif sell_fills and not buy_fills:
            direction = "SHORT"
        else:
            direction = "LONG"

        entry_px = avg_sell if direction == "SHORT" else avg_buy
        exit_px  = avg_buy  if direction == "SHORT" else avg_sell
        fill_t   = sell_time if direction == "SHORT" else buy_time

        # Format fill time nicely
        try:
            ft = datetime.strptime(fill_t[:15], "%Y%m%d %H:%M:%S").strftime("%H:%M:%S")
        except Exception:
            ft = fill_t[:8] if fill_t else "?"

        closed.append({
            "symbol"      : sym,
            "direction"   : direction,
            "qty"         : int(matched_qty),
            "entry_price" : entry_px,
            "exit_price"  : exit_px,
            "realized_pnl": realized,
            "fill_time"   : ft,
        })

    return sorted(closed, key=lambda x: x["fill_time"])


def show_positions(session):
    now = datetime.now().strftime("%H:%M:%S")

    # ── Account summary ───────────────────────────────────────────────
    try:
        summary = _get(session, f"/portfolio/{ACCOUNT_ID}/summary")
    except Exception as e:
        print(f"\n  ✗ Cannot fetch account summary: {e}")
        print("    Make sure gateway is running and you are authenticated")
        return False

    nav    = summary.get("netliquidationvalue", {}).get("amount", 0)
    cash   = summary.get("totalcashvalue",      {}).get("amount", 0)
    ur_pnl = summary.get("unrealizedpnl",       {}).get("amount", 0)
    r_pnl  = summary.get("realizedpnl",         {}).get("amount", 0)
    maint  = summary.get("maintenancemarginreq", {}).get("amount", 0)
    excess = summary.get("excessliquidity",      {}).get("amount", 0)
    bp     = summary.get("buyingpower",          {}).get("amount", 0)

    # Net daily P&L = realized (all closed trades today) + unrealized (open now)
    net_daily_pnl = r_pnl + ur_pnl

    # ── Clear screen & header ─────────────────────────────────────────
    print("\033c", end="")
    print("=" * 80)
    print(f"  IBKR P&L MONITOR  |  {now}  |  Account: {ACCOUNT_ID}")
    print("=" * 80)

    # ── Account overview ──────────────────────────────────────────────
    print(f"\n  {'Net Liquidation:':<24} ${nav:>14,.2f}")
    print(f"  {'Cash:':<24} ${cash:>14,.2f}")
    print(f"  {'Maintenance Margin:':<24} ${maint:>14,.2f}")
    print(f"  {'Excess Liquidity:':<24} ${excess:>14,.2f}")
    print(f"  {'Buying Power:':<24} ${bp:>14,.2f}")

    # ── NET DAILY P&L — the number that matters ───────────────────────
    print(f"\n  {'─' * 76}")
    net_str = f"${net_daily_pnl:>13,.2f}"
    print(f"  {BOLD}NET DAILY P&L (open + closed):{RESET:<1}  "
          f"{_colour(net_daily_pnl, BOLD + net_str + RESET)}")
    print(f"  {'  Realized   (closed trades):':<32} "
          f"{_colour(r_pnl,  f'${r_pnl:>12,.2f}')}")
    print(f"  {'  Unrealized (open positions):':<32} "
          f"{_colour(ur_pnl, f'${ur_pnl:>12,.2f}')}")
    print(f"  {'─' * 76}")

    # ── Open positions ────────────────────────────────────────────────
    try:
        positions = _get(session, f"/portfolio/{ACCOUNT_ID}/positions/0")
    except Exception:
        positions = []

    open_pos = [p for p in positions if float(p.get("position", 0)) != 0]

    print(f"\n  OPEN POSITIONS ({len(open_pos)})")
    print(f"  {'─' * 76}")

    if not open_pos:
        print("  (no open positions)")
    else:
        print(f"  {'Symbol':<8} {'Side':<6} {'Qty':>6} {'Avg Cost':>12} "
              f"{'Mkt Price':>12} {'Mkt Value':>14} {'Unrealized':>12} {'% P&L':>8}")
        print(f"  {'─' * 76}")

        total_mkt_val = 0.0
        total_ur      = 0.0

        for p in sorted(open_pos, key=lambda x: x.get("contractDesc", "")):
            sym      = p.get("contractDesc", p.get("ticker", "?"))
            if "\n" in sym:
                sym = sym.split("\n")[0]
            qty      = float(p.get("position", 0))
            avg_cost = float(p.get("avgCost", 0))
            mkt_px   = float(p.get("mktPrice", 0))
            mkt_val  = float(p.get("mktValue", 0))
            ur       = float(p.get("unrealizedPnl", 0))
            side     = "LONG" if qty > 0 else "SHORT"
            qty_abs  = abs(int(qty))

            cost_basis = avg_cost * abs(qty)
            pct = (ur / cost_basis * 100) if cost_basis != 0 else 0.0

            total_mkt_val += mkt_val
            total_ur      += ur

            ur_str  = _colour(ur,  f"${ur:>11,.2f}")
            pct_str = _colour(pct, f"{pct:>7.2f}%")

            print(f"  {sym:<8} {side:<6} {qty_abs:>6} ${avg_cost:>11,.2f} "
                  f"${mkt_px:>11,.2f} ${mkt_val:>13,.2f} {ur_str} {pct_str}")

        print(f"  {'─' * 76}")
        tot_label = f"  {'TOTAL':<8} {'':<6} {'':<6} {'':<12} {'':<12}"
        print(f"{tot_label} ${total_mkt_val:>13,.2f} "
              f"{_colour(total_ur, f'${total_ur:>11,.2f}')}")

    # ── Closed trades today ───────────────────────────────────────────
    closed_trades = _fetch_closed_trades(session)
    total_realized = sum(t["realized_pnl"] for t in closed_trades)

    print(f"\n  CLOSED TRADES TODAY ({len(closed_trades)})  "
          f"— Realized: {_colour(total_realized, f'${total_realized:,.2f}')}")
    print(f"  {'─' * 76}")

    if not closed_trades:
        print("  (no closed trades yet today)")
    else:
        print(f"  {'Time':<10} {'Symbol':<8} {'Dir':<6} {'Qty':>6} "
              f"{'Entry':>12} {'Exit':>12} {'Realized P&L':>14}")
        print(f"  {'─' * 76}")

        for t in closed_trades:
            rpnl    = t["realized_pnl"]
            pnl_str = _colour(rpnl, f"${rpnl:>13,.2f}")
            print(f"  {t['fill_time']:<10} {t['symbol']:<8} {t['direction']:<6} "
                  f"{t['qty']:>6} ${t['entry_price']:>11,.2f} "
                  f"${t['exit_price']:>11,.2f} {pnl_str}")

        print(f"  {'─' * 76}")
        tot_str = _colour(total_realized, f"${total_realized:>13,.2f}")
        print(f"  {'':10} {'TOTAL':<8} {'':<6} {'':<6} {'':<12} {'':<12} {tot_str}")

    # ── Live orders ───────────────────────────────────────────────────
    try:
        orders_resp = _get(session, "/iserver/account/orders",
                           params={"accountId": ACCOUNT_ID, "force": "true"})
        if isinstance(orders_resp, dict):
            orders_list = orders_resp.get("orders", [])
        elif isinstance(orders_resp, list):
            orders_list = orders_resp
        else:
            orders_list = []

        live_orders = [
            o for o in orders_list
            if str(o.get("status", "")).upper()
            not in ("FILLED", "CANCELLED", "INACTIVE")
        ]
    except Exception:
        live_orders = []

    print(f"\n  LIVE ORDERS ({len(live_orders)})")
    print(f"  {'─' * 76}")

    if not live_orders:
        print("  (no live orders)")
    else:
        print(f"  {'Symbol':<8} {'Side':<6} {'Type':<10} {'Qty':>6} "
              f"{'Price':>12} {'Status':<16} {'Order ID':<12}")
        print(f"  {'─' * 76}")

        for o in live_orders:
            sym    = o.get("ticker", o.get("symbol", "?"))
            side   = o.get("side", "?")
            otype  = o.get("orderType", "?")
            qty    = o.get("remainingQuantity", o.get("totalSize", "?"))
            price  = o.get("price", "")
            status = o.get("status", "?")
            oid    = o.get("orderId", o.get("order_id", "?"))
            price_str = f"${float(price):>11,.2f}" if price else f"{'MKT':>12}"
            print(f"  {sym:<8} {side:<6} {otype:<10} {qty:>6} "
                  f"{price_str} {status:<16} {oid:<12}")

    # ── Footer ────────────────────────────────────────────────────────
    print(f"\n  {'─' * 76}")
    print(f"  Last updated: {now}  |  Refreshing every 10 seconds  |  Ctrl-C to exit")
    print(f"  {'─' * 76}\n")

    return True


def main():
    session = _load_session()

    # Auth check — read-only, does NOT tickle or write cookies
    try:
        status = _get(session, "/iserver/auth/status")
        if not status.get("authenticated"):
            print("\n  ✗ NOT AUTHENTICATED — login at https://localhost:5000 first\n")
            return
    except Exception as e:
        print(f"\n  ✗ Cannot reach gateway: {e}\n")
        return

    print("\n  Starting P&L monitor... (Ctrl-C to exit)\n")

    try:
        while True:
            ok = show_positions(session)
            if not ok:
                break
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n\n  P&L monitor stopped.\n")


if __name__ == "__main__":
    main()
