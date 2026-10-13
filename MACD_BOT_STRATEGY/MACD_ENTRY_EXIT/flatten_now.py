"""
flatten_now.py — one-shot account flattener.

Connects to IB Gateway with a dedicated clientId, calls reqGlobalCancel
to kill all working orders, then iterates through positions and sends a
MKT close for each non-zero position. Then disconnects.

Run on the VM after the bot has been stopped, before deploying changes.
"""
import time
import threading
from collections import defaultdict
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order


class Flatten(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self._oid_event = threading.Event()
        self._next_oid = None
        self._positions = []
        self._pos_done = threading.Event()
        self._open_orders = []
        self._oo_done = threading.Event()

    def nextValidId(self, orderId):
        self._next_oid = orderId
        self._oid_event.set()

    def position(self, account, contract, position, avgCost):
        if position != 0:
            self._positions.append((account, contract, int(position), avgCost))

    def positionEnd(self):
        self._pos_done.set()

    def openOrder(self, orderId, contract, order, orderState):
        self._open_orders.append((orderId, contract.symbol, order.action,
                                  order.totalQuantity, order.orderType, orderState.status))

    def openOrderEnd(self):
        self._oo_done.set()

    def error(self, reqId, code, msg, advancedOrderRejectJson="", *args):
        if code in (2104, 2106, 2158, 2107, 2103, 2105, 2150, 202):
            return
        print(f"  [ERR] reqId={reqId} code={code} msg={msg}")

    def get_next_oid(self):
        with threading.Lock():
            oid = self._next_oid
            self._next_oid += 1
            return oid


def main():
    app = Flatten()
    app.connect("127.0.0.1", 4002, clientId=99)
    t = threading.Thread(target=app.run, daemon=True); t.start()

    if not app._oid_event.wait(timeout=10):
        print("ERROR: no nextValidId from gateway")
        app.disconnect()
        return
    # Push our base above any client-2 IDs in flight
    if app._next_oid < 60000:
        app._next_oid = 60000

    print("Connected. Cancelling all working orders ...")
    app.reqGlobalCancel()
    time.sleep(3)

    print("Requesting open orders ...")
    app._oo_done.clear()
    app.reqAllOpenOrders()
    app._oo_done.wait(timeout=8)
    if app._open_orders:
        print(f"  Still {len(app._open_orders)} open orders after global cancel:")
        for o in app._open_orders:
            print(f"    oid={o[0]} {o[1]} {o[2]} {o[3]} {o[4]} status={o[5]}")
    else:
        print("  No open orders.")

    print("\nRequesting positions ...")
    app._pos_done.clear()
    app.reqPositions()
    app._pos_done.wait(timeout=10)
    print(f"Found {len(app._positions)} non-zero position(s):")
    for acct, c, qty, avg in app._positions:
        print(f"  {c.symbol}  qty={qty}  avgCost={avg:.4f}")

    if not app._positions:
        print("\nAccount already flat. Disconnecting.")
        app.disconnect()
        return

    print("\nSending MKT close orders ...")
    for acct, c, qty, avg in app._positions:
        action = "SELL" if qty > 0 else "BUY"
        contract = Contract()
        contract.conId       = c.conId
        contract.symbol      = c.symbol
        contract.secType     = c.secType
        contract.exchange    = "SMART"
        contract.primaryExchange = c.primaryExchange or "NASDAQ"
        contract.currency    = c.currency or "USD"

        order = Order()
        order.action       = action
        order.orderType    = "MKT"
        order.totalQuantity = abs(qty)
        order.tif          = "DAY"
        order.transmit     = True
        # Hard-disable any guardrails that might block a cleanup MKT
        order.outsideRth   = False
        order.eTradeOnly   = False
        order.firmQuoteOnly = False

        oid = app.get_next_oid()
        print(f"  oid={oid}  {action} {abs(qty)} {c.symbol}  MKT")
        app.placeOrder(oid, contract, order)
        time.sleep(0.4)

    print("\nWaiting 12 s for MKT closes to fill ...")
    time.sleep(12)

    print("\nRe-checking positions ...")
    app._positions.clear()
    app._pos_done.clear()
    app.reqPositions()
    app._pos_done.wait(timeout=10)
    if app._positions:
        print(f"  Still {len(app._positions)} non-zero:")
        for acct, c, qty, avg in app._positions:
            print(f"    {c.symbol}  qty={qty}")
    else:
        print("  ACCOUNT FLAT.")

    print("\nFinal open-order check ...")
    app._open_orders.clear()
    app._oo_done.clear()
    app.reqAllOpenOrders()
    app._oo_done.wait(timeout=8)
    if app._open_orders:
        print(f"  Residual orders: {len(app._open_orders)}")
        for o in app._open_orders:
            print(f"    oid={o[0]} {o[1]} {o[2]} {o[3]} {o[4]} status={o[5]}")
    else:
        print("  No residual orders.")

    app.disconnect()
    print("\nDone.")


if __name__ == "__main__":
    main()
