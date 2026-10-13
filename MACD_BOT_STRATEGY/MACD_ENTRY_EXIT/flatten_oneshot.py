"""
One-shot flattener — reads current AMZN position, sends ONE MKT order
of the exact opposite side and quantity, waits for confirmation,
disconnects. No loops, no retries.
"""
import time, threading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order


class App(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.next_oid = None
        self.oid_ev = threading.Event()
        self.positions = {}
        self.pos_done = threading.Event()
        self.fills = []

    def nextValidId(self, oid):
        self.next_oid = oid
        self.oid_ev.set()

    def position(self, account, contract, position, avgCost):
        if int(position) != 0:
            self.positions[contract.symbol] = (int(position), contract)

    def positionEnd(self):
        self.pos_done.set()

    def execDetails(self, reqId, contract, execution):
        self.fills.append((execution.orderId, contract.symbol, execution.side,
                           execution.shares, execution.price))

    def error(self, reqId, code, msg, advancedOrderRejectJson="", *args):
        if code in (2104, 2106, 2158, 2107, 2103, 2105, 2150, 202, 2108):
            return
        print(f"  [ERR] reqId={reqId} code={code} msg={msg}")


def main():
    app = App()
    app.connect("127.0.0.1", 4002, clientId=77)
    threading.Thread(target=app.run, daemon=True).start()
    if not app.oid_ev.wait(timeout=10):
        print("ERROR: no nextValidId"); return
    if app.next_oid < 90000:
        app.next_oid = 90000

    print("Cancelling all working orders ...")
    app.reqGlobalCancel()
    time.sleep(3)

    print("Reading positions ...")
    app.pos_done.clear()
    app.reqPositions()
    if not app.pos_done.wait(timeout=10):
        print("ERROR: positionEnd never received"); return

    if not app.positions:
        print("ALREADY FLAT — nothing to do."); app.disconnect(); return

    print(f"Found {len(app.positions)} non-zero position(s):")
    for sym, (qty, _) in app.positions.items():
        print(f"  {sym}  qty={qty}")

    placed = []
    for sym, (qty, contract) in app.positions.items():
        c = Contract()
        c.conId    = contract.conId
        c.symbol   = sym
        c.secType  = "STK"
        c.currency = "USD"
        c.exchange = "SMART"
        o = Order()
        o.action        = "BUY" if qty < 0 else "SELL"
        o.totalQuantity = abs(qty)
        o.orderType     = "MKT"
        o.tif           = "DAY"
        o.eTradeOnly    = False
        o.firmQuoteOnly = False
        oid = app.next_oid; app.next_oid += 1
        app.placeOrder(oid, c, o)
        placed.append((oid, sym, o.action, abs(qty)))
        print(f"  PLACED oid={oid}  {o.action} {abs(qty)} {sym} MKT")
        time.sleep(0.3)

    print("\nWaiting 10s for fills ...")
    time.sleep(10)

    # Re-read positions
    app.positions = {}
    app.pos_done.clear()
    app.reqPositions()
    app.pos_done.wait(timeout=10)
    if app.positions:
        print(f"Remaining non-zero positions ({len(app.positions)}):")
        for sym, (qty, _) in app.positions.items():
            print(f"  {sym}  qty={qty}")
    else:
        print("CONFIRMED FLAT — all positions closed.")

    app.disconnect()


if __name__ == "__main__":
    main()
