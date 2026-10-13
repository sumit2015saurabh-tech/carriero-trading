"""one-shot: cancel + flatten all positions"""
import time, threading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order


class F(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.pos = []
        self.done = threading.Event()
        self.next_oid = None
        self.oid_evt = threading.Event()

    def nextValidId(self, oid):
        self.next_oid = max(oid, 70000)
        self.oid_evt.set()

    def position(self, a, c, p, av):
        if p != 0:
            self.pos.append((c, int(p), av))

    def positionEnd(self):
        self.done.set()

    def error(self, rid, code, msg, *a, **k):
        if code in (2104, 2106, 2158, 2107, 2103, 2105, 2150, 202, 2176):
            return
        print(f"  [ERR] rid={rid} code={code} msg={msg}", flush=True)


def main():
    a = F()
    a.connect("127.0.0.1", 4002, clientId=88)
    threading.Thread(target=a.run, daemon=True).start()
    a.oid_evt.wait(10)

    print("reqGlobalCancel ...", flush=True)
    a.reqGlobalCancel()
    time.sleep(2)

    print("Querying positions ...", flush=True)
    a.reqPositions()
    a.done.wait(10)
    print(f"Open positions: {len(a.pos)}", flush=True)

    if not a.pos:
        print("ALREADY FLAT", flush=True)
        a.disconnect()
        return

    for c, qty, _ in a.pos:
        side = "SELL" if qty > 0 else "BUY"
        contract = Contract()
        contract.conId = c.conId
        contract.symbol = c.symbol
        contract.secType = c.secType
        contract.exchange = "SMART"
        contract.primaryExchange = c.primaryExchange or "NASDAQ"
        contract.currency = c.currency or "USD"

        o = Order()
        o.action = side
        o.orderType = "MKT"
        o.totalQuantity = abs(qty)
        o.tif = "DAY"
        o.transmit = True
        o.eTradeOnly = False
        o.firmQuoteOnly = False

        print(f"  oid={a.next_oid} {side} {abs(qty)} {c.symbol} MKT", flush=True)
        a.placeOrder(a.next_oid, contract, o)
        a.next_oid += 1
        time.sleep(0.4)

    print("Sleeping 15s for fills ...", flush=True)
    time.sleep(15)

    a.pos.clear()
    a.done.clear()
    a.reqPositions()
    a.done.wait(10)
    print(f"Remaining: {len(a.pos)}", flush=True)
    for c, qty, av in a.pos:
        print(f"  {c.symbol} qty={qty} avg={av:.4f}", flush=True)
    a.disconnect()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
