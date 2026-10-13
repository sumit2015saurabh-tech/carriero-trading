"""
Bulletproof flatten:
  1. reqGlobalCancel
  2. SINGLE snapshot of positions (cancelPositions after positionEnd)
  3. Exactly ONE MKT close per symbol
  4. Wait, re-check ONCE
"""
import time, threading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order


class F(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.pos = {}                            # symbol -> (contract, qty, avg)
        self.pos_done = threading.Event()
        self.next_oid = None
        self.oid_evt = threading.Event()

    def nextValidId(self, oid):
        self.next_oid = max(oid, 80000)
        self.oid_evt.set()

    def position(self, account, contract, qty, avgCost):
        # dict-keyed: dedup updates from the streaming phase
        self.pos[contract.symbol] = (contract, int(qty), avgCost)

    def positionEnd(self):
        self.pos_done.set()

    def error(self, rid, code, msg, *a, **k):
        if code in (2104, 2106, 2158, 2107, 2103, 2105, 2150, 202, 2176):
            return
        print(f"  [ERR] rid={rid} code={code} msg={msg}", flush=True)


def snapshot_positions(a, label):
    a.pos.clear()
    a.pos_done.clear()
    a.reqPositions()
    a.pos_done.wait(15)
    a.cancelPositions()                          # critical — stop the stream
    time.sleep(1)
    non_zero = {s: v for s, v in a.pos.items() if v[1] != 0}
    print(f"--- snapshot ({label}): {len(non_zero)} non-zero positions", flush=True)
    for sym, (_, qty, av) in sorted(non_zero.items()):
        print(f"    {sym:<6s} qty={qty:>7d} avg={av:.4f}", flush=True)
    return non_zero


def main():
    a = F()
    a.connect("127.0.0.1", 4002, clientId=77)
    threading.Thread(target=a.run, daemon=True).start()
    a.oid_evt.wait(10)
    print(f"connected. base oid={a.next_oid}", flush=True)

    print("\n[step 1] reqGlobalCancel ...", flush=True)
    a.reqGlobalCancel()
    time.sleep(3)

    print("\n[step 2] snapshot positions ...", flush=True)
    pos = snapshot_positions(a, "before-flatten")
    if not pos:
        print("\nALREADY FLAT. Disconnecting.", flush=True)
        a.disconnect()
        print("DONE", flush=True)
        return

    print(f"\n[step 3] placing {len(pos)} MKT close order(s) (one per symbol) ...",
          flush=True)
    placed_oids = []
    for sym, (contract, qty, _) in pos.items():
        side = "SELL" if qty > 0 else "BUY"
        c = Contract()
        c.conId = contract.conId
        c.symbol = contract.symbol
        c.secType = contract.secType
        c.exchange = "SMART"
        c.primaryExchange = contract.primaryExchange or "NASDAQ"
        c.currency = contract.currency or "USD"
        o = Order()
        o.action = side
        o.orderType = "MKT"
        o.totalQuantity = abs(qty)
        o.tif = "DAY"
        o.transmit = True
        o.eTradeOnly = False
        o.firmQuoteOnly = False
        oid = a.next_oid
        a.next_oid += 1
        print(f"    oid={oid} {side} {abs(qty)} {sym} MKT", flush=True)
        a.placeOrder(oid, c, o)
        placed_oids.append(oid)
        time.sleep(0.5)

    print(f"\n[step 4] waiting 18s for fills ...", flush=True)
    time.sleep(18)

    print("\n[step 5] snapshot positions ...", flush=True)
    pos2 = snapshot_positions(a, "after-flatten")

    if not pos2:
        print("\nFLATTEN_SUCCESS — account flat.", flush=True)
    else:
        print(f"\nFLATTEN_PARTIAL — {len(pos2)} symbol(s) still open.", flush=True)

    a.disconnect()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
