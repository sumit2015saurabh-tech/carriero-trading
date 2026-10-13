"""Check positions + open orders. Read-only."""
import time, threading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper

class Q(EWrapper, EClient):
    def __init__(s):
        EClient.__init__(s, s)
        s.pos_set = {}      # dict — last value wins (no duplicates)
        s.pos_done = threading.Event()
        s.oo = []
        s.oo_done = threading.Event()

    def nextValidId(s, oid): pass

    def position(s, a, c, p, av):
        # Use dict so only the most recent qty per symbol is kept
        s.pos_set[c.symbol] = (int(p), av)

    def positionEnd(s):
        s.pos_done.set()

    def openOrder(s, oid, c, o, st):
        s.oo.append((oid, c.symbol, o.action, int(o.totalQuantity), o.orderType, st.status))

    def openOrderEnd(s):
        s.oo_done.set()

    def error(s, *a, **k): pass


a = Q()
a.connect("127.0.0.1", 4002, clientId=88)
threading.Thread(target=a.run, daemon=True).start()
time.sleep(2)

a.reqPositions()
a.pos_done.wait(10)
a.cancelPositions()                      # stop the stream

a.reqAllOpenOrders()
a.oo_done.wait(10)

print("=== POSITIONS (non-zero) ===", flush=True)
non_zero = {sym: (qty, av) for sym, (qty, av) in a.pos_set.items() if qty != 0}
for sym, (qty, av) in sorted(non_zero.items()):
    print(f"  {sym:<6s}  qty={qty:>7d}  avg={av:.4f}", flush=True)
print(f"  total: {len(non_zero)}", flush=True)

print("\n=== OPEN ORDERS ===", flush=True)
for oid, sym, act, q, ot, st in a.oo:
    print(f"  oid={oid:<6d} {sym:<6s} {act:<4s} {q:>5d} {ot:<10s} status={st}", flush=True)
print(f"  total: {len(a.oo)}", flush=True)

a.disconnect()
