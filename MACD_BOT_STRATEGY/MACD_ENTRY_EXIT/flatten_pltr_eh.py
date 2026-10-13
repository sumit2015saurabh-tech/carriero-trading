"""
Cancel any pending PLTR sell and place a LMT SELL with outsideRth=True
so it can execute during extended hours (16:00-20:00 ET). Uses a
slightly-below-market LMT to ensure fill if any bid exists.
"""
import threading, time
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order

HOST = "127.0.0.1"
PORT = 4002
CLIENT_ID = 97

class C(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.connected = threading.Event()
        self.positions = {}
        self.positions_done = threading.Event()
        self.next_id = None
        self.tick_bid = None
        self.tick_ask = None
        self.tick_last = None
        self.tick_ready = threading.Event()
        self.order_status = {}
    def nextValidId(self, oid):
        self.next_id = oid
        self.connected.set()
    def position(self, account, contract, position, avgCost):
        if position != 0:
            self.positions[contract.symbol] = (int(position), contract)
    def positionEnd(self):
        self.positions_done.set()
    def tickPrice(self, reqId, tickType, price, attrib):
        if tickType == 1: self.tick_bid  = price
        if tickType == 2: self.tick_ask  = price
        if tickType == 4: self.tick_last = price
        if self.tick_bid is not None and self.tick_ask is not None:
            self.tick_ready.set()
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, *a, **k):
        self.order_status[orderId] = (status, filled, avgFillPrice)
        print(f"[ORD] oid={orderId} status={status} filled={filled} avg={avgFillPrice}")
    def error(self, reqId, code, msg, *a, **k):
        if code in (2104, 2106, 2158, 2107, 2119, 2100, 2103, 2105, 10167):
            return
        print(f"[ERR] reqId={reqId} code={code}: {msg}")

c = C()
c.connect(HOST, PORT, CLIENT_ID)
threading.Thread(target=c.run, daemon=True).start()
if not c.connected.wait(timeout=10):
    print("[FATAL] no nextValidId"); raise SystemExit

print("\n[STEP A] reqGlobalCancel")
c.reqGlobalCancel()
time.sleep(4)

print("\n[STEP B] reqPositions")
c.reqPositions()
c.positions_done.wait(timeout=10)
c.cancelPositions()
if "PLTR" not in c.positions:
    print("[DONE] No PLTR position. Exiting.")
    c.disconnect(); raise SystemExit

qty, contract_from_pos = c.positions["PLTR"]
print(f"  PLTR position: {qty:+d}")

# Build a clean stock contract for PLTR
pltr = Contract()
pltr.symbol = "PLTR"
pltr.secType = "STK"
pltr.currency = "USD"
pltr.exchange = "SMART"
pltr.primaryExchange = "NASDAQ"
pltr.conId = 444857009

print("\n[STEP C] subscribe market data for LMT pricing")
md_id = c.next_id + 100
c.reqMktData(md_id, pltr, "", False, False, [])
got = c.tick_ready.wait(timeout=8)
c.cancelMktData(md_id)
print(f"  bid={c.tick_bid}  ask={c.tick_ask}  last={c.tick_last}")

# Pick LMT price: SELL → place slightly below bid (or last) to ensure fill
# Use a 0.5% buffer
ref = c.tick_bid if c.tick_bid and c.tick_bid > 0 else c.tick_last
if ref is None or ref <= 0:
    print("[FATAL] no usable reference price; cannot place safe LMT")
    c.disconnect(); raise SystemExit
if qty > 0:           # LONG → SELL
    lmt_px = round(ref * 0.995, 2)
    side   = "SELL"
else:                  # SHORT → BUY
    lmt_px = round(ref * 1.005, 2)
    side   = "BUY"
abs_qty = abs(qty)

print(f"\n[STEP D] place LMT {side} {abs_qty} @ {lmt_px}  outsideRth=True  tif=GTC")
o = Order()
o.action = side
o.totalQuantity = abs_qty
o.orderType = "LMT"
o.lmtPrice = lmt_px
o.tif = "GTC"
o.outsideRth = True
o.eTradeOnly = False
o.firmQuoteOnly = False
oid = c.next_id
c.next_id += 1
c.placeOrder(oid, pltr, o)

print("\n[STEP E] wait up to 30s for fill")
deadline = time.time() + 30
while time.time() < deadline:
    time.sleep(1)
    st = c.order_status.get(oid)
    if st and st[0] == "Filled":
        break

print("\n[STEP F] reqPositions verify")
c.positions = {}
c.positions_done.clear()
c.reqPositions()
c.positions_done.wait(timeout=10)
c.cancelPositions()
if "PLTR" not in c.positions:
    print("[SUCCESS] PLTR flat.")
else:
    qty2, _ = c.positions["PLTR"]
    print(f"[STILL OPEN] PLTR qty={qty2}  (LMT order may still be working at IBKR)")
    print(f"  Order {oid}: {c.order_status.get(oid)}")

time.sleep(2)
c.disconnect()
