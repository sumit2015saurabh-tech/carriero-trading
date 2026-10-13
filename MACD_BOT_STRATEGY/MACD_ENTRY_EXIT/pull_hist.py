import threading, time, csv, sys
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

SYMS = ["AAPL","AMD","AMZN","ASTS","AVGO","CBRS","CRM","GE","GEV","GOOGL","GS","INTC",
        "IREN","KTOS","META","MRVL","MS","MSFT","NFLX","NVDA","OKLO","PLTR","QCOM","QQQ",
        "RKLB","SMCI","SMH","SPY","TGT","TSLA","TSM","TXN","VSAT","WMT"]
OUT = "/home/azureuser/MACD_ENTRY_EXIT/hist_7d_3min.csv"

class App(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.rows = {}
        self.done = {}
        self.idmap = {}
    def nextValidId(self, oid):
        self.ready = True
    def historicalData(self, reqId, bar):
        sym = self.idmap.get(reqId)
        self.rows.setdefault(sym, []).append((bar.date, bar.open, bar.high, bar.low, bar.close))
    def historicalDataEnd(self, reqId, start, end):
        self.done[self.idmap.get(reqId)] = True
    def error(self, reqId, code, msg, *a):
        if code not in (2104,2106,2158,2107,2103,2105,2150,2108,2119,2100):
            print(f"  ERR req={reqId} code={code} {msg}")
            if reqId in self.idmap and code in (162,165,200,321,420):
                self.done[self.idmap[reqId]] = True

app = App()
app.connect("127.0.0.1", 4002, clientId=56)
threading.Thread(target=app.run, daemon=True).start()
time.sleep(2)

for i, sym in enumerate(SYMS):
    c = Contract(); c.symbol = sym; c.secType = "STK"; c.exchange = "SMART"; c.currency = "USD"
    rid = 7000 + i
    app.idmap[rid] = sym
    app.reqHistoricalData(rid, c, "", "7 D", "3 mins", "TRADES", 1, 1, False, [])
    time.sleep(1.6)

deadline = time.time() + 90
while time.time() < deadline and len(app.done) < len(SYMS):
    time.sleep(1)

with open(OUT, "w", newline="") as fh:
    w = csv.writer(fh); w.writerow(["symbol","datetime","open","high","low","close"])
    n = 0
    for sym in SYMS:
        for d, o, h, l, cl in app.rows.get(sym, []):
            w.writerow([sym, d, o, h, l, cl]); n += 1
got = sum(1 for s in SYMS if app.rows.get(s))
print(f"symbols with data: {got}/{len(SYMS)}  | total bars: {n}  -> {OUT}")
missing = [s for s in SYMS if not app.rows.get(s)]
if missing: print("MISSING:", missing)
app.disconnect()
