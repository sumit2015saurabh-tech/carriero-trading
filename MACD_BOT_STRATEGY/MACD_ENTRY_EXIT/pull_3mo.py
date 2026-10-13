import threading, time, csv
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

SYMS = ["AAPL","AMD","AMZN","ASTS","AVGO","CBRS","CRM","GE","GEV","GOOGL","GS","INTC",
        "IREN","KTOS","META","MRVL","MS","MSFT","NFLX","NVDA","OKLO","PLTR","QCOM","QQQ",
        "RKLB","SMCI","SMH","SPY","TGT","TSLA","TSM","TXN","VSAT","WMT"]
OUT = "/home/azureuser/MACD_ENTRY_EXIT/hist_3mo_3min.csv"

class App(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self); self.rows = {}; self.done = {}; self.idmap = {}
    def nextValidId(self, oid): pass
    def historicalData(self, reqId, bar):
        self.rows.setdefault(self.idmap[reqId], []).append((bar.date, bar.open, bar.high, bar.low, bar.close))
    def historicalDataEnd(self, reqId, s, e): self.done[self.idmap[reqId]] = True
    def error(self, reqId, code, msg, *a):
        if code in (162,165,200,321,420,366) and reqId in self.idmap: self.done[self.idmap[reqId]] = True
        if code not in (2104,2106,2158,2107,2103,2105,2150,2108,2119,2100,2176):
            print("ERR", reqId, code, str(msg)[:70])

app = App(); app.connect("127.0.0.1", 4002, clientId=61)
threading.Thread(target=app.run, daemon=True).start(); time.sleep(2)

for i, sym in enumerate(SYMS):
    c = Contract(); c.symbol = sym; c.secType = "STK"; c.exchange = "SMART"; c.currency = "USD"
    app.idmap[8000+i] = sym
    app.reqHistoricalData(8000+i, c, "", "3 M", "3 mins", "TRADES", 1, 1, False, [])
    time.sleep(2.5)

deadline = time.time() + 180
while time.time() < deadline and len(app.done) < len(SYMS):
    time.sleep(2)

n = 0
with open(OUT, "w", newline="") as fh:
    w = csv.writer(fh); w.writerow(["symbol","datetime","open","high","low","close"])
    for sym in SYMS:
        for d, o, h, l, cl in app.rows.get(sym, []):
            w.writerow([sym, d, o, h, l, cl]); n += 1
got = sum(1 for s in SYMS if app.rows.get(s))
print(f"symbols: {got}/{len(SYMS)} | total bars: {n} -> {OUT}")
print("MISSING:", [s for s in SYMS if not app.rows.get(s)])
app.disconnect()
