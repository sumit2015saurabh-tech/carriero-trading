import threading, time, csv
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

SYMS = ["AAPL","AMD","AMZN","ASTS","AVGO","CBRS","CRM","GE","GEV","GOOGL","GS","INTC",
        "IREN","KTOS","META","MRVL","MS","MSFT","NFLX","NVDA","OKLO","PLTR","QCOM","QQQ",
        "RKLB","SMCI","SMH","SPY","TGT","TSLA","TSM","TXN","VSAT","WMT"]
ENDS = ["20260529-20:00:00", "20260522-20:00:00", "20260515-20:00:00", "20260508-20:00:00", "20260501-20:00:00"]
OUT = "/home/azureuser/MACD_ENTRY_EXIT/hist_extra.csv"

class App(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self); self.rows = {}; self.done = set(); self.idmap = {}
    def nextValidId(self, oid): pass
    def historicalData(self, reqId, bar):
        self.rows.setdefault(self.idmap[reqId], []).append((bar.date, bar.open, bar.high, bar.low, bar.close))
    def historicalDataEnd(self, reqId, s, e): self.done.add(reqId)
    def error(self, reqId, code, msg, *a):
        if code in (162,165,200,321,420,366): self.done.add(reqId)
        if code not in (2104,2106,2158,2107,2103,2105,2150,2108,2119,2100,2176,2174):
            print("ERR", reqId, code, str(msg)[:50])

app = App(); app.connect("127.0.0.1", 4002, clientId=67)
threading.Thread(target=app.run, daemon=True).start(); time.sleep(2)

rid = 8500; reqs = []
for sym in SYMS:
    c = Contract(); c.symbol = sym; c.secType = "STK"; c.exchange = "SMART"; c.currency = "USD"
    for end in ENDS:
        app.idmap[rid] = sym; reqs.append(rid)
        app.reqHistoricalData(rid, c, end, "1 W", "3 mins", "TRADES", 1, 1, False, [])
        rid += 1; time.sleep(11)

deadline = time.time() + 120
while time.time() < deadline and len(app.done) < len(reqs):
    time.sleep(3)

seen = set(); n = 0
with open(OUT, "w", newline="") as fh:
    w = csv.writer(fh); w.writerow(["symbol","datetime","open","high","low","close"])
    for r in reqs:
        sym = app.idmap[r]
        for d, o, h, l, cl in app.rows.get(r, []):
            k = (sym, d)
            if k in seen: continue
            seen.add(k); w.writerow([sym, d, o, h, l, cl]); n += 1
print(f"extra bars: {n} | symbols with data: {len(set(app.idmap[r] for r in reqs if app.rows.get(r)))}/34 -> {OUT}")
app.disconnect()
