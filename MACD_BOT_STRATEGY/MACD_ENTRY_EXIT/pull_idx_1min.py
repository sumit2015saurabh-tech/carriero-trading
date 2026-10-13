import threading, time, csv
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

SYMS = ["QQQ", "SMH", "SPY"]
ENDS = ["20260605-20:00:00", "20260529-20:00:00", "20260522-20:00:00",
        "20260515-20:00:00", "20260508-20:00:00", "20260501-20:00:00"]
OUT = "/home/azureuser/MACD_ENTRY_EXIT/hist_idx_1min.csv"

class App(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self); self.rows = {}; self.done = set(); self.idmap = {}
    def nextValidId(self, oid): pass
    def historicalData(self, reqId, bar):
        self.rows.setdefault(reqId, []).append((bar.date, bar.open, bar.high, bar.low, bar.close))
    def historicalDataEnd(self, reqId, s, e): self.done.add(reqId)
    def error(self, reqId, code, msg, *a):
        if code in (162, 165, 200, 321, 420, 366): self.done.add(reqId)

app = App(); app.connect("127.0.0.1", 4002, clientId=73)
threading.Thread(target=app.run, daemon=True).start(); time.sleep(3)

results = {}
rid = 9500
def do_pass(targets):
    global rid
    batch = []
    for sym, end in targets:
        c = Contract(); c.symbol = sym; c.secType = "STK"; c.exchange = "SMART"; c.currency = "USD"
        app.idmap[rid] = (sym, end); batch.append(rid)
        app.reqHistoricalData(rid, c, end, "1 W", "1 min", "TRADES", 1, 1, False, [])
        rid += 1; time.sleep(12)
    dl = time.time() + 90
    while time.time() < dl and not all(b in app.done for b in batch):
        time.sleep(2)
    for b in batch:
        sym, end = app.idmap[b]
        if app.rows.get(b):
            results[(sym, end)] = app.rows[b]

targets = [(s, e) for s in SYMS for e in ENDS]
for attempt in range(3):
    missing = [t for t in targets if t not in results]
    if not missing: break
    print(f"pass {attempt+1}: requesting {len(missing)} chunks ...", flush=True)
    do_pass(missing); time.sleep(5)

seen = set(); n = 0
with open(OUT, "w", newline="") as fh:
    w = csv.writer(fh); w.writerow(["symbol", "datetime", "open", "high", "low", "close"])
    for (sym, end), bars in results.items():
        for d, o, h, l, cl in bars:
            k = (sym, d)
            if k in seen: continue
            seen.add(k); w.writerow([sym, d, o, h, l, cl]); n += 1
print(f"1-min idx bars: {n} | chunks ok: {len(results)}/{len(targets)} | syms: {len(set(s for s,_ in results))}/3 -> {OUT}", flush=True)
app.disconnect()
