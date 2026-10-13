import csv
from datetime import datetime
from zoneinfo import ZoneInfo
import ml_predict
BASE = "/home/azureuser/MACD_ENTRY_EXIT"
UTC = ZoneInfo("UTC"); NY = ZoneInfo("America/New_York")

def ema(s, n):
    k = 2.0/(n+1); o = []; e = None
    for x in s:
        e = x if e is None else x*k + e*(1-k); o.append(e)
    return o

bars = []
for r in csv.DictReader(open(f"{BASE}/hist_7d_3min.csv")):
    if r["symbol"] != "MRVL":
        continue
    p = r["datetime"].strip().split()
    de = datetime.strptime(p[0] + " " + p[1], "%Y%m%d %H:%M:%S").replace(tzinfo=UTC).astimezone(NY)
    ts = de.hour*3600 + de.minute*60 + de.second
    if 9*3600+30*60 <= ts <= 15*3600+50*60:
        bars.append((de, float(r["close"])))
bars.sort(key=lambda x: x[0])
cl = [b[1] for b in bars]
macd = [a-b for a, b in zip(ema(cl, 12), ema(cl, 26))]
sig = ema(macd, 9)
print("model ready:", ml_predict.is_ready())
print("MRVL 06-05 entry-signal bars in 11:30-14:00 EST and the ML verdict:")
for i in range(2, len(bars)):
    de = bars[i][0]
    if de.strftime("%Y%m%d") != "20260605":
        continue
    es = de.hour*3600 + de.minute*60 + de.second
    if not (11*3600+30*60 <= es < 14*3600):
        continue
    tu = macd[i] > macd[i-1] and macd[i] < sig[i]
    td = macd[i] < macd[i-1] and macd[i] > sig[i]
    xL = macd[i-1] <= sig[i-1] and macd[i] > sig[i]
    xS = macd[i-1] >= sig[i-1] and macd[i] < sig[i]
    if not (tu or td or xL or xS):
        continue
    side = "LONG" if (tu or xL) else "SHORT"
    rr = ml_predict.predict_from_macd_series(macd[i::-1], sig[i::-1], side, es)
    hhmm = de.strftime("%H:%M")
    print("  " + hhmm + " " + side + " prob_yes=" + str(rr.get("prob_yes")) + " take=" + str(rr.get("take")))
