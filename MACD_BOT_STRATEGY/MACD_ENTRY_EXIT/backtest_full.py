import csv, json, sys
from collections import defaultdict

BASE = "/home/azureuser/MACD_ENTRY_EXIT"
CFG = json.load(open(f"{BASE}/config.json"))["Stocks"]
METHOD = sys.argv[1] if len(sys.argv) > 1 else "CURRENT"   # CURRENT | CHANDELIER | REGIME
K_ATR = 3.0
ER_TH = float(sys.argv[2]) if len(sys.argv) > 2 else 0.40   # efficiency-ratio threshold: >= TH = trending
ER_N = 10

def ema(series, n):
    k = 2.0/(n+1); out = []; e = None
    for x in series:
        e = x if e is None else x*k + e*(1-k)
        out.append(e)
    return out

def cfgf(sym, key, d):
    try: return float(CFG.get(sym, {}).get(key, d))
    except Exception: return d

def run_symbol(sym, bars, method):
    bars.sort(key=lambda x: x[1])
    closes = [b[5] for b in bars]; highs = [b[3] for b in bars]; lows = [b[4] for b in bars]; days = [b[0] for b in bars]
    n = len(bars)
    if n < 30: return None
    e12 = ema(closes, 12); e26 = ema(closes, 26)
    macd = [e12[i]-e26[i] for i in range(n)]
    signal = ema(macd, 9)
    tr = [highs[0]-lows[0]] + [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, n)]
    atr = [sum(tr[max(0, i-13):i+1])/len(tr[max(0, i-13):i+1]) for i in range(n)]
    er = [0.0]*n
    for i in range(ER_N, n):
        path = sum(abs(closes[j]-closes[j-1]) for j in range(i-ER_N+1, i+1))
        er[i] = abs(closes[i]-closes[i-ER_N])/path if path > 0 else 0.0
    mpl = cfgf(sym, "mpl", 0.8); qty = cfgf(sym, "quantity", 500)
    mml = cfgf(sym, "mean_multiplier_l", 1.0); mms = cfgf(sym, "mean_multiplier_s", 1.0)

    sneg = spos = 0.0; ncnt = pcnt = 0; curday = None
    pos = 0; entry = 0.0; stop = 0.0; peak = 0.0; eslope = 0.0; latched = False
    pnl = 0.0; trades = 0; wins = 0

    def close_trade(px):
        nonlocal pos, pnl, trades, wins
        p = (px - entry) * (1 if pos > 0 else -1)
        pnl += p * qty; trades += 1; wins += (1 if p > 0 else 0); pos = 0

    for i in range(n):
        if days[i] != curday:
            if pos != 0: close_trade(closes[i-1])
            curday = days[i]; sneg = spos = 0.0; ncnt = pcnt = 0
        s = signal[i]
        if s < 0: sneg = (sneg*ncnt + s)/(ncnt+1); ncnt += 1
        elif s > 0: spos = (spos*pcnt + s)/(pcnt+1); pcnt += 1
        if i < 2: continue

        if pos == 0:
            tu = macd[i] > macd[i-1]; below = macd[i] < signal[i]
            td = macd[i] < macd[i-1]; above = macd[i] > signal[i]
            xL = (macd[i-1] <= signal[i-1]) and (macd[i] > signal[i])
            xS = (macd[i-1] >= signal[i-1]) and (macd[i] < signal[i])
            gate_l = (sneg == 0.0) or (sneg >= mml*signal[i])
            gate_s = (spos == 0.0) or (spos <= mms*signal[i])
            if (tu and below) or (xL and gate_l):
                pos = 1; entry = closes[i]; peak = closes[i]; stop = entry - mpl; eslope = abs(macd[i]-macd[i-1]); latched = False
            elif (td and above) or (xS and gate_s):
                pos = -1; entry = closes[i]; peak = closes[i]; stop = entry + mpl; eslope = abs(macd[i]-macd[i-1]); latched = False
            continue

        if method == "REGIME" and er[i] >= ER_TH:
            latched = True
        use_chand = (method == "CHANDELIER") or (method == "REGIME" and latched)
        if pos > 0:
            peak = max(peak, highs[i])
            if lows[i] <= stop: close_trade(stop); continue
            delta = macd[i]-macd[i-1]; gamma = delta - (macd[i-1]-macd[i-2])
            if use_chand:
                stop = max(stop, peak - K_ATR*atr[i])
            else:
                g = peak - entry
                if (delta < 0 and gamma < 0) and (g >= 3*mpl or abs(delta) >= eslope):
                    close_trade(closes[i]); continue
                ns = (entry - mpl + 0.5*g) if g < 3*mpl else (entry + g - 2.5*mpl)
                stop = max(stop, ns)
        else:
            peak = min(peak, lows[i])
            if highs[i] >= stop: close_trade(stop); continue
            delta = macd[i]-macd[i-1]; gamma = delta - (macd[i-1]-macd[i-2])
            if use_chand:
                stop = min(stop, peak + K_ATR*atr[i])
            else:
                g = entry - peak
                if (delta > 0 and gamma > 0) and (g >= 3*mpl or abs(delta) >= eslope):
                    close_trade(closes[i]); continue
                ns = (entry + mpl - 0.5*g) if g < 3*mpl else (entry - g + 2.5*mpl)
                stop = min(stop, ns)
    if pos != 0: close_trade(closes[-1])
    return pnl, trades, (100*wins/trades if trades else 0)

rows = defaultdict(list)
for r in csv.DictReader(open(f"{BASE}/hist_7d_3min.csv")):
    dt = r["datetime"].strip()
    try:
        o, h, l, c = float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])
    except ValueError:
        continue
    rows[r["symbol"]].append((dt.split()[0], dt, o, h, l, c))

results = {}
for sym, bars in rows.items():
    res = run_symbol(sym, bars, METHOD)
    if res: results[sym] = res

print(f"=== {METHOD}  entry=pattern/cross+meangate  (7 days, real OHLC, no ML gate) ===")
total = 0.0; tt = 0
for sym in sorted(results, key=lambda s: results[s][0]):
    p, t, w = results[sym]; total += p; tt += t
    print(f"  {sym:6} {p:+9.0f}   trades={t:3}  win%={w:4.0f}")
print("-"*42)
print(f"  {'TOTAL':6} {total:+9.0f}   trades={tt}")
