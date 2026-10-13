import csv, json, sys
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side as BSide
import ml_predict

UTC = ZoneInfo("UTC"); NY = ZoneInfo("America/New_York")
OPEN_S = 9*3600 + 30*60          # 09:30 EST
CLOSE_S = 15*3600 + 50*60        # 15:50 EST (3:50 pm)

BASE = "/home/azureuser/MACD_ENTRY_EXIT"
CFG = json.load(open(f"{BASE}/config.json"))["Stocks"]
K_ATR = 3.0; ER_TH = 0.40; ER_N = 10
ML_LO, ML_HI = 11*3600+30*60, 14*3600        # ML gate window 11:30 - 14:00
WRITE_XLSX = "--xlsx" in sys.argv
REVERSE_MODE = "--reverse" in sys.argv      # exit purely on the opposite-direction signal
DATAFILE = next((a for a in sys.argv[1:] if a.endswith(".csv")), "hist_7d_3min.csv")
ENTRY_FILTER = "--entryfilter" in sys.argv     # only enter when trending (ER >= ER_TH)
DROP_LOSERS = "--droplosers" in sys.argv        # skip the chronic-loser names
TF15 = "--tf15" in sys.argv                      # aggregate 3-min bars to 15-min
ML_PROB = 0.65 if "--ml065" in sys.argv else 0.5 # ML take threshold
LOSERS = {"META", "GOOGL", "SMCI", "IREN", "INTC"}
AVG_ENTRY = "--avgentry" in sys.argv             # fill at (open+close)/2 of signal candle (LOOK-AHEAD)
NEXT_OPEN = "--nextopen" in sys.argv             # realistic: fill at next candle's open
NEXT_AVG = "--nextavg" in sys.argv               # fill at next candle's (open+close)/2
PAT_ZERO = "--patzero" in sys.argv               # PATTERN only: LONG if macd<0, SHORT if macd>0 (no cross)
PAT2 = "--pat2" in sys.argv                       # pattern turn measured vs 2 bars back (M_t vs M_t-2)
LIMIT_PREV = "--limitprev" in sys.argv           # rest LIMIT at (H+L)/2 of signal candle; fills next bar only if reached
SIG_MEAN = ("--sigmean" in sys.argv) or ("--sigmean2" in sys.argv)  # gate: LONG sig<k*neg-mean, SHORT sig>k*pos-mean
SIG_MULT = 2.0 if "--sigmean2" in sys.argv else 1.0                 # k = 1x or 2x the mean
CROSS_ONLY = "--crossonly" in sys.argv           # wait for the actual signal-line cross (no early pattern)
def _argval(flag, d):
    if flag in sys.argv and sys.argv.index(flag)+1 < len(sys.argv):
        try: return float(sys.argv[sys.argv.index(flag)+1])
        except ValueError: return d
    return d
TP_MULT = _argval("--tp", 0.0); SL_MULT = _argval("--sl", 0.0)       # take-profit / stop bracket in MPL units
TP_MODE = TP_MULT > 0 and SL_MULT > 0
INDEX_FILTER = "--indexfilter" in sys.argv       # only trade WITH SPY's recent direction
IDX_SYM = "SPY"; IDX_N = 3; idx_dir = {}
HYBRID = "--hybrid" in sys.argv                  # stationarity-gated exit: chop->TP bracket, trend->trailing
LP_STATS = {"fill": 0, "miss": 0}
HL2 = "--hl2" in sys.argv                         # fill at (high+low)/2 of signal candle (LOOK-AHEAD)
NEXT_HL2 = "--nexthl2" in sys.argv               # realistic: fill at next candle's (high+low)/2
LIVE_MODE = "--live" in sys.argv                 # replicate CURRENT LIVE config: pattern-only + per-symbol
                                                 # zero-line gate + mean-gate + per-stock index filter + limit-HL2 entry
index_macd_map = {}                              # stock -> {dt: index MACD}  (None = index assigned but no data e.g. NASA)
IDX1MIN = "--idx1min" in sys.argv                # compute the index filter on 1-min bars (stocks stay 3-min)
OUT = f"{BASE}/trades_regime_7d.xlsx"
MLP = ml_predict if ml_predict.is_ready() else None

def ema(series, n):
    k = 2.0/(n+1); out = []; e = None
    for x in series:
        e = x if e is None else x*k + e*(1-k); out.append(e)
    return out

def cfgf(sym, key, d):
    try: return float(CFG.get(sym, {}).get(key, d))
    except Exception: return d

def t_sec(hms):
    try: h, m, s = (list(map(int, hms.split(":"))) + [0, 0, 0])[:3]; return h*3600+m*60+s
    except Exception: return -1

def fmt_dt(dt):
    p = dt.split(); d = p[0]; t = p[1] if len(p) > 1 else ""
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}", t

def run_symbol(sym, bars):
    bars.sort(key=lambda x: x[1])
    if TF15:
        agg = {}; order = []
        for day, dt, o, h, l, c in bars:
            key = (day, int(dt[9:11]), int(dt[12:14]) // 15)
            if key not in agg:
                agg[key] = [day, dt, o, h, l, c]; order.append(key)
            else:
                a = agg[key]; a[1] = dt; a[3] = max(a[3], h); a[4] = min(a[4], l); a[5] = c
        bars = [tuple(agg[k]) for k in order]
    dts = [b[1] for b in bars]; opens = [b[2] for b in bars]; closes = [b[5] for b in bars]; highs = [b[3] for b in bars]; lows = [b[4] for b in bars]; days = [b[0] for b in bars]
    n = len(bars)
    if n < 30: return []
    e12 = ema(closes, 12); e26 = ema(closes, 26)
    macd = [e12[i]-e26[i] for i in range(n)]; signal = ema(macd, 9)
    tr = [highs[0]-lows[0]] + [max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1, n)]
    atr = [sum(tr[max(0, i-13):i+1])/len(tr[max(0, i-13):i+1]) for i in range(n)]
    er = [0.0]*n
    for i in range(ER_N, n):
        path = sum(abs(closes[j]-closes[j-1]) for j in range(i-ER_N+1, i+1))
        er[i] = abs(closes[i]-closes[i-ER_N])/path if path > 0 else 0.0
    mpl = cfgf(sym, "mpl", 0.8); qty = cfgf(sym, "quantity", 500)
    mml = cfgf(sym, "mean_multiplier_l", 1.0); mms = cfgf(sym, "mean_multiplier_s", 1.0)
    sneg = spos = 0.0; ncnt = pcnt = 0; curday = None
    pos = 0; entry = 0.0; stop = 0.0; peak = 0.0; ei = 0; eslope = 0.0; latched = False; hyb_tp = False
    trades = []

    def book(xi, xpx, reason):
        nonlocal pos
        side = "LONG" if pos > 0 else "SHORT"
        pnl = (xpx - entry) * (1 if pos > 0 else -1) * qty
        d, et = fmt_dt(dts[ei]); _, xt = fmt_dt(dts[xi])
        trades.append([d, side, et, round(entry, 2), xt, round(xpx, 2), round(pnl, 0), reason, dts[ei]])
        pos = 0

    def ml_ok(i, side):
        if MLP is None: return True
        es = t_sec(fmt_dt(dts[i])[1])
        if not (ML_LO <= es < ML_HI): return True
        try:
            r = MLP.predict_from_macd_series(macd[i::-1], signal[i::-1], side, es)
            p = r.get("prob_yes")
            return True if p is None else (p >= ML_PROB)
        except Exception:
            return True

    for i in range(n):
        if days[i] != curday:
            if pos != 0: book(i-1, closes[i-1], "End of day flat")
            curday = days[i]; sneg = spos = 0.0; ncnt = pcnt = 0
        s = signal[i]
        if s < 0: sneg = (sneg*ncnt + s)/(ncnt+1); ncnt += 1
        elif s > 0: spos = (spos*pcnt + s)/(pcnt+1); pcnt += 1
        if i < 2: continue
        if pos == 0:
            lb = 2 if PAT2 else 1
            tu = macd[i] > macd[i-lb]; below = macd[i] < signal[i]
            td = macd[i] < macd[i-lb]; above = macd[i] > signal[i]
            xL = (macd[i-1] <= signal[i-1]) and (macd[i] > signal[i])
            xS = (macd[i-1] >= signal[i-1]) and (macd[i] < signal[i])
            gl = (sneg == 0.0) or (sneg >= mml*signal[i]); gs = (spos == 0.0) or (spos <= mms*signal[i])
            long_pat = tu and below; long_cross = xL and gl
            short_pat = td and above; short_cross = xS and gs
            trend_ok = (not ENTRY_FILTER) or (er[i] >= ER_TH)
            if LIVE_MODE:
                ll = int(cfgf(sym, "macd_long_line", 3)); sln = int(cfgf(sym, "macd_short_line", 3))
                zlL = (ll == 3) or (ll == 1 and macd[i] > 0.0) or (ll == 2 and macd[i] < 0.0)
                zlS = (sln == 3) or (sln == 1 and macd[i] > 0.0) or (sln == 2 and macd[i] < 0.0)
                long_trig = long_pat and zlL and gl       # pattern + per-symbol zero gate + mean-gate
                short_trig = short_pat and zlS and gs
                idxmap = index_macd_map.get(sym, "NOFILTER")
                if idxmap != "NOFILTER":
                    if idxmap is None:                     # index assigned but no data (e.g. NASA) -> block
                        long_trig = short_trig = False
                    else:
                        # 1-min index vs sub-minute stock bars: align on the minute
                        im = idxmap.get((dts[i][:14] + ":00") if IDX1MIN else dts[i])
                        if im is None:
                            long_trig = short_trig = False
                        else:
                            long_trig = long_trig and (im > 0.0)   # index bullish -> longs only
                            short_trig = short_trig and (im < 0.0)  # index bearish -> shorts only
            elif PAT_ZERO:
                long_trig = long_pat and (macd[i] < 0.0)
                short_trig = short_pat and (macd[i] > 0.0)
            elif CROSS_ONLY:
                long_trig = long_cross
                short_trig = short_cross
            else:
                long_trig = long_pat or long_cross
                short_trig = short_pat or short_cross
            if INDEX_FILTER:
                _id = idx_dir.get(dts[i], 0)
                long_trig = long_trig and _id > 0
                short_trig = short_trig and _id < 0
            nxt = (i+1 < n and days[i+1] == days[i])
            if LIMIT_PREV or LIVE_MODE:
                lim = (highs[i]+lows[i])/2.0
                if LIVE_MODE:
                    long_ok = long_trig and ml_ok(i, "LONG")
                    short_ok = short_trig and ml_ok(i, "SHORT")
                else:
                    sig_ok_L = (not SIG_MEAN) or (sneg == 0.0) or (signal[i] < SIG_MULT*sneg)
                    sig_ok_S = (not SIG_MEAN) or (spos == 0.0) or (signal[i] > SIG_MULT*spos)
                    long_ok = long_trig and trend_ok and sig_ok_L and ml_ok(i, "LONG")
                    short_ok = short_trig and trend_ok and sig_ok_S and ml_ok(i, "SHORT")
                if long_ok:
                    if nxt and lows[i+1] <= lim:
                        LP_STATS["fill"] += 1; pos = 1; entry = lim; peak = entry; stop = entry - mpl; ei = i; eslope = abs(macd[i]-macd[i-1]); latched = long_pat
                    else:
                        LP_STATS["miss"] += 1
                elif short_ok:
                    if nxt and highs[i+1] >= lim:
                        LP_STATS["fill"] += 1; pos = -1; entry = lim; peak = entry; stop = entry + mpl; ei = i; eslope = abs(macd[i]-macd[i-1]); latched = short_pat
                    else:
                        LP_STATS["miss"] += 1
                continue
            if NEXT_OPEN or NEXT_AVG or NEXT_HL2:
                if nxt:
                    fp = opens[i+1] if NEXT_OPEN else ((opens[i+1]+closes[i+1])/2 if NEXT_AVG else (highs[i+1]+lows[i+1])/2)
                else:
                    fp = None
            elif HL2:
                fp = (highs[i]+lows[i])/2
            else:
                fp = (opens[i]+closes[i])/2 if AVG_ENTRY else closes[i]
            if fp is not None and long_trig and trend_ok and ml_ok(i, "LONG"):
                pos = 1; entry = fp; peak = entry; stop = entry - mpl; ei = i; eslope = abs(macd[i]-macd[i-1]); latched = long_pat; hyb_tp = (er[i] < ER_TH)
            elif fp is not None and short_trig and trend_ok and ml_ok(i, "SHORT"):
                pos = -1; entry = fp; peak = entry; stop = entry + mpl; ei = i; eslope = abs(macd[i]-macd[i-1]); latched = short_pat; hyb_tp = (er[i] < ER_TH)
            continue
        if TP_MODE and not HYBRID:
            if pos > 0:
                if lows[i] <= entry - SL_MULT*mpl: book(i, entry - SL_MULT*mpl, "Stop loss"); continue
                if highs[i] >= entry + TP_MULT*mpl: book(i, entry + TP_MULT*mpl, "Take profit"); continue
            else:
                if highs[i] >= entry + SL_MULT*mpl: book(i, entry + SL_MULT*mpl, "Stop loss"); continue
                if lows[i] <= entry - TP_MULT*mpl: book(i, entry - TP_MULT*mpl, "Take profit"); continue
            continue
        if HYBRID:
            if er[i] >= ER_TH:                       # trade broke into a trend -> ride it
                hyb_tp = False; latched = True
            if hyb_tp:                               # still choppy -> quick bracket scalp
                if pos > 0:
                    if lows[i] <= entry - SL_MULT*mpl: book(i, entry - SL_MULT*mpl, "Stop loss (chop)"); continue
                    if highs[i] >= entry + TP_MULT*mpl: book(i, entry + TP_MULT*mpl, "Take profit (chop)"); continue
                else:
                    if highs[i] >= entry + SL_MULT*mpl: book(i, entry + SL_MULT*mpl, "Stop loss (chop)"); continue
                    if lows[i] <= entry - TP_MULT*mpl: book(i, entry - TP_MULT*mpl, "Take profit (chop)"); continue
                continue
        if er[i] >= ER_TH: latched = True
        if REVERSE_MODE:
            if pos > 0:
                if lows[i] <= stop: book(i, stop, "Stop loss"); continue
                rev = (macd[i] < macd[i-1] and macd[i] > signal[i]) or (macd[i-1] >= signal[i-1] and macd[i] < signal[i])
                if rev: book(i, closes[i], "Reverse signal exit"); continue
            else:
                if highs[i] >= stop: book(i, stop, "Stop loss"); continue
                rev = (macd[i] > macd[i-1] and macd[i] < signal[i]) or (macd[i-1] <= signal[i-1] and macd[i] > signal[i])
                if rev: book(i, closes[i], "Reverse signal exit"); continue
            continue
        if pos > 0:
            peak = max(peak, highs[i])
            if lows[i] <= stop:
                book(i, stop, "ATR trail stop" if latched else ("MPL trail stop" if stop > entry else "Stop loss")); continue
            delta = macd[i]-macd[i-1]; gamma = delta - (macd[i-1]-macd[i-2])
            if latched:
                stop = max(stop, peak - K_ATR*atr[i])
            else:
                g = peak - entry
                if (delta < 0 and gamma < 0) and (g >= 3*mpl or abs(delta) >= eslope):
                    book(i, closes[i], "Delta/gamma exit"); continue
                ns = (entry - mpl + 0.5*g) if g < 3*mpl else (entry + g - 2.5*mpl)
                stop = max(stop, ns)
        else:
            peak = min(peak, lows[i])
            if highs[i] >= stop:
                book(i, stop, "ATR trail stop" if latched else ("MPL trail stop" if stop < entry else "Stop loss")); continue
            delta = macd[i]-macd[i-1]; gamma = delta - (macd[i-1]-macd[i-2])
            if latched:
                stop = min(stop, peak + K_ATR*atr[i])
            else:
                g = entry - peak
                if (delta > 0 and gamma > 0) and (g >= 3*mpl or abs(delta) >= eslope):
                    book(i, closes[i], "Delta/gamma exit"); continue
                ns = (entry + mpl - 0.5*g) if g < 3*mpl else (entry - g + 2.5*mpl)
                stop = min(stop, ns)
    if pos != 0: book(n-1, closes[-1], "End of day flat")
    return trades

rows = defaultdict(list)
for r in csv.DictReader(open(f"{BASE}/{DATAFILE}")):
    if DROP_LOSERS and r["symbol"] in LOSERS:
        continue
    raw = r["datetime"].strip()
    try:
        o, h, l, c = float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])
        p = raw.split()
        du = datetime.strptime(p[0] + " " + p[1], "%Y%m%d %H:%M:%S").replace(tzinfo=UTC)
    except (ValueError, IndexError):
        continue
    de = du.astimezone(NY)                          # UTC -> EST/EDT
    tsec = de.hour*3600 + de.minute*60 + de.second
    if not (OPEN_S <= tsec <= CLOSE_S):             # keep only 09:30 - 15:50 EST
        continue
    est = de.strftime("%Y%m%d %H:%M:%S")
    rows[r["symbol"]].append((de.strftime("%Y%m%d"), est, o, h, l, c))

if INDEX_FILTER and IDX_SYM in rows:
    _iser = sorted(rows[IDX_SYM], key=lambda x: x[1])
    _icl = [b[5] for b in _iser]; _idt = [b[1] for b in _iser]
    for j in range(IDX_N, len(_iser)):
        d = _icl[j] - _icl[j-IDX_N]
        idx_dir[_idt[j]] = 1 if d > 0 else (-1 if d < 0 else 0)
if LIVE_MODE:
    _idx_macd = {}
    _idxset = set(str(CFG.get(s, {}).get("index", "")).upper() for s in CFG if str(CFG.get(s, {}).get("index", "")).strip())
    _idxrows = rows                                     # default: index MACD from the 3-min stock data
    if IDX1MIN:                                         # load 1-min index bars (same UTC->EST clip)
        _idxrows = defaultdict(list)
        for r in csv.DictReader(open(f"{BASE}/hist_idx_1min.csv")):
            try:
                o2, h2, l2, c2 = float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])
                p2 = r["datetime"].strip().split()
                du2 = datetime.strptime(p2[0] + " " + p2[1], "%Y%m%d %H:%M:%S").replace(tzinfo=UTC)
            except (ValueError, IndexError):
                continue
            de2 = du2.astimezone(NY); ts2 = de2.hour*3600 + de2.minute*60 + de2.second
            if not (OPEN_S <= ts2 <= CLOSE_S):
                continue
            _idxrows[r["symbol"]].append((de2.strftime("%Y%m%d"), de2.strftime("%Y%m%d %H:%M:%S"), o2, h2, l2, c2))
    for _ix in _idxset:
        if _ix in _idxrows:
            _ib = sorted(_idxrows[_ix], key=lambda x: x[1])
            _icl2 = [b[5] for b in _ib]; _idt2 = [b[1] for b in _ib]
            _e12 = ema(_icl2, 12); _e26 = ema(_icl2, 26)
            _idx_macd[_ix] = {_idt2[j]: _e12[j]-_e26[j] for j in range(len(_ib))}
        else:
            _idx_macd[_ix] = None                       # index symbol absent from data (e.g. NASA)
    for s in CFG:
        _ix = str(CFG.get(s, {}).get("index", "")).strip().upper()
        if _ix:
            index_macd_map[s] = _idx_macd.get(_ix)      # dict, or None if no data; absent => no filter
    _navail = sorted(k for k, v in _idx_macd.items() if v is not None)
    _nblock = sorted(k for k, v in _idx_macd.items() if v is None)
    print(f"LIVE MODE: index data available {_navail} | blocked (no data) {_nblock}")
_universe = None
if "--universe" in sys.argv and sys.argv.index("--universe")+1 < len(sys.argv):
    _universe = set(s.strip().upper() for s in sys.argv[sys.argv.index("--universe")+1].split(","))
    print(f"UNIVERSE restricted to {len(_universe)} symbols: {sorted(_universe)}")
allsym = {sym: run_symbol(sym, rows[sym]) for sym in sorted(rows) if (_universe is None or sym in _universe)}
grand = sum(sum(t[6] for t in tr) for tr in allsym.values())
ntr = sum(len(tr) for tr in allsym.values())
from collections import Counter
reasons = Counter(t[7] for tr in allsym.values() for t in tr)
print(f"ML gate: {'ON' if MLP else 'OFF'}  | regime: ER>={ER_TH} latched | k_ATR={K_ATR}")
print(f"TOTAL: {grand:+.0f}  | trades: {ntr}")
_pnls = [t[6] for tr in allsym.values() for t in tr]
_w = [p for p in _pnls if p > 0]; _l = [p for p in _pnls if p < 0]
_aw = (sum(_w)/len(_w)) if _w else 0; _al = (sum(_l)/len(_l)) if _l else 0
print(f"WIN%: {100*len(_w)/max(1,len(_pnls)):.1f}%  ({len(_w)}W / {len(_l)}L)  | avg win {_aw:+.0f} | avg loss {_al:+.0f} | payoff {(_aw/-_al if _al else 0):.2f}")
if LIMIT_PREV or LIVE_MODE:
    _ts = LP_STATS["fill"] + LP_STATS["miss"]
    print(f"LIMIT FILL RATE: {100*LP_STATS['fill']/max(1,_ts):.1f}%  ({LP_STATS['fill']} filled / {LP_STATS['miss']} missed of {_ts} signals)")
print("exit reasons:", dict(reasons))
daily = defaultdict(float)
for tr in allsym.values():
    for t in tr:
        daily[t[0]] += t[6]
dates = sorted(daily)
equity = peak = maxdd = 0.0; peak_d = trough_d = cur_peak_d = None
for d in dates:
    equity += daily[d]
    if equity > peak: peak = equity; cur_peak_d = d
    if peak - equity > maxdd: maxdd = peak - equity; peak_d = cur_peak_d; trough_d = d
worst = min(dates, key=lambda d: daily[d]) if dates else None
best = max(dates, key=lambda d: daily[d]) if dates else None
print()
print("=== DRAWDOWN ANALYSIS ===")
print(f"trading days: {len(dates)} ({dates[0]} to {dates[-1]})  | cumulative P&L: {equity:+.0f}")
print(f"MAX SINGLE-DAY DRAWDOWN: {daily[worst]:+.0f}  on {worst}")
print(f"MAX TOTAL DRAWDOWN (peak-to-trough): {-maxdd:+.0f}  (peak {peak_d} -> trough {trough_d})")
print(f"best single day: {daily[best]:+.0f} on {best}")
print("worst 5 days:", [(d, round(daily[d])) for d in sorted(dates, key=lambda d: daily[d])[:5]])
print("per stock:")
for sym in sorted(allsym, key=lambda s: sum(t[6] for t in allsym[s])):
    p = sum(t[6] for t in allsym[sym]); print(f"  {sym:6} {p:+8.0f}  ({len(allsym[sym])})")

if "--csv" in sys.argv:
    OUTCSV = f"{BASE}/trades_regime_7d.csv"
    allrows = []
    for sym in allsym:
        for t in allsym[sym]:
            allrows.append(([sym] + t[:8], t[8]))
    allrows.sort(key=lambda x: (x[0][0], x[1]))
    with open(OUTCSV, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["Stock", "Date", "Side", "Entry Time", "Entry Price", "Exit Time", "Exit Price", "P&L ($)", "Exit Reason"])
        for row, _ in allrows:
            w.writerow(row)
    print("Saved", OUTCSV, "rows:", len(allrows))

if WRITE_XLSX:
    wb = Workbook(); wb.remove(wb.active)
    hdr = ["Date", "Side", "Entry Time", "Entry Price", "Exit Time", "Exit Price", "P&L ($)", "Exit Reason"]
    hf = PatternFill("solid", fgColor="1F4E78"); hfont = Font(bold=True, color="FFFFFF")
    thin = BSide(style="thin", color="BFBFBF"); bd = Border(left=thin, right=thin, top=thin, bottom=thin)
    ctr = Alignment(horizontal="center", vertical="center")
    for sym in sorted(allsym):
        tr = sorted(allsym[sym], key=lambda x: x[8])
        ws = wb.create_sheet(sym[:31])
        for c, h in enumerate(hdr, 1):
            cell = ws.cell(row=1, column=c, value=h); cell.fill = hf; cell.font = hfont; cell.alignment = ctr; cell.border = bd
        tot = 0.0
        for ri, t in enumerate(tr, start=2):
            for c, v in enumerate(t[:8], 1):
                cell = ws.cell(row=ri, column=c, value=v); cell.alignment = ctr; cell.border = bd
            tot += t[6]
        last = len(tr) + 2
        ws.cell(row=last, column=6, value="TOTAL").font = Font(bold=True)
        ws.cell(row=last, column=7, value=round(tot, 0)).font = Font(bold=True)
        for col, w in {"A":12,"B":7,"C":11,"D":12,"E":11,"F":11,"G":11,"H":20}.items():
            ws.column_dimensions[col].width = w
        ws.freeze_panes = "A2"
    wb.save(OUT)
    print("Saved", OUT)
