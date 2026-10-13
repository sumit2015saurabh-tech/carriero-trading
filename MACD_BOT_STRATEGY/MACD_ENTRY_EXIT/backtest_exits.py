#!/usr/bin/env python3
"""
backtest_exits.py — compare EXIT strategies on today's real bars.

Entry rule (common to ALL methods, so only the EXIT differs): a MACD
signal-line cross — LONG when macd crosses above signal, SHORT when below.
One position at a time per symbol; re-arm on the next cross; flat at EoD.

Exit methods compared:
  CURRENT   = the live bot: initial stop (entry∓mpl), breakeven (entry±mpl*bem
              → stop entry±mpl*bem*0.5), MPL/4 trail (locks 25% of peak gain),
              and the delta/gamma tech exit (close when fast line rolls over).
  CHANDELIER= initial stop, then trail = peakClose ∓ k*ATR (ratchets only).
  EMA9      = initial stop, exit when close crosses back through EMA9.
  ER_CHAND  = chandelier when entry-bar Efficiency Ratio>0.5 (trend), else CURRENT.
  SPLIT     = half CURRENT + half ER_CHAND  (= average of the two).

CAVEAT: the CSV logs CLOSE only (no high/low). ATR and breakeven 'high' use
close-based proxies (ATR proxy = 1.5 * mean|Δclose| over 14). All methods use
the SAME close-only basis, so the comparison is apples-to-apples; absolute $
would shift a little with true high/low, but the ranking is informative.
"""
import csv, json, sys, math

CSV   = sys.argv[1] if len(sys.argv) > 1 else "price_macd_log_04-06-26.csv"
K_ATR = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
CFG   = json.load(open("config.json"))["Stocks"]

def cfgf(sym, key, default):
    try: return float(CFG.get(sym, {}).get(key, default))
    except Exception: return default

# ---- load + group rows by symbol (3min entry bars, seeded+ready, chronological)
rows = {}
for r in csv.DictReader(open(CSV)):
    if r.get("resolution") != "3min": continue
    if str(r.get("ready")).strip() != "True": continue
    try:
        c = float(r["close"]); m = float(r["macd"]); s = float(r["signal"])
    except Exception:
        continue
    rows.setdefault(r["symbol"], []).append((r.get("bar_time",""), c, m, s))

def ema(series, n):
    k = 2/(n+1); out=[]; e=None
    for x in series:
        e = x if e is None else x*k + e*(1-k)
        out.append(e)
    return out

def atr_proxy(closes, n=14):
    tr=[0.0]+[abs(closes[i]-closes[i-1]) for i in range(1,len(closes))]
    out=[];
    for i in range(len(tr)):
        w=tr[max(0,i-n+1):i+1]; out.append(1.5*sum(w)/len(w))   # *1.5 ≈ true ATR
    return out

def eff_ratio(closes, i, n=10):
    if i < n: return 0.0
    chg = abs(closes[i]-closes[i-n])
    vol = sum(abs(closes[j]-closes[j-1]) for j in range(i-n+1,i+1))
    return chg/vol if vol>0 else 0.0

def run_symbol(sym, data, method):
    closes=[d[1] for d in data]; macd=[d[2] for d in data]; sig=[d[3] for d in data]
    N=len(closes)
    if N < 5: return None
    e9=ema(closes,9); atr=atr_proxy(closes)
    mpl=cfgf(sym,"mpl",cfgf(sym,"MPL_dollar_Amount",0.6))
    bem=cfgf(sym,"bem",0.5); qty=cfgf(sym,"quantity",100)
    trades=[]; pos=None
    i=1
    while i < N:
        cu = macd[i]>sig[i] and macd[i-1]<=sig[i-1]
        cd = macd[i]<sig[i] and macd[i-1]>=sig[i-1]
        if pos is None and (cu or cd):
            side = 1 if cu else -1
            meth = method
            if method=="ER_CHAND":
                meth = "CHANDELIER" if eff_ratio(closes,i)>0.5 else "CURRENT"
            pos = dict(side=side, entry=closes[i], i0=i, meth=meth,
                       stop=closes[i]-side*mpl, be=False, peak=closes[i])
            i+=1; continue
        if pos:
            side=pos["side"]; entry=pos["entry"]; meth=pos["meth"]; c=closes[i]
            exit_px=None
            # hard stop (close-basis, same for all)
            if (side==1 and c<=pos["stop"]) or (side==-1 and c>=pos["stop"]):
                exit_px=pos["stop"]
            if exit_px is None and meth=="CURRENT":
                # breakeven + MPL/4 trail
                if not pos["be"]:
                    bt = entry + side*mpl*bem
                    if (side==1 and c>bt) or (side==-1 and c<bt):
                        pos["be"]=True; pos["stop"]=entry+side*mpl*bem*0.5
                if pos["be"]:
                    pos["peak"]= max(pos["peak"],c) if side==1 else min(pos["peak"],c)
                    ns=entry+(pos["peak"]-entry)*0.25
                    pos["stop"]= max(pos["stop"],ns) if side==1 else min(pos["stop"],ns)
                # delta/gamma tech exit
                if i>=2:
                    d=macd[i]-macd[i-1]; dp=macd[i-1]-macd[i-2]; g=d-dp
                    if (side==1 and d<0 and g<0) or (side==-1 and d>0 and g>0):
                        exit_px=c
            elif exit_px is None and meth=="CHANDELIER":
                pos["peak"]= max(pos["peak"],c) if side==1 else min(pos["peak"],c)
                ch = pos["peak"] - side*K_ATR*atr[i]
                pos["stop"]= max(pos["stop"],ch) if side==1 else min(pos["stop"],ch)
                if (side==1 and c<=pos["stop"]) or (side==-1 and c>=pos["stop"]):
                    exit_px=pos["stop"]
            elif exit_px is None and meth=="EMA9":
                if (side==1 and c<e9[i]) or (side==-1 and c>e9[i]):
                    exit_px=c
            if exit_px is None and i==N-1:        # EoD flat
                exit_px=c
            if exit_px is not None:
                trades.append(side*(exit_px-entry)); pos=None
        i+=1
    pnl_sh=sum(trades); wins=sum(1 for t in trades if t>0)
    return dict(sym=sym, n=len(trades), winpct=(100*wins/len(trades) if trades else 0),
                pnl_sh=pnl_sh, pnl_usd=pnl_sh*qty)

METHODS=["CURRENT","CHANDELIER","EMA9","ER_CHAND"]
syms=sorted(rows.keys())
agg={m:0.0 for m in METHODS}; agg["SPLIT"]=0.0
per={}
for sym in syms:
    per[sym]={}
    for m in METHODS:
        r=run_symbol(sym, rows[sym], m)
        if r: per[sym][m]=r; agg[m]+=r["pnl_usd"]
    if "CURRENT" in per[sym] and "ER_CHAND" in per[sym]:
        sp=0.5*per[sym]["CURRENT"]["pnl_usd"]+0.5*per[sym]["ER_CHAND"]["pnl_usd"]
        per[sym]["SPLIT"]=sp; agg["SPLIT"]+=sp

def line(sym, d):
    def cell(m):
        if m=="SPLIT": return f"{d.get('SPLIT',0):+8.0f}"
        r=d.get(m); return f"{r['pnl_usd']:+8.0f}" if r else f"{'-':>8}"
    return f"{sym:6} " + " ".join(cell(m) for m in ["CURRENT","CHANDELIER","EMA9","ER_CHAND","SPLIT"])

print(f"\n=== EXIT BACKTEST  ({CSV}, k_ATR={K_ATR})  — $ P&L per symbol ===")
print(f"{'SYM':6} {'CURRENT':>8} {'CHANDLR':>8} {'EMA9':>8} {'ER_CHAND':>8} {'SPLIT':>8}")
# show NVDA first, then biggest movers
order=["NVDA"]+[s for s in syms if s!="NVDA"]
for sym in order:
    if sym in per and per[sym].get("CURRENT"): print(line(sym, per[sym]))
print("-"*52)
print(f"{'TOTAL':6} " + " ".join(f"{agg[m]:+8.0f}" for m in ["CURRENT","CHANDELIER","EMA9","ER_CHAND","SPLIT"]))

# NVDA detail
if "NVDA" in per and per["NVDA"].get("CURRENT"):
    print("\n=== NVDA detail (trades / win%) ===")
    for m in ["CURRENT","CHANDELIER","EMA9","ER_CHAND"]:
        r=per["NVDA"].get(m)
        if r: print(f"  {m:11} trades={r['n']:2}  win%={r['winpct']:5.1f}  $P&L={r['pnl_usd']:+.0f}")
