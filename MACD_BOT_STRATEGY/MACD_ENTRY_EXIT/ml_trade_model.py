#!/usr/bin/env python3
"""
ml_trade_model.py  (2026-06-05 relabeling, per user)

Trains a take-or-skip classifier from the labelled sheets.

Labeling rule (per user, 2026-06-05):
  TRADE sheets  (*_Trade.xlsx, the placed trades): label = 1 only when the
        Entry decision cell starts with "yes".  Anything else (No, Maybe,
        blank) = the trade was wrongly placed and should have been blocked = 0.
  BLOCKED sheets (BLOCKED_TRADE_*.xlsx, the gate skips): label = 0 when the
        ENTRY cell is exactly "no" (correctly blocked).  Anything else (the
        "f" marks) = the trade was wrongly blocked and should have fired = 1.

Features = the MACD/indicator setup at each entry, from price_macd_log_<date>.csv.
For blocked rows the side (LONG/SHORT) is recovered from the engine log
tws_<date>.txt; if not found it falls back to the sign of the histogram.
Saves trade_ml_model.joblib for live prediction.
"""
import numpy as np, pandas as pd
from openpyxl import load_workbook
from sklearn.model_selection import StratifiedKFold, cross_val_score, cross_val_predict
from sklearn.ensemble import (HistGradientBoostingClassifier, RandomForestClassifier,
                              GradientBoostingClassifier, ExtraTreesClassifier)
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import confusion_matrix, classification_report
import joblib, glob, os, re

BASE = "/home/azureuser/MACD_ENTRY_EXIT"

def _trade_logcsv(name):
    if name == "Todays_Trade.xlsx":
        return "price_macd_log_02-06-26.csv"
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})_Trade\.xlsx$", name)
    if m:
        y, mo, d = m.groups()
        return f"price_macd_log_{d}-{mo}-{y[2:]}.csv"
    return None

def _blocked_logs(name):
    m = re.match(r"BLOCKED_TRADE_(\d{4})-(\d{2})-(\d{2})\.xlsx$", name)
    if not m:
        return None, None
    y, mo, d = m.groups()
    return f"price_macd_log_{d}-{mo}-{y[2:]}.csv", f"tws_{d}_{mo}_{y[2:]}.txt"

def discover_trade():
    out = []
    names = set(os.path.basename(p) for p in glob.glob(f"{BASE}/*_Trade.xlsx"))
    names.add("Todays_Trade.xlsx")
    for name in sorted(names):
        logcsv = _trade_logcsv(name)
        if logcsv and os.path.exists(f"{BASE}/{logcsv}") and os.path.exists(f"{BASE}/{name}"):
            out.append((name, logcsv, name.replace("_Trade.xlsx", "").replace(".xlsx", "")))
    return out

def discover_blocked():
    out = []
    for p in sorted(glob.glob(f"{BASE}/BLOCKED_TRADE_*.xlsx")):
        name = os.path.basename(p)
        logcsv, twslog = _blocked_logs(name)
        if logcsv and os.path.exists(f"{BASE}/{logcsv}"):
            out.append((name, logcsv, twslog, "B_" + name.replace("BLOCKED_TRADE_", "").replace(".xlsx", "")))
    return out

def _entry_decision_col(ws):
    hdr = [str(ws.cell(row=1, column=c).value or "").lower() for c in range(1, 14)]
    for c, h in enumerate(hdr, 1):
        if "entry" in h and ("y/n" in h or ("yes" in h and "maybe" in h)):
            return c
    for c, h in enumerate(hdr, 1):
        if "yes" in h and "maybe" in h:
            return c
    return 5

FEATURES = ["macd","signal","hist","ema_diff","macd_d1","macd_d2","signal_d1",
            "hist_d1","macd_lag1","macd_lag2","gap","macd_abs","macd_sign",
            "close_ret","side_long","mins_since_open","nbars"]

def trade_label(v):
    return 1 if str(v).strip().lower().startswith("yes") else 0

def blocked_label(v):
    s = str(v).strip().lower()
    return 1 if (s and s != "no" and s != "none") else 0

def t_sec(hms):
    h, m, s = (list(map(int, str(hms).split(":"))) + [0, 0, 0])[:3]
    return h*3600 + m*60 + s

def logsec(wc):
    try:
        tt = str(wc).split("T")[1][:8]; h, m, s = map(int, tt.split(":")); return h*3600+m*60+s
    except Exception:
        return -1

def lag(a, i):
    return a[-1-i] if len(a) > i else (a[0] if len(a) else 0.0)

def features_for(symdf, entry_sec, side):
    df = symdf[symdf["t"] <= entry_sec]
    if len(df) == 0:
        return None
    df = df.tail(6)
    macd = df["macd"].to_numpy(float); signal = df["signal"].to_numpy(float)
    ef = df["ema_fast"].to_numpy(float); es = df["ema_slow"].to_numpy(float)
    close = df["close"].to_numpy(float)
    m0, m1, m2 = lag(macd,0), lag(macd,1), lag(macd,2)
    s0, s1 = lag(signal,0), lag(signal,1)
    h0, h1 = m0-s0, m1-s1
    return {
        "macd": m0, "signal": s0, "hist": h0,
        "ema_diff": lag(ef,0)-lag(es,0),
        "macd_d1": m0-m1, "macd_d2": (m0-m1)-(m1-m2),
        "signal_d1": s0-s1, "hist_d1": h0-h1,
        "macd_lag1": m1, "macd_lag2": m2,
        "gap": abs(m0-s0), "macd_abs": abs(m0), "macd_sign": float(np.sign(m0)),
        "close_ret": (lag(close,0)/lag(close,1)-1.0) if lag(close,1) else 0.0,
        "side_long": 1.0 if str(side).upper()=="LONG" else 0.0,
        "mins_since_open": max(0.0, (entry_sec-(9*3600+30*60))/60.0),
        "nbars": float(len(df)),
    }

def build_side_map(twslog):
    smap = {}
    pat = re.compile(r"(\d\d:\d\d:\d\d),\d+ .*\[(\w+)\] \*\*\* (LONG|SHORT) SIGNAL \*\*\*")
    try:
        for line in open(f"{BASE}/{twslog}"):
            mm = pat.search(line)
            if mm:
                smap[(mm.group(2), mm.group(1))] = mm.group(3)
    except FileNotFoundError:
        pass
    return smap

def side_lookup(smap, sym, hms, hist):
    if (sym, hms) in smap:
        return smap[(sym, hms)]
    base = t_sec(hms)
    for d in (1, -1, 2, -2, 3, -3):
        h = base + d; k = (sym, f"{h//3600:02d}:{(h%3600)//60:02d}:{h%60:02d}")
        if k in smap:
            return smap[k]
    return "LONG" if hist >= 0 else "SHORT"

rows = []; skipped = 0

for xlsx, logcsv, tag in discover_trade():
    wb = load_workbook(f"{BASE}/{xlsx}"); ws = wb.active
    dec_col = _entry_decision_col(ws)
    log = pd.read_csv(f"{BASE}/{logcsv}")
    log["t"] = log["wall_clock_est"].map(logsec)
    log = log[log["t"] >= 0].sort_values("t")
    by_sym = {s: g for s, g in log.groupby("symbol")}
    for r in range(2, ws.max_row+1):
        sym  = ws.cell(row=r, column=1).value
        side = ws.cell(row=r, column=2).value
        et   = ws.cell(row=r, column=3).value
        if not sym or not et:
            skipped += 1; continue
        sdf = by_sym.get(sym)
        if sdf is None:
            skipped += 1; continue
        f = features_for(sdf, t_sec(et), side)
        if f is None:
            skipped += 1; continue
        f["label"] = trade_label(ws.cell(row=r, column=dec_col).value); f["_tag"] = tag
        rows.append(f)

for xlsx, logcsv, twslog, tag in discover_blocked():
    wb = load_workbook(f"{BASE}/{xlsx}"); ws = wb.active
    log = pd.read_csv(f"{BASE}/{logcsv}")
    log["t"] = log["wall_clock_est"].map(logsec)
    log = log[log["t"] >= 0].sort_values("t")
    by_sym = {s: g for s, g in log.groupby("symbol")}
    smap = build_side_map(twslog)
    for r in range(2, ws.max_row+1):
        sym = ws.cell(row=r, column=1).value
        et  = ws.cell(row=r, column=2).value
        if not sym or not et:
            skipped += 1; continue
        sdf = by_sym.get(sym)
        if sdf is None:
            skipped += 1; continue
        probe = features_for(sdf, t_sec(et), "LONG")
        if probe is None:
            skipped += 1; continue
        side = side_lookup(smap, str(sym), str(et).strip(), probe["hist"])
        f = features_for(sdf, t_sec(et), side)
        f["label"] = blocked_label(ws.cell(row=r, column=4).value); f["_tag"] = tag
        rows.append(f)

data = pd.DataFrame(rows)
print(f"=== dataset: {len(data)} samples (skipped {skipped} with no feature match) ===")
if len(data) < 30 or data["label"].nunique() < 2:
    print(f"INSUFFICIENT data ({len(data)} rows) - keeping existing model.")
    raise SystemExit(0)
print("label counts:", data["label"].value_counts().to_dict())
print("by day:")
print(data.groupby("_tag")["label"].agg(["count", "mean"]))

X = data[FEATURES].fillna(0.0).to_numpy(float)
y = data["label"].to_numpy(int)

models = {
 "LogReg":       Pipeline([("sc", StandardScaler()), ("m", LogisticRegression(max_iter=2000, class_weight="balanced"))]),
 "RandomForest": RandomForestClassifier(n_estimators=500, class_weight="balanced", random_state=42, max_depth=6, min_samples_leaf=4, n_jobs=-1),
 "ExtraTrees":   ExtraTreesClassifier(n_estimators=500, class_weight="balanced", random_state=42, max_depth=7, min_samples_leaf=4, n_jobs=-1),
 "GBM":          GradientBoostingClassifier(random_state=42, max_depth=2, n_estimators=200, learning_rate=0.05, subsample=0.8),
 "HistGBM":      HistGradientBoostingClassifier(random_state=42, max_depth=3, learning_rate=0.05, max_iter=300, l2_regularization=1.0, class_weight="balanced"),
}
cv = StratifiedKFold(5, shuffle=True, random_state=42)
print("\n=== 5-fold stratified CV ===")
best = None; best_auc = -1
for name, mdl in models.items():
    auc = cross_val_score(mdl, X, y, cv=cv, scoring="roc_auc").mean()
    acc = cross_val_score(mdl, X, y, cv=cv, scoring="accuracy").mean()
    f1  = cross_val_score(mdl, X, y, cv=cv, scoring="f1").mean()
    print(f"  {name:13} AUC={auc:.3f}  ACC={acc:.3f}  F1={f1:.3f}")
    if auc > best_auc:
        best_auc, best = auc, (name, mdl)

name, mdl = best
print(f"\nBEST MODEL: {name}  (CV AUC={best_auc:.3f})")
pred = cross_val_predict(mdl, X, y, cv=cv)
print("CV confusion [[TN FP][FN TP]]:\n", confusion_matrix(y, pred))
print(classification_report(y, pred, target_names=["No(skip)", "Yes(take)"]))

final = best[1]
final.fit(X, y)
joblib.dump({"model": final, "features": FEATURES, "model_name": name, "cv_auc": float(best_auc)},
            f"{BASE}/trade_ml_model.joblib")
print(f"\nSaved -> {BASE}/trade_ml_model.joblib  (model={name})")
