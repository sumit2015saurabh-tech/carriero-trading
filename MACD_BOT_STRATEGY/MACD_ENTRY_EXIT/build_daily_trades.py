#!/usr/bin/env python3
"""
build_daily_trades.py — run at EoD (cron). Parses TODAY's engine log into round
trips (entry+exit) and writes <YYYY-MM-DD>_Trade.xlsx with Y/N/Maybe dropdowns
for BOTH the entry and the exit. The ML retrainer reads the ENTRY column.
"""
import re, datetime, sys
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side as BSide
from openpyxl.worksheet.datavalidation import DataValidation

BASE  = "/home/azureuser/MACD_ENTRY_EXIT"
# optional arg YYYY-MM-DD for backfill/test; default = today (VM tz America/New_York)
if len(sys.argv) > 1:
    today = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d")
else:
    today = datetime.datetime.now()
L     = f"{BASE}/logs/tws_{today.strftime('%d_%m_%y')}.txt"
OUT   = f"{BASE}/{today.strftime('%Y-%m-%d')}_Trade.xlsx"

fillpat = re.compile(r"(\d\d:\d\d:\d\d),\d+ .*TWS FILL: oid=(\d+)\s+symbol=(\w+)\s+side=(BOT|SLD)\s+qty=(\d+)\s+price=([\d.]+)")
fills = {}
try:
    for line in open(L):
        m = fillpat.search(line)
        if m:
            t, oid, sym, side, qty, px = m.group(1), m.group(2), m.group(3), m.group(4), int(m.group(5)), float(m.group(6))
            fills.setdefault(sym, []).append((t, side, qty, px))
except FileNotFoundError:
    print(f"no log {L} — nothing to write"); raise SystemExit(0)

def wavg(lst):
    tq = sum(q for _, q, _ in lst)
    return (sum(q * p for _, q, p in lst) / tq, tq) if tq else (0, 0)

trips = []  # (entry_t, side, entry_px, exit_t, exit_px)
for sym, fl in fills.items():
    pos = 0; od = 0; en = []; ex = []
    for t, side, qty, px in fl:
        s = qty if side == "BOT" else -qty
        if pos == 0:
            od = 1 if s > 0 else -1; en = [(t, qty, px)]; ex = []; pos = s; continue
        if (s > 0) == (od > 0):
            en.append((t, qty, px)); pos += s
        else:
            cq = min(abs(s), abs(pos)); ex.append((t, cq, px)); pos += (-cq if od > 0 else cq)
            if pos == 0:
                ea, _ = wavg(en); xa, _ = wavg(ex)
                trips.append((sym, en[0][0], "LONG" if od > 0 else "SHORT", round(ea, 4), ex[-1][0], round(xa, 4)))
                en = []; ex = []; rem = abs(s) - cq
                if rem > 0:
                    od = 1 if s > 0 else -1; en = [(t, rem, px)]; pos = (rem if s > 0 else -rem)
    if pos != 0 and en:                                # still-open leg (no exit yet)
        ea, _ = wavg(en)
        trips.append((sym, en[0][0], "LONG" if od > 0 else "SHORT", round(ea, 4), "", ""))

trips.sort(key=lambda r: r[1])

# 2026-06-04: (a) PRESERVE existing labels (so the EoD regen never wipes labels
# you've entered), keyed by (symbol, entry time). (b) AUTO-FILL the bot's own
# ML-gate decision in the Entry column for entries in the 11:30-14:00 window —
# every entry that fired in that window passed the gate (=Yes); the NO/skipped
# signals never became trades. You can override any cell (it's a dropdown).
import os
existing = {}
if os.path.exists(OUT):
    try:
        from openpyxl import load_workbook as _lw
        _ws = _lw(OUT).active
        for r in range(2, _ws.max_row + 1):
            s = _ws.cell(row=r, column=1).value; t = _ws.cell(row=r, column=3).value
            if s and t:
                existing[(str(s), str(t))] = (_ws.cell(row=r, column=7).value,
                                              _ws.cell(row=r, column=8).value)
    except Exception:
        pass

def _esec(hms):
    try:
        h, m, s = (list(map(int, str(hms).split(":"))) + [0, 0, 0])[:3]; return h*3600 + m*60 + s
    except Exception:
        return -1
_WLO, _WHI = 11*3600 + 30*60, 14*3600        # ML-gate window 11:30-14:00 ET

def _labels(sym, et):
    ex = existing.get((str(sym), str(et)))
    eg  = ex[0] if ex and ex[0] not in (None, "") else ""
    ex2 = ex[1] if ex and ex[1] not in (None, "") else ""
    if not eg and _WLO <= _esec(et) < _WHI:
        eg = "Yes"                            # bot's ML gate approved this entry
    return eg, ex2

wb = Workbook(); ws = wb.active; ws.title = today.strftime("%Y-%m-%d")
headers = ["Stock", "Side", "Entry Time", "Entry Price", "Exit Time", "Exit Price",
           "Entry Y/N/Maybe", "Exit Y/N/Maybe"]
hf = PatternFill("solid", fgColor="1F4E78"); hfont = Font(bold=True, color="FFFFFF", size=11)
thin = BSide(style="thin", color="BFBFBF"); bd = Border(left=thin, right=thin, top=thin, bottom=thin)
ctr = Alignment(horizontal="center", vertical="center")
for c, h in enumerate(headers, 1):
    cell = ws.cell(row=1, column=c, value=h); cell.fill = hf; cell.font = hfont; cell.alignment = ctr; cell.border = bd
for i, (sym, et, side, ep, xt, xp) in enumerate(trips, start=2):
    eg, ex2 = _labels(sym, et)
    vals = [sym, side, et, ep, xt, xp, eg, ex2]
    for c, v in enumerate(vals, 1):
        cell = ws.cell(row=i, column=c, value=v); cell.alignment = ctr; cell.border = bd
if trips:                                            # dropdowns need ≥1 data row
    dv1 = DataValidation(type="list", formula1='"Yes,No,Maybe"', allow_blank=True)
    dv2 = DataValidation(type="list", formula1='"Yes,No,Maybe"', allow_blank=True)
    dv1.add(f"G2:G{len(trips)+1}"); dv2.add(f"H2:H{len(trips)+1}")
    ws.add_data_validation(dv1); ws.add_data_validation(dv2)
for col, w in {"A":10,"B":8,"C":12,"D":12,"E":12,"F":12,"G":17,"H":17}.items():
    ws.column_dimensions[col].width = w
ws.freeze_panes = "A2"
wb.save(OUT)
print(f"Wrote {OUT} with {len(trips)} round trips")
