import re, sys
L = sys.argv[1] if len(sys.argv) > 1 else "logs/tws_05_06_26.txt"
CUTOFF = "14:19:49"
fp = re.compile(r"(\d\d:\d\d:\d\d),\d+ .*TWS FILL: oid=(\d+)\s+symbol=(\w+)\s+side=(BOT|SLD)\s+qty=(\d+)\s+price=([\d.]+)")
o = {}
for ln in open(L):
    m = fp.search(ln)
    if not m:
        continue
    t, oid, sym, side, q, p = m.group(1), int(m.group(2)), m.group(3), m.group(4), int(m.group(5)), float(m.group(6))
    if t < CUTOFF:
        continue
    if oid >= 90000:        # exclude flatten orders
        continue
    d = o.setdefault((oid, sym), {"t": t, "sym": sym, "side": side, "q": 0, "pv": 0.0})
    d["q"] += q; d["pv"] += q * p
orders = sorted(o.values(), key=lambda x: x["t"])
by = {}
for d in orders:
    by.setdefault(d["sym"], []).append(d)
total = 0.0; per = {}
for sym, ods in by.items():
    pos = 0; entry = 0.0; realized = 0.0
    for d in ods:
        s = d["q"] if d["side"] == "BOT" else -d["q"]; px = d["pv"] / d["q"]
        if pos == 0:
            pos = s; entry = px
        elif (pos > 0) == (s > 0):
            entry = (entry * abs(pos) + px * abs(s)) / (abs(pos) + abs(s)); pos += s
        else:
            cq = min(abs(s), abs(pos))
            realized += (px - entry) * cq * (1 if pos > 0 else -1)
            pos += (s if abs(s) < abs(pos) else (cq if pos < 0 else -cq))
            if pos != 0 and abs(s) > cq:
                entry = px
    per[sym] = (realized, pos)
    total += realized
print(f"=== Realized P&L from bot trades since restart ({CUTOFF}) ===")
for sym in sorted(per, key=lambda s: per[s][0]):
    r, openpos = per[sym]
    flag = f"  (still open {openpos})" if openpos else ""
    print(f"  {sym:6} {r:+9.0f}{flag}")
print(f"  {'TOTAL':6} {total:+9.0f}  realized")
