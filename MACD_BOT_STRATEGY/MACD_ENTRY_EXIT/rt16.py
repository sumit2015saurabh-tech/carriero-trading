import re
from collections import defaultdict

fills = []
rx = re.compile(r"TWS FILL: oid=(\d+)\s+symbol=(\w+)\s+side=(\w+)\s+qty=(\d+)\s+price=([\d.]+)")
for line in open("logs/tws_16_07_26.txt"):
    m = rx.search(line)
    if not m:
        continue
    t = line.split()[1][:8]
    oid, sym, side, qty, px = m.groups()
    fills.append((t, sym, side, int(qty), float(px)))

pos = defaultdict(int)
avg = defaultdict(float)
openp = defaultdict(lambda: None)
realized = defaultdict(float)
trips = []

for t, sym, side, qty, px in fills:
    d = qty if side == "BOT" else -qty
    cur = pos[sym]
    if cur == 0 or (cur > 0) == (d > 0):
        newpos = cur + d
        avg[sym] = (avg[sym] * abs(cur) + px * qty) / abs(newpos) if newpos != 0 else 0.0
        if openp[sym] is None:
            openp[sym] = (t, side, px)
        pos[sym] = newpos
    else:
        closeqty = min(abs(cur), qty)
        pl = (px - avg[sym]) * closeqty if cur > 0 else (avg[sym] - px) * closeqty
        realized[sym] += pl
        rem = qty - closeqty
        pos[sym] = cur + d
        if abs(pos[sym]) < 1e-9:
            pos[sym] = 0
        if openp[sym]:
            ot, osd, opx = openp[sym]
            trips.append((sym, osd, ot, opx, t, px, closeqty, pl))
        openp[sym] = None
        if rem > 0:
            avg[sym] = px
            openp[sym] = (t, side, px)
            pos[sym] = rem if d > 0 else -rem

tot = 0.0
print("%-6s %-6s %-9s %-9s %9s %9s %6s %10s" % ("SYM", "SIDE", "IN", "OUT", "INpx", "OUTpx", "QTY", "P/L"))
for sym, osd, ot, opx, ct, cpx, q, pl in trips:
    sidel = "SHORT" if osd == "SLD" else "LONG"
    tot += pl
    print("%-6s %-6s %-9s %-9s %9.2f %9.2f %6d %10.2f" % (sym, sidel, ot, ct, opx, cpx, q, pl))
print("-" * 74)
print("TOTAL round-trip P/L (fills, ex-commission): %.2f  | trips=%d" % (tot, len(trips)))
print("leftover open pos:", {k: v for k, v in pos.items() if v})
print()
print("Per-symbol realized:")
for s in sorted(realized, key=lambda x: realized[x]):
    print("  %-6s %10.2f" % (s, realized[s]))
print()
wins = [1 for *_, pl in [(x[0], x[7]) for x in trips] if pl > 0]
nwin = sum(1 for x in trips if x[7] > 0)
print("wins %d / %d  (%.0f%%)" % (nwin, len(trips), 100.0 * nwin / len(trips) if trips else 0))
