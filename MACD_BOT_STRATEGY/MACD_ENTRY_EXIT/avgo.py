import re
L="logs/tws_04_06_26.txt"
fp=re.compile(r"(\d\d:\d\d:\d\d),\d+ .*TWS FILL: oid=(\d+)\s+symbol=AVGO\s+side=(BOT|SLD)\s+qty=(\d+)\s+price=([\d.]+)")
o={}
for ln in open(L):
    m=fp.search(ln)
    if m:
        t,oid,side,q,p=m.group(1),m.group(2),m.group(3),int(m.group(4)),float(m.group(5))
        d=o.setdefault(oid,{"t":t,"side":side,"q":0,"pv":0.0}); d["q"]+=q; d["pv"]+=q*p
orders=sorted(o.values(), key=lambda x:x["t"])
print("AVGO orders (aggregated by order):")
for d in orders:
    print("  %s  %s  %4d @ %.3f" % (d["t"], d["side"], d["q"], d["pv"]/d["q"]))
# round trips
pos=0; entry=0.0; et=""; eside=""; trips=[]
for d in orders:
    s=d["q"] if d["side"]=="BOT" else -d["q"]; px=d["pv"]/d["q"]
    if pos==0:
        pos=s; entry=px; et=d["t"]; eside=d["side"]
    elif (pos>0)==(s>0):
        entry=(entry*abs(pos)+px*abs(s))/(abs(pos)+abs(s)); pos+=s
    else:
        cq=min(abs(s),abs(pos)); pnl=(px-entry)*cq*(1 if pos>0 else -1)
        trips.append((et,eside,entry,d["t"],px,cq,pnl))
        rem=abs(s)-cq; pos = (pos+s)
        if pos!=0 and rem>0: entry=px; et=d["t"]; eside=d["side"]
print("\nRound trips:")
tot=0.0
for et,es,ep,xt,xp,q,pnl in trips:
    tot+=pnl
    hold=es+" "+("LONG" if es=="BOT" else "SHORT")
    print("  %-5s %4d  entry %s @%.2f  ->  exit %s @%.2f   P&L=%+.0f" % (hold,q,et,ep,xt,xp,pnl))
print("\nAVGO total realized: %+.0f   (residual pos=%d)" % (tot,pos))
