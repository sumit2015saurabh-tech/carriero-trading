from datetime import datetime
from cvd_flow import CVDEngine, SymbolFlow, detect_divergence, ET
P=F=0
def t(label,cond,extra=""):
    global P,F
    if cond: P+=1; print(f"  PASS  {label} {extra}")
    else:    F+=1; print(f"  FAIL  {label} {extra}")

d=lambda h,m,s=0: datetime(2026,7,28,h,m,s,tzinfo=ET)

print("--- aggressor classification ---")
f=SymbolFlow("X","tick"); f.on_quote(100.00,100.04)
t("at ask = BUY",  f.classify(100.04)==1)
t("above ask = BUY",f.classify(100.10)==1)
t("at bid = SELL", f.classify(100.00)==-1)
t("below bid = SELL",f.classify(99.90)==-1)
f.last_price=100.01
t("inside spread uptick = BUY", f.classify(100.02)==1)
t("inside spread downtick = SELL", f.classify(100.005)==-1)
f2=SymbolFlow("Y","tick"); f2.on_quote(100.00,100.04)
t("inside, no history = unclassified", f2.classify(100.02)==0)

print("\n--- signed accumulation + 1min/5min buckets ---")
f=SymbolFlow("X","tick"); f.on_quote(100.00,100.04)
f.on_trade(d(9,31),100.04,500)     # buy
f.on_trade(d(9,31,30),100.00,200)  # sell
f.on_trade(d(9,33),100.04,300)     # buy
s5=f.cvd_series(5); s1=f.cvd_series(1)
t("5m net = +600", s5[-1][1]==600, f"(got {s5[-1][1]})")
t("one 5m bucket (9:30)", len(s5)==1 and s5[0][0]==d(9,30))
t("two 1m buckets", len(s1)==2, f"(got {len(s1)})")
t("1m bucket1 = +300", s1[0][1]==300, f"(got {s1[0][1]})")
t("1m cumulative = +600", s1[-1][1]==600, f"(got {s1[-1][1]})")

print("\n--- session reset at 09:30 ---")
f=SymbolFlow("X","tick"); f.on_quote(100.00,100.04)
f.on_trade(datetime(2026,7,27,10,0,tzinfo=ET),100.04,900)
t("day1 has flow", f.cvd_series(5)[-1][1]==900)
f.on_trade(datetime(2026,7,28,10,0,tzinfo=ET),100.04,100)
t("day2 resets to +100", f.cvd_series(5)[-1][1]==100, f"(got {f.cvd_series(5)[-1][1]})")
t("only 1 bucket after reset", len(f.cvd_series(5))==1)

print("\n--- pre/post-session ticks excluded ---")
f=SymbolFlow("X","tick"); f.on_quote(100.00,100.04)
f.on_trade(d(8,0),100.04,999); t("premarket ignored", len(f.cvd_series(5))==0)
f.on_trade(d(16,30),100.04,999); t("afterhours ignored", len(f.cvd_series(5))==0)
f.on_trade(d(9,30),100.04,50);  t("09:30 included", f.cvd_series(5)[-1][1]==50)
f.on_trade(d(15,59),100.04,50); t("15:59 included", f.cvd_series(5)[-1][1]==100)

print("\n--- reqMktData cumulative-volume differencing ---")
f=SymbolFlow("X","mkt"); f.on_quote(100.00,100.04)
f.on_cum_volume(d(9,31),100.04,10000)          # first sample = baseline only
t("baseline no flow", len(f.cvd_series(5))==0)
f.on_cum_volume(d(9,32),100.04,10500)          # +500 at ask = buy
t("delta +500 buy", f.cvd_series(5)[-1][1]==500, f"(got {f.cvd_series(5)[-1][1]})")
f.on_cum_volume(d(9,33),100.00,10800)          # +300 at bid = sell
t("delta 300 sell -> +200", f.cvd_series(5)[-1][1]==200, f"(got {f.cvd_series(5)[-1][1]})")
f.on_cum_volume(d(9,34),100.00,9000)           # rollover guard
t("volume rollover ignored", f.cvd_series(5)[-1][1]==200)

print("\n--- method isolation ---")
e=CVDEngine(["A","B"]); e.plan_subscriptions(["A","B"])
e.on_quote("A",100.0,100.04); e.on_tick_trade("A",100.04,100,d(9,31))
t("tick symbol accepts tick trade", e.snapshot("A")["cvd_5m"]==100)
e.on_mkt_volume("A",100.04,999999,d(9,32))
t("tick symbol ignores mkt volume", e.snapshot("A")["cvd_5m"]==100)

print("\n--- divergence ---")
# price HH, CVD LH  -> bearish
ser=[(d(9,30+i*5),c,p) for i,(c,p) in enumerate(
    [(100,10.0),(500,10.5),(300,10.2),(450,10.9),(200,10.4)])]
div,note=detect_divergence(ser)
t("bearish detected", div=="bearish", f"({note[:48]})")
# price LL, CVD HL -> bullish
ser=[(d(9,30+i*5),c,p) for i,(c,p) in enumerate(
    [(-100,10.0),(-500,9.5),(-300,9.8),(-350,9.1),(-200,9.6)])]
div,note=detect_divergence(ser)
t("bullish detected", div=="bullish", f"({note[:48]})")
# trending together -> none
ser=[(d(9,30+i*5),c,p) for i,(c,p) in enumerate(
    [(100,10.0),(200,10.2),(300,10.4),(400,10.6),(500,10.8)])]
t("no false positive in trend", detect_divergence(ser)[0] is None)
t("short series safe", detect_divergence(ser[:2])[0] is None)

print("\n--- snapshot shape ---")
e=CVDEngine(["Z"]); e.plan_subscriptions(["Z"])
s=e.snapshot("Z")
t("empty snapshot safe", s["cvd_5m"] is None and s["trades"]==0)
e.on_quote("Z",100.0,100.04); e.on_tick_trade("Z",100.04,700,d(9,31))
s=e.snapshot("Z")
t("populated snapshot", s["cvd_5m"]==700 and s["method"]=="tick" and s["bars_5m"]==1)
t("unclassified pct tracked", s["unclassified_pct"]==0.0)
t("missing symbol -> None", e.snapshot("NOPE") is None)

print("\n--- secondary gateway (overflow symbols) ---")
e=CVDEngine(["A","B","C","D","E","F","G","H"])
prim,over=e.plan_subscriptions(["A","B","C","D","E","F","G","H"])
t("primary gets 5", prim==["A","B","C","D","E"], f"({prim})")
t("overflow returned", over==["F","G","H"], f"({over})")
t("overflow starts as none", e.snapshot("F")["method"]=="none")
e.on_quote("F",100.0,100.04); e.on_tick_trade("F",100.04,300,d(9,31))
t("overflow ignores trades before enable", e.snapshot("F")["cvd_5m"] is None)
e.enable_secondary(over)
t("enabled -> tick", e.snapshot("F")["method"]=="tick")
e.on_quote("F",100.0,100.04); e.on_tick_trade("F",100.04,300,d(9,31))
t("secondary accumulates", e.snapshot("F")["cvd_5m"]==300, f"({e.snapshot('F')['cvd_5m']})")
e.on_tick_trade("F",100.00,100,d(9,32))
t("secondary signs correctly", e.snapshot("F")["cvd_5m"]==200, f"({e.snapshot('F')['cvd_5m']})")
t("primary unaffected", e.snapshot("A")["method"]=="tick")
e.demote_secondary()
t("demote on gateway loss", e.snapshot("F")["method"]=="none")

print(f"\n{P} passed, {F} failed")
raise SystemExit(1 if F else 0)
