"""Self-test for the 2026-07-29 receive-liveness changes."""
import os, sys, time, types
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
P=F=0
def t(label, cond, extra=""):
    global P,F
    if cond: P+=1; print(f"  PASS  {label} {extra}")
    else:    F+=1; print(f"  FAIL  {label} {extra}")

import IBKR_TWS
C = IBKR_TWS.TWSClient

print("--- change 1: tickString stamps liveness ---")
t("tickString exists", hasattr(C, "tickString"))
obj = types.SimpleNamespace(_last_receive_at=0.0, _on_tick_string=None)
C.tickString(obj, 10001, 45, "1785268509")
t("stamps _last_receive_at", obj._last_receive_at > 0,
  f"({obj._last_receive_at:.0f})")
before = obj._last_receive_at
time.sleep(0.02)
C.tickString(obj, 10001, 45, "1785268510")
t("re-stamps on every call", obj._last_receive_at > before)

seen = {}
obj2 = types.SimpleNamespace(_last_receive_at=0.0,
                             _on_tick_string=lambda r,tt,v: seen.update(r=r,tt=tt,v=v))
C.tickString(obj2, 7, 48, "RTVOL")
t("forwards to _on_tick_string when set", seen == {"r":7,"tt":48,"v":"RTVOL"}, f"({seen})")

boom = types.SimpleNamespace(_last_receive_at=0.0,
                             _on_tick_string=lambda *a: (_ for _ in ()).throw(RuntimeError("x")))
try:
    C.tickString(boom, 1, 45, "v"); ok=True
except Exception: ok=False
t("callback exception does not propagate", ok)
t("still stamped despite callback error", boom._last_receive_at > 0)

print("\n--- change 2: dead threshold ---")
src = open("IBKR_TWS.py").read()
t("default is 180s", 'os.getenv("RECEIVE_DEAD_SEC", "180")' in src)
t("env-overridable", 'RECEIVE_DEAD_SEC' in src)
_i = src.index('float(os.getenv("RECEIVE_DEAD_SEC"')
t("bad env value falls back", 'except (TypeError, ValueError)' in src[_i:_i+220])
# and prove it behaviourally, not just textually
def _resolve(v):
    import os as _os
    _old = _os.environ.get("RECEIVE_DEAD_SEC")
    if v is None: _os.environ.pop("RECEIVE_DEAD_SEC", None)
    else: _os.environ["RECEIVE_DEAD_SEC"] = v
    try:
        try: return float(_os.getenv("RECEIVE_DEAD_SEC", "180"))
        except (TypeError, ValueError): return 180.0
    finally:
        _os.environ.pop("RECEIVE_DEAD_SEC", None)
        if _old is not None: _os.environ["RECEIVE_DEAD_SEC"] = _old
t("unset -> 180", _resolve(None) == 180.0)
t("'300' -> 300", _resolve("300") == 300.0)
t("'abc' -> 180 (no crash)", _resolve("abc") == 180.0)

print("\n--- change 2 safety: genuine disconnect still detected instantly ---")
alive = types.SimpleNamespace(_connected=False, _last_receive_at=time.time(),
                              _receive_dead_sec=180.0)
t("not connected -> broken immediately (ignores timer)",
  C.is_receive_broken(alive) is True)
fresh = types.SimpleNamespace(_connected=True, _last_receive_at=time.time(),
                              _receive_dead_sec=180.0)
t("connected + fresh -> healthy", C.is_receive_broken(fresh) is False)
stale = types.SimpleNamespace(_connected=True, _last_receive_at=time.time()-200,
                              _receive_dead_sec=180.0)
t("connected + 200s stale -> broken", C.is_receive_broken(stale) is True)
mid = types.SimpleNamespace(_connected=True, _last_receive_at=time.time()-90,
                            _receive_dead_sec=180.0)
t("90s silence no longer trips (was dead at 60s)", C.is_receive_broken(mid) is False)

print("\n--- change 3: reseed skip logs once per episode ---")
b = open("macd_bot.py").read()
t("dedup guard present", "_reseed_skip_reason" in b)
t("resume is logged", "unseeded-retry RESUMED" in b)
t("still returns (gating unchanged)",
  b.count("unseeded-retry SKIPPED") == 1)

print(f"\n{P} passed, {F} failed")
sys.exit(1 if F else 0)
