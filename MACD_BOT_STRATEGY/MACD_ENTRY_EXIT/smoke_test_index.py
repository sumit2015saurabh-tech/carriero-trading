"""Smoke test for the new check_index_alignment helper."""
from entry_logic import check_index_alignment, _resolve_index_cfg, load_config

bullish_setup = {
    "macd":         [0.05, 0.04, 0.02, 0.05, 0.06],
    "macd_signal":  [0.10, 0.11, 0.12, 0.13, 0.14],
    "macd_zl":      0.05,
    "macd_zl_signal": 0.04,
}

bearish_setup = {
    "macd":         [-0.05, -0.04, -0.02, -0.05, -0.06],
    "macd_signal":  [-0.10, -0.11, -0.12, -0.13, -0.14],
    "macd_zl":      -0.05,
    "macd_zl_signal": -0.04,
}

misaligned = {
    "macd":         [0.05, 0.04, 0.02, 0.05, 0.06],
    "macd_signal":  [0.10, 0.11, 0.12, 0.13, 0.14],
    "macd_zl":      0.04,
    "macd_zl_signal": 0.05,
}

print("--- TEST 1: index data missing ---")
ok, why = check_index_alignment("QQQ", "LONG", None)
print(f"  ok={ok}  reason={why}")

print()
print("--- TEST 2: LONG bullish index — default Signal_L=2, should PASS ---")
ok, why = check_index_alignment("QQQ", "LONG", bullish_setup)
print(f"  ok={ok}  reason={why}")

print()
print("--- TEST 3: SHORT bearish index — default Signal_S=2, should PASS ---")
ok, why = check_index_alignment("QQQ", "SHORT", bearish_setup)
print(f"  ok={ok}  reason={why}")

print()
print("--- TEST 4: LONG misaligned ZL, should BLOCK ---")
ok, why = check_index_alignment("QQQ", "LONG", misaligned)
print(f"  ok={ok}  reason={why}")

print()
print("--- TEST 5: resolved cfg for QQQ ---")
cfg = load_config()
merged = _resolve_index_cfg("QQQ", cfg)
keys = ["Signal_L", "Signal_S", "MACDLE_BLW_ZLine", "MACDSE_ABV_ZLine"]
for k in keys:
    print(f"  {k} = {merged.get(k)!r}")

print()
print("--- TEST 6: index w/ no per-index entry — falls back to defaults ---")
merged_unknown = _resolve_index_cfg("XXX_NONEXISTENT", cfg)
for k in keys:
    print(f"  {k} = {merged_unknown.get(k)!r}")

print()
print("--- TEST 7: per-stock index_filter status ---")
for sym in ("AAPL", "META", "TSLA"):
    e = cfg["Stocks"].get(sym, {})
    print(f"  {sym}: index_filter={e.get('index_filter')!r} index={e.get('index')!r}")

print()
print("ALL TESTS COMPLETED")
