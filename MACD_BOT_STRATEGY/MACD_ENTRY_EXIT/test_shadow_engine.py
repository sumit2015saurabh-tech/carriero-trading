"""
test_shadow_engine.py — INTEGRATION test: real TWSEngine → shadow broker.

Unlike test_shadow.py (which drives the broker directly), this drives the REAL
production engine path:

    engine.submit_bracket()   Phase 1: parent LMT + wide STP
      → fill verification     polls shadow order status
      → _place_phase2_orders   STP_MKT placed, wide STP cancelled
    engine.trail_stop()        stop re-priced  → SL_UPDATE, reason=TRAIL
    price hits the stop        → shadow fills it, engine + bot are notified

The TWS client is a stub whose placeOrder/cancelOrder RAISE if they are ever
called — so the test also proves that nothing can leak to a real broker while
SHADOW_MODE=1.

Run:  venv/bin/python test_shadow_engine.py
"""
import logging
import os
import sys
import time

os.environ.setdefault("SHADOW_MODE", "1")
os.environ.setdefault("SHADOW_DIR", "/tmp/shadow_engine_test")
os.environ.setdefault("SHADOW_TICK_SEC", "0.05")
os.environ.setdefault("FILL_VERIFY_INTERVAL", "0.2")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [TWS_ENGINE] %(levelname)s - %(message)s")

import IBKR_TWS                                                   # noqa: E402
from IBKR_TWS import TWSEngine, TWSClient, SHADOW_ON              # noqa: E402
from shadow_broker import SHADOW, rebuild_day, _today_str         # noqa: E402
os.makedirs(os.environ["SHADOW_DIR"], exist_ok=True)

# IBKR_TWS._setup_logging() clears the root handlers at import and writes to
# logs/tws_DD_MM_YY.txt — which is the REAL session log the :8080 dashboard
# tails. Re-point it at a temp file so a test run can never inject fake orders
# into a trading day's log.
import logging as _lg
_r = _lg.getLogger()
for _h in list(_r.handlers):
    if isinstance(_h, _lg.FileHandler):
        _r.removeHandler(_h); _h.close()
_tfh = _lg.FileHandler(os.path.join(os.environ["SHADOW_DIR"], "engine.log"),
                       mode="w", encoding="utf-8")
_tfh.setFormatter(_lg.Formatter("%(asctime)s [TWS_ENGINE] %(levelname)s - %(message)s"))
_r.addHandler(_tfh)



class LeakGuard(TWSClient):
    """A 'connected' client that explodes if anything reaches the socket."""

    @property
    def is_connected(self) -> bool:
        return True

    def placeOrder(self, *a, **k):                                # noqa: N802
        raise AssertionError("LEAK: placeOrder reached the broker in shadow mode")

    def cancelOrder(self, *a, **k):                               # noqa: N802
        raise AssertionError("LEAK: cancelOrder reached the broker in shadow mode")

    def reqGlobalCancel(self, *a, **k):                           # noqa: N802
        raise AssertionError("LEAK: reqGlobalCancel reached the broker in shadow mode")

    def reqPositions(self, *a, **k):                              # noqa: N802
        raise AssertionError("LEAK: reqPositions reached the broker in shadow mode")


def main() -> int:
    assert SHADOW_ON, "SHADOW_MODE must be on for this test"

    engine = TWSEngine()
    client = LeakGuard()
    client._next_order_id = 7000
    engine._tws = client
    client._engine_on_stop_callback = engine._on_order_status_event
    SHADOW.attach(client)

    cleared = []
    engine.set_position_clear_callback(lambda sym: cleared.append(sym))

    SYM, QTY, CONID = "XLK", 500, 111
    ENTRY, MPL = 188.00, 1.40
    SHADOW.on_price(SYM, last=188.40, bid=188.39, ask=188.41)

    # ── Phase 1 ─────────────────────────────────────────────────────────────
    res = engine.submit_bracket(symbol=SYM, conid=CONID, direction="LONG",
                                quantity=QTY, entry_price=ENTRY,
                                stop_price=ENTRY - MPL, mpl=MPL)
    print("bracket:", res)
    assert res.get("parent") and res.get("wide_stp"), res
    time.sleep(0.5)
    st = SHADOW.snapshot()
    assert len(st["orders"]) == 2, st["orders"]
    assert not st["fills"], "must not fill while price is above the entry limit"
    print("✓ Phase 1 placed through the real engine: parent + wide stop, no fill yet")

    # ── price trades through the entry limit ────────────────────────────────
    SHADOW.on_price(SYM, last=187.95, bid=187.94, ask=187.96)
    time.sleep(1.2)                                    # let fill-verify + Phase 2 run
    st = SHADOW.snapshot()
    assert st["positions"], "position should be open"
    pos = st["positions"][0]
    assert pos["qty"] == QTY and pos["side"] == "LONG"
    print(f"✓ entry filled @ {pos['avg']} — engine's fill verification confirmed it")

    # ── Phase 2 must have placed the real stop and cancelled the wide one ───
    deadline = time.time() + 8
    stp = None
    while time.time() < deadline:
        st = SHADOW.snapshot()
        stp = next((o for o in st["orders"] if o["role"] == "STOP"), None)
        if stp:
            break
        time.sleep(0.25)
    assert stp, [o["ref"] for o in SHADOW.snapshot()["orders"]]
    wide = next(o for o in st["orders"] if o["role"] == "STOP-WIDE")
    assert wide["status"] == "Cancelled", wide
    print(f"✓ Phase 2: STP_MKT oid={stp['oid']} @ {stp['price']} placed, "
          f"wide stop cancelled")

    # ── engine.trail_stop() — the real trailing path ────────────────────────
    SHADOW.on_price(SYM, last=190.50, bid=190.49, ask=190.51)
    time.sleep(0.3)
    ok = engine.trail_stop(symbol=SYM, conid=CONID, direction="LONG",
                           new_stp_price=189.20)
    print("trail_stop returned:", ok)
    time.sleep(0.3)
    st = SHADOW.snapshot()
    assert st["sl_events"], "the trail must be recorded"
    ev = st["sl_events"][-1]
    assert ev["reason"] == "TRAIL", ev          # inferred from the real call stack
    assert st["positions"][0]["stop_moves"] >= 1
    print(f"✓ trail recorded: {ev['from']} → {ev['to']} reason={ev['reason']} "
          f"price_at_move={ev['price_at_move']}")

    # ── price falls into the stop ───────────────────────────────────────────
    SHADOW.on_price(SYM, last=189.10, bid=189.09, ask=189.11)
    time.sleep(1.0)
    st = SHADOW.snapshot()
    assert not st["positions"], "stop should have closed the position"
    assert len(st["trades"]) == 1, st["trades"]
    t = st["trades"][0]
    print(f"✓ stop filled → trade closed: {t['entry_price']} → {t['exit_price']}  "
          f"net={t['net_pnl']}  reason={t['exit_reason']}  moves={t['stop_moves']}")
    assert t["exit_reason"] == "STOP HIT" and t["stop_moves"] >= 1

    # ── the bot's position-clear hook fired (engine saw the stop fill) ──────
    deadline = time.time() + 5
    while SYM not in cleared and time.time() < deadline:
        time.sleep(0.2)
    assert SYM in cleared, "engine must notify the bot that the stop filled"
    print("✓ engine → bot position-clear callback fired")

    # ── EoD path uses the shadow global-cancel, not IBKR's ─────────────────
    SHADOW.place(9999, _t(SYM, CONID), transmit=True)
    SHADOW.global_cancel()
    assert all(o["status"] in ("Filled", "Cancelled")
               for o in SHADOW.snapshot()["orders"])
    print("✓ global cancel kills the local book only")

    hist = rebuild_day(_today_str())
    print(f"✓ ledger replay: {len(hist['trades'])} trade(s), "
          f"{len(hist['orders'])} orders, net={hist['pnl']['net']}")
    print("\nALL ENGINE-INTEGRATION TESTS PASSED — no order ever reached IBKR")
    return 0


def _t(sym, conid):
    from IBKR_TWS import OrderTicket, OrderPriority
    return OrderTicket(priority=OrderPriority.STP, seq=1, symbol=sym, conid=conid,
                       side="SELL", order_type="STP", quantity=100, price=1.0,
                       tif="DAY", parent_id="", order_ref=f"STPMKT_{sym}_x")


if __name__ == "__main__":
    sys.exit(main())
