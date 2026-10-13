"""
test_shadow_trail.py — STRICT audit of the live path with no paper account:

  1. price moves            → both dashboards see the new price and P&L
  2. breakeven fires        → stop moves, labelled BREAKEVEN, visible everywhere
  3. trail fires repeatedly → each hop recorded with reason + price at the move
  4. a trail must never move the stop backwards through the engine's own path
  5. stop finally fills     → trade closed with the FULL stop journey attached
  6. over-cover guard       → a stale protective order cannot open a new position
  7. the :8080 dashboard's own parser reproduces all of it from the log alone

Everything is driven through the REAL TWSEngine, with a client that raises if
anything touches the broker socket.

Run:  venv/bin/python test_shadow_trail.py
"""
import logging
import os
import sys
import time

os.environ.setdefault("SHADOW_MODE", "1")
os.environ.setdefault("SHADOW_DIR", "/tmp/shadow_trail_test")
os.environ.setdefault("SHADOW_TICK_SEC", "0.05")
os.environ.setdefault("FILL_VERIFY_INTERVAL", "0.2")

# Wipe the test ledger first: the real ledger deliberately APPENDS all day, so a
# re-run would otherwise see the previous run's trades and the exact-count
# assertions below would (correctly) fail.
import shutil
shutil.rmtree("/tmp/shadow_trail_test", ignore_errors=True)
LOGFILE = "/tmp/shadow_trail_test/engine.log"
os.makedirs("/tmp/shadow_trail_test", exist_ok=True)

from IBKR_TWS import TWSEngine, TWSClient, SHADOW_ON, OrderTicket, OrderPriority  # noqa: E402
from shadow_broker import SHADOW, rebuild_day, _today_str                          # noqa: E402

# IBKR_TWS._setup_logging() runs at import, clears the root handlers and writes to
# logs/tws_DD_MM_YY.txt. Re-point that file handler at the test log so this run
# does not pollute the real session log — and so the :8080 parser check below
# reads exactly the lines this test produced.
_root = logging.getLogger()
for _h in list(_root.handlers):
    if isinstance(_h, logging.FileHandler):
        _root.removeHandler(_h)
        _h.close()
_fh = logging.FileHandler(LOGFILE, mode="w", encoding="utf-8")
_fh.setFormatter(logging.Formatter(
    "%(asctime)s [TWS_ENGINE] %(levelname)s - %(message)s"))
_root.addHandler(_fh)

OK, FAIL = [], []


def check(name, cond, detail=""):
    (OK if cond else FAIL).append(name)
    print(("  PASS  " if cond else "  FAIL  ") + name + (f"   {detail}" if detail else ""))


class LeakGuard(TWSClient):
    @property
    def is_connected(self):
        return True

    def placeOrder(self, *a, **k):
        raise AssertionError("LEAK: placeOrder hit the broker")

    def cancelOrder(self, *a, **k):
        raise AssertionError("LEAK: cancelOrder hit the broker")

    def reqPositions(self, *a, **k):
        raise AssertionError("LEAK: reqPositions hit the broker")


def px(sym, last):
    """One tick — exactly what macd_bot feeds the shadow from the IBKR stream."""
    SHADOW.on_price(sym, last=last, bid=round(last - 0.01, 2), ask=round(last + 0.01, 2))
    time.sleep(0.25)


def main():
    assert SHADOW_ON
    engine = TWSEngine()
    client = LeakGuard()
    client._next_order_id = 5000
    engine._tws = client
    client._engine_on_stop_callback = engine._on_order_status_event
    SHADOW.attach(client)

    SYM, QTY, CONID, MPL = "SOXL", 500, 222, 1.20
    ENTRY = 138.00

    print("\n1 · ENTRY")
    px(SYM, 138.40)
    engine.submit_bracket(symbol=SYM, conid=CONID, direction="LONG", quantity=QTY,
                          entry_price=ENTRY, stop_price=ENTRY - MPL, mpl=MPL)
    px(SYM, 137.95)                      # trades through the limit
    time.sleep(1.5)                      # fill verify + phase 2
    st = SHADOW.snapshot()
    check("entry filled and position open",
          bool(st["positions"]) and st["positions"][0]["qty"] == QTY,
          f"avg={st['positions'][0]['avg'] if st['positions'] else '-'}")
    stop0 = next((o for o in st["orders"] if o["role"] == "STOP"), None)
    check("phase-2 protective stop exists", stop0 is not None,
          f"oid={stop0['oid']} @ {stop0['price']}" if stop0 else "")

    print("\n2 · PRICE MOVES — does the book re-price?")
    px(SYM, 139.50)
    st = SHADOW.snapshot()
    p = st["positions"][0]
    check("live price tracked on the position", p["last"] == 139.50, f"last={p['last']}")
    check("unrealised P&L recomputed on every tick",
          abs(p["unrealized"] - (139.50 - p["avg"]) * QTY) < 0.01,
          f"unreal={p['unrealized']}")
    check("net P&L reflects the open trade", st["pnl"]["net"] != 0.0,
          f"net={st['pnl']['net']}")

    print("\n3 · BREAKEVEN — engine.apply_breakeven()")
    engine.apply_breakeven(symbol=SYM, conid=CONID, direction="LONG",
                           new_stp_price=138.05, sls=0.03)
    time.sleep(0.4)
    st = SHADOW.snapshot()
    be = st["sl_events"][-1] if st["sl_events"] else {}
    check("breakeven move recorded", bool(st["sl_events"]),
          f"{be.get('from')} -> {be.get('to')}")
    check("reason detected as BREAKEVEN", be.get("reason") == "BREAKEVEN",
          f"reason={be.get('reason')}")
    check("market price captured at the moment of the move",
          be.get("price_at_move") == 139.50, f"price_then={be.get('price_at_move')}")
    check("locked-in P&L computed", be.get("locked_pnl") is not None,
          f"locked={be.get('locked_pnl')}")

    print("\n4 · TRAIL — three consecutive hops as price runs up")
    for last, new_stop in ((140.50, 139.20), (141.80, 140.50), (143.00, 141.70)):
        px(SYM, last)
        engine.trail_stop(symbol=SYM, conid=CONID, direction="LONG",
                          new_stp_price=new_stop, sls=0.03)
        time.sleep(0.35)
    st = SHADOW.snapshot()
    p = st["positions"][0]
    hops = st["sl_events"]
    check("every trail hop recorded", len(hops) == 4, f"{len(hops)} moves (1 BE + 3 trail)")
    check("trail hops labelled TRAIL",
          all(h["reason"] == "TRAIL" for h in hops[1:]),
          str([h["reason"] for h in hops]))
    check("stop on the position is the LATEST value", p["stop"] == 141.70,
          f"stop={p['stop']}")
    check("stop move counter correct", p["stop_moves"] == 4, f"moves={p['stop_moves']}")
    check("stop history carries the full journey",
          [h["to"] for h in p["stop_history"]] == [138.05, 139.2, 140.5, 141.7],
          str([h["to"] for h in p["stop_history"]]))
    check("stop only ever moved UP for a long",
          all(h["to"] > h["from"] for h in hops),
          str([(h["from"], h["to"]) for h in hops]))
    check("stop past entry → risk negative, locked profit positive",
          p["risk"] < 0 and p["locked"] > 0,
          f"risk=${p['risk']}  locked=${p['locked']}")

    print("\n5 · STOP FILLS — price pulls back into the trailed stop")
    px(SYM, 141.60)
    time.sleep(0.6)
    st = SHADOW.snapshot()
    check("position closed by the trailed stop", not st["positions"])
    check("one closed trade booked", len(st["trades"]) == 1)
    t = st["trades"][0] if st["trades"] else {}
    check("exit filled at the traded price minus slippage (not a gifted trigger)",
          abs(t.get("exit_price", 0) - 141.59) < 0.02, f"exit={t.get('exit_price')}")
    check("trade records the stop journey",
          t.get("stop_initial") and t.get("stop_final") == 141.7
          and t.get("stop_moves") == 4,
          f"{t.get('stop_initial')} -> {t.get('stop_final')} in {t.get('stop_moves')} moves")
    check("trade is profitable thanks to the trail", t.get("net_pnl", 0) > 0,
          f"net=${t.get('net_pnl')}")
    check("realized P&L equals the sum of closed trades",
          abs(st["pnl"]["realized"] - sum(x["net_pnl"] for x in st["trades"])) < 0.01,
          f"realized={st['pnl']['realized']}")

    print("\n6 · OVER-COVER GUARD — a stale stop fires with no position")
    stale = OrderTicket(priority=OrderPriority.STP, seq=99, symbol=SYM, conid=CONID,
                        side="SELL", order_type="STP", quantity=QTY, price=141.00,
                        tif="DAY", parent_id="", order_ref="STPMKT_STALE_1_SOXL")
    SHADOW.place(8888, stale, transmit=True)
    px(SYM, 140.00)
    time.sleep(0.4)
    st = SHADOW.snapshot()
    o = next((x for x in st["orders"] if x["oid"] == 8888), {})
    check("stale protective order was cancelled, not filled",
          o.get("status") == "Cancelled", f"status={o.get('status')}")
    check("no phantom short position was opened", not st["positions"])
    check("still exactly one trade for the day", len(st["trades"]) == 1)

    print("\n7 · :8080 DASHBOARD — rebuild everything from the log alone")
    import positions_server as ps
    recs, be_ev, execmap = {}, {}, {}
    ps._consume_log_chunk(open(LOGFILE, encoding="utf-8").read(), recs, be_ev, execmap)
    stop_rec = next((r for r in recs.values()
                     if str(r.get("ref", "")).startswith("STPMKT_TWS")), {})
    check("dashboard parser sees the stop order", bool(stop_rec),
          f"oid={stop_rec.get('oid')}")
    check("dashboard shows the CURRENT (trailed) stop price",
          stop_rec.get("price") == 141.7, f"price={stop_rec.get('price')}")
    check("dashboard keeps the original stop for comparison",
          stop_rec.get("orig_price") is not None,
          f"orig={stop_rec.get('orig_price')}")
    check("dashboard has the full stop-move history",
          len(stop_rec.get("stop_history") or []) == 4,
          f"{len(stop_rec.get('stop_history') or [])} moves")
    trips = ps._build_round_trips(recs)
    check("dashboard rebuilds the round trip", len(trips) == 1,
          str(trips[0]) if trips else "none")
    check("dashboard P&L agrees with the shadow's",
          trips and abs(trips[0]["ibkr_pnl"] - st["pnl"]["realized"]) < 0.01,
          f"dash={trips[0]['ibkr_pnl'] if trips else '-'} shadow={st['pnl']['realized']}")

    print("\n8 · HISTORY — replay the session from the ledger")
    h = rebuild_day(_today_str())
    check("history has the trade", len(h["trades"]) == 1)
    check("history has every SL update", len(h["sl_events"]) == 4)
    check("history P&L matches live", abs(h["pnl"]["net"] - st["pnl"]["realized"]) < 0.01,
          f"{h['pnl']['net']} vs {st['pnl']['realized']}")

    print(f"\n{len(OK)} passed, {len(FAIL)} failed")
    if FAIL:
        print("FAILED:", FAIL)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
