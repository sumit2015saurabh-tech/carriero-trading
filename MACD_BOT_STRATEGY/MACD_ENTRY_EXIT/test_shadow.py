"""
test_shadow.py — end-to-end smoke test for the shadow broker.

Simulates the exact order flow the engine produces for one trade:
  Phase 1  parent LMT (transmit=False) + WIDESTP child STP (transmit=True)
  fill     price trades through the limit          → parent fills, child arms
  Phase 2  STPMKT stop placed, WIDESTP cancelled
  trail    two stop modifies (breakeven, then trail)
  exit     price hits the stop                     → position closed, P&L booked

Run:  venv/bin/python test_shadow.py
"""
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, field

os.environ.setdefault("SHADOW_MODE", "1")
os.environ.setdefault("SHADOW_DIR", "/tmp/shadow_test")
os.environ.setdefault("SHADOW_TICK_SEC", "0.05")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [TWS_ENGINE] %(levelname)s - %(message)s")

from shadow_broker import SHADOW, rebuild_day, available_days, _today_str  # noqa: E402
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



@dataclass
class Ticket:
    symbol: str
    conid: int
    side: str
    order_type: str
    quantity: int
    price: float
    aux_price: float = 0.0
    tif: str = "DAY"
    parent_id: str = ""
    order_ref: str = ""
    oca_group: str = ""
    oca_type: int = 1
    callback: object = None


class FakeClient:
    """The parts of TWSClient the shadow broker touches."""
    def __init__(self):
        self._status_lock = threading.Lock()
        self._order_statuses = {}
        self._fill_callbacks = {}
        self._refs = {}
        self._last_order_callback_at = 0.0
        self._last_receive_at = 0.0
        self.stop_events = []

    def resolve_parent_id(self, ref):
        return self._refs.get(ref)

    def _engine_on_stop_callback(self, oid, status, filled, remaining):
        self.stop_events.append((oid, status, filled, remaining))


def main() -> int:
    client = FakeClient()
    SHADOW.attach(client)

    SYM, QTY = "AAPL", 500
    SHADOW.on_price(SYM, last=200.00, bid=199.99, ask=200.01)

    # ── Phase 1: parent LMT BUY @ 199.50 (held) + wide stop (transmits) ──────
    parent = Ticket(SYM, 1, "BUY", "LMT", QTY, 199.50, order_ref="PARENT_1_AAPL")
    poid = SHADOW.place(1001, parent, transmit=False)
    client._refs["PARENT_1_AAPL"] = poid
    wide = Ticket(SYM, 1, "SELL", "STP", QTY, 197.50,
                  parent_id="PARENT_1_AAPL", order_ref="WIDESTP_1_AAPL")
    SHADOW.place(1002, wide, transmit=True)

    time.sleep(0.3)
    st = SHADOW.snapshot()
    assert st["orders"][0]["status"] == "Submitted", "parent must transmit"
    assert st["orders"][1]["status"] == "PreSubmitted", "child waits for parent"
    print("✓ bracket assembled: parent Submitted, child PreSubmitted")

    # ── market does NOT reach the limit → no fill (this is the honest part) ──
    SHADOW.on_price(SYM, last=199.80, bid=199.79, ask=199.81)
    time.sleep(0.3)
    assert not SHADOW.snapshot()["fills"], "limit must not fill above its price"
    print("✓ limit did not fill while the market stayed above it")

    # ── price trades through the limit → parent fills ───────────────────────
    SHADOW.on_price(SYM, last=199.45, bid=199.44, ask=199.46)
    time.sleep(0.4)
    st = SHADOW.snapshot()
    assert st["fills"], "limit should fill once price trades through"
    assert st["positions"] and st["positions"][0]["qty"] == QTY
    assert st["orders"][1]["status"] == "Submitted", "child arms after parent fill"
    print(f"✓ parent filled @ {st['fills'][0]['price']}  position open")

    # ── Phase 2: real stop, wide stop cancelled ─────────────────────────────
    stp = Ticket(SYM, 1, "SELL", "STP", QTY, 198.50,
                 order_ref="STPMKT_TWS_1001_AAPL", oca_group="BRK_AAPL_1")
    SHADOW.place(1003, stp, transmit=True)
    SHADOW.cancel(1002)
    time.sleep(0.2)

    # ── stop moves: breakeven, then a trail ─────────────────────────────────
    SHADOW.on_price(SYM, last=201.00, bid=200.99, ask=201.01)
    time.sleep(0.2)
    SHADOW.modify(1003, stp, 199.50)          # breakeven
    SHADOW.on_price(SYM, last=202.50, bid=202.49, ask=202.51)
    time.sleep(0.2)
    SHADOW.modify(1003, stp, 201.20)          # trail
    st = SHADOW.snapshot()
    assert len(st["sl_events"]) == 2, st["sl_events"]
    assert st["positions"][0]["stop_moves"] == 2
    assert st["positions"][0]["stop"] == 201.20
    print("✓ stop moved twice and both moves are on the position + history")

    # ── price hits the stop → exit ──────────────────────────────────────────
    SHADOW.on_price(SYM, last=201.10, bid=201.09, ask=201.11)
    time.sleep(0.5)
    st = SHADOW.snapshot()
    assert not st["positions"], "position must be flat after the stop fills"
    assert len(st["trades"]) == 1, st["trades"]
    t = st["trades"][0]
    print(f"✓ stop filled — trade closed: {t['direction']} {t['qty']} "
          f"{t['entry_price']} → {t['exit_price']}  net={t['net_pnl']}  "
          f"reason={t['exit_reason']}  stop_moves={t['stop_moves']}")
    assert t["stop_moves"] == 2 and t["exit_reason"] == "STOP HIT"
    assert t["net_pnl"] > 0, "trailed stop above entry must book a profit"

    # ── the engine was told about the stop fill ─────────────────────────────
    assert any(e[1] == "Filled" for e in client.stop_events), client.stop_events
    assert client._order_statuses[1003]["status"] == "Filled"
    print("✓ engine callbacks + get_order_status see the fill")

    # ── SHORT round-trip, market orders ─────────────────────────────────────
    SHADOW.on_price("TSLA", last=300.00, bid=299.98, ask=300.02)
    sell = Ticket("TSLA", 2, "SELL", "MKT", 100, 0.0, order_ref="PARENT_2_TSLA")
    SHADOW.place(2001, sell, transmit=True)
    time.sleep(0.3)
    SHADOW.on_price("TSLA", last=298.00, bid=297.98, ask=298.02)
    cov = Ticket("TSLA", 2, "BUY", "MKT", 100, 0.0, order_ref="CLOSE_2_TSLA")
    SHADOW.place(2002, cov, transmit=True)
    time.sleep(0.4)
    st = SHADOW.snapshot()
    short = [x for x in st["trades"] if x["symbol"] == "TSLA"]
    assert short and short[0]["direction"] == "SHORT" and short[0]["net_pnl"] > 0
    print(f"✓ short round-trip: {short[0]['entry_price']} → "
          f"{short[0]['exit_price']}  net={short[0]['net_pnl']}")

    # ── P&L + positions snapshot shape the engine expects ───────────────────
    pos, fresh = SHADOW.positions_snapshot()
    assert pos == {} and fresh is True
    print(f"✓ net P&L: realized={st['pnl']['realized']}  "
          f"unrealized={st['pnl']['unrealized']}  net={st['pnl']['net']}  "
          f"commission={st['pnl']['commission']}  trades={st['pnl']['trades']}")

    # ── history replay from the ledger (what "open any day" uses) ───────────
    day = _today_str()
    hist = rebuild_day(day)
    assert len(hist["trades"]) == 2, hist["trades"]
    assert len(hist["sl_events"]) == 2
    assert abs(hist["pnl"]["net"] - st["pnl"]["realized"]) < 0.01, \
        (hist["pnl"], st["pnl"])
    assert day in available_days()
    print(f"✓ ledger replay for {day}: {len(hist['trades'])} trades, "
          f"{len(hist['orders'])} orders, net={hist['pnl']['net']}")

    print("\nALL SHADOW TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
