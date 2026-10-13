"""
test_shadow_unit.py — strict unit tests, one per issue that has been found or
fixed in the shadow-trading work. Every test states the failure it guards
against, so a regression names itself.

    venv/bin/python -m unittest test_shadow_unit -v
    venv/bin/python test_shadow_unit.py          (same thing)

Nothing here talks to IBKR. The TWS client is a stub whose broker calls raise.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import threading
import time
import unittest

TEST_DIR = "/tmp/shadow_unit_test"
shutil.rmtree(TEST_DIR, ignore_errors=True)
os.makedirs(TEST_DIR, exist_ok=True)

os.environ["SHADOW_MODE"] = "1"
os.environ["SHADOW_DIR"] = TEST_DIR
os.environ["SHADOW_TICK_SEC"] = "0.05"
os.environ["FILL_VERIFY_INTERVAL"] = "0.2"

import IBKR_TWS                                                        # noqa: E402
from IBKR_TWS import TWSEngine, TWSClient, OrderTicket, OrderPriority  # noqa: E402
import shadow_broker                                                   # noqa: E402
from shadow_broker import (SHADOW, ShadowBroker, rebuild_day,          # noqa: E402
                           _today_str, _is_exit_ref, _commission)
import exit_logic                                                      # noqa: E402

# Keep the engine's own file handler out of logs/tws_*.txt for the test run.
_root = logging.getLogger()
for _h in list(_root.handlers):
    if isinstance(_h, logging.FileHandler):
        _root.removeHandler(_h)
        _h.close()
_LOG = os.path.join(TEST_DIR, "engine.log")
_fh = logging.FileHandler(_LOG, mode="w", encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s [TWS_ENGINE] %(levelname)s - %(message)s"))
_root.addHandler(_fh)
_root.setLevel(logging.INFO)


class LeakGuard(TWSClient):
    """Any call that would reach a real broker raises."""

    @property
    def is_connected(self):
        return True

    def placeOrder(self, *a, **k):
        raise AssertionError("LEAK: placeOrder reached the broker")

    def cancelOrder(self, *a, **k):
        raise AssertionError("LEAK: cancelOrder reached the broker")

    def reqGlobalCancel(self, *a, **k):
        raise AssertionError("LEAK: reqGlobalCancel reached the broker")

    def reqPositions(self, *a, **k):
        raise AssertionError("LEAK: reqPositions reached the broker")


def ticket(sym, side, otype, qty, price, ref, parent="", oca="", aux=0.0, conid=1):
    return OrderTicket(priority=OrderPriority.STP, seq=0, symbol=sym, conid=conid,
                       side=side, order_type=otype, quantity=qty, price=price,
                       aux_price=aux, tif="DAY", parent_id=parent, order_ref=ref,
                       oca_group=oca, oca_type=1 if oca else 0,
                       _created_at=time.time())


class ShadowTestBase(unittest.TestCase):
    """Each test gets a clean broker so state never leaks between cases."""

    def setUp(self):
        self.broker = ShadowBroker()
        self.broker.ledger.base_dir = __import__("pathlib").Path(TEST_DIR) / self.id()
        self.broker.ledger.base_dir.mkdir(parents=True, exist_ok=True)
        self.client = LeakGuard()
        self.client._next_order_id = 1000
        self.broker.attach(self.client)
        self.addCleanup(self.broker.stop)

    def px(self, sym, last, bid=None, ask=None, settle=0.25):
        self.broker.on_price(sym, last=last,
                             bid=bid if bid is not None else round(last - 0.01, 2),
                             ask=ask if ask is not None else round(last + 0.01, 2))
        time.sleep(settle)


# ─────────────────────────────────────────────────────────────────────────────
class TestNoOrderReachesBroker(ShadowTestBase):
    """ISSUE: an order must never leave the VM while SHADOW_MODE=1."""

    def test_place_modify_cancel_never_touch_the_socket(self):
        t = ticket("AAA", "BUY", "LMT", 100, 10.00, "PARENT_1_AAA")
        self.assertEqual(self.broker.place(2001, t, transmit=True), 2001)
        self.assertTrue(self.broker.modify(2001, t, 9.90))
        self.assertTrue(self.broker.cancel(2001))
        # LeakGuard would have raised; reaching here proves nothing was sent.

    def test_positions_are_served_locally_not_from_ibkr(self):
        pos, fresh = self.broker.positions_snapshot()
        self.assertEqual(pos, {})
        self.assertTrue(fresh, "shadow positions are authoritative — never 'unknown'")


# ─────────────────────────────────────────────────────────────────────────────
class TestFillModel(ShadowTestBase):
    """ISSUE: paper fills were fantasy. The model must be pessimistic."""

    def test_limit_does_not_fill_while_market_is_away(self):
        self.px("BBB", 10.50)
        self.broker.place(1, ticket("BBB", "BUY", "LMT", 100, 10.00, "PARENT_1_BBB"), True)
        self.px("BBB", 10.05)
        self.assertEqual(self.broker.snapshot()["fills"], [],
                         "a BUY limit at 10.00 must not fill at 10.05")

    def test_limit_fills_only_when_price_trades_through(self):
        self.px("BBB", 10.50)
        self.broker.place(1, ticket("BBB", "BUY", "LMT", 100, 10.00, "PARENT_1_BBB"), True)
        self.px("BBB", 9.98)
        fills = self.broker.snapshot()["fills"]
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0]["price"], 10.00,
                         "a limit fills AT its price, never better")

    def test_market_buy_pays_the_ask_plus_slippage(self):
        self.px("CCC", 20.00, bid=19.98, ask=20.02)
        self.broker.place(1, ticket("CCC", "BUY", "MKT", 100, 0.0, "PARENT_1_CCC"), True)
        time.sleep(0.3)
        f = self.broker.snapshot()["fills"][0]
        self.assertAlmostEqual(f["price"], 20.02 + shadow_broker.SLIPPAGE, places=4)

    def test_market_sell_hits_the_bid_minus_slippage(self):
        self.px("CCC", 20.00, bid=19.98, ask=20.02)
        self.broker.place(1, ticket("CCC", "SELL", "MKT", 100, 0.0, "PARENT_1_CCC"), True)
        time.sleep(0.3)
        f = self.broker.snapshot()["fills"][0]
        self.assertAlmostEqual(f["price"], 19.98 - shadow_broker.SLIPPAGE, places=4)

    def test_stop_slips_against_you(self):
        self.px("DDD", 50.00)
        self.broker.place(1, ticket("DDD", "BUY", "MKT", 100, 0.0, "PARENT_1_DDD"), True)
        time.sleep(0.3)
        self.broker.place(2, ticket("DDD", "SELL", "STP", 100, 49.00, "STPMKT_2_DDD"), True)
        self.px("DDD", 48.90)
        exit_fill = self.broker.snapshot()["fills"][-1]
        self.assertLess(exit_fill["price"], 49.00,
                        "a triggered stop must fill worse than its trigger")

    def test_no_fill_on_a_stale_quote(self):
        self.broker.place(1, ticket("EEE", "BUY", "MKT", 100, 0.0, "PARENT_1_EEE"), True)
        time.sleep(0.3)
        self.assertEqual(self.broker.snapshot()["fills"], [],
                         "with no quote at all there must be no fill")

    def test_bar_high_low_can_fill_an_order_touched_inside_the_bar(self):
        self.px("FFF", 30.00)
        self.broker.place(1, ticket("FFF", "BUY", "LMT", 100, 29.00, "PARENT_1_FFF"), True)
        # close is back at 30 but the bar traded down to 28.50
        self.broker.on_bar("FFF", close=30.00, high=30.20, low=28.50)
        time.sleep(0.3)
        self.assertEqual(len(self.broker.snapshot()["fills"]), 1,
                         "a limit touched inside the bar must not be silently missed")


# ─────────────────────────────────────────────────────────────────────────────
class TestBracketSemantics(ShadowTestBase):
    """ISSUE: a bracket child must behave the way IBKR's does."""

    def test_child_waits_for_its_parent(self):
        # The parent limit is deliberately UNREACHABLE (5.00) while the child's
        # stop (9.00) sits inside the price range the test walks through. Only
        # the parent-gate can stop the child trading here.
        self.px("GGG", 10.00)
        self.broker.place(1, ticket("GGG", "BUY", "LMT", 100, 5.00,
                                    "PARENT_1_GGG"), transmit=False)
        self.client._ref_to_tws_id["PARENT_1_GGG"] = 1
        self.broker.place(2, ticket("GGG", "SELL", "STP", 100, 9.00,
                                    "WIDESTP_1_GGG", parent="PARENT_1_GGG"), True)
        orders = {o["oid"]: o for o in self.broker.snapshot()["orders"]}
        self.assertEqual(orders[1]["status"], "Submitted")
        self.assertEqual(orders[2]["status"], "PreSubmitted")
        self.px("GGG", 8.80)                     # breaches the child's stop
        self.assertEqual(self.broker.snapshot()["fills"], [],
                         "an unfilled parent means the child cannot trade")
        self.assertEqual(self.broker.snapshot()["positions"], [])

    def test_child_is_resized_to_the_parent_fill(self):
        self.px("HHH", 10.00)
        self.broker.place(1, ticket("HHH", "BUY", "LMT", 100, 9.50, "PARENT_1_HHH"), False)
        self.client._ref_to_tws_id["PARENT_1_HHH"] = 1
        self.broker.place(2, ticket("HHH", "SELL", "STP", 500, 9.00,
                                    "WIDESTP_1_HHH", parent="PARENT_1_HHH"), True)
        self.px("HHH", 9.40)
        orders = {o["oid"]: o for o in self.broker.snapshot()["orders"]}
        self.assertEqual(orders[2]["qty"], 100,
                         "the child must size to what the parent actually filled")

    def test_oca_sibling_is_cancelled_on_the_first_fill(self):
        self.px("III", 50.00)
        self.broker.place(1, ticket("III", "BUY", "MKT", 100, 0.0, "PARENT_1_III"), True)
        time.sleep(0.3)
        self.broker.place(2, ticket("III", "SELL", "STP", 100, 49.00,
                                    "STPMKT_2_III", oca="G1"), True)
        self.broker.place(3, ticket("III", "SELL", "LMT", 100, 52.00,
                                    "TARGET_3_III", oca="G1"), True)
        self.px("III", 48.90)
        orders = {o["oid"]: o for o in self.broker.snapshot()["orders"]}
        self.assertEqual(orders[2]["status"], "Filled")
        self.assertEqual(orders[3]["status"], "Cancelled",
                         "the OCA sibling must die when the stop fills")


# ─────────────────────────────────────────────────────────────────────────────
class TestOverCoverGuard(ShadowTestBase):
    """ISSUE: two live protective orders could close the same shares twice."""

    def test_exit_order_with_no_position_is_cancelled(self):
        self.px("JJJ", 50.00)
        self.broker.place(9, ticket("JJJ", "SELL", "STP", 100, 49.00, "STPMKT_9_JJJ"), True)
        self.px("JJJ", 48.00)
        o = {x["oid"]: x for x in self.broker.snapshot()["orders"]}[9]
        self.assertEqual(o["status"], "Cancelled")
        self.assertEqual(self.broker.snapshot()["positions"], [],
                         "a stale stop must never open a short")

    def test_exit_order_is_capped_at_the_open_quantity(self):
        self.px("KKK", 50.00)
        self.broker.place(1, ticket("KKK", "BUY", "MKT", 100, 0.0, "PARENT_1_KKK"), True)
        time.sleep(0.3)
        self.broker.place(2, ticket("KKK", "SELL", "STP", 500, 49.00, "STPMKT_2_KKK"), True)
        self.px("KKK", 48.50)
        self.assertEqual(self.broker.snapshot()["positions"], [],
                         "the position closes")
        self.assertEqual(self.broker.snapshot()["fills"][-1]["qty"], 100,
                         "only the 100 shares actually held may be sold")

    def test_an_entry_order_is_never_capped(self):
        self.px("LLL", 50.00)
        self.broker.place(1, ticket("LLL", "SELL", "MKT", 300, 0.0, "PARENT_1_LLL"), True)
        time.sleep(0.3)
        p = self.broker.snapshot()["positions"][0]
        self.assertEqual((p["side"], p["qty"]), ("SHORT", 300),
                         "opening a short is not an over-cover")

    def test_exit_ref_classification(self):
        for ref in ("STPMKT_1_X", "WIDESTP_1_X", "CLOSE_1_X", "TECH_1_X",
                    "STPPRICE_1_X", "STPLMT_1_X", "TARGET_1_X", "SQ_1_X_EOD"):
            self.assertTrue(_is_exit_ref(ref), ref)
        for ref in ("PARENT_1_X", "", None):
            self.assertFalse(_is_exit_ref(ref), ref)


# ─────────────────────────────────────────────────────────────────────────────
class TestPnL(ShadowTestBase):
    """ISSUE: the day's realized total must equal the sum of the trades."""

    def _round_trip(self, sym, side_in, side_out, px_in, px_out, qty=100):
        self.px(sym, px_in)
        self.broker.place(1, ticket(sym, side_in, "MKT", qty, 0.0, f"PARENT_1_{sym}"), True)
        time.sleep(0.3)
        self.px(sym, px_out)
        self.broker.place(2, ticket(sym, side_out, "MKT", qty, 0.0, f"CLOSE_2_{sym}"), True)
        time.sleep(0.4)
        return self.broker.snapshot()

    def test_long_round_trip_nets_both_commissions(self):
        st = self._round_trip("MMM", "BUY", "SELL", 100.00, 105.00)
        t = st["trades"][0]
        self.assertEqual(t["direction"], "LONG")
        self.assertAlmostEqual(t["net_pnl"], t["gross_pnl"] - t["commission"], places=2)
        self.assertAlmostEqual(st["pnl"]["realized"], t["net_pnl"], places=2,
                               msg="day realized must equal the trade's net")

    def test_short_round_trip_is_profitable_when_price_falls(self):
        st = self._round_trip("NNN", "SELL", "BUY", 100.00, 95.00)
        t = st["trades"][0]
        self.assertEqual(t["direction"], "SHORT")
        self.assertGreater(t["net_pnl"], 0)

    def test_realized_equals_sum_of_trades_across_symbols(self):
        self._round_trip("OOO", "BUY", "SELL", 50.00, 52.00)
        st = self._round_trip("PPP", "SELL", "BUY", 80.00, 79.00)
        self.assertAlmostEqual(st["pnl"]["realized"],
                               sum(t["net_pnl"] for t in st["trades"]), places=2)

    def test_unrealized_marks_to_the_live_price(self):
        self.px("QQQ", 10.00)
        self.broker.place(1, ticket("QQQ", "BUY", "MKT", 100, 0.0, "PARENT_1_QQQ"), True)
        time.sleep(0.3)
        avg = self.broker.snapshot()["positions"][0]["avg"]
        self.px("QQQ", 12.00)
        self.assertAlmostEqual(self.broker.unrealized(), (12.00 - avg) * 100, places=2)

    def test_commission_model(self):
        self.assertEqual(_commission(0, 10), 0.0)
        self.assertEqual(_commission(100, 50), 1.00, "the $1 minimum applies")
        self.assertEqual(_commission(500, 50), 2.50, "$0.005/share above the minimum")


# ─────────────────────────────────────────────────────────────────────────────
class TestStopUpdates(ShadowTestBase):
    """ISSUE: every SL change must be recorded with its reason and context."""

    def _open_with_stop(self, sym="RRR"):
        self.px(sym, 100.00)
        self.broker.place(1, ticket(sym, "BUY", "MKT", 100, 0.0, f"PARENT_1_{sym}"), True)
        time.sleep(0.3)
        self.stop_ticket = ticket(sym, "SELL", "STP", 100, 98.00, f"STPMKT_2_{sym}")
        self.broker.place(2, self.stop_ticket, True)
        time.sleep(0.1)

    def test_modify_records_old_new_and_context(self):
        self._open_with_stop()
        self.px("RRR", 103.00)
        self.broker.modify(2, self.stop_ticket, 101.00)
        ev = self.broker.snapshot()["sl_events"][-1]
        self.assertEqual((ev["from"], ev["to"]), (98.00, 101.00))
        self.assertEqual(ev["price_at_move"], 103.00)
        self.assertAlmostEqual(ev["delta"], 3.00, places=2)
        self.assertEqual(ev["symbol"], "RRR")

    def test_position_shows_current_stop_and_move_count(self):
        self._open_with_stop()
        # the market has to lead the stop — a SELL stop above the market is
        # already breached and would (correctly) fire instead of resting
        for mkt, p in ((101.00, 99.00), (102.00, 100.50), (103.00, 101.20)):
            self.px("RRR", mkt, settle=0.05)
            self.broker.modify(2, self.stop_ticket, p)
        pos = self.broker.snapshot()["positions"][0]
        self.assertEqual(pos["stop"], 101.20)
        self.assertEqual(pos["stop_initial"], 98.00)
        self.assertEqual(pos["stop_moves"], 3)
        self.assertEqual([h["to"] for h in pos["stop_history"]], [99.00, 100.50, 101.20])

    def test_locked_flips_positive_once_the_stop_passes_entry(self):
        self._open_with_stop()
        avg = self.broker.snapshot()["positions"][0]["avg"]
        self.px("RRR", round(avg + 4.0, 2), settle=0.05)   # keep the market ahead
        self.broker.modify(2, self.stop_ticket, round(avg - 1.0, 2))
        p = self.broker.snapshot()["positions"][0]
        self.assertGreater(p["risk"], 0, "stop below entry = money still at risk")
        self.broker.modify(2, self.stop_ticket, round(avg + 1.0, 2))
        p = self.broker.snapshot()["positions"][0]
        self.assertGreater(p["locked"], 0, "stop above entry = profit locked in")
        self.assertLess(p["risk"], 0)

    def test_a_repriced_stop_rearms(self):
        """A stop moved ABOVE the market must fire on the next tick, not stay armed
        at its old level or stay dormant."""
        self._open_with_stop()
        self.px("RRR", 103.00)
        self.broker.modify(2, self.stop_ticket, 102.50)
        self.px("RRR", 102.40)
        self.assertEqual(self.broker.snapshot()["positions"], [],
                         "the re-priced stop must trigger at its NEW level")

    def test_modify_on_a_filled_order_is_refused(self):
        self._open_with_stop()
        self.px("RRR", 97.00)                       # stop fills
        self.assertFalse(self.broker.modify(2, self.stop_ticket, 96.00),
                         "a filled stop cannot be re-priced")

    def test_stop_history_survives_onto_the_closed_trade(self):
        self._open_with_stop()
        self.px("RRR", 104.00, settle=0.05)
        self.broker.modify(2, self.stop_ticket, 101.00)
        self.broker.modify(2, self.stop_ticket, 102.00)
        self.px("RRR", 101.90)
        t = self.broker.snapshot()["trades"][0]
        self.assertEqual(t["stop_initial"], 98.00)
        self.assertEqual(t["stop_final"], 102.00)
        self.assertEqual(t["stop_moves"], 2)
        self.assertEqual(len(t["stop_history"]), 2)


# ─────────────────────────────────────────────────────────────────────────────
class TestLiveTrailLogic(unittest.TestCase):
    """ISSUE: the trail must now move on ticks — without ever loosening a stop."""

    def _pos(self, direction="LONG", entry=100.0, stop=98.0):
        return {"symbol": "TRL", "direction": direction, "entry_price": entry,
                "stop_price": stop, "stop_limit_price": stop, "target_price": 0.0,
                "be_activated": False, "max_high": entry, "min_low": entry,
                "high_history": [], "low_history": [], "qty": 100}

    def _cfg(self, **over):
        cfg = {"MPL_dollar_Amount": 2.0, "AdditionalPrintLogs": "N",
               "LiveTrailMinMove": 0.02}
        cfg.update(over)
        return cfg

    def test_live_trail_ratchets_up_on_a_favourable_tick(self):
        pos = self._pos()
        a = exit_logic._intrabar_trail(pos, {"price": 104.0}, self._cfg())
        self.assertEqual(a["action"], "UPDATE_STOPS")
        self.assertGreater(pos["stop_price"], 98.0)

    def test_live_trail_never_widens_on_a_pullback(self):
        pos = self._pos()
        exit_logic._intrabar_trail(pos, {"price": 108.0}, self._cfg())
        tightened = pos["stop_price"]
        a = exit_logic._intrabar_trail(pos, {"price": 101.0}, self._cfg())
        self.assertEqual(a["action"], "NONE")
        self.assertEqual(pos["stop_price"], tightened,
                         "a pullback must not loosen the stop")

    def test_tiny_moves_are_suppressed(self):
        """Without a minimum hop, a fast move would emit dozens of 1-cent
        modifies per second and flood the stop history."""
        pos = self._pos()
        exit_logic._intrabar_trail(pos, {"price": 104.0}, self._cfg())
        first = pos["stop_price"]
        a = exit_logic._intrabar_trail(pos, {"price": 104.01}, self._cfg())
        self.assertEqual(a["action"], "NONE")
        self.assertEqual(pos["stop_price"], first,
                         "the stop must not move for a sub-threshold tick")

    def test_breakeven_is_not_claimed_while_the_stop_is_below_entry(self):
        """The old code flipped be_activated on the first favourable tick, which
        silently bypassed the configured High > Entry + MPL*BEM rule."""
        pos = self._pos()
        exit_logic._intrabar_trail(pos, {"price": 100.60}, self._cfg())
        self.assertLess(pos["stop_price"], pos["entry_price"])
        self.assertFalse(pos["be_activated"],
                         "breakeven must not be claimed with the stop below entry")

    def test_breakeven_is_claimed_once_the_stop_passes_entry(self):
        pos = self._pos()
        exit_logic._intrabar_trail(pos, {"price": 112.0}, self._cfg())
        self.assertGreaterEqual(pos["stop_price"], pos["entry_price"])
        self.assertTrue(pos["be_activated"])

    def test_short_live_trail_ratchets_down_only(self):
        pos = self._pos(direction="SHORT", entry=100.0, stop=102.0)
        exit_logic._intrabar_trail(pos, {"price": 96.0}, self._cfg())
        tightened = pos["stop_price"]
        self.assertLess(tightened, 102.0)
        a = exit_logic._intrabar_trail(pos, {"price": 99.0}, self._cfg())
        self.assertEqual(a["action"], "NONE")
        self.assertEqual(pos["stop_price"], tightened)

    def test_monotonic_guard_blocks_a_loosening_update(self):
        """The bar-close breakeven block assigns its level outright, with no
        ratchet check of its own — the guard is what stops it undoing a live
        trail that already moved further."""
        pos = self._pos()
        pos["stop_price"] = 105.0
        action = exit_logic._make_action("UPDATE_STOPS", "TRL", "LONG",
                                         updates={"stop_price": 99.0},
                                         reason="bar-close BE")
        pos["stop_price"] = 99.0                       # simulate the handler's mutation
        guarded = exit_logic._guard_monotonic_stop(pos, action, 105.0)
        self.assertEqual(guarded["action"], "NONE")
        self.assertEqual(pos["stop_price"], 105.0, "the mutation must be rolled back")

    def test_monotonic_guard_allows_a_tightening_update(self):
        pos = self._pos()
        action = exit_logic._make_action("UPDATE_STOPS", "TRL", "LONG",
                                         updates={"stop_price": 101.0},
                                         reason="trail")
        self.assertEqual(
            exit_logic._guard_monotonic_stop(pos, action, 99.0)["action"],
            "UPDATE_STOPS")

    def test_monotonic_guard_is_direction_aware(self):
        pos = self._pos(direction="SHORT", entry=100.0, stop=98.0)
        action = exit_logic._make_action("UPDATE_STOPS", "TRL", "SHORT",
                                         updates={"stop_price": 99.0}, reason="x")
        self.assertEqual(
            exit_logic._guard_monotonic_stop(pos, action, 98.0)["action"], "NONE",
            "for a short, a HIGHER stop is looser")


# ─────────────────────────────────────────────────────────────────────────────
class TestRealTime(ShadowTestBase):
    """ISSUE: fills and the UI must react to a tick, not to a polling timer."""

    def test_a_tick_fires_a_resting_stop_within_milliseconds(self):
        self.px("SSS", 50.00)
        self.broker.place(1, ticket("SSS", "BUY", "MKT", 100, 0.0, "PARENT_1_SSS"), True)
        time.sleep(0.3)
        self.broker.place(2, ticket("SSS", "SELL", "STP", 100, 49.00, "STPMKT_2_SSS"), True)
        time.sleep(0.15)
        t0 = time.time()
        self.broker.on_price("SSS", last=48.50, bid=48.49, ask=48.51)
        deadline = t0 + 1.0
        while self.broker.snapshot()["positions"] and time.time() < deadline:
            time.sleep(0.005)
        latency = time.time() - t0
        self.assertEqual(self.broker.snapshot()["positions"], [],
                         "the stop must fill on the breaching tick")
        self.assertLess(latency, 0.20,
                        f"tick→fill took {latency*1000:.0f}ms — engine is not event-driven")

    def test_material_events_publish_the_snapshot_immediately(self):
        self.px("TTT", 50.00)
        state = self.broker.ledger.state_path()
        if state.exists():
            state.unlink()
        self.broker.place(1, ticket("TTT", "BUY", "MKT", 100, 0.0, "PARENT_1_TTT"), True)
        time.sleep(0.15)
        self.assertTrue(state.exists(),
                        "an order must republish state_today.json at once")
        with state.open() as fh:
            self.assertEqual(json.load(fh)["mode"], "SHADOW")


# ─────────────────────────────────────────────────────────────────────────────
class TestLedgerAndHistory(ShadowTestBase):
    """ISSUE: any past day must replay exactly."""

    def test_day_replays_with_matching_pnl(self):
        self.px("UUU", 10.00)
        self.broker.place(1, ticket("UUU", "BUY", "MKT", 100, 0.0, "PARENT_1_UUU"), True)
        time.sleep(0.3)
        stop = ticket("UUU", "SELL", "STP", 100, 9.50, "STPMKT_2_UUU")
        self.broker.place(2, stop, True)
        time.sleep(0.1)
        self.broker.modify(2, stop, 10.20)
        self.px("UUU", 10.10)
        live = self.broker.snapshot()
        hist = rebuild_day(_today_str(), base_dir=self.broker.ledger.base_dir)
        self.assertEqual(len(hist["trades"]), len(live["trades"]))
        self.assertEqual(len(hist["sl_events"]), len(live["sl_events"]))
        self.assertAlmostEqual(hist["pnl"]["net"], live["pnl"]["realized"], places=2)

    def test_closed_trades_csv_is_written(self):
        self.px("VVV", 10.00)
        self.broker.place(1, ticket("VVV", "BUY", "MKT", 100, 0.0, "PARENT_1_VVV"), True)
        time.sleep(0.3)
        self.px("VVV", 11.00)
        self.broker.place(2, ticket("VVV", "SELL", "MKT", 100, 0.0, "CLOSE_2_VVV"), True)
        time.sleep(0.4)
        csv_path = self.broker.ledger.trades_path()
        self.assertTrue(csv_path.exists())
        body = csv_path.read_text()
        self.assertIn("VVV", body)
        self.assertIn("net_pnl", body)


# ─────────────────────────────────────────────────────────────────────────────
class TestDashboardContract(unittest.TestCase):
    """ISSUE: the :8080 dashboard must read the shadow as if it were the account."""

    @classmethod
    def setUpClass(cls):
        # Every test writes to one shared log, so record where this class starts
        # and parse only from there — otherwise the parser sees other tests'
        # orders and the round-trip counts are meaningless.
        _fh.flush()
        cls._log_offset = os.path.getsize(_LOG)
        cls.engine = TWSEngine()
        cls.client = LeakGuard()
        cls.client._next_order_id = 7700
        cls.engine._tws = cls.client
        cls.client._engine_on_stop_callback = cls.engine._on_order_status_event
        SHADOW.attach(cls.client)
        SHADOW.on_price("WWW", last=100.40, bid=100.39, ask=100.41)
        cls.engine.submit_bracket(symbol="WWW", conid=9, direction="LONG",
                                  quantity=200, entry_price=100.00,
                                  stop_price=99.00, mpl=1.00)
        SHADOW.on_price("WWW", last=99.95, bid=99.94, ask=99.96)
        time.sleep(1.6)
        cls.engine.trail_stop(symbol="WWW", conid=9, direction="LONG",
                              new_stp_price=100.50, sls=0.03)
        time.sleep(0.3)
        SHADOW.on_price("WWW", last=100.40, bid=100.39, ask=100.41)
        time.sleep(0.5)
        _fh.flush()
        import positions_server as ps
        cls.ps = ps
        cls.recs = {}
        with open(_LOG, encoding="utf-8") as fh:
            fh.seek(cls._log_offset)
            ps._consume_log_chunk(fh.read(), cls.recs, {}, {})

    def test_orders_are_parsed_from_the_log(self):
        self.assertTrue(self.recs, "the dashboard parser must see shadow orders")

    def test_the_trailed_stop_price_is_current_not_original(self):
        stop = next(r for r in self.recs.values()
                    if str(r.get("ref", "")).startswith("STPMKT"))
        self.assertEqual(stop["price"], 100.50)
        self.assertEqual(stop["orig_price"], 99.00)
        self.assertEqual(len(stop["stop_history"]), 1)

    def test_round_trip_pnl_nets_the_entry_commission(self):
        """The panel used to omit the opening commission, so a trade read better
        here than on the Shadow Trader by exactly that amount."""
        trips = self.ps._build_round_trips(self.recs)
        self.assertEqual(len(trips), 1)
        t = trips[0]
        self.assertAlmostEqual(t["ibkr_pnl"], t["pnl"] - t["commission"], places=2)
        self.assertGreater(t["entry_commission"], 0)

    def test_live_price_comes_from_the_same_feed_as_the_fill_engine(self):
        SHADOW.on_price("WWW", last=123.45)
        self.assertEqual(self.ps._live_price("WWW"), 123.45)


if __name__ == "__main__":
    unittest.main(verbosity=2, exit=False)
    print("\nlog used by the dashboard-contract tests:", _LOG)
