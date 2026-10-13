"""
flatten_all.py — emergency flatten via direct ibapi connection.
- clientId=99 so it does not conflict with the bot (clientId=1).
- reqGlobalCancel kills all working orders, then sends MKT close for each
  open position, then verifies positions are flat.
"""
import threading
import time
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order

HOST = "127.0.0.1"
PORT = 4002
CLIENT_ID = 99


class Flattener(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self._next_id = None
        self._positions = {}                  # (sym, conid) -> qty (signed)
        self._positions_done = threading.Event()
        self._connected = threading.Event()
        self._order_states = {}               # orderId -> status string
        self._executions_seen = 0

    def nextValidId(self, oid):
        self._next_id = oid
        self._connected.set()
        print(f"[CONN] nextValidId={oid}")

    def get_next_id(self):
        oid = self._next_id
        self._next_id += 1
        return oid

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode in (2104, 2106, 2158, 2107, 2119, 2100, 2103, 2105):
            return                            # benign data-farm messages
        print(f"[ERR] reqId={reqId} code={errorCode}: {errorString}")

    def position(self, account, contract, position, avgCost):
        if position == 0:
            return
        key = (contract.symbol, contract.conId)
        self._positions[key] = (int(position), contract)
        side = "LONG" if position > 0 else "SHORT"
        print(f"[POS] {contract.symbol:6s} {side:5s} qty={abs(int(position))}  conid={contract.conId}")

    def positionEnd(self):
        self._positions_done.set()
        print(f"[POS] positionEnd — total non-zero: {len(self._positions)}")

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice,
                    permId, parentId, lastFillPrice, clientId, whyHeld,
                    mktCapPrice):
        self._order_states[orderId] = status
        print(f"[ORD] oid={orderId} status={status} filled={filled} avg={avgFillPrice}")

    def execDetails(self, reqId, contract, execution):
        self._executions_seen += 1
        print(f"[EXEC] {contract.symbol} {execution.side} qty={execution.shares} px={execution.price}")


def main():
    cli = Flattener()
    cli.connect(HOST, PORT, CLIENT_ID)
    threading.Thread(target=cli.run, daemon=True).start()
    if not cli._connected.wait(timeout=10):
        print("[FATAL] Could not connect to IB Gateway"); return

    # 1. Global cancel ALL working orders on the account
    print("\n[STEP 1] reqGlobalCancel — cancelling all working orders")
    cli.reqGlobalCancel()
    time.sleep(4)                             # let cancels propagate

    # 2. Get current positions
    print("\n[STEP 2] reqPositions")
    cli.reqPositions()
    if not cli._positions_done.wait(timeout=10):
        print("[FATAL] positionEnd not received"); return
    cli.cancelPositions()

    if not cli._positions:
        print("\n[DONE] No open positions to flatten.")
        cli.disconnect()
        return

    # 3. Send MKT close for each position
    print(f"\n[STEP 3] Sending MKT close for {len(cli._positions)} positions")
    closes = []
    for (sym, conid), (qty, base_contract) in cli._positions.items():
        order = Order()
        order.action = "SELL" if qty > 0 else "BUY"
        order.totalQuantity = abs(int(qty))
        order.orderType = "MKT"
        order.tif = "DAY"
        order.transmit = True
        order.eTradeOnly = False              # ibapi v9.85+ default
        order.firmQuoteOnly = False
        # Build a fresh stock contract from the position contract
        c = Contract()
        c.symbol   = sym
        c.secType  = "STK"
        c.currency = "USD"
        c.exchange = "SMART"
        c.primaryExchange = base_contract.primaryExchange or "NASDAQ"
        c.conId    = conid
        oid = cli.get_next_id()
        cli.placeOrder(oid, c, order)
        closes.append((oid, sym, order.action, order.totalQuantity))
        print(f"  -> oid={oid}  {sym:6s} {order.action} qty={order.totalQuantity}")
        time.sleep(0.15)                      # rate-limit

    # 4. Wait for fills
    print(f"\n[STEP 4] Waiting up to 30s for fills...")
    deadline = time.time() + 30
    while time.time() < deadline:
        time.sleep(1)
        filled = sum(1 for oid, _, _, _ in closes
                     if cli._order_states.get(oid) == "Filled")
        if filled >= len(closes):
            print(f"[FILLS] all {filled}/{len(closes)} filled")
            break

    # 5. Verify positions are flat
    print("\n[STEP 5] Re-checking positions...")
    cli._positions = {}
    cli._positions_done.clear()
    cli.reqPositions()
    cli._positions_done.wait(timeout=10)
    cli.cancelPositions()

    if not cli._positions:
        print("\n[SUCCESS] All positions flat.")
    else:
        print(f"\n[WARN] {len(cli._positions)} positions still open:")
        for (sym, _), (qty, _) in cli._positions.items():
            print(f"  {sym:6s} qty={qty}")

    time.sleep(2)
    cli.disconnect()


if __name__ == "__main__":
    main()
