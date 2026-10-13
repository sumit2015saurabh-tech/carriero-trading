import time
import json
import os
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Suppress SSL warnings
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

BASE_URL = "https://localhost:5000/v1/api"
ACCOUNT_ID = "DUXXXXXXX"
TSLA_CONID = 76792991

# Shared session cookie file — same path used by ibkr_broker.py
SESSION_FILE = os.path.join(os.path.expanduser("~"), ".ibkr_session.json")

# Single session used throughout — cookies persist
session = requests.Session()
session.verify = False


def load_cookies():
    """Load saved cookies from the shared session file."""
    if not os.path.exists(SESSION_FILE):
        print(f"No saved session file found at {SESSION_FILE}")
        print("Run ibkr_broker.py first and log in, or log in at https://localhost:5000")
        return False
    try:
        with open(SESSION_FILE) as f:
            cookies = json.load(f)
        for name, info in cookies.items():
            session.cookies.set(
                name,
                info["value"],
                domain=info.get("domain", "localhost"),
                path=info.get("path", "/"),
            )
        print(f"Loaded session cookies from {SESSION_FILE}")
        return True
    except Exception as e:
        print(f"Failed to load cookies: {e}")
        return False


def save_cookies():
    """Save current session cookies to the shared file."""
    try:
        cookies = {}
        for c in session.cookies:
            cookies[c.name] = {
                "value":  c.value,
                "domain": c.domain,
                "path":   c.path,
            }
        with open(SESSION_FILE, "w") as f:
            json.dump(cookies, f)
    except Exception:
        pass


def check_auth():
    """Check if gateway is running and authenticated."""
    # Load saved cookies from ibkr_broker.py or previous runs
    load_cookies()

    try:
        auth = session.get(f"{BASE_URL}/iserver/auth/status", timeout=5)
    except requests.exceptions.ConnectionError:
        print("*** GATEWAY NOT RUNNING ***")
        print("Run: cd clientportal.gw && bash bin/run.sh root/conf.yaml")
        exit(1)

    if auth.status_code == 401:
        print("*** SESSION EXPIRED — Log in at https://localhost:5000 ***")
        exit(1)

    auth_data = auth.json()
    if not auth_data.get("authenticated", False):
        print("*** NOT AUTHENTICATED — Log in at https://localhost:5000 ***")
        exit(1)

    print("Authenticated!")

    # Tickle and save cookies so future scripts can reuse
    tickle = session.post(f"{BASE_URL}/tickle")
    save_cookies()
    if tickle.status_code == 200:
        tickle_data = tickle.json()
        token = tickle_data.get("session")
        if token:
            print(f"Session token active: {token[:16]}...")


def select_account():
    """Select the trading account."""
    acct = session.post(
        f"{BASE_URL}/iserver/account",
        json={"acctId": ACCOUNT_ID}
    )
    print(f"Account: {acct.text}")


def place_order(side="BUY", order_type="LMT", price=200.00, quantity=1):
    """Place an order and return the order ID."""
    order = {
        "acctId": ACCOUNT_ID,
        "conid": TSLA_CONID,
        "side": side,
        "orderType": order_type,
        "quantity": quantity,
        "tif": "DAY"
    }

    if order_type == "LMT":
        order["price"] = price

    print(f"\nPlacing {side} {order_type} order: {quantity} TSLA @ ${price}")

    response = session.post(
        f"{BASE_URL}/iserver/account/{ACCOUNT_ID}/orders",
        json={"orders": [order]}
    )

    print(f"Status: {response.status_code}")
    print(f"Response: {response.text}")

    if response.status_code == 200:
        data = response.json()
        # Handle confirmation
        if isinstance(data, list) and len(data) > 0 and "id" in data[0]:
            reply_id = data[0]["id"]
            print(f"Confirming order (reply ID: {reply_id})...")
            confirm = session.post(
                f"{BASE_URL}/iserver/reply/{reply_id}",
                json={"confirmed": True}
            )
            print(f"Confirm response: {confirm.text}")
            data = confirm.json()

        # Extract order ID from response
        if isinstance(data, list) and len(data) > 0:
            order_id = data[0].get("order_id")
            status = data[0].get("order_status")
            print(f"Order placed! ID: {order_id}, Status: {status}")
            return order_id

    print("Order placement failed.")
    return None


def get_open_tsla_order():
    """Find the open TSLA order and return it."""
    # IBKR sometimes needs two calls
    session.get(f"{BASE_URL}/iserver/account/orders")
    time.sleep(1)
    orders = session.get(f"{BASE_URL}/iserver/account/orders")

    try:
        orders_data = orders.json()
        open_orders = orders_data.get("orders", [])
    except Exception:
        print(f"Error fetching orders: {orders.text}")
        return None

    for o in open_orders:
        if o.get("conid") == TSLA_CONID or o.get("ticker") == "TSLA":
            return o

    return None


def modify_order(order_id, new_price, side="BUY", quantity=1):
    """Modify an existing order."""
    print(f"\nModifying order {order_id}: new price = ${new_price}")

    modified = {
        "acctId": ACCOUNT_ID,
        "conid": TSLA_CONID,
        "orderId": int(order_id),
        "side": side,
        "orderType": "LMT",
        "price": new_price,
        "quantity": quantity,
        "tif": "DAY"
    }

    response = session.put(
        f"{BASE_URL}/iserver/account/{ACCOUNT_ID}/order/{order_id}",
        json=modified
    )

    print(f"Status: {response.status_code}")
    print(f"Response: {response.text}")

    if response.status_code == 200:
        data = response.json()
        if isinstance(data, list) and len(data) > 0 and "id" in data[0]:
            reply_id = data[0]["id"]
            print(f"Confirming modification (reply ID: {reply_id})...")
            confirm = session.post(
                f"{BASE_URL}/iserver/reply/{reply_id}",
                json={"confirmed": True}
            )
            print(f"Confirm response: {confirm.text}")
            print("Order modified successfully!")
            return True

    print("Modification failed.")
    return False


# ============================================================
# MAIN — Everything runs in one session
# ============================================================
if __name__ == "__main__":

    print("=" * 50)
    print("AUTHENTICATING...")
    print("=" * 50)
    check_auth()
    select_account()

    # --- PLACE A LIMIT ORDER ---
    print("\n" + "=" * 50)
    print("PLACING ORDER...")
    print("=" * 50)
    order_id = place_order(
        side="BUY",
        order_type="LMT",
        price=200.00,       # well below market so it stays open
        quantity=1
    )

    if order_id is None:
        print("Could not place order. Exiting.")
        exit(1)

    # Wait a moment for the order to register
    print("\nWaiting 3 seconds for order to register...")
    time.sleep(3)

    # --- FIND THE ORDER ---
    print("\n" + "=" * 50)
    print("FINDING TSLA ORDER...")
    print("=" * 50)
    tsla_order = get_open_tsla_order()

    if tsla_order is None:
        print("No open TSLA order found.")
        exit(1)

    current_price = float(tsla_order.get("price", 0))
    order_id = tsla_order.get("orderId")
    print(f"Found: Order {order_id} @ ${current_price}")

    # --- MODIFY THE ORDER: price - $1 ---
    print("\n" + "=" * 50)
    print("MODIFYING ORDER...")
    print("=" * 50)
    new_price = current_price - 1
    modify_order(
        order_id=order_id,
        new_price=new_price,
        side=tsla_order.get("side", "BUY"),
        quantity=int(tsla_order.get("remainingQuantity", tsla_order.get("totalSize", 1)))
    )