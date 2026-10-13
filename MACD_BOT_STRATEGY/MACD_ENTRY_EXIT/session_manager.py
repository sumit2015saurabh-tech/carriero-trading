"""
Session Manager for IBKR ibeam-only Setup
==========================================
Architecture:
  - ibeam (CP REST API on port 5000) handles EVERYTHING:
    - Order placement via /iserver/account/{id}/orders
    - Market data via /iserver/marketdata/snapshot (polled)
    - No ib-gateway — no competing session issues

Responsibilities:
  1. Ensure ibeam is running and authenticated
  2. Tickle every 50s to keep CP session alive
  3. Write session state to file
  4. Detect drops and reauthenticate
"""

import subprocess, time, json, os, sys, requests, logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SESSION_MGR] %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/home/azureuser/MACD_ENTRY_EXIT/logs/session_manager.log"),
    ],
)
logger = logging.getLogger(__name__)

SESSION_FILE    = "/home/azureuser/MACD_ENTRY_EXIT/ibkr_session.json"
CP_BASE         = "https://localhost:5000/v1/api"
TICKLE_INTERVAL = 50
MAX_AUTH_WAIT   = 300


def cp_get(path, timeout=8):
    try:
        r = requests.get(f"{CP_BASE}{path}", verify=False, timeout=timeout)
        return r.json() if r.ok else None
    except Exception:
        return None


def cp_post(path, timeout=8):
    try:
        r = requests.post(f"{CP_BASE}{path}", verify=False, timeout=timeout)
        return r.json() if r.ok else None
    except Exception:
        return None


def write_session_file(state: dict):
    state["updated_at"] = datetime.utcnow().isoformat()
    with open(SESSION_FILE, "w") as f:
        json.dump(state, f, indent=2)


def container_running(name):
    result = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        capture_output=True, text=True
    )
    return result.stdout.strip() == "true"


def ensure_ibeam_running():
    if container_running("ibeam"):
        return True
    logger.info("Starting ibeam container...")
    result = subprocess.run(["docker", "start", "ibeam"], capture_output=True, text=True)
    if result.returncode == 0:
        time.sleep(10)
        return True
    logger.error("Could not start ibeam: %s", result.stderr.strip())
    return False


def wait_for_ibeam_auth(timeout=MAX_AUTH_WAIT):
    logger.info("Waiting up to %ds for ibeam to authenticate...", timeout)
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = cp_get("/iserver/auth/status")
        if status and status.get("authenticated") and status.get("connected"):
            logger.info("ibeam authenticated ✓  competing=%s", status.get("competing"))
            return True
        time.sleep(5)
    logger.error("ibeam not authenticated after %ds", timeout)
    return False


def tickle():
    try:
        r = requests.post(f"{CP_BASE}/tickle", verify=False, timeout=8)
        return r.ok
    except Exception:
        return False


def main():
    import urllib3
    urllib3.disable_warnings()

    logger.info("=" * 60)
    logger.info("IBKR Session Manager (ibeam-only) starting")
    logger.info("=" * 60)

    # Phase 1: Ensure ibeam is running and authenticated
    ensure_ibeam_running()
    if not wait_for_ibeam_auth():
        logger.error("ibeam not authenticated. Exiting.")
        sys.exit(1)

    # Phase 2: Write ready state
    write_session_file({
        "cp_port": 5000,
        "cp_authenticated": True,
        "tws_port": None,
        "tws_ready": False,
        "status": "ready",
    })
    logger.info("Session ready — entering keep-alive loop")

    # Phase 3: Keep-alive loop
    last_tickle = time.time()
    while True:
        now = time.time()

        # Tickle every 50s
        if now - last_tickle >= TICKLE_INTERVAL:
            ok = tickle()
            if ok:
                logger.info("Tickle OK — CP session alive")
            else:
                logger.warning("Tickle FAILED — checking auth")
                status = cp_get("/iserver/auth/status")
                if not (status and status.get("authenticated")):
                    logger.warning("Session lost — reauthenticating")
                    ensure_ibeam_running()
                    wait_for_ibeam_auth()
            last_tickle = now

        # Monitor ibeam container
        if not container_running("ibeam"):
            logger.warning("ibeam container stopped — restarting")
            ensure_ibeam_running()
            wait_for_ibeam_auth()

        # Write updated state
        status = cp_get("/iserver/auth/status") or {}
        write_session_file({
            "cp_port": 5000,
            "cp_authenticated": status.get("authenticated", False),
            "competing": status.get("competing", False),
            "tws_port": None,
            "tws_ready": False,
            "status": "running",
        })

        time.sleep(10)


if __name__ == "__main__":
    main()
