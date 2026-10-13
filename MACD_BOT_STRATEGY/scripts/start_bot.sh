#!/bin/bash
DISCORD_WEBHOOK="${DISCORD_WEBHOOK_URL:-}"
BOT_DIR="/home/azureuser/MACD_ENTRY_EXIT"
DATE=$(date '+%Y-%m-%d')
TIMESTAMP=$(date '+%Y-%m-%d %H:%M %Z')
LOG_FILE="/home/azureuser/cron_job_logs/startup.log"

notify() {
    echo "[$DATE] $1" >> "$LOG_FILE"
    curl -s -X POST "$DISCORD_WEBHOOK" \
        -H "Content-Type: application/json" \
        -d "{\"content\":\"$1\"}"
}

echo "" >> "$LOG_FILE"
echo "========== $DATE ==========" >> "$LOG_FILE"
notify "🚀 **Carriero** — Startup sequence initiated at $TIMESTAMP"

# ── Step 1: iBeam — NOT REQUIRED for the TWS bot (2026-05-26) ──
# The bot trades via the TWS API (IB Gateway, port 4002). iBeam only served the
# old Client-Portal REST API, which was removed from the bot. The ibeam container
# no longer exists, so the old "docker start ibeam" just produced a confusing
# "No such container" failure every morning. Skip it entirely.
notify "ℹ️ **Carriero** — iBeam skipped (not required for the TWS bot)."

# ── Step 2: Start IB Gateway if not running ──
if ! docker ps --format '{{.Names}}' | grep -q "^ib-gateway$"; then
    notify "⚙️ **Carriero** — IB Gateway not running. Starting now..."
    docker start ib-gateway >> "$LOG_FILE" 2>&1
    sleep 60
    notify "✅ **Carriero** — IB Gateway started."
else
    notify "✅ **Carriero** — IB Gateway already running. Skipping start."
fi

# ── Step 3: Check IB Gateway TWS API ──
# 2026-05-26 fix: was port 4004 via ib_insync — 4004 is the CONTAINER port (Docker
# maps it to host 4002) and ib_insync isn't installed in system python3, so this
# check ALWAYS failed and aborted startup. Now uses the venv's ibapi on host port
# 4002 and waits for nextValidId, which proves the gateway is authenticated and the
# API is ready to trade.
check_ibgateway() {
    "$BOT_DIR/venv/bin/python" -c "
import threading
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
class T(EWrapper, EClient):
    def __init__(self): EClient.__init__(self, self); self.ev=threading.Event()
    def nextValidId(self, oid): self.ev.set()
    def error(self, *a): pass
t=T()
try:
    t.connect('127.0.0.1', 4002, clientId=10)
    threading.Thread(target=t.run, daemon=True).start()
    ok = t.ev.wait(10)
    try: t.disconnect()
    except Exception: pass
    exit(0 if ok else 1)
except Exception:
    exit(1)
" 2>/dev/null
}

if check_ibgateway; then
    notify "✅ **Carriero** — IB Gateway connected successfully."
else
    notify "⚠️ **Carriero** — IB Gateway connection FAILED. Restarting, retrying in 90 seconds..."
    docker restart ib-gateway >> "$LOG_FILE" 2>&1
    sleep 90
    if check_ibgateway; then
        notify "✅ **Carriero** — IB Gateway recovered after restart."
    else
        notify "🚨 **Carriero** — IB Gateway STILL FAILED at $TIMESTAMP. Bot NOT started. Manual intervention needed."
        exit 1
    fi
fi

# ── Step 4: iBeam auth check — SKIPPED (2026-05-26) ──
# IB Gateway authenticates itself (IBC auto-login); Step 3 already confirmed the
# TWS API is live (nextValidId on 4002). The old Client-Portal auth probe on
# https://localhost:5000 is irrelevant to the TWS bot and used to abort startup
# on a false negative. Removed from the critical path.
notify "ℹ️ **Carriero** — iBeam auth check skipped (TWS API already verified in Step 3)."

# ── Step 5: Start screens only if not already running ──
cd "$BOT_DIR"

if ! screen -ls | grep -q "Trade"; then
    # SHADOW MODE (2026-08-06): orders are simulated locally by shadow_broker.py
    # instead of being sent to the IBKR paper account (paper fills were unusable —
    # limit entries never filled and the sim exchange kept dying). Market data
    # still comes from IBKR. Set SHADOW_MODE=0 to send real orders again; the live
    # order code was commented out, not deleted.
    screen -dmS Trade bash -c "cd $BOT_DIR && source venv/bin/activate && export HIST_MIN_INTERVAL=4 && export REENTRY_CAP=0 && export SHADOW_MODE=1 && python3 macd_bot.py"
    notify "▶️ **Carriero** — Trade screen started (macd_bot.py)"
else
    notify "ℹ️ **Carriero** — Trade screen already running, skipped."
fi

if ! screen -ls | grep -q "Excel"; then
    screen -dmS Excel bash -c "cd $BOT_DIR && source venv/bin/activate && python3 excel_sync.py"
    notify "▶️ **Carriero** — Excel screen started (excel_sync.py)"
else
    notify "ℹ️ **Carriero** — Excel screen already running, skipped."
fi

if ! screen -ls | grep -q "Discord"; then
    screen -dmS Discord bash -c "cd $BOT_DIR && source venv/bin/activate && python3 discord_monitor.py"
    notify "▶️ **Carriero** — Discord screen started (discord_monitor.py)"
else
    notify "ℹ️ **Carriero** — Discord screen already running, skipped."
fi

sleep 3

# ── Step 6: Final confirmation ──
RUNNING=$(screen -ls | grep -E "Trade|Excel|Discord" | wc -l)
if [ "$RUNNING" -eq 3 ]; then
    notify "✅ **Carriero Bot FULLY STARTED** — $TIMESTAMP | All 3 screens up. Ready to trade."
else
    notify "⚠️ **Carriero** — Startup complete but only $RUNNING/3 screens running at $TIMESTAMP. Check VM."
fi
