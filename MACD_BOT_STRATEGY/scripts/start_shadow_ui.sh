#!/bin/bash
# start_shadow_ui.sh — keeps the Shadow Trader dashboard (port 8090) alive.
# Runs as its own process, NOT inside the trading bot, so the page stays up
# when the bot is down — which is exactly when you review a past session.
# Idempotent: cron calls it every 5 minutes and it exits if already running.
cd /home/azureuser/MACD_ENTRY_EXIT || exit 1
pgrep -f "[s]hadow_server.py" >/dev/null && exit 0
screen -dmS ShadowUI bash -c "exec venv/bin/python -u shadow_server.py > logs/shadow_ui.log 2>&1"
echo "$(date) started ShadowUI on :8090"
