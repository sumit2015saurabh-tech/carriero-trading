#!/bin/bash
cd /home/azureuser/MACD_ENTRY_EXIT || exit 1
pgrep -f "[d]ata_dashboard.py" >/dev/null && exit 0
screen -dmS DataDash bash -c "exec venv/bin/python -u data_dashboard.py > logs/data_dashboard.log 2>&1"
echo "$(date) started DataDash"
