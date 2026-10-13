#!/bin/bash

DISCORD_WEBHOOK="${DISCORD_WEBHOOK_URL:-}"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M %Z')

# ── Kill all 3 screens ──
for SCREEN in Trade Excel Discord; do
    if screen -ls | grep -q "$SCREEN"; then
        screen -S "$SCREEN" -X quit
        echo "$SCREEN screen stopped"
    else
        echo "$SCREEN screen was not running"
    fi
done

# 2026-05-26: belt-and-suspenders — kill by process name too, so an orphan
# launched under a different screen name (e.g. 'excelsync') can't survive the
# name-based quit above and collide with the next 9:10 start.
pkill -f 'macd_bot.py'        2>/dev/null && echo "macd_bot.py process killed"
pkill -f 'excel_sync.py'      2>/dev/null && echo "excel_sync.py process killed"
pkill -f 'discord_monitor.py' 2>/dev/null && echo "discord_monitor.py process killed"

sleep 2

# ── Confirm all stopped ──
REMAINING=$(screen -ls | grep -E "Trade|Excel|Discord" | wc -l)
if [ "$REMAINING" -eq 0 ]; then
    curl -s -X POST "$DISCORD_WEBHOOK" \
        -H "Content-Type: application/json" \
        -d "{\"content\":\"🛑 **Carriero Bot STOPPED** — $TIMESTAMP\nAll screens closed.\"}"
else
    curl -s -X POST "$DISCORD_WEBHOOK" \
        -H "Content-Type: application/json" \
        -d "{\"content\":\"⚠️ **Carriero** — Stop ran at $TIMESTAMP but $REMAINING screen(s) still running. Check VM.\"}"
fi
