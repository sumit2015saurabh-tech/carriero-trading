#!/bin/bash

DISCORD_WEBHOOK="${DISCORD_WEBHOOK_URL:-}"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M %Z')

# ── Kill any remaining screens ──
REMAINING=$(screen -ls | grep -E "Trade|Excel|Discord" | wc -l)

if [ "$REMAINING" -gt 0 ]; then
    for SCREEN in Trade Excel Discord; do
        if screen -ls | grep -q "$SCREEN"; then
            screen -S "$SCREEN" -X quit
            echo "$SCREEN screen killed"
        fi
    done
    curl -s -X POST "$DISCORD_WEBHOOK" \
        -H "Content-Type: application/json" \
        -d "{\"content\":\"🧹 **Carriero Cleanup** — $TIMESTAMP\n$REMAINING screen(s) were still running and have been killed. VM shutting down in 5 minutes.\"}"
else
    curl -s -X POST "$DISCORD_WEBHOOK" \
        -H "Content-Type: application/json" \
        -d "{\"content\":\"✅ **Carriero Cleanup** — $TIMESTAMP\nNo screens running. VM shutting down in 5 minutes.\"}"
fi

# 2026-05-26: final sweep — kill the bot processes by name regardless of their
# screen name, so an orphan (e.g. a mis-named 'excelsync' screen) can never
# survive into the next session and double-write the config/Excel files.
pkill -f 'macd_bot.py'        2>/dev/null && echo "macd_bot.py process killed"
pkill -f 'excel_sync.py'      2>/dev/null && echo "excel_sync.py process killed"
pkill -f 'discord_monitor.py' 2>/dev/null && echo "discord_monitor.py process killed"
