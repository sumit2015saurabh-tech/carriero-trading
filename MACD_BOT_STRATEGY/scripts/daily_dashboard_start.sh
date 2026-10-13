#!/bin/bash
# Daily 8:45 ET dashboard bring-up + FORCED 2FA.
# The DATA gateway (ib-gateway-data / U391957, port 4003) does its full re-auth at
# IBKR's overnight server reset (~23:45 ET) and fires a 2FA push then -- which is
# missed overnight and IBC does NOT re-prompt (ReloginAfterSecondFactorAuthenticationTimeout
# is not set). So we OWN the morning re-auth here: a COLD `docker restart` makes IBC
# find no autorestart token -> "full authentication will be required" -> a fresh 2FA
# push to IBKR Mobile. We RE-FIRE that cold restart (new push) until the user approves
# ("Login has completed"), so the 2FA prompt reliably lands at 8:45 and the feed ends
# up authenticated for the day. Touches ONLY the data gateway (4003) + the DataDash
# dashboard (8081) -- the trading bot / trade gateway (4002) and trading hours are untouched.
LOG=/home/azureuser/cron_job_logs/daily_dashboard.log
GW=ib-gateway-data
MAX_ATTEMPTS=8          # up to ~8 pushes spanning ~8:45 -> ~9:15
echo "===== $(date '+%F %H:%M:%S %Z') daily dashboard start (force 2FA @ 8:45) =====" >> "$LOG"

login_ok=0
for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  echo "$(date +%H:%M:%S) attempt ${attempt}/${MAX_ATTEMPTS}: COLD restart $GW -> forces full auth / 2FA push" >> "$LOG"
  docker restart "$GW" >> "$LOG" 2>&1

  # (a) wait up to ~120s for IBC to actually send the push this attempt
  for i in $(seq 1 24); do
    if docker logs "$GW" --since 130s 2>&1 | grep -q "Second Factor Authentication initiated"; then
      echo "$(date +%H:%M:%S)   2FA push initiated -- approve on IBKR Mobile" >> "$LOG"; break
    fi
    sleep 5
  done

  # (b) wait up to ~210s (> SecondFactorAuthenticationTimeout=180) for the user to approve
  for i in $(seq 1 42); do
    if docker logs "$GW" --since 400s 2>&1 | grep -q "Login has completed"; then
      login_ok=1; break
    fi
    sleep 5
  done

  if [ "$login_ok" = "1" ]; then
    echo "$(date +%H:%M:%S)   Login has completed (2FA approved) on attempt ${attempt}" >> "$LOG"
    break
  fi
  echo "$(date +%H:%M:%S)   no login yet (push missed/expired) -- re-firing" >> "$LOG"
done

if [ "$login_ok" != "1" ]; then
  echo "$(date +%H:%M:%S) WARNING: 2FA still not approved after ${MAX_ATTEMPTS} attempts" >> "$LOG"
fi

# wait for the API port, then (re)start the dashboard against the now-authenticated feed
for i in $(seq 1 36); do
  if timeout 2 bash -c "</dev/tcp/127.0.0.1/4003" 2>/dev/null; then
    echo "$(date +%H:%M:%S) 4003 up after ${i} tries" >> "$LOG"; break
  fi
  sleep 5
done
sleep 20   # let managedAccounts + market-data subscription settle

cd /home/azureuser/MACD_ENTRY_EXIT
for sid in $(screen -ls 2>/dev/null | grep -oE "[0-9]+\.DataDash"); do screen -S "$sid" -X quit; done
pkill -9 -f "[p]ython.*data_dashboard"
sleep 3
screen -wipe >/dev/null 2>&1
bash /home/azureuser/scripts/start_data_dashboard.sh >> "$LOG" 2>&1
sleep 14
echo "$(date +%H:%M:%S) login_ok=${login_ok} dashboard procs=$(pgrep -f "[p]ython.*data_dashboard" | wc -l) port=$(curl -s -o /dev/null -w "%{http_code}" --max-time 6 http://127.0.0.1:8081)" >> "$LOG"
