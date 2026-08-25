#!/bin/bash -l

exec 200>/opt/pricegetter/checkFreshness.lock
flock -n 200 || { echo "$(date -Is) checkFreshness.sh already running, exiting" >&2; exit 1; }

cd /opt/pricegetter

mkdir -p logs
find logs -maxdepth 1 -type f -name "checkFreshness-*.log" -mtime +7 -delete
LOGFILE="logs/checkFreshness-$(date +%Y-%m-%d).log"
exec >>"$LOGFILE" 2>&1

source /opt/pricegetter/lib-discord.sh
load_discord_webhook

on_error() {
    local ec=$?
    notify_discord "checkFreshness.sh itself failed on $(hostname) at $(date -Is), line $LINENO, exit $ec (script error, not a data-freshness alert)"
}
trap on_error ERR

set -e
set -o pipefail

echo "=== $(date -Is) starting ==="

source ./pricegetter/bin/activate
python checkFreshness.py

echo "=== $(date -Is) done ==="
