#!/bin/bash -l

exec 200>/opt/pricegetter/loadPrices.lock
flock -n 200 || { echo "$(date -Is) loadPrices.sh already running, exiting" >&2; exit 1; }

cd /opt/pricegetter

mkdir -p logs
find logs -maxdepth 1 -type f -name "loadPrices-*.log" -mtime +7 -delete
LOGFILE="logs/loadPrices-$(date +%Y-%m-%d).log"
exec >>"$LOGFILE" 2>&1

source /opt/pricegetter/lib-discord.sh
load_discord_webhook

on_error() {
    local ec=$?
    local tail_output
    tail_output=$(tail -n 25 "$LOGFILE" 2>/dev/null | cut -c1-1200)
    notify_discord "loadPrices.sh failed on $(hostname) at $(date -Is), line $LINENO, exit $ec

Last log lines:
$tail_output"
}
trap on_error ERR

set -e
set -o pipefail

echo "=== $(date -Is) starting ==="

source ./pricegetter/bin/activate
python aggloader-esi.py
gzip /opt/orderbooks/*.csv
#rm /tmp/orderset*.csv
mv /tmp/aggregatecsv.csv.gz /opt/web/market/public/
find /opt/orderbooks/ -type f -name "orderset*" -printf "%T@ %p\n"|sort|tail -n1|cut -f 2 -d " "|xargs -i ln -sf {} /opt/orderbooks/latest.csv.gz
find /opt/orderbooks/ -type f -name "orderset*" -printf "%T@ %p\n"|sort|tail -n1|cut -f 2 -d " "|cut -f 1 -d ".">/opt/orderbooks/currentset.txt

echo "=== $(date -Is) done ==="
