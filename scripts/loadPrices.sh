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

source ./.venv/bin/activate
# Exit 75 = ESI unhealthy: the status endpoint is unreachable or reports
# region orders Down, region pages failed after retries, or the fetch
# deadline passed. The loader has already rolled back and published nothing.
# Discord is told once when this starts (the state file records the outage)
# and once on the next fully successful run (see the end of this script), not
# on every run in between. Exiting 0 keeps the systemd unit out of a failed
# state, since this isn't a fault in this script.
ESI_STATE=/opt/pricegetter/esi-unhealthy.state
rc=0
python aggloader-esi.py || rc=$?
if [ "$rc" -eq 75 ]; then
    reason=$(grep '^ESI-UNHEALTHY: ' "$LOGFILE" | tail -n 1 | cut -c16-1200)
    reason=${reason:-unknown (see logs/aggloader-esi-$(date -u +%Y-%m-%d).log)}
    echo "=== $(date -Is) skipped: ESI unhealthy, nothing published: $reason ==="
    if [ ! -e "$ESI_STATE" ]; then
        notify_discord "loadPrices.sh on $(hostname) at $(date -Is): ESI problem, nothing published.
Reason: $reason
Further failures are not reported until it recovers."
        printf '%s %s\n' "$(date -Is)" "$reason" > "$ESI_STATE"
    fi
    exit 0
fi
if [ "$rc" -ne 0 ]; then (exit "$rc"); fi
gzip /opt/orderbooks/*.csv
#rm /tmp/orderset*.csv
mv /tmp/aggregatecsv.csv.gz /opt/web/market/public/
find /opt/orderbooks/ -type f -name "orderset*" -printf "%T@ %p\n"|sort|tail -n1|cut -f 2 -d " "|xargs -i ln -sf {} /opt/orderbooks/latest.csv.gz
find /opt/orderbooks/ -type f -name "orderset*" -printf "%T@ %p\n"|sort|tail -n1|cut -f 2 -d " "|cut -f 1 -d ".">/opt/orderbooks/currentset.txt

# Refresh planner stats on the partition just loaded into. Without this the new
# orderSet value is unknown to the stats and queries on it pick a bad index
# (~500ms instead of ~1ms) until autoanalyze catches up. Samples ~30k rows, <1s.
runuser -u postgres -- psql -d marketdata -X -q -v ON_ERROR_STOP=1 -c "
DO \$\$ DECLARE p text;
BEGIN
    SELECT format('orders_%s', max(id) / 100 % 10) INTO p FROM orderset;
    EXECUTE format('ANALYZE %I', p);
    RAISE NOTICE 'analyzed %', p;
END \$\$;"

if [ -e "$ESI_STATE" ]; then
    since=$(cut -d' ' -f1 "$ESI_STATE")
    notify_discord "loadPrices.sh on $(hostname) at $(date -Is): ESI recovered, prices published again (failing since $since)."
    rm -f "$ESI_STATE"
fi

echo "=== $(date -Is) done ==="
