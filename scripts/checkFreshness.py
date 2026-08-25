import datetime
import glob
import logging
import os

import redis
import requests

LOG_RETENTION_DAYS = 7
TRITANIUM_TYPE_ID = 34
JITA_4_4_STATION_ID = 60003760

# aggloader-esi.py sets these with ex=5400 (90 minutes) on every successful run,
# so Redis itself expires them once they're stale -- a missing key already means
# "not updated in the last 1.5 hours". Keep this in sync with the `ex=` value
# used in aggloader-esi.py's Redis writes.
KEYS_TO_CHECK = [
    "{}|{}|true".format(JITA_4_4_STATION_ID, TRITANIUM_TYPE_ID),
    "{}|{}|false".format(JITA_4_4_STATION_ID, TRITANIUM_TYPE_ID),
]


def setup_logging():
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    logfile = 'logs/checkFreshness-{}.log'.format(today)
    logging.basicConfig(filename=logfile, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=LOG_RETENTION_DAYS)
    for path in glob.glob('logs/checkFreshness-*.log'):
        datestr = os.path.basename(path)[len('checkFreshness-'):-len('.log')]
        try:
            filedate = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        except ValueError:
            continue
        if filedate < cutoff:
            os.remove(path)


def notify_discord(message):
    webhook_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'discord-webhook.txt')
    if not os.path.exists(webhook_path):
        logging.warning("No discord-webhook.txt found, cannot send alert")
        return
    with open(webhook_path) as f:
        webhook = f.read().strip()
    if not webhook:
        return
    try:
        requests.post(webhook, json={'content': message}, timeout=10)
    except requests.exceptions.RequestException as e:
        logging.error("Failed to notify discord: {}".format(e))


if __name__ == "__main__":
    setup_logging()

    redisdb = redis.StrictRedis()

    missing = [key for key in KEYS_TO_CHECK if redisdb.ttl(key) is None or redisdb.ttl(key) < 0]
    ttls = {key: redisdb.ttl(key) for key in KEYS_TO_CHECK}
    logging.info("Tritanium/Jita 4-4 Redis TTLs: {}".format(ttls))

    if missing:
        last_update = redisdb.get("fp-lastupdate")
        last_update = last_update.decode() if last_update else "unknown"
        message = ("ALERT: Tritanium at Jita 4-4 hasn't been updated in Redis in over 1.5 hours "
                    "(missing key(s): {}). Pipeline's last reported full completion: {}. "
                    "The market data pipeline may be stuck.").format(", ".join(missing), last_update)
        logging.error(message)
        notify_discord(message)
    else:
        logging.info("Tritanium/Jita 4-4 data is fresh (TTLs: {})".format(ttls))
