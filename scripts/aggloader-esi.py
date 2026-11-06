from sqlalchemy import create_engine, Column, MetaData, Table, Index, text
from sqlalchemy import Integer, String, Text, Float, Boolean, BigInteger, Numeric, SmallInteger, DateTime
import time
import requests
from requests_futures.sessions import FuturesSession
import requests_futures
from concurrent.futures import as_completed
import datetime
import csv
import time
import sys
import re
import pandas
import numpy
import redis
import json
import os
import shutil
import base64
import atexit

import gzip
import glob
from io import StringIO
from six.moves import configparser
import traceback


import logging

REQUEST_TIMEOUT = 30
LOG_RETENTION_DAYS = 7

# Exit code meaning "ESI is unhealthy, nothing was published, try next run".
# loadPrices.sh treats this as a clean skip (no Discord alert); the
# checkFreshness canary is what alerts if it persists past 90 minutes.
EXIT_ESI_UNHEALTHY = 75

ESI_STATUS_URL = 'https://esi.evetech.net/meta/status'
REGION_ORDERS_ROUTE = '/markets/{region_id}/orders'
STRUCTURE_ORDERS_ROUTE = '/markets/structures/{structure_id}'

# Back off (pausing new requests until the window resets) once ESI's error
# budget drops to this many remaining errors, rather than running into 420s.
ERROR_LIMIT_FLOOR = 20
# Hard cap on the whole fetch phase. Runs are 30 min apart; past this we give
# up rather than publish late or pile up behind the flock.
FETCH_DEADLINE_SECONDS = 20 * 60

# Shared throttle state: wall-clock time before which no new request batch is sent.
throttle = {'pause_until': 0.0}


class ESIUnhealthy(Exception):
    pass


def noteRateHeaders(resp):
    """Update the shared pause from ESI's error-limit / rate-limit headers."""
    h = resp.headers
    pause = 0
    try:
        if resp.status_code in (420, 429):
            pause = int(h.get('Retry-After') or h.get('X-ESI-Error-Limit-Reset') or 60)
        elif 'X-ESI-Error-Limit-Remain' in h and int(h['X-ESI-Error-Limit-Remain']) <= ERROR_LIMIT_FLOOR:
            pause = int(h.get('X-ESI-Error-Limit-Reset') or 60)
    except ValueError:
        pause = 60
    if pause > 0:
        until = time.time() + pause + 1
        if until > throttle['pause_until']:
            logging.warning("ESI error/rate limit hit (status {}, remain {}); pausing new requests for {}s".format(
                resp.status_code, h.get('X-ESI-Error-Limit-Remain'), pause + 1))
            throttle['pause_until'] = until


def waitForThrottle(deadline):
    wait = throttle['pause_until'] - time.time()
    if wait > 0:
        if time.time() + wait > deadline:
            raise ESIUnhealthy('error-limit pause of {:.0f}s would pass the fetch deadline'.format(wait))
        logging.warning("Waiting {:.0f}s for ESI error limit to reset".format(wait))
        time.sleep(wait)


def getESIStatus(useragent, compatdate):
    """Return {route path: status} for GET routes from ESI's /meta/status
    (the API behind developers.eveonline.com/status), or None if it can't be
    fetched. The caller treats None as "ESI is down"."""
    try:
        r = requests.get(ESI_STATUS_URL, headers={'User-Agent': useragent, 'X-Compatibility-Date': compatdate},
                         timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return {x['path']: x['status'] for x in r.json()['routes'] if x.get('method') == 'GET'}
    except Exception as e:
        logging.error("Could not fetch ESI status ({})".format(e))
        return None

def setupLogging(debug=False):
    today = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d')
    logfile = 'logs/aggloader-esi-{}.log'.format(today)
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(filename=logfile,level=level,format='%(asctime)s %(levelname)s %(message)s')

    cutoff = datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=LOG_RETENTION_DAYS)
    for path in glob.glob('logs/aggloader-esi-*.log'):
        datestr = os.path.basename(path)[len('aggloader-esi-'):-len('.log')]
        try:
            filedate = datetime.datetime.strptime(datestr,'%Y-%m-%d').replace(tzinfo=datetime.UTC)
        except ValueError:
            continue
        if filedate < cutoff:
            os.remove(path)



    
def processData(result,orderwriter,ordersetid,connection,orderTable):
    
    try:
        resp=result.result()
        noteRateHeaders(resp)
        regionid=result.region
        logging.info('Process {} {} {} {}'.format(resp.status_code,result.url,result.retry,result.region))
        if resp.status_code==200:
            try:
                orders=resp.json()
                logging.info('{} orders on page {} {}'.format(len(orders),result.fullurl,result.page))
                for order in orders:
                    if not result.structure and int(order['location_id'])>100000000 and order['is_buy_order']:
                        pass
                    else:
                        ordersWritten['count']+=1
                        orderwriter.writerow([order['order_id'],
                                            order['type_id'],
                                            order['issued'],
                                            order['is_buy_order'],
                                            order['volume_remain'],
                                            order['volume_total'],
                                            order['min_volume'],
                                            order['price'],
                                            order['location_id'],
                                            order['range'],
                                            order['duration'],
                                            regionid,
                                            ordersetid]
                                        )

                if len(orders)>0:
                    if int(result.page) < int(resp.headers['X-Pages']):
                        logging.info('{}'.format(resp.headers['X-Pages']))
                        nextpage=result.url
                    else:
                        nextpage=None
                else:
                    nextpage=None
                logging.info('{}: next page {}'.format(result.url,nextpage))
                return {'retry':0,'url':nextpage,'region':result.region,'page':result.page+1,'structure':result.structure}
            except Exception as inst:
                logging.error("URL: {} could not be parsed".format(result.url))
                logging.error("{} {} {} {}".format(type(inst),inst.args,inst,traceback.format_exc()))
                file = open("logs/{}-{}.txt".format(result.region,result.page),"wb")
                file.write(resp.content)
                file.close()
        elif resp.status_code==403:
            logging.error("403 status. {} returned {}".format(resp.url,resp.status_code))
            return {'retry':4}
        elif resp.status_code==404:
            logging.error("404 status. {} returned {}".format(resp.url,resp.status_code))
            return {'retry':4}
        elif resp.status_code in (420,429):
            # Throttled, not a real attempt: requeue at the same retry count.
            # The pause itself is applied once, before the next batch (see
            # noteRateHeaders/waitForThrottle), not per response.
            logging.error("{} status (throttled). {} will be retried after the pause".format(resp.status_code,resp.url))
            return {'retry':result.retry,'url':result.url,'region':result.region,'page':result.page,'structure':result.structure}
        else:
            logging.error("Non 200 status. {} returned {} on retry {}".format(resp.url,resp.status_code,result.retry))
            return {'retry':result.retry+1,'url':result.url,'region':result.region,'page':result.page,'structure':result.structure}
    except requests.exceptions.ConnectionError as e:
        logging.error(e)
        return {'retry':result.retry+1,'url':result.url,'region':result.region,'page':result.page,'structure':result.structure}
    return {'retry':result.retry+1,'url':result.url,'region':result.region,'page':result.page,'structure':result.structure}
    
    
    


def getData(requestsConnection,url,retry,page,region,structure):
    future=requestsConnection.get(url+str(page),timeout=REQUEST_TIMEOUT)
    logging.info('getting {}#{}#{}#{}'.format(retry,page,region,url+str(page)))
    future.url=url
    future.fullurl=url+str(page)
    future.page=page
    future.retry=retry
    future.region=region
    future.structure=structure
    return future


ordersWritten = {'count': 0}


def fetchAll(session,urls,maxtries,orderwriter,ordersetid,connection,orderTable,deadline):
    """Fetch every url (following pagination), retrying failed pages up to
    maxtries attempts in total with a short backoff between rounds, and
    honouring ESI's error limit. Returns the list of pages that never
    succeeded. Raises ESIUnhealthy if the deadline passes."""
    failed=[]
    while len(urls)>0:
        if time.time() > deadline:
            raise ESIUnhealthy('fetch deadline passed with {} page(s) outstanding'.format(len(urls)))
        waitForThrottle(deadline)
        maxretry=max(u['retry'] for u in urls)
        if maxretry>0:
            backoff=min(5*(2**(maxretry-1)),30)
            logging.warning("Backing off {}s before retry round {}".format(backoff,maxretry))
            time.sleep(backoff)
        futures=[]
        logging.warning("Loop restarting {} ({} requests)".format(ordersetid,len(urls)));
        for url in urls:
            logging.info('URL:{}  Retry:{} page:{}'.format(url['url'],url['retry'],url['page']));
            futures.append(getData(session,url['url'],url['retry'],url['page'],url['region'],url['structure']))
        urls=[]
        for result in as_completed(futures):
            if result.cancelled():
                continue
            presult=processData(result,orderwriter,ordersetid,connection,orderTable)
            if presult['retry'] == 0 and presult['url'] is not None:
                logging.info('{} has more pages. {}'.format(result.url,presult['retry']))
                urls.append(presult)
            elif 0 < presult['retry'] < maxtries:
                urls.append(presult)
                logging.info("adding {} to retry {}".format(result.url,presult['retry']))
            elif presult['retry'] == maxtries:
                failed.append(presult)
            # retry > maxtries is the 403/404 "don't retry" marker
            if throttle['pause_until'] > time.time():
                # Stop sending the rest of this batch into the error limit;
                # requeue anything not yet started at its current retry count.
                for f in futures:
                    if f.cancel():
                        urls.append({'retry':f.retry,'url':f.url,'region':f.region,'page':f.page,'structure':f.structure})
    return failed


def esiUnhealthyExit(reason):
    """Exit with EXIT_ESI_UNHEALTHY. The 'ESI-UNHEALTHY:' stderr line lands in
    the loadPrices log, and loadPrices.sh puts it in the Discord report."""
    logging.error("ESI unhealthy, nothing published: {}".format(reason))
    print("ESI-UNHEALTHY: {}".format(reason), file=sys.stderr, flush=True)
    sys.exit(EXIT_ESI_UNHEALTHY)


def abortRun(trans,reason):
    """Roll back the orderset (nothing is published) and exit with
    EXIT_ESI_UNHEALTHY. The atexit hook removes the staging CSV."""
    trans.rollback()
    esiUnhealthyExit(reason)


if __name__ == "__main__":
    debug = '--debug' in sys.argv
    setupLogging(debug)

    fileLocation = os.path.dirname(os.path.realpath(__file__))
    inifile=fileLocation+'/esi.cfg'

    config = configparser.ConfigParser()
    config.read(inifile)

    clientid=config.get('oauth','clientid')
    secret=config.get('oauth','secret')
    refreshtoken=config.get('oauth','refreshtoken')

    reqs_num_workers=config.getint('requests','max_workers')
    useragent=config.get('requests','useragent')

    connectionstring=config.get('database','connectionstring')

    engine = create_engine(connectionstring, echo=False)
    metadata = MetaData()
    connection = engine.connect()
    

    compatdate = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=11)).strftime('%Y-%m-%d')

    session = FuturesSession(max_workers=reqs_num_workers)
    session.headers.update({'User-Agent':useragent,'X-Compatibility-Date':compatdate});
    orderTable = Table('orders',metadata,
                            Column('id',Integer,primary_key=True, autoincrement=True),
                            Column('orderID',BigInteger, primary_key=False,autoincrement=False),
                            Column('typeID',Integer),
                            Column('issued',DateTime),
                            Column('buy',Boolean),
                            Column('volume',BigInteger),
                            Column('volumeEntered',BigInteger),
                            Column('minVolume',BigInteger),
                            Column('price',Numeric(scale=4,precision=19)),
                            Column('stationID',BigInteger),
                            Column('range',String(12)),
                            Column('duration',Integer),
                            Column('region',Integer),
                            Column('orderSet',BigInteger)
                            )
                            
    Index("orders_1",orderTable.c.typeID)
    Index("orders_2",orderTable.c.typeID,orderTable.c.buy)
    Index("orders_5",orderTable.c.region,orderTable.c.typeID,orderTable.c.buy)
    Index("orders_6",orderTable.c.region)


    orderSet=Table('orderset',metadata,
                    Column('id',BigInteger,primary_key=True, autoincrement=True),
                    Column('downloaded',DateTime)
                )



    #metadata.create_all(engine,checkfirst=True)

    urls=[]

    regionids = connection.execute(text('select distinct "regionID" from evesde."staStations" where "stationID"<100000000 order by 1')).fetchall()
    for row in regionids:
        regionid = row[0]
        urls.append({'url':"https://esi.evetech.net/markets/{}/orders/?order_type=all&datasource=tranquility&page=".format(regionid),'retry':0,'page':1,'region':regionid,'structure':0})

    # Virtual PLEX Market region - not a real space region, so it has no entry in
    # evesde.staStations and gets skipped by the loop above unless added explicitly.
    urls.append({'url':"https://esi.evetech.net/markets/19000001/orders/?order_type=all&datasource=tranquility&page=",'retry':0,'page':1,'region':19000001,'structure':0})

    deadline = time.time() + FETCH_DEADLINE_SECONDS
    esistatus = getESIStatus(useragent, compatdate)
    if esistatus is None:
        esiUnhealthyExit("ESI status endpoint {} unreachable; assuming ESI is down".format(ESI_STATUS_URL))
    logging.warning("ESI status: region orders {}, structure orders {}".format(
        esistatus.get(REGION_ORDERS_ROUTE), esistatus.get(STRUCTURE_ORDERS_ROUTE)))
    if esistatus.get(REGION_ORDERS_ROUTE) == 'Down':
        esiUnhealthyExit("ESI status reports {} as Down".format(REGION_ORDERS_ROUTE))

    # SQLAlchemy 2 starts a transaction on the first statement (the region
    # list above). End it so the orderset gets a transaction of its own.
    if connection.in_transaction():
        connection.commit()
    trans = connection.begin()

    # UTC, stored without an offset: the column is timestamp without time zone.
    connection.execute(orderSet.insert(),{'downloaded':datetime.datetime.now(datetime.UTC).replace(tzinfo=None).isoformat()})

    result=connection.execute(text("select currval('orderset_id_seq')")).fetchone()

    ordersetid=result[0]



    csvpath = '/tmp/orderset-{}.csv'.format(ordersetid)

    def _cleanup_csv(path=csvpath):
        # Only still present if the run aborted before the final shutil.move
        # to /opt/orderbooks/ at the end of a successful run.
        if os.path.exists(path):
            logging.warning("Removing incomplete staging file {} after failed run".format(path))
            os.remove(path)
    atexit.register(_cleanup_csv)

    csvfd = os.open(csvpath, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(csvfd, 'w') as csvfile:
        orderwriter = csv.writer(csvfile,quoting=csv.QUOTE_MINIMAL,delimiter="\t")
        try:
            failed = fetchAll(session,urls,3,orderwriter,ordersetid,connection,orderTable,deadline)
        except ESIUnhealthy as e:
            abortRun(trans,'region fetch aborted: {}'.format(e))
        if failed:
            # A missing region silently skews the region and universe
            # aggregates, so a partial region set is never published.
            abortRun(trans,'{} region page(s) failed after retries, e.g. {}'.format(
                len(failed),', '.join('{}p{}'.format(f['region'],f['page']) for f in failed[:5])))
        urls=[]
        
        if esistatus.get(STRUCTURE_ORDERS_ROUTE) == 'Down':
            logging.warning("ESI reports {} as Down; skipping structure markets this run".format(STRUCTURE_ORDERS_ROUTE))
            fetchStructures = False
        else:
            fetchStructures = True
        if fetchStructures:
            # Get authorization
            token=clientid+':'+secret
            message_bytes = token.encode('ascii')
            base64_bytes = base64.b64encode(message_bytes)
            base64_message = base64_bytes.decode('ascii')
            headers = {'Authorization':'Basic '+ base64_message,'User-Agent':useragent,"Content-Type": "application/x-www-form-urlencoded"}
            query = {'grant_type':'refresh_token','refresh_token':refreshtoken}

            response = None
            for ssoretry in range(3):
                try:
                    r = requests.post('https://login.eveonline.com/v2/oauth/token',data=query,headers=headers,timeout=REQUEST_TIMEOUT)
                    response = r.json()
                    if 'access_token' not in response:
                        raise ValueError('SSO response missing access_token: {}'.format(response))
                    break
                except (requests.exceptions.RequestException, ValueError) as e:
                    logging.error("SSO token refresh failed on attempt {}: {}".format(ssoretry+1,e))
                    response = None
                    if ssoretry < 2:
                        time.sleep(5)
            if response is None:
                raise RuntimeError('SSO token refresh failed after 3 attempts; aborting before structure market fetch')

            accesstoken = response['access_token']
            refreshtokennew = response['refresh_token']
            if refreshtokennew != refreshtoken:
                cfgfile = open(inifile,'w')
                config.set('oauth','refreshtoken',refreshtokennew)
                config.write(cfgfile)
                cfgfile.close()

            logging.debug("Access Token {}".format(accesstoken))
            logging.debug("refresh Token {}".format(refreshtokennew))




            session.headers.update({'Authorization':'Bearer '+accesstoken,'X-Compatibility-Date':compatdate});
        
            results=connection.execute(text('select "stationID",mss."regionID" from evesde."staStations" sta join evesde."mapSolarSystems" mss on mss."solarSystemID"=sta."solarSystemID"  where "stationID">100000000')).fetchall()
            for result in results:
                urls.append({'url':"https://esi.evetech.net/markets/structures/{}/?&datasource=tranquility&page=".format(result[0]),'retry':0,'page':1,'region':result[1],'structure':1})
        
        
            try:
                failed = fetchAll(session,urls,2,orderwriter,ordersetid,connection,orderTable,deadline)
            except ESIUnhealthy as e:
                # Region data is complete at this point; structures only add
                # structure buy orders, so losing them is a degraded-but-usable set.
                logging.error("Structure fetch aborted ({}); publishing without remaining structure orders".format(e))
                failed = None
            if failed:
                logging.warning("{} structure page(s) failed after retries; their buy orders are missing from orderset {}".format(len(failed),ordersetid))

    if ordersWritten['count'] == 0:
        abortRun(trans,'no orders fetched at all')
    logging.warning("{} orders fetched for orderset {}".format(ordersWritten['count'],ordersetid))

    logging.warning("Loading Data File {}".format(ordersetid));
    connection.execute(text("""copy orders_{}("orderID","typeID",issued,buy,volume,"volumeEntered","minVolume",price,"stationID",range,duration,region,"orderSet") from '/tmp/orderset-{}.csv'""".format(int((int(ordersetid)/100)%10),ordersetid)))
    logging.warning("Complete load {}".format(ordersetid));
    trans.commit()
    


    logging.warning("Pandas populating sell {}".format(ordersetid));
    
    sell=pandas.read_sql_query("""select region||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders  where "orderSet"={} and buy=False group by region,"typeID",buy,price order by region,"typeID",price asc""".format(ordersetid),connection);
    logging.warning("Pandas populating buy {}".format(ordersetid));
    buy=pandas.read_sql_query("""select region||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders  where "orderSet"={} and buy=True group by region,"typeID",buy,price order by region,"typeID",price desc""".format(ordersetid),connection);
    logging.warning("Pandas populated {}".format(ordersetid));


    logging.warning("Sell Math running {}".format(ordersetid));
    sell['min']=sell.groupby('what')['price'].transform('min')
    sell['volume']=sell['volume'].where(sell['price']<=sell['min']*100, 0)
    sell['cumsum']=sell.groupby('what')['volume'].cumsum()
    sell['fivepercent']=sell.groupby('what')['volume'].transform('sum')/20
    sell['lastsum']=sell.groupby('what')['cumsum'].shift(1)
    sell.fillna(0,inplace=True)
    sell['applies']=numpy.where(sell['cumsum']<=sell['fivepercent'], sell['volume'], sell['fivepercent']-sell['lastsum'])
    # Assign back explicitly: with copy-on-write (pandas 3) the numeric subset
    # is a copy, so writing into it would leave sell unchanged.
    num = sell._get_numeric_data().columns
    sell[num] = sell[num].clip(lower=0)
    sell['applies']=sell['applies'].mask(sell.groupby('what')['applies'].transform('sum')==0, 0.01)
    sell['weight']=sell['volume'].mask(sell.groupby('what')['volume'].transform('sum')==0, 0.01)
    logging.warning("Buy Math running {}".format(ordersetid));
    buy['max']=buy.groupby('what')['price'].transform('max')
    buy['volume']=buy['volume'].where(buy['price']>=buy['max']/100, 0)
    buy['cumsum']=buy.groupby('what')['volume'].cumsum()
    buy['fivepercent']=buy.groupby('what')['volume'].transform('sum')/20
    buy['lastsum']=buy.groupby('what')['cumsum'].shift(1)
    buy.fillna(0,inplace=True)
    buy['applies']=numpy.where(buy['cumsum']<=buy['fivepercent'], buy['volume'], buy['fivepercent']-buy['lastsum'])
    # Assign back explicitly: with copy-on-write (pandas 3) the numeric subset
    # is a copy, so writing into it would leave buy unchanged.
    num = buy._get_numeric_data().columns
    buy[num] = buy[num].clip(lower=0)
    buy['applies']=buy['applies'].mask(buy.groupby('what')['applies'].transform('sum')==0, 0.01)
    buy['weight']=buy['volume'].mask(buy.groupby('what')['volume'].transform('sum')==0, 0.01)
    
    
    logging.warning("Aggregating {}".format(ordersetid));
    sell['_wp']=sell['price']*sell['weight']
    sell['_wp5']=sell['price']*sell['applies']
    gsell = sell.groupby('what')
    sellagg = pandas.DataFrame()
    sellagg['weightedaverage']=gsell['_wp'].sum()/gsell['weight'].sum()
    sellagg['maxval']=gsell['price'].max()
    sellagg['minval']=gsell['price'].min()
    sellagg['stddev']=gsell['price'].std()
    sellagg['median']=gsell['price'].median()
    sellagg.fillna(0.01,inplace=True)
    sellagg['volume']=gsell['volume'].sum()
    sellagg['numorders']=gsell['price'].count()
    sellagg['fivepercent']=gsell['_wp5'].sum()/gsell['applies'].sum()
    sellagg['orderSet']=ordersetid
    buy['_wp']=buy['price']*buy['weight']
    buy['_wp5']=buy['price']*buy['applies']
    gbuy = buy.groupby('what')
    buyagg = pandas.DataFrame()
    buyagg['weightedaverage']=gbuy['_wp'].sum()/gbuy['weight'].sum()
    buyagg['maxval']=gbuy['price'].max()
    buyagg['minval']=gbuy['price'].min()
    buyagg['stddev']=gbuy['price'].std()
    buyagg['median']=gbuy['price'].median()
    buyagg.fillna(0.01,inplace=True)
    buyagg['volume']=gbuy['volume'].sum()
    buyagg['numorders']=gbuy['price'].count()
    buyagg['fivepercent']=gbuy['_wp5'].sum()/gbuy['applies'].sum()
    buyagg['orderSet']=ordersetid
    agg2=pandas.concat([buyagg,sellagg])
    
    
#    logging.warning("Outputing to DB {}".format(ordersetid));
#    agg2.to_sql('aggregates',connection,index=True,if_exists='append')
    logging.warning("Outputing to Redis {}".format(ordersetid));
    redisdb = redis.StrictRedis()
    pipe = redisdb.pipeline()
    count=0;
    for row in agg2.itertuples():
        pipe.set(row[0], "{}|{}|{}|{}|{}|{}|{}|{}".format(row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]),ex=5400)
        count+=1
        if count>1000:
            count=0
            pipe.execute()
    pipe.execute()


    logging.warning("Outputing to CSV {}".format(ordersetid));
    agg2.to_csv(path_or_buf="/tmp/aggregatecsv.csv.gz",compression='gzip');

    logging.warning("Station Aggregates {}".format(ordersetid));
    
    logging.warning("Pandas populating sell {}".format(ordersetid));
    
    #sell=pandas.read_sql_query("""select "stationID"||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders  where "orderSet"={} and "stationID" in (60003760,60008494,60011866,60004588,60005686) and buy=False group by "stationID","typeID",buy,price order by "stationID","typeID",price asc""".format(ordersetid),connection);
    sell=pandas.read_sql_query("""select "stationID"||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders  where "orderSet"={} and buy=False group by "stationID","typeID",buy,price order by "stationID","typeID",price asc""".format(ordersetid),connection);
    logging.warning("Pandas populating buy {}".format(ordersetid));
    #buy=pandas.read_sql_query("""select "stationID"||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders  where "orderSet"={} and "stationID" in (60003760,60008494,60011866,60004588,60005686) and buy=True group by "stationID","typeID",buy,price order by "stationID","typeID",price desc""".format(ordersetid),connection);
    buy=pandas.read_sql_query("""select "stationID"||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders  where "orderSet"={} and buy=True group by "stationID","typeID",buy,price order by "stationID","typeID",price desc""".format(ordersetid),connection);
    logging.warning("Pandas populated {}".format(ordersetid));


    logging.warning("Sell Math running {}".format(ordersetid));
    sell['min']=sell.groupby('what')['price'].transform('min')
    sell['volume']=sell['volume'].where(sell['price']<=sell['min']*100, 0)
    sell['cumsum']=sell.groupby('what')['volume'].cumsum()
    sell['fivepercent']=sell.groupby('what')['volume'].transform('sum')/20
    sell['lastsum']=sell.groupby('what')['cumsum'].shift(1)
    sell.fillna(0,inplace=True)
    sell['applies']=numpy.where(sell['cumsum']<=sell['fivepercent'], sell['volume'], sell['fivepercent']-sell['lastsum'])
    # Assign back explicitly: with copy-on-write (pandas 3) the numeric subset
    # is a copy, so writing into it would leave sell unchanged.
    num = sell._get_numeric_data().columns
    sell[num] = sell[num].clip(lower=0)
    sell['applies']=sell['applies'].mask(sell.groupby('what')['applies'].transform('sum')==0, 0.01)
    sell['weight']=sell['volume'].mask(sell.groupby('what')['volume'].transform('sum')==0, 0.01)
    logging.warning("Buy Math running {}".format(ordersetid));
    buy['max']=buy.groupby('what')['price'].transform('max')
    buy['volume']=buy['volume'].where(buy['price']>=buy['max']/100, 0)
    buy['cumsum']=buy.groupby('what')['volume'].cumsum()
    buy['fivepercent']=buy.groupby('what')['volume'].transform('sum')/20
    buy['lastsum']=buy.groupby('what')['cumsum'].shift(1)
    buy.fillna(0,inplace=True)
    buy['applies']=numpy.where(buy['cumsum']<=buy['fivepercent'], buy['volume'], buy['fivepercent']-buy['lastsum'])
    # Assign back explicitly: with copy-on-write (pandas 3) the numeric subset
    # is a copy, so writing into it would leave buy unchanged.
    num = buy._get_numeric_data().columns
    buy[num] = buy[num].clip(lower=0)
    buy['applies']=buy['applies'].mask(buy.groupby('what')['applies'].transform('sum')==0, 0.01)
    buy['weight']=buy['volume'].mask(buy.groupby('what')['volume'].transform('sum')==0, 0.01)


    logging.warning("Aggregating {}".format(ordersetid));
    sell['_wp']=sell['price']*sell['weight']
    sell['_wp5']=sell['price']*sell['applies']
    gsell = sell.groupby('what')
    sellagg = pandas.DataFrame()
    sellagg['weightedaverage']=gsell['_wp'].sum()/gsell['weight'].sum()
    sellagg['maxval']=gsell['price'].max()
    sellagg['minval']=gsell['price'].min()
    sellagg['stddev']=gsell['price'].std()
    sellagg['median']=gsell['price'].median()
    sellagg.fillna(0.01,inplace=True)
    sellagg['volume']=gsell['volume'].sum()
    sellagg['numorders']=gsell['price'].count()
    sellagg['fivepercent']=gsell['_wp5'].sum()/gsell['applies'].sum()
    sellagg['orderSet']=ordersetid
    buy['_wp']=buy['price']*buy['weight']
    buy['_wp5']=buy['price']*buy['applies']
    gbuy = buy.groupby('what')
    buyagg = pandas.DataFrame()
    buyagg['weightedaverage']=gbuy['_wp'].sum()/gbuy['weight'].sum()
    buyagg['maxval']=gbuy['price'].max()
    buyagg['minval']=gbuy['price'].min()
    buyagg['stddev']=gbuy['price'].std()
    buyagg['median']=gbuy['price'].median()
    buyagg.fillna(0.01,inplace=True)
    buyagg['volume']=gbuy['volume'].sum()
    buyagg['numorders']=gbuy['price'].count()
    buyagg['fivepercent']=gbuy['_wp5'].sum()/gbuy['applies'].sum()
    buyagg['orderSet']=ordersetid
    agg2=pandas.concat([buyagg,sellagg])
        
    
    #logging.warning("Outputing to DB {}".format(ordersetid));
    #agg2.to_sql('aggregates',connection,index=True,if_exists='append')
    logging.warning("Outputing to Redis {}".format(ordersetid));
    count=0;
    for row in agg2.itertuples():
        pipe.set(row[0], "{}|{}|{}|{}|{}|{}|{}|{}".format(row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]),ex=5400)
        count+=1
        if count>1000:
            count=0
            pipe.execute()
    pipe.execute()
    

    logging.warning("System Aggregates {}".format(ordersetid));
    
    logging.warning("Pandas populating sell {}".format(ordersetid));
    
    #sell=pandas.read_sql_query("""select "solarSystemID"||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders join evesde."staStations" on orders."stationID"="staStations"."stationID" where "orderSet"={} and "solarSystemID" in (30000142,30000144) and buy=False group by "solarSystemID","typeID",buy,price order by "solarSystemID","typeID",price asc""".format(ordersetid),connection);
    sell=pandas.read_sql_query("""select "solarSystemID"||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders join evesde."staStations" on orders."stationID"="staStations"."stationID" where "orderSet"={} and buy=False group by "solarSystemID","typeID",buy,price order by "solarSystemID","typeID",price asc""".format(ordersetid),connection);
    logging.warning("Pandas populating buy {}".format(ordersetid));
    #buy=pandas.read_sql_query("""select "solarSystemID"||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders join evesde."staStations" on orders."stationID"="staStations"."stationID" where "orderSet"={} and "solarSystemID" in (30000142,30000144) and buy=True group by "solarSystemID","typeID",buy,price order by "solarSystemID","typeID",price desc""".format(ordersetid),connection);
    buy=pandas.read_sql_query("""select "solarSystemID"||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders join evesde."staStations" on orders."stationID"="staStations"."stationID" where "orderSet"={} and buy=True group by "solarSystemID","typeID",buy,price order by "solarSystemID","typeID",price desc""".format(ordersetid),connection);
    logging.warning("Pandas populated {}".format(ordersetid));


    logging.warning("Sell Math running {}".format(ordersetid));
    sell['min']=sell.groupby('what')['price'].transform('min')
    sell['volume']=sell['volume'].where(sell['price']<=sell['min']*100, 0)
    sell['cumsum']=sell.groupby('what')['volume'].cumsum()
    sell['fivepercent']=sell.groupby('what')['volume'].transform('sum')/20
    sell['lastsum']=sell.groupby('what')['cumsum'].shift(1)
    sell.fillna(0,inplace=True)
    sell['applies']=numpy.where(sell['cumsum']<=sell['fivepercent'], sell['volume'], sell['fivepercent']-sell['lastsum'])
    # Assign back explicitly: with copy-on-write (pandas 3) the numeric subset
    # is a copy, so writing into it would leave sell unchanged.
    num = sell._get_numeric_data().columns
    sell[num] = sell[num].clip(lower=0)
    sell['applies']=sell['applies'].mask(sell.groupby('what')['applies'].transform('sum')==0, 0.01)
    sell['weight']=sell['volume'].mask(sell.groupby('what')['volume'].transform('sum')==0, 0.01)
    logging.warning("Buy Math running {}".format(ordersetid));
    buy['max']=buy.groupby('what')['price'].transform('max')
    buy['volume']=buy['volume'].where(buy['price']>=buy['max']/100, 0)
    buy['cumsum']=buy.groupby('what')['volume'].cumsum()
    buy['fivepercent']=buy.groupby('what')['volume'].transform('sum')/20
    buy['lastsum']=buy.groupby('what')['cumsum'].shift(1)
    buy.fillna(0,inplace=True)
    buy['applies']=numpy.where(buy['cumsum']<=buy['fivepercent'], buy['volume'], buy['fivepercent']-buy['lastsum'])
    # Assign back explicitly: with copy-on-write (pandas 3) the numeric subset
    # is a copy, so writing into it would leave buy unchanged.
    num = buy._get_numeric_data().columns
    buy[num] = buy[num].clip(lower=0)
    buy['applies']=buy['applies'].mask(buy.groupby('what')['applies'].transform('sum')==0, 0.01)
    buy['weight']=buy['volume'].mask(buy.groupby('what')['volume'].transform('sum')==0, 0.01)


    logging.warning("Aggregating {}".format(ordersetid));
    sell['_wp']=sell['price']*sell['weight']
    sell['_wp5']=sell['price']*sell['applies']
    gsell = sell.groupby('what')
    sellagg = pandas.DataFrame()
    sellagg['weightedaverage']=gsell['_wp'].sum()/gsell['weight'].sum()
    sellagg['maxval']=gsell['price'].max()
    sellagg['minval']=gsell['price'].min()
    sellagg['stddev']=gsell['price'].std()
    sellagg['median']=gsell['price'].median()
    sellagg.fillna(0.01,inplace=True)
    sellagg['volume']=gsell['volume'].sum()
    sellagg['numorders']=gsell['price'].count()
    sellagg['fivepercent']=gsell['_wp5'].sum()/gsell['applies'].sum()
    sellagg['orderSet']=ordersetid
    buy['_wp']=buy['price']*buy['weight']
    buy['_wp5']=buy['price']*buy['applies']
    gbuy = buy.groupby('what')
    buyagg = pandas.DataFrame()
    buyagg['weightedaverage']=gbuy['_wp'].sum()/gbuy['weight'].sum()
    buyagg['maxval']=gbuy['price'].max()
    buyagg['minval']=gbuy['price'].min()
    buyagg['stddev']=gbuy['price'].std()
    buyagg['median']=gbuy['price'].median()
    buyagg.fillna(0.01,inplace=True)
    buyagg['volume']=gbuy['volume'].sum()
    buyagg['numorders']=gbuy['price'].count()
    buyagg['fivepercent']=gbuy['_wp5'].sum()/gbuy['applies'].sum()
    buyagg['orderSet']=ordersetid
    try:
        agg2=pandas.concat([buyagg,sellagg])
        
    
       # logging.warning("Outputing to DB {}".format(ordersetid));
       # agg2.to_sql('aggregates',connection,index=True,if_exists='append')
        logging.warning("Outputing to Redis {}".format(ordersetid));
        count=0;
        for row in agg2.itertuples():
            pipe.set(row[0], "{}|{}|{}|{}|{}|{}|{}|{}".format(row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]),ex=5400)
            count+=1
            if count>1000:
                count=0
                pipe.execute()
        pipe.execute()
    except ZeroDivisionError:
         logging.warning("bah!")

    logging.warning("Universe Aggregates {}".format(ordersetid));
    
    logging.warning("Pandas populating sell {}".format(ordersetid));
    
    sell=pandas.read_sql_query("""select '0|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders where "orderSet"={} and buy=False group by "typeID",buy,price order by "typeID",price asc""".format(ordersetid),connection);
    logging.warning("Pandas populating buy {}".format(ordersetid));
    buy=pandas.read_sql_query("""select '0|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders where "orderSet"={} and buy=True group by "typeID",buy,price order by "typeID",price desc""".format(ordersetid),connection);
    logging.warning("Pandas populated {}".format(ordersetid));


    logging.warning("Sell Math running {}".format(ordersetid));
    sell['min']=sell.groupby('what')['price'].transform('min')
    sell['volume']=sell['volume'].where(sell['price']<=sell['min']*100, 0)
    sell['cumsum']=sell.groupby('what')['volume'].cumsum()
    sell['fivepercent']=sell.groupby('what')['volume'].transform('sum')/20
    sell['lastsum']=sell.groupby('what')['cumsum'].shift(1)
    sell.fillna(0,inplace=True)
    sell['applies']=numpy.where(sell['cumsum']<=sell['fivepercent'], sell['volume'], sell['fivepercent']-sell['lastsum'])
    # Assign back explicitly: with copy-on-write (pandas 3) the numeric subset
    # is a copy, so writing into it would leave sell unchanged.
    num = sell._get_numeric_data().columns
    sell[num] = sell[num].clip(lower=0)
    sell['applies']=sell['applies'].mask(sell.groupby('what')['applies'].transform('sum')==0, 0.01)
    sell['weight']=sell['volume'].mask(sell.groupby('what')['volume'].transform('sum')==0, 0.01)
    logging.warning("Buy Math running {}".format(ordersetid));
    buy['max']=buy.groupby('what')['price'].transform('max')
    buy['volume']=buy['volume'].where(buy['price']>=buy['max']/100, 0)
    buy['cumsum']=buy.groupby('what')['volume'].cumsum()
    buy['fivepercent']=buy.groupby('what')['volume'].transform('sum')/20
    buy['lastsum']=buy.groupby('what')['cumsum'].shift(1)
    buy.fillna(0,inplace=True)
    buy['applies']=numpy.where(buy['cumsum']<=buy['fivepercent'], buy['volume'], buy['fivepercent']-buy['lastsum'])
    # Assign back explicitly: with copy-on-write (pandas 3) the numeric subset
    # is a copy, so writing into it would leave buy unchanged.
    num = buy._get_numeric_data().columns
    buy[num] = buy[num].clip(lower=0)
    buy['applies']=buy['applies'].mask(buy.groupby('what')['applies'].transform('sum')==0, 0.01)
    buy['weight']=buy['volume'].mask(buy.groupby('what')['volume'].transform('sum')==0, 0.01)


    logging.warning("Aggregating {}".format(ordersetid));
    sell['_wp']=sell['price']*sell['weight']
    sell['_wp5']=sell['price']*sell['applies']
    gsell = sell.groupby('what')
    sellagg = pandas.DataFrame()
    sellagg['weightedaverage']=gsell['_wp'].sum()/gsell['weight'].sum()
    sellagg['maxval']=gsell['price'].max()
    sellagg['minval']=gsell['price'].min()
    sellagg['stddev']=gsell['price'].std()
    sellagg['median']=gsell['price'].median()
    sellagg.fillna(0.01,inplace=True)
    sellagg['volume']=gsell['volume'].sum()
    sellagg['numorders']=gsell['price'].count()
    sellagg['fivepercent']=gsell['_wp5'].sum()/gsell['applies'].sum()
    sellagg['orderSet']=ordersetid
    buy['_wp']=buy['price']*buy['weight']
    buy['_wp5']=buy['price']*buy['applies']
    gbuy = buy.groupby('what')
    buyagg = pandas.DataFrame()
    buyagg['weightedaverage']=gbuy['_wp'].sum()/gbuy['weight'].sum()
    buyagg['maxval']=gbuy['price'].max()
    buyagg['minval']=gbuy['price'].min()
    buyagg['stddev']=gbuy['price'].std()
    buyagg['median']=gbuy['price'].median()
    buyagg.fillna(0.01,inplace=True)
    buyagg['volume']=gbuy['volume'].sum()
    buyagg['numorders']=gbuy['price'].count()
    buyagg['fivepercent']=gbuy['_wp5'].sum()/gbuy['applies'].sum()
    buyagg['orderSet']=ordersetid
    agg2=pandas.concat([buyagg,sellagg])
        
    
    #logging.warning("Outputing to DB {}".format(ordersetid));
    #agg2.to_sql('aggregates',connection,index=True,if_exists='append')
    logging.warning("Outputing to Redis {}".format(ordersetid));
    count=0;
    for row in agg2.itertuples():
        pipe.set(row[0], "{}|{}|{}|{}|{}|{}|{}|{}".format(row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]),ex=5400)
        count+=1
        if count>1000:
            count=0
            pipe.execute()
    pipe.execute()
    

    
    
    
    
    logging.warning("Storing some stats for the front page {}".format(ordersetid));
    result=connection.execute(text("""select array_to_json(array_agg(t)) from (select coun,"stationName",orders."stationID",vol from (select "stationID",count(*) coun,sum(volume) vol from orders where "orderSet"={} and buy=false group by "stationID" order by count(*)) orders join evesde."staStations" on orders."stationID"="staStations"."stationID" order by coun desc limit 10) t""".format(ordersetid))).fetchone()
    redisdb.set("fp-sell",json.dumps(result[0]));
    result=connection.execute(text("""select array_to_json(array_agg(t)) from (select coun,"stationName",orders."stationID",vol from (select "stationID",count(*) coun,sum(volume) vol from orders where "orderSet"={} and buy=true group by "stationID" order by count(*)) orders join evesde."staStations" on orders."stationID"="staStations"."stationID" order by coun desc limit 10) t""".format(ordersetid))).fetchone()
    redisdb.set("fp-buy",json.dumps(result[0]));
    # UTC without an offset, the format the site (and the PHP rollback) parses.
    redisdb.set("fp-lastupdate",datetime.datetime.now(datetime.UTC).replace(tzinfo=None).isoformat())
    logging.warning("Complete {}".format(ordersetid))

    orderbookpath = """/opt/orderbooks/orderset-{}.csv""".format(ordersetid)
    shutil.move("""/tmp/orderset-{}.csv""".format(ordersetid),orderbookpath)
    os.chmod(orderbookpath, 0o644)
