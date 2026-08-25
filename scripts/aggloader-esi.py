from sqlalchemy import create_engine, Column, MetaData, Table, Index
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

import gzip
import glob
from io import StringIO
from six.moves import configparser
import traceback


import logging

REQUEST_TIMEOUT = 30
LOG_RETENTION_DAYS = 7

def setupLogging(debug=False):
    today = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    logfile = 'logs/aggloader-esi-{}.log'.format(today)
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(filename=logfile,level=level,format='%(asctime)s %(levelname)s %(message)s')

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=LOG_RETENTION_DAYS)
    for path in glob.glob('logs/aggloader-esi-*.log'):
        datestr = os.path.basename(path)[len('aggloader-esi-'):-len('.log')]
        try:
            filedate = datetime.datetime.strptime(datestr,'%Y-%m-%d')
        except ValueError:
            continue
        if filedate < cutoff:
            os.remove(path)



    
def processData(result,orderwriter,ordersetid,connection,orderTable):
    
    try:
        resp=result.result()
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
        elif resp.status_code==420:
            logging.error("420 status. sleeping for 60.  {} returned {} on retry {}".format(resp.url,resp.status_code,result.retry))
            time.sleep(60)
            return {'retry':result.retry+1,'url':result.url,'region':result.region,'page':result.page,'structure':result.structure}
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
    

    compatdate = (datetime.datetime.utcnow() - datetime.timedelta(hours=11)).strftime('%Y-%m-%d')

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

    regionids = connection.execute('select distinct "regionID" from evesde."staStations" where "stationID"<100000000 order by 1').fetchall()
    for row in regionids:
        regionid = row[0]
        urls.append({'url':"https://esi.evetech.net/markets/{}/orders/?order_type=all&datasource=tranquility&page=".format(regionid),'retry':0,'page':1,'region':regionid,'structure':0})

    # Virtual PLEX Market region - not a real space region, so it has no entry in
    # evesde.staStations and gets skipped by the loop above unless added explicitly.
    urls.append({'url':"https://esi.evetech.net/markets/19000001/orders/?order_type=all&datasource=tranquility&page=",'retry':0,'page':1,'region':19000001,'structure':0})

    trans = connection.begin()

    connection.execute(orderSet.insert(),downloaded=datetime.datetime.now().isoformat())

    result=connection.execute("select currval('orderset_id_seq')").fetchone()

    ordersetid=result[0]



    csvpath = '/tmp/orderset-{}.csv'.format(ordersetid)
    csvfd = os.open(csvpath, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(csvfd, 'w') as csvfile:
        orderwriter = csv.writer(csvfile,quoting=csv.QUOTE_MINIMAL,delimiter="\t")
        # Loop through the urls in batches
        while len(urls)>0:
            futures=[]
            logging.warning("Loop restarting {}".format(ordersetid));
            for url in urls:
                logging.info('URL:{}  Retry:{} page:{}'.format(url['url'],url['retry'],url['page']));
                futures.append(getData(session,url['url'],url['retry'],url['page'],url['region'],url['structure']))
            urls=[]
            for result in as_completed(futures):
                presult=processData(result,orderwriter,ordersetid,connection,orderTable)
                if presult['retry']==1 or presult['retry']==2:
                    urls.append(presult)
                    logging.info("adding {} to retry {}".format(result.url,presult['retry']))
                if presult['retry'] == 0 and presult['url'] is not None:
                    logging.info('{} has more pages. {}'.format(result.url,presult['retry']))
                    urls.append(presult)
        
        # Get authorization
        token=clientid+':'+secret
        message_bytes = token.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        headers = {'Authorization':'Basic '+ base64_message,'User-Agent':useragent,"Content-Type": "application/x-www-form-urlencoded"}
        query = {'grant_type':'refresh_token','refresh_token':refreshtoken}
        r = requests.post('https://login.eveonline.com/v2/oauth/token',data=query,headers=headers,timeout=REQUEST_TIMEOUT)
        response = r.json()
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
        
        results=connection.execute('select "stationID",mss."regionID" from evesde."staStations" sta join evesde."mapSolarSystems" mss on mss."solarSystemID"=sta."solarSystemID"  where "stationID">100000000').fetchall()
        for result in results:
            urls.append({'url':"https://esi.evetech.net/markets/structures/{}/?&datasource=tranquility&page=".format(result[0]),'retry':0,'page':1,'region':result[1],'structure':1})
        
        
        while len(urls)>0:
            futures=[]
            logging.warning("Loop restarting {}".format(ordersetid));
            for url in urls:
                logging.info('URL:{}  Retry:{} page:{}'.format(url['url'],url['retry'],url['page']));
                futures.append(getData(session,url['url'],url['retry'],url['page'],url['region'],url['structure']))
            urls=[]
            for result in as_completed(futures):
                presult=processData(result,orderwriter,ordersetid,connection,orderTable)
                if presult['retry']==1:
                    urls.append(presult)
                    logging.info("adding {} to retry {}".format(result.url,presult['retry']))
                if presult['retry'] == 0 and presult['url'] is not None:
                    logging.info('{} has more pages. {}'.format(result.url,presult['retry']))
                    urls.append(presult)

    logging.warning("Loading Data File {}".format(ordersetid));
    connection.execute("""copy orders_{}("orderID","typeID",issued,buy,volume,"volumeEntered","minVolume",price,"stationID",range,duration,region,"orderSet") from '/tmp/orderset-{}.csv'""".format(int((int(ordersetid)/100)%10),ordersetid))
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
    num = sell._get_numeric_data()
    num[num < 0] = 0
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
    num = buy._get_numeric_data()
    num[num < 0] = 0
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
    num = sell._get_numeric_data()
    num[num < 0] = 0
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
    num = buy._get_numeric_data()
    num[num < 0] = 0
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
    num = sell._get_numeric_data()
    num[num < 0] = 0
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
    num = buy._get_numeric_data()
    num[num < 0] = 0
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
    num = sell._get_numeric_data()
    num[num < 0] = 0
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
    num = buy._get_numeric_data()
    num[num < 0] = 0
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
    result=connection.execute("""select array_to_json(array_agg(t)) from (select coun,"stationName",orders."stationID",vol from (select "stationID",count(*) coun,sum(volume) vol from orders where "orderSet"={} and buy=false group by "stationID" order by count(*)) orders join evesde."staStations" on orders."stationID"="staStations"."stationID" order by coun desc limit 10) t""".format(ordersetid)).fetchone()
    redisdb.set("fp-sell",json.dumps(result[0]));
    result=connection.execute("""select array_to_json(array_agg(t)) from (select coun,"stationName",orders."stationID",vol from (select "stationID",count(*) coun,sum(volume) vol from orders where "orderSet"={} and buy=true group by "stationID" order by count(*)) orders join evesde."staStations" on orders."stationID"="staStations"."stationID" order by coun desc limit 10) t""".format(ordersetid)).fetchone()
    redisdb.set("fp-buy",json.dumps(result[0]));
    redisdb.set("fp-lastupdate",datetime.datetime.utcnow().isoformat())
    logging.warning("Complete {}".format(ordersetid))

    orderbookpath = """/opt/orderbooks/orderset-{}.csv""".format(ordersetid)
    shutil.move("""/tmp/orderset-{}.csv""".format(ordersetid),orderbookpath)
    os.chmod(orderbookpath, 0o644)
