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


import logging
logging.basicConfig(filename='logs/aggloader-esi.log',level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')



def RateLimited(maxPerSecond):
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                time.sleep(leftToWait)
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate


    
    
def processData(result,orderwriter,ordersetid,connection,orderTable):
    
    try:
        resp=result.result()
        regionid=result.region
        logging.info('Process {} {} {} {}'.format(resp.status_code,result.url,result.retry,result.region))
        if resp.status_code==200:
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
                nextpage=result.url
            else:
                nextpage=None
            logging.info('{}: next page {}'.format(result.url,nextpage))
            return {'retry':0,'url':nextpage,'region':result.region,'page':result.page+1,'structure':result.structure}
        elif resp.status_code==403:
            logging.error("403 status. {} returned {}".format(resp.url,resp.status_code))
            return {'retry':4}
        else:
            logging.error("Non 200 status. {} returned {}".format(resp.url,resp.status_code))
            return {'retry':result.retry+1,'url':result.url,'region':result.region,'page':result.page,'structure':result.structure}
    except requests.exceptions.ConnectionError as e:
        logging.error(e)
        return {'retry':result.retry+1,'url':result.url,'region':result.region,'page':result.page,'structure':result.structure}
    
    
    
    


@RateLimited(150)
def getData(requestsConnection,url,retry,page,region,structure):
    future=requestsConnection.get(url+str(page))
    logging.info('getting {}#{}#{}#{}'.format(retry,page,region,url+str(page)))
    future.url=url
    future.fullurl=url+str(page)
    future.page=page
    future.retry=retry
    future.region=region
    future.structure=structure
    return future


if __name__ == "__main__":
    import ConfigParser, os
    fileLocation = os.path.dirname(os.path.realpath(__file__))
    inifile=fileLocation+'/esi.cfg'

    config = ConfigParser.ConfigParser()
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
    

    session = FuturesSession(max_workers=reqs_num_workers)
    session.headers.update({'UserAgent':useragent});
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
    
    for regionid in xrange(10000001,10000070):
        if regionid not in (10000024,10000026):
            urls.append({'url':"https://esi.tech.ccp.is/latest/markets/{}/orders/?order_type=all&datasource=tranquility&page=".format(regionid),'retry':0,'page':1,'region':regionid,'structure':0})

    trans = connection.begin()

    connection.execute(orderSet.insert(),downloaded=datetime.datetime.now().isoformat())

    result=connection.execute("select currval('orderset_id_seq')").fetchone()

    ordersetid=result[0]



    with open('/tmp/orderset-{}.csv'.format(ordersetid), 'wb') as csvfile:
        orderwriter = csv.writer(csvfile,quoting=csv.QUOTE_MINIMAL,delimiter="\t")
        # Loop through the urls in batches
        while len(urls)>0:
            futures=[]
            logging.warn("Loop restarting {}".format(ordersetid));
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
        
    

        # Get authorization
        headers = {'Authorization':'Basic '+ base64.b64encode(clientid+':'+secret),'User-Agent':useragent}
        query = {'grant_type':'refresh_token','refresh_token':refreshtoken}
        r = requests.post('https://login.eveonline.com/oauth/token',params=query,headers=headers)
        response = r.json()
        accesstoken = response['access_token']
        logging.warn("Access Token {}".format(accesstoken))




        session.headers.update({'Authorization':'Bearer '+accesstoken});
        
        results=connection.execute('select "stationID",mss."regionID" from evesde."staStations" sta join evesde."mapSolarSystems" mss on mss."solarSystemID"=sta."solarSystemID"  where "stationID">100000000').fetchall()
        for result in results:
            urls.append({'url':"https://esi.tech.ccp.is/latest/markets/structures/{}/?&datasource=tranquility&page=".format(result[0]),'retry':0,'page':1,'region':result[1],'structure':1})
        
        
        while len(urls)>0:
            futures=[]
            logging.warn("Loop restarting {}".format(ordersetid));
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


    logging.warn("Loading Data File {}".format(ordersetid));
    connection.execute("""copy orders_{}("orderID","typeID",issued,buy,volume,"volumeEntered","minVolume",price,"stationID",range,duration,region,"orderSet") from '/tmp/orderset-{}.csv'""".format((int(ordersetid)/100)%10,ordersetid))
    logging.warn("Complete load {}".format(ordersetid));
    trans.commit()
    


    logging.warn("Pandas populating sell {}".format(ordersetid));
    
    sell=pandas.read_sql_query("""select region||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders  where "orderSet"={} and buy=False group by region,"typeID",buy,price order by region,"typeID",price asc""".format(ordersetid),connection);
    logging.warn("Pandas populating buy {}".format(ordersetid));
    buy=pandas.read_sql_query("""select region||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders  where "orderSet"={} and buy=True group by region,"typeID",buy,price order by region,"typeID",price desc""".format(ordersetid),connection);
    logging.warn("Pandas populated {}".format(ordersetid));


    logging.warn("Sell Math running {}".format(ordersetid));
    sell['min']=sell.groupby('what')['price'].transform('min')
    sell['volume']=sell.apply(lambda x: 0 if x['price']>x['min']*100 else x['volume'],axis=1)
    sell['cumsum']=sell.groupby('what')['volume'].apply(lambda x: x.cumsum())
    sell['fivepercent']=sell.groupby('what')['volume'].transform('sum')/20
    sell['lastsum']=sell.groupby('what')['cumsum'].shift(1)
    sell.fillna(0,inplace=True)
    sell['applies']=sell.apply(lambda x: x['volume'] if x['cumsum']<=x['fivepercent'] else x['fivepercent']-x['lastsum'],axis=1)
    num = sell._get_numeric_data()
    num[num < 0] = 0
    logging.warn("Buy Math running {}".format(ordersetid));
    buy['max']=buy.groupby('what')['price'].transform('max')
    buy['volume']=buy.apply(lambda x: 0 if x['price']<x['max']/100 else x['volume'],axis=1)
    buy['cumsum']=buy.groupby('what')['volume'].apply(lambda x: x.cumsum())
    buy['fivepercent']=buy.groupby('what')['volume'].transform('sum')/20
    buy['lastsum']=buy.groupby('what')['cumsum'].shift(1)
    buy.fillna(0,inplace=True)
    buy['applies']=buy.apply(lambda x: x['volume'] if x['cumsum']<=x['fivepercent'] else x['fivepercent']-x['lastsum'],axis=1)
    num = buy._get_numeric_data()
    num[num < 0] = 0
    
    
    logging.warn("Aggregating {}".format(ordersetid));
    sellagg = pandas.DataFrame()
    sellagg['weightedaverage']=sell.groupby('what').apply(lambda x: numpy.average(x.price, weights=x.volume))
    sellagg['maxval']=sell.groupby('what')['price'].max()
    sellagg['minval']=sell.groupby('what')['price'].min()
    sellagg['stddev']=sell.groupby('what')['price'].std()
    sellagg['median']=sell.groupby('what')['price'].median()
    sellagg.fillna(0.01,inplace=True)
    sellagg['volume']=sell.groupby('what')['volume'].sum()
    sellagg['numorders']=sell.groupby('what')['price'].count()
    sellagg['fivepercent']=sell.groupby('what').apply(lambda x: numpy.average(x.price, weights=x.applies))
    sellagg['orderSet']=ordersetid
    buyagg = pandas.DataFrame()
    buyagg['weightedaverage']=buy.groupby('what').apply(lambda x: numpy.average(x.price, weights=x.volume))
    buyagg['maxval']=buy.groupby('what')['price'].max()
    buyagg['minval']=buy.groupby('what')['price'].min()
    buyagg['stddev']=buy.groupby('what')['price'].std()
    buyagg['median']=buy.groupby('what')['price'].median()
    buyagg.fillna(0.01,inplace=True)
    buyagg['volume']=buy.groupby('what')['volume'].sum()
    buyagg['numorders']=buy.groupby('what')['price'].count()
    buyagg['fivepercent']=buy.groupby('what').apply(lambda x: numpy.average(x.price, weights=x.applies))
    buyagg['orderSet']=ordersetid
    agg2=pandas.concat([buyagg,sellagg])
    
    
    logging.warn("Outputing to DB {}".format(ordersetid));
    agg2.to_sql('aggregates',connection,index=True,if_exists='append')
    logging.warn("Outputing to Redis {}".format(ordersetid));
    redisdb = redis.StrictRedis()
    pipe = redisdb.pipeline()
    count=0;
    for row in agg2.itertuples():
        pipe.set(row[0], "{}|{}|{}|{}|{}|{}|{}|{}".format(row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]))
        count+=1
        if count>1000:
            count=0
            pipe.execute()
    pipe.execute()


    logging.warn("Station Aggregates {}".format(ordersetid));
    
    logging.warn("Pandas populating sell {}".format(ordersetid));
    
    sell=pandas.read_sql_query("""select "stationID"||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders  where "orderSet"={} and "stationID" in (60003760,60008494,60011866,60004588,60005686) and buy=False group by "stationID","typeID",buy,price order by "stationID","typeID",price asc""".format(ordersetid),connection);
    logging.warn("Pandas populating buy {}".format(ordersetid));
    buy=pandas.read_sql_query("""select "stationID"||'|'||"typeID"||'|'||buy as what,price,sum(volume) volume from orders  where "orderSet"={} and "stationID" in (60003760,60008494,60011866,60004588,60005686) and buy=True group by "stationID","typeID",buy,price order by "stationID","typeID",price desc""".format(ordersetid),connection);
    logging.warn("Pandas populated {}".format(ordersetid));


    logging.warn("Sell Math running {}".format(ordersetid));
    sell['min']=sell.groupby('what')['price'].transform('min')
    sell['volume']=sell.apply(lambda x: 0 if x['price']>x['min']*100 else x['volume'],axis=1)
    sell['cumsum']=sell.groupby('what')['volume'].apply(lambda x: x.cumsum())
    sell['fivepercent']=sell.groupby('what')['volume'].transform('sum')/20
    sell['lastsum']=sell.groupby('what')['cumsum'].shift(1)
    sell.fillna(0,inplace=True)
    sell['applies']=sell.apply(lambda x: x['volume'] if x['cumsum']<=x['fivepercent'] else x['fivepercent']-x['lastsum'],axis=1)
    num = sell._get_numeric_data()
    num[num < 0] = 0
    logging.warn("Buy Math running {}".format(ordersetid));
    buy['max']=buy.groupby('what')['price'].transform('max')
    buy['volume']=buy.apply(lambda x: 0 if x['price']<x['max']/100 else x['volume'],axis=1)
    buy['cumsum']=buy.groupby('what')['volume'].apply(lambda x: x.cumsum())
    buy['fivepercent']=buy.groupby('what')['volume'].transform('sum')/20
    buy['lastsum']=buy.groupby('what')['cumsum'].shift(1)
    buy.fillna(0,inplace=True)
    buy['applies']=buy.apply(lambda x: x['volume'] if x['cumsum']<=x['fivepercent'] else x['fivepercent']-x['lastsum'],axis=1)
    num = buy._get_numeric_data()
    num[num < 0] = 0
        
    
    logging.warn("Aggregating {}".format(ordersetid));
    sellagg = pandas.DataFrame()
    sellagg['weightedaverage']=sell.groupby('what').apply(lambda x: numpy.average(x.price, weights=x.volume))
    sellagg['maxval']=sell.groupby('what')['price'].max()
    sellagg['minval']=sell.groupby('what')['price'].min()
    sellagg['stddev']=sell.groupby('what')['price'].std()
    sellagg['median']=sell.groupby('what')['price'].median()
    sellagg.fillna(0.01,inplace=True)
    sellagg['volume']=sell.groupby('what')['volume'].sum()
    sellagg['numorders']=sell.groupby('what')['price'].count()
    sellagg['fivepercent']=sell.groupby('what').apply(lambda x: numpy.average(x.price, weights=x.applies))
    sellagg['orderSet']=ordersetid
    buyagg = pandas.DataFrame()
    buyagg['weightedaverage']=buy.groupby('what').apply(lambda x: numpy.average(x.price, weights=x.volume))
    buyagg['maxval']=buy.groupby('what')['price'].max()
    buyagg['minval']=buy.groupby('what')['price'].min()
    buyagg['stddev']=buy.groupby('what')['price'].std()
    buyagg['median']=buy.groupby('what')['price'].median()
    buyagg.fillna(0.01,inplace=True)
    buyagg['volume']=buy.groupby('what')['volume'].sum()
    buyagg['numorders']=buy.groupby('what')['price'].count()
    buyagg['fivepercent']=buy.groupby('what').apply(lambda x: numpy.average(x.price, weights=x.applies))
    buyagg['orderSet']=ordersetid
    agg2=pandas.concat([buyagg,sellagg])
        
    
    logging.warn("Outputing to DB {}".format(ordersetid));
    agg2.to_sql('aggregates',connection,index=True,if_exists='append')
    logging.warn("Outputing to Redis {}".format(ordersetid));
    count=0;
    for row in agg2.itertuples():
        pipe.set(row[0], "{}|{}|{}|{}|{}|{}|{}|{}".format(row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]))
        count+=1
        if count>1000:
            count=0
            pipe.execute()
    pipe.execute()
    

    
    logging.warn("Storing some stats for the front page {}".format(ordersetid));
    result=connection.execute("""select array_to_json(array_agg(t)) from (select coun,"stationName",orders."stationID",vol from (select "stationID",count(*) coun,sum(volume) vol from orders where "orderSet"={} and buy=false group by "stationID" order by count(*)) orders join evesde."staStations" on orders."stationID"="staStations"."stationID" order by coun desc limit 10) t""".format(ordersetid)).fetchone()
    redisdb.set("fp-sell",json.dumps(result[0]));
    result=connection.execute("""select array_to_json(array_agg(t)) from (select coun,"stationName",orders."stationID",vol from (select "stationID",count(*) coun,sum(volume) vol from orders where "orderSet"={} and buy=true group by "stationID" order by count(*)) orders join evesde."staStations" on orders."stationID"="staStations"."stationID" order by coun desc limit 10) t""".format(ordersetid)).fetchone()
    redisdb.set("fp-buy",json.dumps(result[0]));
    redisdb.set("fp-lastupdate",datetime.datetime.utcnow().isoformat())
    logging.warn("Complete {}".format(ordersetid))

    shutil.move("""/tmp/orderset-{}.csv""".format(ordersetid),"""/opt/orderbooks/orderset-{}.csv""".format(ordersetid))
