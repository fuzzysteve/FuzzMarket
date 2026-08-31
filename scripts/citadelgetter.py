import requests
from sqlalchemy import create_engine,MetaData,Table,Column,INTEGER,FLOAT,VARCHAR,BigInteger


import logging
logging.basicConfig(filename='logs/citadelfullloader.log',level=logging.WARN,format='%(asctime)s %(levelname)s %(message)s')


engine = create_engine('postgresql+psycopg2://marketdata:marketdatapass@localhost/marketdata', echo=False)
metadata = MetaData()
connection = engine.connect()

staStations =  Table('staStations2', metadata,
    Column('stationID', BigInteger, primary_key=True, autoincrement=False, nullable=False),
    Column('security', INTEGER()),
    Column('dockingCostPerVolume', FLOAT(precision=53)),
    Column('maxShipVolumeDockable', FLOAT(precision=53)),
    Column('officeRentalCost', INTEGER()),
    Column('operationID', INTEGER(),index=True),
    Column('stationTypeID', INTEGER(),index=True),
    Column('corporationID', INTEGER(),index=True),
    Column('solarSystemID', INTEGER(),index=True),
    Column('constellationID', INTEGER(),index=True),
    Column('regionID', INTEGER(),index=True),
    Column('stationName', VARCHAR(length=100)),
    Column('x', FLOAT(precision=53)),
    Column('y', FLOAT(precision=53)),
    Column('z', FLOAT(precision=53)),
    Column('reprocessingEfficiency', FLOAT(precision=53)),
    Column('reprocessingStationsTake', FLOAT(precision=53)),
    Column('reprocessingHangarFlag', INTEGER()),
    schema='evesde'
)


headers={'User-Agent':'Fuzzwork Market Citadel Name Getter'};

from requests_oauthlib import OAuth2Session
token = {'access_token':'','refresh_token':'Refresh token goes here!','token_type': 'Bearer','expires_in': '-30'}
client_id = r'Client ID goes here!'
refresh_url = 'https://login.eveonline.com/oauth/token'
extra = { 'client_id': client_id,'client_secret': r'Client secret goes here!'}

def token_saver(token):
    pass


client = OAuth2Session(client_id, token=token, auto_refresh_url=refresh_url,auto_refresh_kwargs=extra, token_updater=token_saver)


citadellistreq=client.get('https://esi.tech.ccp.is/latest/universe/structures/?datasource=tranquility')
citadellist=citadellistreq.json()




for citadel in citadellist:
    citadeldetails=client.get("https://esi.tech.ccp.is/latest/universe/structures/{}/?datasource=tranquility".format(citadel),headers=headers)
    if citadeldetails.status_code==200:
        cjson=citadeldetails.json()
        if cjson['type_id'] in [35826,35827,35833,35834,35836,40340,47512,47513,47514,47515,47516]:
            citadelmarket=client.get("https://esi.tech.ccp.is/latest/markets/structures/{}/?page=1&datasource=tranquility".format(citadel))
            if citadelmarket.status_code==200:
                connection.execute(staStations.insert(),stationID=citadel,stationName=cjson['name'],stationTypeID=cjson['type_id'],solarSystemID=cjson['solar_system_id'],x=cjson['position']['x'],y=cjson['position']['y'],z=cjson['position']['z'])
                logging.warn('Citadel id {} inserted'.format(citadel))
            else:
                logging.warn('Citadel id {} not inserted, no market'.format(citadel))
        else:
            logging.warn('Citadel id {} not inserted, no market possible'.format(citadel))
    else:
        logging.warn('Citadel id {} failed with status code {}'.format(citadel,citadeldetails.status_code))
