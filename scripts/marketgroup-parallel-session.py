import requests
from sqlalchemy import create_engine,MetaData,Table,Column,INTEGER,FLOAT,VARCHAR,UnicodeText,DECIMAL,Boolean,select,literal_column
import requests_cache
from requests_futures.sessions import FuturesSession
import requests_futures
from concurrent.futures import as_completed

from tqdm import tqdm


def getitems(typelist):
    typefuture=[]
    print "getitems"
    for typeid in typelist:
        if isinstance(typeid,basestring) and typeid.startswith("https"):
            typefuture.append(session.get(str(typeid)))
        else:
            typefuture.append(session.get(typelookupurl.format(typeid)))
    badlist=[]
    pbar = tqdm(total=len(typelist))
    for typedata in as_completed(typefuture):
        if typedata.result().status_code==200:
            itemjson=typedata.result().json()
            item=itemjson.get('type_id')
            if int(item) in sdetypelist:
                try:
                    connection.execute(invTypes.update().where(invTypes.c.typeID == literal_column(str(item))),
                               typeID=item,
                               typeName=itemjson['name'],
                               groupID=itemjson.get('group_id',None),
                               marketGroupID=itemjson.get('market_group_id',None),
                               capacity=itemjson.get('capacity',None),
                               published=itemjson.get('published',False),
                               portionSize=itemjson.get('portion_size',None),
                               volume=itemjson['volume'])
                except:
                    pass
            else:
                    connection.execute(invTypes.insert(),
                                typeID=item,
                                typeName=itemjson['name'],
                                marketGroupID=itemjson.get('market_group_id',None),
                                groupID=itemjson.get('group_id',None),
                                published=itemjson.get('published',False),
                                volume=itemjson.get('volume',None),
                                capacity=itemjson.get('capacity',None),
                                portionSize=itemjson.get('portion_size',None),
                                mass=itemjson.get('mass',None)
                                )
        else:
            badlist.append(typedata.result().url)
            print typedata.result().url
        pbar.update(1)
    return badlist





engine = create_engine('postgresql+psycopg2://marketdata:marketdatapass@localhost/marketdata', echo=False,connect_args={"application_name":"itemloader"})
metadata = MetaData()
connection = engine.connect()
trans = connection.begin()



invTypes =  Table('invTypes', metadata,
        Column('typeID', INTEGER(), primary_key=True, autoincrement=False, nullable=False),
        Column('groupID', INTEGER(),index=True),
        Column('typeName', VARCHAR(length=100)),
        Column('description',UnicodeText()),
        Column('mass', FLOAT(precision=53)),
        Column('volume', FLOAT(precision=53)),
        Column('capacity', FLOAT(precision=53)),
        Column('portionSize', INTEGER()),
        Column('raceID', INTEGER()),
        Column('basePrice', DECIMAL(precision=19, scale=4)),
        Column('published', Boolean),
        Column('marketGroupID', INTEGER()),
        Column('iconID', INTEGER()),
        Column('soundID', INTEGER()),
        Column('graphicID', INTEGER()),
        schema="evesde"
)


maintypelist=[]

groupurl="https://esi.evetech.net/latest/markets/groups/?datasource=tranquility"

grouplookupurl="https://esi.evetech.net/latest/markets/groups/{}/?datasource=tranquility&language=en-us"


typelookupurl='https://esi.evetech.net/latest/universe/types/{}/'

errorcount=0
requests_cache.install_cache("item_cache",expire_after=35000)

lookup=select([invTypes])
result=connection.execute(lookup).fetchall()

sdetypelist=[]

for typedata in result:
    sdetypelist.append(typedata.typeID)

reqs_num_workers=50

session = FuturesSession(max_workers=reqs_num_workers)

groups=requests.get(groupurl)

groupjson=groups.json()


groupfuture=[]


for group in groupjson:
    groupfuture.append(session.get(grouplookupurl.format(group)))


pbar = tqdm(total=len(groupjson))
for groupdata in as_completed(groupfuture):
    try:
        groupdatajson=groupdata.result().json()
        for grouptype in groupdatajson.get('types',[]):
            maintypelist.append(grouptype)
        pbar.update(1)
    except:
        print groupdata.result().url
pbar.close()


firstbadlist=getitems(maintypelist)
print "Getting badlist"
secondbadlist=getitems(firstbadlist)


trans.commit()
