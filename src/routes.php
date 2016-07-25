<?php
// Routes

$app->get('/appraisal/',function ($request, $response, $args) {
    return $this->renderer->render($response, 'appraisal.phtml', $args);    
});

$app->post('/appraisal/',function ($request, $response, $args) {
    
    $region=$_POST['region'];
    if (!ctype_digit($region)) {
        exit;
    }

    $db = new PDO("pgsql:host=localhost;dbname=marketdata;user=marketdata;password=marketdatapass");
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    $sql=<<<EOS
select lower("typeName") "typeName","typeID" from evesde."invTypes" where published=true and "marketGroupID" is not null
EOS;
    $stmt = $db->prepare($sql);
    $stmt->execute();
    $typeidlookup=array();
    while ($row = $stmt->fetchObject()) {
            $typeidlookup[$row->typeName]=$row->typeID;
    }
    $inventory=array();

    if (array_key_exists('pasteblock',$_POST)) {
        $entries=explode("\n", $_POST['pasteblock']);
    } else {
        exit;
    }

    foreach ($entries as $entry) {
        $entry=strtolower(preg_replace('/(\d),(\d)/','$1$2',$entry));
        if (preg_match("/^(30 Day Pilot.*)\t(\d+)\t(.*)$/", trim($entry), $matches)) {
            if (isset($typeidlookup[$matches[1]])) {
                if (isset($inventory[$typeidlookup[$matches[1]]])) {
                    $inventory[$typeidlookup[$matches[1]]]+=$matches[2];
                } else {
                    $inventory[$typeidlookup[$matches[1]]]=$matches[2];
                }
            }
        } elseif (preg_match("/^(\d+) (.*)$/", trim($entry), $matches)) {
            if (isset($typeidlookup[$matches[2]])) {
                if (isset($inventory[$typeidlookup[$matches[2]]])) {
                    $inventory[$typeidlookup[$matches[2]]]+=$matches[1];
                } else {
                    $inventory[$typeidlookup[$matches[2]]]=$matches[1];
                }
            }
        } elseif (preg_match("/^(.*)\t([\d.,]+)\t/", trim($entry), $matches)) {
            if (isset($typeidlookup[$matches[1]])) {
                $quantity=str_replace(',', '', str_replace(',', '', $matches[2]));
                if (isset($inventory[$typeidlookup[$matches[1]]])) {
                    $inventory[$typeidlookup[$matches[1]]]+=$quantity;
                } else {
                    $inventory[$typeidlookup[$matches[1]]]=$quantity;
                }
            }
        } elseif (preg_match("/^(.*):\s+([\d.,]+)/", trim($entry), $matches)) {
            if (isset($typeidlookup[$matches[1]])) {
                $quantity=str_replace(',', '', str_replace(',', '', $matches[2]));
                if (isset($inventory[$typeidlookup[$matches[1]]])) {
                    $inventory[$typeidlookup[$matches[1]]]+=$quantity;
                } else {
                    $inventory[$typeidlookup[$matches[1]]]=$quantity;
                }
            }
        } elseif (preg_match("/^(.*)\t([\d.,]+)/", trim($entry), $matches)) {
            if (isset($typeidlookup[$matches[1]])) {
                $quantity=str_replace(',', '', str_replace(',', '', $matches[2]));
                if (isset($inventory[$typeidlookup[$matches[1]]])) {
                    $inventory[$typeidlookup[$matches[1]]]+=$quantity;
                } else {
                    $inventory[$typeidlookup[$matches[1]]]=$quantity;
                }
            }
        } elseif (preg_match("/^\[(.*),.*]/", trim($entry), $matches)) {
            if (isset($typeidlookup[$matches[1]])) {
                $quantity=1;
                if (isset($inventory[$typeidlookup[$matches[1]]])) {
                    $inventory[$typeidlookup[$matches[1]]]+=$quantity;
                } else {
                    $inventory[$typeidlookup[$matches[1]]]=$quantity;
                }
            }
        } elseif (preg_match("/^(.*), Qty: (\d+)/", trim($entry), $matches)) {
            if (isset($typeidlookup[$matches[1]])) {
                $quantity=$matches[2];
                if (isset($inventory[$typeidlookup[$matches[1]]])) {
                    $inventory[$typeidlookup[$matches[1]]]+=$quantity;
                } else {
                    $inventory[$typeidlookup[$matches[1]]]=$quantity;
                }
            }
        } elseif (preg_match("/^.*\t(.*)\t.*/", trim($entry), $matches)) {
            if (isset($typeidlookup[$matches[1]])) {
                $quantity=1;
                if (isset($inventory[$typeidlookup[$matches[1]]])) {
                    $inventory[$typeidlookup[$matches[1]]]+=$quantity;
                } else {
                    $inventory[$typeidlookup[$matches[1]]]=$quantity;
                }
            }
        } elseif (preg_match("/^(.*)/", trim($entry), $matches)) {
            if (isset($typeidlookup[$matches[1]])) {
                $quantity=1;
                if (isset($inventory[$typeidlookup[$matches[1]]])) {
                    $inventory[$typeidlookup[$matches[1]]]+=$quantity;
                } else {
                    $inventory[$typeidlookup[$matches[1]]]=$quantity;
                }
            }
        }
    }

    if (count($inventory)) {
        $sql="insert into appraisal (identifier,list) values (:id,:list)"; 
        $stmt = $db->prepare($sql);
        $identifier=uniqid();
        $stmt->execute(array(":id"=>$identifier,":list"=>json_encode($inventory)));

        return $response->withRedirect('/appraisal/'.$region.'/'.$identifier);
    }
    return $this->renderer->render($response, 'appraisal.phtml', $args);
});


$app->get('/appraisal/{region:[0-9]+}/{identifier}',function ($request, $response, $args) {

    $db = new PDO("pgsql:host=localhost;dbname=marketdata;user=marketdata;password=marketdatapass");
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    $sql=<<<EOS
select "typeName","typeID",volume from evesde."invTypes" where published=true and "marketGroupID" is not null
EOS;
    $stmt = $db->prepare($sql);
    $stmt->execute();
    $typenamelookup=array();
    while ($row = $stmt->fetchObject()) {
            $typenamelookup[$row->typeID]=array("typeName"=>$row->typeName,"volume"=>$row->volume);
    }
    $sql='select "regionName","regionID" from evesde."mapRegions" where "regionID"=:region';
    $stmt = $db->prepare($sql);
    $stmt->execute(array(":region"=>$args['region']));
    $row = $stmt->fetchObject();
    $regionName=$row->regionName;
    $regionid=$row->regionID;

    $args['region']=$regionName;
    $args['regionid']=$regionid;

    $sql='select list from appraisal where identifier=:id';
    $stmt = $db->prepare($sql);
    $stmt->execute(array(":id"=>$args['identifier']));
    $row = $stmt->fetchObject();
    $list=$row->list;

    $items=json_decode($list);
    
    $redis = new Predis\Client();

    $appraisal=array();
    $total=array();
    $total['pbuy']=0;
    $total['mbuy']=0;
    $total['psell']=0;
    $total['msell']=0;
    $total['volume']=0;
    foreach ($items as $item=>$quantity) {
        $buydetails=explode("|",$redis->get($regionid.'|'.$item."|true"));
        $selldetails=explode("|",$redis->get($regionid.'|'.$item."|false"));
        $line=array();
        $line['typeid']=$item;
        $line['typename']=$typenamelookup[$item]['typeName'];
        $line['quantity']=$quantity;
        $line['pbuy']=$quantity*$buydetails[7];
        $line['psell']=$quantity*$selldetails[7];
        $line['mbuy']=$quantity*$buydetails[1];
        $line['msell']=$quantity*$selldetails[2];
        $line['volume']=$quantity*$typenamelookup[$item]['volume'];
        $total['pbuy']+=$line['pbuy'];
        $total['mbuy']+=$line['mbuy'];
        $total['psell']+=$line['psell'];
        $total['msell']+=$line['msell'];
        $total['volume']+=$line['volume'];
        $appraisal[]=$line;
    }

    $args['appraisal']=$appraisal;
    $args['total']=$total;
    return $this->renderer->render($response, 'displayappraisal.phtml', $args);    

});


$app->get('/aggregates/',function ($request, $response, $args) {
    $redis = new Predis\Client();
    $aggregate=array();
    $allGetVars = $request->getQueryParams();
    if (isset($allGetVars['region'])){
        $region=$allGetVars['region'];
    } elseif (isset($allGetVars['station'])){
         $region=$allGetVars['station'];
    }
    $types=$allGetVars['types'];
    $ordertype=array("true"=>"buy","false"=>"sell");
    foreach (explode(",",$types) as $type) {
        if ($type == '') {
            continue;
        }
        foreach (array("true","false") as $buy) {
            $details=explode("|",$redis->get($region.'|'.$type."|".$buy));
            if ($details[0]==""){
                $details[0]=null;
            }
            $aggregate[$type][$ordertype[$buy]]=array(
                "weightedAverage"=>$details[0],
                "max"=>$details[1],
                "min"=>$details[2],
                "stddev"=>$details[3],
                "median"=>$details[4],
                "volume"=>$details[5],
                "orderCount"=>$details[6],
                "percentile"=>$details[7]
            );
        }
    }
    $response=$response->withHeader('Access-Control-Allow-Origin', '*');
    $resWithExpires = $this->cache->withExpires($response->withJson($aggregate), time() + 300);
    return $resWithExpires;
});



$app->get('/', function ($request, $response, $args) {
    $redis = new Predis\Client();
    $aggregate=array();
    foreach (array(34,35,36,37,38,39,40,11399,29668,40520) as $type) {
        $aggregate[$type]=array();
        foreach (array("true","false") as $buy) {
            $aggregate[$type][$buy]=explode("|",$redis->get('60003760|'.$type."|".$buy));
        }
    }
    $args['fpbuy']=json_decode($redis->get('fp-buy'));
    $args['fpsell']=json_decode($redis->get('fp-sell'));
    $args['fplastupdate']=$redis->get('fp-lastupdate');
    $args['types']=array("Tritanium","Pyrite","Mexallon","Isogen","Nocxium","Zydrine","Megacyte","Morphite","PLEX","Skill Injector");
    $args['maggs']=$aggregate;
    return $this->renderer->render($response, 'index.phtml', $args);
});

$app->get("/api/typeids", function ($request, $response)  use ($app) {
    $db = new PDO("pgsql:host=localhost;dbname=marketdata;user=marketdata;password=marketdatapass");
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    $sql='select "typeID" as value,"typeName" as label from evesde."invTypes" where "marketGroupID" is not null order by "typeName"';
    $stmt = $db->prepare($sql);
    $stmt->execute();
    $result= $stmt->fetchAll(PDO::FETCH_ASSOC);
    $resWithExpires = $this->cache->withExpires($response->withJson($result), time() + 3600);
    return $resWithExpires;
});

$app->get("/api/regionids", function ($request, $response)  use ($app) {
    $db = new PDO("pgsql:host=localhost;dbname=marketdata;user=marketdata;password=marketdatapass");
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    $sql='select "regionID" as value,"regionName" as label from evesde."mapRegions" where "regionID"<11000000 and "regionID" not in (10000004,10000017) order by "regionName"';
    $stmt = $db->prepare($sql);
    $stmt->execute();
    $result= $stmt->fetchAll(PDO::FETCH_ASSOC);
    $resWithExpires = $this->cache->withExpires($response->withJson($result), time() + 3600);
    return $resWithExpires;
});

$app->get("/api/stationids", function ($request, $response) use ($app) {
    $db = new PDO("pgsql:host=localhost;dbname=marketdata;user=marketdata;password=marketdatapass");
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    $sql='select "stationID" as value,"stationName" as label from evesde."staStations" order by "stationName"';
    $stmt = $db->prepare($sql);
    $stmt->execute();
    $result= $stmt->fetchAll(PDO::FETCH_ASSOC);
    $resWithExpires = $this->cache->withExpires($response->withJson($result), time() + 3600);
    return $resWithExpires;
});

$app->get('/type/', function ($request, $response, $args) {
    return $this->renderer->render($response, 'types.phtml', $args);
});
$app->get('/hub/', function ($request, $response, $args) {
    return $this->renderer->render($response, 'hub.phtml', $args);
});
$app->get('/region/', function ($request, $response, $args) {
    return $this->renderer->render($response, 'region.phtml', $args);
});
$app->get('/station/', function ($request, $response, $args) {
    return $this->renderer->render($response, 'station.phtml', $args);
});
$app->get('/aggregate/', function ($request, $response, $args) {
    return $this->renderer->render($response, 'aggregate.phtml', $args);
});
$app->get('/api/', function ($request, $response, $args) {

    $files=glob('/opt/orderbooks/*.csv.gz');
    rsort($files);
    $args['files']=$files;

    return $this->renderer->render($response, 'api.phtml', $args);
});
$app->get('/about/', function ($request, $response, $args) {
    return $this->renderer->render($response, 'about.phtml', $args);
});

$app->get('/type/{type:[0-9]+}/', function ($request, $response, $args) {
    $db = new PDO("pgsql:host=localhost;dbname=marketdata;user=marketdata;password=marketdatapass");
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    $ordersetsql="select max(id) from orderset";
    $stmt = $db->prepare($ordersetsql);
    $stmt->execute();
    $result= $stmt->fetchAll(PDO::FETCH_ASSOC);
    $orderset=$result['0']['max'];

    $sellordersql=<<<EOS
        SELECT "orderID",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",duration,"stationName","typeName","regionName",region
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        WHERE orders."typeID"=:typeid
        AND orders."orderSet"=:orderset
        AND buy = False
        order by price asc,region
EOS;
    $buyordersql=<<<EOS
        SELECT "orderID",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",range,duration,"stationName","typeName","regionName",region
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        WHERE orders."typeID"=:typeid
        AND orders."orderSet"=:orderset
        AND buy = True
        order by price desc,region
EOS;
    $stmt = $db->prepare($sellordersql);
    $stmt->execute(array(":typeid"=>$args['type'],":orderset"=>$orderset));
    $sellorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt = $db->prepare($buyordersql);
    $stmt->execute(array(":typeid"=>$args['type'],":orderset"=>$orderset));
    $buyorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt =$db->prepare('select "typeName" from evesde."invTypes" where "typeID"=:typeid');
    $stmt->execute(array(":typeid"=>$args['type']));
    $nameres=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $db=null;
    $args['typename']=$nameres[0]['typeName'];
    $args['orderset']=$orderset;
    $args['buyorders']=$buyorders;
    $args['sellorders']=$sellorders;
    return $this->renderer->render($response, 'type.phtml', $args);

    
});


$app->get('/region/{region:[0-9]+}/type/{type:[0-9]+}/', function ($request, $response, $args) {
    $db = new PDO("pgsql:host=localhost;dbname=marketdata;user=marketdata;password=marketdatapass");
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    $ordersetsql="select max(id) from orderset";
    $stmt = $db->prepare($ordersetsql);
    $stmt->execute();
    $result= $stmt->fetchAll(PDO::FETCH_ASSOC);
    $orderset=$result['0']['max'];

    $sellordersql=<<<EOS
        SELECT "orderID",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",duration,"stationName","typeName","regionName",region
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        WHERE orders."typeID"=:typeid
        AND orders."orderSet"=:orderset
        AND region=:region
        AND buy = False
        order by price asc,region
EOS;
    $buyordersql=<<<EOS
        SELECT "orderID",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",range,duration,"stationName","typeName","regionName",region
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        WHERE orders."typeID"=:typeid
        AND orders."orderSet"=:orderset
        AND buy = True
        AND region=:region
        order by price desc,region
EOS;
    $stmt = $db->prepare($sellordersql);
    $stmt->execute(array(":typeid"=>$args['type'],":orderset"=>$orderset,":region"=>$args['region']));
    $sellorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt = $db->prepare($buyordersql);
    $stmt->execute(array(":typeid"=>$args['type'],":orderset"=>$orderset,":region"=>$args['region']));
    $buyorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt =$db->prepare('select "typeName" from evesde."invTypes" where "typeID"=:typeid');
    $stmt->execute(array(":typeid"=>$args['type']));
    $nameres=$stmt->fetchAll(PDO::FETCH_ASSOC);


    $db=null;
    $args['typename']=$nameres[0]['typeName'];
    $args['orderset']=$orderset;
    $args['buyorders']=$buyorders;
    $args['sellorders']=$sellorders;
    return $this->renderer->render($response, 'type.phtml', $args);
});

$app->get('/empire/type/{type:[0-9]+}/', function ($request, $response, $args) {
    $db = new PDO("pgsql:host=localhost;dbname=marketdata;user=marketdata;password=marketdatapass");
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    $ordersetsql="select max(id) from orderset";
    $stmt = $db->prepare($ordersetsql);
    $stmt->execute();
    $result= $stmt->fetchAll(PDO::FETCH_ASSOC);
    $orderset=$result['0']['max'];

    $sellordersql=<<<EOS
        SELECT "orderID",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",duration,"stationName","typeName","regionName",region
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        join "marketRegion" on orders.region="marketRegion".regionid
        WHERE orders."typeID"=:typeid
        AND orders."orderSet"=:orderset
        AND regiontype <= 2
        AND buy = False
        order by price asc,region
EOS;
    $buyordersql=<<<EOS
        SELECT "orderID",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",range,duration,"stationName","typeName","regionName",region
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        join "marketRegion" on orders.region="marketRegion".regionid
        WHERE orders."typeID"=:typeid
        AND orders."orderSet"=:orderset
        AND buy = True
        AND regiontype <= 2
        order by price desc,region
EOS;
    $stmt = $db->prepare($sellordersql);
    $stmt->execute(array(":typeid"=>$args['type'],":orderset"=>$orderset));
    $sellorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt = $db->prepare($buyordersql);
    $stmt->execute(array(":typeid"=>$args['type'],":orderset"=>$orderset));
    $buyorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt =$db->prepare('select "typeName" from evesde."invTypes" where "typeID"=:typeid');
    $stmt->execute(array(":typeid"=>$args['type']));
    $nameres=$stmt->fetchAll(PDO::FETCH_ASSOC);


    $db=null;
    $args['typename']=$nameres[0]['typeName'];
    $args['orderset']=$orderset;
    $args['buyorders']=$buyorders;
    $args['sellorders']=$sellorders;
    return $this->renderer->render($response, 'type.phtml', $args);
});

$app->get('/hub/type/{type:[0-9]+}/', function ($request, $response, $args) {
    $db = new PDO("pgsql:host=localhost;dbname=marketdata;user=marketdata;password=marketdatapass");
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    $ordersetsql="select max(id) from orderset";
    $stmt = $db->prepare($ordersetsql);
    $stmt->execute();
    $result= $stmt->fetchAll(PDO::FETCH_ASSOC);
    $orderset=$result['0']['max'];

    $sellordersql=<<<EOS
        SELECT "orderID",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",duration,"stationName","typeName","regionName",region
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        join hubstations on orders."stationID"=hubstations.stationID
        WHERE orders."typeID"=:typeid
        AND orders."orderSet"=:orderset
        AND buy = False
        order by price asc,region
EOS;
    $buyordersql=<<<EOS
        SELECT "orderID",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",range,duration,"stationName","typeName","regionName",region
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        join hubstations on orders."stationID"=hubstations.stationID
        WHERE orders."typeID"=:typeid
        AND orders."orderSet"=:orderset
        AND buy = True
        order by price desc,region
EOS;
    $stmt = $db->prepare($sellordersql);
    $stmt->execute(array(":typeid"=>$args['type'],":orderset"=>$orderset));
    $sellorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt = $db->prepare($buyordersql);
    $stmt->execute(array(":typeid"=>$args['type'],":orderset"=>$orderset));
    $buyorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt =$db->prepare('select "typeName" from evesde."invTypes" where "typeID"=:typeid');
    $stmt->execute(array(":typeid"=>$args['type']));
    $nameres=$stmt->fetchAll(PDO::FETCH_ASSOC);


    $db=null;
    $args['typename']=$nameres[0]['typeName'];
    $args['orderset']=$orderset;
    $args['buyorders']=$buyorders;
    $args['sellorders']=$sellorders;
    return $this->renderer->render($response, 'type.phtml', $args);
});


$app->get('/history/{orderid:[0-9]+}', function ($request, $response, $args) {
    $db = new PDO("pgsql:host=localhost;dbname=marketdata;user=marketdata;password=marketdatapass");
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    
    $sellordersql=<<<EOS
        SELECT "orderID","orderSet",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",duration,"stationName","typeName","regionName"
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        AND orders."orderID"=:orderid
        AND buy = False
        order by "orderSet" desc
EOS;
    $buyordersql=<<<EOS
        SELECT "orderID","orderSet",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",range,duration,"stationName","typeName","regionName"
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        AND orders."orderID"=:orderid
        AND buy = True
        order by "orderSet" desc
EOS;
    $stmt = $db->prepare($sellordersql);
    $stmt->execute(array(":orderid"=>$args['orderid']));
    $sellorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt = $db->prepare($buyordersql);
    $stmt->execute(array(":orderid"=>$args['orderid']));
    $buyorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $args['orderset']=$orderset;
    $args['buyorders']=$buyorders;
    $args['sellorders']=$sellorders;
    return $this->renderer->render($response, 'history.phtml', $args);

    
});

$app->get('/station/{station:[0-9]+}/type/{type:[0-9]+}/', function ($request, $response, $args) {
    $db = new PDO("pgsql:host=localhost;dbname=marketdata;user=marketdata;password=marketdatapass");
    $db->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
    $ordersetsql="select max(id) from orderset";
    $stmt = $db->prepare($ordersetsql);
    $stmt->execute();
    $result= $stmt->fetchAll(PDO::FETCH_ASSOC);
    $orderset=$result['0']['max'];

    $sellordersql=<<<EOS
        SELECT "orderID",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",duration,"stationName","typeName","regionName",region
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        WHERE orders."typeID"=:typeid
        AND orders."orderSet"=:orderset
        AND orders."stationID"=:station
        AND buy = False
        order by price asc,region
EOS;
    $buyordersql=<<<EOS
        SELECT "orderID",orders."typeID",issued,orders.volume,"volumeEntered","minVolume",price,orders."stationID",range,duration,"stationName","typeName","regionName",region
        FROM orders
        JOIN evesde."staStations" sta on orders."stationID"=sta."stationID"
        JOIN evesde."invTypes" it on orders."typeID"=it."typeID"
        JOIN evesde."mapRegions" mr on mr."regionID"=region
        WHERE orders."typeID"=:typeid
        AND orders."orderSet"=:orderset
        AND buy = True
        AND orders."stationID"=:station
        order by price desc,region
EOS;
    $stmt = $db->prepare($sellordersql);
    $stmt->execute(array(":typeid"=>$args['type'],":orderset"=>$orderset,":station"=>$args['station']));
    $sellorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt = $db->prepare($buyordersql);
    $stmt->execute(array(":typeid"=>$args['type'],":orderset"=>$orderset,":station"=>$args['station']));
    $buyorders=$stmt->fetchAll(PDO::FETCH_ASSOC);
    $stmt =$db->prepare('select "typeName" from evesde."invTypes" where "typeID"=:typeid');
    $stmt->execute(array(":typeid"=>$args['type']));
    $nameres=$stmt->fetchAll(PDO::FETCH_ASSOC);


    $db=null;
    $args['typename']=$nameres[0]['typeName'];
    $args['orderset']=$orderset;
    $args['buyorders']=$buyorders;
    $args['sellorders']=$sellorders;
    return $this->renderer->render($response, 'type.phtml', $args);
});
