var marketGroupsBase="https://market.fuzzwork.co.uk/api/marketgroup/";

var regionid="10000002";
var selecteditem="";

$.fn.dataTable.ext.order.intl = function ( locales, options ) {
    if ( window.Intl ) {
        var collator = new window.Intl.Collator( locales, options );
        var types = $.fn.dataTable.ext.type;
 
        delete types.order['string-pre'];
        types.order['string-asc'] = collator.compare;
        types.order['string-desc'] = function ( a, b ) {
            return collator.compare( a, b ) * -1;
        };
    }
};



function loadMarketGroupsBase() {
    $.getJSON(marketGroupsBase+"0/",function(data,status,xhr) {
        $.map(data.marketgroups,function(group){
                $("#browsemenulist").append("<li data-groupid='"+group.marketGroupID+"' class='groupLink' data-groupicon='"+group.iconID+"'>"+group.marketGroupName+"</li>");
            }
        );
        $('.groupLink').click(function(event){event.stopPropagation();openSubGroup(event.target);});
    });
}

function openSubGroup(group)
{
    var node;
    var itemcount=0;
    if ($(group).children('ul').length>0) {
        $(group).children('ul').toggle();
    } else {
        $(group).append('<ul class="subdisplay"></ul>');
        node=$(group).children('ul');
        $.getJSON(marketGroupsBase+group.dataset.groupid+"/",function(data,status,xhr) {
            $.map(data.marketgroups,function(item){
                node.append("<li data-groupid='"+item.marketGroupID+"' class='groupLink' data-groupicon='"+group.iconID+"'>"+item.marketGroupName+"</li>");
            });
            $.map(data.types,function(item){
                 node.append("<li data-type='"+item.typeID+"' class='itemLink'>"+item.typeName+"</li>");
            });
            $('.itemLink').click(function(event){event.stopPropagation();updateInfo(event.target.dataset.type);});
        });
    }
}

function ccpRound(number,digits) {
    return Number.parseFloat(number).toPrecision(digits)
}

function updateInfo(itemid)
{
    selecteditem=itemid
    loadItem(itemid);
    try {
        var stateObj = {};
//        history.pushState(stateObj, itemid, "/browser/"+regionid+"/"+itemid+"/");
    } catch(err) { console.log("No pushstate");  }
}

function loadItem(type) {
    selecteditem=type
    if (isFinite(type)) {
        lookupUrl="https://market.fuzzwork.co.uk/api/region/"+regionid+"/type/"+type+"/";
    } else {
        lookupUrl=type;
    }
    $.getJSON(lookupUrl,function(data){
        $.fn.dataTable.ext.order.intl();
        selldata=$('#selldata').DataTable();
        selldata.rows().remove();
        for (var order in data.sellorders) {
            if (ccpRound(data.sellorders[order].security)>0.5) {
                secclass="high";
            } else if (ccpRound(data.sellorders[order].security)>0 ) {
                secclass="low";
            } else {
                secclass="null";
            }
            newrow=selldata.row.add([data.sellorders[order].issued,new Intl.NumberFormat().format(data.sellorders[order].volume),new Intl.NumberFormat().format(data.sellorders[order].volumeEntered),new Intl.NumberFormat().format(data.sellorders[order].minVolume),new Intl.NumberFormat().format(data.sellorders[order].price),data.sellorders[order].duration,"<span class='securityClass "+secclass+"'>"+ccpRound(data.sellorders[order].security,1)+"</span>"+data.sellorders[order].stationName,data.sellorders[order].regionName]);
        }
        selldata.draw();
        selldata.columns.adjust().draw();
        buydata=$('#buydata').DataTable();
        buydata.rows().remove();
        for (var order in data.buyorders) {
            if (ccpRound(data.buyorders[order].security)>0.5) {
                secclass="high";
            } else if (ccpRound(data.buyorders[order].security)>0 ) {
                secclass="low";
            } else {
                secclass="null";
            }
            newrow=buydata.row.add([data.buyorders[order].issued,new Intl.NumberFormat().format(data.buyorders[order].volume),new Intl.NumberFormat().format(data.buyorders[order].volumeEntered),new Intl.NumberFormat().format(data.buyorders[order].minVolume),new Intl.NumberFormat().format(data.buyorders[order].price),data.buyorders[order].range,new Intl.NumberFormat().format(data.buyorders[order].duration),"<span class='securityClass "+secclass+"'>"+ccpRound(data.buyorders[order].security,1)+"</span>"+data.buyorders[order].stationName,data.buyorders[order].regionName]);
        }
        buydata.draw();
        buydata.columns.adjust().draw();
        $("#leftdata").empty();
        $("#rightdata").empty();
        $("#leftdata").append("<h1>"+data.typename+"</h5>")


    });
}

