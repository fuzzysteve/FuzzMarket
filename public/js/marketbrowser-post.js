$(document).ready( function () {
        loadMarketGroupsBase();
        $('#selldata').DataTable({
            paging: false,
            searching: false,
            "info": false,
            responsive: true,
            "columns": [
            {type:'date'}, 
            {type:'num-fmt',responsivePriority: 3}, 
            {type:'num-fmt'}, 
            {type:'num-fmt'}, 
            {type:'num-fmt',responsivePriority: 1}, 
            {type:'num-fmt'}, 
            {type:'html',responsivePriority: 2}, 
            {type:'text'}, 
        ]
        });

        $('#buydata').DataTable({
            paging: false,
            searching: false,
            "info": false,
            responsive: true,
            "columns": [
            {type:'date'}, 
            {type:'num-fmt',responsivePriority: 3}, 
            {type:'num-fmt'}, 
            {type:'num-fmt'}, 
            {type:'num-fmt',responsivePriority: 1}, 
            {type:'num-fmt'}, 
            {type:'num-fmt'}, 
            {type:'html',responsivePriority: 2}, 
            {type:'text'}, 
        ]
        });


        $('#selltab').click(function(event){
            event.stopPropagation();
            $("#buydata_wrapper").hide();
            $("#selldata_wrapper").show();
            $("#selldata").DataTable().columns.adjust().responsive.recalc()
            $('#selltab').toggleClass("selectedtab");
            $('#buytab').toggleClass("selectedtab");
        });
        $('#buytab').click(function(event){
            event.stopPropagation();
            $("#selldata_wrapper").hide();
            $("#buydata_wrapper").show();
            $("#buydata").DataTable().columns.adjust().responsive.recalc()
            $('#selltab').toggleClass("selectedtab");
            $('#buytab').toggleClass("selectedtab");
        });

  $(function() {
    $.getJSON( "/api/typeids", function( data ) {
    $( "#searchbarInput" ).autocomplete({
      source: data,
      minLength: 2,
      select: function( event, ui ) {
          event.preventDefault();
          $( "#searchbarInput" ).val(ui.item.label);
          loadItem(ui.item.value);
      }
    });
  });
});



} );
$('#regionselector').change(function(){

    regionid=$('#regionselector').val();
    if (selecteditem!=0){
        loadItem(selecteditem);
    }


});
