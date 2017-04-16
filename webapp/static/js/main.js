mapboxgl.accessToken = 'pk.eyJ1IjoibmduZyIsImEiOiJjaXZ6YW1iYzcwMm03MnRvdThsa3JxdmFsIn0.-o5RpWc7uSDCIiNEpjgzNA';

var pos, neg, neu;
var map = new mapboxgl.Map({
    container: 'map',
    style: 'mapbox://styles/mapbox/streets-v8',
    center: [0,0],
    zoom: 2
});

var buildFeature = function(coord) {
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": coord
        },
        "properties": {
            "marker-symbol": "monument"
        }
    }
};

var features = coords.map(function(c) {
    return buildFeature(c)
});

map.on('style.load', function () {
    map.addSource("markers", {
        "type": "geojson",
        "data": {
            "type": "FeatureCollection",
            "features": features
        }
    });

    map.addLayer({
        "id": "markers",
        "type": "symbol",
        "source": "markers",
        "layout": {
            "icon-image": "{marker-symbol}-15",
            "text-field": "{title}",
            "text-offset": [0, 0.6],
            "text-anchor": "top"
        }
    });
});


var btn = document.getElementById("submit");
btn.addEventListener("click", function(){
    var q = document.getElementById("search");
    window.location = "/?q=" + q.value;
});


// websockets
var WS_URL = "ws://" + location.host + "/receive";
var inbox = new ReconnectingWebSocket(WS_URL);
inbox.onclose = function() {
    console.log("inbox closed");
    this.inbox = new WebSocket(inbox.url);
}

inbox.onmessage = function(msg) {
     var msg = JSON.parse(msg.data);
     console.log(msg);
}
