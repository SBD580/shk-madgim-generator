var elasticsearch = require('elasticsearch');
var _ = require('lodash');
var random = require('geojson-random');
var turf = require('turf');
var Promise = require('bluebird');

var startTime = Date.now()-1*1000*60*60*24; // 1 day from now
var endTime = Date.now();
var itemsConcurrent = 10;
var itemMinTime = 1*60; // 1 min
var itemMaxTime = 2*60*60; // 2 hours
var pathResolution = 10; // 10 seconds
var itemSpeedMin = 400; // m/s
var itemSpeedMax = 700; // m/s

startTime = Math.floor(startTime/1000);
endTime = Math.floor(endTime/1000);

var client = new elasticsearch.Client({
    host: 'localhost:9200',
    requestTimeout: 200000
    //log: 'trace'
});

client.indices.delete({index:'items',body:{
    // number_of_replicas: 0
}}).catch(function(e) {
    if (e.body.error.type != 'index_not_found_exception')
        throw e;
}).then(function(){
    return client.indices.create({index: 'items'}).catch(function(e) {
        if (e.body.error.type != 'index_already_exists_exception')
            throw e;
    });
},console.error).then(function() {
   return client.indices.putMapping({
        index: 'items',
        type: 'item',
        body: {
            properties: {
                startTime: {
                    type: 'long'
                },
                endTime: {
                    type: 'long'
                },
                path: {
                    type: 'geo_shape'
                }
            }
        }
    });
},console.error).then(function() {
    var promises = {};
    var items = [];
    for (var time = startTime; time <= endTime; time++) {
        // remove deleted items
        _.remove(items, function (item) {
            return item.endTime < time;
        });

        // generate (and store) new items
        var newItems = [];
        var newItemsCount = (itemsConcurrent - items.length);
        for (var i = 0; i < newItemsCount; i++) {
            var item = generateItem(time);
            items.push(item);
            newItems.push(item);
        }

        if(newItems.length){
            promises[time] = Promise.join(client.bulk({
                index: 'items',
                type: 'item',
                body: _.flatten(_.map(newItems,function(item,i){
                    return [{create:{_id:time+'_'+i}},item];
                }))
            })).bind({time:time,items:newItems.length}).then(function(){
                console.log(this.items+' items created ('+this.time+')');
                delete promises[this.time];
            });
        }
    }

    return Promise.all(_.values(promises));
},console.error).then(function(){
    console.log('DONE');
},console.error);

function generateItem(startTime){
    var itemEndTime = startTime + Math.floor(itemMinTime + (itemMaxTime - itemMinTime) * Math.random());
    var pathLength = Math.floor((itemEndTime - startTime) / pathResolution);
    var speed = itemSpeedMin + (itemSpeedMax - itemSpeedMin) * Math.random();
    var path = [[-90 + Math.random() * 180, -45 + Math.random() * 90,0,startTime]];
    var angle = -180 + Math.random() * 360;
    for (var j = 0; j < pathLength; j++) {
        angle += (j + 1) % 20 == 0 ? -30 + Math.random() * 60 : 0;
        var p = turf.destination(path[path.length - 1], speed * pathResolution / 1000, angle, 'kilometers').geometry.coordinates;
        if (p[0] < -180) p[0] += 360;
        if (p[0] > 180) p[0] -= 360;
        if (p[1] < -90) p[1] += 180;
        if (p[1] > 90) p[1] -= 180;
        p.splice(p.length,0,0,startTime+pathResolution*j);
        path.push(p);
    }

    return {
        startTime: startTime,
        endTime: itemEndTime,
        path: {
            type: 'linestring',
            coordinates: path
        }
    };
}