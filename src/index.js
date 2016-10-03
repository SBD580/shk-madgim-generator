var commandLineArgs = require('command-line-args');
var usage = require('command-line-usage');
var elasticsearch = require('elasticsearch');
var _ = require('lodash');
var random = require('geojson-random');
var turf = require('turf');
var Promise = require('bluebird');
var args = process.argv.slice(2);

var SOURCES = ['RS1','RS2','RS3','RS4','RS5','RS6'];

var optionsDefinitions = [
    {name: 'start', type: Number, defaultValue: Date.now()-1*1000*60*60*24, description: 'The starting time in milliseconds (default to 24 hours ago)'},
    {name: 'end', type: Number, defaultValue: Date.now(), description: 'The ending time in milliseconds (default to now)'},
    {name: 'items', type: Number, defaultValue: 10, description: 'Number of concurrent items (default to 10)'},
    {name: 'itemMinTime', type: Number, defaultValue: 60, description: 'Minimum number of seconds for an item to exists (default to 1 minute)'},
    {name: 'itemMaxTime', type: Number, defaultValue:2*60*60, description: 'Maximum number of seconds for an item to exists (default to 2 hours)'},
    {name: 'itemMinSpeed', type: Number, defaultValue: 400, description: 'Minimum speed for an item in m/s (default to 400)'},
    {name: 'itemMaxSpeed', type: Number, defaultValue: 700, description: 'Maximum speed for an item in m/s (default to 700)'},
    {name: 'res', type: Number, defaultValue: 10, description: 'Path resolution in seconds (default to 10)'},
    {name: 'clean', type: Boolean, defaultValue: false, description: 'remove currently existing indices'},
    {name: 'elastic', type: String, defaultValue: 'localhost:9200', description: 'host:port for the elasticsearch instance'},
    {name: 'help', type: Boolean, defaultValue: false, description: 'print usage message and exit'}
];
var options = commandLineArgs(optionsDefinitions);

if(options.help){
    console.error(usage({header:'Options',optionList:optionsDefinitions}));
    process.exit(1);
}

var removeExistingIndices = options.clean;
var itemsConcurrent = options.items;
var startTime = options.start; // 1 day from now
var endTime = options.end;
var itemMinTime = options.itemMinTime; // 1 min
var itemMaxTime = options.itemMaxTime; // 2 hours
var pathResolution = options.res; // 10 seconds
var itemSpeedMin = options.itemMinSpeed; // m/s
var itemSpeedMax = options.itemMaxSpeed; // m/s

startTime = Math.floor(startTime/1000);
endTime = Math.floor(endTime/1000);

var client = new elasticsearch.Client({
    host: options.elastic,
    requestTimeout: 200000
    //log: 'trace'
});

(removeExistingIndices?client.indices.delete({index:'items',body:{
    // number_of_replicas: 0
}}).catch(function(e) {
    if (e.body.error.type != 'index_not_found_exception')
        throw e;
}):Promise.resolve(null)).then(function(){
    return client.indices.create({index: 'items'}).catch(function(e) {
        if (e.body.error.type != 'index_already_exists_exception')
            throw e;
    });
}).then(function() {
   return client.indices.putMapping({
        index: 'items',
        type: 'item',
        body: {
            _all: {
                enabled: false
            },
            properties: {   
                indexed: {
                    type: 'date',
                    format: 'epoch_second'
                },
                startTime: {
                    type: 'date',
                    format: 'epoch_second'
                },
                endTime: {
                    type: 'date',
                    format: 'epoch_second'
                },
                path: {
                    type: 'geo_shape'
                },
                src: {
                    type: 'string',
                    index: 'not_analyzed'
                },
                type: {
                    type:'string',
                    index: 'not_analyzed'
                }
            }
        }
    });
}).then(function() {
    var promises = {};
    var items = [];
    for (var time = startTime; time <= endTime; time++) {
        // remove deleted items
        _.remove(items, function (item) {
            return item.endTime < time;
        });

        // generate new items
        var newItems = [];
        var newItemsCount = (itemsConcurrent - items.length);
        for (var i = 0; i < newItemsCount; i++) {
            var item = randomItem(time);
            items.push(item);
            newItems.push(item);
        }

        // store new items
        if(newItems.length){
            promises[time] = Promise.join(client.bulk({
                index: 'items',
                type: 'item',
                body: _.flatten(_.map(newItems,function(item,i){
                    return [{create:{}},_.assign({indexed:parseInt(Date.now()/1000)},item)];
                }))
            })).bind({time:time,items:newItems.length}).then(function(){
                console.log(this.items+' items created ('+this.time+')');
                delete promises[this.time];
            });
        }
    }

    return Promise.all(_.values(promises));
}).then(function(){
    console.log('DONE');
}).catch(console.error);

function randomItem(startTime){
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

    var type = Math.random()<=0.3?'T':'R';
    return {
        startTime: startTime,
        endTime: itemEndTime,
        type: type,
        src: _.sampleSize(SOURCES,type=='T'?_.random(1,4):1),
        path: {
            type: 'linestring',
            coordinates: path
        }
    };
}