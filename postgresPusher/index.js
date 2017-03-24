'use strict';

let globalContext;

const async = require('async'),
      pg = require('pg'),
      Tile = require('geotile'),
      moment = require("moment"),
      NodeCache = require( "node-cache" ),
      Services = require('./services');

const TIMESPAN_TYPES = {
                'hour': {
                    format: "[hour]-YYYY-MM-DDHH:00", rangeFormat: "hour"
                },
                'day': {
                    format: "[day]-YYYY-MM-DD", rangeFormat: "day"
                },
                'month': {
                   format: "[month]-YYYY-MM", rangeFormat: "month"
                },
                'week': {
                   format: "[week]-YYYY-WW", rangeFormat: "isoweek"
                },
                'year': {
                   format: "[year]-YYYY", rangeFormat: "year"
                }
};

const FORTIS_DATA_STORE_TTL = 3600;
const fortisSiteCache = new NodeCache( { stdTTL: FORTIS_DATA_STORE_TTL} );
const PUSH_PARALLELISM = process.env.PUSH_PARALLELISM || 50;
const MAX_RETRIES = 5;
const FORTIS_SITE_NAME = process.env.FORTIS_SITE_NAME;
const FetchSiteDefinition = (siteId, callback) => {
    const siteDefintion = fortisSiteCache.get(siteId);

    if(siteDefintion && siteDefintion.properties){
        callback(undefined, siteDefintion);
    }else{
        globalContext.log(`Loading site settings from settings service for site [${siteId}]`);
        Services.getSiteDefintion(siteId, (error, response, body) => {
            if(!error && response.statusCode === 200 && body.data && body.data.siteDefinition) {
                const settings = body.data.siteDefinition.sites;
                
                if(settings && settings.length > 0 && settings[0] && settings[0].properties){
                        fortisSiteCache.set(siteId, settings[0]);
                        callback(undefined, settings[0]);
                }else{
                        const errMsg = `site [${siteId}] does not exist.`; 
                        globalContext.log(errMsg);
                        callback(errMsg, undefined);
                }
            }else{
                const errMsg = `[${error}] occured while processing message request`;
                globalContext.log(errMsg);
                callback(errMsg, undefined);
            }
        });
    }
};

let appInsightsClient = require("applicationinsights").getClient();
let layerType;
let postgresClient;
let processedFeatures = new Set();
let failures = new Set();

function RaiseException(errorMsg, exit){
    globalContext.log('error occurred: ' + errorMsg);

    if(appInsightsClient.config){
        appInsightsClient.trackException(new Error(errorMsg));
        if(exit && globalContext){
            globalContext.log("Exiting from error " + errorMsg);
        }
    }else{
        globalContext.log('App Insight is not properly setup. Please make sure APPINSIGHTS_INSTRUMENTATIONKEY is defined');
    }
}

function pushFeature(feature, callback) {
    let tileJSON;
    
    try{
         tileJSON = parseRow(feature);
    }catch(err){
         let errMsg = `error[${err}] occurred parsing tile Entry [${feature}]`
         RaiseException(errMsg);
         failures.add(feature);
         return callback(errMsg);
    }

    let tupleKey = tileJSON[0];
    let measures = tileJSON[1];

    if (!Array.isArray(tupleKey)) return callback('tuple key is not a proper json array');
    if (tupleKey.length < 5) return callback('expecting tuple key to contain (source,term,layer,period,tileId)');
    if (measures.length < 2) return callback('measures need to contain at least 3 values');

    let source = tupleKey[0];
    let keyword = tupleKey[1];
    let layer = tupleKey[2];
    let period = tupleKey[3];
    let tileId = tupleKey[4];
    let negSentiment = measures[1];
    let mentions = measures[0];
    let periodSplit = period.split('-');
    let periodType = periodSplit[0].replace("[", "").replace("]","");
    let periodDate;
    
    let tile;
    
    try{
        if(TIMESPAN_TYPES[periodType]){
            periodDate = moment(period, TIMESPAN_TYPES[periodType].format).startOf(TIMESPAN_TYPES[periodType].rangeFormat).format();
        }else{
            globalContext.log(`invalid timespan ${feature}`);
            return callback();
        }
        tile = Tile.tileFromTileId(tileId);
    }catch(err){
        let errMsg = `error[${err}] occurred parsing tile feature for Entry [${JSON.stringify(tupleKey)}] for tileId [${tileId}]`;
        RaiseException(errMsg);
        failures.add(feature);
        return callback(errMsg);
    }
    
    let upsertQuery = `INSERT INTO tiles (
        tileid, keyword, period, periodtype, perioddate, source, neg_sentiment, mentions, zoom, layer, layertype, geog
    ) VALUES (
        '${tileId}',
        '${keyword}',
        '${period}',
        '${periodType}',
        '${periodDate}',
        '${source}',
        ${negSentiment},
        ${mentions},
        ${tile.zoom},
        '${layer}',
        '${layerType}',
        ST_SetSRID(ST_MakePoint(${tile.centerLongitude}, ${tile.centerLatitude}), 4326)
    ) ON CONFLICT (tileid, keyword, layer, period, source) DO UPDATE SET
        neg_sentiment = ${negSentiment},
        mentions = ${mentions}
    ;`;
    
    processedFeatures.add(tupleKey.join(","));
    
    let successful = false;
    let attempts = 0;
        
    async.whilst(
        () => {
            return !successful && attempts < MAX_RETRIES;
        },
        createBlobCallback => {
            postgresClient.query(upsertQuery, (err, results) => {
                attempts++;
                
                if (!err){
                    successful = true
                }else{
                    let errMsg = `error[${err}] occurred writing tile Entry to postgres`
                    RaiseException(errMsg);
                }

                return createBlobCallback();
            });
        },
        callback
    );
}

function pushResultSets(features, callback) {
    async.eachLimit(features, PUSH_PARALLELISM, pushFeature, callback);
}

function parseRow(line){
    var tupleEnd = /\)+\,/g;
    var lineEnd = /\]+\)/g;
    var quoteRE = /\'/g;
    var intro = /\(+\(/g;
    var none = /\,+\s+\None+\,/i;

    let str = line.replace(intro, "[[").replace(none, ", 'none',").replace(quoteRE, '"').replace(lineEnd, "]]").replace(tupleEnd, "],");

    let lineJSON = JSON.parse(str);
    if(lineJSON && Array.isArray(lineJSON) && lineJSON.length == 2){
         return lineJSON;
    }else{
        throw new Error("Invalid record format");
    }
}

module.exports = function(context, resultSet) {
    globalContext = context;
    layerType = context.bindingData.layertype;

    let start = new Date();

    try{
        let tileEntries = resultSet.split("\n");

        FetchSiteDefinition(FORTIS_SITE_NAME, (error, siteDefinition) => {
            const storageConnectionString = siteDefinition.properties.featuresConnectionString;
            if(!error && siteDefinition && tileEntries && storageConnectionString){
                postgresClient = new pg.Client(storageConnectionString);
                postgresClient.connect(err => {
                    context.log(`processing ${tileEntries.length} result set features`);
                    pushResultSets(tileEntries, err => {
                        let deltaMs = new Date().getTime() - start.getTime();
                        context.log(`total execution time: [${deltaMs}] Processed messages: [${processedFeatures.size}] failures: [${failures.size}]`);
                        let processedError;
                        
                        if(failures.size > 1){
                            processedError = `Check logs for error details.`;
                        }
                        
                        context.done(processedError);
                    });
                });
            }
        });
    }catch(error){
        RaiseException(error, true);
    }
};
