"use strict"

let PostgresService = require('postgres-client');
let appInsightsClient = require("applicationinsights").getClient();
let Promise = require('promise');
let Services = require('./services');
let azureStorage = require('azure-storage');
let nlp = require('nlp_compromise');
let NodeCache = require( "node-cache" );

let logger = {};

const FORTIS_DATA_STORE_TTL = 3600;
const fortisSiteCache = new NodeCache( { stdTTL: FORTIS_DATA_STORE_TTL} );
const TOKENIZER_MIN_CHARACTERS_ALLOWED = 2;
const LOCATION_CONFIDENCE_THRESHOLD = 0.6;
const FORTIS_SITE_NAME = process.env.FORTIS_SITE_NAME;
const GEOTWIT_STORAGE_ACCT_NAME = process.env.PCT_GEOTWIT_AZURE_STORAGE_ACCOUNT;
const GEOTWIT_STORAGE_ACCESS_KEY = process.env.PCT_GEOTWIT_AZURE_STORAGE_KEY;
const tableSvc = azureStorage.createTableService(GEOTWIT_STORAGE_ACCT_NAME, GEOTWIT_STORAGE_ACCESS_KEY);
const TWITTER_USER_TABLE = "users";
const FetchSiteDefinition = (siteId, callback) => {
    const siteDefintion = fortisSiteCache.get(siteId);

    if(siteDefintion && siteDefintion.properties){
        callback(undefined, siteDefintion);
    }else{
        logger(`Loading site settings from settings service for site [${siteId}]`);
        Services.getSiteDefintion(siteId, (error, response, body) => {
            if(!error && response.statusCode === 200 && body.data && body.data.siteDefinition) {
                const settings = body.data.siteDefinition.sites;
                
                if(settings && settings.length > 0 && settings[0] && settings[0].properties){
                        fortisSiteCache.set(siteId, settings[0]);
                        callback(undefined, settings[0]);
                }else{
                        const errMsg = `site [${siteId}] does not exist.`; 
                        logger(errMsg);
                        callback(errMsg, undefined);
                }
            }else{
                const errMsg = `[${error}] occured while processing message request`;
                logger(errMsg);
                callback(errMsg, undefined);
            }
        });
    }
};

function RaiseException(errorMsg){
    logger('error occured: ' + errorMsg);

    if(appInsightsClient.config){
        appInsightsClient.trackException(new Error(errorMsg));
    }else{
        logger('App Insight is not properly setup. Please make sure APPINSIGHTS_INSTRUMENTATIONKEY is defined');
    }
}

function LatLongToGeoJSON(lat, lon, properties){
    let feature = JSON.parse(`{"type":"Point","coordinates":[${lon}, ${lat}]}`);

    return Object.assign({}, feature, (properties ? {properties} : {}));
}

function CreateFeatureCollection(feature){
    return {
            "type": "FeatureCollection",
            "features": [feature]
    };
}

function LocalitiesByTwitterUserGraphCallback(resolve, reject, userId){
    if(userId && userId.length > 0){
        tableSvc.retrieveEntity(TWITTER_USER_TABLE, userId.substring(0, 2), userId, (error, result, response) => {
            if(error){
                reject(`Unable to retrieve resource[${userId}] from table storage. message [${error}]`);
            }else if(result && result.location){
                let userLocation = JSON.parse(result.location._);
                
                if(userLocation.confidence && userLocation.confidence >= LOCATION_CONFIDENCE_THRESHOLD){
                    logger(`shared user location ${JSON.stringify(userLocation)}`);
                    let feature = CreateFeatureCollection(LatLongToGeoJSON(userLocation.latitude || userLocation.lat, userLocation.longitude || userLocation.lon, {"confidence": userLocation.confidence, "geoTwitUser": userId}));
                    resolve(feature);
                }else{
                    reject(`Location didn't meet the confidence threshhold [${LOCATION_CONFIDENCE_THRESHOLD}]`);
                }
            }else{
                reject(`undefined result return from table storage for userid [${userId}]`);
            }
        });
    }else{
        reject(`userId is undefined.`);
    }
}

function LocalitiesByNameCallback(error, results, resolve, reject, sentence, fieldNames) {
    if (error) {
        RaiseException(`Error occured [${error}]`);
        reject(`Error occured retrieving localities. [${error}]`);
    }
    else if (results && results.rows.length > 0) {
        var lexicon = {};
        var featureMap = {};
        results.rows.forEach(function (locationObject) {
            fieldNames.split(", ").forEach(function (fieldName) {
                locationObject[fieldName].split(",").forEach(location => {
                    if (location && location.length>3){
                        lexicon[location.toLowerCase()] = 'Place';
                        featureMap[location.toLowerCase()] = locationObject.feature;
                    }
                });
            });
        });
        
        let mentionedLocations = nlp(sentence.toLowerCase(), { lexicon: lexicon }).match('#Place+').asArray();
        let featuresSet = new Set();
        mentionedLocations.forEach(function (location) {
            if(featureMap[location.text]){
                featuresSet.add(featureMap[location.text]);
            }        
        })
        let features = Array.from(featuresSet).map(feature => {return JSON.parse(feature)});
        logger(`LOCATION [${JSON.stringify(features)}] sentence [${sentence}]`);
        resolve({ "type": "FeatureCollection", "features": Array.from(features) });
    } else {
        reject("No localities found");
    }
}

function findLocality(prevMessage, lang, sentence, supportedLanguages, storageConnectionString, callback) {
     let fieldNames = `${supportedLanguages.join("_name, ").replace(/en_/g, "")}_name`;//alternatenames,
        fieldNames += ", alternatenames";
     
     let query = `SELECT ${fieldNames},
	               MAX(st_asgeojson(geog)) as feature
                   FROM localities
                   GROUP BY ${fieldNames}`;
    
    logger(`previous rejection message [${prevMessage}]`);
    let promise = new Promise((resolve, reject) => {
        PostgresService(storageConnectionString, query, (error, results) => {
                LocalitiesByNameCallback(error, results, resolve, reject, sentence, fieldNames);
        });
    });

    return promise.then(locations => {
        callback(locations, lang, undefined)
    }, 
    error => callback(undefined, lang, error));
}

function ParseSharedLocation(features){
    logger(`shared user location ${JSON.stringify(features)}`);
    
    if(features && features.type && features.type === "FeatureCollection" && features.features && features.features.length > 0){
        return Object.assign({}, features, {
                                            features: features.features.map(feature => Object.assign({}, feature, {properties: {"source": "sharedLocation"}}))
                                           });
    }else if(features){
        let sharedLocationCoords = features.coordinates ? features.coordinates : features;
        logger(`feature is now ${JSON.stringify(sharedLocationCoords)}`);
        return CreateFeatureCollection(LatLongToGeoJSON(sharedLocationCoords[1], sharedLocationCoords[0], {"source": "sharedLocation"}));
    }
}

function inferLocations(userId, lang, sentence, callback, userSharedLocation, siteDefinition){
      const supportedLanguages = siteDefinition.properties.supportedLanguages;
      const storageConnectionString = siteDefinition.properties.featuresConnectionString;

      if(!supportedLanguages || !storageConnectionString){
             let errMsg = `either supportedLanguages or storageConnectionString is undefined.`;
             callback(undefined, undefined, errMsg);
      }else if(lang && supportedLanguages && supportedLanguages.indexOf(lang) == -1){
             let errMsg = `[${lang}] is an unsupported language.`;
             callback(undefined, lang, errMsg);
      }else if(userSharedLocation){//user shared location, no need to look it up
             const featureCollection = ParseSharedLocation(userSharedLocation);
             callback(featureCollection, lang, undefined);
      }else{
            let findLocationByUserPromise = new Promise((resolve, reject) => {
                LocalitiesByTwitterUserGraphCallback(resolve, reject, userId);
            });

            findLocationByUserPromise.then(locations => callback(locations, lang, undefined), 
                    error => findLocality(error, lang, sentence, supportedLanguages, storageConnectionString, callback)
            );
      }
}

function PreValidate(){
    if(!FORTIS_SITE_NAME){
        RaiseException(`Fortis site name is undefined error.`);

        return false;
    }

    if(!process.env.REACT_APP_SERVICE_HOST){
        RaiseException(`REACT_APP_SERVICE_HOST undefined error.`);

        return false;
    }

    if(!GEOTWIT_STORAGE_ACCT_NAME){
        RaiseException(`GEOTWIT_STORAGE_ACCT_NAME undefined error.`);

        return false;
    }

    if(!GEOTWIT_STORAGE_ACCESS_KEY){
        RaiseException(`GEOTWIT_STORAGE_ACCESS_KEY undefined error.`);

        return false;
    }

    return true;
}

module.exports = {
    findLocation: function(userId, lang, sentence, userSharedLocation, loggerInstance, callback) {
        logger = loggerInstance;

        if(PreValidate()){
            FetchSiteDefinition(FORTIS_SITE_NAME, (error, siteDefinition) => {
                if(!error && siteDefinition && siteDefinition.properties){
                    inferLocations(userId, lang, sentence, callback, userSharedLocation, siteDefinition);
                }else{
                    const errorMsg = `Undefined site definition error for site ${FORTIS_SITE_NAME}`;
                    callback(undefined, undefined, errorMsg);
                }
            });
        }else{
            callback(undefined, undefined, "Required env settings error.");
        }
    },
    LocalitiesByNameCallback:LocalitiesByNameCallback
};