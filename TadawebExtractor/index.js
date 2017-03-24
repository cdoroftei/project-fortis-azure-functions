"use strict";

let turf = require('@turf/turf');
let NodeCache = require( "node-cache" );
let Services = require('./services');
let appInsights = require("applicationinsights");
let appInsightsClient = appInsights.getClient();
let moment = require('moment');
let globalContext;

const FORTIS_SITE_NAME = process.env.FORTIS_SITE_NAME;
const FORTIS_DATA_STORE_TTL = 3600;
const SENTIMENT_MAP = {
    'negative': 0,
    'positive': 1,
    'neutral': 0.6
};

const fortisSiteCache = new NodeCache( { stdTTL: FORTIS_DATA_STORE_TTL} );
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

function HashCode(str){
    let hash = 0;
    if (str.length == 0) return hash;
    for (let i = 0; i < str.length; i++) {
        let char = str.charCodeAt(i);
        hash = ((hash<<5)-hash)+char;
        hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
}

function RaiseException(errorMsg) {
    globalContext.log('error occured: ' + errorMsg);

    if (appInsightsClient.config) {
        appInsightsClient.trackException(new Error(errorMsg));
    } else {
        globalContext.log('App Insight is not properly setup. Please make sure APPINSIGHTS_INSTRUMENTATIONKEY is defined');
    }
}

function FeatureInsideBBox(latLngCoords, bbox){
    const pt1 = {
            "type": "Feature",
            "geometry": {
                   "type": "Point",
                   "coordinates": [latLngCoords[1], latLngCoords[0]]
            }
    }, bboxPoly = turf.bboxPolygon(bbox);

    return turf.inside(pt1, bboxPoly);
}

function ParseEventHubMessage(tadaEventHubMessage, bbox){
      if(tadaEventHubMessage.language && tadaEventHubMessage.link && tadaEventHubMessage.sources 
       && tadaEventHubMessage.published_at && tadaEventHubMessage.text){           
            let iso_8601_created_at;
        
            try{
                    iso_8601_created_at = moment(tadaEventHubMessage.published_at, 'YYYY-MM-DD', 'en').toISOString();
            }catch(e){
                    RaiseException(`Failed parsing tadaweb published article date for event ${tadaEventHubMessage}`);

                    return undefined;
            }

            let message = {
                        "source": "tadaweb",
                        "created_at": iso_8601_created_at,
                        "lang": tadaEventHubMessage.language,
                        "message": {
                            "id": HashCode(tadaEventHubMessage.link).toString(),
                            "message": tadaEventHubMessage.text,
                            "link": tadaEventHubMessage.link,
                            "originalSources": tadaEventHubMessage.tada ? [tadaEventHubMessage.tada.name || ""] : "",
                            "title": tadaEventHubMessage.title
                        }
            };

            if(tadaEventHubMessage.sentiment && SENTIMENT_MAP[tadaEventHubMessage.sentiment.toLowerCase()]){
                message.message.neg_sentiment = SENTIMENT_MAP[tadaEventHubMessage.sentiment.toLowerCase()];
            }

            if(tadaEventHubMessage.cities && Array.isArray(tadaEventHubMessage.cities) && tadaEventHubMessage.cities.length > 0){
                try{
                    const featureCollection = Object.assign({}, {type: "FeatureCollection", 
                                                                     features: tadaEventHubMessage.cities.filter(feature=>feature.coordinates && FeatureInsideBBox(feature.coordinates, bbox))
                                                                                                  .map(feature=>Object.assign({}, {"type": "Point", "coordinates": [feature.coordinates[1], feature.coordinates[0]]}))});
                        
                    if(featureCollection.features.length > 0){
                            message.message.geo = featureCollection;
                    }
                }catch(error){
                    RaiseException(`Error [${error}]. Failed parsing tadaweb location details for event ${tadaEventHubMessage}`);

                    return undefined;
                }
            }

            return message;
      }else{
          RaiseException(`Required fields missing error encountered when parsing tadaweb article ${tadaEventHubMessage}`);

          return undefined;
      }
      
}

module.exports = function (context, tadaEventHubMessage) {
    globalContext = context;
    let errorMsg;

    FetchSiteDefinition(FORTIS_SITE_NAME, (error, siteDefinition) => {
        console.log('service returned');
        if(!error && siteDefinition) {
                const bbox = siteDefinition.properties.targetBbox.map(item=>parseFloat(item));
                let prenlpMessage = ParseEventHubMessage(tadaEventHubMessage, bbox);

                if(bbox && Array.isArray(bbox) && prenlpMessage){
                    context.bindings.preNlpQueueItem = prenlpMessage;
                    context.log(`Processed incoming tada event for ${JSON.stringify(tadaEventHubMessage)}`);
                    context.log(`Output ${JSON.stringify(prenlpMessage)}`);
                }else{
                    errorMsg = `An error occured post processing the tadaweb event. Check the above logs.`
                }
        }else{
                errorMsg = `[${error}] occured while looking up site definition`;
                context.log(errMsg);
        }

        
        context.done(errorMsg);
    });
}