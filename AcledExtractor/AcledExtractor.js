"use strict"

let moment = require('moment');
let azure = require('azure-storage');
let request = require('request');
let asyncEachLimit = require('async/eachLimit');
let turf = require('@turf/turf');
let appInsightsClient = require("applicationinsights").getClient();
let NodeCache = require( "node-cache" );
let Services = require('./services');
let logger = {};

const FORTIS_DATA_STORE_TTL = 3600;
const fortisSiteCache = new NodeCache( { stdTTL: FORTIS_DATA_STORE_TTL} );
const ASYNC_QUEUE_LIMIT = 100;
const ACLED_MAX_READ = 500;
const ACLED_ENDPOINT = 'acleddata.com';
const PRE_NLP_QUEUE = process.env.PRE_NLP_QUEUE;
const FORTIS_SITE_NAME = process.env.FORTIS_SITE_NAME;
const FIRST_ACLED_PAGE = 1;
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

let TextBase64QueueMessageEncoder = azure.QueueMessageEncoder.TextBase64QueueMessageEncoder;
let retryOperations = new azure.LinearRetryPolicyFilter();
let queueSvc = azure.createQueueService();
let messageCount = 0;
let errorMessageSet = new Set();

queueSvc.messageEncoder = new TextBase64QueueMessageEncoder();
queueSvc.createQueueIfNotExists(PRE_NLP_QUEUE, (error, result, response) => {
    if (error) {
        RaiseException(`Unable to create new azure queue ${PRE_NLP_QUEUE}`);
    }
});

function PreValidate(sinceDate, bbox) {
    let dateCheck = sinceDate && moment(sinceDate, "yyyy-mm-dd").isBefore(moment()) ? true : false; // date is prior to today

    if (!dateCheck) {
        RaiseException(`invalid from-date.`);
        return false;
    }

    if(!PRE_NLP_QUEUE){
        RaiseException(`undefined prenlp queue error.`);
        return false;
    }

    if(bbox && bbox.length === 4){
        return true;
    }else{
        RaiseException(`invalid bounding box [${boundingBox}]`);
        return false;
    }
}

function EventCBHandler(item, boundingBox, callback){
        let pt1 = {
            "type": "Feature",
            "geometry": {
                   "type": "Point",
                   "coordinates": [item.longitude, item.latitude]
            }
        };

        let poly = turf.bboxPolygon(boundingBox);
        if (turf.inside(pt1, poly)) {
                var iso_8601_created_at = null;
  
                try{
                    iso_8601_created_at = moment(item.event_date, 'YYYY-MM-DD', 'en');
                }catch(e){
                    console.error(JSON.stringify(tweet));
                }

                let message = {
                         'source': 'acled',
                         'created_at': iso_8601_created_at,
                         'message': {
                             "id": item.data_id,
                             "message": item.notes,
                             "event_date": item.event_date,
                             "created_at":  iso_8601_created_at,
                             "fatalities": item.fatalities,
                             "source": item.source,
                             "gwno": item.gwno,
                             "ally_actor_1": item.ally_actor_1,
                             "actor1": item.actor1,
                             "originalSources": [(item.source || "N/A")],
                             "title": `${item.event_date || ""} - ${item.source || ""} - ${item.event_type || ""}`,
                             "event_type": item.event_type,
                             "ally_actor_2": item.ally_actor_2,
                             "actor2": item.actor2,
                             "actor1Type": item.inter1,
                             "actor2Type": item.inter2
                         }
                };

                if(item.latitude && item.longitude){
                    message.message.geo = [parseFloat(item.longitude), parseFloat(item.latitude)];
                }

                try {
                    queueSvc.createMessage(PRE_NLP_QUEUE, JSON.stringify(message), (error, result, response) => {
                        if (error) {
                            RaiseException(`Azure Queue push error occured error [${error}]`);
                        } else {
                            if (++messageCount % 100 === 0) {
                                logger(`Wrote ${messageCount} messages to output queue.`);
                            }
                        }

                        return callback();
                   });
                } catch (error) {
                    RaiseException(`Issue with pushing message ${JSON.stringify(message)} to out queue.`);
                    
                    return callback();
                }
        } else {
            callback();
        }
}

// Retrieve all content posted from a given date to present-day
function retrieveAcledContent(fromDate, page, bbox, callback) {
    let GET = {
            url : `http://${ACLED_ENDPOINT}/api/acled/read?event_date=${fromDate}&event_date_where=%3E&page=${page}`,
            json: true,
            withCredentials: false
     };

     request(GET,  
            (error, response, body) => {
                if(!error && response.statusCode === 200 && body) {
                    if(body.count > 0){
                        logger(`Read ${body.count} items from acled on page ${page}`);

                        asyncEachLimit(body.data, ASYNC_QUEUE_LIMIT, (item, asyncCB)=>EventCBHandler(item, bbox, asyncCB), 
                               finalCBErr => {
                                   if(body.count === ACLED_MAX_READ){
                                       retrieveAcledContent(fromDate, ++page, bbox, callback);
                                   }else if(body.count < ACLED_MAX_READ){
                                       return callback(messageCount, errorMessageSet);
                                   }
                               }
                        );
                    }else{
                        return callback(messageCount, errorMessageSet);
                    }
                }else{
                    RaiseException(`[${error}] occured while processing acled request`);
                    return callback(messageCount, errorMessageSet);
                }
         }
    );
}

function RaiseException(errorMsg) {
    logger('error occured: ' + errorMsg);
    errorMessageSet.add(errorMsg);

    if (appInsightsClient.config) {
        appInsightsClient.trackException(new Error(errorMsg));
    } else {
        logger('App Insight is not properly setup. Please make sure APPINSIGHTS_INSTRUMENTATIONKEY is defined');
    }
}

module.exports = {
        ProcessRecentAcledActivity: function (fromDate, loggerInstance, callback) {
            logger = loggerInstance;
            logger("Acled extractor running");

            FetchSiteDefinition(FORTIS_SITE_NAME, (error, siteDefinition) => {
                if(!error && siteDefinition) {
                    const bbox = siteDefinition.properties.targetBbox.map(item=>parseFloat(item));
                    if (PreValidate(fromDate, bbox)) {
                        logger(`Processing Acled data from ${fromDate}`);
                        retrieveAcledContent(fromDate, FIRST_ACLED_PAGE, bbox, callback);
                    } else {
                        logger("PreValidate failed");
                        return callback(0, errorMessageSet);
                    }
                }else{
                    const errMsg = `[${error}] occured while looking up site definition`;
                    errorMessageSet.add(errMsg);
                    logger(errMsg);
                    return callback(0, errorMessageSet);
                }
            });
        }
};
