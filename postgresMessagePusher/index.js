'use strict';

let appInsightsClient = require("applicationinsights").getClient();
let NodeCache = require( "node-cache" );
let Services = require('./services');
let async = require('async');
let pg = require('pg');

let globalContext;
let postgresClient;
let processedFeatures = new Set();
let failures = new Set();

const FORTIS_DATA_STORE_TTL = 3600;
const fortisSiteCache = new NodeCache( { stdTTL: FORTIS_DATA_STORE_TTL} );
const MAX_CHARACTER_LIMIT = 200;
const PUSH_PARALLELISM = process.env.MESSAGE_PUSH_PARALLELISM || 50;
const MAX_RETRIES = 5;
const FORTIS_SITE_NAME = process.env.FORTIS_SITE_NAME;
const requiredFields = ["Sentiment", "Language", "MessageId", "PartitionKey", "Sentence", "EventProcessedUtcTime", "Keywords", "Locations"]; 
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

function RaiseException(errorMsg, exit){
    globalContext.log('postgresMessagePusher - error occurred: ' + errorMsg);

    if(appInsightsClient.config){
        appInsightsClient.trackException(new Error(errorMsg));
        if(exit && globalContext){
            globalContext.log("Exiting from error " + errorMsg);
            globalContext.done(errorMsg);
        }
    }else{
        globalContext.log('App Insight is not properly setup. Please make sure APPINSIGHTS_INSTRUMENTATIONKEY is defined');
    }
}

function ConvertGeoJsonToMultiPoint(feature){
    let multiPoint = ``;

    if(feature && Array.isArray(feature)){
        feature.forEach(location => {
           if(location.coordinates && Array.isArray(location.coordinates)){
               multiPoint += (multiPoint !== '' ? ',' : '') + `${location.coordinates.join(" ")}`;
           }
        });
    }

    if(multiPoint !== ''){
        return `ST_GeomFromText('MULTIPOINT(${multiPoint})', 4326)`;
    }else{
        throw new Error(`Invalid mapping feature[${JSON.stringify(feature)}]`);
    }
}

function TrimSentence(keywords, sentence, maxLength){
    let trimmedSentence = sentence;

    if(keywords && sentence && keywords.length > 0 && sentence.length > maxLength){
        let keyword = keywords[0];
        let maxLeftCharacters = 20;
        let RegEx = new RegExp(keyword, "i");
        let keywordFirstOccurence = sentence.search(RegEx);
        if(keywordFirstOccurence < maxLeftCharacters){
            return sentence.substring(0, sentence.length > maxLength ? maxLength : sentence.length);
        }

        let leftIndex = 0;
        for(let i = keywordFirstOccurence - 1; i > 0 && (keywordFirstOccurence - i) < maxLeftCharacters; i-- ){
            if(sentence.charAt(i) === ' '){
                leftIndex = i;
            }
        }

        return sentence.substring(leftIndex + 1, leftIndex + maxLength < sentence.length ? leftIndex + maxLength : sentence.length);
    }else{
        return trimmedSentence;
    }
}

function pushMessage(messageStr, callback) {
    let messageJSON, errMsg;
    
    try{
         messageJSON = JSON.parse(messageStr);
    }catch(err){
         errMsg = `error[${err}] occurred parsing message entry [${messageStr}]`;
         RaiseException(errMsg);
         failures.add(messageStr);
         return callback();
    }

    let errorOccured;
    let messageId = messageJSON.MessageId || "N/A";
    let keywords = messageJSON.Keywords;
    let sentence = TrimSentence(keywords, messageJSON.Sentence, MAX_CHARACTER_LIMIT);
    //Ignore tweeted messages
    let isRewteet = (messageJSON.RetweetedMessageId && messageJSON.RetweetedMessageId.length > 0) ? true : false;
    let locations = messageJSON.Locations;
    let originalSources = messageJSON.OriginalSources && Array.isArray(messageJSON.OriginalSources) && messageJSON.OriginalSources.length > 0 ? messageJSON.OriginalSources : [messageJSON.PartitionKey];
    let title = messageJSON.Title || "";
    let link = messageJSON.Link || "";

    requiredFields.forEach(field => {
        if(!messageJSON[field]){
            errMsg = `message ${messageId} is missing field [${field}]`;
            RaiseException(errMsg);

            errorOccured = true;
        }
    });

    if(errorOccured){
        return callback();
    }else if(keywords && keywords.length > 0 && locations && locations.length > 0 && !isRewteet){
      let upsertQuery;
      
      try{
            upsertQuery = `INSERT INTO tilemessages (
                messageid, source, keywords, createdtime, geog, neg_sentiment, ${messageJSON.Language.toLowerCase()}_sentence, orig_language,
                full_text, link, title, original_sources
            ) VALUES (
                '${messageId}',
                '${messageJSON.PartitionKey}',
                '{"${keywords.join('","')}"}',
                '${messageJSON.EventProcessedUtcTime}',
                ${ConvertGeoJsonToMultiPoint(locations)},
                ${messageJSON.Sentiment},
                '${sentence.replace(/\'/g, "\''")}',
                '${messageJSON.Language}',
                '${messageJSON.Sentence.replace(/\'/g, "\''")}',
                '${link.replace(/\'/g, "\''")}',
                '${title.replace(/\'/g, "\''")}',
                '{"${originalSources.join('","')}"}'
            ) ON CONFLICT (messageid, source) DO UPDATE SET
                keywords = '{"${keywords.join('","')}"}',
                original_sources = '{"${originalSources.join('","')}"}',
                link = '${link.replace(/\'/g, "\''")}',
                title = '${title.replace(/\'/g, "\''")}',
                full_text = '${messageJSON.Sentence.replace(/\'/g, "\''")}',
                geog = ${ConvertGeoJsonToMultiPoint(locations)},
                createdtime = '${messageJSON.EventProcessedUtcTime}',
                neg_sentiment = ${messageJSON.Sentiment},
                ${messageJSON.Language.toLowerCase()}_sentence = '${sentence.replace(/\'/g, "\''")}'
            ;`;
        
            let successful = false;
            let attempts = 0;
            if(processedFeatures.size > 0 && processedFeatures.size % 200 === 0){
                globalContext.log(`Processed ${processedFeatures.size} messages`);
            }
        
            async.whilst(
                () => {
                    return !successful && attempts < MAX_RETRIES;
                },
                createBlobCallback => {
                    postgresClient.query(upsertQuery, (err, results) => {
                        attempts++;

                        if (!err){
                            successful = true;
                            processedFeatures.add(messageId);
                        }else{
                            globalContext.log(upsertQuery);
                            let errMsg = `error[${err}] occurred writing tile Entry to postgres`
                            RaiseException(errMsg);
                        }

                        return createBlobCallback();
                    });
                },
                callback
            );
        }catch(error){
            errMsg = `message failed postgres processing, ${upsertQuery}`;
            RaiseException(errMsg);
            callback();
        }
    }else{
        callback();
    }
}

function pushResultSets(messages, callback) {
    async.eachLimit(messages, PUSH_PARALLELISM, pushMessage, callback);
}

module.exports = function(context, resultSet) {
    if (!process.env.FORTIS_SITE_NAME) {
        return context.done('Required environmental variable FORTIS_SITE_NAME not set: exiting.');
    }

    globalContext = context;

    let start = new Date();

    try{
        let messageEntries = resultSet.split("\n");
        FetchSiteDefinition(FORTIS_SITE_NAME, (error, siteDefinition) => {
            const storageConnectionString = siteDefinition.properties.featuresConnectionString;
            if(!error && siteDefinition && messageEntries && storageConnectionString){
                postgresClient = new pg.Client(storageConnectionString);
                postgresClient.connect(err => {
                    context.log(`processing ${messageEntries.length} result set features`);
                    pushResultSets(messageEntries, err => {
                        let deltaMs = new Date().getTime() - start.getTime();
                        context.log(`total execution time: [${deltaMs}] Processed messages: [${processedFeatures.size}] failures: [${failures.size}]`);
                        
                        if(failures.size > 1){
                            let processedError = `Check logs for error details.`;
                            RaiseException(processedError, true);
                        }else{
                            context.done();
                        }
                    });
                });
            }else{
                const errorMsg = `Undefined site definition error for site ${FORTIS_SITE_NAME}`;
                context.done(errorMsg); 
            }
        });
    }catch(error){
        RaiseException(error, true);
    }
};
