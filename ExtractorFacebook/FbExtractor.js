"use strict"

let graph = require('fbgraph');
let asyncEachLimit = require('async/eachLimit');
let moment = require('moment');
let Services = require('./services');
let azure = require('azure-storage');
let NodeCache = require( "node-cache" );
let TextBase64QueueMessageEncoder = require('azure-storage').QueueMessageEncoder.TextBase64QueueMessageEncoder;

const AZURE_STORAGE_RETRY_COUNT = 3;
const AZURE_STORAGE_RETRY_INTERVAL = 1000;
const retryOperations = new azure.LinearRetryPolicyFilter(AZURE_STORAGE_RETRY_COUNT, AZURE_STORAGE_RETRY_INTERVAL);
const FORTIS_DATA_STORE_TTL = 3600;
const fortisSiteCache = new NodeCache( { stdTTL: FORTIS_DATA_STORE_TTL} );
const FORTIS_SITE_NAME = process.env.FORTIS_SITE_NAME;
const ASYNC_PAGE_LIMIT = 20;
const ASYNC_POST_LIMIT = 20;
const ASYNC_POST_COMMENTS_LIMIT = 20;
const FB_PAGE_LIMIT = 30;
const FB_POST_COMMENT_LIMIT = 40;
const ERROR_CODES = [2500, 803, 100];
const ATTIC_WEEKS_SINCE_NOW = 1;
const MESSAGE_QUEUE = process.env.PRE_NLP_QUEUE;

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

let queueSvc = azure.createQueueService().withFilter(retryOperations);
queueSvc.messageEncoder = new TextBase64QueueMessageEncoder();
queueSvc.createQueueIfNotExists(MESSAGE_QUEUE, (error, result, response) => {
    if (error) {
        appInsightsClient.trackException(new Error(`Unable to create new azure queue ${MESSAGE_QUEUE}`));
    }
});

var appInsightsClient = {};
var messageCount = 0;
var disableQueueOperations = false;
var logger = {};
let errorMessageSet = new Set();

function PreValidate(facebookPages, accessToken, sinceDate, untilDate, messageQueue){
    let fbPagesCheck = facebookPages && Array.isArray(facebookPages) && facebookPages.length > 0;
    let dateCheck = sinceDate && untilDate && moment(sinceDate).isBefore(moment(untilDate));

    if(!fbPagesCheck){
        RaiseException(`facebookPages undefined error.`);

        return false;
    }

    if(!FORTIS_SITE_NAME){
        RaiseException(`Fortis site name is undefined error.`);

        return false;
    }

    if(!messageQueue){
        RaiseException(`PRE_NLP_QUEUE env setting undefined error.`);

        return false;
    }

    if(!accessToken){
        RaiseException(`accessToken undefined error.`);

        return false;
    }

    if(!dateCheck){
        RaiseException(`invalid from and/or to date.`);

        return false;
    }

    return true;
}

// Retrieve all content posted to a page, including those posts shared
// by those who aren't page owner (which is why the '/feed' branch
// is used instead of '/posts')
//
// Note: if no date range is specified, this just pulls the latest batch of stories on a page. Otherwise,
//   set `since` and `until` (both epoch time) for date range. For now, hardcoded to go March 1 - June 30
function retrieveRecentPagePosts(pageName, fromDate, toDate, callback) {
  let sinceDate = moment(toDate).subtract(ATTIC_WEEKS_SINCE_NOW, 'week').format("YYYY-MM-DDTHH:mm:ss");
  let queryString = `${pageName}/feed?fields=message,link,from,name,created_time,place,caption&since=${sinceDate}&until=${toDate}`;

  logger(`Processing ${pageName} request [${queryString}]`);

    let graphResponseHandler = (error, response) => {
        try {
            let continuePaging = response.paging && response.paging.next && response.data.length === FB_PAGE_LIMIT ? true : false;
            pagePostsResponseHandler(error, response, pageName, fromDate, callback, !continuePaging);

            if(continuePaging){
                try{
                    graph.get(response.paging.next, graphResponseHandler);
                }catch(err){
                    logger(`Error [${err}] occured for page [${pageName}]`);
                }
            }
            
        } catch (error) {
            logger(`Error [${error}] Occured`);
        }
    };
    
  graph.get(queryString, {limit: FB_PAGE_LIMIT}, graphResponseHandler);
}

function pushMessageToStorageQueue(message, callback){
    try {
        if(!disableQueueOperations){
            queueSvc.createMessage(MESSAGE_QUEUE, JSON.stringify(message), (error, result, response) => {
                if (error) {
                    RaiseException(`Azure Queue push error occured error [${error}]`);
                }else{
                    if(++messageCount % 300 === 0){
                        logger(`Wrote ${messageCount} messages to output queue.`);
                    }
                }
                
                return callback();
            });
        }else{
            ++messageCount;

            return callback();
        }
    } catch (error) {
        RaiseException(`Issue with pushing message ${JSON.stringify(message)} to out queue.`);
        return callback();
    }
}

function queueFacebookPost(postMessage, callback, pageName) {
    let message = {
        'source': 'facebook-messages',
        'created_at': moment().toISOString(),
        'message': postMessage
    };

    if(postMessage.place && postMessage.place.location){
        message.message.geo = LatLongToGeoJSON(postMessage.place.location.latitude, postMessage.place.location.longitude);
        delete message.message.place;
    }

    if(postMessage.caption){
        message.message.originalSources = [pageName];
    }

    if(postMessage.name){
        message.message.title = postMessage.name;
    }

    pushMessageToStorageQueue(message, callback);
}

function LatLongToGeoJSON(lat, lon){
    return JSON.parse(`{"type":"Point","coordinates":[${lon}, ${lat}]}`);
}

function RaiseException(errorMsg){
    logger('error occured: ' + errorMsg);
    errorMessageSet.add(errorMsg);

    if(appInsightsClient.config){
        appInsightsClient.trackException(new Error(errorMsg));
    }else{
        logger('App Insight is not properly setup. Please make sure APPINSIGHTS_INSTRUMENTATIONKEY is defined');
    }
}

function postCommentsResponseHandler(comment, callback, postMessageId, location, pageName){
    let message = {
            'source': 'facebook-comments',
            'created_at': moment().toISOString(),
            'message-id': postMessageId,
            'message': comment
    };

    if(pageName){
        message.message.originalSources = [pageName];
    }

    if(location && location.location){
        message.message.geo = location.location;
    }
    
    pushMessageToStorageQueue(message, callback);
}

function postCommentGraphResponseHandler(error, commentsResponse, postMessage, callback, fromDate, lastPage, pageName){
    if (error) {
        if(error.exception.code == 'ENOTFOUND') {
          RaiseException(`DNS error getting post comments for [${postMessage.id}]`);
        } else {
          RaiseException(`Unexpected Error retrieving comments: ${JSON.stringify(err)}`);
        }

        cb();
    }

    if (!error && commentsResponse && commentsResponse.comments && commentsResponse.comments.data) {
      asyncEachLimit(commentsResponse.comments.data, ASYNC_POST_COMMENTS_LIMIT, (comment, cb) => {
          if(!AlreadyQueuedMessage(comment.created_time, fromDate)){
             postCommentsResponseHandler(comment, cb, postMessage.id, postMessage.place, pageName);
          }else{
              cb();
          }
        }, commentsError => {
            if(lastPage){
                PagePostCallbackHandler(callback, postMessage, fromDate, pageName);
            }
        }
      );
    }else if(lastPage){
        PagePostCallbackHandler(callback, postMessage, fromDate, pageName);
    }
}

function PagePostCallbackHandler(callback, postMessage, fromDate, pageName){
     if(!AlreadyQueuedMessage(postMessage.created_time, fromDate)){
            queueFacebookPost(postMessage, callback, pageName);
     }else{
         callback();
     }
}

function retrievePostComments(postMessage, callback, fromDate, pageName) {
  let postCommentsGraphQuery = `${postMessage.id}/?fields=comments`;
  
  let graphResponseHandler = (error, response) => {
       if (error) {
          if(ERROR_CODES.indexOf(error.code) > -1) {
            RaiseException(`ERROR_CODES error[${JSON.stringify(error)}]`);
          } else {
            RaiseException(`unexpected error : ${JSON.stringify(error)}`);
          }

          callback();
        }else{
            let continuePaging = response.paging && response.paging.next && response.comments.data.length === FB_POST_COMMENT_LIMIT;
            postCommentGraphResponseHandler(error, response, postMessage, callback, fromDate, !continuePaging, pageName);

            if(continuePaging){
                graph.get(response.paging.next, graphResponseHandler);
            }
        }
  };
    
  graph.get(postCommentsGraphQuery, {limit: FB_POST_COMMENT_LIMIT}, graphResponseHandler);
}

function AlreadyQueuedMessage(postDate, fromDate){
    return moment(postDate).isBefore(moment(fromDate));
}

function pagePostsResponseHandler(error, pagePostsResponse, pageName, fromDate, callback, lastPage){
      if (error) {
          if(ERROR_CODES.indexOf(error.code) > -1) {
            RaiseException(`Page ${pageName} isn't available error[${JSON.stringify(error)}]`);
          } else {
            RaiseException(`unexpected error for page ${pageName} error: ${JSON.stringify(error)}`);
          }

          return callback();
      }

      if (!error && pagePostsResponse && pagePostsResponse.data && Array.isArray(pagePostsResponse.data)) {
        // this map results in an array of page post id's 
        asyncEachLimit(pagePostsResponse.data, ASYNC_POST_LIMIT, (postMessage, cb) => {
            retrievePostComments(postMessage, cb, fromDate, pageName);
        }, pagePostsError => {
            if(lastPage){
                logger(`All posts / comments have been processed in the queue for page [${pageName}]`);
                
                return callback();
            }
        });
      }else if(lastPage){
          return callback();
      }
}

module.exports = {
    ProcessRecentFBActivity: function(fbPageEnties, fromDate, toDate, applicationInsightClient, loggerInstance, disableQueueWrites, callback){
        appInsightsClient = applicationInsightClient;
        logger = loggerInstance;
        disableQueueOperations = disableQueueWrites;

        FetchSiteDefinition(FORTIS_SITE_NAME, (error, siteDefinition) => {
            if(PreValidate(fbPageEnties, siteDefinition.properties.fbToken, fromDate, toDate, MESSAGE_QUEUE)){
                if(!error){
                    logger(`Processing ${fbPageEnties.length} Facebook pages from ${fromDate} to ${toDate}`);
                    graph.setAccessToken(siteDefinition.properties.fbToken);
                    
                    asyncEachLimit(fbPageEnties, ASYNC_PAGE_LIMIT, (page, cb) => {
                        if(page.pageUrl && page.pageUrl.replace(/\s/g,"") !== ""){
                            retrieveRecentPagePosts(page.pageUrl, fromDate, toDate, cb);
                        }else{
                            let errorMsg = `Page reference ${page.RowKey} threw an undefined url error`;
                            RaiseException(errorMsg);

                            return cb();
                        }
                    }, errorMsg =>{
                        callback(messageCount, errorMessageSet);
                    });
                }else{
                    const errorMsg = `Undefined site definition error for site ${FORTIS_SITE_NAME}`;
                    callback(undefined, undefined, errorMsg);
                }
            }else{
                callback(messageCount, errorMessageSet);
            }
        });
    }
};
