"use strict";

let moment = require('moment');
let Extractor = require('./FbExtractor');
var appInsightsClient = require("applicationinsights").getClient();

module.exports = function (context, jobrunMetadata) {
    let sinceDate = jobrunMetadata.last || false;
    
    if(process.env.FB_EXTRACTOR_FROM_DATE){
           sinceDate = process.env.FB_EXTRACTOR_FROM_DATE;
    }else if(!sinceDate){
           sinceDate = moment().subtract(2, "hours").format("YYYY-MM-DDTHH:mm:ss");
    }
    
    let untilDate = jobrunMetadata.overridenToDate || moment().format("YYYY-MM-DDTHH:mm:ss");
    let disableQueueWrites = jobrunMetadata.disableQueueWrites || false;
    
    appInsightsClient.trackEvent("extractor running");
    let outputQueue = [];
    context.log(`Node.js Facebook Extractor function started sinceDate: [${sinceDate}] untilDate: [${untilDate}]`);
    Extractor.ProcessRecentFBActivity(context.bindings.fbPageTable, sinceDate, untilDate, appInsightsClient, context.log, disableQueueWrites, 
                        (messageCount, errorMessageSet) => {
                            context.log(`Processed ${messageCount} messages to the fbOutputQueue queue. Error count [${errorMessageSet.size}]`);
                            if(jobrunMetadata.overridenToDate){
                                let errorMsg = errorMessageSet.size > 0 ? `${errorMessageSet.size} error(s) occured` : undefined;
                                context.log(`Error: ${errorMsg}`);
                                context.done(errorMsg, messageCount);
                            }else{
                                context.done();
                            }
                        }
    );
};
