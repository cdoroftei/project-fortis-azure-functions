"use strict";

let azureStorage = require('azure-storage');
let appInsights = require("applicationinsights");
let appInsightsClient = appInsights.getClient();
let globalContext;

const PRE_NLP_TABLE = process.env.PRE_NLP_TABLE;

function RaiseException(errorMsg, exit){
    globalContext.log('PreNlpProcessor - error occurred: ' + errorMsg);

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

module.exports = function (context, inputMessage) {
    globalContext = context;

    try{
        if(!PRE_NLP_TABLE){
            throw new Error("PRE_NLP_TABLE env setting undefined error.");
        }

        let guid = inputMessage.message.id;
        let source = inputMessage.source;
        
        if(guid && source){
            let createdAt = new Date(inputMessage.created_at? inputMessage.created_at : Date.now());
            let pk = `${source.toLowerCase()}-${createdAt.getMonth()+1}${createdAt.getFullYear()}`;            
            let tableEntry = {
                    PartitionKey: pk,
                    RowKey: guid.toString(),
                    Message : JSON.stringify(inputMessage),
                    Source : source,
                    Lang : inputMessage.message.lang};
                    
            if(inputMessage.message.geo){
                tableEntry.Geo = JSON.stringify(inputMessage.message.geo);
            }

            let tableService = azureStorage.createTableService();

            tableService.createTableIfNotExists(PRE_NLP_TABLE, (error, result, response) => {
                if(!error){
                    tableService.insertEntity(PRE_NLP_TABLE, tableEntry, (error, result, response) => {
                        if(!error){
                            context.bindings.nlpInputQueueItem = inputMessage;
                        }

                        context.done();
                    });
                }else{
                    throw new Error('Error occured while trying to create azure table.');
                }
            });
        } else{  
            throw new Error(`InputMessage does not contain either a valid source or id: [${JSON.stringify(inputMessage)}]`); 
        } 
    } catch(error) {
        RaiseException(error, true);
    }
}