let LocationInferenceService = require('./LocationInference');

module.exports = function (context, input) {
    try {
        const sentence = input.message.message;
        const title = input.message.title || "";
        const link = input.message.link || "";
        const userId = input.message.user_id || "";
        const lang = input.message.lang;
        const messageId = input.message.id;
        const originalSources = input.message.originalSources || [input.source];
        const userProvidedLocation = input.message.geo;
        const retweetedId = input.message.retweet_id || "";
        const retweetCount = input.message.retweet_count || 0;
        
        if(!sentence){
            const errMsg = `undefined message error occured [${input}]`;
            context.log(errMsg);
            context.done();
        }
        
        LocationInferenceService.findLocation(userId, lang, sentence, userProvidedLocation, 
                                              context.log, (locations, lang, error) => {
                                                   if(locations && locations.features.length > 0 && !error){
                                                        deliverMessageToEventHub(messageId, sentence, locations, lang, 
                                                                                 input.source, input.created_at, retweetedId, 
                                                                                 retweetCount, originalSources, title, link, context);
                                                   }else if(error){
                                                       const errMsg = `An error occured [${error}].`;
                                                       context.done(errMsg);
                                                   }else{
                                                       const errMsg = `Unable to find a targeted location.`;
                                                       context.log(errMsg);
                                                       context.done();
                                                   }
                                              });
    } catch (err) {
        context.log(err);
        context.done(err);
    }
}

function deliverMessageToEventHub(messageId, message, featureCollection, lang, source, 
                                  creationDate, retweetedId, retweetCount, originalSources, 
                                  title, link, context) {
    try{
        context.bindings.outputEventHubMessage = {
            "Language": lang,
            "Sentence": message,
            "Title": title,
            "Link": link,
            "MessageId": messageId.toString(),
            "Created": creationDate,
            "PartitionKey": source,
            "RetweetedMessageId": retweetCount,
            "RetweetCount": retweetCount,
            "Source": source,
            "OriginalSources": originalSources, 
            "Locations": featureCollection && featureCollection.features ? featureCollection.features : []
        };
    }catch(error){
        context.log("Error occured trying to write message to EH.");
        context.done(error);
        
        return;
    }

    context.done();
}