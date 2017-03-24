"use strict";

let Extractor = require('./AcledExtractor');
let moment = require('moment');

module.exports = function (context, runInterval) {
    let sinceDate = process.env.ACLED_EXTRACTOR_FROM_DATE || context.fromDate;
    if(runInterval.last){
         sinceDate = moment(runInterval.last).format("YYYY-MM-DD");
    }else if(!sinceDate){
        context.done("Unable to determine valid from date for acled run");
    }
    
    context.log(`Node.js Acled Extractor function started sinceDate: [${sinceDate}]`);

    Extractor.ProcessRecentAcledActivity(sinceDate, context.log,
        (messageCount, errorMessageSet) => {
            context.log(`Processed ${messageCount} items to the Acled table. Error count [${errorMessageSet.size}]`);
            
            if(errorMessageSet.size > 0){
               context.done(`${errorMessageSet.size} events failed processing.`);
            }else{
               context.done(); 
            }
        }
    );
};
