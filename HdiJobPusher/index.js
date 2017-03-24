const request = require('request')

const ParseStorageAccount = (connectionString, key) => {
          let matchedPosition = connectionString.indexOf(key);

          if(matchedPosition > -1){
              matchedPosition++;
              let endPosition = connectionString.indexOf(";", matchedPosition);
              endPosition = endPosition === -1 ? connectionString.length: endPosition;
              
              return connectionString.substring(matchedPosition + key.length, endPosition); 
          }else{
              return undefined;
          }
 };

function createByTilePayload() {
  let date = new Date()
  date.setDate(date.getDate() - 1)
  let dateString = date.toISOString().substring(0, 10)
  let filePattern = `/${dateString}/*.json`
  let clusterStorageName = process.env.CLUSTER_STORAGE_ACCOUNT_NAME
  let clusterContainerName = process.env.CLUSTER_NAME
  const storageAccount = process.env.AZURE_STORAGE_CONNECTION_STRING

  return {
    "file" : `wasb://${clusterContainerName}@${clusterStorageName}.blob.core.windows.net/fortis/bytileAggregator_OneFile.py`,
    "pyFiles": `wasb://${clusterContainerName}@${clusterStorageName}.blob.core.windows.net/fortis/artifacts.zip`, 
    "conf": { 
      "spark.executor.cores" : "7",
      "spark.executors" : "4",
      "spark.submit.deployMode" : "client",
      "spark.executor.memory" : "8g"
    },
    "args": [
      ParseStorageAccount(storageAccount, "AccountName"),
      ParseStorageAccount(storageAccount, "AccountKey"),
      process.env.MESSAGES_CONTAINER,
      filePattern,
      process.env.TILES_PREV_CONTAINER,
      process.env.TILES_CONTAINER,
      process.env.PROCESSED_MESSAGES_CONTAINER,
      "/*/part*",
      process.env.TIMESERIES_CONTAINER,
      "searchkeywords",
      "keywordFilters",
      "models",
      "SWN3_ar_ur.json"
  ] }
}

function pushHdiJobs(context) {
  const appName = process.env.APP_NAME
  const endpoint = `http://${appName}.azurewebsites.net/api/jobs/push`

  const byTileBody = `${JSON.stringify(createByTilePayload())}`
  context.log(`Pushing HDI job with payload: ${byTileBody}`)
  request.post({url: endpoint, form: {data: byTileBody}}, function (error, response, body) {
    if (error) {
      context.log(`Received HTTP request error: '${error}'`)
    } else if (response.statusCode != 200) {
      context.log(`Invalid HTTP response code '${response.statusCode}'`)
    } else {
      context.done()
    }
  })
}

module.exports = function (context, runInterval) {
  pushHdiJobs(context);
}