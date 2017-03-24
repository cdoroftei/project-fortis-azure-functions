"use strict"

let request = require('request');

module.exports = {
    getSiteDefintion(siteId, callback){
        const fragment = `fragment FortisSiteDefinitionView on SiteCollection {
                            sites {
                                properties {
                                    featuresConnectionString
                                    supportedLanguages
                                }
                            }
                        }`;

        const query = `  ${fragment}
                        query Sites($siteId: String) {
                            siteDefinition: sites(siteId: $siteId) {
                                ...FortisSiteDefinitionView
                            }
                        }`;

        const variables = {siteId};
        const host = process.env.REACT_APP_SERVICE_HOST
        const POST = {
            url : `${host}/api/settings`,
            method : "POST",
            json: true,
            withCredentials: false,
            body: { query, variables }
        };
        
        request(POST, callback);
  }
}