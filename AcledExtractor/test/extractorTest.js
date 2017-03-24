"use strict"

const assert = require('assert'),
      sinon = require('sinon'), 
      proxyquire = require('proxyquire').noCallThru(),
      fs = require('fs');

describe('Acled Extractor service test', () => {
    var queueServiceStub = {  
        createMessage: sinon.stub().callsArgWith(2, null, null),
        createQueueIfNotExists: sinon.stub().callsArgWith(1, null)
    };

    var azureStub = { 
        createQueueService: sinon.stub().returns(queueServiceStub) 
    };

    var applicationinsightsStub = {  
        getClient: sinon.stub().returns({
            trackException:sinon.stub().returns("ok"),
            trackEvent:sinon.stub().returns("ok")
        }) 
    };

    var pusherFunction = proxyquire('../index.js', {  
        'azure-storage': azureStub,
        'applicationinsights':applicationinsightsStub
    });

    it('can extract a json list of events from Acled', done => {

        fs.readFile('./test/fixtures/sample.json', (err, acledStoriesMock) => {
            if (err) return console.log(err);

            let contextMock = {
                fromDate: "2016-12-1",
                log: msg => console.log(msg),
                done: (err, storyCount) => {
                    console.log(err);
                    assert(!err);
                   // assert(queueCount > 0);
                    done();
                },
                bindings: {fbOutputQueue: []}
            };

            let jobRun = {last: "2016-10-12"}
            
            pusherFunction(contextMock, jobRun);
        });
    });
});