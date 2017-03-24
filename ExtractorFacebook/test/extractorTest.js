"use strict"

const assert = require('assert'),
      fs = require('fs'),
      pusherFunction = require('../index');

describe('Facebook Extractor service test', () => {
    it('can extract a json list of messages from FB', done => {

        fs.readFile('./test/fixtures/sample.json', (err, fbPagesMock) => {
            if (err) return console.log(err);

            let contextMock = {
                log: msg => console.log(msg),
                done: (err, queueCount) => {
                    assert(!err);
                    done();
                },
                bindings: {
                           fbOutputQueue: [], 
                           fbPageTable: JSON.parse(fbPagesMock)
                          }
            };

            let jobRun = {last: "2016-09-30T21:09:00", overridenToDate: "2016-10-13T16:12:01-04:00", disableQueueWrites: true}
            
            pusherFunction(contextMock, jobRun);
        });
    });
});