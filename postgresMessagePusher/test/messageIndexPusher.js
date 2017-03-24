"use strict"

let assert = require('assert');
let sinon = require('sinon');  
let proxyquire = require('proxyquire').noCallThru(); 
let azure = require('azure-storage');
let fs = require('fs');

describe('indexPusher service', () => {
    it('can push a resultsets blob', done => {
        fs.readFile('./test/fixtures/part-sample.txt', 'utf8', (err, resultSetBlob) => {
            let context = {
                log: msg => console.log(msg),
                bindingData: {
                },
                done: err => {
                    console.log(err);
                    assert(!err);
                    done();
                }
            };

            var pgStub = {  
                Client: sinon.stub().returns({
                    'connect':sinon.stub().callsArgWith(0),
                    'query':sinon.stub().callsArgWith(1, null)
                })
            };

            process.env.POSTGRES_CONNECTION_STRING = 'connection'

            var pusherFunction = proxyquire('../index.js', {  
                'pg':pgStub
            });

            pusherFunction(context, resultSetBlob);
        });
    });
});