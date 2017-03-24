"use strict"

var assert = require('assert');
var sinon = require('sinon');  
var proxyquire = require('proxyquire').noCallThru(); 
var azure = require('azure-storage');  
var applicationinsights = require("applicationinsights");

describe('PreNlpProcessor service', () => {
    let fbMsgQueueMessage ={
    "source": "facebook-messages",
    "created_at": "2016-10-11T22:01:59.376Z",
    "message": {
        "id": "172887752765768_1032148073506394",
        "from": {
            "name": "المرصد الليبى لحقوق الإنسان _ libyan observatory humanrights",
            "category": "Non-Governmental Organization (NGO)",
            "id": "172887752765768"
        },
        "message": "اللهم آخي بين المسلمين واهل البلد الواحد واجمع شملهم وشتت عدوهم \nصلاة عيد مشتركة بين الأخوة #الطوارق و #التبو",
        "picture": "https://scontent.xx.fbcdn.net/v/t1.0-0/s130x130/13612263_1032148073506394_9089223564819192732_n.jpg?oh=1481dda04b6a804c1f15b196e2d14b16&oe=589E29C0",
        "link": "https://www.facebook.com/mrsdlibya/photos/a.184921321562411.67720.172887752765768/1032148073506394/?type=3",
        "name": "Timeline Photos",
        "icon": "https://www.facebook.com/images/icons/photo.gif",
        "privacy": {
            "value": "",
            "description": "",
            "friends": "",
            "allow": "",
            "deny": ""
        },
        "type": "photo",
        "status_type": "added_photos",
        "object_id": "1032148073506394",
        "created_time": "2016-07-07T11:07:54+0000",
        "updated_time": "2016-07-07T11:07:54+0000",
        "is_hidden": false,
        "is_expired": false,
        "likes": {
            "data": [
                {
                    "id": "1083191055105835",
                    "name": "Om Deia"
                }
            ],
            "paging": {
                "cursors": {
                    "before": "MTA4MzE5MTA1NTEwNTgzNQZDZD",
                    "after": "MTAyMDc3NDM4MzQ2NDkwMTAZD"
                }
            }
        }
    }
    };

    var tableServiceStub = {  
        insertEntity: sinon.stub().callsArgWith(2, null, null),
        createTableIfNotExists: sinon.stub().callsArgWith(1, null)
    };

    var azureStub = {  
        createTableService: sinon.stub().returns(tableServiceStub) };

    var applicationinsightsStub = {  
        getClient: sinon.stub().returns({
            trackException:sinon.stub().returns("ok"),
            trackEvent:sinon.stub().returns("ok")
        }) 
    };

    var preNlpProcessor = proxyquire('../index.js', {  
        'azure-storage': azureStub,
        'applicationinsights':applicationinsightsStub
    }); 


    it('should push a message to NLP Input Azure Queue ', done => {
        let context = {
            log: msg => console.log(msg),
            done: err => {
                if(err) {
                    console.log(err);
                }
                assert(!err);
                assert.equal(JSON.stringify(fbMsgQueueMessage), JSON.stringify(context.bindings.nlpInputQueueItem));
                done();
            },
            bindings:{}
        };

        preNlpProcessor(context, fbMsgQueueMessage);
    });

});