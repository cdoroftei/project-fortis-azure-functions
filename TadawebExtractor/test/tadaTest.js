"use strict"

const assert = require('assert'),
    sinon = require('sinon'),
    proxyquire = require('proxyquire').noCallThru(),
    tadaFunction = require('../index');

describe('Tadaweb Event Hub service', () => {
    it('should push a message to Pre NLP Input Azure Queue ', done => {
        let testMessage = {"language":"id","link":"http://www.republika.co.id/berita/gaya-hidup/info-sehat/16/11/15/ogo0bu384-uji-coba-sistem-lavitrap-sukses-di-riau","published_at":"2016-11-15","sources":["http://www.republika.co.id/","http://nasional.republika.co.id/"],"tags":["Dengue","Indonesia"],"text":"REPUBLIKA.CO.ID, Anggota Dewan Pakar Ikatan Dokter Indonesia (IDI) Riau, Ririe Fachriani Malisie, mengatakan alat lavitrap telah diuji coba di Kabupaten Pelalawan, Riau. Riau menjadi daerah percontohan pertama, sebab berdasarkan data Dinas Kesehatan Provinsi Riau, wilayah ini memasuki siklus lima tahunan perkembangan nyamuk.","title":"Uji Coba Sistem Lavitrap Sukses di Riau ","EventProcessedUtcTime":"2016-11-15T06:10:56.7001479Z","PartitionId":0,"EventEnqueuedUtcTime":"2016-11-15T06:10:56.6550000Z"};

        let context = {
            log: msg => console.log(msg),
            done: err => {
                if(err) {
                    console.log(err);
                }
                assert(!err);
                assert.equal(testMessage.text, context.bindings.preNlpQueueItem.message.message);
                assert.equal(testMessage.link, context.bindings.preNlpQueueItem.message.link);
                done();
            },
            bindings:{
                preNlpQueueItem: {}
            }
        };

        tadaFunction(context, testMessage);
    });
    it('should push a message to Pre NLP Input Azure Queue with shared location ', done => {
        let testMessage = {"cities":[{"city":"Benghazi","coordinates":[32.109,20.0756]},{"city":"ganfouda","coordinates":[35.0401,9.49363]}],"language":"en","link":"https://vivalibya.wordpress.com/2016/12/11/%d8%a7%d8%b9%d9%8a%d8%a7%d9%86-%d9%85%d9%86-%d8%b9%d8%af%d8%a9-%d9%82%d8%a8%d8%a7%d8%a6%d9%84-%d9%8a%d8%a8%d8%a7%d8%b1%d9%83%d9%88%d9%86-%d8%ae%d8%b1%d9%88%d8%ac-%d8%a7%d9%84%d9%85%d8%b9%d8%aa%d9%82/","published_at":"2016-12-11","sources":["http://www.alchemyapi.com/","http://www.bing.com/","http://www.tadaweb.com/","https://vivalibya.wordpress.com/"],"tags":["OCHA"],"text":"Elders from several tribes are discovering out detainees in the Social Council reception unpack God rest families who wrongfully arrested","title":"Elders from several tribes are discovering out detainees in them the Social Council"};

        let context = {
            log: msg => console.log(msg),
            done: err => {
                if(err) {
                    console.log(err);
                }
                assert(!err);
                assert.equal(testMessage.link, context.bindings.preNlpQueueItem.message.link);
                assert.ok(context.bindings.preNlpQueueItem.message.geo.features.length === 1);
                done();
            },
            bindings:{
                preNlpQueueItem: {}
            }
        };

        tadaFunction(context, testMessage);
    });
});