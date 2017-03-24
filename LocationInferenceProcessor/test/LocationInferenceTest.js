"use strict"

const assert = require('assert'),
    sinon = require('sinon'),
    proxyquire = require('proxyquire').noCallThru();

describe('Location Inference service tests', () => {
    process.env.PCT_GEOTWIT_AZURE_STORAGE_ACCOUNT = 'account';
    process.env.FEATURES_CONNECTION_STRING = "connection string"
    process.env.PCT_GEOTWIT_AZURE_STORAGE_KEY = "key"

    var localities = {
        rows:
        [
            {
                name: 'Zāwiyat at Tart',
                ar_name: 'Zāwiyat في تارت',
                alternatenames: 'Thintis,Zauiet Tert,Zawiyat at Tarit,Zawiyat at Tart,Zāwiyat at Tarit,Zāwiyat at Tart',
                feature: '{"type":"Point","coordinates":[22.06667,32.8]}'
            },
            {
                name: 'Tripoli',
                ar_name: 'طرابلس',
                alternatenames: 'Baladiyat Tarabulus,Baladīyat Ţarābulus,District de Tripoli,Sha`biyat Tarabulus,Sha‘bīyat Ţarābulus,Tarabulus,Tripoli,Wilayat Tarabulus,Wilāyat Ţarābulus,trabls,Ţarābulus,شعبية طرابلس,طرابلس',
                feature: '{"type":"Point","coordinates":[13.18472,32.88972]}'
            },
            {
                name: 'Qaryat Qārāt',
                ar_name: 'قرية Qārāt',
                alternatenames: '',
                feature: '{"type":"Point","coordinates":[13.07583,32.21194]}'
            }
        ]
    };

    var tableServiceStub = {
        retrieveEntity: sinon.stub().callsArgWith(3, null, null)
    };

    tableServiceStub.retrieveEntity.withArgs(sinon.match.any, '10', '100092769', sinon.match.any).callsArgWith(3, null, {
        PartitionKey: { _: '10' },
        RowKey: { _: '100092769' },
        location: { _: '{"lat":11.276784455403686,"lon":26.329739168286324,"confidence":0}' }
    });

    tableServiceStub.retrieveEntity.withArgs(sinon.match.any, '10', '100092788', sinon.match.any).callsArgWith(3, null, {
        PartitionKey: { _: '10' },
        RowKey: { _: '100092788' },
        location: { _: '{"lat":22.27678445,"lon":33.329739,"confidence":0.8}' }
    });

    var azureStub = {
        createTableService: sinon.stub().returns(tableServiceStub)
    };

    var postgresStub = sinon.stub().callsArgWith(2, null, localities);

    var applicationinsightsStub = {
        getClient: sinon.stub().returns({
            config: sinon.stub().returns("ok"),
            trackException: sinon.stub().returns("ok"),
            trackEvent: sinon.stub().returns("ok")
        })
    };

    var locationInference = proxyquire('../LocationInference.js', {
        'azure-storage': azureStub,
        'applicationinsights': applicationinsightsStub,
        'postgres-client': postgresStub
    });

    it('Verify we can pull locations for an untracked user id, and no provided language, for an english message', done => {
        let message = "This is a test message that talks about news in Zāwiyat at Tart";
        let expectedLocationFeature = '{"type":"Point","coordinates":[22.06667,32.8]}';

        let callback = (location, lang, error) => {
            try {
                assert(location && location.features && location.features.length == 1);
                assert(JSON.stringify(location.features[0]) == expectedLocationFeature);
            } catch (err) {
                done(err);
                return;
            }

            done();
        };

        locationInference.findLocation("no user", undefined, message, undefined, console.log, callback);
    });

    it('Verify we can pull locations for an untracked user id, and no provided language, for an arabic message', done => {
        let message = "هذه  رسالة  الذي يتحدث عن صحفي في  بنغازي طرابلس";
        let expectedLocationFeature = '{"type":"Point","coordinates":[13.18472,32.88972]}';

        let callback = (location, lang, error) => {
            try {
                assert.ok(location && location.features && location.features.length == 1);
                assert(JSON.stringify(location.features[0]) == expectedLocationFeature);
            } catch (err) {
                done(err);

                return;
            }

            done();
        };

        locationInference.findLocation("no user", undefined, message, undefined, console.log, callback);
    });

    it('Verify we can pull locations for a tracked user id but fails the confidence check, and no provided language, for an arabic message', done => {
        let message = "هذه  رسالة  الذي يتحدث عن صحفي في  بنغازي طرابلس";
        let expectedLocationFeature = '{"type":"Point","coordinates":[13.18472,32.88972]}';

        let callback = (location, lang, error) => {
            try {
                assert.ok(location && location.features && location.features.length == 1);
                assert(JSON.stringify(location.features[0]) == expectedLocationFeature);
            } catch (err) {
                done(err);

                return;
            }

            done();
        };

        locationInference.findLocation("100092769", undefined, message, undefined, console.log, callback);
    });

    it('Verify we can pull locations for a tracked user id but fails the confidence check, and no provided language, for an english message', done => {
        let message = "This is a test message that talks about news in Zāwiyat at Tart";
        let expectedLocationFeature = '{"type":"Point","coordinates":[22.06667,32.8]}';

        let callback = (location, lang, error) => {
            try {
                assert(location && location.features && location.features.length == 1);
                assert(JSON.stringify(location.features[0]) == expectedLocationFeature);
            } catch (err) {
                done(err);
                return;
            }

            done();
        };

        locationInference.findLocation("100092769", undefined, message, undefined, console.log, callback);
    });

    it('Verify we can pull locations for a tracked user that passes the confidence check, and no provided language, for an english message', done => {
        let message = "This is a test message that talks about news in Zāwiyat at Tart";
        let expectedLocationFeature = '{"type":"Point","coordinates":[33.329739,22.27678445],"properties":{"confidence":0.8,"geoTwitUser":"100092788"}}';

        let callback = (location, lang, error) => {
            try {
                assert(location && location.features && location.features.length == 1);
                assert(JSON.stringify(location.features[0]) == expectedLocationFeature);
            } catch (err) {
                done(err);
                return;
            }

            done();
        };

        locationInference.findLocation("100092788", undefined, message, undefined, console.log, callback);
    });

    it('Verify we can pull locations for a tracked user that passes the confidence check, and no provided language, for an arabic message', done => {
        let message = "هذه رسالة الاختبار الذي يتحدث عن صحفي في بنغازي وطرابلس";
        let expectedLocationFeature = '{"type":"Point","coordinates":[33.329739,22.27678445],"properties":{"confidence":0.8,"geoTwitUser":"100092788"}}';

        let callback = (location, lang, error) => {
            try {
                assert(location && location.features && location.features.length == 1);
                assert(JSON.stringify(location.features[0]) == expectedLocationFeature);
            } catch (err) {
                done(err);
                return;
            }

            done();
        };

        locationInference.findLocation("100092788", undefined, message, undefined, console.log, callback);
    });

    it('Verify that the inferred location matches a users shared location', done => {
        let message = "This is a test message that talks about things in Benghazi and tripoli";
        let location = [20.06859, 32.11486];

        let callback = (locations, lang, error) => {
            try {
                assert.ok(locations && locations.features && locations.features.length == 1);
                assert.ok(locations.features[0].properties.source == 'sharedLocation');
                assert.ok(locations.features[0].coordinates[0] == location[0] && locations.features[0].coordinates[1] == location[1]);
            } catch (err) {
                done(err);

                return;
            }

            done();
        };

        locationInference.findLocation("1001198238", undefined, message, location, console.log, callback);
    });

    it('Verify that the inferred location matches a users shared feature collection', done => {
        let message = "This is a test message that talks about things in Benghazi and tripoli";
        let feature1 = {"type":"Point","coordinates":[33.329739,22.27678445]};
        let feature2 = {"type":"Point","coordinates":[34.329739,21.27678445]};
        let location = {type: "FeatureCollection", features: [feature1, feature2]};

        let callback = (locations, lang, error) => {
            try {
                console.log(JSON.stringify(locations));
                assert.ok(locations.features.length === location.features.length);
                assert.ok(locations.features[0].properties.source == 'sharedLocation');
                assert.ok(locations.features[0].coordinates[0] == location.features[0].coordinates[0] && locations.features[0].coordinates[1] == location.features[0].coordinates[1]);
            } catch (err) {
                done(err);

                return;
            }

            done();
        };

        locationInference.findLocation("1001198238", undefined, message, location, console.log, callback);
    });

    it('Verify that the inferred location with coordinates matches a users shared location', done => {
        let message = "This is a test message that talks about things in Benghazi and tripoli";
        let location = {coordinates: [20.06859, 32.11486]};

        let callback = (locations, lang, error) => {
            try {
                assert.ok(locations && locations.features && locations.features.length == 1);
                assert.ok(locations.features[0].properties.source == 'sharedLocation');
                assert.ok(locations.features[0].coordinates[0] == location.coordinates[0] && locations.features[0].coordinates[1] == location.coordinates[1]);
            } catch (err) {
                done(err);

                return;
            }

            done();
        };

        locationInference.findLocation("1001198238", undefined, message, location, console.log, callback);
    });

    it('Verify that no location mentioned exits gracefully.', done => {
        let message = "This is a test message that talks about things.";

        let callback = (locations, lang, error) => {
            try {
                assert.ok(locations && locations.features && locations.features.length == 0);
            } catch (err) {
                done(err);

                return;
            }

            done();
        };

        locationInference.findLocation("no user", undefined, message, undefined,
            console.log, callback);
    });

    it('Verify that we will not attempt to process an unsupported language.', done => {
        let message = "This is a test message that talks about things.";

        let callback = (locations, lang, error) => {
            try {
                assert.ok(error.indexOf('unsupported language') > -1);
            } catch (err) {
                done(err);

                return;
            }

            done();
        };

        locationInference.findLocation("no user", 'zz', message, undefined, console.log, callback);
    });

    it('Verify detection of localities in english text when locality is defined in alternatename column', done => {
        let message = "This is a test message that talks about news in Thintis";
        let expectedLocationFeature = '{"type":"Point","coordinates":[22.06667,32.8]}';

        let callback = (location, lang, error) => {
            try {
                assert(location && location.features && location.features.length == 1);
                assert(JSON.stringify(location.features[0]) == expectedLocationFeature);
            } catch (err) {
                done(err);
                return;
            }

            done();
        };

        locationInference.findLocation("no user", undefined, message, undefined, console.log, callback);
    });

    it('Verify detection of localities in arabic text when locality is defined in alternatename column', done => {
        let message = "طرابلس";
        let expectedLocationFeature = '{"type":"Point","coordinates":[13.18472,32.88972]}';

        let callback = (location, lang, error) => {
            try {
                assert(location && location.features && location.features.length == 1);
                assert(JSON.stringify(location.features[0]) == expectedLocationFeature);
            } catch (err) {
                done(err);
                return;
            }

            done();
        };

        locationInference.findLocation("no user", undefined, message, undefined, console.log, callback);
    });

    it('Verify we can pull multiple locations for an untracked user id, and no provided language, for an english message', done => {
        let message = "This is a test message that talks about news in tripoli and Zāwiyat at Tart";
        let expectedLocationFeatures = '[{"type":"Point","coordinates":[13.18472,32.88972]},{"type":"Point","coordinates":[22.06667,32.8]}]';

        let callback = (location, lang, error) => {
            try {
                assert(location && location.features && location.features.length == 2);
                assert(JSON.stringify(location.features) == expectedLocationFeatures);
            } catch (err) {
                done(err);
                return;
            }

            done();
        };

        locationInference.findLocation("no user", undefined, message, undefined, console.log, callback);
    });

    it('Verify we can pull multiple locations for an untracked user id, and no provided language, for an english message when alternative name field is empty', done => {
        let message = "This is a test message that talks about news in Qaryat Qārāt";
        let expectedLocationFeatures = '[{"type":"Point","coordinates":[13.07583,32.21194]}]';

        let callback = (location, lang, error) => {
            try {
                assert(location && location.features && location.features.length == 1);
                assert(JSON.stringify(location.features) == expectedLocationFeatures);
            } catch (err) {
                done(err);
                return;
            }

            done();
        };

        locationInference.findLocation("no user", undefined, message, undefined, console.log, callback);
    });


    it('Verify we can pull multiple locations for an untracked user id, and no provided language, for an english message when message contains multiple references to the same location', done => {
        let message = "I live in Qaryat Qārāt. This is a test message that talks about news in Qaryat Qārāt";
        let expectedLocationFeatures = '[{"type":"Point","coordinates":[13.07583,32.21194]}]';

        let callback = (location, lang, error) => {
            try {
                assert(location && location.features && location.features.length == 1);
                assert(JSON.stringify(location.features) == expectedLocationFeatures);
            } catch (err) {
                done(err);
                return;
            }

            done();
        };

        locationInference.findLocation("no user", undefined, message, undefined, console.log, callback);
    });

});

