'use strict';

const fs = require('fs'),
      blobPusher = require('./index.js');

let contextMock = {
    log: console.log,
    done: function(err) {
        console.log('done called: ' + err);
        process.exit(1);
    }
};

fs.readFile('test/fixtures/sample.json', 'utf8', function (err, blobText) {
    if (err) return console.log(err);

    let blobAsJson = JSON.parse(blobText);

    blobPusher(contextMock, blobAsJson);
});
