var fs = require('fs');
const { logger } = require('./logger.js');

/*** Utils ***/
function saveJSONFile(filename, json_data) {
    try {
        fs.writeFileSync(filename, JSON.stringify(json_data, null, 4));
    } catch (err) {
        // An error occurred
        logger.error(err);
    }
}

function wait(ms) {
    var start = Date.now(),
        now = start;
    while (now - start < ms) {
      now = Date.now();
    }
}

module.exports = {saveJSONFile, wait};