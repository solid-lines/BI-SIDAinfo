var fs = require('fs');
const { logger, logger_fr } = require('./logger.js');

/*** Utils ***/
function saveJSONFile(filename, json_data) {
    try {
        fs.writeFileSync(filename, JSON.stringify(json_data, null, 4));
    } catch (err) {
        // An error occurred
        logger.error(err);
    }
}

module.exports = {saveJSONFile};