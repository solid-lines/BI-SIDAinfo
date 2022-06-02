const https = require("https");
const { logger } = require('./logger.js');
const endpointConfig = require('./config.json');
const { reject } = require("async");
var fs = require('fs');
var dhis2_to_script_file = require('./dhis2_to_script_file.js');
const utils = require('./utils.js');
const { exit } = require("process");

function retrieve_data(SOURCE_OU_CODE) {

    //PROGRAMS
    const PROGRAM_TARV = "e3swbbSnbQ2";
    const PROGRAM_PTME_MERE = "MVooF5iCp8L";
    const PROGRAM_PTME_ENFANT = "PiXg9cX1i0y";

    const ou_mapping_filename='./ou_mapping.json'
    const rawdata = fs.readFileSync(ou_mapping_filename);
    const OU_MAPPING = JSON.parse(rawdata);

    const orgUnit = OU_MAPPING[SOURCE_OU_CODE];
    const parent_DHIS2data_folder = "PREVIOUS_DHIS2_data"
    const DHIS2data_folder = parent_DHIS2data_folder + "/" + SOURCE_OU_CODE

    //check if folder exists. If not, create it. If yes, remove directory & create it again
    if (!fs.existsSync(DHIS2data_folder)) {
        fs.mkdirSync(DHIS2data_folder);
    } else {
        fs.rmSync(DHIS2data_folder, { recursive: true, force: true }); // remove directory and its content
        fs.mkdirSync(DHIS2data_folder);
    }

    if(typeof orgUnit === "undefined"){
        logger.error(`There is no mapping to the org unit ${SOURCE_OU_CODE}`)
        exit(-1)
    }

    main();

    //Main
    async function main() {
        logger.info(`Retrieving SIDAinfo data from OU ${SOURCE_OU_CODE} (${orgUnit}) and server ${endpointConfig.dhisServer}`)
        await saveTEIs(orgUnit).catch((err) => {
            logger.error(err)
        });;
        dhis2_to_script_file.generate_patient_index_and_teis(SOURCE_OU_CODE);
    }


    //Get TEIs given a PROGRAM and an ORGUNIT
    async function getTEIs(programUID, orgUnit) {

        return new Promise((resolve) => {
            let TEIs;
            try {
                let optionsgetmsg = {
                    host: endpointConfig.dhisServer, // here only the domain name (no http/https !)
                    port: 443,
                    path: '/api/trackedEntityInstances.json?program=' + encodeURIComponent(programUID) + '&ou=' + orgUnit + '&fields=*,enrollments&skipPaging=true', // the rest of the url with parameters if needed
                    method: 'GET', // do GET
                    headers: {
                        'Content-Type': 'application/json',
                        'Cache-Control': 'no-cache',
                        'Pragma': 'no-cache'
                    },
                    auth: endpointConfig.dhisUser + ':' + endpointConfig.dhisPass
                };

                // do the GET request
                let reqGet = https.request(optionsgetmsg, function (res) {
                    var bodyChunks = [];
                    res.on('data', function (chunk) {
                        bodyChunks.push(chunk);
                    }).on('end', function () {
                        let body = Buffer.concat(bodyChunks);
                        let myJSONParse = JSON.parse(body);

                        if (myJSONParse.length != 0) {
                            TEIs = myJSONParse.trackedEntityInstances;
                            resolve(TEIs)
                        } else {
                            TEIs = "";
                            resolve(TEIs)
                        }
                    })
                });

                reqGet.end();

                reqGet.on('error', function (e) {
                    logger.error(e);
                    TEIs = "";
                    //resolve({ enrolledTEIs: TEIs })
                    reject(e)
                });

            } catch (error) {
                TEIs = "";
                //resolve({ enrolledTEIs: TEIs })
                reject(e)
            }
        });
    }


    async function saveTEIs(orgUnit) {
        //Get TEIs from 3 programs for a given orgUnit
        logger.info("Retrieving enfant_teis")
        let enfant_teis = await getTEIs(PROGRAM_PTME_ENFANT, orgUnit).catch((err) => {
            logger.error(err)
        });

        logger.info("Retrieving mere_teis")
        let mere_teis = await getTEIs(PROGRAM_PTME_MERE, orgUnit).catch((err) => {
            logger.error(err)
        });

        logger.info("Retrieving tarv_teis")
        let tarv_teis = await getTEIs(PROGRAM_TARV, orgUnit).catch((err) => {
            logger.error(err)
        });

        /**** Save to JSON file ****/
        const ENFANT_DHIS2_FILE = DHIS2data_folder + "/enfant.json";
        const MERE_DHIS2_FILE = DHIS2data_folder + "/mere.json";
        const TARV_DHIS2_FILE = DHIS2data_folder + "/tarv.json";

        if (typeof enfant_teis === "undefined"){
            logger.warn("No enfant TEIs retrieved");
            enfant_teis = []
        }
        if (typeof mere_teis === "undefined") {
            logger.warn("No mere TEIs retrieved");
            mere_teis = []
        }
        if (typeof tarv_teis === "undefined") {
            logger.warn("No tarv TEIs retrieved");
            tarv_teis = []
        }

        // TODO remove soft-deleted events: https://jira.dhis2.org/browse/DHIS2-12285

        try {
            logger.info("Saving enfant.json file")
            utils.saveJSONFile(ENFANT_DHIS2_FILE, enfant_teis);
            logger.info("Saving mere.json file")
            utils.saveJSONFile(MERE_DHIS2_FILE, mere_teis);
            logger.info("Saving tarv.json file")
            utils.saveJSONFile(TARV_DHIS2_FILE, tarv_teis);
        } catch (err) {
            // An error occurred
            logger.error(err);
        }
    }
}

module.exports = {
    retrieve_data
}