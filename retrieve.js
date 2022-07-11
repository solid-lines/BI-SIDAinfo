const https = require("https");
const Moment = require('moment');
const { logger_retrieve } = require('./logger.js');
var fs = require('fs');
var dhis2_to_script_file = require('./dhis2_to_script_files.js');
const utils = require('./utils.js');
var pjson = require('./package.json');

function retrieve_data(SOURCE_OU_CODE, SOURCE_DATE) {
    logger_retrieve.info(`Running retrieve. Version ${pjson.version}`)
    try{
        retrieve_data_complete(SOURCE_OU_CODE, SOURCE_DATE)
    } catch (error) {
        logger_retrieve.error(error.stack)
        process.exit(1)
    }
}


function retrieve_data_complete(SOURCE_OU_CODE, SOURCE_DATE) {
    logger_retrieve.info(`Running retrieve for site:${SOURCE_OU_CODE} and export date:${SOURCE_DATE}`)

    //PROGRAMS
    const PROGRAM_TARV = "e3swbbSnbQ2";
    const PROGRAM_PTME_MERE = "MVooF5iCp8L";
    const PROGRAM_PTME_ENFANT = "PiXg9cX1i0y";

    const endpoint_filename='./config.json'
    const endpoint_rawdata = fs.readFileSync(endpoint_filename);
    const endpointConfig = JSON.parse(endpoint_rawdata);

    const ou_mapping_filename='./ou_mapping.json'
    const rawdata = fs.readFileSync(ou_mapping_filename);
    const OU_MAPPING = JSON.parse(rawdata);

    const EXPORT_DUMP_DATE = Moment(SOURCE_DATE.replace("-", ""), "YYYYMMDD")
    if (EXPORT_DUMP_DATE.isValid() == false){
        logger_retrieve.error(`Invalid export_dump_date ${SOURCE_DATE}`)
        process.exit(1)
    }

    const orgUnit = OU_MAPPING[SOURCE_OU_CODE];
    if(typeof orgUnit === "undefined"){
        logger_retrieve.error(`There is no mapping to the org unit ${SOURCE_OU_CODE}`)
        process.exit(1)
    }

    const parent_DHIS2data_folder = "PREVIOUS_DHIS2_data"
    const SOURCE_ID = SOURCE_OU_CODE + '_' + SOURCE_DATE
    const DHIS2data_folder = parent_DHIS2data_folder + "/" + SOURCE_ID

    //check if folder exists. If not, create it. If yes, remove directory & create it again
    if (!fs.existsSync(parent_DHIS2data_folder)) {
        logger_retrieve.info(`Creating folder ${parent_DHIS2data_folder}`)
        fs.mkdirSync(parent_DHIS2data_folder);
        logger_retrieve.info(`Creating folder ${DHIS2data_folder}`)
        fs.mkdirSync(DHIS2data_folder);
    } else if (!fs.existsSync(DHIS2data_folder)) {
        logger_retrieve.info(`Creating folder ${DHIS2data_folder}`)
        fs.mkdirSync(DHIS2data_folder);
    } else {
        logger_retrieve.info(`Removing folder ${DHIS2data_folder} (including files)`)
        fs.rmSync(DHIS2data_folder, { recursive: true, force: true }); // remove directory and its content
        logger_retrieve.info(`Creating folder ${DHIS2data_folder}`)
        fs.mkdirSync(DHIS2data_folder);
    }

    // create folder for new data from SIDAinfo dump
    const SOURCES_FOLDERNAME = "NEW_SIDAINFO_data"
    const SOURCES_PATH = `${SOURCES_FOLDERNAME}/${SOURCE_ID}`
    if (!fs.existsSync(SOURCES_PATH)) {
        logger_retrieve.info(`Creating folder (for copying later the SIDAinfo dump files) ${SOURCES_PATH}`)
        fs.mkdirSync(SOURCES_PATH, { recursive: true, force: true });
    }

    main();

    //Main
    async function main() {
        logger_retrieve.info(`Retrieving SIDAinfo data from OU ${SOURCE_OU_CODE} (${orgUnit}) and server ${endpointConfig.dhisServer}`)
        await saveTEIs(orgUnit).catch((err) => {
            logger_retrieve.error(err)
        });;
        dhis2_to_script_file.generate_patient_index_and_teis(SOURCE_ID);
        logger_retrieve.info(`*** PROCESS FINISHED SUCCESSFULLY ***`);
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
                        if (res.statusCode != 200) {
                            logger_retrieve.error(`response statusCode=${res.statusCode}`)
                            logger_retrieve.error(body)
                            TEIs = "";
                            resolve(TEIs)                            
                        } else if (myJSONParse.length != 0) {
                            TEIs = myJSONParse.trackedEntityInstances;
                            resolve(TEIs)
                        } else {
                            logger_retrieve.error(body)
                            TEIs = "";
                            resolve(TEIs)
                        }
                    })
                });

                reqGet.end();

                reqGet.on('error', function (e) {
                    logger_retrieve.error(e);
                    TEIs = "";
                    resolve(TEIs)
                });

            } catch (error) {
                logger_retrieve.error(error);
                TEIs = "";
                resolve(TEIs)
            }
        });
    }


    async function saveTEIs(orgUnit) {
        //Get TEIs from 3 programs for a given orgUnit
        logger_retrieve.info("Retrieving enfant_teis")
        let enfant_teis = await getTEIs(PROGRAM_PTME_ENFANT, orgUnit).catch((err) => {
            logger_retrieve.error(err)
        });

        logger_retrieve.info("Retrieving mere_teis")
        let mere_teis = await getTEIs(PROGRAM_PTME_MERE, orgUnit).catch((err) => {
            logger_retrieve.error(err)
        });

        logger_retrieve.info("Retrieving tarv_teis")
        let tarv_teis = await getTEIs(PROGRAM_TARV, orgUnit).catch((err) => {
            logger_retrieve.error(err)
        });

        /**** Save to JSON file ****/
        const ENFANT_DHIS2_FILE = DHIS2data_folder + "/enfant.json";
        const MERE_DHIS2_FILE = DHIS2data_folder + "/mere.json";
        const TARV_DHIS2_FILE = DHIS2data_folder + "/tarv.json";

        if (typeof enfant_teis === "undefined"){
            logger_retrieve.warn("No enfant TEIs retrieved");
            enfant_teis = []
        }
        if (typeof mere_teis === "undefined") {
            logger_retrieve.warn("No mere TEIs retrieved");
            mere_teis = []
        }
        if (typeof tarv_teis === "undefined") {
            logger_retrieve.warn("No tarv TEIs retrieved");
            tarv_teis = []
        }

        // TODO remove soft-deleted events: https://jira.dhis2.org/browse/DHIS2-12285

        try {
            logger_retrieve.info(`Saving ${ENFANT_DHIS2_FILE} file`)
            utils.saveJSONFile(ENFANT_DHIS2_FILE, enfant_teis);
            logger_retrieve.info(`Saving ${MERE_DHIS2_FILE} file`)
            utils.saveJSONFile(MERE_DHIS2_FILE, mere_teis);
            logger_retrieve.info(`Saving ${TARV_DHIS2_FILE} file`)
            utils.saveJSONFile(TARV_DHIS2_FILE, tarv_teis);
        } catch (err) {
            // An error occurred
            logger_retrieve.error(err);
        }
    }
}

module.exports = {
    retrieve_data
}