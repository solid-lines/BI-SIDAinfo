const https = require("https");
const { logger, logger_fr } = require('./logger.js');
const endpointConfig = require('./config.json');
const { reject } = require("async");
var fs = require('fs');

//PROGRAMS
const PROGRAM_TARV = "e3swbbSnbQ2";
const PROGRAM_PTME_MERE = "MVooF5iCp8L";
const PROGRAM_PTME_ENFANT = "PiXg9cX1i0y";

//ORGUNITS
const OU_MAPPING = {
    "003BDI003S010912": "FG6tYYTDf5d", // Centre Akabanga Rumonge
    "003BDI006S010120": "g9r6PbRYVAi", // Centre Akabanga Gitega
    "003BDI010S020502": "nniW4f9J1tB", // Centre Akabanga Nyanza-Lac
    "003BDI012S010120": "yW1SToCNaYm", // Centre Akabanga Muyinga
    "003BDI013S010301": "IFiJar1g6y9", // Hôpital de Kibumbu
    "003BDI014S010120": "ZAZapFNLXru", // Centre Akabanga Ngozi
    "003BDI017S010401": "YIJAETW8k0a", // Centre Akabanga Bujumbura
    "003BDI017S020203": "DLsHsaJhtnk", // Hôpital Militaire de Kamenge
};

//TODO: provide orgUnit as an argument for retrieveDHISdata.js
//const orgUnit = "DLsHsaJhtnk";
const SOURCE_ID = "003BDI017S020203"; //TODO: parametrizar
const orgUnit = OU_MAPPING[SOURCE_ID];
const parent_DHIS2data_folder = "DHIS2_data"
const DHIS2data_folder = parent_DHIS2data_folder + "/" + SOURCE_ID

//check if TEIs folder exists. If not, create it
if (!fs.existsSync(DHIS2data_folder)) {
    fs.mkdirSync(DHIS2data_folder);
}


//Main
saveTEIs(orgUnit);

//Get TEIs given a PROGRAM and an ORGUNIT
async function getTEIs(programUID, orgUnit) {

    return new Promise((resolve) => {
        let TEIs;
        try {
            let optionsgetmsg = {
                host: endpointConfig.dhisServer, // here only the domain name (no http/https !)
                port: 443,
                path: '/api/trackedEntityInstances.json?program=' + encodeURIComponent(programUID) + '&ou=' + orgUnit + '&fields=*,enrollments&paging=false', // the rest of the url with parameters if needed
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
    //Get TEIs from 3 programs given an orgUnit
    var enfant_teis = await getTEIs(PROGRAM_PTME_ENFANT, orgUnit).catch((err) => {
        logger.error(err)
    });

    var mere_teis = await getTEIs(PROGRAM_PTME_MERE, orgUnit).catch((err) => {
        logger.error(err)
    });

    var tarv_teis = await getTEIs(PROGRAM_TARV, orgUnit).catch((err) => {
        logger.error(err)
    });

    /**** Save to JSON file ****/
    const ENFANT_DHIS2_FILE = DHIS2data_folder + "/enfant.json";
    const MERE_DHIS2_FILE = DHIS2data_folder + "/mere.json";
    const TARV_DHIS2_FILE = DHIS2data_folder + "/tarv.json";
    try {
        fs.writeFileSync(ENFANT_DHIS2_FILE, JSON.stringify(enfant_teis));
        fs.writeFileSync(MERE_DHIS2_FILE, JSON.stringify(mere_teis));
        fs.writeFileSync(TARV_DHIS2_FILE, JSON.stringify(tarv_teis));
    } catch (err) {
        // An error occurred
        logger.error(err);
    }
}


