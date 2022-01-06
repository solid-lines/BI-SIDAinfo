var fs = require('fs');
const https = require("https");
const { logger, logger_fr } = require('./logger.js');
const endpointConfig = require('./config.json');


/*********** Read actions.json ********/
const SOURCE_OU_CODE = "003BDI017S020203";//TODO: parametrizar
const SOURCE_DATE = "2021_08_05";//TODO: parametrizar
const ACTIONS_FOLDER = "actions/" + SOURCE_OU_CODE;
const ACTIONS_FILE = "actions/" + SOURCE_OU_CODE + "/actions.json";

//check if folder exists. If not, create it
if (!fs.existsSync(ACTIONS_FOLDER)) {
    fs.mkdirSync(ACTIONS_FOLDER);
}

var actions = JSON.parse(fs.readFileSync(ACTIONS_FILE)); //Array


/********** Classify by resource type ******/

var TEIs = getActionByType(actions, "TEI");

var enrollments = getActionByType(actions, "Enrollment");

var events = getActionByType(actions, "EVENT");

var TEAs = getActionByType(actions, "TEA");

var DEs = getActionByType(actions, "DE");


/************** TEI actions upload *********************/

/* Delete */
var TEIs_toDelete = getActionByAction(TEIs, "DELETE");

//En 2.33: borra TEI, enrollment pero no Events
//TODO: borrar eventos
TEIs_toDelete.forEach((TEI) => {
    //deleteTEI(TEI); //TODO: descomentar
});

/* Create */
var TEIs_toCreate = getActionByAction(TEIs, "CREATE");

TEIs_toCreate.forEach((TEI) => {

    var payload = getTEIpayload(TEI);
    logger.info(`Creating TEI ${TEI.resource}`);//, payload: ${JSON.stringify(payload)}`);
    //createTEI(TEI, payload);//TODO: descomentar
});

/* Update */
var TEIs_toUpdate = getActionByAction(TEIs, "UPDATE");
TEIs_toUpdate.forEach((TEI) => {

    var payload = getTEIpayload(TEI);
    logger.info(`Updating TEI ${TEI.resource}`);//, payload: ${JSON.stringify(payload)}`);
    //updateTEI(TEI);//TODO: descomentar
});


/************** TEA actions upload *********************/

/* Delete */
var TEAs_toDelete = getActionByAction(TEAs, "DELETE");
TEAs_toDelete.forEach((TEA) => {
    deleteTEA(TEA);
});
/* Create */
var TEAs_toCreate = getActionByAction(TEAs, "CREATE");
TEAs_toCreate.forEach((TEA) => {
    createTEA(TEA);
});
/* Update */
var TEAs_toUpdate = getActionByAction(TEAs, "UPDATE");
//TODO: test TEAdeletion, TEAcreation and TEAupdate (no data in this ou dump)

/************** Enrollment actions upload *********************/

/* Delete */

/* Create */

/* Update */

/************** Events actions upload *********************/

/* Delete */

/* Create */

/* Update */

/************** DataElements actions upload *********************/

/* Delete */

/* Create */

/* Update */



function getActionByType(actions, type) {
    var actions = actions.filter((action) => {
        if (action.type == type) {
            return action;
        }
    });
    return actions;
}

function getActionByAction(actions, operation) {
    var actions = actions.filter((action) => {
        if (action.action == operation) {
            return action;
        }
    });
    return actions;
}


/*************** API calls *****************/
//When you delete a TEI, you delete all data associated with the TEI (except events?).
async function deleteTEI(TEI) {

    logger.debug(`Deleting TEI ${TEI}`);
    return new Promise((resolve) => {

        try {
            let optionsgetmsg = {
                host: endpointConfig.dhisServer, // here only the domain name (no http/https !)
                port: 443,
                path: '/api/trackedEntityInstances/' + encodeURIComponent(TEI.uid),
                method: 'DELETE',
                headers: {
                    'Content-Type': 'application/json',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                },
                auth: endpointConfig.dhisUser + ':' + endpointConfig.dhisPass
            };

            // do the DELETE request
            let reqDel = https.request(optionsgetmsg, function (res) {
                var bodyChunks = [];
                res.on('data', function (chunk) {
                    bodyChunks.push(chunk);
                }).on('end', function () {
                    let body = Buffer.concat(bodyChunks);
                    let myJSONParse = JSON.parse(body);

                    logger.info(myJSONParse);
                    resolve(myJSONParse);
                })
            });

            reqDel.end();

            reqDel.on('error', function (e) {
                logger.error(e);
                reject(e)
            });

        } catch (error) {
            reject(e)
        }
    });
}

async function deleteTEA(TEA) {

    logger.debug(`Deleting TEA ${TEA}`);
    //TODO
}

//TODO: body with payload
//TODO: "Non-unique attribute value '003BDI017S020203002602' for attribute dsWUbqvV9GW"
async function createTEI(TEI, payload) {

    logger.debug(`Creating TEI ${TEI}`);
    return new Promise((resolve) => {

        try {
            let optionsgetmsg = {
                host: endpointConfig.dhisServer, // here only the domain name (no http/https !)
                port: 443,
                path: '/api/trackedEntityInstances/',
                method: 'POST',
                body: payload,
                headers: {
                    'Content-Type': 'application/json',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                },
                auth: endpointConfig.dhisUser + ':' + endpointConfig.dhisPass
            };

            // do the POST request
            let reqDel = https.request(optionsgetmsg, function (res) {
                var bodyChunks = [];
                res.on('data', function (chunk) {
                    bodyChunks.push(chunk);
                }).on('end', function () {
                    let body = Buffer.concat(bodyChunks);
                    let myJSONParse = JSON.parse(body);

                    logger.info(myJSONParse);
                    resolve(myJSONParse);
                })
            });

            reqDel.end();

            reqDel.on('error', function (e) {
                logger.error(e);
                reject(e)
            });

        } catch (error) {
            reject(e)
        }
    });
}

async function createTEA(TEA) {

    logger.debug(`Creating TEA ${TEA}`);
    //TODO
}



/********* Read TEI data ***********/

function getTEIpayload(TEI) {
    var uid = TEI.uid;
    return readTEI(uid);

}

function readTEI(TEI_uid) {

    var TEI_file;
    const TEI_fileName = "./teis/" + SOURCE_OU_CODE + "_" + SOURCE_DATE + "/" + TEI_uid + ".json";
    if (!fs.existsSync(TEI_fileName)) {
        logger.error(`FileError; TEI file (${TEI_fileName}) doesn't exist`)
    } else {
        TEI_file = JSON.parse(fs.readFileSync(TEI_fileName))
    }

    return TEI_file;

}