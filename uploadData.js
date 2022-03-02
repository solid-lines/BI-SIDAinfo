const axios = require('axios');
var fs = require('fs');
const { logger, logger_fr } = require('./logger.js');
const endpointConfig = require('./config.json');


/*********** Read actions.json ********/
const SOURCE_OU_CODE = "003BDI017S020203";//TODO: parametrizar
const SOURCE_DATE = "2021_08_05";//TODO: parametrizar
const ACTIONS_FOLDER = "actions/" + SOURCE_OU_CODE;
const ACTIONS_FILE = "actions/" + SOURCE_OU_CODE + "/actions.json";

//Actions
const CREATE = "CREATE";
const DELETE = "DELETE";
const UPDATE = "UPDATE";

//Resource Types
const TEI_TYPE = "TEI";
const TEA_TYPE = "TEA";
const ENROLLMENT_TYPE = "ENROLLMENT";
const EVENT_TYPE = "EVENT";
const DE_TYPE = "DE";

//check if folder exists. If not, create it
if (!fs.existsSync(ACTIONS_FOLDER)) {
    fs.mkdirSync(ACTIONS_FOLDER);
}

var actions = JSON.parse(fs.readFileSync(ACTIONS_FILE)); //Array


/********** Classify by resource type ******/

var TEIs = getActionByResourceType(actions, TEI_TYPE);

var TEAs = getActionByResourceType(actions, TEA_TYPE);

var enrollments = getActionByResourceType(actions, ENROLLMENT_TYPE);

var events = getActionByResourceType(actions, EVENT_TYPE);

var DEs = getActionByResourceType(actions, DE_TYPE);


// /************** TEI actions upload *********************/

/* Create */

const TEIs_toCreate = getActionByOperationType(TEIs, CREATE);

TEIs_toCreate.forEach((TEI) => {
    const payload = getTEIpayload(TEI.uid);
    logger.info(`Create TEI ${TEI.uid}, payload: ${JSON.stringify(payload)}`);
    post_resource(TEI_TYPE, TEI.uid, payload);
});

/* Delete */
// In 2.36, delete a TEI means: delete TEI, delete enrollment, delete events (but in 2.33, events were not deleted).
const TEIs_toDelete = getActionByOperationType(TEIs, DELETE);
TEIs_toDelete.forEach((TEI) => {
    logger.info(`Delete TEI ${TEI.uid}`);
    delete_resource(TEI_TYPE, TEI.uid)
});

// /************** TEA actions upload *********************/

// TODO: improvement. send one PUT per TEI (no one PUT per each TEA)

/* Create */
const TEAs_toCreate = getActionByOperationType(TEAs, CREATE);
TEAs_toCreate.forEach((TEA) => {
    const payload = getTEApayload(TEA.TEI);
    logger.info(`Create TEA ${TEA.TEA} (TEI ${TEA.TEI}), payload: ${JSON.stringify(payload)}`);
    put_resource(TEA_TYPE, TEA.TEI, payload);
});


/* Delete */
const TEAs_toDelete = getActionByOperationType(TEAs, DELETE);
TEAs_toDelete.forEach((TEA) => {
    const payload = getTEApayload(TEA.TEI);
    logger.info(`Delete TEA ${TEA.TEA} (TEI ${TEA.TEI}), payload: ${JSON.stringify(payload)}`);
    put_resource(TEA_TYPE, TEA.TEI, payload);
});

/* Update */
const TEAs_toUpdate = getActionByOperationType(TEAs, UPDATE);
TEAs_toUpdate.forEach((TEA) => {
    const payload = getTEApayload(TEA.TEI);
    logger.info(`Update TEA ${TEA.TEA} (TEI ${TEA.TEI}), payload: ${JSON.stringify(payload)}`);
    put_resource(TEA_TYPE, TEA.TEI, payload);
});


/************** Enrollment actions upload *********************/

/* Create */
const enrollments_to_create = getActionByOperationType(enrollments, CREATE);
enrollments_to_create.forEach((enrollment) => {
    const enrollment_uid = enrollment.uid
    const payload = get_enrollment_payload(enrollment.TEI, enrollment_uid);
    logger.info(`Create enrollment ${enrollment_uid} (TEI ${enrollment.TEI}), payload: ${JSON.stringify(payload)}`);
    post_resource(ENROLLMENT_TYPE, enrollment_uid, payload);
});

/* Delete */
const enrollments_to_delete = getActionByOperationType(enrollments, DELETE);
enrollments_to_delete.forEach((enrollment) => {
    const enrollment_uid = enrollment.uid
    logger.info(`Delete enrollment ${enrollment_uid} (TEI ${enrollment.TEI})`);
    delete_resource(ENROLLMENT_TYPE, enrollment_uid);
});

/* Update */
//Enrollment_STATUS_UPDATE
const enrollments_to_update = getActionByOperationType(enrollments, UPDATE);
enrollments_to_update.forEach((enrollment) => {
    const enrollment_uid = enrollment.uid
    const payload = get_enrollment_status_payload(enrollment.TEI, enrollment_uid);

    logger.info(`Update enrollment status ${enrollment_uid} (TEI ${enrollment.TEI}), payload: ${JSON.stringify(payload)}`);
    put_resource(ENROLLMENT_TYPE, enrollment_uid, payload);
});

// /************** Events actions upload *********************/

// /* Delete */

// /* Create */

// /* Update */

// /************** DataElements actions upload *********************/

// /* Delete */

// /* Create */

// /* Update */



function getActionByResourceType(actions, resource_type) {
    var actions = actions.filter((action) => {
        if (action.type == resource_type) {
            return action;
        }
    });
    return actions;
}

function getActionByOperationType(actions, operation_type) {
    var actions = actions.filter((action) => {
        if (action.action == operation_type) {
            return action;
        }
    });
    return actions;
}


// /*************** API calls *****************/
async function post_resource(resource_type, uid, payload) {
    
    const mapping = {
        [TEI_TYPE]: "trackedEntityInstances/",
        [ENROLLMENT_TYPE]: "enrollments/"
    }


    logger.info(`POST ${resource_type} ${uid}. url '${mapping[resource_type]}'`);

    const config = {
        baseURL: endpointConfig.dhis2Server,
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        },
        auth: {
            username: endpointConfig.dhis2User,
            password: endpointConfig.dhis2Pass
        },
        validateStatus: function (status) {
            return status == 200; // TEI. not only create also update
        },
    }

    axios.post(mapping[resource_type], payload, config)
      .then(function (response) {
        logger.info(JSON.stringify(response.data))
      })
      .catch(function (error) {
        logger.error(error.response.data)
      });

}

async function put_resource(resource_type, uid, payload) {
    
    const mapping = {
        [TEA_TYPE]: "trackedEntityInstances/"+uid+"?program=MVooF5iCp8L", // TODO only needs a valid program uid
        [ENROLLMENT_TYPE]: "enrollments/"+uid
    }

    logger.info(`PUT ${resource_type} ${uid}. url '${mapping[resource_type]}'`);

    const config = {
        baseURL: endpointConfig.dhis2Server,
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        },
        auth: {
            username: endpointConfig.dhis2User,
            password: endpointConfig.dhis2Pass
        },
        validateStatus: function (status) {
            return status == 200;
        },
    }

    axios.put(mapping[resource_type], payload, config)
      .then(function (response) {
        logger.info(JSON.stringify(response.data))
      })
      .catch(function (error) {
        logger.error(error.response.data)
      });
}

async function delete_resource(resource_type, uid) {
    
    const mapping = {
        [TEI_TYPE]: "trackedEntityInstances/"+uid,
        [ENROLLMENT_TYPE]: "enrollments/"+uid
    }

    logger.info(`DELETE ${resource_type} ${uid}. url '${mapping[resource_type]}'`);

    const config = {
        baseURL: endpointConfig.dhis2Server,
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        },
        auth: {
            username: endpointConfig.dhis2User,
            password: endpointConfig.dhis2Pass
        },
        validateStatus: function (status) {
            return status == 200;
        },
    }

    axios.delete(mapping[resource_type], config)
      .then(function (response) {
        logger.info(JSON.stringify(response.data))
      })
      .catch(function (error) {
        logger.error(error.response.data)
      });
}


/********* Read TEI data ***********/

function getTEIpayload(tei_uid) {
    return readTEI(tei_uid);
}

function getTEApayload(tei_uid) {
    let tei = readTEI(tei_uid);
    delete tei['enrollments'] // remove enrollments key-value
    return tei
}


function get_enrollment_payload(tei_uid, enrollment_uid){
    const tei = readTEI(tei_uid);
    const enrollment = tei['enrollments'].filter(function(enrol){
        return enrol.enrollment == enrollment_uid;
    })
    return enrollment[0] // return one enrollment
}

function get_enrollment_status_payload(tei_uid, enrollment_uid){
    let enrollment = get_enrollment_payload(tei_uid, enrollment_uid)
    delete enrollment['events'] // remove events key-value
    return enrollment
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