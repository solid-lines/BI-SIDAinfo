const axios = require('axios');
var fs = require('fs');
const { logger, logger_fr } = require('./logger.js');
const endpointConfig = require('./config.json');

//TODO add await in requests

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
const DV_TYPE = "DV";

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

var DVs = getActionByResourceType(actions, DV_TYPE);


/************** TEI actions upload *********************/

/* Delete */
// In 2.36, delete a TEI means: delete TEI, delete enrollment, delete events (but in 2.33, events were not deleted).
const TEIs_toDelete = getActionByOperationType(TEIs, DELETE);
TEIs_toDelete.forEach((TEI) => {
    logger.info(`Delete TEI ${TEI.uid}`);
    delete_resource(TEI_TYPE, TEI.uid)
});

/* Create */
const TEIs_toCreate = getActionByOperationType(TEIs, CREATE);
TEIs_toCreate.forEach((TEI) => {
    const payload = getTEIpayload(TEI.uid);
    logger.info(`Create TEI ${TEI.uid}, payload: ${JSON.stringify(payload)}`);
    post_resource(TEI_TYPE, TEI.uid, payload);
});


/************** TEA actions upload *********************/

// TODO: improvement. send one PUT per TEI (no one PUT per each TEA)

/* Delete */
const TEAs_toDelete = getActionByOperationType(TEAs, DELETE);
TEAs_toDelete.forEach((TEA) => {
    const payload = getTEApayload(TEA.TEI);
    logger.info(`Delete TEA ${TEA.TEA} (TEI ${TEA.TEI}), payload: ${JSON.stringify(payload)}`);
    put_resource(TEA_TYPE, TEA.TEI, payload);
});

/* Create */
const TEAs_toCreate = getActionByOperationType(TEAs, CREATE);
TEAs_toCreate.forEach((TEA) => {
    const payload = getTEApayload(TEA.TEI);
    logger.info(`Create TEA ${TEA.TEA} (TEI ${TEA.TEI}), payload: ${JSON.stringify(payload)}`);
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

/* Delete */
const enrollments_to_delete = getActionByOperationType(enrollments, DELETE);
enrollments_to_delete.forEach((enrollment) => {
    const enrollment_uid = enrollment.uid
    logger.info(`Delete enrollment ${enrollment_uid} (TEI ${enrollment.TEI})`);
    delete_resource(ENROLLMENT_TYPE, enrollment_uid);
});

/* Create */
const enrollments_to_create = getActionByOperationType(enrollments, CREATE);
enrollments_to_create.forEach((enrollment) => {
    const enrollment_uid = enrollment.uid
    const payload = get_enrollment_payload(enrollment.TEI, enrollment_uid);
    logger.info(`Create enrollment ${enrollment_uid} (TEI ${enrollment.TEI}), payload: ${JSON.stringify(payload)}`);
    post_resource(ENROLLMENT_TYPE, enrollment_uid, payload);
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

/************** Events actions upload *********************/

/* Delete */
const events_to_delete = getActionByOperationType(events, DELETE);
events_to_delete.forEach((event) => {
    const event_uid = event.uid
    const enrollment_uid = event.enrollment
    const tei_uid = event.TEI
    logger.info(`Delete event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid})`);
    delete_resource(EVENT_TYPE, event_uid);
});

/* Create */
const events_to_create = getActionByOperationType(events, CREATE);
events_to_create.forEach((event) => {
    const event_uid = event.uid
    const enrollment_uid = event.enrollment
    const tei_uid = event.TEI
        const payload = get_event_payload(tei_uid, enrollment_uid, event_uid);
    logger.info(`Create event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
    post_resource(EVENT_TYPE, event_uid, payload);
});

/* Update */
//Event_STATUS_UPDATE + Event_DUEDATE_UPDATE
const events_to_update = getActionByOperationType(events, UPDATE);
events_to_update.forEach((event) => {
    const event_uid = event.uid
    const enrollment_uid = event.enrollment
    const tei_uid = event.TEI
    const payload = get_event_payload(tei_uid, enrollment_uid, event_uid); // send all payload, incuding data values
    logger.info(`Update event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
    put_resource(EVENT_TYPE, event_uid, payload);
});

// /************** DataElements/DataValues actions upload *********************/

// TODO. Y si enviar todos los campos del actual? Que pasa con los que estan deleted? probar con PUT y con POST

/* Delete */
//send the datavalue empty ("")
const dv_to_delete = getActionByOperationType(DVs, DELETE);
dv_to_delete.forEach((dv) => {
    const dv_de = dv.dataElement
    const event_uid = dv.event
    const enrollment_uid = dv.enrollment
    const tei_uid = dv.TEI
    const payload = get_dv_delete_event_payload(tei_uid, enrollment_uid, event_uid, dv_de);
    logger.info(`Delete datavalue ${dv_de} event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
    put_resource(DV_TYPE, event_uid+"/"+dv_de, payload);
});

/* Create */
const dv_to_create = getActionByOperationType(DVs, CREATE);
dv_to_create.forEach((dv) => {
    const dv_de = dv.dataElement
    const event_uid = dv.event
    const enrollment_uid = dv.enrollment
    const tei_uid = dv.TEI
    const payload = get_dv_event_payload(tei_uid, enrollment_uid, event_uid, dv_de);
    logger.info(`Create datavalue ${dv_de} event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
    put_resource(DV_TYPE, event_uid+"/"+dv_de, payload);
});


/* Update */
const dv_to_update = getActionByOperationType(DVs, UPDATE);
dv_to_update.forEach((dv) => {
    const dv_de = dv.dataElement
    const event_uid = dv.event
    const enrollment_uid = dv.enrollment
    const tei_uid = dv.TEI
    const payload = get_dv_event_payload(tei_uid, enrollment_uid, event_uid, dv_de);
    logger.info(`Update datavalue ${dv_de} event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
    put_resource(DV_TYPE, event_uid+"/"+dv_de, payload);
});


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
        [ENROLLMENT_TYPE]: "enrollments/",
        [EVENT_TYPE]: "events/"
    }


    logger.info(`POST ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
    const baseURL = `https://${endpointConfig.dhisServer}/api/`
    const config = {
        baseURL: baseURL,
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        },
        auth: {
            username: endpointConfig.dhisUser,
            password: endpointConfig.dhisPass
        },
        validateStatus: function (status) {
            return status == 200; // TEI. not only create also update
        },
    }

    axios.post(mapping[resource_type], payload, config)
      .then(function (response) {
        logger.info(`POST ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
        logger.info(JSON.stringify(response.data))
      })
      .catch(function (error) {
        logger.error(`POST ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
        logger.error(error.response.data)
      });

}

async function put_resource(resource_type, uid, payload) {
    
    const mapping = {
        [TEA_TYPE]: "trackedEntityInstances/"+uid+"?program=MVooF5iCp8L", // TODO only needs a valid program uid
        [ENROLLMENT_TYPE]: "enrollments/"+uid,
        [EVENT_TYPE]: "events/"+uid,
        [DV_TYPE]: "events/"+uid
    }

    logger.info(`PUT ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
    const baseURL = `https://${endpointConfig.dhisServer}/api/`
    const config = {
        baseURL: baseURL,
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        },
        auth: {
            username: endpointConfig.dhisUser,
            password: endpointConfig.dhisPass
        },
        validateStatus: function (status) {
            return status == 200;
        },
    }

    axios.put(mapping[resource_type], payload, config)
      .then(function (response) {
        logger.info(`PUT ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
        logger.info(JSON.stringify(response.data))
      })
      .catch(function (error) {
        logger.error(`PUT ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
        logger.error(error.response.data)
      });
}

async function delete_resource(resource_type, uid) {
    
    const mapping = {
        [TEI_TYPE]: "trackedEntityInstances/"+uid,
        [ENROLLMENT_TYPE]: "enrollments/"+uid,
        [EVENT_TYPE]: "events/"+uid
    }

    logger.info(`DELETE ${resource_type} ${uid}. url '${mapping[resource_type]}'`);

    const baseURL = `https://${endpointConfig.dhisServer}/api/`
    const config = {
        baseURL: baseURL,
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        },
        auth: {
            username: endpointConfig.dhisUser,
            password: endpointConfig.dhisPass
        },
        validateStatus: function (status) {
            return status == 200;
        },
    }

    axios.delete(mapping[resource_type], config)
      .then(function (response) {
        logger.info(`DELETE ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
        logger.info(JSON.stringify(response.data))
      })
      .catch(function (error) {
        logger.error(`DELETE ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
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
    const enrollments = tei['enrollments'].filter(function(enrol){
        return enrol.enrollment == enrollment_uid;
    })
    return enrollments[0] // return one enrollment
}

function get_enrollment_status_payload(tei_uid, enrollment_uid){
    let enrollment = get_enrollment_payload(tei_uid, enrollment_uid)
    delete enrollment['events'] // remove events key-value
    return enrollment
}

function get_event_payload(tei_uid, enrollment_uid, event_uid){
    const enrollment = get_enrollment_payload(tei_uid, enrollment_uid)
    const events = enrollment['events'].filter(function(ev){
        return ev.event == event_uid;
    })
    events[0]["enrollment"]=enrollment_uid // add link to enrollment
    return events[0] // return one event
}

function get_dv_event_payload(tei_uid, enrollment_uid, event_uid, de_uid){
    let event = get_event_payload(tei_uid, enrollment_uid, event_uid)
    delete event['enrollment'] // remove enrollment
    // delete event['dueDate'] // remove dueDate // KEEP DUEDATE DUE TO A BUG IN DHIS2 https://jira.dhis2.org/browse/DHIS2-12934
    delete event['eventDate'] // remove eventDate
    const dvs = event['dataValues'].filter(function(dv){
        return dv.dataElement == de_uid;
    })
    delete event['dataValues'] // remove dataValues
    event['dataValues'] = dvs
    return event
}


function get_dv_delete_event_payload(tei_uid, enrollment_uid, event_uid, de_uid){
    const dv = { dataElement: de_uid, value: ""}
    let event = get_dv_event_payload(tei_uid, enrollment_uid, event_uid, de_uid) // return event with empty dataValues
    event['dataValues'].push(dv)
    return event
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
