const axios = require('axios');
var fs = require('fs');
const { logger_upload } = require('./logger.js');
const utils = require('./utils.js');
var pjson = require('./package.json');


function upload_data(SOURCE_OU_CODE) {
    logger_upload.info(`Running update (upload) version ${pjson.version}`)
    try{
        upload_data_complete(SOURCE_OU_CODE)
    } catch (error) {
        logger_upload.error(error.stack)
        process.exitCode = 1;
    }
}

//TODO add await in requests
function upload_data_complete(SOURCE_OU_CODE) {
    logger_upload.info(`Running upload for ${SOURCE_OU_CODE}`)

    const endpoint_filename='./config.json'
    const endpoint_rawdata = fs.readFileSync(endpoint_filename);
    const endpointConfig = JSON.parse(endpoint_rawdata);    

    /*********** Read actions.json ********/
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
    i=0
    TEIs_toDelete.forEach((TEI) => {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        logger_upload.info(`Delete TEI ${TEI.uid}`);
        delete_resource(TEI_TYPE, TEI.uid)
    });

    /* Create */
    const TEIs_toCreate = getActionByOperationType(TEIs, CREATE);
    i=0
    TEIs_toCreate.forEach((TEI) => {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        const payload = getTEIpayload(TEI.uid);
        logger_upload.info(`Create TEI ${TEI.uid}, payload: ${JSON.stringify(payload)}`);
        post_resource(TEI_TYPE, TEI.uid, payload);
    });


    /************** TEA actions upload *********************/

    // TODO: improvement. send one PUT per TEI (no one PUT per each TEA)

    /* Delete */
    const TEAs_toDelete = getActionByOperationType(TEAs, DELETE);
    i=0
    TEAs_toDelete.forEach((TEA) => {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        const payload = getTEApayload(TEA.TEI);
        logger_upload.info(`Delete TEA ${TEA.TEA} (TEI ${TEA.TEI}), payload: ${JSON.stringify(payload)}`);
        put_resource(TEA_TYPE, TEA.TEI, payload);
    });

    /* Create */
    const TEAs_toCreate = getActionByOperationType(TEAs, CREATE);
    i=0
    TEAs_toCreate.forEach((TEA) => {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        const payload = getTEApayload(TEA.TEI);
        logger_upload.info(`Create TEA ${TEA.TEA} (TEI ${TEA.TEI}), payload: ${JSON.stringify(payload)}`);
        put_resource(TEA_TYPE, TEA.TEI, payload);
    });

    /* Update */
    const TEAs_toUpdate = getActionByOperationType(TEAs, UPDATE);
    i=0
    TEAs_toUpdate.forEach((TEA) => {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        const payload = getTEApayload(TEA.TEI);
        logger_upload.info(`Update TEA ${TEA.TEA} (TEI ${TEA.TEI}), payload: ${JSON.stringify(payload)}`);
        put_resource(TEA_TYPE, TEA.TEI, payload);
    });


    /************** Enrollment actions upload *********************/

    /* Delete */
    const enrollments_to_delete = getActionByOperationType(enrollments, DELETE);
    i=0
    enrollments_to_delete.forEach((enrollment) => {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        const enrollment_uid = enrollment.uid
        logger_upload.info(`Delete enrollment ${enrollment_uid} (TEI ${enrollment.TEI})`);
        delete_resource(ENROLLMENT_TYPE, enrollment_uid);
    });

    /* Create */
    const enrollments_to_create = getActionByOperationType(enrollments, CREATE);
    i=0
    enrollments_to_create.forEach((enrollment) => {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        const enrollment_uid = enrollment.uid
        const payload = get_enrollment_payload(enrollment.TEI, enrollment_uid);
        logger_upload.info(`Create enrollment ${enrollment_uid} (TEI ${enrollment.TEI}), payload: ${JSON.stringify(payload)}`);
        post_resource(ENROLLMENT_TYPE, enrollment_uid, payload);
    });

    /* Update */
    //Enrollment_STATUS_UPDATE
    const enrollments_to_update = getActionByOperationType(enrollments, UPDATE);
    i=0
    enrollments_to_update.forEach((enrollment) => {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        const enrollment_uid = enrollment.uid
        const payload = get_enrollment_status_payload(enrollment.TEI, enrollment_uid);
        logger_upload.info(`Update enrollment status ${enrollment_uid} (TEI ${enrollment.TEI}), payload: ${JSON.stringify(payload)}`);
        put_resource(ENROLLMENT_TYPE, enrollment_uid, payload);
    });

    /************** Events actions upload *********************/

    /* Delete */
    const events_to_delete = getActionByOperationType(events, DELETE);
    let list_events_to_delete = []
    events_to_delete.forEach((event) => {
        const event_uid = event.uid
        const enrollment_uid = event.enrollment
        const tei_uid = event.TEI
        logger_upload.info(`Delete event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid})`);
        //delete_resource(EVENT_TYPE, event_uid);
        payload = {"event": event_uid}
        list_events_to_delete.push(payload)
        if ((list_events_to_delete.length % 50) == 0){
            logger_upload.info("List lenght == 50")
            logger_upload.info(list_events_to_delete)
            delete_list_resources(EVENT_TYPE, list_events_to_delete);
            //clean list
            list_events_to_delete = []
        }
    });
    // send last list of events
    if (list_events_to_delete.length>0){
        delete_list_resources(EVENT_TYPE, list_events_to_delete);
    }

    /* Create */

    const events_to_create = getActionByOperationType(events, CREATE);
    let list_events_to_create = []
    events_to_create.forEach((event) => {
        const event_uid = event.uid
        const enrollment_uid = event.enrollment
        const tei_uid = event.TEI
        const payload = get_event_payload(tei_uid, enrollment_uid, event_uid);
        logger_upload.info(`Create event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
        //post_resource(EVENT_TYPE, event_uid, payload);
        list_events_to_create.push(payload)
        if ((list_events_to_create.length % 50) == 0){
            logger_upload.info("List lenght == 50")
            logger_upload.info(list_events_to_create)
            post_list_resources(EVENT_TYPE, list_events_to_create);
            //clean list
            list_events_to_create = []
        }
    });
    // send last list of events
    if (list_events_to_create.length>0){
        post_list_resources(EVENT_TYPE, list_events_to_create);
    }


    /* Update */
    //Event_STATUS_UPDATE + Event_DUEDATE_UPDATE
    const events_to_update = getActionByOperationType(events, UPDATE);
    i=0
    events_to_update.forEach((event) => {
        utils.wait(500)
        i=i+1
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        const event_uid = event.uid
        const enrollment_uid = event.enrollment
        const tei_uid = event.TEI
        const payload = get_event_payload(tei_uid, enrollment_uid, event_uid); // send all payload, incuding data values
        logger_upload.info(`Update event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
        put_resource(EVENT_TYPE, event_uid, payload);
    });

    // /************** DataElements/DataValues actions upload *********************/

    // TODO. Y si enviar todos los campos del actual? Que pasa con los que estan deleted? probar con PUT y con POST

    /* Delete */
    //send the datavalue empty ("")
    const dv_to_delete = getActionByOperationType(DVs, DELETE);
    i=0
    dv_to_delete.forEach((dv) => {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }       
        const dv_de = dv.dataElement
        const event_uid = dv.event
        const enrollment_uid = dv.enrollment
        const tei_uid = dv.TEI
        const payload = get_dv_delete_event_payload(tei_uid, enrollment_uid, event_uid, dv_de);
        logger_upload.info(`Delete datavalue ${dv_de} event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
        put_resource(DV_TYPE, event_uid+"/"+dv_de, payload);
    });

    /* Create */
    const dv_to_create = getActionByOperationType(DVs, CREATE);
    i = 0
    dv_to_create.forEach((dv) => {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }    
        const dv_de = dv.dataElement
        const event_uid = dv.event
        const enrollment_uid = dv.enrollment
        const tei_uid = dv.TEI
        const payload = get_dv_event_payload(tei_uid, enrollment_uid, event_uid, dv_de);
        logger_upload.info(`Create datavalue ${dv_de} event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
        put_resource(DV_TYPE, event_uid+"/"+dv_de, payload);
    });


    /* Update */
    const dv_to_update = getActionByOperationType(DVs, UPDATE);
    i=0;
    dv_to_update.forEach((dv) => {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }    
        const dv_de = dv.dataElement
        const event_uid = dv.event
        const enrollment_uid = dv.enrollment
        const tei_uid = dv.TEI
        const payload = get_dv_event_payload(tei_uid, enrollment_uid, event_uid, dv_de);
        logger_upload.info(`Update datavalue ${dv_de} event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
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


        logger_upload.info(`POST ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
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
            logger_upload.info(`POST ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.info(JSON.stringify(response.data))
        })
        .catch(function (error) {
            logger_upload.error(`POST ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.error(error)
            if (("response" in error) && ("data" in error.response)) {
                logger_upload.error(error.response.data)
            }
        });

    }

    async function post_list_resources(resource_type, list_resources) {
        
        const mapping = {
    //        [TEI_TYPE]: "trackedEntityInstances",
    //        [ENROLLMENT_TYPE]: "enrollments",
            [EVENT_TYPE]: "events"
        }

        let payload = new Object();
        payload[mapping[resource_type]] = list_resources;

        logger_upload.info(payload)

        logger_upload.info(`POST List of ${resource_type}. url '${mapping[resource_type]}/'`);
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
            logger_upload.info(`POST ${resource_type}. url '${mapping[resource_type]}/'`);
            logger_upload.info(JSON.stringify(response.data))
        })
        .catch(function (error) {
            logger_upload.error(`POST ${resource_type}. url '${mapping[resource_type]}/'`);
            logger_upload.error(error)
            if (("response" in error) && ("data" in error.response)) {
                logger_upload.error(error.response.data)
            }
        });

    }

    async function delete_list_resources(resource_type, list_resources) {
        
        const mapping = {
    //        [TEI_TYPE]: "trackedEntityInstances",
    //        [ENROLLMENT_TYPE]: "enrollments",
            [EVENT_TYPE]: "events"
        }

        let payload = new Object();
        payload[mapping[resource_type]] = list_resources;

        logger_upload.info(payload)

        logger_upload.info(`POST ?strategy=DELETE List of ${resource_type}. url '${mapping[resource_type]}/'`);
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

        axios.post(mapping[resource_type]+"?strategy=DELETE", payload, config)
        .then(function (response) {
            logger_upload.info(`POST ?strategy=DELETE ${resource_type}. url '${mapping[resource_type]}/'`);
            logger_upload.info(JSON.stringify(response.data))
        })
        .catch(function (error) {
            logger_upload.error(`POST ?strategy=DELETE ${resource_type}. url '${mapping[resource_type]}/'`);
            logger_upload.error(error)
            if (("response" in error) && ("data" in error.response)) {
                logger_upload.error(error.response.data)
            }
        });

    }


    async function put_resource(resource_type, uid, payload) {
        
        const mapping = {
            [TEA_TYPE]: "trackedEntityInstances/"+uid+"?program=MVooF5iCp8L", // TODO only needs a valid program uid
            [ENROLLMENT_TYPE]: "enrollments/"+uid,
            [EVENT_TYPE]: "events/"+uid,
            [DV_TYPE]: "events/"+uid
        }

        logger_upload.info(`PUT ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
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
            logger_upload.info(`PUT ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.info(JSON.stringify(response.data))
        })
        .catch(function (error) {
            logger_upload.error(`PUT ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.error(error);
            if (("response" in error) && ("data" in error.response)) {
                logger_upload.error(error.response.data)
            }
        });
    }

    async function delete_resource(resource_type, uid) {
        
        const mapping = {
            [TEI_TYPE]: "trackedEntityInstances/"+uid,
            [ENROLLMENT_TYPE]: "enrollments/"+uid,
            [EVENT_TYPE]: "events/"+uid
        }

        logger_upload.info(`DELETE ${resource_type} ${uid}. url '${mapping[resource_type]}'`);

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
            logger_upload.info(`DELETE ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.info(JSON.stringify(response.data))
        })
        .catch(function (error) {
            logger_upload.error(`DELETE ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.error(error)
            if (("response" in error) && ("data" in error.response)) {
                logger_upload.error(error.response.data)
            }
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
        const TEI_fileName = `./GENERATED_data/teis/${SOURCE_OU_CODE}/${TEI_uid}.json`;
        if (!fs.existsSync(TEI_fileName)) {
            logger_upload.error(`FileError; TEI file (${TEI_fileName}) doesn't exist`)
        } else {
            TEI_file = JSON.parse(fs.readFileSync(TEI_fileName))
        }

        return TEI_file;
    }
}

module.exports = {
    upload_data
}