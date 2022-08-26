const axios = require('axios');
var fs = require('fs');
const logger = require('./logger.js');
const utils = require('./utils.js');
var pjson = require('./package.json');

const keypress = async () => {
    process.stdin.setRawMode(true)
    return new Promise(resolve => process.stdin.once('data', () => {
      process.stdin.setRawMode(false)
      resolve()
    }))
  }


async function upload_data(SOURCE_OU_CODE, SOURCE_DATE) {
    const SOURCE_ID = SOURCE_OU_CODE + '_' + SOURCE_DATE
    global.logger_upload = logger.get_logger_upload(SOURCE_ID)
    logger_upload.info(`Running upload. Version ${pjson.version}`)
    try{
        await upload_data_complete(SOURCE_OU_CODE, SOURCE_DATE)
        //logger_upload.info(`*** PROCESS FINISHED ***`);
    } catch (error) {
        logger_upload.error(error.stack)
        logger_upload.error(`*** PROCESS FINISHED WITH ERRORS ***`);
        process.exit(1)
    }
    // TODO logger_upload.info(`*** PROCESS FINISHED SUCCESSFULLY ***`);
}

//TODO add await in requests
async function upload_data_complete(SOURCE_OU_CODE, SOURCE_DATE) {
    logger_upload.info(`Running upload for site:${SOURCE_OU_CODE} and export date:${SOURCE_DATE}`)

    const endpoint_filename='./config.json'
    const endpoint_rawdata = fs.readFileSync(endpoint_filename);
    const endpointConfig = JSON.parse(endpoint_rawdata);
    const auth_token = Buffer.from(`${endpointConfig.dhisUser}:${endpointConfig.dhisPass}`, 'utf8').toString('base64')

    /*********** Read actions.json ********/
    const SOURCE_ID = SOURCE_OU_CODE + '_' + SOURCE_DATE
    const ACTIONS_FILE = "GENERATED_data/" + SOURCE_ID + "/actions.json";

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

    //check if files exists
    if (!fs.existsSync(ACTIONS_FILE)) {
        logger_upload.error("ArgError;Missed action file="+ACTIONS_FILE)
        process.exit(1) // exit, no changes
    }

    logger_upload.info(`Reading file ${ACTIONS_FILE}`)

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
    if (TEIs_toDelete.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`DELETE ${TEIs_toDelete.length} TEIs`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }
    // KUDOS https://stackoverflow.com/questions/37576685/using-async-await-with-a-foreach-loop
    for (const TEI of TEIs_toDelete) {
        utils.wait(1000)
        logger_upload.debug(`Delete TEI ${TEI.uid}`);
        await delete_resource(TEI_TYPE, TEI.uid)
        process.stdout.write('.')
    }

    /* Create */
    const TEIs_toCreate = getActionByOperationType(TEIs, CREATE);
    let list_teis_to_create = []
    if (TEIs_toCreate.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`CREATE ${TEIs_toCreate.length} TEIs`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }    
    for (const TEI of TEIs_toCreate) {
        const payload = getTEIpayload(TEI.uid);
        logger_upload.debug(`Create TEI ${TEI.uid}, payload: ${JSON.stringify(payload)}`);
        //post_resource(TEI_TYPE, TEI.uid, payload);
        list_teis_to_create.push(payload)
        if ((list_teis_to_create.length % 10) == 0){
            process.stdout.write('.'.repeat(10))
            await post_list_resources(TEI_TYPE, list_teis_to_create);
            //clean list
            list_teis_to_create = []
            utils.wait(10000)
        }
    }

    // send last list of resources
    if (list_teis_to_create.length>0){
        process.stdout.write(".".repeat(list_teis_to_create.length))
        await post_list_resources(TEI_TYPE, list_teis_to_create);
    }


    /************** TEA actions upload *********************/

    /* Delete */
    const TEAs_toDelete = getActionByOperationType(TEAs, DELETE);
    const TEIs_TEAs_toDelete = Array.from(new Set(TEAs_toDelete.map(tea => tea.TEI)));
    if (TEAs_toDelete.length != 0) {
        logger_upload.info(`DELETE ${TEAs_toDelete.length} TEAs from ${TEIs_TEAs_toDelete.length} TEIs`)
        for (const TEA of TEAs_toDelete) {
            const payload = getTEApayload(TEA.TEI);
            logger_upload.debug(`Delete TEA ${TEA.TEA} (TEI ${TEA.TEI}), payload: ${JSON.stringify(payload)}`);
        }
    }


    /* Create */
    const TEAs_toCreate = getActionByOperationType(TEAs, CREATE);
    const TEIs_TEAs_toCreate = Array.from(new Set(TEAs_toCreate.map(tea => tea.TEI)));
    if (TEAs_toCreate.length != 0) {
        logger_upload.info(`CREATE ${TEAs_toCreate.length} TEAs`)
        for (const TEA of TEAs_toCreate) {
            const payload = getTEApayload(TEA.TEI);
            logger_upload.debug(`CREATE TEA ${TEA.TEA} (TEI ${TEA.TEI}), payload: ${JSON.stringify(payload)}`);
        }
    }

    /* Update */
    const TEAs_toUpdate = getActionByOperationType(TEAs, UPDATE);
    const TEIs_TEAs_toUpdate = Array.from(new Set(TEAs_toUpdate.map(tea => tea.TEI)));
    if (TEAs_toUpdate.length != 0) {
        logger_upload.info(`UPDATE ${TEAs_toUpdate.length} TEAs`)
        for (const TEA of TEAs_toUpdate) {
            const payload = getTEApayload(TEA.TEI);
            logger_upload.debug(`UPDATE TEA ${TEA.TEA} (TEI ${TEA.TEI}), payload: ${JSON.stringify(payload)}`);
        }
    }

    // final upload process (DELETE, CREATE, UPDATE)
    const TEIs_TEAs_toUpload = Array.from(new Set(TEIs_TEAs_toDelete.concat(TEIs_TEAs_toCreate, TEIs_TEAs_toUpdate)))
    if (TEIs_TEAs_toUpload.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`Updating ${TEIs_TEAs_toUpload.length} TEIs due to change/s in the TEA/s`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }    
    for (const TEI of TEIs_TEAs_toUpload) {
        logger_upload.error(TEIs_TEAs_toUpload)
        utils.wait(1000)
        const payload = getTEApayload(TEI);
        logger_upload.debug(`Updating TEI: ${TEI} due to change/s in the TEA/s, payload: ${JSON.stringify(payload)}`);
        await put_resource(TEA_TYPE, TEI, payload);
        process.stdout.write('.')
    }


    /************** Enrollment actions upload *********************/

    /* Delete */
    const enrollments_to_delete = getActionByOperationType(enrollments, DELETE);
    if (enrollments_to_delete.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`DELETE ${enrollments_to_delete.length} ENROLLMENTS`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }    
    for (const enrollment of enrollments_to_delete) {
        utils.wait(1000)
        const enrollment_uid = enrollment.uid
        logger_upload.debug(`Delete enrollment ${enrollment_uid} (TEI ${enrollment.TEI})`);
        await delete_resource(ENROLLMENT_TYPE, enrollment_uid);
        process.stdout.write('.')
    }

    /* Create */
    const enrollments_to_create = getActionByOperationType(enrollments, CREATE);
    if (enrollments_to_create.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`CREATE ${enrollments_to_create.length} ENROLLMENTS`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }
    i=0
    for (const enrollment of enrollments_to_create) {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        const enrollment_uid = enrollment.uid
        const payload = get_enrollment_payload(enrollment.TEI, enrollment_uid);
        logger_upload.debug(`Create enrollment ${enrollment_uid} (TEI ${enrollment.TEI}), payload: ${JSON.stringify(payload)}`);
        await post_resource(ENROLLMENT_TYPE, enrollment_uid, payload);
        process.stdout.write('.')
    }

    /* Update */
    //Enrollment_STATUS_UPDATE
    const enrollments_to_update = getActionByOperationType(enrollments, UPDATE);
    if (enrollments_to_update.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`UPDATE ${enrollments_to_update.length} ENROLLMENTS`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }
    i=0
    for (const enrollment of enrollments_to_update) {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        const enrollment_uid = enrollment.uid
        const payload = get_enrollment_status_payload(enrollment.TEI, enrollment_uid);
        logger_upload.debug(`Update enrollment status ${enrollment_uid} (TEI ${enrollment.TEI}), payload: ${JSON.stringify(payload)}`);
        await put_resource(ENROLLMENT_TYPE, enrollment_uid, payload);
        process.stdout.write('.')
    }

    /************** Events actions upload *********************/

    /* Delete */
    const events_to_delete = getActionByOperationType(events, DELETE);
    if (events_to_delete.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`DELETE ${events_to_delete.length} EVENTS`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }    
    let list_events_to_delete = []
    for (const event of events_to_delete) {
        const event_uid = event.uid
        const enrollment_uid = event.enrollment
        const tei_uid = event.TEI
        logger_upload.debug(`Delete event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid})`);
        payload = {"event": event_uid}
        list_events_to_delete.push(payload)
        if ((list_events_to_delete.length % 50) == 0){
            process.stdout.write('.'.repeat(50))
            await delete_list_resources(EVENT_TYPE, list_events_to_delete);
            //clean list
            list_events_to_delete = []
        }
    }
    // send last list of events
    if (list_events_to_delete.length>0){
        process.stdout.write(".".repeat(list_events_to_delete.length))
        await delete_list_resources(EVENT_TYPE, list_events_to_delete);
    }


    /* Create */
    const events_to_create = getActionByOperationType(events, CREATE);
    if (events_to_create.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`CREATE ${events_to_create.length} EVENTS`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }
    let list_events_to_create = []
    for (const event of events_to_create) {
        const event_uid = event.uid
        const enrollment_uid = event.enrollment
        const tei_uid = event.TEI
        const payload = get_event_payload(tei_uid, enrollment_uid, event_uid);
        logger_upload.debug(`Create event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
        //post_resource(EVENT_TYPE, event_uid, payload);
        list_events_to_create.push(payload)
        if ((list_events_to_create.length % 50) == 0){
            process.stdout.write('.'.repeat(50))            
            await post_list_resources(EVENT_TYPE, list_events_to_create);
            //clean list
            list_events_to_create = []
        }
    }
    // send last list of events
    if (list_events_to_create.length>0){
        process.stdout.write(".".repeat(list_events_to_create.length))
        await post_list_resources(EVENT_TYPE, list_events_to_create);
    }


    /* Update */
    //Event_STATUS_UPDATE + Event_DUEDATE_UPDATE
    const events_to_update = getActionByOperationType(events, UPDATE);
    if (events_to_update.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`UPDATE ${events_to_update.length} EVENTS`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }
    i=0
    for (const event of events_to_update) {
        utils.wait(500)
        i=i+1
        if ((i%50) == 0){
            utils.wait(10000)
        }
        const event_uid = event.uid
        const enrollment_uid = event.enrollment
        const tei_uid = event.TEI
        const payload = get_event_payload(tei_uid, enrollment_uid, event_uid); // send all payload, incuding data values
        logger_upload.debug(`Update event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
        await put_resource(EVENT_TYPE, event_uid, payload);
        process.stdout.write('.')
    }

    // /************** DataElements/DataValues actions upload *********************/

    // TODO. Y si enviar todos los campos del actual? Que pasa con los que estan deleted? probar con PUT y con POST

    /* Delete */
    //send the datavalue empty ("")
    const dv_to_delete = getActionByOperationType(DVs, DELETE);
    if (dv_to_delete.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`DELETE ${dv_to_delete.length} DATA VALUE (EVENT)`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }
    i=0
    for (const dv of dv_to_delete) {
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
        logger_upload.debug(`Delete datavalue ${dv_de} event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
        await put_resource(DV_TYPE, event_uid+"/"+dv_de, payload);
        process.stdout.write('.')
    }

    /* Create */
    const dv_to_create = getActionByOperationType(DVs, CREATE);
    if (dv_to_create.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`CREATE ${dv_to_create.length} DATA VALUE (EVENT)`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }
    i = 0
    for (const dv of dv_to_create) {
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
        logger_upload.debug(`Create datavalue ${dv_de} event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
        await put_resource(DV_TYPE, event_uid+"/"+dv_de, payload);
        process.stdout.write('.')
    }


    /* Update */
    const dv_to_update = getActionByOperationType(DVs, UPDATE);
    if (dv_to_update.length != 0) {
        process.stdout.write('\n')
        logger_upload.info(`UPDATE ${dv_to_update.length} DATA VALUE (EVENT)`)
        process.stdout.write('Press any key to continue\n')
        await keypress()
    }
    i=0;
    for (const dv of dv_to_update) {
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
        logger_upload.debug(`Update datavalue ${dv_de} event ${event_uid} (TEI ${tei_uid}) (enrollment ${enrollment_uid}), payload: ${JSON.stringify(payload)}`);
        await put_resource(DV_TYPE, event_uid+"/"+dv_de, payload);
        process.stdout.write('.')
    }


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


        logger_upload.debug(`POST ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
        const baseURL = `https://${endpointConfig.dhisServer}/api/`
        const config = {
            baseURL: baseURL,
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache',
                'Authorization': `Basic ${auth_token}`,                
                'Pragma': 'no-cache'
            },
            validateStatus: function (status) {
                return status == 200; // TEI. not only create also update
            },
        }

        await axiosInstance.post(mapping[resource_type], payload, config)
        .then(function (response) {
            logger_upload.debug(`Response POST ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.debug(JSON.stringify(response.data))
        })
        .catch(function (error) {
            logger_upload.error(`Response POST ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.error(JSON.stringify(error.toJSON()))
            if (error.response !== undefined) {
                logger_upload.error(JSON.stringify(error.response.data))
            }
        });

    }

    async function post_list_resources(resource_type, list_resources) {
        
        const mapping = {
            [TEI_TYPE]: "trackedEntityInstances",
    //        [ENROLLMENT_TYPE]: "enrollments",
            [EVENT_TYPE]: "events"
        }

        let payload = new Object();
        payload[mapping[resource_type]] = list_resources;

        logger_upload.debug(JSON.stringify(payload))
        logger_upload.debug(`POST List of ${resource_type}. url '${mapping[resource_type]}/'`);
        const baseURL = `https://${endpointConfig.dhisServer}/api/`
        const config = {
            baseURL: baseURL,
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache',
                'Authorization': `Basic ${auth_token}`,
                'Pragma': 'no-cache'
            },
            validateStatus: function (status) {
                return status == 200; // TEI. not only create also update
            },
        }

        await axiosInstance.post(mapping[resource_type], payload, config)
        .then(function (response) {
            logger_upload.debug(`Response POST List ${resource_type}. url '${mapping[resource_type]}/'`);
            logger_upload.debug(JSON.stringify(response.data))
        })
        .catch(function (error) {
            logger_upload.error(`Response POST List ${resource_type}. url '${mapping[resource_type]}/'`);
            logger_upload.error(JSON.stringify(error.toJSON()))
            if (error.response !== undefined) {
                logger_upload.error(JSON.stringify(error.response.data))
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

        logger_upload.debug(JSON.stringify(payload))

        logger_upload.debug(`POST ?strategy=DELETE List of ${resource_type}. url '${mapping[resource_type]}/'`);
        const baseURL = `https://${endpointConfig.dhisServer}/api/`
        const config = {
            baseURL: baseURL,
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache',
                'Authorization': `Basic ${auth_token}`,
                'Pragma': 'no-cache'
            },
            validateStatus: function (status) {
                return status == 200; // TEI. not only create also update
            },
        }

        await axiosInstance.post(mapping[resource_type]+"?strategy=DELETE", payload, config)
        .then(function (response) {
            logger_upload.debug(`Response POST ?strategy=DELETE ${resource_type}. url '${mapping[resource_type]}/'`);
            logger_upload.debug(JSON.stringify(response.data))
        })
        .catch(function (error) {
            logger_upload.error(`Response POST ?strategy=DELETE ${resource_type}. url '${mapping[resource_type]}/'`);
            logger_upload.error(JSON.stringify(error.toJSON()))
            if (error.response !== undefined) {
                logger_upload.error(JSON.stringify(error.response.data))
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

        logger_upload.debug(`PUT ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
        const baseURL = `https://${endpointConfig.dhisServer}/api/`
        const config = {
            baseURL: baseURL,
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache',
                'Authorization': `Basic ${auth_token}`,
                'Pragma': 'no-cache'
            },
            validateStatus: function (status) {
                return status == 200;
            },
        }

        await axiosInstance.put(mapping[resource_type], payload, config)
        .then(function (response) {
            logger_upload.debug(`Response PUT ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.debug(JSON.stringify(response.data))
        })
        .catch(function (error) {
            logger_upload.error(`Response PUT ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.error(JSON.stringify(error.toJSON()))
            if (error.response !== undefined) {
                logger_upload.error(JSON.stringify(error.response.data))
            }
        });
    }

    async function delete_resource(resource_type, uid) {
        
        const mapping = {
            [TEI_TYPE]: "trackedEntityInstances/"+uid,
            [ENROLLMENT_TYPE]: "enrollments/"+uid,
            [EVENT_TYPE]: "events/"+uid
        }

        logger_upload.debug(`DELETE ${resource_type} ${uid}. url '${mapping[resource_type]}'`);

        const baseURL = `https://${endpointConfig.dhisServer}/api/`
        const config = {
            baseURL: baseURL,
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Cache-Control': 'no-cache',
                'Authorization': `Basic ${auth_token}`,
                'Pragma': 'no-cache'
            },
            validateStatus: function (status) {
                return status == 200;
            },
        }

        await axiosInstance.delete(mapping[resource_type], config)
        .then(function (response) {
            logger_upload.debug(`Response DELETE ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.debug(JSON.stringify(response.data))
        })
        .catch(function (error) {
            logger_upload.error(`Response DELETE ${resource_type} ${uid}. url '${mapping[resource_type]}'`);
            logger_upload.error(JSON.stringify(error.toJSON()))
            if (error.response !== undefined) {
                logger_upload.error(JSON.stringify(error.response.data))
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
        const TEI_fileName = `./GENERATED_data/${SOURCE_ID}/teis/${TEI_uid}.json`;
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