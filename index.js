var fs = require('fs');
const yargs = require('yargs');
const { logger, logger_fr } = require('./logger.js');
var _ = require('lodash');

//UIDs
const TEA_CODE_PATIENT = "dsWUbqvV9GW";

//Labels
const PROGRAM_TARV_LABEL_ENROLLMENT = "TARV_enrollment"
const PROGRAM_PTME_MERE_LABEL_ENROLLMENT = "PTME_MERE_enrollment"
const PROGRAM_PTME_ENFANT_LABEL_ENROLLMENT = "PTME_ENFANT_enrollment"

const PROGRAM_TARV = "TARV"
const PROGRAM_PTME_MERE = "PTME_MERE"
const PROGRAM_PTME_ENFANT = "PTME_ENFANT"

const PROGRAMSTAGE_TARV_PREMIER_DEBUT_ARV_LABEL = "PREMIER_DEBUT_ARV"
const PROGRAMSTAGE_TARV_TARV_LABEL = "TARV"
const PROGRAMSTAGE_TARV_CONSULTATION_LABEL = "CONSULTATION"
const PROGRAMSTAGE_TARV_DEBUT_TRAITEMENT_TB_LABEL = "DEBUT_TRAITEMENT_TB"
const PROGRAMSTAGE_TARV_PERDU_DE_VUE_LABEL = "PERDU_DE_VUE"
const PROGRAMSTAGE_TARV_SORTIE_LABEL = "SORTIE"

const PROGRAMSTAGE_PTME_MERE_ADMISSION_PTME_MERE_LABEL = "ADMISSION"
const PROGRAMSTAGE_PTME_MERE_PREMIERDEBUTARVPTME_LABEL = "PREMIERDEBUTARVPTME"
const PROGRAMSTAGE_PTME_MERE_DELIVERY_LABEL = "DELIVERY"
const PROGRAMSTAGE_PTME_MERE_SORTIE_LABEL = "SORTIE"

const PROGRAMSTAGE_PTME_ENFANT_ADMISSION_LABEL = "ADMISSION"
const PROGRAMSTAGE_PTME_ENFANT_PCR_INITIAL_LABEL = "PCR_INITIAL"
const PROGRAMSTAGE_PTME_ENFANT_PCR_SUIVI_LABEL = "PCR_SUIVI"
const PROGRAMSTAGE_PTME_ENFANT_SORTIE_LABEL = "SORTIE"


//Programs list
const program_labels = [PROGRAM_PTME_ENFANT_LABEL_ENROLLMENT, PROGRAM_PTME_MERE_LABEL_ENROLLMENT, PROGRAM_TARV_LABEL_ENROLLMENT];
const programs = [PROGRAM_PTME_ENFANT, PROGRAM_PTME_MERE, PROGRAM_TARV]; //Important: same program order for program_labels and programs

//Stages list for each program
const ENFANT_programStages = [
    PROGRAMSTAGE_PTME_ENFANT_ADMISSION_LABEL,
    PROGRAMSTAGE_PTME_ENFANT_PCR_INITIAL_LABEL,
    PROGRAMSTAGE_PTME_ENFANT_PCR_SUIVI_LABEL,
    PROGRAMSTAGE_PTME_ENFANT_SORTIE_LABEL
];
const MERE_programStages = [
    PROGRAMSTAGE_PTME_MERE_ADMISSION_PTME_MERE_LABEL,
    PROGRAMSTAGE_PTME_MERE_PREMIERDEBUTARVPTME_LABEL,
    PROGRAMSTAGE_PTME_MERE_DELIVERY_LABEL,
    PROGRAMSTAGE_PTME_MERE_SORTIE_LABEL
];
const TARV_programStages = [
    PROGRAMSTAGE_TARV_PREMIER_DEBUT_ARV_LABEL,
    PROGRAMSTAGE_TARV_TARV_LABEL,
    PROGRAMSTAGE_TARV_CONSULTATION_LABEL,
    PROGRAMSTAGE_TARV_DEBUT_TRAITEMENT_TB_LABEL,
    PROGRAMSTAGE_TARV_PERDU_DE_VUE_LABEL,
    PROGRAMSTAGE_TARV_SORTIE_LABEL
];

const PREVIOUS = "PREVIOUS";
const CURRENT = "CURRENT";

//Actions
const DELETE = "DELETE";
const UPDATE = "UPDATE";
const CREATE = "CREATE";

//Resource Types
const TEI_TYPE = "TEI";
const ENROLLMENT_TYPE = "Enrollment";
const TEA_TYPE = "TEA";
const EVENT_TYPE = "EVENT";
const DE_TYPE = "DE";

/**************************************/

/**
 * Command line tool configuration
 */
const argv = yargs
    .command('diff', 'Generate changes log from a given Organization Unit (Health Facility) and two Export Dates (previous data dump and current data dump)', {
        org_unit: {
            description: 'the Organization Unit code (e.g. 003BDI006S010120)',
            alias: ['o', 'ou'],
            type: 'text',
        },
        export_date_current: {
            description: 'the current export date (YYYY_MM_DD)',
            alias: ['ec', 'ecd'],
            type: 'text',
        },
    })
    .demandOption(['org_unit', 'export_date_current'], 'Please provide both Organization Unit code and export date arguments to work with this tool')
    .help()
    .alias('help', 'h')
    .argv;

/**************************************/

/**
 * Parsing arguments
 */
let SOURCE_OU_CODE;
let SOURCE_DATE_CURRENT;
if (argv._.includes('diff')) {
    SOURCE_OU_CODE = argv.org_unit;
    SOURCE_DATE_CURRENT = argv.export_date_current;
} else {
    process.exit(1)
}

/**************************************/

/**
 * Read input files
 */
const PATIENT_UID_FILE_PREVIOUS = "./DHIS2_data/" + SOURCE_OU_CODE + "/patient_code_uid.json";

if (!fs.existsSync(PATIENT_UID_FILE_PREVIOUS)) {
    logger.error(`ArgError; Patient_uid_file (${PATIENT_UID_FILE_PREVIOUS}) from previous data dump doesn't exist`)
} else {
    var previous_patient_code_uid = JSON.parse(fs.readFileSync(PATIENT_UID_FILE_PREVIOUS))
}

//const PATIENT_UID_FILE_CURRENT = SOURCE_DATE_CURRENT + "-" + SOURCE_OU_CODE + "-patient_code_uid.json";
const PATIENT_UID_FILE_CURRENT = "new_dump_additions-patient_code_uid.json";

if (!fs.existsSync(PATIENT_UID_FILE_CURRENT)) {
    logger.error(`ArgError; Patient_uid_file (${PATIENT_UID_FILE_CURRENT}) from current data dump doesn't exist`)
} else {
    var current_patient_code_uid = JSON.parse(fs.readFileSync(PATIENT_UID_FILE_CURRENT))
}

//Files
const TEIS_FILE = "./DHIS2_data/" + SOURCE_OU_CODE + "/teis.json";
var dhis_teis;
if (!fs.existsSync(TEIS_FILE)) {
    logger.error(`ArgError; TEIs file (${TEIS_FILE}) from dhis data doesn't exist`)
} else {
    dhis_teis = JSON.parse(fs.readFileSync(TEIS_FILE))
}

/**************************************************************************/

var listOfActions = [];

/**
 * TEIS
 */

var changed_TEIs = false;

//List of TEIs codepatient
var teis_toBeUpdated = [];
var teis_toBeCreated = [];
var teis_toBeDeleted = [];

var previousTEIs_PatientCodes = Object.keys(previous_patient_code_uid);
var currentTEIs_PatientCodes = Object.keys(current_patient_code_uid);

var missingTEIs_patientCodes = _.difference(previousTEIs_PatientCodes, currentTEIs_PatientCodes);
var newTEIs_patientCodes = _.difference(currentTEIs_PatientCodes, previousTEIs_PatientCodes);
var commonTEIs_patientCodes = _.intersection(previousTEIs_PatientCodes, currentTEIs_PatientCodes);
/*logger.info(`TEIs list from PREVIOUS data dump: ${previousTEIs_PatientCodes}`);
logger.info(`TEIs list from CURRENT data dump: ${currentTEIs_PatientCodes}`);*/

//Log missing TEIs
missingTEIs_patientCodes.forEach((TEI) => {
    changed_TEIs = true;
    logger.info(`TEI_DELETION; Patient ${TEI} (uid: ${previous_patient_code_uid[TEI].uid}) will be removed from DHIS2 server; Not present in new data dump`);
    teis_toBeDeleted.push(TEI);
    var dict = getAction_TEI(DELETE, TEI);
    dict.uid = previous_patient_code_uid[TEI].uid;
    listOfActions.push(dict);
});

//Log new TEIs
newTEIs_patientCodes.forEach((TEI) => {
    changed_TEIs = true;
    logger.info(`TEI_CREATION; Patient ${TEI} will be created in DHIS2 server; Not present in previous data dump`);
    teis_toBeCreated.push(TEI);
    var dict = getAction_TEI(CREATE, TEI);
    listOfActions.push(dict);
})

var teis_toUpdateTEA = [];
var teis_toUpdateEnroll = [];

commonTEIs_patientCodes.forEach((TEI) => {
    //logger.info(`TEI_REVIEW; Patient ${TEI} (uid: ${previous_patient_code_uid[TEI].uid}) will be reviewed; Present in previous data dump`);
    var changes = checkTEIDifference(TEI); //TODO: in process (cascade)
    changed_TEIs = changes.changed_TEI;
    if (changed_TEIs) {
        teis_toBeUpdated.push(TEI);
        if (changes.changed_TEA) {
            teis_toUpdateTEA.push(previous_patient_code_uid[TEI].uid);
        }
        if (changes.changed_enroll) {
            teis_toUpdateEnroll.push(TEI);
        }

    }
})

//Statistics
logger.info(`TEIs to be created: ${teis_toBeCreated.length} (${teis_toBeCreated})`);
logger.info(`TEIs to be deleted: ${teis_toBeDeleted.length} (${teis_toBeDeleted})`);
logger.info(`TEIs to be updated: ${teis_toBeUpdated.length} (${teis_toBeUpdated})`);
logger.info(`TEIs to be updated because of TEA difference: ${teis_toUpdateTEA.length} (${teis_toUpdateTEA})`);
logger.info(`TEIs to be updated because of enrollment difference: ${teis_toUpdateEnroll.length} (${teis_toUpdateEnroll})`);
logger.info(`Total TEIs (common + new - missing): ${commonTEIs_patientCodes.length + newTEIs_patientCodes.length - missingTEIs_patientCodes.length}`);
/**
 * WRITE actions to file
 */
const ACTIONS_LIST_FILE = "actions.json";
try {
    fs.writeFileSync(ACTIONS_LIST_FILE, JSON.stringify(listOfActions));
} catch (err) {
    // An error occurred
    logger.error(err);
}

//***********************************************************************************************/


/**
 * Read TEI files
 */
function readDUMP_TEIs(TEI_uid) {

    var TEI_file;
    const TEI_fileName = "./teis/" + SOURCE_OU_CODE + "_" + SOURCE_DATE_CURRENT + "/" + TEI_uid + ".json";
    if (!fs.existsSync(TEI_fileName)) {
        logger.error(`FileError; TEI file (${TEI_fileName}) (previous dump) doesn't exist`)
    } else {
        TEI_file = JSON.parse(fs.readFileSync(TEI_fileName))
    }

    return TEI_file;

}

function readDHIS_TEIs(TEI_uid) {

    var TEI_data = dhis_teis.filter((TEI) => {
        if (TEI.trackedEntityInstance == TEI_uid) {
            return TEI;
        }
    });

    if (TEI_data.length > 1) { //Misma TEI aparece varias veces en teis.json (de DHIS2) si está enrolled en varios programs
        var enrollments = [];
        var enrollment_uids = [];
        TEI_data.forEach((TEI) => {
            TEI.enrollments.forEach((enroll) => {
                if (!enrollment_uids.includes(enroll.enrollment)){
                    enrollments.push(enroll);
                    enrollment_uids.push(enroll.enrollment);
                }
            })
        });
        TEI_data[0].enrollments = enrollments; //Todos los enrollments en un solo TEI obj.
    }
    return TEI_data[0];
}

/**
 * Given a patient already in DHIS, check the differences in the data
 * Logs the changes (remain the same or will be updated in the server)
 * @param {*} codepatient 
 */
function checkTEIDifference(codepatient) {

    var changed_TEA = false;
    var changed_enroll = false;

    //TEI uids
    var dhisTEI_uid = previous_patient_code_uid[codepatient].uid;
    var newTEI_uid = current_patient_code_uid[codepatient].uid;

    //DHIS uploaded file
    //var dhisTEI_file = readTEIFile(dhisTEI_uid, PREVIOUS);readDHIS_TEIs
    var dhisTEI_file = readDHIS_TEIs(dhisTEI_uid);

    //New dump file
    var newTEI_file = readDUMP_TEIs(newTEI_uid);

    if (typeof dhisTEI_file !== "undefined" && typeof newTEI_file !== "undefined") {

        /**
         * ATTRIBUTES (TEA)
         */
        changed_TEA = checkTEAexistence(codepatient, dhisTEI_file, newTEI_file, previous_patient_code_uid);

        /**
         * ENROLLMENTS
         */
        programs.forEach((program) => {
            changed_enroll = checkEnrollmentExistence(dhisTEI_file, newTEI_file, codepatient, program, previous_patient_code_uid, current_patient_code_uid);
        });

    } else {
        logger.error(`FileError; Process stopped because of a missing file`)
    }

    var changed = {};
    changed.changed_TEI = false;
    if (changed_TEA) {
        changed.changed_TEI = true;
        changed.changed_TEA = changed_TEA;
    }
    if (changed_enroll) {
        changed.changed_TEI = true;
        changed.changed_enroll = changed_enroll;
    }

    return changed;
}

/**
 * Checks the existence of a TEA in the new data dump
 * 
 * @param {*} codepatient 
 * @param {*} dhisTEI_file 
 * @param {*} newTEI_file 
 * @param {*} previous_patient_code_uid 
 */
function checkTEAexistence(codepatient, dhisTEI_file, newTEI_file, previous_patient_code_uid) {

    var changed_missing = false;
    var changed_new = false;
    var changed_common = false;

    //TEAs arrays
    var dhisTEAs_data = dhisTEI_file.attributes;
    var newTEAs_data = newTEI_file.attributes;

    //TEAs UIDs arrays
    var dhisTEAs_uids = [];
    dhisTEAs_data.forEach(attr => {
        dhisTEAs_uids.push(attr.attribute)
    })

    var newTEAs_uids = [];
    newTEAs_data.forEach(attr => {
        newTEAs_uids.push(attr.attribute)
    })

    //Comparison
    var missingTEAs = _.difference(dhisTEAs_uids, newTEAs_uids);
    /*
    DEBUG ONLY
    if (missingTEAs.length != 0) {
        console.log("Missing TEAs:" + missingTEAs + "de la TEI: " + dhisTEI_file.trackedEntityInstance)
    }*/

    var newTEAs = _.difference(newTEAs_uids, dhisTEAs_uids);
    var commonTEAs = _.intersection(dhisTEAs_uids, newTEAs_uids);

    missingTEAs.forEach((TEA) => {
        changed_missing = true;
        logger.info(`TEA_DELETION; TEA ${TEA} will be removed from patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
        var dict = getAction_TEA(DELETE, previous_patient_code_uid[codepatient].uid, TEA, dhisTEAs_data[dhisTEAs_uids.indexOf(TEA)].value);
        listOfActions.push(dict);
    });

    newTEAs.forEach((TEA) => {
        changed_new = true;
        logger.info(`TEA_CREATION; TEA ${TEA} will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);
        var dict = getAction_TEA(CREATE, previous_patient_code_uid[codepatient].uid, TEA, newTEAs_data[newTEAs_uids.indexOf(TEA)].value);
        listOfActions.push(dict);
    });

    commonTEAs.forEach((TEA) => {
        //logger.info(`TEA_REVIEW; TEA ${TEA} will be reviewed for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Present in previous data dump`);

        //Skip TEA_CODE_PATIENT (will always be the same at this point)
        if (TEA != TEA_CODE_PATIENT) {
            if (changed_common) {
                checkTEADifference(TEA, codepatient, dhisTEAs_data[dhisTEAs_uids.indexOf(TEA)], newTEAs_data[newTEAs_uids.indexOf(TEA)]);
            } else {
                changed_common = checkTEADifference(TEA, codepatient, dhisTEAs_data[dhisTEAs_uids.indexOf(TEA)], newTEAs_data[newTEAs_uids.indexOf(TEA)]);

            }
        }
    });

    return (changed_common || changed_missing || changed_new);
}

/**
 * Given a TEA check the differences with the new data
 * Logs the changes (remain the same or will be updated in the server)
 * @param {*} TEA 
 * @param {*} codepatient 
 * @param {*} dhisTEA TEA object : {
      "attribute": "dsWUbqvV9GW",
      "value": "003BDI017S020203001422",
      "storedBy": "SidaInfoIntegrator"
    }
 * @param {*} newTEA
 */
function checkTEADifference(TEA, codepatient, dhisTEA, newTEA) {

    var updated_TEA = false;
    //Has same value then log and do nothing
    //Has different value then UPDATE
    var valueDHIS = dhisTEA.value;
    var valueNew = newTEA.value;
    if (valueNew === true) {
        valueNew = "true";
    }

    if (valueDHIS === valueNew) {
        //logger.info(`TEA_INFO; TEA ${TEA} won't be updated for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; It has the same value as the previous dump`)
    } else {
        updated_TEA = true;
        logger.info(`TEA_UPDATE; TEA ${TEA} will be updated for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Previous value: ${valueDHIS} , New value: ${valueNew}`)
        var dict = getAction_TEA(UPDATE, previous_patient_code_uid[codepatient].uid, TEA, valueDHIS, valueNew);
        listOfActions.push(dict);
    }

    return updated_TEA;
}

/**
 * Checks the existence of an Enrollment in the new data dump
 * 
 */
function checkEnrollmentExistence(dhisTEI_file, newTEI_file, codepatient, program, previous_patient_code_uid, current_patient_code_uid) {
    var changed_enroll_missing = false;
    var changed_enroll_new = false;
    var changed_enroll_common = false;

    var programIndex = programs.indexOf(program);
    var programLabel = program_labels[programIndex];
    if (programLabel in previous_patient_code_uid[codepatient]) {
        if (programLabel in current_patient_code_uid[codepatient]) {

            var previousEnrollment_dates = Object.keys(previous_patient_code_uid[codepatient][programLabel]) //Array of dates('2019-11-18') 
            var currentEnrollment_dates = Object.keys(current_patient_code_uid[codepatient][programLabel])

            var missingEnrollments = _.difference(previousEnrollment_dates, currentEnrollment_dates);
            var newEnrollments = _.difference(currentEnrollment_dates, previousEnrollment_dates);
            var commonEnrollments = _.intersection(previousEnrollment_dates, currentEnrollment_dates);

            if (missingEnrollments.length != 0) { //Some enrollments are missing in the current dump
                changed_enroll_missing = true;
                missingEnrollments.forEach((enrollment_date) => {
                    //if (previous_patient_code_uid[codepatient][programLabel][enrollment_date] in ["gLIFm0XojDB", "QRXEs6OJEDE", "kFzWdrL2yRb"]) {
                    logger.info(`Enrollment_DELETION; ${program} (${previous_patient_code_uid[codepatient][programLabel][enrollment_date]}) (${enrollment_date}) will be removed from patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
                    var dict = {};
                    dict.action = DELETE;
                    dict.resource = previous_patient_code_uid[codepatient][programLabel][enrollment_date]; //Enrollment_uid
                    dict.type = ENROLLMENT_TYPE;
                    dict.TEI = previous_patient_code_uid[codepatient].uid;
                    dict.program = program;
                    //dict.value = enrollment_date;
                    listOfActions.push(dict);
                });
            }
            if (newEnrollments.length != 0) { //There are new enrollments in the current dump
                changed_enroll_new = true;
                newEnrollments.forEach((enrollment_date) => {
                    logger.info(`Enrollment_CREATION; ${program} (${enrollment_date}) will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);
                    var dict = {};
                    dict.action = CREATE;
                    dict.type = ENROLLMENT_TYPE;
                    dict.TEI = previous_patient_code_uid[codepatient].uid;
                    dict.program = program;
                    dict.value = enrollment_date;
                    dict.status = getEnrollmentStatus(newTEI_file.enrollments, current_patient_code_uid[codepatient][programLabel][enrollment_date]);
                    listOfActions.push(dict);
                });
            }

            if (commonEnrollments.length != 0) { //Some enrollments are present in both dumps
                commonEnrollments.forEach((enrollment_date) => {
                    //logger.info(`Enrollment_REVIEW; Enrollment ${previous_patient_code_uid[codepatient][program][enrollment_date]} (${program})
                    //will be reviewed for patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server;
                    //Present in previous data dump`);

                    if (changed_enroll_common) {
                        checkEnrollmentDifference(dhisTEI_file, newTEI_file, enrollment_date, codepatient, programLabel, previous_patient_code_uid, current_patient_code_uid);
                    } else {
                        changed_enroll_common = checkEnrollmentDifference(dhisTEI_file, newTEI_file, enrollment_date, codepatient, programLabel, previous_patient_code_uid, current_patient_code_uid);

                    }

                });
            }
        } else { //enrollment in previous dump but not in current dump (same as missing enrollment)
            changed_enroll_missing = true;
            logger.info(`Enrollment_DELETION; ${program} ${(Object.values(previous_patient_code_uid[codepatient][programLabel]))} and all its associated events will be removed from patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
            var dict = {};
            dict.action = DELETE;
            dict.resource = Object.values(previous_patient_code_uid[codepatient][programLabel]); //Enrollment_uid
            dict.type = ENROLLMENT_TYPE;
            dict.TEI = previous_patient_code_uid[codepatient].uid;
            dict.program = program;
            //dict.value = enrollment_date;
            listOfActions.push(dict);
        }
    } else {
        if (programLabel in current_patient_code_uid[codepatient]) { //enrollment in current dump but not in previous dump (same as new enrollment)
            changed_enroll_new = true;
            logger.info(`Enrollment_CREATION; ${programLabel} will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);

            //var currentEvents_dates = Object.keys(current_patient_code_uid[codepatient][program]);
            var currentEnrollment_uids = Object.values(current_patient_code_uid[codepatient][programLabel])
            var currentEnrollment_dates = Object.keys(current_patient_code_uid[codepatient][programLabel])
            var current_enrollment_keys = [];
            currentEnrollment_uids.forEach((uid) => {
                var date = currentEnrollment_dates[currentEnrollment_uids.indexOf(uid)];
                current_enrollment_keys.push([program] + "-" + uid);
                var dict = {};
                dict.action = CREATE;
                dict.type = ENROLLMENT_TYPE;
                //dict.value = uid; //Para identificar los eventos que hay que crear asociados a cada enrollment
                dict.value = date;
                dict.TEI = previous_patient_code_uid[codepatient].uid;
                dict.program = program;
                dict.status = getEnrollmentStatus(newTEI_file.enrollments, current_patient_code_uid[codepatient][programLabel][date]);
                //var enrollmentEvents = [];
                if (current_enrollment_keys.length != 0) { //There are new events for that stage in the current dump
                    current_enrollment_keys.forEach((key) => {
                        //Para cada enrollment iterar sobre sus eventos, leerlos del nuevo dump, y crearlos con los mismo datos
                        var stages = Object.keys(current_patient_code_uid[codepatient][key]);

                        stages.forEach((stage) => {

                            var events = Object.keys(current_patient_code_uid[codepatient][key][stage]);

                            events.forEach((date) => {
                                logger.info(`Event_CREATION; ${[program]} event on ${date} for a new enrollment will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);
                                //How to point to the newly created enrollment?

                                var event = {};
                                event.action = CREATE;
                                event.resource = date; //Event UID
                                event.type = EVENT_TYPE;
                                event.TEI = previous_patient_code_uid[codepatient].uid;
                                event.stage = stage;
                                event.eventData = getEventData(newTEI_file.enrollments, uid, date);
                                //enrollmentEvents.push(event);
                                listOfActions.push(event);

                            })
                        })

                    });
                    //dict.enrollmentEvents = enrollmentEvents;
                    listOfActions.push(dict);
                }
            })
        }
    }
    return (changed_enroll_new || changed_enroll_missing || changed_enroll_common);
}

function getEventData(enrollmentData, enrollmentUID, eventDate) {

    var enroll = enrollmentData.filter((enroll) => {
        if (enroll.enrollment == enrollmentUID) {
            return enroll;
        }
    });
    var event = enroll[0].events.filter(function (entry) {
        if (entry.eventDate == eventDate) {
            return entry;
        }
    });

    return event;
}


function checkEnrollmentDifference(dhisTEI_file, newTEI_file, enrollment_date, codepatient, programLabel, previous_patient_code_uid, current_patient_code_uid) {

    var changed = false;
    //Check enrollment events existence for each programStage
    var programStages = [];
    var dhis_enrollment_key = "";
    var current_enrollment_key = "";
    var enrollment_uid_previous = previous_patient_code_uid[codepatient][programLabel][enrollment_date];
    var enrollment_uid_current = current_patient_code_uid[codepatient][programLabel][enrollment_date];
    if (programLabel == PROGRAM_PTME_ENFANT_LABEL_ENROLLMENT) {
        //Assign corresponding programStages
        programStages = ENFANT_programStages;
        //Build Enrollment Keys (eg. "PTME_ENFANT-scWpqiXLR5u")
        dhis_enrollment_key = PROGRAM_PTME_ENFANT + "-" + enrollment_uid_previous;
        current_enrollment_key = PROGRAM_PTME_ENFANT + "-" + enrollment_uid_current;

    } else if (programLabel == PROGRAM_PTME_MERE_LABEL_ENROLLMENT) {
        programStages = MERE_programStages;
        dhis_enrollment_key = PROGRAM_PTME_MERE + "-" + enrollment_uid_previous;
        current_enrollment_key = PROGRAM_PTME_MERE + "-" + enrollment_uid_current;

    } else if (programLabel == PROGRAM_TARV_LABEL_ENROLLMENT) {
        programStages = TARV_programStages;
        dhis_enrollment_key = PROGRAM_TARV + "-" + enrollment_uid_previous;
        current_enrollment_key = PROGRAM_TARV + "-" + enrollment_uid_current;
    }


    /**
    * PROGRAM STAGES
    */
    programStages.forEach((stage) => {
        if (changed) {
            checkStageEvents(programLabel, codepatient, stage, dhis_enrollment_key, current_enrollment_key, enrollment_uid_previous, enrollment_uid_current, previous_patient_code_uid, current_patient_code_uid);
        } else {
            changed = checkStageEvents(programLabel, codepatient, stage, dhis_enrollment_key, current_enrollment_key, enrollment_uid_previous, enrollment_uid_current, previous_patient_code_uid, current_patient_code_uid);

        }
    })


    /**
     * ENROLLMENT STATUS
     */
    var enrollments_current = newTEI_file.enrollments;
    var enrollments_previous = dhisTEI_file.enrollments;

    /***** Extract enrollment status ******/

    var status_current = getEnrollmentStatus(enrollments_current, enrollment_uid_current);
    var status_previous = getEnrollmentStatus(enrollments_previous, enrollment_uid_previous);

    if (status_current.toUpperCase() != status_previous.toUpperCase()) {
        changed = true;
        logger.info(`Enrollment_UPDATE; Enrollment status will be updated for patient ${codepatient} (enrollment_uid: ${enrollment_uid_previous}) in DHIS2 server; Previous value: ${status_previous} , New value: ${status_current}`)
        var dict = {};
        dict.action = UPDATE;
        dict.resource = previous_patient_code_uid[codepatient][programLabel][enrollment_date]; //Enrollment_uid
        dict.type = ENROLLMENT_TYPE;
        dict.TEI = previous_patient_code_uid[codepatient].uid;
        dict.program = programLabel;
        dict.previousValue = status_previous;
        dict.currentValue = status_current;
        listOfActions.push(dict);
    }

    return changed;

}

function getEnrollmentStatus(enrollmentsData, enrollmentUID) {
    //Enrollments UIDs array
    var enrollments_uids = [];
    enrollmentsData.forEach(enroll => {
        enrollments_uids.push(enroll.enrollment);
    });

    return enrollmentsData[enrollments_uids.indexOf(enrollmentUID)].status;
}

/**
 * Given a program stage get the events it contains for a given patient 
 * and check the differences with the new data
 * Logs the changes (remain the same or will be updated in the server)
 * @param {*} stage 
 * @param {*} codepatient 
 * @param {*} dhis_enrollment_key Program_stage_label + "-" + enrollment_uid
 * @param {*} current_enrollment_key Program_stage_label + "-" + enrollment_uid
 * @param {*} previous_patient_code_uid 
 * @param {*} current_patient_code_uid 
 */
function checkStageEvents(programLabel, codepatient, stage, dhis_enrollment_key, current_enrollment_key, enrollment_uid_previous, enrollment_uid_current, previous_patient_code_uid, current_patient_code_uid) {
    var changed_missing = false;
    var changed_new = false;
    var changed_common = false;
    if (typeof previous_patient_code_uid[codepatient][dhis_enrollment_key] !== "undefined") { //There are events associated to that enrollment in the previous file

        if (typeof current_patient_code_uid[codepatient][current_enrollment_key] !== "undefined") {
            if (stage in previous_patient_code_uid[codepatient][dhis_enrollment_key]) { //Stage exist in previous file enrollment
                if (stage in current_patient_code_uid[codepatient][current_enrollment_key]) {//Stage exist in current file enrollment

                    /**
                    * EVENTS
                    */
                    var previousEvents_dates = Object.keys(previous_patient_code_uid[codepatient][dhis_enrollment_key][stage]);
                    var currentEvents_dates = Object.keys(current_patient_code_uid[codepatient][current_enrollment_key][stage]);

                    var missingEvents = _.difference(previousEvents_dates, currentEvents_dates);
                    var newEvents = _.difference(currentEvents_dates, previousEvents_dates);
                    var commonEvents = _.intersection(previousEvents_dates, currentEvents_dates);

                    if (missingEvents.length != 0) { //Some events for that stage are missing in the current dump
                        changed_missing = true;
                        missingEvents.forEach((event_date) => {
                            logger.info(`Event_DELETION; ${stage} event (${previous_patient_code_uid[codepatient][dhis_enrollment_key][stage][event_date]}) on ${event_date} will be removed from patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
                            var dict = {};
                            dict.action = DELETE;
                            dict.resource = previous_patient_code_uid[codepatient][dhis_enrollment_key][stage][event_date]; //Event UID
                            dict.type = EVENT_TYPE;
                            dict.TEI = previous_patient_code_uid[codepatient].uid;
                            dict.enrollment = enrollment_uid_previous;
                            dict.stage = stage;
                            listOfActions.push(dict);
                        });
                    }
                    if (newEvents.length != 0) { //There are new events for that stage in the current dump
                        changed_new = true;
                        newEvents.forEach((event_date) => {
                            logger.info(`Event_CREATION; ${[stage]} event on ${event_date} will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);
                            var dict = {};
                            dict.action = CREATE;
                            dict.resource = event_date; //Event UID
                            dict.type = EVENT_TYPE;
                            dict.TEI = previous_patient_code_uid[codepatient].uid;
                            dict.enrollment = enrollment_uid_previous;
                            dict.stage = stage;
                            //TODO: dict.eventData = ;
                            listOfActions.push(dict);
                        });
                    }

                    if (commonEvents.length != 0) { //Some events for that stage are present in both dumps
                        commonEvents.forEach((event_date) => {
                            //logger.info(`Event_REVIEW; Event ${previous_patient_code_uid[codepatient][program][event_date]} (${program})
                            //will be reviewed for patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server;
                            //Present in previous data dump`);

                            var dhisTEI_file = readDHIS_TEIs(previous_patient_code_uid[codepatient].uid);
                            var newTEI_file = readDUMP_TEIs(current_patient_code_uid[codepatient].uid);

                            var previousEvent_uid = previous_patient_code_uid[codepatient][dhis_enrollment_key][stage][event_date];
                            var currentEvent_uid = current_patient_code_uid[codepatient][current_enrollment_key][stage][event_date];

                            /**
                            * DATA VALUES
                            */
                            var dhis_dataValues = getDataValues(dhisTEI_file, enrollment_uid_previous, previousEvent_uid);
                            var new_dataValues = getDataValues(newTEI_file, enrollment_uid_current, currentEvent_uid);

                            if (changed_common) {
                                checkDataValuesExistence(enrollment_uid_previous, codepatient, previous_patient_code_uid[codepatient].uid, event_date, stage, dhis_dataValues, new_dataValues, previousEvent_uid);
                            } else {
                                changed_common = checkDataValuesExistence(enrollment_uid_previous, codepatient, previous_patient_code_uid[codepatient].uid, event_date, stage, dhis_dataValues, new_dataValues, previousEvent_uid);
                                //Solo log, las actions va a un nivel más bajo (DE)
                                /*if (changed_common) {
                                    var dict = {};
                                    dict.action = UPDATE;
                                    dict.resource = event_date; //Event UID
                                    dict.type = EVENT_TYPE;
                                    dict.TEI = previous_patient_code_uid[codepatient].uid;
                                    dict.enrollment = enrollment_uid_previous;
                                    dict.stage = stage;
                                    //TODO: dict.eventData = ;
                                    listOfActions.push(dict);
                                }*/
                            }

                            //TODO: what else is there to compare from an event data apart from its dataValues?
                            //here

                        });
                    }


                } else { //Stage doesn't exist in current file enrollment
                    changed_missing = true;
                    logger.info(`Stage_DELETION; ${[stage]} stage from enrollment (${enrollment_uid_current}) and all its associated events will be removed for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
                    var events = Object.keys(previous_patient_code_uid[codepatient][dhis_enrollment_key][stage]);
                    //For each event on that stage
                    if (typeof events !== "undefined") {
                        events.forEach((event_date) => {
                            logger.info(`Event_DELETION; ${stage} event (${previous_patient_code_uid[codepatient][dhis_enrollment_key][stage][event_date]}) on ${event_date} will be removed from patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
                            var dict = {};
                            dict.action = DELETE;
                            dict.resource = previous_patient_code_uid[codepatient][dhis_enrollment_key][stage][event_date]; //Event UID
                            dict.type = EVENT_TYPE;
                            dict.TEI = previous_patient_code_uid[codepatient].uid;
                            dict.enrollment = enrollment_uid_previous;
                            dict.stage = stage;
                            listOfActions.push(dict);
                        });
                    }
                }
            } else { //Stage doesn't exist in previous file enrollment
                if (stage in current_patient_code_uid[codepatient][current_enrollment_key]) {
                    changed_new = true;
                    Object.keys(current_patient_code_uid[codepatient][current_enrollment_key][stage]).forEach((date) => {
                        logger.info(`Event_CREATION; ${[stage]} event on ${date} will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);
                        var dict = {};
                        dict.action = CREATE;
                        dict.resource = date; //Event date
                        dict.type = EVENT_TYPE;
                        dict.TEI = previous_patient_code_uid[codepatient].uid;
                        dict.enrollment = enrollment_uid_previous;
                        dict.stage = stage;
                        //TODO: dict.eventData = ;
                        listOfActions.push(dict);
                    })

                }
            }
        } else { //No events associated to that enrollment in the current file
            changed_missing = true;
            logger.info(`Enrollment_DELETION; ${[enrollment_uid_current]} enrollment and all its associated events will be removed for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
            var dict = {};
            dict.action = DELETE;
            dict.resource = enrollment_uid_current; //Enrollment_uid
            dict.type = ENROLLMENT_TYPE;
            dict.TEI = previous_patient_code_uid[codepatient].uid;
            dict.program = programLabel;
            listOfActions.push(dict);
        }



    } else { //No events associated to that enrollment in the previous file
        if (typeof current_patient_code_uid[codepatient][current_enrollment_key] !== "undefined") { //There weren't before, there are now
            if (stage in current_patient_code_uid[codepatient][current_enrollment_key]) {//Stage exist in current file enrollment

                var currentEvents_dates = Object.keys(current_patient_code_uid[codepatient][current_enrollment_key][stage]);

                if (currentEvents_dates.length != 0) { //There are new events for that stage in the current dump
                    currentEvents_dates.forEach((event_date) => {
                        changed_new = true;
                        logger.info(`Event_CREATION; ${[stage]} event on ${event_date} will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);
                        var dict = {};
                        dict.action = CREATE;
                        dict.resource = event_date; //Event UID
                        dict.type = EVENT_TYPE;
                        dict.TEI = previous_patient_code_uid[codepatient].uid;
                        dict.enrollment = enrollment_uid_previous;
                        dict.stage = stage;
                        //TODO: dict.eventData = ;
                        listOfActions.push(dict);
                    });
                }
            } else {
                logger.error(`Data_Error; There is an empty enrollment generated (${current_enrollment_key}) for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in the new data dump; Review SIDAInfo-DHIS2 script`);

            }


        } else { //No events before and no events now
            //Do nothing
        }
    }

    return (changed_common || changed_missing || changed_new);
}

function getDataValues(TEI_file, enrollment_uid, event_uid) {
    /***************** Read event ****************/
    /***** Extract enrollment info ******/
    //Enrollments array
    var enrollments_data = TEI_file.enrollments;

    //Enrollments UIDs array
    var enrollments_uids = [];
    enrollments_data.forEach(enroll => {
        enrollments_uids.push(enroll.enrollment);
    });

    /***** Extract events info ******/
    //Events array
    var events = enrollments_data[enrollments_uids.indexOf(enrollment_uid)].events;

    //Events UIDs arrays
    var events_uids = [];
    events.forEach(event => {
        events_uids.push(event.event);
    });

    /***** Extract dataValues info ******/
    //Event data
    var event = events[events_uids.indexOf(event_uid)];

    return event.dataValues;
}

/**
 * Given an event check the differences with its datavalues with the new data
 * Logs the changes (remain the same or will be updated in the server)
 */
function checkDataValuesExistence(enrollment_uid_previous, codepatient, patient_uid, event_date, stage, dhis_dataValues, new_dataValues, previousEvent_uid) {

    var changed_missing = false;
    var changed_new = false;
    var changed_common = false;
    //For each event
    //Check DE existence
    if (typeof dhis_dataValues !== "undefined") { //Event had dataValues in previous dump
        if (typeof new_dataValues !== "undefined") { //Event has dataValues in new dump

            //DEs UIDs arrays
            var dhisDEs_uids = [];
            dhis_dataValues.forEach(DE => {
                dhisDEs_uids.push(DE.dataElement)
            })

            var newDEs_uids = [];
            new_dataValues.forEach(DE => {
                newDEs_uids.push(DE.dataElement)
            })

            var missingDEs = _.difference(dhisDEs_uids, newDEs_uids);
            var newDEs = _.difference(newDEs_uids, dhisDEs_uids);
            var commonDEs = _.intersection(dhisDEs_uids, newDEs_uids);

            if (missingDEs.length != 0) { //Some DEs for that event are missing in the current dump
                missingDEs.forEach((DE) => {
                    changed_missing = true;
                    logger.info(`DE_DELETION; ${DE} dataElement will be removed for event ${previousEvent_uid} (${event_date}) , ${stage} stage, patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
                    var dict = {};
                    dict.action = DELETE;
                    dict.resource = DE; //DE UID
                    dict.type = DE_TYPE;
                    dict.TEI = previous_patient_code_uid[codepatient].uid;
                    dict.enrollment = enrollment_uid_previous;
                    dict.stage = stage;
                    dict.event = previousEvent_uid;
                    listOfActions.push(dict);
                });
            }
            if (newDEs.length != 0) { //There are new DEs for that event in the current dump
                newDEs.forEach((DE) => {
                    changed_new = true;
                    logger.info(`DE_CREATION; ${DE} dataElement for event ${previousEvent_uid} (${event_date}), ${stage} stage, will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);
                    var dict = {};
                    dict.action = CREATE;
                    dict.resource = DE; //DE UID
                    dict.type = DE_TYPE;
                    dict.TEI = previous_patient_code_uid[codepatient].uid;
                    dict.enrollment = enrollment_uid_previous;
                    dict.stage = stage;
                    dict.event = previousEvent_uid;
                    dict.dataValue = new_dataValues[newDEs_uids.indexOf(DE)].value;
                    listOfActions.push(dict);
                });
            }

            if (commonDEs.length != 0) { //Some events for that stage are present in both dumps
                commonDEs.forEach((DE) => {
                    if (changed_common) {
                        checkDataValueDifference(previousEvent_uid, DE, codepatient, patient_uid, dhis_dataValues[dhisDEs_uids.indexOf(DE)], new_dataValues[newDEs_uids.indexOf(DE)]);
                    } else {
                        changed_common = checkDataValueDifference(previousEvent_uid, DE, codepatient, patient_uid, dhis_dataValues[dhisDEs_uids.indexOf(DE)], new_dataValues[newDEs_uids.indexOf(DE)]);

                    }
                })

            }

        } else { //Event doesn't have dataValues in new dump
            dhis_dataValues.forEach((DE) => {
                changed_missing = true;
                logger.info(`DE_DELETION; ${DE.dataElement} dataElement with value "${DE.value}" will be removed for event ${previousEvent_uid} (${event_date}) , ${stage} stage, patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
                var dict = {};
                dict.action = DELETE;
                dict.resource = DE.dataElement; //DE UID
                dict.type = DE_TYPE;
                dict.TEI = previous_patient_code_uid[codepatient].uid;
                dict.enrollment = enrollment_uid_previous;
                dict.stage = stage;
                dict.event = previousEvent_uid;
                listOfActions.push(dict);
            });
        }

    } else { //Event didn't have dataValues in previous dump
        if (typeof new_dataValues !== "undefined") { //Event didn't have dataValues before but has now (in new dump)
            var newDEs_uids = [];
            new_dataValues.forEach(DE => {
                newDEs_uids.push(DE.dataElement)
            });
            new_dataValues.forEach((DE) => {
                changed_new = true;
                logger.info(`DE_CREATION; ${DE.dataElement} dataElement with value (${DE.value}) for event ${previousEvent_uid} (${event_date}), ${stage} stage, will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);
                var dict = {};
                dict.action = CREATE;
                dict.resource = DE.dataElement; //DE UID
                dict.type = DE_TYPE;
                dict.TEI = previous_patient_code_uid[codepatient].uid;
                dict.enrollment = enrollment_uid_previous;
                dict.stage = stage;
                dict.event = previousEvent_uid;
                dict.dataValue = new_dataValues[newDEs_uids.indexOf(DE.dataElement)].value;
                listOfActions.push(dict);
            });

        } else { //Event doesn't have dataValues in previous nor current dump
            //do nothing
        }
    }

    return (changed_common || changed_new || changed_missing);
}

/**
 * Given a DE check the differences with the new data
 * Logs the changes (remain the same or will be updated in the server)
 * @param {*} DE 
 * @param {*} codepatient 
 * @param {*} dhisDE dataValue object : {dataElement: 'a3WwFDKNfQH', value: '2'}
 * @param {*} newDE
 */
function checkDataValueDifference(previousEvent_uid, DE, codepatient, patient_uid, dhisDE, newDE) {

    var changed = false;

    //Has same value then log and do nothing
    //Has different value then UPDATE
    var valueDHIS = dhisDE.value;
    var valueNew = newDE.value;
    if(valueNew === true) {
        valueNew = "true";
    }

    if (typeof valueDHIS !== "undefined") { //DE has value in previous dump
        if (typeof valueNew !== "undefined") { //DE has value in current dump
            //compare values
            if (valueDHIS == valueNew) {
                //logger.info(`DE_INFO; DE ${DE} won't be updated for patient ${codepatient} (uid: ${patient_uid}) in DHIS2 server; It has the same value as the previous dump`)
            } else {
                changed = true;
                logger.info(`DE_UPDATE; DE ${DE} will be updated for patient ${codepatient} (uid: ${patient_uid}) in DHIS2 server; Previous value: ${valueDHIS} , New value: ${valueNew}`)
                var dict = {};
                dict.action = UPDATE;
                dict.resource = DE; //DE UID
                dict.type = DE_TYPE;
                dict.TEI = previous_patient_code_uid[codepatient].uid;
                dict.event = previousEvent_uid;
                dict.previousValue = valueDHIS;
                dict.currentValue = valueNew;
                listOfActions.push(dict);
            }
        } else { //DE without value in current dump
            changed = true;
            logger.info(`DE_UPDATE; DE ${DE} will be updated for patient ${codepatient} (uid: ${patient_uid}) in DHIS2 server; Previous value: ${valueDHIS} , New value: NO VALUE`)
            var dict = {};
            dict.action = UPDATE;
            dict.resource = DE; //DE UID
            dict.type = DE_TYPE;
            dict.TEI = previous_patient_code_uid[codepatient].uid;
            dict.event = previousEvent_uid;
            dict.previousValue = valueDHIS;
            dict.currentValue = "";
            listOfActions.push(dict);
        }
    } else { //DE without value in previous dump
        if (typeof valueNew !== "undefined") { //DE has value in current dump
            changed = true;
            logger.info(`DE_UPDATE; DE ${DE} will be updated for patient ${codepatient} (uid: ${patient_uid}) in DHIS2 server; Previous value: NO VALUE , New value: ${valueNew}`)
            var dict = {};
            dict.action = UPDATE;
            dict.resource = DE; //DE UID
            dict.type = DE_TYPE;
            dict.TEI = previous_patient_code_uid[codepatient].uid;
            dict.event = previousEvent_uid;
            dict.previousValue = "";
            dict.currentValue = valueNew;
            listOfActions.push(dict);
        } else { //DE without value in current dump
            //keep the same, do nothing
        }
    }

    return changed;
}

/****************** ACTIONS FUNCTIONS **********************/

function getAction_TEI(action, TEI) {
    var dict = {};
    dict.action = action;
    dict.resource = TEI;
    dict.type = TEI_TYPE;
    return dict;
}

function getAction_TEA(action, TEI, TEA, previousValue, currentValue) {
    var dict = {};
    dict.action = action;
    dict.resource = TEA;
    dict.type = TEA_TYPE;
    dict.TEI = TEI;
    dict.previousValue = previousValue;
    dict.currentValue = currentValue;
    return dict;
}
