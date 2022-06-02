var fs = require('fs');
const { logger_diff } = require('./logger.js');
var _ = require('lodash');
const utils = require('./utils.js');

//UIDs
const TEA_CODE_PATIENT = "dsWUbqvV9GW";

//Labels
const PROGRAM_TARV_LABEL_ENROLLMENT = "TARV_enrollment"
const PROGRAM_PTME_MERE_LABEL_ENROLLMENT = "PTME_MERE_enrollment"
const PROGRAM_PTME_ENFANT_LABEL_ENROLLMENT = "PTME_ENFANT_enrollment"

const PROGRAM_TARV = "TARV"
const PROGRAM_PTME_MERE = "PTME_MERE"
const PROGRAM_PTME_ENFANT = "PTME_ENFANT"

const PROGRAMS_MAPPING = {
    "TARV": "e3swbbSnbQ2", 
    "PTME_MERE": "MVooF5iCp8L", 
    "PTME_ENFANT": "PiXg9cX1i0y"
};

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

//Actions
const DELETE = "DELETE";
const UPDATE = "UPDATE";
const CREATE = "CREATE";

//Resource Types
const TEI_TYPE = "TEI";
const ENROLLMENT_TYPE = "ENROLLMENT";
const TEA_TYPE = "TEA";
const EVENT_TYPE = "EVENT";
const DV_TYPE = "DV";

const PREVIOUS_FOLDER = "PREVIOUS_DHIS2_data"
const CURRENT_FOLDER = "GENERATED_data"

function generate_diff(SOURCE_OU_CODE){
    try{
        logger_diff.info(`Running diff for ${SOURCE_OU_CODE}`)
        /**
         * Read input files
         */
        const PATIENT_DUMP_PREVIOUS_FILE = "./" + PREVIOUS_FOLDER + "/" + SOURCE_OU_CODE + "/previous_all_patient_index.json";
        logger_diff.info(PATIENT_DUMP_PREVIOUS_FILE)
        if (!fs.existsSync(PATIENT_DUMP_PREVIOUS_FILE)) {
            logger_diff.error(`ArgError; Patient_uid_file (${PATIENT_DUMP_PREVIOUS_FILE}) from previous data dump doesn't exist`)
        } else {
            var previous_all_patient_index = JSON.parse(fs.readFileSync(PATIENT_DUMP_PREVIOUS_FILE))
        }

        //const PATIENT_UID_FILE_CURRENT = SOURCE_DATE_CURRENT + "-" + SOURCE_OU_CODE + "-patient_code_uid.json";
        const PATIENT_DUMP_CURRENT_FILE = "./" + CURRENT_FOLDER + "/current_all_patient_index.json";


        if (!fs.existsSync(PATIENT_DUMP_CURRENT_FILE)) {
            logger_diff.error(`ArgError; Patient_uid_file (${PATIENT_DUMP_CURRENT_FILE}) from current data dump doesn't exist`)
        } else {
            var current_all_patient_index = JSON.parse(fs.readFileSync(PATIENT_DUMP_CURRENT_FILE))
        }

        //Files
        const TEIS_FILE = "./" + PREVIOUS_FOLDER + "/" + SOURCE_OU_CODE + "/teis.json";
        var dhis_teis;
        if (!fs.existsSync(TEIS_FILE)) {
            logger_diff.error(`ArgError; TEIs file (${TEIS_FILE}) from dhis data doesn't exist`)
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

        var previousTEIs_PatientCodes = Object.keys(previous_all_patient_index);
        var currentTEIs_PatientCodes = Object.keys(current_all_patient_index);

        var newTEIs_patientCodes = _.difference(currentTEIs_PatientCodes, previousTEIs_PatientCodes);
        var missingTEIs_patientCodes = _.difference(previousTEIs_PatientCodes, currentTEIs_PatientCodes);
        var commonTEIs_patientCodes = _.intersection(previousTEIs_PatientCodes, currentTEIs_PatientCodes);
        /*logger_diff.info(`TEIs list from PREVIOUS data dump: ${previousTEIs_PatientCodes}`);
        logger_diff.info(`TEIs list from CURRENT data dump: ${currentTEIs_PatientCodes}`);*/

        // CREATE TEIs
        newTEIs_patientCodes.forEach((TEI) => {
            changed_TEIs = true;
            teis_toBeCreated.push(TEI);
            const patient_uid = current_all_patient_index[TEI].uid;
            
            var dict = getAction_TEI(CREATE, TEI);
            dict.uid = patient_uid;
            listOfActions.push(dict);
            logger_diff.info(`TEI_CREATE; Patient ${TEI} (${patient_uid}) will be created. Not present in previous data dump`);
        })

        // DELETE TEIs
        missingTEIs_patientCodes.forEach((TEI) => {
            changed_TEIs = true;
            teis_toBeDeleted.push(TEI);
            const patient_uid = previous_all_patient_index[TEI].uid;
            
            var dict = getAction_TEI(DELETE, TEI);
            dict.uid = patient_uid;
            listOfActions.push(dict);
            logger_diff.info(`TEI_DELETE; Patient ${TEI} (${patient_uid}) will be deleted from DHIS2 server; Not present in new data dump`);
        });

        var teis_toUpdateTEA = [];
        var teis_toUpdateEnroll = [];

        commonTEIs_patientCodes.forEach((TEI) => {
            var changes = checkTEIDifference(TEI);
            changed_TEIs = changes.changed_TEI;
            if (changed_TEIs) {
                teis_toBeUpdated.push(TEI);
                if (changes.changed_TEA) {
                    teis_toUpdateTEA.push(previous_all_patient_index[TEI].uid);
                }
                if (changes.changed_enroll) {
                    teis_toUpdateEnroll.push(TEI);
                }

            }
        })

        //Statistics
        logger_diff.info(`TEIs to be created: ${teis_toBeCreated.length} (${teis_toBeCreated})`);
        logger_diff.info(`TEIs to be deleted: ${teis_toBeDeleted.length} (${teis_toBeDeleted})`);
        logger_diff.info(`TEIs to be updated: ${teis_toBeUpdated.length} (${teis_toBeUpdated})`);
        logger_diff.info(`TEIs to be updated because of TEA difference: ${teis_toUpdateTEA.length} (${teis_toUpdateTEA})`);
        logger_diff.info(`TEIs to be updated because of enrollment difference (including change in events): ${teis_toUpdateEnroll.length} (${teis_toUpdateEnroll})`);
        logger_diff.info(`Total TEIs (common + new - missing): ${commonTEIs_patientCodes.length + newTEIs_patientCodes.length - missingTEIs_patientCodes.length}`);
        /**
         * WRITE actions to file
         */
        const ACTIONS_LIST_FILE = "./actions/" + SOURCE_OU_CODE + "/actions.json";
        const ACTIONS_FOLDER = "actions/" + SOURCE_OU_CODE;

        //check if folder exists. If not, create it
        if (!fs.existsSync(ACTIONS_FOLDER)) {
            fs.mkdirSync(ACTIONS_FOLDER);
        }

        try {
            utils.saveJSONFile(ACTIONS_LIST_FILE, listOfActions);
        } catch (err) {
            // An error occurred
            logger_diff.error(err);
        }
    } catch (error) {
        logger_diff.error(error.stack)
        process.exitCode = 1;
    }

    // AND NOW, all the definition of the functions.

    //***********************************************************************************************/
    //***********************************************************************************************/
    //***********************************************************************************************/


    /**
     * Read TEI files
     */
    function read_current_TEIs(TEI_uid) {

        var TEI_file;
        const TEI_fileName = `./${CURRENT_FOLDER}/teis/${SOURCE_OU_CODE}/${TEI_uid}.json`;
        if (!fs.existsSync(TEI_fileName)) {
            logger_diff.error(`FileError; TEI file (${TEI_fileName}) (from data dump) doesn't exist`)
        } else {
            TEI_file = JSON.parse(fs.readFileSync(TEI_fileName))
        }

        return TEI_file;

    }

    function read_previous_TEIs(TEI_uid) {

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
        var dhisTEI_uid = previous_all_patient_index[codepatient].uid;
        var newTEI_uid = current_all_patient_index[codepatient].uid;

        //DHIS uploaded file
        var previous_TEI_file = read_previous_TEIs(dhisTEI_uid);

        //New dump file
        var current_TEI_file = read_current_TEIs(newTEI_uid);

        if (typeof previous_TEI_file !== "undefined" && typeof current_TEI_file !== "undefined") {

            /**
             * ATTRIBUTES (TEA)
             */
            changed_TEA = checkTEAexistence(codepatient, previous_TEI_file, current_TEI_file, previous_all_patient_index);

            /**
             * ENROLLMENTS
             */
            programs.forEach((program) => { // iterate over programs
                if (checkEnrollmentExistence(previous_TEI_file, current_TEI_file, codepatient, program, previous_all_patient_index, current_all_patient_index)){
                    changed_enroll = true;
                }
            });

        } else {
            logger_diff.error(`FileError; Process stopped because of a missing file`)
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
     * @param {*} previous_all_patient_index 
     */
    function checkTEAexistence(codepatient, dhisTEI_file, newTEI_file, previous_all_patient_index) {

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
            
            var dict = getAction_TEA(DELETE, previous_all_patient_index[codepatient].uid, TEA, dhisTEAs_data[dhisTEAs_uids.indexOf(TEA)].value);
            listOfActions.push(dict);
            logger_diff.info(`TEA_DELETE; TEA ${TEA} will be deleted from patient ${codepatient} (${previous_all_patient_index[codepatient].uid}). Not present in new data dump`);
        });

        newTEAs.forEach((TEA) => {
            changed_new = true;
            
            var dict = getAction_TEA(CREATE, previous_all_patient_index[codepatient].uid, TEA, newTEAs_data[newTEAs_uids.indexOf(TEA)].value);
            listOfActions.push(dict);
            logger_diff.info(`TEA_CREATE; TEA ${TEA} will be created for patient ${codepatient} (${previous_all_patient_index[codepatient].uid}). Not present in previous data dump`);
        });

        commonTEAs.forEach((TEA) => {
            //logger_diff.info(`TEA_REVIEW; TEA ${TEA} will be reviewed for patient ${codepatient} (${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Present in previous data dump`);

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
            //logger_diff.info(`TEA_INFO; TEA ${TEA} won't be updated for patient ${codepatient} (${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; It has the same value as the previous dump`)
        } else {
            updated_TEA = true;
            
            var dict = getAction_TEA(UPDATE, previous_all_patient_index[codepatient].uid, TEA, valueDHIS, valueNew);
            listOfActions.push(dict);
            logger_diff.info(`TEA_UPDATE; TEA ${TEA} will be updated for patient ${codepatient} (${previous_all_patient_index[codepatient].uid}). Previous value: ${valueDHIS} , New value: ${valueNew}`)
        }

        return updated_TEA;
    }

    /**
     * Checks the existence of an Enrollment in the new data dump
     * 
     */
    function checkEnrollmentExistence(previous_TEI_file, current_TEI_file, codepatient, program, previous_all_patient_index, current_all_patient_index) {
        var changed_enroll_missing = false;
        var changed_enroll_new = false;
        var changed_enroll_common = false;

        var programIndex = programs.indexOf(program);
        var programLabel = program_labels[programIndex];
        const final_program_label = programLabel.replace("_enrollment", "")
        const final_program_uid = PROGRAMS_MAPPING[final_program_label]

        if (programLabel in previous_all_patient_index[codepatient]) { // Enrollment in the previous data dump
            if (programLabel in current_all_patient_index[codepatient]) { // There is/are enrollment/s about this particular program in both dump files

                var previousEnrollment_dates = Object.keys(previous_all_patient_index[codepatient][programLabel]) //Array of dates('2019-11-18') 
                var currentEnrollment_dates = Object.keys(current_all_patient_index[codepatient][programLabel])

                var missingEnrollments = _.difference(previousEnrollment_dates, currentEnrollment_dates);
                var newEnrollments = _.difference(currentEnrollment_dates, previousEnrollment_dates);
                var commonEnrollments = _.intersection(previousEnrollment_dates, currentEnrollment_dates);

                //Some enrollments are missing in the current dump
                missingEnrollments.forEach((enrollment_date) => {
                    changed_enroll_missing = true;
                    enrollment_uid = previous_all_patient_index[codepatient][programLabel][enrollment_date]; //Enrollment_uid
                    tei_uid = previous_all_patient_index[codepatient].uid;

                    var dict = {};
                    dict.action = DELETE;
                    dict.type = ENROLLMENT_TYPE;
                    dict.uid = enrollment_uid
                    dict.enrollmentDate = enrollment_date;
                    dict.TEI = tei_uid
                    dict.program = final_program_uid;
                    dict.programLabel = final_program_label;
                    listOfActions.push(dict);
                    logger_diff.info(`ENROLLMENT_DELETE; Program: ${final_program_label} (${final_program_uid}). Enrollment: ${enrollment_date} (${enrollment_uid}) will be deleted from patient ${codepatient} (${tei_uid}). Not present in new data dump`);
                });

                //There are new enrollments in the current dump
                newEnrollments.forEach((enrollment_date) => {
                    changed_enroll_new = true;
                    enrollment_uid = current_all_patient_index[codepatient][programLabel][enrollment_date]
                    tei_uid = previous_all_patient_index[codepatient].uid

                    var dict = {};
                    dict.action = CREATE;
                    dict.type = ENROLLMENT_TYPE;
                    dict.uid = enrollment_uid;
                    dict.enrollmentDate = enrollment_date;
                    dict.TEI = tei_uid;
                    dict.program = final_program_uid;
                    dict.programLabel = final_program_label;
                    dict.status = getEnrollmentStatus(current_TEI_file.enrollments, enrollment_uid);
                    listOfActions.push(dict);
                    logger_diff.info(`ENROLLMENT_CREATE; Program: ${final_program_label} (${final_program_uid}). Enrollment: ${enrollment_date} (${enrollment_uid}) will be created from patient ${codepatient} (${tei_uid}). Not present in previous data dump`);
                });


                //Some enrollments are present in both dumps
                commonEnrollments.forEach((enrollment_date) => {
                    //logger_diff.info(`ENROLLMENT_REVIEW; TEI ${current_all_patient_index[codepatient].uid} Program: ${program} Enrollment date (${enrollment_date})`)
                    //will be reviewed for patient ${codepatient} (${previous_patient_code_uid[codepatient].uid}) in DHIS2 server;
                    //Present in previous data dump`);
                    const checkEnrollmentDifferenceV = checkEnrollmentDifference(previous_TEI_file, current_TEI_file, enrollment_date, codepatient, programLabel, previous_all_patient_index, current_all_patient_index);
                    if (checkEnrollmentDifferenceV) {
                        changed_enroll_common = true
                    }

                });

            } else { //enrollment in previous dump but not in current dump (same as missing enrollment)
                changed_enroll_missing = true;
                
                // could be one or more enrollments
                const previous_enrollments = previous_all_patient_index[codepatient][programLabel]
                for (const [enrollment_date, enrollment_uid] of Object.entries(previous_enrollments)) {
                    var dict = {};
                    dict.action = DELETE;
                    dict.type = ENROLLMENT_TYPE;
                    dict.uid = enrollment_uid
                    dict.enrollmentDate = enrollment_date;
                    dict.TEI = previous_all_patient_index[codepatient].uid;
                    dict.program = final_program_uid;
                    dict.programLabel = final_program_label;
                    listOfActions.push(dict);
                    logger_diff.info(`ENROLLMENT_DELETE; Program: ${final_program_label} (${final_program_uid}). Enrollment: ${enrollment_date} (${enrollment_uid}) will be deleted from patient ${codepatient} (${previous_all_patient_index[codepatient].uid}). Not present in new data dump`);
                }
            }
        } else { // Enrollment NOT in the previous data dump
            if (programLabel in current_all_patient_index[codepatient]) { //enrollment in current dump but not in previous dump (same as new enrollment)
                changed_enroll_new = true;

                const new_enrollments = current_all_patient_index[codepatient][programLabel]
                for (const [enrollment_date, enrollment_uid] of Object.entries(new_enrollments)) {
                    tei_uid = previous_all_patient_index[codepatient].uid;
                    
                    var dict = {};
                    dict.action = CREATE;
                    dict.type = ENROLLMENT_TYPE;
                    dict.uid = enrollment_uid; //Para identificar los eventos que hay que crear asociados a cada enrollment
                    dict.enrollmentDate = enrollment_date;
                    dict.TEI = tei_uid;
                    dict.program = final_program_uid;
                    dict.programLabel = final_program_label;
                    dict.status = getEnrollmentStatus(current_TEI_file.enrollments, enrollment_uid);
                    listOfActions.push(dict);
                    logger_diff.info(`ENROLLMENT_CREATE; Program: ${final_program_label} (${final_program_uid}). Enrollment: ${enrollment_date} (${enrollment_uid}) will be created from patient ${codepatient} (${tei_uid}). Not present in previous data dump`);
                }
            } else {
                // Not Enrollment in the previous data dump AND not enrollment in the current dump.
            }
        }
        return (changed_enroll_new || changed_enroll_missing || changed_enroll_common);
    }

    /**
     * For a particular program and particular enrollmentDate
     * Check Enrollment status difference
     * Check Events related to this enrollment difference
     */
    function checkEnrollmentDifference(previous_TEI_file, current_TEI_file, enrollment_date, codepatient, programLabel, previous_all_patient_index, current_all_patient_index) {

        const enrollment_uid = previous_all_patient_index[codepatient][programLabel][enrollment_date]
        const tei_uid = previous_all_patient_index[codepatient].uid

        const final_program_label = programLabel.replace("_enrollment", "")
        const final_program_uid = PROGRAMS_MAPPING[final_program_label]

        //logger_diff.info(`REVIEW enrollment ${programLabel}. Enrollment ${enrollment_uid} (${enrollment_date}) Patient ${codepatient} (${tei_uid}).`);

        var changed = false;

        //Check enrollment existence for each programStage
        var programStages = [];
        var previous_enrollment_key = "";
        var current_enrollment_key = "";
        var enrollment_uid_previous = previous_all_patient_index[codepatient][programLabel][enrollment_date];
        var enrollment_uid_current = current_all_patient_index[codepatient][programLabel][enrollment_date];
        if (programLabel == PROGRAM_PTME_ENFANT_LABEL_ENROLLMENT) {
            //Assign corresponding programStages
            programStages = ENFANT_programStages;
            //Build Enrollment Keys (eg. "PTME_ENFANT-scWpqiXLR5u")
            previous_enrollment_key = PROGRAM_PTME_ENFANT + "-" + enrollment_uid_previous;
            current_enrollment_key = PROGRAM_PTME_ENFANT + "-" + enrollment_uid_current;
        } else if (programLabel == PROGRAM_PTME_MERE_LABEL_ENROLLMENT) {
            programStages = MERE_programStages;
            previous_enrollment_key = PROGRAM_PTME_MERE + "-" + enrollment_uid_previous;
            current_enrollment_key = PROGRAM_PTME_MERE + "-" + enrollment_uid_current;
        } else if (programLabel == PROGRAM_TARV_LABEL_ENROLLMENT) {
            programStages = TARV_programStages;
            previous_enrollment_key = PROGRAM_TARV + "-" + enrollment_uid_previous;
            current_enrollment_key = PROGRAM_TARV + "-" + enrollment_uid_current;
        }


        /**
        * PROGRAM STAGES
        */
        programStages.forEach((stage) => {

            // Review stage by stage
            const checkStageEventsFlag = checkStageEvents(programLabel, codepatient, stage, previous_enrollment_key, current_enrollment_key, enrollment_uid_previous, enrollment_uid_current, previous_all_patient_index, current_all_patient_index);
            if (checkStageEventsFlag) {
                changed = true
            }
        })

        /**
         * ENROLLMENT STATUS
         */
        var enrollments_current = current_TEI_file.enrollments;
        var enrollments_previous = previous_TEI_file.enrollments;

        /***** Extract enrollment status ******/

        var status_current = getEnrollmentStatus(enrollments_current, enrollment_uid_current);
        var status_previous = getEnrollmentStatus(enrollments_previous, enrollment_uid_previous);

        if (status_current.toUpperCase() != status_previous.toUpperCase()) {
            changed = true;
            
            var dict = {};
            dict.action = UPDATE;
            dict.type = ENROLLMENT_TYPE;
            dict.uid = enrollment_uid;
            dict.enrollmentDate = enrollment_date;
            dict.TEI = tei_uid;
            dict.program = final_program_uid;
            dict.programLabel = final_program_label;
            dict.previousStatus = status_previous;
            dict.currentStatus = status_current;
            listOfActions.push(dict);
            logger_diff.info(`ENROLLMENT_STATUS_UPDATE; Program: ${final_program_label} (${final_program_uid}). Enrollment: ${enrollment_date} (${enrollment_uid}) STATUS will be updated from patient ${codepatient} (${tei_uid}). Previous status: ${status_previous}, New status: ${status_current}`)
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
     * @param {*} previous_enrollment_key Program_stage_label + "-" + enrollment_uid
     * @param {*} current_enrollment_key Program_stage_label + "-" + enrollment_uid
     * @param {*} previous_all_patient_index 
     * @param {*} current_all_patient_index 
     */
    function checkStageEvents(programLabel, codepatient, stage, previous_enrollment_key, current_enrollment_key, enrollment_uid_previous, enrollment_uid_current, previous_all_patient_index, current_all_patient_index) {
        var changed_new = false;
        var changed_missing = false;
        var changed_common = false;
        const final_program_label = programLabel.replace("_enrollment", "")
        const final_program_uid = PROGRAMS_MAPPING[final_program_label]


        const previous_all_enrollment_events_index = previous_all_patient_index[codepatient][previous_enrollment_key]; // diccionario con los stages y cada stage tiene una lista de eventos
        const current_all_enrollment_events_index = current_all_patient_index[codepatient][current_enrollment_key];
        if (typeof previous_all_enrollment_events_index !== "undefined") { // There are events associated to that enrollment in the previous file
            if (typeof current_all_enrollment_events_index !== "undefined") { // There are events associated to that enrollment in the current file
                if (stage in previous_all_enrollment_events_index) { // Stage exist in previous file enrollment
                    if (stage in current_all_enrollment_events_index) {// Stage exist in current file enrollment

                        // Revisar altas, bajas y modificaciones
                        const patient_uid = previous_all_patient_index[codepatient].uid

                        /**
                        * EVENTS
                        */
                        var previousEvents_dates = Object.keys(previous_all_enrollment_events_index[stage]);
                        var currentEvents_dates = Object.keys(current_all_enrollment_events_index[stage]);

                        var missingEvents = _.difference(previousEvents_dates, currentEvents_dates);
                        var newEvents = _.difference(currentEvents_dates, previousEvents_dates);
                        var commonEvents = _.intersection(previousEvents_dates, currentEvents_dates);

                        //Some events for that stage are missing in the current dump
                        missingEvents.forEach((event_date) => {
                            changed_missing = true;
                            const event_uid = previous_all_enrollment_events_index[stage][event_date]
                            
                            var dict = {};
                            dict.action = DELETE;
                            dict.type = EVENT_TYPE;
                            dict.uid = event_uid;
                            dict.eventDate = event_date;
                            dict.TEI = patient_uid;
                            dict.enrollment = enrollment_uid_previous;
                            dict.program = final_program_uid;
                            dict.programLabel = final_program_label;
                            dict.programStage = stage;                        
                            listOfActions.push(dict);
                            logger_diff.info(`EVENT_DELETE; Program: ${final_program_label} (${final_program_uid}). Program Stage ${[stage]}. Event (${event_uid}) on ${event_date} will be deleted for patient ${codepatient} (${patient_uid}) in previous dump but not present in current data dump`);
                        });

                        //There are new events for that stage in the current dump
                        newEvents.forEach((event_date) => {
                            changed_new = true;
                            const event_uid = current_all_enrollment_events_index[stage][event_date]
                            const patient_uid = previous_all_patient_index[codepatient].uid
                            
                            var dict = {};
                            dict.action = CREATE;
                            dict.type = EVENT_TYPE;
                            dict.uid = event_uid;
                            dict.eventDate = event_date; //Event date
                            dict.TEI = patient_uid;
                            dict.enrollment = enrollment_uid_previous;
                            dict.program = final_program_uid;
                            dict.programLabel = final_program_label;
                            dict.programStage = stage;
                            listOfActions.push(dict);
                            logger_diff.info(`EVENT_CREATE; Program: ${final_program_label} (${final_program_uid}). Program Stage ${[stage]}. Event (${event_uid}) on ${event_date} will be created for patient ${codepatient} (${patient_uid}). Not present in previous data dump`);
                        });

                        //Some events for that stage are present in both dumps
                        commonEvents.forEach((event_date) => {
                            const event_uid = current_all_enrollment_events_index[stage][event_date]

                            //logger_diff.info(`Event_REVIEW; Event ${event_uid} Program: ${final_program_label} (${final_program_uid})`)
                            //will be reviewed for patient ${codepatient} (${previous_patient_code_uid[codepatient].uid}) in DHIS2 server;
                            //Present in previous data dump`);

                            const previous_TEI_file = read_previous_TEIs(previous_all_patient_index[codepatient].uid);
                            const current_TEI_file = read_current_TEIs(current_all_patient_index[codepatient].uid);

                            const previousEvent_uid = previous_all_enrollment_events_index[stage][event_date];
                            const currentEvent_uid = current_all_enrollment_events_index[stage][event_date];

                            /**
                            * DATA VALUES
                            */
                            const previous_dataValues = getDataValues(previous_TEI_file, enrollment_uid_previous, previousEvent_uid);
                            const current_dataValues = getDataValues(current_TEI_file, enrollment_uid_current, currentEvent_uid);

                            const checkDataValuesExistenceV = checkDataValuesExistence(enrollment_uid_previous, codepatient, patient_uid, event_date, stage, previous_dataValues, current_dataValues, event_uid, final_program_uid, final_program_label);

                            if (checkDataValuesExistenceV) {
                                changed_common = true
                            }

                            /**
                             * EVENT STATUS
                             */

                            var status_previous = getEventStatus(previous_TEI_file, event_uid);
                            var status_current = getEventStatus(current_TEI_file, event_uid);

                            if (status_current.toUpperCase() != status_previous.toUpperCase()) {
                                changed = true;
                                
                                var dict = {};
                                dict.action = UPDATE;
                                dict.type = EVENT_TYPE;
                                dict.uid = event_uid;
                                dict.uid = event_uid;
                                dict.eventDate = event_date; //Event date
                                dict.TEI = patient_uid;
                                dict.enrollment = enrollment_uid_previous;
                                dict.program = final_program_uid;
                                dict.programLabel = final_program_label;
                                dict.programStage = stage;
                                dict.previousStatus = status_previous;
                                dict.currentStatus = status_current;
                                listOfActions.push(dict);
                                logger_diff.info(`EVENT_UPDATE_STATUS; Program: ${final_program_label} (${final_program_uid}). Program Stage ${[stage]}. Event (${event_uid}) on ${event_date} will be updated for patient ${codepatient} (${patient_uid}). Previous status: ${status_previous}, New status: ${status_current}`);
                            }

                            /**
                             * EVENT DUE DATE
                             */

                            var dueDate_previous = getEventDueDate(previous_TEI_file, event_uid);
                            var dueDate_current = getEventDueDate(current_TEI_file, event_uid);
    
                            if (dueDate_previous.toUpperCase() != dueDate_current.toUpperCase()) {
                                changed = true;
                                
                                var dict = {};
                                dict.action = UPDATE;
                                dict.type = EVENT_TYPE;
                                dict.uid = event_uid;
                                dict.uid = event_uid;
                                dict.eventDate = event_date; //Event date
                                dict.TEI = patient_uid;
                                dict.enrollment = enrollment_uid_previous;
                                dict.program = final_program_uid;
                                dict.programLabel = final_program_label;
                                dict.programStage = stage;
                                dict.previousDueDate = dueDate_previous;
                                dict.currentDueDate = dueDate_current;
                                listOfActions.push(dict);
                                logger_diff.info(`EVENT_UPDATE_DUEDATE; Program: ${final_program_label} (${final_program_uid}). Program Stage ${[stage]}. Event (${event_uid}) on ${event_date} will be updated for patient ${codepatient} (${patient_uid}). Previous dueDate: ${dueDate_previous}, New dueDate: ${dueDate_current}`);
                            }

                        });

                    } else { // Stage doesn't exist in current file enrollment
                        changed_missing = true;
                        logger_diff.info(`All_Events_ProgramStage_DELETE; ${[stage]} stage from enrollment (${enrollment_uid_current}) and all its associated events will be deleted for patient ${codepatient} (${previous_all_patient_index[codepatient].uid}). Not present in new data dump`);
                        var events = Object.keys(previous_all_enrollment_events_index[stage]);
                        //For each event on that stage
                        if (typeof events !== "undefined") {
                            events.forEach((event_date) => {
                                const event_uid = previous_all_enrollment_events_index[stage][event_date]
                                const patient_uid = previous_all_patient_index[codepatient].uid
                                
                                var dict = {};
                                dict.action = DELETE;
                                dict.type = EVENT_TYPE;
                                dict.uid = event_uid;
                                dict.eventDate = event_date;
                                dict.TEI = patient_uid;
                                dict.enrollment = enrollment_uid_previous;
                                dict.program = final_program_uid;
                                dict.programLabel = final_program_label;
                                dict.programStage = stage;
                                listOfActions.push(dict);
                                logger_diff.info(`EVENT_DELETE; Program: ${final_program_label} (${final_program_uid}). Program Stage ${[stage]}. Event (${event_uid}) on ${event_date} will be deleted for patient ${codepatient} (${patient_uid}) in previous dump but not present in current data dump`);
                            });
                        }
                    }
                } else { //Stage doesn't exist in previous file enrollment
                    if (stage in current_all_enrollment_events_index) {
                        changed_new = true;
                        Object.keys(current_all_enrollment_events_index[stage]).forEach((event_date) => {
                            const event_uid = current_all_enrollment_events_index[stage][event_date]
                            const patient_uid = previous_all_patient_index[codepatient].uid
                            
                            var dict = {};
                            dict.action = CREATE;
                            dict.type = EVENT_TYPE;
                            dict.eventDate = event_date; //Event date
                            dict.uid = event_uid;
                            dict.TEI = patient_uid;
                            dict.enrollment = enrollment_uid_previous;
                            dict.program = final_program_uid;
                            dict.programLabel = final_program_label;
                            dict.programStage = stage;
                            listOfActions.push(dict);
                            logger_diff.info(`EVENT_CREATE; Program: ${final_program_label} (${final_program_uid}). Program Stage ${[stage]}. Event (${event_uid}) on ${event_date} will be created for patient ${codepatient} (${patient_uid}). Not present in previous data dump`);
                        })

                    } else {
                        // Do nothing
                    }
                }
            } else { // No events associated to that enrollment in the current file
                var previousEvents_dates = []
                if ("stage" in previous_all_enrollment_events_index){
                    previousEvents_dates = Object.keys(previous_all_enrollment_events_index[stage]);
                    logger_diff.info(`All_Events_ProgramStage_DELETE; ${previous_enrollment_key} ${[stage]}`);
                }

                //There are new events for that stage in the current dump
                previousEvents_dates.forEach((event_date) => {
                    changed_new = true;
                    const event_uid = previous_all_enrollment_events_index[stage][event_date]
                    const patient_uid = previous_all_patient_index[codepatient].uid
                    
                    var dict = {};
                    dict.action = DELETE;
                    dict.type = EVENT_TYPE;
                    dict.uid = event_uid;
                    dict.eventDate = event_date;
                    dict.TEI = patient_uid;
                    dict.enrollment = enrollment_uid_previous;
                    dict.program = final_program_uid;
                    dict.programLabel = final_program_label;
                    dict.programStage = stage;
                    listOfActions.push(dict);
                    logger_diff.info(`EVENT_DELETE; Program: ${final_program_label} (${final_program_uid}). Program Stage ${[stage]}. Event (${event_uid}) on ${event_date} will be created for patient ${codepatient} (${patient_uid}). Not present in current data dump`);
                });
            }
        } else { // No events associated to that enrollment in the previous file, for any of the program stages
            if (typeof current_all_enrollment_events_index !== "undefined") { // There are events for that enrollment
                if (stage in current_all_enrollment_events_index) { // Stage exist in current file enrollment

                    var currentEvents_dates = Object.keys(current_all_enrollment_events_index[stage]);
                    logger_diff.info(`All_Events_ProgramStage_CREATE; ${previous_enrollment_key} ${[stage]}`);

                    //There are new events for that stage in the current dump
                    currentEvents_dates.forEach((event_date) => {
                        changed_new = true;
                        const event_uid = current_all_enrollment_events_index[stage][event_date]
                        const patient_uid = current_all_patient_index[codepatient].uid
                        
                        var dict = {};
                        dict.action = CREATE;
                        dict.type = EVENT_TYPE;
                        dict.uid = event_uid;
                        dict.eventDate = event_date;
                        dict.TEI = patient_uid;
                        dict.enrollment = enrollment_uid_previous;
                        dict.programStage = stage;
                        dict.program = final_program_uid;
                        dict.programLabel = final_program_label;
                        listOfActions.push(dict);
                        logger_diff.info(`EVENT_CREATE; Program: ${final_program_label} (${final_program_uid}). Program Stage ${[stage]}. Event (${event_uid}) on ${event_date} will be created for patient ${codepatient} (${patient_uid}). Not present in previous data dump`);
                    });
                } else {
                    // No events in the current patient index for that stage
                    // Do nothing. This is managed by another part of the code
                }
            } else { //No events before and no events now
                // Do nothing
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
        if (enrollment_uid == "qI6sxCauRF2") {
            console.log(enrollment_uid)
            console.log(enrollments_uids.indexOf(enrollment_uid))
            console.log(enrollments_data)    
        }
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


    function getEventStatus(TEI_data, event_uid) {
        var status = "ACTIVE"; // Default value if no status is present
        TEI_data.enrollments.forEach(enrollment => {
            enrollment.events.forEach(event => {
                if ((event.event == event_uid) && ('status' in event) ){
                    status = event.status
                }
            });
        });

        return status;
    }

    function getEventDueDate(TEI_data, event_uid) {
        var dueDate; // Default value if no status is present
        TEI_data.enrollments.forEach(enrollment => {
            enrollment.events.forEach(event => {
                if ((event.event == event_uid) && ('dueDate' in event) ){
                    dueDate = event.dueDate
                }
            });
        });
        if (typeof dueDate !== "undefined" && dueDate.includes("T00:00:00.000")) {
            dueDate = dueDate.replace("T00:00:00.000","")
        }

        return dueDate;
    }

    function getValueByDE(dataValues, de_uid){
        let r_value;
        dataValues.forEach(dv => {
            if (dv['dataElement'] == de_uid) {
                r_value = dv['value'];
            }
        });

        return r_value;
    }

    /**
     * Given an event check the differences with its datavalues with the new data
     * Logs the changes (remain the same or will be updated in the server)
     */
    function checkDataValuesExistence(enrollment_uid, codepatient, patient_uid, event_date, stage, previous_dataValues, current_dataValues, event_uid, program_uid, program_label) {

        var changed_missing = false;
        var changed_new = false;
        var changed_common = false;
        //For each event
        //Check DE existence
        // A DE can only be once in a event
        if (typeof previous_dataValues !== "undefined" && previous_dataValues.length !== 0) { //Event had dataValues in previous dump
            if (typeof current_dataValues !== "undefined" && current_dataValues.length !== 0) { //Event has dataValues in new dump

                //DEs UIDs arrays
                var dhisDEs_uids = [];
                previous_dataValues.forEach(DE => {
                    dhisDEs_uids.push(DE.dataElement)
                })

                var newDEs_uids = [];
                current_dataValues.forEach(DE => {
                    newDEs_uids.push(DE.dataElement)
                })

                var missingDEs = _.difference(dhisDEs_uids, newDEs_uids);
                var newDEs = _.difference(newDEs_uids, dhisDEs_uids);
                var commonDEs = _.intersection(dhisDEs_uids, newDEs_uids);

                //Some DEs for that event are missing in the current dump
                missingDEs.forEach((de_uid) => {
                    changed_missing = true;

                    const de_value = getValueByDE(previous_dataValues, de_uid)
                    var dict = {};
                    dict.action = DELETE;
                    dict.type = DV_TYPE;
                    dict.event = event_uid; // event uid
                    dict.eventDate = event_date;
                    dict.dataElement = de_uid; //DE UID
                    dict.dataValue = de_value;
                    dict.TEI = patient_uid;
                    dict.enrollment = enrollment_uid;
                    dict.program = program_uid;
                    dict.programLabel = program_label;
                    dict.programStage = stage;
                    listOfActions.push(dict);
                    logger_diff.info(`DV_DELETE; Patient ${codepatient} (${patient_uid}). Program: ${program_label} (${program_uid}). Program Stage ${[stage]}. Event (${event_uid}) ${event_date}. DataElement (${de_uid}) with value ${de_value} will be deleted. Not present in current data dump`);
                });

                //There are new DEs for that event in the current dump
                newDEs.forEach((de_uid) => {
                    changed_new = true;

                    const de_value = getValueByDE(current_dataValues, de_uid)
                    var dict = {};
                    dict.action = CREATE;
                    dict.type = DV_TYPE;
                    dict.event = event_uid; // event uid
                    dict.eventDate = event_date;
                    dict.dataElement = de_uid; //DE UID
                    dict.dataValue = de_value;
                    dict.TEI = patient_uid;
                    dict.enrollment = enrollment_uid;
                    dict.program = program_uid;
                    dict.programLabel = program_label;
                    dict.programStage = stage;
                    listOfActions.push(dict);
                    logger_diff.info(`DV_CREATE; Patient ${codepatient} (${patient_uid}). Program: ${program_label} (${program_uid}). Program Stage ${[stage]}. Event (${event_uid}) ${event_date}. DataElement (${de_uid}) with value ${de_value} will be created. Not present in previous data dump`);
                });

                //Some events for that stage are present in both dumps
                commonDEs.forEach((DE) => {
                    const changed_commonV = checkDataValueDifference(event_uid, event_date, DE, codepatient, patient_uid, previous_dataValues[dhisDEs_uids.indexOf(DE)], current_dataValues[newDEs_uids.indexOf(DE)], enrollment_uid, program_uid, program_label, stage);
                    if (changed_commonV) {
                        changed_common = true
                    }
                })

            } else { //Event has dataValues in previous dump BUT has NOT dataValues in current dump
                previous_dataValues.forEach((DE) => {
                    changed_missing = true;
                    const de_uid = DE.dataElement;
                    const de_value = DE.value

                    var dict = {};
                    dict.action = DELETE;
                    dict.type = DV_TYPE;
                    dict.event = event_uid; // event uid
                    dict.eventDate = event_date;
                    dict.dataElement = de_uid; //DE UID
                    dict.dataValue = de_value;
                    dict.TEI = patient_uid;
                    dict.enrollment = enrollment_uid;
                    dict.program = program_uid;
                    dict.programLabel = program_label;
                    dict.programStage = stage;
                    listOfActions.push(dict);
                    logger_diff.info(`DV_DELETE; Patient ${codepatient} (${patient_uid}). Program: ${program_label} (${program_uid}). Program Stage ${[stage]}. Event (${event_uid}) ${event_date}. DataElement (${de_uid}) with value ${de_value} will be deleted. Not present in current data dump`);
                });
            }

        } else { //Event didn't have dataValues in previous dump
            if (typeof current_dataValues !== "undefined" && current_dataValues.length !== 0) { //Event didn't have dataValues before but has now (in new dump)
                var newDEs_uids = [];
                current_dataValues.forEach(DE => {
                    newDEs_uids.push(DE.dataElement)
                });
                current_dataValues.forEach((DE) => {
                    changed_new = true;
                    const de_uid = DE.dataElement;
                    const de_value = current_dataValues[newDEs_uids.indexOf(DE.dataElement)].value

                    var dict = {};
                    dict.action = CREATE;
                    dict.type = DV_TYPE;
                    dict.event = event_uid; // event uid
                    dict.eventDate = event_date;
                    dict.dataElement = de_uid; //DE UID
                    dict.dataValue = de_value;
                    dict.TEI = patient_uid;
                    dict.enrollment = enrollment_uid;
                    dict.program = program_uid;
                    dict.programLabel = program_label;
                    dict.programStage = stage;
                    listOfActions.push(dict);
                    logger_diff.info(`DV_CREATE; Patient ${codepatient} (${patient_uid}). Program: ${program_label} (${program_uid}). Program Stage ${[stage]}. Event (${event_uid}) ${event_date}. DataElement (${de_uid}) with value ${de_value} will be created. Not present in previous data dump`);
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
     * @param {*} de_uid 
     * @param {*} codepatient 
     * @param {*} previous_dv dataValue object : {dataElement: 'a3WwFDKNfQH', value: '2'}
     * @param {*} current_dv
     */
    function checkDataValueDifference(event_uid, event_date, de_uid, codepatient, patient_uid, previous_dv, current_dv, enrollment_uid, program_uid, program_label, programStage) {

        var changed = false;

        //Has same value then log and do nothing
        //Has different value then UPDATE
        var previous_value = previous_dv.value;
        var current_value = current_dv.value;

        if (typeof previous_value === "undefined" || typeof current_value === "undefined") {
            logger_diff.error(`Review the script code. Debug event (${event_uid}). Previous DE ${JSON.stringify(previous_dv)}. Current DE ${JSON.stringify(current_dv)}]`)
        }

        if (previous_value !== current_value) {
            changed = true;
            var dict = {};
            dict.action = UPDATE;
            dict.type = DV_TYPE;
            dict.event = event_uid;
            dict.eventDate = event_date;
            dict.dataElement = de_uid;
            dict.previousDataElementValue = previous_value;
            dict.currentDataElementValue = current_value;
            dict.TEI = patient_uid;
            dict.enrollment = enrollment_uid;
            dict.program = program_uid;
            dict.programLabel = program_label;
            dict.programStage = programStage;
            listOfActions.push(dict);
            logger_diff.info(`DV_UPDATE; Patient ${codepatient} (${patient_uid}). Program: ${program_label} (${program_uid}). Program Stage ${programStage}. Event (${event_uid}) ${event_date}. DataElement (${de_uid}) will be updated. Previous value ${previous_value}, Current value ${current_value} `);
        } else {
            //logger_diff.info(`DV_INFO; DE ${DE} won't be updated for patient ${codepatient} (${patient_uid}); It has the same value as the previous dump`)
        }

        return changed;
    }

    /****************** ACTIONS FUNCTIONS **********************/

    function getAction_TEI(action, TEI) {
        var dict = {};
        dict.action = action;
        dict.type = TEI_TYPE;
        dict.patient_code = TEI;
        return dict;
    }

    function getAction_TEA(action, TEI, TEA, previousValue, currentValue) {
        var dict = {};
        dict.action = action;
        dict.type = TEA_TYPE;
        dict.TEA = TEA;
        dict.TEI = TEI;
        if (typeof currentValue === 'undefined'){ // CREATE or DELETE
            dict.value = previousValue
        } else { // UPDATE
            dict.previousValue = previousValue;
            dict.currentValue = currentValue;
        }
        return dict;
    }

} // end of generate_diff

module.exports = {
    generate_diff
}