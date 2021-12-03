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

const PROGRAM_TARV_LABEL = "TARV"
const PROGRAM_PTME_MERE = "PTME_MERE"
const PROGRAM_PTME_ENFANT= "PTME_ENFANT"

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
        export_date_previous: {
            description: 'the previous export date (YYYY_MM_DD)',
            alias: ['ep', 'epd'],
            type: 'text',
        },
        export_date_current: {
            description: 'the current export date (YYYY_MM_DD)',
            alias: ['ec', 'ecd'],
            type: 'text',
        },
    })
    .demandOption(['org_unit', 'export_date_previous', 'export_date_current'], 'Please provide both Organization Unit code and export dates arguments to work with this tool')
    .help()
    .alias('help', 'h')
    .argv;

/**************************************/

/**
 * Parsing arguments
 */
let SOURCE_OU_CODE;
let SOURCE_DATE_PREVIOUS;
let SOURCE_DATE_CURRENT;
if (argv._.includes('diff')) {
    SOURCE_OU_CODE = argv.org_unit;
    SOURCE_DATE_PREVIOUS = argv.export_date_previous;
    SOURCE_DATE_CURRENT = argv.export_date_current;
} else {
    process.exit(1)
}

/**************************************/

/**
 * Read input files
 */
const PATIENT_UID_FILE_PREVIOUS = SOURCE_DATE_PREVIOUS + "-" + SOURCE_OU_CODE + "-patient_code_uid.json";
if (!fs.existsSync(PATIENT_UID_FILE_PREVIOUS)) {
    logger.error(`ArgError; Patient_uid_file (${PATIENT_UID_FILE_PREVIOUS}) from previous data dump doesn't exist`)
} else {
    var previous_patient_code_uid = JSON.parse(fs.readFileSync(PATIENT_UID_FILE_PREVIOUS))
}

const PATIENT_UID_FILE_CURRENT = SOURCE_DATE_CURRENT + "-" + SOURCE_OU_CODE + "-patient_code_uid.json";

if (!fs.existsSync(PATIENT_UID_FILE_CURRENT)) {
    logger.error(`ArgError; Patient_uid_file (${PATIENT_UID_FILE_CURRENT}) from current data dump doesn't exist`)
} else {
    var current_patient_code_uid = JSON.parse(fs.readFileSync(PATIENT_UID_FILE_CURRENT))
}

/**************************************/

/**
 * TEIS
 */
var previousTEIs_PatientCodes = Object.keys(previous_patient_code_uid);
var currentTEIs_PatientCodes = Object.keys(current_patient_code_uid);

var missingTEIs_patientCodes = _.difference(previousTEIs_PatientCodes, currentTEIs_PatientCodes);
var newTEIs_patientCodes = _.difference(currentTEIs_PatientCodes, previousTEIs_PatientCodes);
var commonTEIs_patientCodes = _.intersection(previousTEIs_PatientCodes, currentTEIs_PatientCodes);
/*logger.info(`TEIs list from PREVIOUS data dump: ${previousTEIs_PatientCodes}`);
logger.info(`TEIs list from CURRENT data dump: ${currentTEIs_PatientCodes}`);*/

//Log missing TEIs
missingTEIs_patientCodes.forEach((TEI) => {
    logger.info(`TEI_DELETION; Patient ${TEI} (uid: ${previous_patient_code_uid[TEI].uid}) will be removed from DHIS2 server; Not present in new data dump`);
});

//Log new TEIs
newTEIs_patientCodes.forEach((TEI) => {
    logger.info(`TEI_CREATION; Patient ${TEI} will be created in DHIS2 server; Not present in previous data dump`);
})


commonTEIs_patientCodes.forEach((TEI) => {
    //logger.info(`TEI_REVIEW; Patient ${TEI} (uid: ${previous_patient_code_uid[TEI].uid}) will be reviewed; Present in previous data dump`);
    checkDifference(TEI); //TODO: in process
})

/**
 * Given a patient already in DHIS, check the differences in the data
 * Logs the changes (remain the same or will be updated in the server)
 * @param {*} codepatient 
 */
function checkDifference(codepatient) {


    //TEI uids
    var dhisTEI_uid = previous_patient_code_uid[codepatient].uid;
    var newTEI_uid = current_patient_code_uid[codepatient].uid;

    /****************** Read TEI files ********************/

    //DHIS uploaded file
    var dhisTEI_file;
    const dhisTEI_fileName = "./teis/" + SOURCE_OU_CODE + "_" + SOURCE_DATE_PREVIOUS + "/" + dhisTEI_uid + ".json";
    if (!fs.existsSync(dhisTEI_fileName)) {
        logger.error(`FileError; TEI file (${dhisTEI_fileName}) (previous dump) doesn't exist`)
    } else {
        dhisTEI_file = JSON.parse(fs.readFileSync(dhisTEI_fileName))
    }

    //New dump file
    var newTEI_file;
    const newTEI_fileName = "./teis/" + SOURCE_OU_CODE + "_" + SOURCE_DATE_CURRENT + "/" + newTEI_uid + ".json";
    if (!fs.existsSync(newTEI_fileName)) {
        logger.error(`FileError; TEI file (${newTEI_fileName}) (new dump) doesn't exist`)
    } else {
        newTEI_file = JSON.parse(fs.readFileSync(newTEI_fileName))
    }

    if (typeof dhisTEI_file !== "undefined" && typeof newTEI_file !== "undefined") {


        /**
         * ATTRIBUTES (TEA)
         */
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
            logger.info(`TEA_DELETION; TEA ${TEA} will be removed from patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
        });

        newTEAs.forEach((TEA) => {
            logger.info(`TEA_CREATION; TEA ${TEA} will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);
        });

        commonTEAs.forEach((TEA) => {
            //logger.info(`TEA_REVIEW; TEA ${TEA} will be reviewed for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Present in previous data dump`);

            //Skip TEA_CODE_PATIENT (will always be the same at this point)
            if (TEA != TEA_CODE_PATIENT) {
                checkTEADifference(TEA, codepatient, dhisTEAs_data[dhisTEAs_uids.indexOf(TEA)], newTEAs_data[newTEAs_uids.indexOf(TEA)]); //TODO: 
            }
        });

        /**
         * ENROLLMENTS
         */
        const programs = [PROGRAM_PTME_ENFANT_LABEL_ENROLLMENT, PROGRAM_PTME_MERE_LABEL_ENROLLMENT, PROGRAM_TARV_LABEL_ENROLLMENT];

        programs.forEach((program) => {
            if (program in previous_patient_code_uid[codepatient]) {
                if (program in current_patient_code_uid[codepatient]) {

                    var previousEnrollment_dates = Object.keys(previous_patient_code_uid[codepatient][program]) //Array of dates('2019-11-18') 
                    var currentEnrollment_dates = Object.keys(current_patient_code_uid[codepatient][program])

                    var missingEnrollments = _.difference(previousEnrollment_dates, currentEnrollment_dates);
                    var newEnrollments = _.difference(currentEnrollment_dates, previousEnrollment_dates);
                    var commonEnrollments = _.intersection(previousEnrollment_dates, currentEnrollment_dates);

                    if (missingEnrollments.length != 0) { //Some enrollments are missing in the current dump
                        missingEnrollments.forEach((enrollment_date) => {
                            logger.info(`Enrollment_DELETION; ${program} ${previous_patient_code_uid[codepatient][program][enrollment_date]} will be removed from patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
                        });
                    }
                    if (newEnrollments.length != 0) { //There are new enrollments in the current dump
                        newEnrollments.forEach((enrollment_date) => {
                            logger.info(`Enrollment_CREATION; ${program} will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);
                        });
                    }

                    if (commonEnrollments.length != 0) { //Some enrollments are missing in the current dump
                        commonEnrollments.forEach((enrollment_date) => {
                            //logger.info(`Enrollment_REVIEW; Enrollment ${previous_patient_code_uid[codepatient][program][enrollment_date]} (${program})
                            //will be reviewed for patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server;
                            //Present in previous data dump`);

                            checkEnrollmentDifference(enrollment_date, codepatient, program); //TODO  
                        });
                    }
                } else { //enrollment in previous dump but not in current dump (same as missing enrollment)
                    logger.info(`Enrollment_DELETION; ${program} ${(Object.values(previous_patient_code_uid[codepatient][program]))} will be removed from patient ${codepatient}  (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in new data dump`);
                }
            } else {
                if (program in current_patient_code_uid[codepatient]) { //enrollment in current dump but not in previous dump (same as new enrollment)
                    logger.info(`Enrollment_CREATION; ${program} will be created for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Not present in previous data dump`);
                }
            }
        })


        //previous_patient_code_uid[codepatient]
        //current_patient_code_uid[codepatient]


    }



    //Check events

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

    //Has same value then log and do nothing (skip porque todo va bien)
    //Has different value then UPDATE
    var valueDHIS = dhisTEA.value;
    var valueNew = newTEA.value;

    if (valueDHIS === valueNew) {
        //logger.info(`TEA_INFO; TEA ${TEA} won't be updated for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; It has the same value as the previous dump`)
    } else {
        logger.info(`TEA_UPDATE; TEA ${TEA} will be updated for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Previous value: ${valueDHIS} , New value: ${valueNew}`)
        //build new TEI payload
    }
}

/**
 * Given an enrollment check the differences with the new data
 * Logs the changes (remain the same or will be updated in the server)
 * @param {*} enrollment_date 
 * @param {*} codepatient 
 */
function checkEnrollmentDifference(enrollment_date, codepatient, program) {

    //Check enrollment events existence for each programStage

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

    var programStages = [];
    if (program == PROGRAM_PTME_ENFANT_LABEL_ENROLLMENT) {
        programStages = ENFANT_programStages;
    } else if (program == PROGRAM_PTME_MERE_LABEL_ENROLLMENT) {
        programStages = MERE_programStages;
    } else if (program == PROGRAM_TARV_LABEL_ENROLLMENT) {
        programStages = TARV_programStages;
    }

    /**
    * PROGRAM STAGES
    */
    programStages.forEach((stage) => {
        console.log(stage)

    })
    /*
    var missingEvents 
    var newEvents 
    var commonEvents
*/
    //dhisTEI_file //newTEI_file
}