var fs = require('fs');
const yargs = require('yargs');
const { logger, logger_fr } = require('./logger.js');
var _ = require('lodash');


const PERSON_TE_TYPE = "XV3kldsZq0H";
const TEA_CODE_PATIENT = "dsWUbqvV9GW";
const TEA_BIRTHDATE = "qA5Vsiat4Sm";
const TEA_SEX = "HhsxFysTpdu";
const TEA_ENTRYMODE = "MqqM2vt5RQA";
const TEA_LOG = "dkfd2UClEwu"; // Erreurs de validation (liste)
const TEA_TOBEREVIEWED = "ynBaBzJpqGD"; // Erreurs de validation (oui / non)

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
console.log(PATIENT_UID_FILE_PREVIOUS)
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
var commonTEIs_patientCodes = _.intersection(previousTEIs_PatientCodes,currentTEIs_PatientCodes);
/*logger.info(`TEIs list from PREVIOUS data dump: ${previousTEIs_PatientCodes}`);
logger.info(`TEIs list from CURRENT data dump: ${currentTEIs_PatientCodes}`);*/

//Log missing TEIs
missingTEIs_patientCodes.forEach((TEI) => {
    logger.info(`DELETION; Patient ${TEI} (uid: ${previous_patient_code_uid[TEI].uid}) will be removed from DHIS2 server; Not present in new data dump`);
});

//Log new TEIs
newTEIs_patientCodes.forEach((TEI) => {
    logger.info(`CREATION; Patient ${TEI} will be created in DHIS2 server; Not present in previous data dump`);
})


commonTEIs_patientCodes.forEach((TEI) => {
    logger.info(`UPDATE; Patient ${TEI} (uid: ${previous_patient_code_uid[TEI].uid}) will be reviewed; Present in previous data dump`);
    checkDifference(TEI); //TODO: 
} )

/**
 * Given a patient already in DHIS, check the differences in the data
 * Logs the changes (remain the same or will be updated in the server)
 * @param {*} codepatient 
 */
function checkDifference(codepatient) {

    var dhisData = previous_patient_code_uid[codepatient];
    var newData = current_patient_code_uid[codepatient];

    //If equal -> logger.debug and do nothing (no need to update) //skip porque todo va bien

    //Check TEAs (trackedEntityAttributes)

    //Check enrollments

    //Check events

}