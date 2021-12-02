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
    //logger.info(`UPDATE; Patient ${TEI} (uid: ${previous_patient_code_uid[TEI].uid}) will be reviewed; Present in previous data dump`);
    checkDifference(TEI); //TODO: 
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

    /**
     * ATTRIBUTES (TEA)
     */
    //Compare attributes
    if (typeof dhisTEI_file !== "undefined" && typeof newTEI_file !== "undefined") {

        var dhisTEAs_data = dhisTEI_file.attributes;
        var newTEAs_data = newTEI_file.attributes;

        //Coger attribute UID y construir array con solo los UIDs de los attributes
        var dhisTEAs_uids = [];
        dhisTEAs_data.forEach(attr => {
            dhisTEAs_uids.push(attr.attribute)
        })

        var newTEAs_uids = [];
        newTEAs_data.forEach(attr => {
            newTEAs_uids.push(attr.attribute)
        })

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
        })

        commonTEAs.forEach((TEA) => {
            //logger.info(`TEA_UPDATE; TEA ${TEA} will be updated for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Present in previous data dump`);
            
            //Skip TEA_CODE_PATIENT (will always be the same at this point)
            if(TEA != TEA_CODE_PATIENT){
                checkTEADifference(TEA, codepatient, dhisTEAs_data[dhisTEAs_uids.indexOf(TEA)], newTEAs_data[newTEAs_uids.indexOf(TEA)]); //TODO: 
            }
        })


    }


    //Check enrollments

    //Check events

}

/**
 * Given a TEA check the differences with the new data
 * Logs the changes (remain the same or will be updated in the server)
 * @param {*} TEA 
 * @param {*} codepatient 
 */
 function checkTEADifference(TEA, codepatient, dhisTEAs_data, newTEAs_data) {

        //Has same value then log and do nothing (skip porque todo va bien)
        //Has different value then UPDATE
        var valueDHIS = dhisTEAs_data.value;
        var valueNew = newTEAs_data.value;
        console.log(valueDHIS)
        console.log(valueNew)
        if (valueDHIS === valueNew){
            //logger.info(`TEA_INFO; TEA ${TEA} won't be updated for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; It has the same value as the previous dump`)
        } else {
            logger.info(`TEA_UPDATE; TEA ${TEA} will be updated for patient ${codepatient} (uid: ${previous_patient_code_uid[codepatient].uid}) in DHIS2 server; Previous value: ${valueDHIS} , New value: ${valueNew}`)
            //build new TEI payload
        }
 }