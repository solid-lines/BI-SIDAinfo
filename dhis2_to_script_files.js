var fs = require('fs');
const { logger } = require('./logger.js');
const utils = require('./utils.js')

const PATIENT_CODE_TEA = "dsWUbqvV9GW";
//PROGRAMS
const TARV_UID = "e3swbbSnbQ2";
const MERE_UID = "MVooF5iCp8L";
const ENFANT_UID = "PiXg9cX1i0y";

//PROGRAM NAMES
const TARV = "TARV";
const MERE = "PTME_MERE";
const ENFANT = "PTME_ENFANT";

//PROGRAM STAGES
const PROGRAMSTAGE_TARV_PREMIER_DEBUT_ARV = "R7lNOFLLdIs";
const PROGRAMSTAGE_TARV_TARV = "CTBhldBbg5x";
const PROGRAMSTAGE_TARV_CONSULTATION = "sPrQc1FGfP1";
const PROGRAMSTAGE_TARV_DEBUT_TRAITEMENT_TB = "ZF6rqY3Jmt5";
const PROGRAMSTAGE_TARV_PERDU_DE_VUE = "aDBN3tSC41E";
const PROGRAMSTAGE_TARV_SORTIE = "WV6yIfebK6w";

const PROGRAMSTAGE_PTME_MERE_ADMISSION_PTME_MERE = "GCDZcx2b3Lr"
const PROGRAMSTAGE_PTME_MERE_PREMIERDEBUTARVPTME = "z00BOUSdTuM"
const PROGRAMSTAGE_PTME_MERE_DELIVERY = "lt2U1bYvz0r"
const PROGRAMSTAGE_PTME_MERE_SORTIE = "LSIPBEYcQ5R"

const PROGRAMSTAGE_PTME_ENFANT_ADMISSION = "ZWtVELL8YQS"
const PROGRAMSTAGE_PTME_ENFANT_PCR_INITIAL = "D10B1LUle3y"
const PROGRAMSTAGE_PTME_ENFANT_PCR_SUIVI = "Dyn481NuzbJ"
const PROGRAMSTAGE_PTME_ENFANT_SORTIE = "ql4K11mDbzw"

//PROGRAM STAGES NAMES
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


//DICT
var stagesDict = {};
stagesDict[PROGRAMSTAGE_TARV_PREMIER_DEBUT_ARV] = PROGRAMSTAGE_TARV_PREMIER_DEBUT_ARV_LABEL
stagesDict[PROGRAMSTAGE_TARV_TARV] = PROGRAMSTAGE_TARV_TARV_LABEL
stagesDict[PROGRAMSTAGE_TARV_CONSULTATION] = PROGRAMSTAGE_TARV_CONSULTATION_LABEL
stagesDict[PROGRAMSTAGE_TARV_DEBUT_TRAITEMENT_TB] = PROGRAMSTAGE_TARV_DEBUT_TRAITEMENT_TB_LABEL
stagesDict[PROGRAMSTAGE_TARV_PERDU_DE_VUE] = PROGRAMSTAGE_TARV_PERDU_DE_VUE_LABEL
stagesDict[PROGRAMSTAGE_TARV_SORTIE] = PROGRAMSTAGE_TARV_SORTIE_LABEL

stagesDict[PROGRAMSTAGE_PTME_MERE_ADMISSION_PTME_MERE] = PROGRAMSTAGE_PTME_MERE_ADMISSION_PTME_MERE_LABEL
stagesDict[PROGRAMSTAGE_PTME_MERE_PREMIERDEBUTARVPTME] = PROGRAMSTAGE_PTME_MERE_PREMIERDEBUTARVPTME_LABEL
stagesDict[PROGRAMSTAGE_PTME_MERE_DELIVERY] = PROGRAMSTAGE_PTME_MERE_DELIVERY_LABEL
stagesDict[PROGRAMSTAGE_PTME_MERE_SORTIE] = PROGRAMSTAGE_PTME_MERE_SORTIE_LABEL

stagesDict[PROGRAMSTAGE_PTME_ENFANT_ADMISSION] = PROGRAMSTAGE_PTME_ENFANT_ADMISSION_LABEL
stagesDict[PROGRAMSTAGE_PTME_ENFANT_PCR_INITIAL] = PROGRAMSTAGE_PTME_ENFANT_PCR_INITIAL_LABEL
stagesDict[PROGRAMSTAGE_PTME_ENFANT_PCR_SUIVI] = PROGRAMSTAGE_PTME_ENFANT_PCR_SUIVI_LABEL
stagesDict[PROGRAMSTAGE_PTME_ENFANT_SORTIE] = PROGRAMSTAGE_PTME_ENFANT_SORTIE_LABEL


function generate_patient_index_and_teis(source_id) {

    logger.info("Processing retrieved data")

    const SOURCE_ID = source_id;
    const parent_DHIS2data_folder = "PREVIOUS_DHIS2_data"
    const DHIS2data_folder = parent_DHIS2data_folder + "/" + SOURCE_ID


    const ENFANT_FILE = DHIS2data_folder + "/enfant.json";
    const MERE_FILE = DHIS2data_folder + "/mere.json";
    const TARV_FILE = DHIS2data_folder + "/tarv.json";
    const PATIENT_CODE_UID = DHIS2data_folder + "/previous_all_patient_index.json";
    const TEIS_FILE = DHIS2data_folder + "/teis.json";

    //DHIS2 retrieved data
    var dhis_data = [];
    var patient_code_uid = {};

    //***** READ FILES ******/
    var enfant_data = JSON.parse(fs.readFileSync(ENFANT_FILE));
    var mere_data = JSON.parse(fs.readFileSync(MERE_FILE));
    var tarv_data = JSON.parse(fs.readFileSync(TARV_FILE));

    //Merge TEIs
    dhis_data = [
        ...enfant_data,
        ...mere_data,
        ...tarv_data
    ];

    //Fill patient_code_uid
    dhis_data.forEach((TEI) => {
        //CODE PATIENT key
        var patient = getPatientCode(TEI);

        if (typeof patient_code_uid[patient] === "undefined") {
            //TEI uid
            patient_code_uid[patient] = { "uid": TEI.trackedEntityInstance };
        }


        //Enrollments keys
        TEI.enrollments.forEach((enroll) => {

            // due to https://jira.dhis2.org/browse/DHIS2-12285
            if (enroll.deleted == true){
                logger.debug(`Enrollment deleted: ${JSON.stringify(enroll)}`)
                return;
            }

            //Enrollment info
            var enrollmentLabel = getEnrollmentLabel(enroll);
            const programName = getProgramName(enroll.program)
            if (programName === MERE){ // There could be more than one enrollment in Mere program
                if (!(enrollmentLabel in patient_code_uid[patient])) {
                    patient_code_uid[patient][enrollmentLabel] = {};
                }
            } else {
                patient_code_uid[patient][enrollmentLabel] = {};
            }
            patient_code_uid[patient][enrollmentLabel][getDHIS2dateFormat(enroll.enrollmentDate)] = enroll.enrollment;

            //Events info
            //Extract stage keys
            var enrollmentUID_label = getEnrollmentUIDLabel(enroll);
            //patient_code_uid[patient][enrollmentUID_label] = {};
            enroll.events.forEach((event) => {
                if (typeof patient_code_uid[patient][enrollmentUID_label] === "undefined") {
                    patient_code_uid[patient][enrollmentUID_label] = {};
                }
                //Check stage
                var stage = event.programStage;
                if (typeof patient_code_uid[patient][enrollmentUID_label][stagesDict[stage]] === "undefined") {

                    const evs = getEvents_format(enroll.events, stage)
                    if (Object.keys(evs).length != 0){
                        patient_code_uid[patient][enrollmentUID_label][stagesDict[stage]] = getEvents_format(enroll.events, stage);
                    }
                }
            });
        })
    });


    //Main
    logger.info(`Saving ${TEIS_FILE} file`)
    utils.saveJSONFile(TEIS_FILE, dhis_data);
    logger.info(`Saving ${PATIENT_CODE_UID} file`)
    utils.saveJSONFile(PATIENT_CODE_UID, patient_code_uid);

}

/******** FUNCTIONS  *******/

function getPatientCode(TEI) {
    var attribute = TEI.attributes.filter(function (entry) {
        return entry.attribute === PATIENT_CODE_TEA;
    });
    return attribute[0].value;
}


function getEnrollmentLabel(enrollment) {
    var program = getProgramName(enrollment.program);
    return program + "_enrollment";
}


function getProgramName(programUID) {
    if (programUID === TARV_UID) {
        return TARV;
    } else if (programUID == MERE_UID) {
        return MERE;
    } else if (programUID == ENFANT_UID) {
        return ENFANT;
    }
}


function getEnrollmentUIDLabel(enroll) {
    var program = getProgramName(enroll.program);
    return program + "-" + enroll.enrollment;
}


function getEvents_format(events_data, stage) {
    var events = {};
    var stage_events = [];
    stage_events = events_data.filter(function (entry) {
        if (entry.programStage === stage) {
            return entry;
        }
    })
    stage_events.forEach((event) => {
        var obj = {};
        var date;
        if (event.status === "SCHEDULE") {
            date = event.dueDate;
        } else {
            date = event.eventDate;
        }

        if(typeof date === "undefined"){
            logger.error("Unexpected format: event without date")
            logger.error(JSON.stringify(event))
        } else {
            obj[getDHIS2dateFormat(date)] = event.event;
        }
        

        // due to https://jira.dhis2.org/browse/DHIS2-12285
        if (event.deleted == false){
            Object.assign(events, obj)
        } else {
            logger.debug(`Event deleted: ${JSON.stringify(event)}`)
        }
        
    });
    return events;
}


function getDHIS2dateFormat(date) {
    return date.substr(0, 10);
}

module.exports = { generate_patient_index_and_teis };