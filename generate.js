var fs = require('fs');
const parse = require('csv-parse/lib/sync')
const optionSets = require('./config-optionSets.json');
const Moment = require('moment');
const { generateCode } = require('dhis2-uid'); //https://github.com/dhis2/dhis2-uid
const logger = require('./logger.js');
const chardet = require('chardet');
var pjson = require('./package.json');

function change_encoding(filename){
    const originalEncoding = chardet.detectFileSync(filename)
    const file = fs.readFileSync(filename, originalEncoding.encoding);
    fs.writeFileSync(filename+".updated", file, 'UTF-8');    
}

// {
//     "003BDI003S010912": "FG6tYYTDf5d", // Centre Akabanga Rumonge
//     "003BDI006S010120": "g9r6PbRYVAi", // Centre Akabanga Gitega
//     "003BDI010S020502": "nniW4f9J1tB", // Centre Akabanga Nyanza-Lac
//     "003BDI012S010120": "yW1SToCNaYm", // Centre Akabanga Muyinga
//     "003BDI013S010301": "IFiJar1g6y9", // Hôpital de Kibumbu
//     "003BDI014S010120": "ZAZapFNLXru", // Centre Akabanga Ngozi
//     "003BDI017S010401": "YIJAETW8k0a", // Centre Akabanga Bujumbura
//     "17020203": "DLsHsaJhtnk", // Hôpital Militaire de Kamenge
// }



function generate_data(SOURCE_OU_CODE, SOURCE_DATE) {
    const SOURCE_ID = SOURCE_OU_CODE + '_' + SOURCE_DATE
    global.logger_generation = logger.get_logger_generation(SOURCE_ID)
    global.logger_generation_fr = logger.get_logger_generation_fr(SOURCE_ID)

    logger_generation.info(`Running generate. Version ${pjson.version}`)
    try{
        generate_complete(SOURCE_OU_CODE, SOURCE_DATE)
    } catch (error) {
        logger_generation.error(error.stack)
        logger_generation_fr.error(error.stack)
        process.exit(1)
    }
    logger_generation.info(`*** PROCESS FINISHED SUCCESSFULLY ***`);
}

function generate_complete(SOURCE_OU_CODE, SOURCE_DATE){
    logger_generation.info(`Running generate for site:${SOURCE_OU_CODE} and export date: ${SOURCE_DATE}`)

    const EXPORT_DUMP_DATE = Moment(SOURCE_DATE.replace("-", ""), "YYYYMMDD")
    if (EXPORT_DUMP_DATE.isValid() == false){
        logger_generation.error(`ArgError;Invalid export_dump_date ${SOURCE_DATE}`)
        process.exit(1)
    }

    const ou_mapping_filename='./ou_mapping.json'
    const rawdata = fs.readFileSync(ou_mapping_filename);
    const OU_MAPPING = JSON.parse(rawdata);
    if ((SOURCE_OU_CODE in OU_MAPPING) == false) {
        logger_generation.error("ArgError;The Org Unit (" + SOURCE_OU_CODE + ") is not mapped against DHIS2 in ou_mapping.json file")
        process.exit(1);
    }

    const SOURCE_ID = SOURCE_OU_CODE + '_' + SOURCE_DATE

    const PREVIOUS_PATIENT_INDEX_FILENAME = "PREVIOUS_DHIS2_data/" + SOURCE_ID +"/previous_all_patient_index.json";
    
    if (!fs.existsSync(PREVIOUS_PATIENT_INDEX_FILENAME)) {
        const empty_dict = {}
        saveJSONFile(PREVIOUS_PATIENT_INDEX_FILENAME, empty_dict)
    }
    
    var previous_patient_index = JSON.parse(fs.readFileSync(PREVIOUS_PATIENT_INDEX_FILENAME))
    var new_patient_index = {};
        
    const SOURCES_FOLDERNAME = "NEW_SIDAINFO_data"

    const SOURCES_PATH = `${SOURCES_FOLDERNAME}/${SOURCE_ID}/ftp_${SOURCE_OU_CODE}_${EXPORT_DUMP_DATE.format("DD-MM-YYYY")}`

    const csvFilename_ENFANT_PTME = SOURCES_PATH + '_ENFANT_PTME.txt.updated'
    const csvFilename_FileActive = SOURCES_PATH + '_FileActive.txt.updated'
    const csvFilename_ADMISSION_DETAIL = SOURCES_PATH + '_ADMISSION_DETAIL.txt.updated'
    const csvFilename_TABLE_ARV = SOURCES_PATH + '_TABLE_ARV.txt.updated'
    const csvFilename_FEMME_ENCEINTE = SOURCES_PATH + '_FEMME_ENCEINTE.txt.updated'
    const csvFilename_CONSULTATION = SOURCES_PATH + '_CONSULTATION.txt.updated'

    const all_files_in_dump = [csvFilename_ENFANT_PTME, csvFilename_FileActive, csvFilename_ADMISSION_DETAIL, csvFilename_TABLE_ARV, csvFilename_FEMME_ENCEINTE, csvFilename_CONSULTATION]

    all_files_in_dump.forEach(f_csv => {
        const renamed_f_csv = f_csv.replace("txt.updated", "txt")
        change_encoding(renamed_f_csv);
    });
    
    
    const PERSON_TE_TYPE = "XV3kldsZq0H";
    const TEA_CODE_PATIENT = "dsWUbqvV9GW";
    const TEA_BIRTHDATE = "qA5Vsiat4Sm";
    const TEA_SEX = "HhsxFysTpdu";
    const TEA_ENTRYMODE = "MqqM2vt5RQA";
    const TEA_LOG = "dkfd2UClEwu"; // Erreurs de validation (liste)
    const TEA_TOBEREVIEWED = "ynBaBzJpqGD"; // Erreurs de validation (oui / non)
    
    
    const PROGRAM_TARV = "e3swbbSnbQ2"; //"TARV"; //"e3swbbSnbQ2";
    const PROGRAM_PTME_MERE = "MVooF5iCp8L"; //"PROGRAM_PTME_MERE"; // "MVooF5iCp8L"
    const PROGRAM_PTME_ENFANT = "PiXg9cX1i0y"; //"PROGRAM_PTME_ENFANT"; // "PiXg9cX1i0y"
    
    const PROGRAMSTAGE_TARV_PREMIER_DEBUT_ARV = "R7lNOFLLdIs"; // "PROGRAMSTAGE_TARV_PREMIER_DEBUT_ARV"; //"R7lNOFLLdIs"
    const PROGRAMSTAGE_TARV_TARV = "CTBhldBbg5x"; //"PROGRAMSTAGE_TARV_TARV"; //"CTBhldBbg5x"
    const PROGRAMSTAGE_TARV_CONSULTATION = "sPrQc1FGfP1"; //"PROGRAMSTAGE_TARV_CONSULTATION"; //"sPrQc1FGfP1"
    const PROGRAMSTAGE_TARV_DEBUT_TRAITEMENT_TB = "ZF6rqY3Jmt5"; //"PROGRAMSTAGE_TARV_DEBUT_TRAITEMENT_TB" //"ZF6rqY3Jmt5"
    const PROGRAMSTAGE_TARV_PERDU_DE_VUE = "aDBN3tSC41E"; //"PROGRAMSTAGE_TARV_PERDU_DE_VUE" //"aDBN3tSC41E"
    const PROGRAMSTAGE_TARV_SORTIE = "WV6yIfebK6w"; //"PROGRAMSTAGE_TARV_SORTIE" //"WV6yIfebK6w"
    
    const PROGRAMSTAGE_PTME_MERE_ADMISSION_PTME_MERE = "GCDZcx2b3Lr" // "PROGRAMSTAGE_PTME_MERE_ADMISSION_PTME_MERE"; // "GCDZcx2b3Lr"
    const PROGRAMSTAGE_PTME_MERE_PREMIERDEBUTARVPTME = "z00BOUSdTuM" // "PROGRAMSTAGE_PTME_MERE_PREMIERDEBUTARVPTME"; // "z00BOUSdTuM"
    const PROGRAMSTAGE_PTME_MERE_DELIVERY = "lt2U1bYvz0r" // "PROGRAMSTAGE_PTME_MERE_DELIVERY"; //"lt2U1bYvz0r"
    const PROGRAMSTAGE_PTME_MERE_SORTIE = "LSIPBEYcQ5R" // "PROGRAMSTAGE_PTME_MERE_SORTIE"; //"LSIPBEYcQ5R"
    
    const PROGRAMSTAGE_PTME_ENFANT_ADMISSION = "ZWtVELL8YQS" // "PROGRAMSTAGE_PTME_ENFANT_ADMISSION" //"ZWtVELL8YQS"
    const PROGRAMSTAGE_PTME_ENFANT_PCR_INITIAL = "D10B1LUle3y" // "PROGRAMSTAGE_PTME_ENFANT_PCR_INITIAL" //"D10B1LUle3y"
    const PROGRAMSTAGE_PTME_ENFANT_PCR_SUIVI = "Dyn481NuzbJ" // "PROGRAMSTAGE_PTME_ENFANT_PCR_SUIVI" //"Dyn481NuzbJ"
    const PROGRAMSTAGE_PTME_ENFANT_SORTIE = "ql4K11mDbzw" // "PROGRAMSTAGE_PTME_ENFANT_SORTIE" //"ql4K11mDbzw"
    
    
    const PROGRAM_TARV_LABEL = "TARV"
    const PROGRAM_PTME_MERE_LABEL = "PTME_MERE"
    const PROGRAM_PTME_ENFANT_LABEL = "PTME_ENFANT"
    
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
    
    const DE_PCR_Resultat = "a3WwFDKNfQH"; // BI SIDAInfo - PCR Resultat
    const DE_PTME_ENFANT_Cause_Sortie = "MDqiPh1g8gu"; // BI SIDAInfo - PTME enfant Cause Sortie
    
    const DE_STATUT_AVANT_CETTE_GROSSESSE = "ddECdbeS1t1" //BI SIDAInfo - Statut avant cette grossesse
    const DE_ARV_COMMENCE_AVANT_CETTE_GROSSESSE = "rbSdkxc1FUi" // BI SIDAInfo - ARV commencé avant cette grossesse
    const DE_ISSUE_DE_LA_GROSSESSE = "sbFu46qJghp" // BI SIDAInfo - Issue de la grossesse
    
    const DE_TBTypeExamen = "QhIwnQGZcgC" // BI SIDAInfo - TB type examen
    const DE_TBResultat = "Vyzn8cK2GcY" // BI SIDAInfo - TB résultat
    const DE_Retour_traitement_APDV = "sEZXekYwSzk" // BI SIDAInfo - Retour à traitement après perdu de vue,
    const DE_TARV_CAUSE_SORTIE = "Q89OJE3w488" // BI SIDAInfo - TARV Cause de sortie
    const DE_TARV_STATUT_DATE_SORTIE = "wey2CP9t3OO" // BI SIDAInfo - TARV Statut à la date de sortie
    
    const DE_TARV_Molecule = "Ekyuflg2Wa4" // BI SIDAInfo - Molecule
    const DE_TARV_Quantité = "a0FWGvi7zq9" // BI SIDAInfo - Quantité (jours)
    const DE_TARV_Prochaine = "t3ymJOfZvnR" // BI SIDAInfo - Prochaine rendez-vous (jours)
    const DE_TARV_Premier_system = "fvSF03NkyO1" // BI SIDAInfo - Première visite ARV dans le système
    
    const DHIS2_DATEFORMAT = "YYYY-MM-DD"
    const SIDAINFO_DATEFORMAT = "YYYY-MM-DD"
    
    const MIN_YEAR_EVENTS = "1950"
    const MIN_YEAR_BIRTH = "1900"
    
    const STORED_BY_USERNAME = "SidaInfoIntegrator"
    
    const SEPARATOR_PATIENT = "-"
    
    let personList = []
    
    let patientCodeBlockList = new Set()
    
    class Person {
        constructor(code, sex, birthdate, entryMode, ARVdatedebut) {
            this.code = code;
            this.sex = sex;
            this.birthdate = birthdate;
            this.entryMode = entryMode;
            if (ARVdatedebut != "") {
                this.ARVdatedebut = Moment(ARVdatedebut, SIDAINFO_DATEFORMAT);
            }
            this.ARV_admissions = []
            this.ARV_treatments = []
            this.PTME_mere_admissions = []
            this.pregnancies = []; // list of PTME_mere
            this.consultations = [];
            this.LTFU = []; // list of LTFU
            this.RTT = []; // list of RTT
            this.log = []; // list of log messages
        }
    
        get_LTFU() {
            return this.LTFU;
        }
    
        printIsEnfant() {
            if (this.entryMode == 10) {
                return "(enfant) "
            } else {
                return ""
            }
    
        }
    
        validatePerson() {
            validateCode(this.code);
            this.validatePTME_mere_programs();
            this.validateTARV_events();
            this.validateLTFU_RTT_events();
            this.validateConsultation_events();
            this.validateARV_debut();
        }
    
    
        validateTARV_events() {
            if (typeof this.ARV_DateSortie !== "undefined") { //ARV_DateSortie is a moment
                let allEvents = this.get_all_events_sorted_by_eventDate()
                let lastEvent = allEvents[allEvents.length - 1]
                if (this.ARV_DateSortie.isBefore(lastEvent.eventDate)) {
                    this.addLog(`01;Le patient a des evenements après une date sortie. Date sortie: ${this.ARV_DateSortie.format(DHIS2_DATEFORMAT)}. Dernier évènement enregistré : ${lastEvent.eventDate.format(DHIS2_DATEFORMAT)}`)
                    logger_generation.warn(`01;${this.code};Patient ${this.code} has events (visits) after its exit date. Date sortie: ${this.ARV_DateSortie.format(DHIS2_DATEFORMAT)}. Last known event date: ${lastEvent.eventDate.format(DHIS2_DATEFORMAT)}`)
                    logger_generation_fr.warn(`Le patient ${this.code} a des évènements (visites) après la date de Sortie. Date sortie: ${this.ARV_DateSortie.format(DHIS2_DATEFORMAT)}. Date du dernier événement connu: ${lastEvent.eventDate.format(DHIS2_DATEFORMAT)};Erreur 01;${this.code}`)
                }
            }
        }
    
        validateLTFU_RTT_events() {
            if (this.RTT.length > this.LTFU.length) { // More RTT than LTFU
                logger_generation.error(`X1;${this.code};Error;Patient ${this.code} has more RTT than LTFU`)
                logger_generation_fr.error(`Le patient ${this.code} a plus de "retours à traitement" que "perdues de vue";Erreur X1;${this.code}`)
                //this.addLog(`Le patient ${this.code} a plus de "retours à traitement" que "perdues de vue`)
            }
    
            if ((this.LTFU.length - this.RTT.length) > 1) { // The difference between LTFU and RTT is more than one
                logger_generation.error(`X2;${this.code};Error;Patient ${this.code} has LTFU minus RTT > 1 (${this.LTFU.length - this.RTT.length})`)
                logger_generation_fr.error(`Le patient ${this.code} a perdus de vue moins retour à traitement > 1 (${this.LTFU.length - this.RTT.length});Erreur X2;${this.code}`)
            }
    
        }
    
        // Validate that all Consultation dates are different
        validateConsultation_events() {
            const datesToReview_moment = this.consultations.map(t => t.dateConsultation).sort((a, b) => a - b);
            const datesToReview = datesToReview_moment.map(m => m.valueOf());
            const duplicatesUnix = getDuplicateArrayElements(datesToReview); // for getting the duplicates
            if (checkIfArrayIsUnique(datesToReview) == false) {
                this.addLog(`03;Le patient a plus d'une consultation (table CONSULTATION) à la même date. La/les date(s) dupliquée(s) est/sont: ${duplicatesUnix.map(d => new Moment(d).format(DHIS2_DATEFORMAT))}`)
                logger_generation.warn(`03;${this.code};Patient ${this.code} has more than one consultation (table CONSULTATION) for the same date. The duplicated date/s is/are: ${duplicatesUnix.map(d => new Moment(d).format(DHIS2_DATEFORMAT))}. All the dates of these consultations (field 'dateConsultation') are: ${datesToReview_moment.map(t => t.format(DHIS2_DATEFORMAT))}`)
                logger_generation_fr.warn(`Le patient ${this.code} a plus d'une consultation (table CONSULTATION) à la même date. La/les date(s) dupliquée(s) est/sont: ${duplicatesUnix.map(d => new Moment(d).format(DHIS2_DATEFORMAT))}. Toutes les dates des consultations (champs 'dateConsultation') sont : ${datesToReview_moment.map(t => t.format(DHIS2_DATEFORMAT))};Erreur 03;${this.code}`)
            }
        }
    
        validatePTME_mere_programs() {
    
            // validate that if this patient is enrolled in the PTME program, she is Female
            if (this.getPregnancies().length > 0) {
                if (this.sex != "F") {
                    this.addLog(`04;Le patient n'est pas une FEMME mais il est enregistré dans le programme PTME Mère.`)
                    logger_generation.warn(`04;${this.code};Patient ${this.code} is not FEMALE but it is enrolled in the PTME Mere program.`)
                    logger_generation_fr.warn(`Le patient ${this.code} n'est pas une FEMME mais il est enregistré dans le programme PTME Mère.;Erreur 04;${this.code}`)
                }
            }
    
    
            // validate that a Person MUST NOT have more than one open enrollment (FemmeEnceinte-DateVisiteSuiviPTME empty).
            const enrollmentDates = this.getPregnancies().map(t => t.enrollmentDate);
            const enrollmentCloseDates = this.getPregnancies().map(t => t.enrollmentCloseDate).filter(function (el) {
                return typeof el !== "undefined";
            });
    
            if (enrollmentDates.length - enrollmentCloseDates.length > 1) {
                patientCodeBlockList.add(this.code)
                logger_generation.error(`05;${this.code};Patient ${this.code} has more than one PTME admission active at the same time (start date without end date): ${JSON.stringify(this.getPregnancies())}`)
                logger_generation_fr.error(`Le patient ${this.code} a plus d'une admission PTME active au même temps (dateDebut sans DateFin) : ${JSON.stringify(this.getPregnancies())};Erreur 05;${this.code}`)
            }
    
    
            // validate that the enrollment dates for the PTME mere program are unique (a person can not enroll the same program more than one time the same day)
            const datesToReview = enrollmentDates.map(v => v.valueOf()); // use values for comparing Objects
            const duplicatesUnix = getDuplicateArrayElements(datesToReview); // for getting the duplicates
            if (checkIfArrayIsUnique(datesToReview) == false) {
                patientCodeBlockList.add(this.code)
                logger_generation.error(`06;${this.code};Patient ${this.code} has more than one pregnancy (entry in FEMME_ENCEINTE table) for the same date. The duplicated date/s is/are: ${duplicatesUnix.map(d => new Moment(d).format(DHIS2_DATEFORMAT))}. All the enrollment days in the PTME meré program (FEMME_ENCEINTE table, column DateVisitiSuiviPTME) are: ${enrollmentDates.map(d => d.format(DHIS2_DATEFORMAT))}`)
                logger_generation_fr.error(`Le patient ${this.code} a plus d'une grossesse (enregistrement dans la table FEMME_ENCEINTE) à la même date. La/les date(s) dupliquée(s) est/sont: ${duplicatesUnix.map(d => new Moment(d).format(DHIS2_DATEFORMAT))}. Toutes les dates de d'enregistrement dans le programme PTME meré (table FEMME_ENCEINTE, champ 'DateVisitiSuiviPTME') sont : ${enrollmentDates.map(d => d.format(DHIS2_DATEFORMAT))};Erreur 06;${this.code}`)
            }
    
    
            //Important: the Femme_Enceinte table only has pregnancies after 2010 – PMTCT episodes before then (which can be found in Admission_detail) won’t be included. 
            const datedebuts = this.PTME_mere_admissions.map(t => t.datedebut);
            const datefins = this.PTME_mere_admissions.map(t => t.datefin).filter(function (el) {
                return el != "";
            });

            const enrollmentDates_str = enrollmentDates.map(t => t.format(SIDAINFO_DATEFORMAT))
            const difference_datedebut = datedebuts.filter(x => !enrollmentDates_str.includes(x));
            
            if (datedebuts.length != enrollmentDates.length) {
                logger_generation.warn(`41;${this.code};Patient ${this.code} has more PTME admissions [${datedebuts.length}] (ADMISSION_DETAIL table) than pregnancies [${enrollmentDates.length}] (FEMME_ENCEINTE table). The missed admission dates are: ${difference_datedebut}`)
                logger_generation_fr.warn(`Le patient ${this.code} a plus d’admissions PTME ([${datedebuts.length}]) dans la table ADMISSION_DETAIL que grossesses ([${enrollmentDates.length}]) dans la table FEMME_ENCEINTE.;Erreur 41;${this.code}`)
            }

            const enrollmentCloseDates_str = enrollmentCloseDates.map(t => t.format(SIDAINFO_DATEFORMAT))
            const difference_datefins = datefins.filter(x => !enrollmentCloseDates_str.includes(x));
            if (datefins.length != enrollmentCloseDates.length) {
                logger_generation.warn(`44;${this.code};Patient ${this.code} has more PTME finalized [${datefins.length}] (ADMISSION_DETAIL table) than pregnancies [${enrollmentDates.length}] (FEMME_ENCEINTE table). The missed finalized dates are: ${difference_datefins}`)
                logger_generation_fr.warn(`Le patient ${this.code} a plus d’admissions PTME ([${datedebuts.length}]) dans la table ADMISSION_DETAIL que grossesses ([${enrollmentDates.length}]) dans la table FEMME_ENCEINTE.;Erreur 44;${this.code}`)
            }
    
        }
    
        // Validation 38: There is one or more record in Table_ARV, but no ARVdatedebut date in FileActive (or the ARVdatedebut is missing completely).
        validateARV_debut(){
            if ((this.ARV_treatments.length > 0) && (typeof this.ARVdatedebut === 'undefined')) {
                const table_ARV_firstEventDate = this.ARV_treatments[0].eventDate
                logger_generation.warn(`38;${this.code};Patient ${this.code} has a Table ARV first event: ${table_ARV_firstEventDate.format(DHIS2_DATEFORMAT)} but FileActive ARV debut is empty.`);
                logger_generation_fr.warn(`Le patient ${this.code} a un premier évènement dans Table_ARV (${table_ARV_firstEventDate.format(DHIS2_DATEFORMAT)}) mais le champs debut ARV dans FileActive est vide.;Erreur 38;${this.code}`);
            }
        }
    
        setDatedepist(datedepist) {
            this.datedepist = datedepist;// TODO change to Moment
        }
    
        getDatedepist() {
            return this.datedepist;
        }
        setPTME_enfant_program(PTME_enfant_program) {
            this.PTME_enfant_program = PTME_enfant_program;
        }
    
        getPTME_enfant_program() {
            return this.PTME_enfant_program;
        }
    
        getPTME_mere_admissions() {
            return this.PTME_mere_admissions;
        }
    
        getPregnancies() {
            return this.pregnancies;
        }
    
        getConsultations() {
            return this.consultations;
        }
    
        addARV_admission(datedebut, datefin) {
            this.ARV_admissions.push({ "datedebut": datedebut, "datefin": datefin })
        }
    
        addARV_treatment(datetraitement, dateprochainrendev, codemolecule, qte) {
            var prochainrendevDate = datetraitement; //CalculateDates changes the original, make a copy first
            prochainrendevDate = calculateDate(prochainrendevDate, dateprochainrendev);
            this.ARV_treatments.push({ "datetraitement": Moment(datetraitement, SIDAINFO_DATEFORMAT), "dateprochainrendev": prochainrendevDate, "segaDate": prochainrendevDate, "num_days": dateprochainrendev, "codemolecule": codemolecule, "qte": qte }) // dateprochainrendev is the dueDate of the next event
            this.ARV_treatments.sort((a, b) => a.datetraitement - b.datetraitement)
        }
    
        addRTT(rttDate) {
            // Check if there is already this date in the list
            if (this.RTT.includes(rttDate)) {
                logger_generation.error(`X3;${this.code};Error;Patient ${this.code} has same RTT date for different LTFU: ${rttDate.format(DHIS2_DATEFORMAT)}`)
                logger_generation_fr.error(`Le patient ${this.code} a la même date de retour au traitement pour des différentes dates de perdu de vue: ${rttDate.format(DHIS2_DATEFORMAT)};Erreur X3;${this.code}`)
            } else {
                this.RTT.push(rttDate);
                this.RTT.sort((a, b) => a - b)
            }
        }
    
        addLog(message) {
            this.log.push(message);
        }
    
        get_str_log() {
            const complete_log = this.log.join("\n")
            if (complete_log.length > 1200){
                return "##IMPORTANT##. Il y a plus d'erreurs, mais en raison de la taille de ce champ, il n'est pas possible de les afficher\n\n" + complete_log.slice(0, 1000);
            } else {
                return complete_log;
            }
        }
    
        getTARV_enrollment_date() {
            const datedebuts = this.ARV_admissions.map(t => t.datedebut);
            sortDates(datedebuts); // OK
            if (datedebuts.length > 0) {
                return Moment(datedebuts[0], SIDAINFO_DATEFORMAT);
            }
        }
    
        getPremier_debut_ARV_treatment() {
            if (this.ARV_treatments.length > 0) {
                return this.ARV_treatments[0]
            }
        }
    
        getPremier_debut_ARV_momentDate() {
            const datetraitements = this.ARV_treatments.map(t => t.datetraitement);
            sortDates(datetraitements); // OK
            if (datetraitements.length > 0) {
                return Moment(datetraitements[0], SIDAINFO_DATEFORMAT);
            }
        }
    
        getPremier_debut_ARV_eventDate() {
            if(typeof this.getPremier_debut_ARV_momentDate() !== "undefined") {
                return this.getPremier_debut_ARV_momentDate().format(DHIS2_DATEFORMAT);
            } else {
                return "NO_DATE"
            }
        }
    
        isDebutARV() {
            let table_ARV_firstEvent = this.getPremier_debut_ARV_momentDate(); //First event in table ARV
            let file_active_ARV_debut = this.ARVdatedebut; //ARVdatedebut in File Active (could be undefined)
            let admission_detail_ARV_debut = this.getTARV_enrollment_date(); //Admission_detail
            let file_active_debut_log = file_active_ARV_debut;
            if (typeof file_active_ARV_debut !== "undefined") {
                file_active_debut_log = file_active_ARV_debut.format(DHIS2_DATEFORMAT);
            }
            //logger.debug(`ZZZ;Patient ${this.code} ARV first event: ${this.getPremier_debut_ARV_eventDate()}, File Active ARV debut: ${file_active_debut_log} Admission detail ARV debut: ${admission_detail_ARV_debut.format(DHIS2_DATEFORMAT)}`);
    
            if (typeof file_active_ARV_debut !== "undefined") {
                if (!file_active_ARV_debut.isSame(admission_detail_ARV_debut)) {
                    logger_generation.warn(`36;${this.code};Patient ${this.code} has unexpected different dates for File Active ARV debut: ${file_active_debut_log} and Admission detail ARV debut: ${admission_detail_ARV_debut.format(DHIS2_DATEFORMAT)}`);
                    logger_generation_fr.warn(`Le patient ${this.code} a une difference inattendue de dates pour le debut ARV dans FileActive (${file_active_debut_log}) et Admission_detail (${admission_detail_ARV_debut.format(DHIS2_DATEFORMAT)});Erreur 36;${this.code}`);
                }
                if (file_active_ARV_debut.isAfter(table_ARV_firstEvent)) {
                    logger_generation.warn(`37;${this.code};Patient ${this.code} has a date for File Active ARV debut: ${file_active_debut_log} which is later than the earliest visit in Table ARV: ${table_ARV_firstEvent.format(DHIS2_DATEFORMAT)}`)
                    logger_generation_fr.warn(`Le patient ${this.code} a dans FileActive une date debut ARV de ${file_active_debut_log} qui est plus tard que la première visite TARV dans TableARV : ${table_ARV_firstEvent.format(DHIS2_DATEFORMAT)};Erreur 37;${this.code}`)
                }
                if (file_active_ARV_debut.isSame(table_ARV_firstEvent)) { //Create Program Stage Premier Debut
                    return true;
                }
    
            }
            return false;
        }
    
        // excluding the premier
        getMiddle_ARV() {
            const datetraitements = this.ARV_treatments;
            sortDatesByIndex(datetraitements, "datetraitement");
            if (datetraitements.length > 1) {
                return datetraitements.slice(1);
            } else {
                return [];
            }
        }
    
        get_all_events_sorted_by_eventDate() {
    
            let allEvents = [];
    
            allEvents = allEvents.concat([...this.ARV_treatments])
            allEvents = allEvents.concat([...this.consultations])
            if (typeof this.ARV_DateSortie !== 'undefined') {
                const sortie_event = {
                    "eventDate": this.ARV_DateSortie,
                    "segaDate": this.ARV_DateSortie,
                    "sortie": true
                }
                allEvents.push(sortie_event)
            }
    
            allEvents.forEach(element => {
                if ("datetraitement" in element) {
                    element.eventDate = element.datetraitement;
                }
                if ("dateConsultation" in element) {
                    element.eventDate = element.dateConsultation;
                }
            });
    
            sortDatesByIndex(allEvents, "eventDate")
    
            return allEvents;
    
        }
    
        //Builds an array of events holding the event date in each case
        get_all_events_for_LTFU() {
    
            let allEvents = this.get_all_events_sorted_by_eventDate();
    
            // corner case. Sortie is NOT the latest event. Action: Remove Sortie
            let lastEvent = allEvents[allEvents.length - 1]
            if ((typeof this.ARV_DateSortie !== "undefined") && (this.ARV_DateSortie.isBefore(lastEvent.eventDate))) {
                const sortie_index = allEvents.findIndex(x => x.sortie === true);
                allEvents.splice(sortie_index, 1)
            }
    
    
            // Remove all events prior to ARV debut
            const starting_moment = this.getPremier_debut_ARV_momentDate()
            if (typeof starting_moment != "undefined") {
    
                const index = allEvents.findIndex(x => x.eventDate.isSame(starting_moment));
                if (index != 0) {
                    allEvents = allEvents.slice(index)
                }
            }
    
            return allEvents
    
        }
    
        getFirstARVafterLTFU(ltfu) {
            const ARVdates = this.ARV_treatments;
            var rtt;
            ARVdates.some(arv => {
                if (ltfu.isSameOrBefore(arv.datetraitement)) {
                    rtt = arv.datetraitement;
                    return true
                }
            });
            return rtt;
        }
    
        setARV_RTT(date_rtt) {
            const ARVdates = this.ARV_treatments;
            ARVdates.some(arv => {
                if (arv.datetraitement.isSame(date_rtt)) {
                    arv["RTT"] = true
                    return true
                }
            });
        }
    
    
        getFirstConsultationAfterLTFU(ltfu) {
            const consultations = this.consultations;
            var rtt;
            consultations.some(consultation => {
                if (ltfu.isSameOrBefore(consultation.dateConsultation)) {
                    rtt = consultation.dateConsultation;
                    return true;
                }
            });
            return rtt;
        }
    
        setConsultation_RTT(date_rtt) {
            const consultations = this.consultations;
            consultations.some(consultation => {
                if (consultation.dateConsultation.isSame(date_rtt)) {
                    consultation["RTT"] = true
                    return true
                }
            });
        }
    
        generateRTT(ltfu) {
            //logger.info("PATIENT=" + this.code);
            //logger.info("PATIENT=" + this.getUID());
            //logger.info("LTFU=" + ltfu.format(DHIS2_DATEFORMAT));
            var firstARVafterLTFU = this.getFirstARVafterLTFU(ltfu); //Returns .datetraitement
            if (typeof firstARVafterLTFU !== 'undefined') {
                //logger.info("ARV =" + firstARVafterLTFU.format(DHIS2_DATEFORMAT));
            }
    
            var firstConsultationAfterLTFU = this.getFirstConsultationAfterLTFU(ltfu); //Returns .dateConsultation
            if (typeof firstConsultationAfterLTFU !== 'undefined') {
                //logger.info("CONS=" + firstConsultationAfterLTFU.format(DHIS2_DATEFORMAT));
            }
    
            var rtt;
            //logger.info("---------------------");
    
            if (typeof firstARVafterLTFU !== 'undefined') {
                if (typeof firstConsultationAfterLTFU !== 'undefined') {
                    if (firstConsultationAfterLTFU.isSameOrBefore(firstARVafterLTFU)) {
                        this.setConsultation_RTT(firstConsultationAfterLTFU)
                        rtt = firstConsultationAfterLTFU;
                    } else {
                        this.setARV_RTT(firstARVafterLTFU)
                        rtt = firstARVafterLTFU;
                    }
                } else {
                    this.setARV_RTT(firstARVafterLTFU)
                    rtt = firstARVafterLTFU;
                }
            } else if (typeof firstConsultationAfterLTFU !== 'undefined') {
                this.setConsultation_RTT(firstConsultationAfterLTFU)
                rtt = firstConsultationAfterLTFU;
            }
    
            if (typeof rtt !== 'undefined')
                this.addRTT(rtt);
        }
    
        getBookingDate_ARV(datetraitement) {
            const previousTreatment = this.getPrevious_ARV(datetraitement);
            return previousTreatment.dateprochainrendev;
        }
    
        getPrevious_ARV(datetraitement) {
            const datetraitements = this.ARV_treatments.map(t => t.datetraitement);
            datetraitements.sort((a, b) => a - b)
            const currentTreatmentIndex = this.ARV_treatments.findIndex(x => x.datetraitement === datetraitement);
            const previousTreatmentDate = datetraitements[currentTreatmentIndex - 1]
            const previousTreatment = this.ARV_treatments.find(x => x.datetraitement === previousTreatmentDate);
            return previousTreatment;
        }
    
        getNext_ARV() { //getLast_ARV
            const datetraitements = this.ARV_treatments.map(t => t.datetraitement);
            if (datetraitements.length > 0) {
                datetraitements.sort((a, b) => a - b)
                const lastDate = datetraitements[datetraitements.length - 1]
                const lastTreatment = this.ARV_treatments.find(x => x.datetraitement === lastDate);
                return lastTreatment.dateprochainrendev; //prochainerendevDate converted to moment when first stored (addARV_treatment())
            }
        }
    
        generateLTFU() {
    
            const events = this.get_all_events_for_LTFU();
    
            if (events.length == 0) {
                return;
            }
    
            // generated potential ltfu date
            events.forEach((event, index) => {
                let previousEvent = events[index - 1];
    
                if ("datetraitement" in event) { // ARV
                    event["ltfuDate"] = calculateDate(Moment(event["segaDate"], SIDAINFO_DATEFORMAT), 29);//PEPFAR 28-day standard to be considered LTFU
                } else { // Consultation or Sortie
                    let potential_LTFU = calculateDate(Moment(event["segaDate"], SIDAINFO_DATEFORMAT), 29);//PEPFAR 28-day standard to be considered LTFU
                    if ((index == 0) || (potential_LTFU.isAfter(previousEvent.ltfuDate))) {
                        event.ltfuDate = potential_LTFU
                    } else {
                        event.ltfuDate = previousEvent.ltfuDate
                    }
                }
            })
    
            // generated ltfu event
            events.forEach((event, index) => {
                let previousEvent = events[index - 1];
                if (index > 0) {
                    if (event.eventDate.isAfter(previousEvent.ltfuDate)) {
                        this.addLTFU_ARV(previousEvent.ltfuDate); // Moment object
                    }
                }
            })
    
            //create a LTFU event for the latest event
            const ltfuDate_last_tarv_scheduled_appoinment = calculateDate(this.getNext_ARV(), 29);  //PEPFAR 28-day standard to be considered LTFU
            const last_event = events[events.length - 1] // Take the latest event (by eventDate). TODO What about more than one event the same date?
    
            // if the latest event is a TARV. create a LTFU event if the scheduled event is overdue
            if (("datetraitement" in last_event) && (EXPORT_DUMP_DATE.isAfter(ltfuDate_last_tarv_scheduled_appoinment)) &&
                ((typeof this.ARV_DateSortie == 'undefined') || ((typeof this.ARV_DateSortie != 'undefined') && this.ARV_DateSortie.isAfter(ltfuDate_last_tarv_scheduled_appoinment)) || ((typeof this.ARV_DateSortie != 'undefined') && !("sortie" in last_event)))) {
                this.addLTFU_ARV(ltfuDate_last_tarv_scheduled_appoinment); // Moment object
            }
    
            // if the latest event is a consultation
            if (!("datetraitement" in last_event) && !("sortie" in last_event)) { // Only Consultation
                if ((last_event.ltfuDate.isSameOrAfter(ltfuDate_last_tarv_scheduled_appoinment) && EXPORT_DUMP_DATE.isAfter(last_event.ltfuDate))
                    && ((typeof this.ARV_DateSortie == 'undefined') || ((typeof this.ARV_DateSortie != 'undefined') && this.ARV_DateSortie.isAfter(last_event.ltfuDate)) || ((typeof this.ARV_DateSortie != 'undefined') && !("sortie" in last_event)))) {
                    this.addLTFU_ARV(last_event.ltfuDate); // Moment object
                }
            }
        }
    
        addLTFU_ARV(eventdate) {
            this.LTFU.push(eventdate);
        }
    
        addPTME_mere_admission(datedebut, datefin) {
            this.PTME_mere_admissions.push({ "datedebut": datedebut, "datefin": datefin })
        }
    
        addPregnancy(pregnancy) {
            this.pregnancies.push(pregnancy)
        }
    
        addConsultation(dateConsultation, TBTypeExamen, TBResultat, TBDateDebutTraitement) {
            let consultation = {}
            consultation["dateConsultation"] = dateConsultation;
            consultation["dueDate"] = dateConsultation;
            consultation["segaDate"] = dateConsultation; // segaDate is an invented name for the need to calculate the LTFU date
            if ((TBTypeExamen != "") && validateOption("TBTypeExamen", TBTypeExamen, this.code, "CONSULTATION")) {
                consultation["TBTypeExamen"] = TBTypeExamen;
            }
            if ((TBResultat != "") && validateOption("TBResultat", TBResultat, this.code, "CONSULTATION")) {
                consultation["TBResultat"] = TBResultat;
            }
            if (TBDateDebutTraitement != "") {
                consultation["TBDateDebutTraitement"] = Moment(TBDateDebutTraitement, SIDAINFO_DATEFORMAT);
            }
            this.consultations.push(consultation)
            this.consultations.sort((a, b) => (a.dateConsultation - b.dateConsultation))
    
        }
    
        setARV_CauseSortie(ARV_CauseSortie) {
            this.ARV_CauseSortie = ARV_CauseSortie;
        }
    
        setARV_DateSortie(datesortie) {
            this.ARV_DateSortie = datesortie;
        }
    
    
        getSortieStatusARV() {
            let status = optionSets["SortieStatus"].options["Actif"] //In case there's no LTFU neither RTT the status is Actif
    
            //Cases 'Perdu de vue': if there's a LTFU but there's not return to treatment
            if (this.LTFU.length > this.RTT.length) {
                status = optionSets["SortieStatus"].options["Perdu de vue"];
            }
    
            // corner case. Sortie is NOT the latest event
            let allEvents = this.get_all_events_sorted_by_eventDate();
            let lastEvent = allEvents[allEvents.length - 1]
            if ((typeof this.ARV_DateSortie !== "undefined") && (this.ARV_DateSortie.isBefore(lastEvent.eventDate))) {
                // Add LFTU dates into allevents
                const ltfu_dates = this.get_LTFU().map((ltfu_date) => ({ "eventDate": ltfu_date, "lftu": true }));
                allEvents = allEvents.concat(ltfu_dates)
    
                //sort all events
                sortDatesByIndex(allEvents, "eventDate")
    
                const sortie_index = allEvents.findIndex(x => x.sortie === true);
                const event_previous_sortie = allEvents[sortie_index - 1]
                //check if the previous event before sortie is a LTFU
                if ((typeof event_previous_sortie !== "undefined") && "lftu" in event_previous_sortie) {
                    status = optionSets["SortieStatus"].options["Perdu de vue"];
                }
            }
    
    
            //(if patient had not yet started ARV (no ‘Premier debut ARV’ event yet) then ‘TARV pas encore commencé’.)
            if (this.ARV_treatments.length == 0) {
                status = optionSets["SortieStatus"].options["TARV pas encore commencé"]
            }
            return status;
        }
    
        getPremier_debut_ARV_event_UID() {
            var eventUID = getEventUID(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_PREMIER_DEBUT_ARV_LABEL, this.getPremier_debut_ARV_eventDate())
            writeNewEvent(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_PREMIER_DEBUT_ARV_LABEL, this.getPremier_debut_ARV_eventDate(), eventUID)
            return eventUID;
        }
    
        getTARV_event_UID(eventDate) {
            var eventUID = getEventUID(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_TARV_LABEL, eventDate)
            writeNewEvent(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_TARV_LABEL, eventDate, eventUID)
            return eventUID;
        }
    
        getConsultation_event_UID(eventDate) {
            var eventUID = getEventUID(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_CONSULTATION_LABEL, eventDate)
            writeNewEvent(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_CONSULTATION_LABEL, eventDate, eventUID)
            return eventUID;
        }
    
        getLTFU_event_UID(eventDate) {
            var eventUID = getEventUID(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_PERDU_DE_VUE_LABEL, eventDate)
            writeNewEvent(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_PERDU_DE_VUE_LABEL, eventDate, eventUID)
            return eventUID;
        }
    
        getDebut_traitement_TB_event_UID(eventDate) {
            var eventUID = getEventUID(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_DEBUT_TRAITEMENT_TB_LABEL, eventDate)
            writeNewEvent(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_DEBUT_TRAITEMENT_TB_LABEL, eventDate, eventUID)
            return eventUID;
        }
    
        getARV_DateSortie_event_UID() {
            var eventUID = getEventUID(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_SORTIE_LABEL, this.ARV_DateSortie.format(DHIS2_DATEFORMAT))
            writeNewEvent(this.code, PROGRAM_TARV_LABEL + "-" + this.getARV_enrollment_UID(), PROGRAMSTAGE_TARV_SORTIE_LABEL, this.ARV_DateSortie.format(DHIS2_DATEFORMAT), eventUID)
            return eventUID;
        }
    
        /**** DHIS2 GENERATION ****************************************************/
    
        getOrgUnit() {
            if (SOURCE_OU_CODE in OU_MAPPING) {
                return (OU_MAPPING[SOURCE_OU_CODE])
            } else {
                logger_generation.error("07;;The Org Unit of the files (" + SOURCE_OU_CODE + ") is not mapped against DHIS2")
                logger_generation_fr.error("Le site des fichiers (" + SOURCE_OU_CODE + ") n'est pas lié avec une unité organisationelle de DHIS2;Erreur 07;")
                process.exit(1);
            }
        }
    
        getUID() {
    
            if (this.code in previous_patient_index) {
                return previous_patient_index[this.code]["uid"]
            } else {
                const new_uid = generateCode();
                previous_patient_index[this.code] = {}
                previous_patient_index[this.code]["uid"] = new_uid;
                return new_uid;
            }
        }
    
        getPTME_enfant_enrollment_UID() {
            const d_key = PROGRAM_PTME_ENFANT_LABEL + "_enrollment";
            const enrollmentDateFormat = this.getPTME_enfant_program().getEnrollmentDate().format(DHIS2_DATEFORMAT);
            if (this.code in previous_patient_index) {
                if (d_key in previous_patient_index[this.code]) {
                    if (typeof previous_patient_index[this.code][d_key][enrollmentDateFormat] !== "undefined") {
                        writeNewEnrollment(this.code, d_key, enrollmentDateFormat, previous_patient_index[this.code][d_key][enrollmentDateFormat])
                        return previous_patient_index[this.code][d_key][enrollmentDateFormat];
                    }
                } else {
                    previous_patient_index[this.code][d_key] = {}
    
                }
            }
            const new_PTME_enfant_enrollment_uid = generateCode();
            previous_patient_index[this.code][d_key][enrollmentDateFormat] = new_PTME_enfant_enrollment_uid;
            writeNewEnrollment(this.code, d_key, enrollmentDateFormat, new_PTME_enfant_enrollment_uid)
            return new_PTME_enfant_enrollment_uid;
    
        }
    
        getARV_enrollment_UID() {
            const d_key = PROGRAM_TARV_LABEL + "_enrollment";
            const enrollmentDateFormat = this.getTARV_enrollment_date().format(DHIS2_DATEFORMAT)
            if (this.code in previous_patient_index) {
                if (d_key in previous_patient_index[this.code]) {
                    if (typeof previous_patient_index[this.code][d_key][enrollmentDateFormat] !== "undefined") {
                        writeNewEnrollment(this.code, d_key, enrollmentDateFormat, previous_patient_index[this.code][d_key][enrollmentDateFormat])
                        return previous_patient_index[this.code][d_key][enrollmentDateFormat];
                    }
                } else {
                    previous_patient_index[this.code][d_key] = {}
                }
            }
            const new_TARV_enrollment_uid = generateCode();
            previous_patient_index[this.code][d_key][enrollmentDateFormat] = new_TARV_enrollment_uid;
            writeNewEnrollment(this.code, d_key, enrollmentDateFormat, new_TARV_enrollment_uid)
            return new_TARV_enrollment_uid;
    
        }
    
        getDHIS2_TE_Attributes() {
    
            var attributes = []
    
            attributes.push({ "attribute": TEA_CODE_PATIENT, "value": this.code, "storedBy": STORED_BY_USERNAME }) // code
            attributes.push({ "attribute": TEA_SEX, "value": this.sex, "storedBy": STORED_BY_USERNAME }) // sex
            attributes.push({ "attribute": TEA_BIRTHDATE, "value": this.birthdate.format(DHIS2_DATEFORMAT), "storedBy": STORED_BY_USERNAME }) // birthdate
            attributes.push({ "attribute": TEA_ENTRYMODE, "value": this.entryMode, "storedBy": STORED_BY_USERNAME }) // entryMode
            if (this.get_str_log().length) {
                attributes.push({ "attribute": TEA_TOBEREVIEWED, "value": "true", "storedBy": STORED_BY_USERNAME }) // log
                attributes.push({ "attribute": TEA_LOG, "value": this.get_str_log(), "storedBy": STORED_BY_USERNAME }) // log
            }
    
            return attributes;
        }
    
        generateDHIS2_TEI_Payload() {
            var tei = {};
            tei["orgUnit"] = this.getOrgUnit();
            var uid = this.getUID();
            tei["trackedEntityInstance"] = uid;
    
            new_patient_index[this.code] = {}
            new_patient_index[this.code]["uid"] = uid;
    
            tei["trackedEntityType"] = PERSON_TE_TYPE;
            tei["attributes"] = this.getDHIS2_TE_Attributes();
            tei["enrollments"] = this.generateDHIS2_enrollments()
            return tei;
        }
    
        generateDHIS2_enrollments() {
            var dhis2_enrollments = []
    
            // TARV
            if (this.ARV_admissions.length > 0) {
                var dhis2_TARV_enrollment = {};
                dhis2_TARV_enrollment["enrollment"] = this.getARV_enrollment_UID();
                dhis2_TARV_enrollment["trackedEntityInstance"] = this.getUID();
                dhis2_TARV_enrollment["program"] = PROGRAM_TARV;
                dhis2_TARV_enrollment["status"] = "ACTIVE";
                dhis2_TARV_enrollment["orgUnit"] = this.getOrgUnit();
                dhis2_TARV_enrollment["enrollmentDate"] = this.getTARV_enrollment_date().format(DHIS2_DATEFORMAT);
                dhis2_TARV_enrollment["incidentDate"] = EXPORT_DUMP_DATE.format(DHIS2_DATEFORMAT);
                dhis2_TARV_enrollment["events"] = this.generateDHIS2_TARV_events();
                dhis2_enrollments.push(dhis2_TARV_enrollment)
            }
    
    
            // PTME mere
            const PTME_mere_programs = this.getPregnancies();
    
            PTME_mere_programs.forEach(PTME_mere_program => {
                var dhis2_PTME_mere_enrollment = {};
                dhis2_PTME_mere_enrollment["enrollment"] = PTME_mere_program.getPTME_mere_enrollment_UID();
                dhis2_PTME_mere_enrollment["trackedEntityInstance"] = this.getUID();
                dhis2_PTME_mere_enrollment["program"] = PROGRAM_PTME_MERE;
                dhis2_PTME_mere_enrollment["status"] = PTME_mere_program.getEnrollmentStatus();
                dhis2_PTME_mere_enrollment["orgUnit"] = this.getOrgUnit();
                dhis2_PTME_mere_enrollment["enrollmentDate"] = PTME_mere_program.getPTME_Mere_EnrollmentDate().format(DHIS2_DATEFORMAT);
                dhis2_PTME_mere_enrollment["incidentDate"] = EXPORT_DUMP_DATE.format(DHIS2_DATEFORMAT);
                dhis2_PTME_mere_enrollment["events"] = this.generateDHIS2_PTME_mere_events(PTME_mere_program);
                dhis2_enrollments.push(dhis2_PTME_mere_enrollment)
            });
    
    
            // PTME enfant
            const PTME_enfant_program = this.getPTME_enfant_program();
            if (typeof PTME_enfant_program != 'undefined') {
                var dhis2_PTME_enfant_enrollment = {};
                dhis2_PTME_enfant_enrollment["enrollment"] = this.getPTME_enfant_enrollment_UID();
                dhis2_PTME_enfant_enrollment["trackedEntityInstance"] = this.getUID();
                dhis2_PTME_enfant_enrollment["program"] = PROGRAM_PTME_ENFANT;
                dhis2_PTME_enfant_enrollment["status"] = PTME_enfant_program.getEnrollmentStatus();
                dhis2_PTME_enfant_enrollment["orgUnit"] = this.getOrgUnit();
                dhis2_PTME_enfant_enrollment["enrollmentDate"] = PTME_enfant_program.getEnrollmentDate().format(DHIS2_DATEFORMAT);
                dhis2_PTME_enfant_enrollment["incidentDate"] = EXPORT_DUMP_DATE.format(DHIS2_DATEFORMAT);
                dhis2_PTME_enfant_enrollment["events"] = this.generateDHIS2_PTME_enfant_events();
                dhis2_enrollments.push(dhis2_PTME_enfant_enrollment)
            }
    
            return dhis2_enrollments
        }
    
        generateDHIS2_PTME_mere_events(PTME_mere_program) {
            var dhis2_events = []
    
            // Stub for the event
            const dhis2_PTME_mere_event_stub = {
                "program": PROGRAM_PTME_MERE,
                "orgUnit": this.getOrgUnit(),
                "trackedEntityInstance": this.getUID(),
                "storedBy": STORED_BY_USERNAME
            };
    
            //******ProgramStage: Admission PTME Mere
            let dhis2_Admission_PTME_mere_event = Object.assign({}, dhis2_PTME_mere_event_stub); // copy from stub
            dhis2_Admission_PTME_mere_event["event"] = PTME_mere_program.getAdmission_PTME_Mere_event_UID(this.code);
            dhis2_Admission_PTME_mere_event["programStage"] = PROGRAMSTAGE_PTME_MERE_ADMISSION_PTME_MERE;
            dhis2_Admission_PTME_mere_event["eventDate"] = PTME_mere_program.getAdmission_PTME_Mere_EventDate();
            dhis2_Admission_PTME_mere_event["dueDate"] = PTME_mere_program.getAdmission_PTME_Mere_EventDate();
            dhis2_Admission_PTME_mere_event["dataValues"] = [];
            dhis2_Admission_PTME_mere_event["dataValues"].push({
                "dataElement": DE_STATUT_AVANT_CETTE_GROSSESSE, //BI SIDAInfo - Statut avant cette grossesse
                "value": PTME_mere_program.getStatusBeforePregnancy()
            })
            dhis2_Admission_PTME_mere_event["dataValues"].push({
                "dataElement": DE_ARV_COMMENCE_AVANT_CETTE_GROSSESSE, //BI SIDAInfo - ARV commencé avant cette grossesse
                "value": PTME_mere_program.getARVStartedBeforePregnancy()
            })
            dhis2_events.push(dhis2_Admission_PTME_mere_event)
    
            //******ProgramStage: Premier debut ARV (PTME)
            // only if first TARV is between PTME mere enrolment date and enrollment close date
            if (typeof this.getPremier_debut_ARV_momentDate() != 'undefined' && isBetweenDates(PTME_mere_program.getEnrollmentDate(), PTME_mere_program.getEnrollmentCloseDate(), this.getPremier_debut_ARV_momentDate())) {
                let dhis2_PremierDebutARVPTME_event = Object.assign({}, dhis2_PTME_mere_event_stub); // copy from stub
                dhis2_PremierDebutARVPTME_event["event"] = PTME_mere_program.getPremierDebutARVPTME_event_UID(this.code, this.getPremier_debut_ARV_eventDate());
                dhis2_PremierDebutARVPTME_event["programStage"] = PROGRAMSTAGE_PTME_MERE_PREMIERDEBUTARVPTME;
                dhis2_PremierDebutARVPTME_event["eventDate"] = this.getPremier_debut_ARV_eventDate();
                dhis2_PremierDebutARVPTME_event["dueDate"] = this.getPremier_debut_ARV_eventDate();
                dhis2_events.push(dhis2_PremierDebutARVPTME_event)
            }
    
            //******ProgramStage: Delivery
            // only if there is a deliveryDate
            if (typeof PTME_mere_program.getDeliveryDate() != 'undefined') {
    
                let dhis2_PTME_delivery_event = Object.assign({}, dhis2_PTME_mere_event_stub); // copy from stub
                dhis2_PTME_delivery_event["event"] = PTME_mere_program.getPTME_delivery_event_UID(this.code);
                dhis2_PTME_delivery_event["programStage"] = PROGRAMSTAGE_PTME_MERE_DELIVERY;
                dhis2_PTME_delivery_event["eventDate"] = PTME_mere_program.getDeliveryDate_eventDate();
                dhis2_PTME_delivery_event["dueDate"] = PTME_mere_program.getDeliveryDate_eventDate();
    
                if (typeof PTME_mere_program.getDeliveryOutcome() != 'undefined') {
                    dhis2_PTME_delivery_event["dataValues"] = [];
                    dhis2_PTME_delivery_event["dataValues"].push({
                        "dataElement": DE_ISSUE_DE_LA_GROSSESSE, // BI SIDAInfo - Issue de la grossesse
                        "value": PTME_mere_program.getDeliveryOutcome()
                    })
                }
                dhis2_events.push(dhis2_PTME_delivery_event)
            }
    
            //******ProgramStage: Sortie
            // only if there is enrollment Close Date
            if (typeof PTME_mere_program.getEnrollmentCloseDate() != 'undefined') {
    
                let dhis2_PTME_mere_sortie_event = Object.assign({}, dhis2_PTME_mere_event_stub); // copy from stub
                dhis2_PTME_mere_sortie_event["event"] = PTME_mere_program.getSortie_event_UID(this.code);
                dhis2_PTME_mere_sortie_event["programStage"] = PROGRAMSTAGE_PTME_MERE_SORTIE;
                dhis2_PTME_mere_sortie_event["eventDate"] = PTME_mere_program.getSortie_eventDate();
                dhis2_PTME_mere_sortie_event["dueDate"] = PTME_mere_program.getSortie_eventDate();
                dhis2_events.push(dhis2_PTME_mere_sortie_event)
            }
    
            return dhis2_events
    
        }
    
        generateDHIS2_PTME_enfant_events() {
            var dhis2_events = []
            // PTME enfant
            const PTME_enfant_program = this.getPTME_enfant_program();
            if (typeof PTME_enfant_program != 'undefined') {
                // Stub for the event
                const dhis2_PTME_enfant_event_stub = {
                    "program": PROGRAM_PTME_ENFANT,
                    "orgUnit": this.getOrgUnit(),
                    "trackedEntityInstance": this.getUID(),
                    "storedBy": STORED_BY_USERNAME
                };
    
    
                //***** ProgramStage: Admission PTME enfant */
                let dhis2_PTME_enfant_admission_event = Object.assign({}, dhis2_PTME_enfant_event_stub); // copy from stub
                dhis2_PTME_enfant_admission_event["event"] = PTME_enfant_program.getAdmission_event_UID(this.code, this);
                dhis2_PTME_enfant_admission_event["programStage"] = PROGRAMSTAGE_PTME_ENFANT_ADMISSION;
                dhis2_PTME_enfant_admission_event["eventDate"] = PTME_enfant_program.getAdmission_PTME_Enfant_eventDate();
                dhis2_PTME_enfant_admission_event["dueDate"] = PTME_enfant_program.getAdmission_PTME_Enfant_eventDate();
                dhis2_events.push(dhis2_PTME_enfant_admission_event)
    
    
                //******ProgramStage: PCR initial
                // only if there is a PCR initial
                if (typeof PTME_enfant_program.getPCR_initial_date() != 'undefined') {
    
                    let dhis2_PTME_enfant_pcr_initial_event = Object.assign({}, dhis2_PTME_enfant_event_stub); // copy from stub
                    dhis2_PTME_enfant_pcr_initial_event["event"] = PTME_enfant_program.getPCR_initial_event_UID(this.code, this);
                    dhis2_PTME_enfant_pcr_initial_event["programStage"] = PROGRAMSTAGE_PTME_ENFANT_PCR_INITIAL;
                    dhis2_PTME_enfant_pcr_initial_event["eventDate"] = PTME_enfant_program.getPCR_initial_date_eventDate();
                    dhis2_PTME_enfant_pcr_initial_event["dueDate"] = PTME_enfant_program.getPCR_initial_date_eventDate();
                    if (typeof PTME_enfant_program.getPCR_initial_result() != 'undefined'
                        && PTME_enfant_program.getPCR_initial_result() != "") {
                        dhis2_PTME_enfant_pcr_initial_event["dataValues"] = [];
                        dhis2_PTME_enfant_pcr_initial_event["dataValues"].push({
                            "dataElement": DE_PCR_Resultat, //DE_PTME_ENFANT_PCR_initial_result
                            "value": PTME_enfant_program.getPCR_initial_result()
                        })
                    }
                    dhis2_events.push(dhis2_PTME_enfant_pcr_initial_event)
                }
    
                //******ProgramStage: PCR de suivi
                PTME_enfant_program.getPCR_follow_up().forEach(
                    pcr => {
                        let dhis2_PTME_enfant_pcr_suivi_event = Object.assign({}, dhis2_PTME_enfant_event_stub); // copy from stub
                        dhis2_PTME_enfant_pcr_suivi_event["event"] = PTME_enfant_program.getPCR_follow_up_event_UID(this.code, pcr.date.format(DHIS2_DATEFORMAT), this);
                        dhis2_PTME_enfant_pcr_suivi_event["programStage"] = PROGRAMSTAGE_PTME_ENFANT_PCR_SUIVI;
                        dhis2_PTME_enfant_pcr_suivi_event["eventDate"] = pcr.date.format(DHIS2_DATEFORMAT);
                        dhis2_PTME_enfant_pcr_suivi_event["dueDate"] = pcr.date.format(DHIS2_DATEFORMAT);
                        if (pcr.result != '') {
                            dhis2_PTME_enfant_pcr_suivi_event["dataValues"] = [];
                            dhis2_PTME_enfant_pcr_suivi_event["dataValues"].push({
                                "dataElement": DE_PCR_Resultat, // "PTME ENFANT PCR RESULT SUIVI"
                                "value": pcr.result
                            })
                        }
                        dhis2_events.push(dhis2_PTME_enfant_pcr_suivi_event)
                    }
                )
                //******ProgramStage: Sortie
                // only if there is Sortie Date
                if (typeof PTME_enfant_program.getDateSortie() != 'undefined') {
                    let dhis2_PTME_enfant_sortie_event = Object.assign({}, dhis2_PTME_enfant_event_stub); // copy from stub
                    dhis2_PTME_enfant_sortie_event["event"] = PTME_enfant_program.getSortie_event_UID(this.code, this);
                    dhis2_PTME_enfant_sortie_event["programStage"] = PROGRAMSTAGE_PTME_ENFANT_SORTIE;
                    dhis2_PTME_enfant_sortie_event["eventDate"] = PTME_enfant_program.getDateSortie_eventDate();
                    dhis2_PTME_enfant_sortie_event["dueDate"] = PTME_enfant_program.getDateSortie_eventDate();
                    if (typeof PTME_enfant_program.getCauseSortie() != 'undefined') {
                        dhis2_PTME_enfant_sortie_event["dataValues"] = [];
                        dhis2_PTME_enfant_sortie_event["dataValues"].push({
                            "dataElement": DE_PTME_ENFANT_Cause_Sortie, // "PTME ENFANT CAUSE SORTIE"
                            "value": PTME_enfant_program.getCauseSortie()
                        })
                    }
                    dhis2_events.push(dhis2_PTME_enfant_sortie_event)
                }
    
            }
    
            return dhis2_events
    
        }
    
        generateDHIS2_TARV_events() {
            var dhis2_events = []
    
            // TARV
    
            // Stub for the event
            const dhis2_TARV_event_stub = {
                "program": PROGRAM_TARV,
                "orgUnit": this.getOrgUnit(),
                "trackedEntityInstance": this.getUID(),
                "storedBy": STORED_BY_USERNAME
            };
    
            if (typeof this.getPremier_debut_ARV_momentDate() !== 'undefined') {
    
                //***********ProgramStage: Premier Debut ARV
                if (this.isDebutARV()) {
                    let dhis2_PremierDebutARV_event = Object.assign({}, dhis2_TARV_event_stub); //copy from stub
                    dhis2_PremierDebutARV_event["event"] = this.getPremier_debut_ARV_event_UID();
                    dhis2_PremierDebutARV_event["programStage"] = PROGRAMSTAGE_TARV_PREMIER_DEBUT_ARV;
                    dhis2_PremierDebutARV_event["eventDate"] = this.getPremier_debut_ARV_eventDate();
                    dhis2_PremierDebutARV_event["dueDate"] = this.getPremier_debut_ARV_eventDate();
                    dhis2_PremierDebutARV_event["dataValues"] = [];
                    dhis2_PremierDebutARV_event["dataValues"].push({
                        "dataElement": DE_TARV_Molecule, // BI SIDAInfo - Molecule
                        "value": this.getPremier_debut_ARV_treatment().codemolecule
                    })
    
                    if (this.getPremier_debut_ARV_treatment().qte != "") {
                        dhis2_PremierDebutARV_event["dataValues"].push({
                            "dataElement": DE_TARV_Quantité, // BI SIDAInfo - Quantité (jours)
                            "value": this.getPremier_debut_ARV_treatment().qte
                        })
                    }
    
                    dhis2_PremierDebutARV_event["dataValues"].push({
                        "dataElement": DE_TARV_Prochaine, // BI SIDAInfo - Prochaine rendez-vous (jours)
                        "value": this.getPremier_debut_ARV_treatment().num_days
                    })
    
                    dhis2_PremierDebutARV_event["dataValues"].push({
                        "dataElement": DE_TARV_Premier_system, // BI SIDAInfo - Première visite ARV dans le système
                        "value": "true"
                    })
    
                    dhis2_events.push(dhis2_PremierDebutARV_event);
                } else {
                    let dhis2_TARV_event = Object.assign({}, dhis2_TARV_event_stub); //copy from stub
                    dhis2_TARV_event["event"] = this.getTARV_event_UID(this.getPremier_debut_ARV_eventDate());
                    dhis2_TARV_event["programStage"] = PROGRAMSTAGE_TARV_TARV;
                    dhis2_TARV_event["eventDate"] = this.getPremier_debut_ARV_eventDate();
                    dhis2_TARV_event["dueDate"] = this.getPremier_debut_ARV_eventDate();
                    dhis2_TARV_event["dataValues"] = [];
                    dhis2_TARV_event["dataValues"].push({
                        "dataElement": DE_TARV_Molecule, // BI SIDAInfo - Molecule
                        "value": this.getPremier_debut_ARV_treatment().codemolecule
                    })
                    if (this.getPremier_debut_ARV_treatment().qte != "") {
                        dhis2_TARV_event["dataValues"].push({
                            "dataElement": DE_TARV_Quantité, // BI SIDAInfo - Quantité (jours)
                            "value": this.getPremier_debut_ARV_treatment().qte
                        })
                    }
                    dhis2_TARV_event["dataValues"].push({
                        "dataElement": DE_TARV_Prochaine, // BI SIDAInfo - Prochaine rendez-vous (jours)
                        "value": this.getPremier_debut_ARV_treatment().num_days
                    })
                    if (typeof this.getPremier_debut_ARV_treatment().RTT !== 'undefined') {
                        dhis2_TARV_event["dataValues"].push({
                            "dataElement": DE_Retour_traitement_APDV, // BI SIDAInfo - Retour à traitement après perdu de vue
                            "value": "true"
                        })
                    }
                    dhis2_TARV_event["dataValues"].push({
                        "dataElement": DE_TARV_Premier_system, // BI SIDAInfo - Première visite ARV dans le système
                        "value": "true"
                    })
                    dhis2_events.push(dhis2_TARV_event);
                }
    
            } // if (typeof this.getPremier_debut_ARV_momentDate() !== 'undefined') {
    
    
            //***********ProgramStage: TARV
            const TARVs = this.getMiddle_ARV();
            TARVs.forEach(
                tarv => {
                    let dhis2_TARV_event = Object.assign({}, dhis2_TARV_event_stub); //copy from stub
                    dhis2_TARV_event["event"] = this.getTARV_event_UID(tarv.datetraitement.format(DHIS2_DATEFORMAT));
                    dhis2_TARV_event["programStage"] = PROGRAMSTAGE_TARV_TARV;
                    dhis2_TARV_event["eventDate"] = tarv.datetraitement.format(DHIS2_DATEFORMAT);
                    dhis2_TARV_event["dueDate"] = tarv.dueDate.format(DHIS2_DATEFORMAT);
                    dhis2_TARV_event["dataValues"] = [];
                    dhis2_TARV_event["dataValues"].push({
                        "dataElement": DE_TARV_Molecule, // BI SIDAInfo - Molecule
                        "value": tarv.codemolecule
                    })
                    if (tarv.qte != "") {
                        dhis2_TARV_event["dataValues"].push({
                            "dataElement": DE_TARV_Quantité, // BI SIDAInfo - Quantité (jours)
                            "value": tarv.qte
                        })
                    }
                    dhis2_TARV_event["dataValues"].push({
                        "dataElement": DE_TARV_Prochaine, // BI SIDAInfo - Prochaine rendez-vous (jours)
                        "value": tarv.num_days
                    })
                    if (typeof tarv.RTT !== 'undefined') {
                        dhis2_TARV_event["dataValues"].push({
                            "dataElement": DE_Retour_traitement_APDV, // BI SIDAInfo - Retour à traitement après perdu de vue
                            "value": "true"
                        })
                    }
                    dhis2_events.push(dhis2_TARV_event);
                }
            )
            if (typeof this.getNext_ARV() !== 'undefined') {
                //Create the last empty TARV event with booking date but not event date (is that possible in DHIS2?)
                const dhis2_emptyTARV_event = Object.assign({}, dhis2_TARV_event_stub); //copy from stub
                dhis2_emptyTARV_event["event"] = this.getTARV_event_UID(this.getNext_ARV().format(DHIS2_DATEFORMAT)); // Important. No eventDate, so use dueDate because there is no collision
                dhis2_emptyTARV_event["programStage"] = PROGRAMSTAGE_TARV_TARV;
                dhis2_emptyTARV_event["status"] = "SCHEDULE";
                dhis2_emptyTARV_event["dueDate"] = this.getNext_ARV().format(DHIS2_DATEFORMAT);
                dhis2_events.push(dhis2_emptyTARV_event);
            }
    
    
            //***********ProgramStage: Consultation
            var consultations = this.getConsultations();
            consultations.forEach(
                consultation => {
                    let dhis2_Consultation_event = Object.assign({}, dhis2_TARV_event_stub); //copy from stub
                    dhis2_Consultation_event["event"] = this.getConsultation_event_UID(consultation.dateConsultation.format(DHIS2_DATEFORMAT));
                    dhis2_Consultation_event["programStage"] = PROGRAMSTAGE_TARV_CONSULTATION;
                    dhis2_Consultation_event["eventDate"] = consultation.dateConsultation.format(DHIS2_DATEFORMAT); //VALIDATED?
                    dhis2_Consultation_event["dueDate"] = consultation.dateConsultation.format(DHIS2_DATEFORMAT); //VALIDATED?
                    dhis2_Consultation_event["dataValues"] = [];
                    if (typeof consultation.RTT !== 'undefined') {
                        dhis2_Consultation_event["dataValues"].push({
                            "dataElement": DE_Retour_traitement_APDV, // BI SIDAInfo - Retour à traitement après perdu de vue
                            "value": "true"
                        })
                    }
    
                    if (typeof consultation.TBTypeExamen !== 'undefined') {
                        dhis2_Consultation_event["dataValues"].push({
                            "dataElement": DE_TBTypeExamen, // BI SIDAInfo - TB type examen
                            "value": consultation.TBTypeExamen
                        })
                    }
    
                    if (typeof consultation.TBResultat !== 'undefined') {
                        dhis2_Consultation_event["dataValues"].push({
                            "dataElement": DE_TBResultat, // BI SIDAInfo - TB résultat
                            "value": consultation.TBResultat
                        })
                    }
    
                    dhis2_events.push(dhis2_Consultation_event);
                }
            )
    
            //***********ProgramStage: Perdu de vue
            var LTFUs;
            if (this.LTFU.length > 0) {
                LTFUs = this.LTFU;
                LTFUs.forEach(
                    LTFUdate => {
                        let dhis2_LTFU_event = Object.assign({}, dhis2_TARV_event_stub); //copy from stub
                        dhis2_LTFU_event["event"] = this.getLTFU_event_UID(LTFUdate.format(DHIS2_DATEFORMAT));
                        dhis2_LTFU_event["programStage"] = PROGRAMSTAGE_TARV_PERDU_DE_VUE;
                        dhis2_LTFU_event["eventDate"] = LTFUdate.format(DHIS2_DATEFORMAT);
                        dhis2_LTFU_event["dueDate"] = LTFUdate.format(DHIS2_DATEFORMAT);
                        dhis2_events.push(dhis2_LTFU_event);
                    }
                )
            }
    
    
            //***********ProgramStage: Debut traitement TB (same iteration than Consultation programStage generation, INTEGRATE)
            // TODO test this code when there is a value for TBDateDebutTraitement (right now, everything is empty)
            consultations.forEach(
                consultation => {
                    // NOTE: right now, all TBDateDebutTraitement are empty
                    if (typeof consultation.TBDateDebutTraitement !== 'undefined') {
                        let dhis2_Debut_traitement_TB_event = Object.assign({}, dhis2_TARV_event_stub); //copy from stub
                        dhis2_Debut_traitement_TB_event["event"] = this.getDebut_traitement_TB_event_UID(consultation.TBDateDebutTraitement.format(DHIS2_DATEFORMAT));
                        dhis2_Debut_traitement_TB_event["programStage"] = PROGRAMSTAGE_TARV_DEBUT_TRAITEMENT_TB;
                        dhis2_Debut_traitement_TB_event["eventDate"] = consultation.TBDateDebutTraitement.format(DHIS2_DATEFORMAT); //VALIDATED?
                        dhis2_Debut_traitement_TB_event["dueDate"] = consultation.TBDateDebutTraitement.format(DHIS2_DATEFORMAT); //VALIDATED?
    
                        dhis2_events.push(dhis2_Debut_traitement_TB_event);
                    }
                }
            )
    
            //***********ProgramStage: Sortie
            if (typeof this.ARV_DateSortie !== 'undefined') {
                let dhis2_ARV_sortie_event = Object.assign({}, dhis2_TARV_event_stub); //copy from stub
                dhis2_ARV_sortie_event["event"] = this.getARV_DateSortie_event_UID();
                dhis2_ARV_sortie_event["programStage"] = PROGRAMSTAGE_TARV_SORTIE;
                dhis2_ARV_sortie_event["eventDate"] = this.ARV_DateSortie.format(DHIS2_DATEFORMAT);
                dhis2_ARV_sortie_event["dueDate"] = this.ARV_DateSortie.format(DHIS2_DATEFORMAT);
                dhis2_ARV_sortie_event["dataValues"] = [];
                dhis2_ARV_sortie_event["dataValues"].push({
                    "dataElement": DE_TARV_CAUSE_SORTIE, // BI SIDAInfo - TARV Cause de sortie
                    "value": this.ARV_CauseSortie.causesortie
                })
                dhis2_ARV_sortie_event["dataValues"].push({
                    "dataElement": DE_TARV_STATUT_DATE_SORTIE, // BI SIDAInfo - TARV Statut à la date de sortie
                    "value": this.getSortieStatusARV() //returns 'Actif' or 'Perdu de vue'
                })
                dhis2_events.push(dhis2_ARV_sortie_event);
            }
    
            return dhis2_events
        }
    
        has_enrollments() {
            let any_enrollment = false
    
            // TARV
            if (this.ARV_admissions.length > 0) {
                any_enrollment = true
            }
    
            // PTME mere
            if (this.getPregnancies().length > 0) {
                any_enrollment = true
            }
    
            // PTME enfant
            const PTME_enfant_program = this.getPTME_enfant_program();
            if (typeof PTME_enfant_program != 'undefined') {
                any_enrollment = true
            }
    
            return any_enrollment;
        }
    }
    
    
    class PTME_mere {
        // enrollmentDate = Admission datedebut = Femme enceinte_DateVisiteSuiviPTME
        // (get/set) enrollmentCloseDate = Admission datefin
        // deliveryDate = Femme enceinte_DateAccoucheSuiviPTME
        // deliveryOutcome = Femme enceinte_IssueGrossesse
        // datedepist = FileActive_datedepist
        constructor(codepatient, enrollmentDate, deliveryDate, deliveryOutcome, datedepist) {
            this.codepatient = codepatient;
            this.enrollmentDate = Moment(enrollmentDate, SIDAINFO_DATEFORMAT);
            if (deliveryDate != "") {
                this.deliveryDate = Moment(deliveryDate, SIDAINFO_DATEFORMAT);
            }
            if (deliveryOutcome != "") {
                this.deliveryOutcome = deliveryOutcome
            }
            if (typeof datedepist != 'undefined') {
                this.datedepist = datedepist;
            }
        }
    
        getAdmission_PTME_Mere_EventDate() {
            return this.enrollmentDate.format(DHIS2_DATEFORMAT);
        }
    
        getPTME_Mere_EnrollmentDate() {
            return this.enrollmentDate;
        }
    
        getPTME_mere_enrollment_UID() {
            const d_key = PROGRAM_PTME_MERE_LABEL + "_enrollment";
            const enrollmentDateFormat = this.getEnrollmentDate().format(DHIS2_DATEFORMAT)
            if (this.codepatient in previous_patient_index) {
                if (d_key in previous_patient_index[this.codepatient]) {
                    if (enrollmentDateFormat in previous_patient_index[this.codepatient][d_key]) {
                        writeNewEnrollment(this.codepatient, d_key, enrollmentDateFormat, previous_patient_index[this.codepatient][d_key][enrollmentDateFormat])
                        return previous_patient_index[this.codepatient][d_key][enrollmentDateFormat];
                    }
                } else {
                    previous_patient_index[this.codepatient][d_key] = {}
                }
            }
            const new_PTME_enfant_enrollment_uid = generateCode();
            previous_patient_index[this.codepatient][d_key][enrollmentDateFormat] = new_PTME_enfant_enrollment_uid;
            writeNewEnrollment(this.codepatient, d_key, enrollmentDateFormat, previous_patient_index[this.codepatient][d_key][enrollmentDateFormat], new_PTME_enfant_enrollment_uid)
            return new_PTME_enfant_enrollment_uid;
        }
    
        getAdmission_PTME_Mere_event_UID(codepatient) {
            var eventUID = getEventUID(codepatient, PROGRAM_PTME_MERE_LABEL + "-" + this.getPTME_mere_enrollment_UID(), PROGRAMSTAGE_PTME_MERE_ADMISSION_PTME_MERE_LABEL, this.getAdmission_PTME_Mere_EventDate())
            writeNewEvent(codepatient, PROGRAM_PTME_MERE_LABEL + "-" + this.getPTME_mere_enrollment_UID(), PROGRAMSTAGE_PTME_MERE_ADMISSION_PTME_MERE_LABEL, this.getAdmission_PTME_Mere_EventDate(), eventUID)
            return eventUID;
        }
    
        getPremierDebutARVPTME_event_UID(codepatient, eventDate) {
            var eventUID = getEventUID(codepatient, PROGRAM_PTME_MERE_LABEL + "-" + this.getPTME_mere_enrollment_UID(), PROGRAMSTAGE_PTME_MERE_PREMIERDEBUTARVPTME_LABEL, eventDate)
            writeNewEvent(codepatient, PROGRAM_PTME_MERE_LABEL + "-" + this.getPTME_mere_enrollment_UID(), PROGRAMSTAGE_PTME_MERE_PREMIERDEBUTARVPTME_LABEL, eventDate, eventUID)
            return eventUID;
        }
    
        getPTME_delivery_event_UID(codepatient) {
            var eventUID = getEventUID(codepatient, PROGRAM_PTME_MERE_LABEL + "-" + this.getPTME_mere_enrollment_UID(), PROGRAMSTAGE_PTME_MERE_DELIVERY_LABEL, this.getDeliveryDate_eventDate())
            writeNewEvent(codepatient, PROGRAM_PTME_MERE_LABEL + "-" + this.getPTME_mere_enrollment_UID(), PROGRAMSTAGE_PTME_MERE_DELIVERY_LABEL, this.getDeliveryDate_eventDate(), eventUID)
            return eventUID;
        }
    
        getSortie_event_UID(codepatient) {
            var eventUID = getEventUID(codepatient, PROGRAM_PTME_MERE_LABEL + "-" + this.getPTME_mere_enrollment_UID(), PROGRAMSTAGE_PTME_MERE_SORTIE_LABEL, this.getSortie_eventDate())
            writeNewEvent(codepatient, PROGRAM_PTME_MERE_LABEL + "-" + this.getPTME_mere_enrollment_UID(), PROGRAMSTAGE_PTME_MERE_SORTIE_LABEL, this.getSortie_eventDate(), eventUID)
            return eventUID;
        }
    
        getEnrollmentDate() {
            return this.enrollmentDate;
        }
    
        getDeliveryDate() {
            return this.deliveryDate;
        }
    
        getDeliveryDate_eventDate() {
            return this.deliveryDate.format(DHIS2_DATEFORMAT);
        }
    
        getDeliveryOutcome() {
            return this.deliveryOutcome;
        }
    
        setEnrollmentCloseDate(enrollmentCloseDate) {
            this.enrollmentCloseDate = Moment(enrollmentCloseDate, SIDAINFO_DATEFORMAT);
        }
    
        getEnrollmentCloseDate() {
            return this.enrollmentCloseDate;
        }
    
        getSortie_eventDate() { // EventDate for stage "Sortie PTME Mere"
            return this.enrollmentCloseDate.format(DHIS2_DATEFORMAT)
        }
    
    
        // DE: Statut avant cette grossesse (from stage Admission PTME mere)
        getStatusBeforePregnancy() {
            if (typeof this.datedepist != 'undefined') {
                if (isBeforeDate(Moment(this.datedepist, SIDAINFO_DATEFORMAT), this.enrollmentDate)) {
                    return optionSets["StatusBeforePregnancy"].options["Statut déja connu"]
                }
            }
            return optionSets["StatusBeforePregnancy"].options["Statut non connu"]
    
        }
    
        // DE: ARV commencé avant cette grossesse (from stage Admission PTME mere)
        getARVStartedBeforePregnancy() {
            let patient = personList.find(x => x.code === this.codepatient);
            if (typeof patient !== 'undefined') {
                const ARV_treatments = patient.ARV_treatments;
                if (typeof ARV_treatments !== 'undefined' && ARV_treatments.length > 0) {
                    if (isBeforeDate(Moment(ARV_treatments[0].datetraitement, SIDAINFO_DATEFORMAT), this.enrollmentDate)) {
                        return optionSets["ARVBeforePregnancy"].options["ARV déjà commencé"]
                    }
                }
                return optionSets["ARVBeforePregnancy"].options["ARV pas encore commencé"]
            }
        }
    
        getEnrollmentStatus() {
            if (typeof this.getEnrollmentCloseDate() != 'undefined') {
                return "COMPLETED"
            }
            else {
                return "ACTIVE"
            }
        }
    
        validatePTME_mere() {
            if ((typeof this.deliveryOutcome != 'undefined') && (validateOption("Issue Grossesse", this.deliveryOutcome, this.codepatient, "FEMME_ENCEINTE") == false)) {
                return false;
            }
    
            if (validateOption("StatusBeforePregnancy", this.getStatusBeforePregnancy(), this.codepatient) == false) {
                return false;
            }
    
            if (validateOption("ARVBeforePregnancy", this.getARVStartedBeforePregnancy(), this.codepatient) == false) {
                return false;
            }
            return true;
        }
    }
    
    
    class PTME_enfant {
        constructor(enrollmentDate, admission_PTME_enfant_date, pcr_list) {
            this.enrollmentDate = Moment(enrollmentDate, SIDAINFO_DATEFORMAT);
            this.admission_PTME_enfant_date = admission_PTME_enfant_date;
            this.pcr_list = pcr_list;
        }
    
        getEnrollmentDate() {
            return this.enrollmentDate;
        }
    
        getPCR_list() {
            return this.pcr_list;
        }
    
        getDateAdmissionEnfPTME() {
            return this.admission_PTME_enfant_date;
        }
    
        getAdmission_PTME_Enfant_eventDate() {
            return this.admission_PTME_enfant_date.format(DHIS2_DATEFORMAT);
        }
    
        getDateSortie_eventDate() {
            return this.dateSortie.format(DHIS2_DATEFORMAT);
        }
    
        getDateSortie() {
            return this.dateSortie;
        }
    
        setDateSortie(dateSortie) {
            this.dateSortie = dateSortie;
        }
    
        setCauseSortie(causeSortie) {
            this.causeSortie = causeSortie;
        }
    
        getCauseSortie() {
            return this.causeSortie;
        }
    
        getPCR_initial_date() {
            if (this.getPCR_list().length > 0) {
                return this.getPCR_list()[0].date;
            }
        }
    
        getPCR_initial_date_eventDate() {
            if (typeof this.getPCR_initial_date() != 'undefined') {
                return this.getPCR_initial_date().format(DHIS2_DATEFORMAT);
            }
        }
    
        getPCR_initial_result() {
            if (this.getPCR_list().length > 0) {
                return this.getPCR_list()[0].result;
            }
        }
    
        getPCR_follow_up() {
            if (this.getPCR_list().length > 1) {
                return this.getPCR_list().slice(0);
            }
            return [];
        }
    
        getEnrollmentStatus() {
            if (typeof this.getDateSortie() != 'undefined') {
                return "COMPLETED"
            }
            else {
                return "ACTIVE"
            }
        }
    
        getAdmission_event_UID(codepatient, person) {
            var eventUID = getEventUID(codepatient, PROGRAM_PTME_ENFANT_LABEL + "-" + person.getPTME_enfant_enrollment_UID(), PROGRAMSTAGE_PTME_ENFANT_ADMISSION_LABEL, this.getAdmission_PTME_Enfant_eventDate())
            writeNewEvent(codepatient, PROGRAM_PTME_ENFANT_LABEL + "-" + person.getPTME_enfant_enrollment_UID(), PROGRAMSTAGE_PTME_ENFANT_ADMISSION_LABEL, this.getAdmission_PTME_Enfant_eventDate(), eventUID);
            return eventUID;
        }
    
        getPCR_initial_event_UID(codepatient, person) {
            var eventUID = getEventUID(codepatient, PROGRAM_PTME_ENFANT_LABEL + "-" + person.getPTME_enfant_enrollment_UID(), PROGRAMSTAGE_PTME_ENFANT_PCR_INITIAL_LABEL, this.getPCR_initial_date_eventDate())
            writeNewEvent(codepatient, PROGRAM_PTME_ENFANT_LABEL + "-" + person.getPTME_enfant_enrollment_UID(), PROGRAMSTAGE_PTME_ENFANT_PCR_INITIAL_LABEL, this.getPCR_initial_date_eventDate(), eventUID)
            return eventUID;
        }
    
        getPCR_follow_up_event_UID(codepatient, eventDate, person) {
            var eventUID = getEventUID(codepatient, PROGRAM_PTME_ENFANT_LABEL + "-" + person.getPTME_enfant_enrollment_UID(), PROGRAMSTAGE_PTME_ENFANT_PCR_SUIVI_LABEL, eventDate)
            writeNewEvent(codepatient, PROGRAM_PTME_ENFANT_LABEL + "-" + person.getPTME_enfant_enrollment_UID(), PROGRAMSTAGE_PTME_ENFANT_PCR_SUIVI_LABEL, eventDate, eventUID);
    
            return eventUID;
        }
    
        getSortie_event_UID(codepatient, person) {
    
            var eventUID = getEventUID(codepatient, PROGRAM_PTME_ENFANT_LABEL + "-" + person.getPTME_enfant_enrollment_UID(), PROGRAMSTAGE_PTME_ENFANT_SORTIE_LABEL, this.getDateSortie_eventDate())
            writeNewEvent(codepatient, PROGRAM_PTME_ENFANT_LABEL + "-" + person.getPTME_enfant_enrollment_UID(), PROGRAMSTAGE_PTME_ENFANT_SORTIE_LABEL, this.getDateSortie_eventDate(), eventUID)
    
            return eventUID;
        }
    
        validatePTME_enfant() {
            // TODO
        }
    
    }
    
    /*****************************************************************************************/
    /**************************  PARSING STARTS HERE  ****************************************/
    /*****************************************************************************************/
    
    logger_generation.info(`Start processing OU=${SOURCE_OU_CODE} EXPORT_DUMP_DATE=${SOURCE_DATE}`)
    
    /********************* ENFANT PTME *******************************************************/
    
    function paddy(num, padlen, padchar) {
        var pad_char = typeof padchar !== 'undefined' ? padchar : '0';
        var pad = new Array(1 + padlen).join(pad_char);
        return (pad + num).slice(-pad.length);
    }
    
    //read the csv file
    let csvFile_ENFANT_PTME = fs.readFileSync(csvFilename_ENFANT_PTME);
    
    //parse the csv file
    const records_ENFANT_PTME = parse(csvFile_ENFANT_PTME, {
        skip_empty_lines: true,
        delimiter: ';'
    })
    
    
    const enfants = records_ENFANT_PTME.map((row, row_number) => {
    
        const CURRENT_TABLE = "ENFANT_PTME"
    
        const site = row [0]
        row.shift(); // Remove the first column that contains the health facility SIDA info code
    
        // [0] ExportDate - [1] CodeEnf - [2] DateAdmissionEnfPTME - [3] SexeEnfant - [4] CodeMere - [5] DateNaissance - [6] LieuNaissance - [7] ModeAlimentation - [8] PoidsNaissance - [9] TratePropARVMere
        // [10] TratePropARVEnfant - [11] DateDepistage - [12] PCR1DateResultat - [13] PCR2DateResultat - [14] PCR3DateResultat - [15] PCR4DateResultat - [16] Sero_9mois_Date_resultat - [17] Sero_18mois_date_resultat - [18] PCR1Prelevement - [19] PCR2Prelevement
        // [20] PCR3Prelevement - [21] PCR4Prelevement - [22] Sero_9Mois_prelevement - [23] Sero_18Mois_prelevement - [24] PrelevementAutre - [25] PCR1_resultat - [26] PCR2_resultat - [27] PCR3_resultat - [28] PCR4_resultat - [29] Sero_9mois_Resultat
        // [30] Sero_18mois_Resultat - [31] ResultatAutre - [32] Refere - [33] Recu_Cotri - [34] Cotri_DateDebut - [35] Cotri_DateFin - [36] Cotri_Causefin - [37] INH_DateDebut - [38] INH_DateFin - [39] INH_Causefin
        // [40] DateSortie - [41] CauseSortie - [42] Dateseuvrage
    
        const sex = row[3]; //SexeEnfant
        const codepatient = site + SEPARATOR_PATIENT + paddy(row[1],5) + "E"; // code
        if (codepatient === site + SEPARATOR_PATIENT){
            logger_generation.error(`39;;Patient with empty patient code. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.error(`Patient avec un champs code patient vide. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 39`)
            return false;
        }
    
        validateOption("sexe", sex, codepatient, CURRENT_TABLE);
    
        const birthdate_raw = row[5]
        const birthdate = Moment(birthdate_raw, SIDAINFO_DATEFORMAT);
    
        const entryMode = "10"; //10: PTME enfant
        var patient_enfant = new Person(codepatient, sex, birthdate, entryMode);
    
        if (validateDate(birthdate, MIN_YEAR_BIRTH) == false) {
            patient_enfant.addLog(`08;Le patient a une date de naissance avec une DATE inattendu : ${birthdate}.`)
            logger_generation.warn(`08;${codepatient};Patient ${codepatient} has a Birth date with an unexpected DATE ${birthdate}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une date de naissance avec une DATE inattendu : ${birthdate}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 08;${codepatient}`)
        }
    
        /** Create PTME enfant program */
        const DateAdmissionEnfPTME_raw = row[2];
        if (DateAdmissionEnfPTME_raw==""){ // if the date admission is empty
            logger_generation.error(`09;${codepatient};Patient ${codepatient} has a Date Admission Enfant PTME with an empty DATE. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.error(`Le patient ${codepatient} a une Date Admission Enfant PTME avec une DATE vacie. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 09;${codepatient}`)
            return false; // not create the enfant
        }
        const DateAdmissionEnfPTME = Moment(DateAdmissionEnfPTME_raw, SIDAINFO_DATEFORMAT)
        
        if (validateDate(DateAdmissionEnfPTME, MIN_YEAR_EVENTS) == false) {
            logger_generation.error(`09;${codepatient};Patient ${codepatient} has a Date Admission Enfant PTME with an unexpected DATE ${DateAdmissionEnfPTME_raw}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.error(`Le patient ${codepatient} a une Date Admission Enfant PTME avec une DATE inattendu : ${DateAdmissionEnfPTME_raw}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 09;${codepatient}`)
            return false; // not create the enfant
        }
    
    
        const PCR1Prelevement_raw = row[18];
        const PCR1Prelevement = Moment(PCR1Prelevement_raw, SIDAINFO_DATEFORMAT)
        if (PCR1Prelevement_raw && validateDate(PCR1Prelevement, MIN_YEAR_EVENTS) == false) {
            patient_enfant.addLog(`10;Le patient a une date PCR 1 avec une DATE inattendu : ${PCR1Prelevement_raw}.`)
            logger_generation.warn(`10;${codepatient};Patient ${codepatient} has a PCR 1 with an unexpected DATE ${PCR1Prelevement_raw}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une date PCR 1 avec une DATE inattendu : ${PCR1Prelevement_raw}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 10;${codepatient}`)
        }
    
        const PCR2Prelevement_raw = row[19];
        const PCR2Prelevement = Moment(PCR2Prelevement_raw, SIDAINFO_DATEFORMAT)
        if (PCR2Prelevement_raw && validateDate(PCR2Prelevement, MIN_YEAR_EVENTS) == false) {
            patient_enfant.addLog(`10;Date PCR 2 avec une DATE inattendu : ${PCR2Prelevement_raw}.`)
            logger_generation.warn(`10;${codepatient};Patient ${codepatient} has a PCR 2 with an unexpected DATE ${PCR2Prelevement_raw}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une date PCR 2 avec une DATE inattendu : ${PCR2Prelevement_raw}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 10;${codepatient}`)
        }
    
        const PCR3Prelevement_raw = row[20];
        const PCR3Prelevement = Moment(PCR3Prelevement_raw, SIDAINFO_DATEFORMAT)
        if (PCR3Prelevement_raw && validateDate(PCR3Prelevement, MIN_YEAR_EVENTS) == false) {
            patient_enfant.addLog(`10;Date PCR 3 avec une DATE inattendu : ${PCR3Prelevement_raw}.`)
            logger_generation.warn(`10;${codepatient};Patient ${codepatient} has a PCR 3 with an unexpected DATE ${PCR3Prelevement_raw}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une date PCR 3 avec une DATE inattendu : ${PCR3Prelevement_raw}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 10;${codepatient}`)
        }
    
        const PCR4Prelevement_raw = row[21];
        const PCR4Prelevement = Moment(PCR4Prelevement_raw, SIDAINFO_DATEFORMAT)
        if (PCR4Prelevement_raw && validateDate(PCR4Prelevement, MIN_YEAR_EVENTS) == false) {
            patient_enfant.addLog(`10;Date PCR 4 avec une DATE inattendu : ${PCR4Prelevement_raw}.`)
            logger_generation.warn(`10;${codepatient};Patient ${codepatient} has a PCR 4 with an unexpected DATE ${PCR4Prelevement_raw}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une date PCR 4 avec une DATE inattendu : ${PCR4Prelevement_raw}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 10;${codepatient}`)
        }
    
        const PrelevementAutre_raw = row[24];
        const PrelevementAutre = Moment(PrelevementAutre_raw, SIDAINFO_DATEFORMAT)
        if (PrelevementAutre_raw && validateDate(PrelevementAutre, MIN_YEAR_EVENTS) == false) {
            patient_enfant.addLog(`10;Date PCR Prelevement Autre avec une DATE inattendu : ${PrelevementAutre_raw}.`)
            logger_generation.warn(`10;${codepatient};Patient ${codepatient} has a PCR Autre with an unexpected DATE ${PrelevementAutre_raw}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une date PCR Prelevement Autre avec une DATE inattendu : ${PrelevementAutre_raw}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 10;${codepatient}`)
        }
    
        const PCR1 = row[25]; // PCR1_resultat
        const PCR2 = row[26];
        const PCR3 = row[27];
        const PCR4 = row[28];
        const ResultatAutre = row[31];
    
        let pcr_list = [];
    
        pcr_list.push({ "date": PCR1Prelevement, "result": PCR1 })
        pcr_list.push({ "date": PCR2Prelevement, "result": PCR2 })
        pcr_list.push({ "date": PCR3Prelevement, "result": PCR3 })
        pcr_list.push({ "date": PCR4Prelevement, "result": PCR4 })
        pcr_list.push({ "date": PrelevementAutre, "result": ResultatAutre })
    
        if (!(validatePCR_list(pcr_list, patient_enfant))) {
            pcr_list = []; // clean PCR list (removing all PCR)
        }
    
        // remove pcr without PCR date
        pcr_list = pcr_list.filter(pcr => pcr.date.isValid())
    
        // sort PCR test by date
        pcr_list.sort((a, b) => a.date - b.date)
    
        // validate that all PCR results (if present) are valid options
        pcr_list = pcr_list.filter(pcr => {
            if (pcr.result != "") {
                return validateOption("enfant_ptme_pcr_result", pcr.result, codepatient, CURRENT_TABLE)
            } else {
                return false;
            }
        })
    
        var PTME_enfant_program = new PTME_enfant(birthdate, DateAdmissionEnfPTME, pcr_list);
    
        const DateSortie_raw = row[40];
        const DateSortie = Moment(DateSortie_raw, SIDAINFO_DATEFORMAT)
        if (DateSortie_raw && validateDate(DateSortie, MIN_YEAR_EVENTS) == false) {
            patient_enfant.addLog(`11;Le patient a une date Sortie avec une DATE inattendu : ${DateSortie_raw}.`)
            logger_generation.warn(`11;${codepatient};Patient ${codepatient} has a Date Sortie with an unexpected DATE ${DateSortie_raw}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une Date Sortie avec une DATE inattendu : ${DateSortie_raw}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 11;${codepatient}`)
        }
    
        const CauseSortie = row[41];
    
        // If there is a date, there is a cause
        if (DateSortie_raw && (validateOption("enfant_ptme_causesortie", CauseSortie, codepatient, CURRENT_TABLE))) {
            PTME_enfant_program.setDateSortie(DateSortie);
            PTME_enfant_program.setCauseSortie(CauseSortie);
        }
    
        patient_enfant.setPTME_enfant_program(PTME_enfant_program)
    
        return patient_enfant;
    });
    
    enfants.filter(Boolean).forEach(enfant => { // filter enfants that are "false", due to the missing code patient
        if (isDuplicated(enfant.code) == false) {
            personList.push(enfant)
        }
    })
    
    
    /********************** FILE ACTIVE *************************************************************/
    
    //read the csv file
    let csvFile_FileActive = fs.readFileSync(csvFilename_FileActive);
    
    //Note: convert from ascii to UTF-8 is done in a preliminary step (bash script)
    
    //parse the csv file
    var tmp_patientCode = ""
    
    const records_FileActive = parse(csvFile_FileActive, {
        skip_empty_lines: true,
        delimiter: ';',
        cast: function (value, context) {
            if (context.index === 2) {
                tmp_patientCode = value;
            }
            if (context.index === 17) { // entryMode. Sometimes is 08 instead of 8
                if (Object.is(parseInt(value, 10), NaN)) {
                    logger_generation.error(`12;${tmp_patientCode};Error;Patient ${tmp_patientCode} in the FileActive table has a value for entryMode (${value}) that is not a number. Please, check patient ${tmp_patientCode} or line ${context.lines} in the FileActive table.`);
                    logger_generation_fr.error(`Le patient ${tmp_patientCode} de la table FileActive a une valeur pour codeModeEntree (${value}) qui n'est pas un nombre. Veuillez vérifier patient ${tmp_patientCode} ou la ligne ${context.lines} dans la table FileActive.;Erreur 12;${tmp_patientCode}`);
                } else {
                    return String(parseInt(value, 10))
                }
            } else {
                return value
            }
        }
    });
    
    const activePatients = records_FileActive.map((row, row_number) => {
    
        const CURRENT_TABLE = "FileActive"
    
        const site = row [0]
        row.shift(); // Remove the first column that contains the health facility SIDA info code
    
        // [0] ExportDate - [1] codeidpatient - [2] dateadmission - [3] datenaissance - [4] sexe - [5] ARVdatedebut - [6] codemoleculeARV - [7] sortie - [8] datesortie - [9] causesortie
        // [10] causedeces - [11] dateretour - [12] datesortiehistoire - [13] causesortiehistoire - [14] iedea_id - [15] datedepist - [16] codemodeentree - [17] codeprov - [18] codecommune - [19] stadepvvih
        // [20] situationmatri - [21] codeprofession - [22] UID
    
        const codepatient = site + SEPARATOR_PATIENT + row[1];
        const birthdate_raw = row[3]
        const birthdate = Moment(birthdate_raw, SIDAINFO_DATEFORMAT);
        const ARVdatedebut_raw = row[5];
    
        const sex = row[4];
        validateOption("sexe", sex, codepatient, CURRENT_TABLE);
        const entryMode = row[16];
        validateOption("Mode d’entrée", entryMode, codepatient, CURRENT_TABLE);
        var patient = new Person(codepatient, sex, birthdate, entryMode, ARVdatedebut_raw);
    
        if (validateDate(birthdate, MIN_YEAR_BIRTH) == false) {
            patient.addLog(`08;Date de naissance avec une DATE inattendu : ${birthdate}.`)
            logger_generation.warn(`08;${codepatient};Patient ${codepatient} has a Birth date with an unexpected DATE ${birthdate}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une date de naissance avec une DATE inattendu : ${birthdate}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 08;${codepatient}`)
        }
    
        if (ARVdatedebut_raw != "") {
            if (validateDate(patient.ARVdatedebut, MIN_YEAR_EVENTS) == false) {
                patient.addLog(`40;Debut date avec une DATE inattendu : ${patient.ARVdatedebut}.`)
                logger_generation.warn(`40;${codepatient};Patient ${codepatient} has a ARVdatedebut date with an unexpected DATE ${patient.ARVdatedebut}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
                logger_generation_fr.warn(`Le patient ${codepatient} a un ARVdatedebut inattendu : ${patient.ARVdatedebut}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 40;${codepatient}`)
            }
        }
    
    
        const datedepist_raw = row[15];
        if (datedepist_raw != "") {
            patient.setDatedepist(datedepist_raw)
            if (validateDate(Moment(datedepist_raw, SIDAINFO_DATEFORMAT), MIN_YEAR_EVENTS) == false) {
                patient.addLog(`13;Le patient a une date Depist avec une DATE inattendu : ${datedepist_raw}.`)
                logger_generation.warn(`13;${codepatient};Patient ${codepatient} has a Date Depist with an unexpected DATE ${datedepist_raw}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
                logger_generation_fr.warn(`Le patient ${codepatient} a une Date Depist avec une DATE inattendu : ${datedepist_raw}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 13;${codepatient}`)
            }
        }
    
    
        const sortie = row[7];
        const datesortie_raw = row[8];
        const datesortie = Moment(datesortie_raw, SIDAINFO_DATEFORMAT)
        if (sortie === "Vrai" && validateDate(datesortie, MIN_YEAR_EVENTS) == false) {
            patient.addLog(`14;Le patient a une date Sortie avec une DATE inattendu : ${datesortie_raw}.`)
            logger_generation.warn(`14;${codepatient};Error;Warning ${codepatient} has a Date Sortie with an unexpected DATE ${datesortie_raw}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une Date Sortie avec une DATE inattendu : ${datesortie_raw}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 14;${codepatient}`)
        }
    
        var causesortie = row[9];
        if (causesortie.normalize().localeCompare("Transféré".normalize()) == 0) {
            causesortie = "1"
        }
    
        if (causesortie.normalize().localeCompare("Fin de traitement prophyl. post-expositionnel".normalize()) == 0) {
            causesortie = "4"
        }
        // T = Transféré sortant et correspond au code numérique 1
        if (causesortie.normalize().localeCompare("T".normalize()) == 0) {
            causesortie = "1"
        }
        // F = Fin prophylaxie et correspond au code numérique 6 et ce code était utilisé avant pour le cas de dépannage (personne venant d’un autre site mais qui ne retournera pas peut être à ce site) et après on faisait sortir le client en disant fin prophylaxie pour justifier le médicament sorti.
        if (causesortie.normalize().localeCompare("F".normalize()) == 0) {
            causesortie = "6"
        }
    
        if (sortie === "Vrai") {
            if (validateOption("causesortie", causesortie, patient.code, CURRENT_TABLE) && (["1", "2", "5", "6"].includes(causesortie))) {
                patient.setARV_CauseSortie({ "causesortie": causesortie });
                patient.setARV_DateSortie(Moment(datesortie, SIDAINFO_DATEFORMAT));
            }
        }
        return patient;
    });
    
    activePatients.forEach(activePatient => {
        if (isDuplicated(activePatient.code) == false) {
            personList.push(activePatient)
        }
    })
    
    
    /**************************** ADMISSION DETAIL *******************************************************/
    
    //read the csv file
    let csvFile_ADMISSION_DETAIL = fs.readFileSync(csvFilename_ADMISSION_DETAIL);
    
    //parse the csv file
    const records_ADMISSION_DETAIL = parse(csvFile_ADMISSION_DETAIL, {
        skip_empty_lines: true,
        delimiter: ';'
    })
    
    records_ADMISSION_DETAIL.map((row, row_number) => {
    
        const CURRENT_TABLE = "ADMISSION_DETAIL"
    
        const site = row [0]
        row.shift(); // Remove the first column that contains the health facility SIDA info code
    
        // [0] ExportDate - [1] codepatient - [2] enrollment - [3] datedebut - [4] datefin
    
        const codepatient = site + SEPARATOR_PATIENT + row[1];
        const enrollment = row[2]
        const datedebut = row[3]
        const datefin = row[4]
    
        let patient = personList.find(x => x.code === codepatient);
        if (typeof patient === 'undefined') {
            patientCodeBlockList.add(codepatient);
            logger_generation.error(`18;${codepatient};Patient ${codepatient} appears in the ${CURRENT_TABLE} table but not in the File_Active table. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table.`)
            logger_generation_fr.error(`Le patient ${codepatient} est dans la table ${CURRENT_TABLE} mais n'est pas dans la table File_Active. Veuillez confirmer la ligne ${row_number + 1} de la table ${CURRENT_TABLE}.;Erreur 18;${codepatient}`)
        }
    
        if (datedebut && validateDate(Moment(datedebut, SIDAINFO_DATEFORMAT), MIN_YEAR_EVENTS) == false) {
            patient.addLog(`15;Le patient a une date Debut avec une DATE inattendu : ${datedebut}.`)
            logger_generation.warn(`15;${codepatient};Patient ${codepatient} has a Date Debut with an unexpected DATE ${datedebut}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une Date Debut avec une DATE inattendu : ${datedebut}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 15;${codepatient}`)
        }
    
        if (datefin && validateDate(Moment(datefin, SIDAINFO_DATEFORMAT), MIN_YEAR_EVENTS) == false) {
            patient.addLog(`16;Le patient a une date Fin avec une DATE inattendu : ${datefin}.`)
            logger_generation.warn(`16;${codepatient};Patient ${codepatient} has a Date Fin with an unexpected DATE ${datefin}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une Date Fin avec une DATE inattendu : ${datefin}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 16;${codepatient}`)
        }
    
        if (datefin != "" && (isBeforeDate(Moment(datedebut, SIDAINFO_DATEFORMAT), Moment(datefin, SIDAINFO_DATEFORMAT)) == false)) {
            patientCodeBlockList.add(codepatient);
            logger_generation.error(`17;${codepatient};Patient ${codepatient} has an ADMISSION where the start date (datedebut ${datedebut}) is not before the end date (datefin ${datefin}).`);
            logger_generation_fr.error(`Le patient ${codepatient} a une ADMISSION dont la dateDebut (${datedebut}) n'est pas avant la dateFIN (${datefin}).;Erreur 17;${codepatient}`);
        }
    
        if ((enrollment === "ARV") && (typeof patient !== 'undefined')) {
            patient.addARV_admission(datedebut, datefin)
        }
    
        // Jean Claude has confirmed that PTMEE and PTMES are actually child entries, rather than mothers,
        if ((enrollment === "PTME") && (typeof patient !== 'undefined')) {
            patient.addPTME_mere_admission(datedebut, datefin)
        }
    })
    
    /**************************** TABLE ARV *******************************************************/
    
    //read the csv file
    let csvFile_TABLE_ARV = fs.readFileSync(csvFilename_TABLE_ARV);
    
    //parse the csv file
    const records_TABLE_ARV = parse(csvFile_TABLE_ARV, {
        skip_empty_lines: true,
        delimiter: ';'
    })
    
    records_TABLE_ARV.map((row, row_number) => {
    
        const CURRENT_TABLE = "TABLE_ARV"
    
        const site = row [0]
        row.shift(); // Remove the first column that contains the health facility SIDA info code
    
        // [0] ExportDate - [1] codepatientAclin - [2] datetraitement - [3] dateprochainrendev - [4] codemolecule - [5] qte
    
        const codepatient = site + SEPARATOR_PATIENT + row[1];
        const datetraitement = row[2]
        const dateprochainrendev = row[3] // number of days until the next treatment
        const codemolecule = row[4].trim()
        const qte = row[5].trim()
    
        let patient = personList.find(x => x.code === codepatient);
        if (typeof patient === 'undefined') {
            patientCodeBlockList.add(codepatient);
            logger_generation.error(`18;${codepatient};Patient ${codepatient} appears in the ${CURRENT_TABLE} table but not in the File_Active table. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table.`);
            logger_generation_fr.error(`Le patient ${codepatient} est dans la table ${CURRENT_TABLE} mais n'est pas dans la table File_Active. Veuillez confirmer la ligne ${row_number + 1} de la table ${CURRENT_TABLE}.;Erreur 18;${codepatient}`);
        }
    
        if (datetraitement && validateDate(Moment(datetraitement, SIDAINFO_DATEFORMAT), MIN_YEAR_EVENTS) == false) {
            if (typeof patient !== 'undefined') {
                patient.addLog(`19;Le patient a une date traitement avec une DATE inattendu : ${datetraitement}.`)
            }
            logger_generation.warn(`19;${codepatient};Patient ${codepatient} has a Date traitement with an unexpected DATE ${datetraitement}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a une Date traitement avec une DATE inattendu : ${datetraitement}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 19;${codepatient}`)
        }
    
        if (dateprochainrendev < 0) { // number of days until the next treatment SHOULD NOT be minor than cero
            if (typeof patient !== 'undefined') {
                patient.addLog(`02;Le patient a un nombre de jours au prochain rendez-vous qui est inférieur à 0 (le nombre de jours est ${dateprochainrendev} à ${datetraitement}).`)
            }
            logger_generation.warn(`02;${codepatient};Patient ${codepatient} has a number of days until next treatment below 0 (the number of days is ${dateprochainrendev}). Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.warn(`Le patient ${codepatient} a un nombre de jours au prochain rendez-vous qui est inférieur à 0 (le nombre de jours est ${dateprochainrendev}). Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 02;${codepatient}`)
        }
    
        if (typeof patient !== 'undefined') {
            if (datetraitement != "" && dateprochainrendev != "") { //check that the two values are present
                const treatment_already = patient.ARV_treatments.find(e => e.datetraitement.isSame(Moment(datetraitement, SIDAINFO_DATEFORMAT)))
                if (typeof treatment_already == "undefined") {
                    patient.addARV_treatment(datetraitement, dateprochainrendev, codemolecule, qte)
                } else { // if duplicated
                    if (dateprochainrendev != treatment_already.num_days) { // different number of treatment days. Select the minimum number of days
                        logger_generation.warn(`20;${codepatient};Patient ${codepatient} has two ARV treatments on the same date (${datetraitement}), but each with different number of days until next appointment:  ${dateprochainrendev}  ${treatment_already.num_days}. (The smaller number will be imported.)`);
                        logger_generation_fr.warn(`Le patient ${codepatient} a deux traitements ARV à la même date (${datetraitement}), mais le nombre de jours au prochain rendez-vous est différent : ${dateprochainrendev} et ${treatment_already.num_days} (le nombre plus petit sera importé).;Erreur 20;${codepatient}`);
                        patient.addLog(`20;Le patient a deux traitements ARV à la même date (${datetraitement}), mais le nombre de jours au prochain rendez-vous est différent : ${dateprochainrendev} et ${treatment_already.num_days} (le nombre plus petit sera importé).`);
                        if (parseInt(dateprochainrendev) < parseInt(treatment_already.num_days)) { // change the values
                            logger_generation.info(`Patient ${codepatient}. Internal change due selection of the minimum number of days (datetraitement:${datetraitement}). OLD values: dateprochainrendev: ${treatment_already.num_days} ; codemolecule: ${treatment_already.codemolecule} ; qte: ${treatment_already.qte}. NEW values: dateprochainrendev: ${dateprochainrendev} ; codemolecule: ${codemolecule} ; qte: ${qte}`);
                            treatment_already.num_days = dateprochainrendev;
                            treatment_already.codemolecule = codemolecule;
                            treatment_already.qte = qte;
                        }
                    } else {
                        // Only warning
                        logger_generation.warn(`21;${codepatient};Patient ${codepatient} has duplicated ARV_treatment date: datetraitement: ${datetraitement}, dateprochainrendev: ${dateprochainrendev}. Error automatically resolved during import (the duplicate treatment dates have been merged into a single treatment date).`);
                        logger_generation_fr.warn(`Le patient ${codepatient} a des dates de traitement ARV dupliquées : datetraitement ${datetraitement}, (dateprochainrendev ${dateprochainrendev}) Erreur automatiquement résolue lors de l’importation (les dates traitements dupliquées sont fusionnées dans une seule date traitement).;Erreur 21;${codepatient}`);
                        //patient.addLog(`21;Dupliqué ARV_treatment date: datetraitement:${datetraitement}; dateprochainrendev: ${dateprochainrendev}`);
                    }
                }
            } else {
                patientCodeBlockList.add(codepatient);
                logger_generation.error(`22;${codepatient};Patient ${codepatient} has missing data for the ARV_treatment: datetraitement:${datetraitement}, dateprochainrendev: ${dateprochainrendev}`);
                logger_generation_fr.error(`Le patient ${codepatient} manque des données pour le traitement ARV : datetraitement: ${datetraitement}, dateprochainrendev: ${dateprochainrendev};Erreur 22;${codepatient}`);
            }
        }
    })
    
    
    /**************************** CONSULTATION *******************************************************/
    
    //read the csv file
    let csvFile_CONSULTATION = fs.readFileSync(csvFilename_CONSULTATION);
    
    //parse the csv file
    const records_CONSULTATION = parse(csvFile_CONSULTATION, {
        skip_empty_lines: true,
        delimiter: ';'
    })
    
    records_CONSULTATION.map((row, row_number) => {
    
        const CURRENT_TABLE = "CONSULTATION"
    
        const site = row [0]
        row.shift(); // Remove the first column that contains the health facility SIDA info code
    
        // [0] ExportDate - [1] codepatientIO - [2] dateConsultation - [3] stadeOMS - [4] CV_detectable - [5] CV_valeur - [6] datecv - [7] datebilan - [8] TBDateDepistage - [9] TBTypeExamen
        // [10] TBREsultat - [11] TBType - [12] TBDateDebutTraitement - [13] TBDateFinTraitement - [14] cd4_pourcentage - [15] cd4_abs - [16] datecd4
    
        const codepatient = site + SEPARATOR_PATIENT + row[1];
        const dateConsultation = row[2]
        const TBTypeExamen = row[9]
        const TBResultat = row[10]
        const TBDateDebutTraitement = row[12]; // TODO always EMPTY
    
        let patient = personList.find(x => x.code === codepatient);
        if (typeof patient !== 'undefined') {
            if (TBDateDebutTraitement && validateDate(Moment(TBDateDebutTraitement, SIDAINFO_DATEFORMAT), MIN_YEAR_EVENTS) == false) {
                patient.addLog(`23;TB Date Debut Traitement avec une DATE inattendu : ${TBDateDebutTraitement}.`)
                logger_generation.warn(`23;${codepatient};Patient ${codepatient} has a TB Date Debut Traitement with an unexpected DATE ${TBDateDebutTraitement}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
                logger_generation_fr.warn(`Le patient ${codepatient} a une TB Date Debut Traitement avec une DATE inattendu : ${TBDateDebutTraitement}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 23;${codepatient}`)
            }
    
            const dateConsultationMoment = Moment(dateConsultation, SIDAINFO_DATEFORMAT)
            if ((dateConsultation != "") && validateDate(dateConsultationMoment, MIN_YEAR_EVENTS)) {
                const datesToReview_moment = patient.consultations.map(t => t.dateConsultation).sort((a, b) => a - b);
                const datesToReview = datesToReview_moment.map(m => m.valueOf()); // moment().valueOf() function is used to get the number of milliseconds since the Unix Epoch.
                if (datesToReview.includes(dateConsultationMoment.valueOf())) {
                    patient.addLog(`03;Plus d'une consultation (table CONSULTATION) à la même date. La date dupliquée est: ${dateConsultationMoment.format(DHIS2_DATEFORMAT)}`)
                    logger_generation.warn(`03;${patient.code};Patient ${patient.code} has more than one consultation (table CONSULTATION) for the same date. The duplicated date is: ${dateConsultationMoment.format(DHIS2_DATEFORMAT)}.`)
                    logger_generation_fr.warn(`Le patient ${patient.code} a plus d'une consultation (table CONSULTATION) à la même date. La date dupliquée est: ${dateConsultationMoment.format(DHIS2_DATEFORMAT)}.;Erreur 03;${codepatient}`)
                } else {
                    patient.addConsultation(dateConsultationMoment, TBTypeExamen, TBResultat, TBDateDebutTraitement);
                }
            } else {
                patient.addLog(`24;CONSULTATION avec une DATE inattendu : ${dateConsultation}.`)
                logger_generation.warn(`24;${codepatient};Patient ${codepatient} has a CONSULTATION with an unexpected DATE ${dateConsultation}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
                logger_generation_fr.warn(`Le patient ${codepatient} a une CONSULTATION avec une DATE inattendu : ${dateConsultation}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 24;${codepatient}`)
            }
        } else if (codepatient === '') {
            logger_generation.error(`25;;Error;NO codepatient appears in row number ${row_number + 1} of the ${CURRENT_TABLE} table.`)
            logger_generation_fr.error(`Il n'y a pas de codepatient dans la ligne ${row_number + 1} de la table ${CURRENT_TABLE}.;Erreur 25;`)
        } else {
            patientCodeBlockList.add(codepatient);
            logger_generation.error(`18;${codepatient};Patient ${codepatient} appears in the ${CURRENT_TABLE} table but not in the File_Active table. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.error(`Le patient ${codepatient} est dans la table ${CURRENT_TABLE} mais n'est pas dans la table File_Active. Veuillez confirmer la ligne ${row_number + 1} de la table ${CURRENT_TABLE}.;Erreur 18;${codepatient}`)
        }
    })
    
    
    
    /**************************** FEMME ENCEINTE *******************************************************/
    
    //read the csv file
    let csvFile_FEMME_ENCEINTE = fs.readFileSync(csvFilename_FEMME_ENCEINTE);
    
    //parse the csv file
    const records_FEMME_ENCEINTE = parse(csvFile_FEMME_ENCEINTE, {
        skip_empty_lines: true,
        delimiter: ';'
    })
    
    records_FEMME_ENCEINTE.map((row, row_number) => {
    
        const CURRENT_TABLE = "FEMME_ENCEINTE"
    
        const site = row [0]
        row.shift(); // Remove the first column that contains the health facility SIDA info code

        row.unshift(row[0]) // WORKAROUND. Adding the ExportDate at the beginning will keep the order

        // [0] ExportDate - [1] ID (removed but duplicated manually the ExportDate in order to keep the order) - [2] CodePatientSuiviPTME - [3] DateVisiteSuiviPTME - [4] AmenorrheePTME - [5] DateAccoucheSuiviPTME - [6] LieuAccoucheSuivi - [7] ModeAccoucheSuivi - [8] StructureAccouchement - [9] EtatMere
        // [10] AllaitementMaternelPTME - [11] EtatEnfantSuiviPTME - [12] DateDecesSuivi - [13] PartenaireDateNaissance - [14] PartenaireConseil - [15] PartenaireDepiste - [16] PartenaireRecupere - [17] PartenaireResultat - [18] PartenaireRefere - [19] Cotri
        // [20] DateRegle - [21] Gestite - [22] DateProbable - [23] AgeGrossesseAccouche - [24] IssueGrossesse - [25] EnfantInfecte - [26] Taille - [27] DateGrossesse
        const codepatient = site + SEPARATOR_PATIENT + row[2]; //CodePatientSuiviPTME
    
        const enrollmentDate = row[3] //DateVisiteSuiviPTME
        const deliveryDate = row[5]; // DateAccoucheSuiviPTME. Could be EMPTY
        const deliveryOutcome = row[24]; // IssueGrossesse. Could be EMPTY
    
        const twoMonthsBefore = EXPORT_DUMP_DATE.clone().subtract(2, 'months');
        if (deliveryDate && isBeforeDate(Moment(deliveryDate, SIDAINFO_DATEFORMAT), Moment(enrollmentDate, SIDAINFO_DATEFORMAT)) && twoMonthsBefore.isBefore(Moment(enrollmentDate, SIDAINFO_DATEFORMAT))) {
            patientCodeBlockList.add(codepatient);
            logger_generation.error(`28;${codepatient};Patient ${codepatient} has an enrollment Date ${enrollmentDate} after the delivery Date ${deliveryDate}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
            logger_generation_fr.error(`Le patient ${codepatient} la DateVisiteSuiviPTME (${enrollmentDate}) n'est pas avant la DateAccoucheSuiviPTME (${deliveryDate}). Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 28;${codepatient}`);
        }
    
        let patient = personList.find(x => x.code === codepatient);
        if (typeof patient !== 'undefined') {
    
            if (enrollmentDate && validateDate(Moment(enrollmentDate, SIDAINFO_DATEFORMAT), MIN_YEAR_EVENTS) == false) {
                patient.addLog(`26;DateVisiteSuiviPTME avec une DATE inattendu : ${enrollmentDate}.`)
                logger_generation.warn(`26;${codepatient};Patient ${codepatient} has a enrollment Date with an unexpected DATE ${enrollmentDate}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
                logger_generation_fr.warn(`Le patient ${codepatient} a une DateVisiteSuiviPTME avec une DATE inattendu : ${enrollmentDate}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 26;${codepatient}`)
            }
    
            if (deliveryDate && validateDate(Moment(deliveryDate, SIDAINFO_DATEFORMAT), MIN_YEAR_EVENTS) == false) {
                patient.addLog(`27;DateAccoucheSuiviPTME avec une DATE inattendu : ${deliveryDate}.`)
                logger_generation.warn(`27;${codepatient};Patient ${codepatient} has a delivery Date with an unexpected DATE ${deliveryDate}. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table`)
                logger_generation_fr.warn(`Le patient ${codepatient} a une DateAccoucheSuiviPTME avec une DATE inattendu : ${deliveryDate}. Veuillez consulter la ligne ${row_number + 1} dans la table ${CURRENT_TABLE};Erreur 27;${codepatient}`)
            }
    
            const PTME_admission = patient.PTME_mere_admissions.find(x => x.datedebut === enrollmentDate);
            if (typeof PTME_admission !== 'undefined') {
                const pregnancy = new PTME_mere(codepatient, enrollmentDate, deliveryDate, deliveryOutcome, patient.getDatedepist())
                if (PTME_admission.datefin != "") {
                    pregnancy.setEnrollmentCloseDate(PTME_admission.datefin)
                }
                if (pregnancy.validatePTME_mere()) {
                    patient.addPregnancy(pregnancy);
                }
            } else {
                patientCodeBlockList.add(codepatient);
                logger_generation.error(`29;${codepatient};Patient ${codepatient} has a DateVisiteSuiviPTME (${enrollmentDate}) in the table ${CURRENT_TABLE} that is not present in the ADMISSION table. It should match the date debut of one of the PTME mere admissions. List of PTME meré admissions records of the table ADMISSION: ${JSON.stringify(patient.PTME_mere_admissions)}`)
                logger_generation_fr.error(`Le patient ${codepatient} a une DateVisiteSuiviPTME (${enrollmentDate}) dans la table ${CURRENT_TABLE} qui n'est pas présente dans la table ADMISSION. La DataVisiteSuiviPTME devrait correspondre à l'une des dateDebut des admissions PTME mère. Liste des admissions PTME mère de la table ADMISSION : ${JSON.stringify(patient.PTME_mere_admissions)};Erreur 29;${codepatient}`)
            }
        } else {
            patientCodeBlockList.add(codepatient);
            logger_generation.error(`18;${codepatient};Patient ${codepatient} appears in the ${CURRENT_TABLE} table but not in the File_Active table. Check the row number ${row_number + 1} in the ${CURRENT_TABLE} table.`)
            logger_generation_fr.error(`Le patient ${codepatient} est dans la table ${CURRENT_TABLE} mais n'est pas dans la table File_Active. Veuillez confirmer la ligne ${row_number + 1} de la table ${CURRENT_TABLE}.;Erreur 18;${codepatient}`)
        }
    })
    
    // validate PTME mere program
    personList.forEach(person => {
        if (person.getPregnancies().length > 0) {
            person.validatePTME_mere_programs();
        }
    });
    
    
    /**************************** LTFU & RTT generation *******************************************************/
    
    // Calculate ARV "booking date" (dueDate // scheduled)
    personList.forEach(person => {
        if (person.ARV_treatments.length > 0) {
            let previousTreatment;
            person.ARV_treatments.forEach(treatment => {
                if (typeof previousTreatment !== "undefined") {
                    treatment.dueDate = previousTreatment.dateprochainrendev;
                }
                previousTreatment = treatment
            })
        }
    });
    
    // Reminder: The TARV event for the LAST next visit is NOT created.
    
    // Calculate LTFU
    personList.forEach(person => {
        if (person.entryMode != '7') { // skipping PEP/PreP patients with ‘Mode d’entrée’=7 ‘Prophylaxie post-exposition’
            person.generateLTFU();
        }
    });
    
    // Calculate RTT
    personList.forEach(person => {
        person.LTFU.forEach(ltfu => {
            person.generateRTT(ltfu);
        })
    });
    
    
    /*******************************************************************************************/
    /*******************************************************************************************/
    
    function isDuplicated(code) {
        let patient = personList.find(x => x.code === code);
        if (typeof patient === 'undefined') {
            return false;
        } else {
            patientCodeBlockList.add(code);
            logger_generation.error(`30;${code};Patient ${code} is duplicated.`);
            logger_generation_fr.error(`Le patient ${code} est un doublon.;Erreur 30;${code}`);
            return true;
        }
    }
    
    // Order by date
    // dateArray is an array of strings
    function sortDates(dateArray) {
        dateArray.sort((a, b) => new Moment(a, SIDAINFO_DATEFORMAT) - new Moment(b, SIDAINFO_DATEFORMAT));
    }
    
    // In the array, the index-value MUST be a Moment object
    function sortDatesByIndex(dateArray, index) {
        dateArray.sort((a, b) => a[index] - b[index]);
    }
    
    
    // return true if the firstDate is previous or equal to the lastDate
    // parameter Moment Dates
    function isBeforeDate(firstDate, lastDate) {
        if (firstDate.isValid() && lastDate.isValid()) {
            return firstDate <= lastDate; //Assuming you pass a proper formatted date as a parameter
        } else {
            return false;
        }
    }
    
    // all the inputs are Moment dates
    function isBetweenDates(oldestDate, newestDate, expectedMiddleDate) {
        if (typeof newestDate == 'undefined') {
            newestDate = EXPORT_DUMP_DATE;
        }
        return expectedMiddleDate.isBetween(oldestDate, newestDate, 'days', '[]') // all inclusive
    }
    
    
    function validateOption(optionSetUID, optionToValidate, patientCode, tableName = "") {
        const validOptions = Object.values(optionSets[optionSetUID].options);
    
        if (!validOptions.includes(optionToValidate)) { //If NOT a valid option
            if (tableName == "") {
                patientCodeBlockList.add(patientCode);
                logger_generation.error(`31;${patientCode};Patient ${patientCode}. Unexpected option value: '${optionToValidate}' for the optionSet '${optionSets[optionSetUID].name}'. Expected values are: ${validOptions}`);
                logger_generation_fr.error(`Le patient ${patientCode} a une valeur inattendue '${optionToValidate}' pour la liste d'options '${optionSets[optionSetUID].name}'. Les valeurs attendues sont : ${validOptions};Erreur 31;${patientCode}`);
            } else {
                patientCodeBlockList.add(patientCode);
                logger_generation.error(`31;${patientCode};Patient ${patientCode}. In table ${tableName}, unexpected option value: '${optionToValidate}' for the optionSet '${optionSets[optionSetUID].name}'. Expected values are: ${validOptions}`);
                logger_generation_fr.error(`Le patient ${patientCode} a une valeur inattendue '${optionToValidate}' pour la liste d'options '${optionSets[optionSetUID].name}' de la table ${tableName}. Les valeurs attendues sont : ${validOptions};Erreur 31;${patientCode}`);
            }
            return false;
        }
        return true;
    }
    
    function validateCode(patientCode) {
        // regular expression to match required code format
        var re_patient_code = /^([0-9]{8}-[0-9]{6})$/;
        var re_enfant_code = /^([0-9]{8}-[0-9]{5}E)$/;
    
        if ((re_patient_code.test(patientCode) == false) && (re_enfant_code.test(patientCode) == false)) {
            patientCodeBlockList.add(patientCode);
            logger_generation.error(`32;${patientCode};Patient '${patientCode}' has a patient code that does not follow the expected pattern (6 numbers or 5 numbers + the character E))`);
            logger_generation_fr.error(`Le patient '${patientCode}' a un code de patient qui ne suit pas le modèle attendu (6 chiffres or 5 chiffres + la lettre E)).;Erreur 32;${patientCode}`);
        }
    }
    
    //date is a Moment date
    function validateDate(date, fromYear) {
        return date.isBetween(fromYear + '-12-31', undefined); // moment(undefined) evaluates as moment() [now]
    }
    
    
    function checkIfArrayIsUnique(myArray) {
        return myArray.length === new Set(myArray).size;
    }
    
    function getDuplicateArrayElements(arr) {
        var sorted_arr = arr.slice().sort();
        var results = [];
        for (var i = 0; i < sorted_arr.length - 1; i++) {
            if (sorted_arr[i + 1] === sorted_arr[i]) {
                results.push(sorted_arr[i]);
            }
        }
        return results;
    }
    
    function validatePCR_list(pcr_list, patient_enfant) {
        const patientCode = patient_enfant.code;
        let flag = true; // for checking if there is any invalid date in all th PCR list
    
        // Check that all valid results have a date associated
        pcr_list.forEach(pcr => {
            if (pcr.result != "") {
                if (validateOption("enfant_ptme_pcr_result", pcr.result, patientCode, "ENFANT_PTME") && pcr.date.isValid() == false) {
                    flag = false
                    patient_enfant.addLog(`33;Résultat PCR valid mais sans date : ${JSON.stringify(pcr)}`);
                    logger_generation.warn(`33;${patientCode};Patient ${patientCode} (enfant) has a valid PCR result BUT without PCR date: ${JSON.stringify(pcr)}`);
                    logger_generation_fr.warn(`Le patient ${patientCode} (enfant) a un résultat PCR valid mais sans date : ${JSON.stringify(pcr)};Erreur 33;${patientCode}`);
                }
            }
        })
    
        // Check that all dates are consecutives
        const pcr_dates = pcr_list.filter(pcr => pcr.date.isValid()).map(pcr => pcr.date)
        const pcr_dates_processed = [...pcr_dates]; // clone the array
        pcr_dates_processed.sort((a, b) => a - b)
        if (JSON.stringify(pcr_dates) != JSON.stringify(pcr_dates_processed)) {
            flag = false
            patient_enfant.addLog(`34;Le patient a des dates de PCR qui ne sont pas consécutives : ${JSON.stringify(pcr_dates)}`);
            logger_generation.warn(`34;${patientCode};Patient ${patientCode} (enfant) has PCR dates that are not consecutive: ${JSON.stringify(pcr_dates)}`);
            logger_generation_fr.warn(`Le patient ${patientCode} (enfant) a des dates de PCR qui ne sont pas consécutives : ${JSON.stringify(pcr_dates)};Erreur 34;${patientCode}`);
        }
    
        // Validate that all PCRs have different day
        const p_dates = pcr_dates.map(date => date.format(DHIS2_DATEFORMAT));
        const duplicateElements = findDuplicates(p_dates)
        if (duplicateElements.length > 0){
            flag = false
            patient_enfant.addLog(`35;Le patient (enfant) a deux PCR à la même date : ${JSON.stringify(pcr_dates)}`);
            logger_generation.warn(`35;${patientCode};Patient ${patientCode} (enfant) with more than one PCR on the very same date: ${duplicateElements}`);
            logger_generation_fr.warn(`Le patient ${patientCode} (enfant) a deux PCR à la même date : ${duplicateElements};Erreur 35;${patientCode}`);
        }
    
        return flag
    }
    
    function findDuplicates(arr){
        let sorted_arr = arr.slice().sort(); // You can define the comparing function here. 
        // JS by default uses a crappy string compare.
        // (we use slice to clone the array so the
        // original array won't be modified)
        let results = [];
        for (let i = 0; i < sorted_arr.length - 1; i++) {
          if (sorted_arr[i + 1] == sorted_arr[i]) {
            results.push(sorted_arr[i]);
          }
        }
        return results;
    }
    
    // initialDate = string "YYYY-MM-DD"
    // returns a Moment
    function calculateDate(initialDate, daysToAdd) {
        return new Moment(initialDate, SIDAINFO_DATEFORMAT).add(daysToAdd, 'days');
    }
    
    function saveJSONFile(filename, json_data) {
        try {
            fs.writeFileSync(filename, JSON.stringify(json_data, null, 2));
        } catch (err) {
            // An error occurred
            logger_generation.error(err);
        }
    }
    
    function getEventUID(codepatient, program, programStage, eventPrimaryKey) {
        if (codepatient in previous_patient_index) {
            if (program in previous_patient_index[codepatient]) {
                if (programStage in previous_patient_index[codepatient][program]) {
                    if (eventPrimaryKey in previous_patient_index[codepatient][program][programStage]) {
                        return previous_patient_index[codepatient][program][programStage][eventPrimaryKey];
                    }
                } else {
                    previous_patient_index[codepatient][program][programStage] = {}
                }
            } else {
                previous_patient_index[codepatient][program] = {}
                previous_patient_index[codepatient][program][programStage] = {}
            }
        }
    
        const new_uid = generateCode();
        previous_patient_index[codepatient][program][programStage][eventPrimaryKey] = new_uid;
        return new_uid;
    
    }
    
    function writeNewEvent(codepatient, program, programStage, eventPrimaryKey, eventUID) {
        if (codepatient in new_patient_index) {
            if (program in new_patient_index[codepatient]) {
                if (programStage in new_patient_index[codepatient][program]) {
                    if (eventPrimaryKey in new_patient_index[codepatient][program][programStage]) {
                        new_patient_index[codepatient][program][programStage][eventPrimaryKey] = eventUID;
                    }
                } else {
                    new_patient_index[codepatient][program][programStage] = {}
                }
            } else {
                new_patient_index[codepatient][program] = {}
                new_patient_index[codepatient][program][programStage] = {}
            }
        }
    
        new_patient_index[codepatient][program][programStage][eventPrimaryKey] = eventUID;
    }
    
    function writeNewEnrollment(codepatient, d_key, enrollmentDateFormat, enrollmentUID) {
        if (codepatient in new_patient_index) {
            if (d_key in new_patient_index[codepatient]) {
                new_patient_index[codepatient][d_key][enrollmentDateFormat] = enrollmentUID
            } else {
                new_patient_index[codepatient][d_key] = {}
                new_patient_index[codepatient][d_key][enrollmentDateFormat] = enrollmentUID
            }
        }
    }
    
    
    /*******************************************************************************************/
    /*******************************************************************************************/
    
    personList.forEach(person => {
        person.validatePerson()
    });
    
    logger_generation.info(`Initial patient list size ${personList.length}`);
    logger_generation_fr.info(`Taille de la liste initiale des patients ${personList.length}`);
    logger_generation.info(`Patient blocked list size ${patientCodeBlockList.size}`);
    logger_generation_fr.info(`Taille de la liste des patients bloqués ${patientCodeBlockList.size}`);
    
    const PARENT_TEIS_FOLDER = "GENERATED_data"
    const teis_folder = PARENT_TEIS_FOLDER + "/" + SOURCE_ID + "/" + "teis"
    const NEW_PATIENT_INDEX_FILENAME = PARENT_TEIS_FOLDER + "/" + SOURCE_ID + "/generated_all_patient_index.json";
    
    //check if TEIs folder exists. If not, create it
    if (!fs.existsSync(teis_folder)) {
        fs.mkdirSync(teis_folder, { recursive: true });
    }
    
    // Not all patientCodes in patienCodeBlockList are in the personList (because some could appear in Admission table but not in File Active or enfant PTME)
    personList.forEach(person => {
        if (patientCodeBlockList.has(person.code)) {
            logger_generation.error(`${person.code};Patient ${person.code} is blocked. Skipping payload generation`)
            //logger_generation_fr.info(`${person.code};Patient ${person.code} is bloqué. Ignorer payload generation`)
        } else if (["7", "9"].includes(person.entryMode)) {
            logger_generation.error(`42;${person.code};Patient ${person.code} is excluded due to entryMode ${person.entryMode} (prophylaxis patients and decentralised follow-ups are excluded from import).`)
            logger_generation_fr.error(`Le patient ${person.code} est exclu en raison du CodeModeEntrée ${person.entryMode} (les patients en prophylaxie post-exposition et les suivis décentralisés sont exclus de l’import);42;${person.code}`);
        } else if (person.has_enrollments() == false){
            logger_generation.error(`43;${person.code};Patient ${person.code} is blocked because they have no enrollments/admissions.`)
            logger_generation_fr.error(`Le patient ${person.code} est bloqué parce qu’ils n’ont pas d’inscriptions/admission;43;${person.code}`);
        } else {
            logger_generation.info("Generating TEI payload for patient " + person.code)
            saveJSONFile(PARENT_TEIS_FOLDER + "/" + SOURCE_ID + "/teis/" + person.getUID() + ".json", person.generateDHIS2_TEI_Payload())
        }
    });
    
    saveJSONFile(NEW_PATIENT_INDEX_FILENAME, new_patient_index);
}


module.exports = {
    generate_data
}