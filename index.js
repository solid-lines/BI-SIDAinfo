const yargs = require('yargs');
const retrieve = require("./retrieveDHISdata.js");
const diff = require("./diff.js");
const upload = require("./uploadData.js");

/**************************************/

/**
 * Command line tool configuration
 */
const argv = yargs
    .command('retrieve', 'Retrieve data from given Organization Unit (Health Facility)', {
        org_unit: {
            description: 'the Organization Unit code (e.g. 17020203)',
            alias: ['o', 'ou'],
            type: 'text'        }
    })
    .command('diff', 'Generate diff changes file from a given Organization Unit (Health Facility) and two Export Dates (previous data dump and current data dump)', {
        org_unit: {
            description: 'the Organization Unit code (e.g. 17020203)',
            alias: ['o', 'ou'],
            type: 'text',
        }
    })
    .command('upload', 'Upload changes for a given Organization Unit (Health Facility)', {
        org_unit: {
            description: 'the Organization Unit code (e.g. 17020203)',
            alias: ['o', 'ou'],
            type: 'text',
        }
    })
    .demandOption(['org_unit'], 'Please provide Organization Unit code argument to work with this tool')
    .help()
    .alias('help', 'h')
    .argv;

/**************************************/

/**
 * Parsing arguments
 */
if (argv._.includes('diff')) {
    const SOURCE_OU_CODE = argv.org_unit;
    diff.generate_diff(SOURCE_OU_CODE)
} else if (argv._.includes('retrieve')) {
    const SOURCE_OU_CODE = argv.org_unit;
    retrieve.retrieve_data(SOURCE_OU_CODE)
} else if (argv._.includes('upload')) {
    const SOURCE_OU_CODE = argv.org_unit;
    upload.upload_data(SOURCE_OU_CODE)
} else {
    process.exit(1)
}
