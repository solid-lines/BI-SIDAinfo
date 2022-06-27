const yargs = require('yargs');
const retrieve = require("./retrieve_DHIS_data.js");
const diff = require("./diff.js");
const upload = require("./upload_data.js");

/**************************************/

/**
 * Command line tool configuration
 */
const argv = yargs
    .command('retrieve', 'Retrieve data from given Organization Unit (Health Facility)', {
        org_unit: {
            description: 'the Organization Unit code (e.g. 17020203)',
            alias: ['o', 'ou'],
            type: 'text'
        },
        export_dump_date: {
            description: 'the CSV export dump date (YYYY-MM-DD)',
            type: 'text',
        }
    })
    .command('diff', 'Generate diff changes file from a given Organization Unit (Health Facility) and two Export Dates (previous data dump and current data dump)', {
        org_unit: {
            description: 'the Organization Unit code (e.g. 17020203)',
            alias: ['o', 'ou'],
            type: 'text',
        },
        export_dump_date: {
            description: 'the CSV export dump date (YYYY-MM-DD)',
            type: 'text',
        }
    })
    .command('upload', 'Upload changes for a given Organization Unit (Health Facility)', {
        org_unit: {
            description: 'the Organization Unit code (e.g. 17020203)',
            alias: ['o', 'ou'],
            type: 'text',
        },
        export_dump_date: {
            description: 'the CSV export dump date (YYYY-MM-DD)',
            type: 'text',
        }
    })
    .demandOption(['org_unit', 'export_dump_date'], 'Please provide both Organization Unit code and export date arguments to work with this tool')
    .help()
    .alias('help', 'h')
    .argv;

/**************************************/

/**
 * Parsing arguments
 */
if (argv._.includes('diff')) {
    const SOURCE_OU_CODE = argv.org_unit;
    const SOURCE_DATE = argv.export_dump_date;
    diff.generate_diff(SOURCE_OU_CODE, SOURCE_DATE)
} else if (argv._.includes('retrieve')) {
    const SOURCE_OU_CODE = argv.org_unit;
    const SOURCE_DATE = argv.export_dump_date;
    retrieve.retrieve_data(SOURCE_OU_CODE, SOURCE_DATE)
} else if (argv._.includes('upload')) {
    const SOURCE_OU_CODE = argv.org_unit;
    const SOURCE_DATE = argv.export_dump_date;
    upload.upload_data(SOURCE_OU_CODE, SOURCE_DATE)
} else {
    process.exit(1)
}
