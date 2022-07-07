const yargs = require('yargs');
const retrieve = require("./retrieve_DHIS_data.js");
const generate = require("./generate.js");
const diff = require("./diff.js");
const upload = require("./upload_data.js");

/**************************************/

/**
 * Command line tool configuration
 */
const argv = yargs
    .command('retrieve', 'Retrieve data from given site (Health Facility)', {
        site: {
            description: 'the site code (e.g. 17020203)',
            type: 'text'
        },
        export_dump_date: {
            description: 'the CSV export dump date (YYYY-MM-DD)',
            type: 'text',
        }
    })
    .command('generate', 'Generate data from given site (Health Facility)', {
        site: {
            description: 'the site code (e.g. 17020203)',
            type: 'text'
        },
        export_dump_date: {
            description: 'the CSV export dump date (YYYY-MM-DD)',
            type: 'text',
        }
    })
    .command('diff', 'Generate diff changes file from a given site (Health Facility) and two Export Dates (previous data dump and current data dump)', {
        site: {
            description: 'the site code (e.g. 17020203)',
            type: 'text',
        },
        export_dump_date: {
            description: 'the CSV export dump date (YYYY-MM-DD)',
            type: 'text',
        }
    })
    .command('upload', 'Upload changes for a given site (Health Facility)', {
        site: {
            description: 'the site code (e.g. 17020203)',
            type: 'text',
        },
        export_dump_date: {
            description: 'the CSV export dump date (YYYY-MM-DD)',
            type: 'text',
        }
    })
    .demandOption(['site', 'export_dump_date'], 'Please provide both site code and export date arguments to work with this tool')
    .help()
    .alias('help', 'h')
    .version()
    .alias('version', 'v')
    .argv;

/**************************************/

/**
 * Parsing arguments
 */
if (argv._.includes('retrieve')) {
    const SOURCE_OU_CODE = argv.site;
    const SOURCE_DATE = argv.export_dump_date;
    retrieve.retrieve_data(SOURCE_OU_CODE, SOURCE_DATE)
} else if (argv._.includes('generate')) {
    const SOURCE_OU_CODE = argv.site;
    const SOURCE_DATE = argv.export_dump_date;
    generate.generate_data(SOURCE_OU_CODE, SOURCE_DATE)
} else if (argv._.includes('diff')) {
    const SOURCE_OU_CODE = argv.site;
    const SOURCE_DATE = argv.export_dump_date;
    diff.generate_diff(SOURCE_OU_CODE, SOURCE_DATE)
} else if (argv._.includes('upload')) {
    const SOURCE_OU_CODE = argv.site;
    const SOURCE_DATE = argv.export_dump_date;
    upload.upload_data(SOURCE_OU_CODE, SOURCE_DATE)
} else {
    process.exit(1)
}
