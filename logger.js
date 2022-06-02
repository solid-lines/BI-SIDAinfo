const dotenv = require('dotenv');
const path = require('path');

// Note: Ensure you have a .env file and include 'log' and 'logError'.
const ENV_FILE = path.join(__dirname, '.env');
dotenv.config({ path: ENV_FILE });

const winston = require('winston');

const logger = winston.createLogger({
  level: 'debug',
  format: winston.format.combine(
 
    // winston.format.timestamp({
    //   format: 'YYYY-MM-DD HH:mm:ss'
    // }),
    winston.format.json()
  ),
  transports: [
    // - Write all logs with level `error` and below to `error.log`
    new winston.transports.File({ format: winston.format.simple(), filename: process.env.logError, level: 'warn', json: false }),
    // - Write all logs with level `debug` and below to `SIDAInfo.log`
    new winston.transports.File({ format: winston.format.simple(), filename: process.env.log, level: 'debug', json: false }),
    new winston.transports.Console({format: winston.format.simple(), level: 'info'})
  ],
});


const logger_fr = winston.createLogger({
  level: 'warn',
  format: winston.format.simple(),
  transports: [
    new winston.transports.File({ format: winston.format.simple(), filename: process.env.logErrorFR, level: 'warn'})
  ],
});

const logger_upload = winston.createLogger({
  level: 'debug',
  format: winston.format.combine(
 
    // winston.format.timestamp({
    //   format: 'YYYY-MM-DD HH:mm:ss'
    // }),
    winston.format.json()
  ),
  transports: [
    // - Write all logs with level `debug` and below to `SIDAInfo.log`
    new winston.transports.File({ format: winston.format.simple(), filename: process.env.log_upload, level: 'debug', json: false }),
    new winston.transports.Console({format: winston.format.simple(), level: 'info'})
  ],
});


const logger_upload_fr = winston.createLogger({
  level: 'warn',
  format: winston.format.simple(),
  transports: [
    new winston.transports.File({ format: winston.format.simple(), filename: process.env.log_upload_fr, level: 'warn'})
  ],
});

const logger_diff = winston.createLogger({
  level: 'debug',
  format: winston.format.combine(
 
    // winston.format.timestamp({
    //   format: 'YYYY-MM-DD HH:mm:ss'
    // }),
    winston.format.json()
  ),
  transports: [
    // - Write all logs with level `debug` and below to `SIDAInfo.log`
    new winston.transports.File({ format: winston.format.simple(), filename: process.env.log_diff, level: 'debug', json: false }),
    new winston.transports.Console({format: winston.format.simple(), level: 'info'})
  ],
});


module.exports = {logger, logger_fr, logger_diff, logger_upload, logger_upload_fr};