const dotenv = require('dotenv');
const path = require('path');

// Note: Ensure you have a .env file and include 'log' and 'logError'.
const ENV_FILE = path.join(__dirname, '.env');
dotenv.config({ path: ENV_FILE });

const winston = require('winston');

function get_logger_retrieve(SOURCE_ID) {

  const log_filename = `${process.env.log_folder}/${SOURCE_ID}/${process.env.log_retrieve}`
  const log_error_filename = `${process.env.log_folder}/${SOURCE_ID}/${process.env.log_retrieve_error}`

  const logger_retrieve = winston.createLogger({
    level: 'debug',
    format: winston.format.combine(
      winston.format.timestamp({
        format: 'YYYY-MM-DD HH:mm:ss'
      }),
      winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
    ),
    transports: [
      // - Write all logs with level `warn` and below
      new winston.transports.File({ filename: log_error_filename, level: 'warn', json: false }),
      // - Write all logs with level `debug` and below
      new winston.transports.File({ filename: log_filename, level: 'debug', json: false }),
      // - Print on console all logs with level `info` and below
      new winston.transports.Console({ level: 'info' })
    ],
  });

  return logger_retrieve
}


function get_logger_generation(SOURCE_ID) {

  const log_filename = `${process.env.log_folder}/${SOURCE_ID}/${process.env.log_generation}`
  const log_error_filename = `${process.env.log_folder}/${SOURCE_ID}/${process.env.log_generation_error}`


  const logger_generation = winston.createLogger({
    level: 'debug',
    format: winston.format.combine(
      winston.format.timestamp({
        format: 'YYYY-MM-DD HH:mm:ss'
      }),
      winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
    ),
    transports: [
      // - Write all logs with level `warn` and below
      new winston.transports.File({ filename: log_error_filename, level: 'warn', json: false }),
      // - Write all logs with level `debug` and below
      new winston.transports.File({ filename: log_filename, level: 'debug', json: false }),
      // - Print on console all logs with level `info` and below
      new winston.transports.Console({ level: 'info' })
    ],
  });

  return logger_generation
}


function get_logger_generation_fr(SOURCE_ID) {

  const log_filename = `${process.env.log_folder}/${SOURCE_ID}/${process.env.log_generation_error_fr}`

  const logger_generation_fr = winston.createLogger({
    level: 'warn',
    format: winston.format.combine(
      winston.format.timestamp({
        format: 'YYYY-MM-DD HH:mm:ss'
      }),
      winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
    ),
    transports: [
      // - Write all logs with level `warn` and below
      new winston.transports.File({ filename: log_filename, level: 'warn', json: false })
    ],
  });
  return logger_generation_fr
}


function get_logger_diff(SOURCE_ID) {

  const log_filename = `${process.env.log_folder}/${SOURCE_ID}/${process.env.log_diff}`
  const logger_diff = winston.createLogger({
    level: 'debug',
    format: winston.format.combine(
      winston.format.timestamp({
        format: 'YYYY-MM-DD HH:mm:ss'
      }),
      winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
    ),
    transports: [
      // - Write all logs with level `debug` and below
      new winston.transports.File({ filename: log_filename, level: 'debug', json: false }),
      // - Print on console all logs with level `info` and below
      new winston.transports.Console({ level: 'info' })
    ],
  });
  return logger_diff  
}


function get_logger_upload(SOURCE_ID) {

  const log_filename = `${process.env.log_folder}/${SOURCE_ID}/${process.env.log_upload}`

  const logger_upload = winston.createLogger({
    level: 'debug',
    format: winston.format.combine(
      winston.format.timestamp({
        format: 'YYYY-MM-DD HH:mm:ss'
      }),
      winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
    ),
    transports: [
      // - Write all logs with level `debug` and below
      new winston.transports.File({ filename: log_filename, level: 'debug', json: false }),
      // - Print on console all logs with level `info` and below
      new winston.transports.Console({ level: 'info' })
    ],
  });

  return logger_upload;
}


module.exports = {get_logger_retrieve, get_logger_generation, get_logger_generation_fr, get_logger_diff, get_logger_upload};