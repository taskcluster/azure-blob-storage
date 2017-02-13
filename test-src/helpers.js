import config   from 'typed-env-config';
import fs       from 'fs';

let schema = fs.readFileSync(`${__dirname}/schemas/data_block_blob_schema.json`, 'utf8');
let schemaObj = JSON.parse(schema);

let logSchema = fs.readFileSync(`${__dirname}/schemas/log_schema.json`, 'utf8');
let logSchemObj = JSON.parse(logSchema);

let cfg = config({});
let credentials = {
  accountName: cfg.azureBlob.accountName,
  accountKey: cfg.azureBlob.accountKey,
};

module.exports = {
  schema: schemaObj,
  logSchema: logSchemObj,
  credentials: credentials,
};
