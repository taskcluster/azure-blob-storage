import config   from 'typed-env-config';
import fs       from 'fs';

let schema = fs.readFileSync(`${__dirname}/schemas/data_block_blob_schema.json`, 'utf8');
let schemaObj = JSON.parse(schema);

let logSchema = fs.readFileSync(`${__dirname}/schemas/log_schema.json`, 'utf8');
let logSchemObj = JSON.parse(logSchema);

let logSchemaV2 = fs.readFileSync(`${__dirname}/schemas/log_schemaV2.json`, 'utf8');
let logSchemV2Obj = JSON.parse(logSchemaV2);

let schemaV1 = fs.readFileSync(`${__dirname}/schemas/schema_v1.json`, 'utf8');
let schemaV1Obj = JSON.parse(schemaV1);

let schemaV2 = fs.readFileSync(`${__dirname}/schemas/schema_v2.json`, 'utf8');
let schemaV2Obj = JSON.parse(schemaV2);

let cfg = config({});
let credentials = {
  accountName: cfg.azureBlob.accountName,
  accountKey: cfg.azureBlob.accountKey,
};

module.exports = {
  schema: schemaObj,
  logSchema: logSchemObj,
  logSchemaV2: logSchemV2Obj,
  schemaV1: schemaV1Obj,
  schemaV2: schemaV2Obj,
  credentials: credentials,
};
