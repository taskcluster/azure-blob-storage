const fs = require('fs');
const taskcluster = require('taskcluster-client');

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

let credentials = {};
suiteSetup(async () => {
  credentials.accountId = process.env.AZURE_ACCOUNT;
  credentials.accessKey = process.env.AZURE_ACCOUNT_KEY;

  if (credentials.accountId && credentials.accessKey) {
    return;
  }

  // load credentials from the secret if running in CI
  if (process.env.TASKCLUSTER_PROXY_URL) {
    console.log('loading credentials from secret via TASKCLUSTER_PROXY_URL');
    const client = new taskcluster.Secrets({rootUrl: process.env.TASKCLUSTER_PROXY_URL});
    const res = await client.get('project/taskcluster/testing/azure');
    credentials.accountId = res.secret.AZURE_ACCOUNT;
    credentials.accessKey = res.secret.AZURE_ACCOUNT_KEY;
    return;
  }

  console.error('set $AZURE_ACCOUNT and $AZURE_ACCOUNT_KEY to a testing Azure storage account.');
  process.exit(1);
});

module.exports = {
  schema: schemaObj,
  logSchema: logSchemObj,
  logSchemaV2: logSchemV2Obj,
  schemaV1: schemaV1Obj,
  schemaV2: schemaV2Obj,
  credentials: credentials,
};
