const {schemaV1, schemaV2, logSchema, logSchemaV2, credentials} = require('./helpers');
const assume = require('assume');
const debug = require('debug')('azure-blob-storage-test:version');
const uuid = require('uuid');
const {DataContainer, DataBlockBlob} = require('../lib');

describe('Azure Blob Storage - Data Container Version Support', () => {

  let containerName = `${uuid.v4()}`;
  let logContainerName = `${uuid.v4()}`;
  let blobV1Name = 'blobV1';
  let blobV2Name = 'blobV2';
  let appendBlobName = 'appendBlob';
  let dataContainerV1;
  let dataContainerV2;
  let logContainerV1;
  let logContainerV2;

  before(async () =>{
    assume(credentials.accountName).is.ok();
    assume(credentials.accountKey).is.ok();

    dataContainerV1 = new DataContainer({
      credentials: credentials,
      schema: schemaV1,
      schemaVersion: 1,
      containerName,
    });
    await dataContainerV1.init();

    dataContainerV2 = new DataContainer({
      credentials: credentials,
      schema: schemaV2,
      schemaVersion: 2,
      containerName,
    });
    await dataContainerV2.init();

    logContainerV1 = new DataContainer({
      credentials: credentials,
      schema: logSchema,
      schemaVersion: 1,
      containerName: logContainerName,
    });
    await logContainerV1.init();

    logContainerV2 = new DataContainer({
      credentials: credentials,
      schema: logSchemaV2,
      schemaVersion: 2,
      containerName: logContainerName,
    });
    await logContainerV2.init();
  });

  after(async () => {
    if (dataContainerV1) {
      await dataContainerV1.removeContainer();
    }
    if (dataContainerV2) {
      await dataContainerV2.removeContainer();
    }

    if (logContainerV1) {
      await logContainerV1.removeContainer();
    }
    if (logContainerV2) {
      await logContainerV2.removeContainer();
    }
  });

  it('should create a dataBlockBlob in a container with version 1 of schema', async () => {
    let blobName = `${blobV1Name}`;
    debug(`create a dataBlockBlob with name: ${blobName}`);
    let blob = await dataContainerV1.createDataBlockBlob({
      name: blobName,
    }, {
      value: 40,
    });

    debug('check if the data block blob was created');
    let list = await dataContainerV1.listBlobs({
      prefix: blobName,
    });
    assume(list).exists();
    let blobs = list.blobs;
    assume(blobs).is.array();
    assume(blobs.length).equals(1);
    assume(blobs[0] instanceof DataBlockBlob).is.ok();

    debug('check the content of the data block blob created.');
    let data = await blob.load();
    assume(data.value).equals(40);
  });

  it('should create a dataBlockBlob in a data container with version 2 of json schema', async () => {
    let blobName = `${blobV2Name}`;
    debug(`create a dataBlockBlob with name: ${blobName}`);
    let blob = await dataContainerV2.createDataBlockBlob({
      name: blobName,
    }, {
      name: 'blobV2',
    });

    debug('check if the data block blob was created');
    let list = await dataContainerV2.listBlobs({
      prefix: blobName,
    });
    assume(list).exists();
    let blobs = list.blobs;
    assume(blobs).is.array();
    assume(blobs.length).equals(1);
    assume(blobs[0] instanceof DataBlockBlob).is.ok();

    debug('check the content of the data block blob created.');
    let data = await blob.load();
    assume(data.name).equals('blobV2');
  });

  it('should load a dataBlockBlob with v1 from a data container with v2', async () => {
    debug('load the blob created with v1 of the schema');
    let blobV1 = await dataContainerV2.load(blobV1Name, true);

    debug(`update the content of the blob: ${blobV1Name}`);
    let modifier = (data) => {
      data.value = 100;
    };
    await blobV1.modify(modifier);

    debug('test the content updated');
    let content = await blobV1.load();
    assume(content.value).equals(100);
  });

  it('should create an append data blob with version 1 of schema, update the content with v2', async () => {
    debug(`create an append data blob with name ${appendBlobName}`);
    let appendBlob = await logContainerV1.createAppendDataBlob({name: appendBlobName});

    debug('append data with v1');
    let logV1 = {
      event: 'serverfault',
      code: 5234,
      reason: 'Disk full',
    };
    await appendBlob.append(logV1);

    let content = await appendBlob.load();
    assume(content).equals(JSON.stringify(logV1));

    appendBlob = await logContainerV2.load(appendBlobName);

    debug('append data with v2');
    let logV2 = {
      event: 'networkfault',
      code: 5236,
      reason: 'Connetion error',
      location: 'US',
    };
    await appendBlob.append(logV2);

    content = await appendBlob.load();
    assume(content).equals(JSON.stringify(logV1)+JSON.stringify(logV2));
  });
});
