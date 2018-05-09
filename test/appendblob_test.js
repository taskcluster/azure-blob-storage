const assume = require('assume');
const debug = require('debug')('azure-blob-storage-test:data-blob');
const {logSchema, credentials} = require('./helpers');
const uuid = require('uuid');
const {DataContainer, AppendDataBlob} = require('../lib');

suite('Azure Blob Storage - Append Data Blob Tests', () => {
  let dataContainer;
  const containerName = `container-blob-append-test${uuid.v4()}`;
  const blobNamePrefix = 'blob';

  suiteSetup(async () => {
    assume(credentials.accountId).is.ok();
    assume(credentials.accessKey).is.ok();
    dataContainer = new DataContainer({
      credentials: credentials,
      schema: logSchema,
      containerName,
    });
    await dataContainer.init();

    assume(dataContainer).exists('Expected a data container instance');
  });

  suiteTeardown(async () => {
    if (dataContainer) {
      await dataContainer.removeContainer();
    }
  });

  test('should create an append data blob, append content, list, load and delete', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create an append blob with name: ${blobName}`);
    let blob = await dataContainer.createAppendDataBlob({
      name: blobName,
    });
    assume(blob instanceof AppendDataBlob).is.ok('Expected an instance of AppendDataBlob');

    debug('append content to the blob');
    let log = {
      event: 'serverfault',
      code: 5234,
      reason: 'Disk full',
    };
    await blob.append(log);

    debug('check if the data block blob was created');
    let list = await dataContainer.listBlobs({
      prefix: blobName,
    });
    assume(list).exists();
    let blobs = list.blobs;
    assume(blobs).is.array();
    assume(blobs.length).equals(1);
    assume(blobs[0] instanceof AppendDataBlob).is.ok();

    debug('load the content of the append blob');
    let data = await blob.load();
    assume(data).equals(JSON.stringify(log));

    debug(`delete the data block blob with name ${blobName}`);
    await blob.remove();
  });

  test('should create an append data blob, try to append an invalid data', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create an append blob with name: ${blobName}`);
    let blob = await dataContainer.createAppendDataBlob({
      name: blobName,
    });
    assume(blob instanceof AppendDataBlob).is.ok('Expected an instance of AppendDataBlob');

    debug('try to append invalid content to the blob');
    let log = {
      code: 'wrong value',
    };
    try {
      await blob.append(log);
    } catch (error) {
      assume(error.code).equals('SchemaValidation',
        'A SchemaValidationError should have been thrown for invalid data');
      return;
    }
    assume(false).is.true('Expected an error when trying to append invalid content.');
  });
});
