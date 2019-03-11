const assume = require('assume');
const azure = require('fast-azure-storage');
const debug = require('debug')('azure-blob-storage-test:data-container');
const {schema, credentials} = require('./helpers');
const uuid = require('uuid');
const {DataContainer, DataBlockBlob, AppendDataBlob} = require('../');

suite('Schema Versioning', () => {
  const containerNamePrefix = 'test';
  let onTeardown = null;

  teardown(async function() {
    if (onTeardown) {
      await onTeardown();
      onTeardown = null;
    }
  });

  const setupContainer = async (credentials) => {
    const containerName = `data-container-test${uuid.v4()}`;
    const blobService = new azure.Blob(credentials);
    await blobService.createContainer(containerName);

    onTeardown = async () => {
      await blobService.deleteContainer(containerName);
    };

    return {blobService, containerName};
  };

  test('create an instance of data container with a schema from before v4.0.0', async () => {
    const {blobService, containerName} = await setupContainer(credentials);

    // write out an "old-style" schema.  Notably, this old schema has 'id', not '$id'..
    const schema = {
      $schema: 'http://json-schema.org/draft-06/schema#',
      id: `http://${credentials.accountId}.blob.core.windows.net/${containerName}/.schema.v1.json`,
      title: 'test json schema',
      type: 'object',
      properties: {
        value: {
          type: 'integer',
        },
      },
      additionalProperties: false,
      required: ['value'],
    };
    await blobService.putBlob(containerName, '.schema.v1', {type: 'BlockBlob'}, JSON.stringify(schema));

    // also write out a data value to fetch
    const content = {version: 1, content: {value: 13}};
    await blobService.putBlob(containerName, 'some-data', {type: 'BlockBlob'}, JSON.stringify(content));

    const container = new DataContainer({credentials, schema, containerName});
    await container.init();

    const blob = await container.load('some-data');

    const before = await blob.load();
    assume(before.value).to.equal(13);

    await blob.modify(blob => {
      blob.value = 14;
    });

    const after = await blob.load();
    assume(after.value).to.equal(14);
  });
});
