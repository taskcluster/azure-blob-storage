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

  const schemav1 = {
    $schema: 'http://json-schema.org/draft-06/schema#',
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

  const schemav2 = {
    ...schemav1,
    properties: {
      value: {
        type: 'integer',
      },
      newValue: {
        type: 'integer',
      },
    },
    required: ['value', 'newValue'],
  };

  test('schema upgrades are supported with create', async () => {
    const {blobService, containerName} = await setupContainer(credentials);

    const containerv1 = new DataContainer({credentials, schema: schemav1, schemaVersion: 1, containerName});
    await containerv1.init();

    await containerv1.createDataBlockBlob({name: 'some-data'}, {value: 10});

    const containerv2 = new DataContainer({credentials, schema: schemav2, schemaVersion: 2, containerName});
    await containerv2.init();

    await containerv2.createDataBlockBlob({name: 'new-data'}, {value: 20, newValue: 40});

    const someDataBlob = await containerv2.load('some-data', false);
    assume(await someDataBlob.load()).to.deeply.equal({value: 10});
    const newDataBlob = await containerv2.load('new-data', false);
    assume(await newDataBlob.load()).to.deeply.equal({value: 20, newValue: 40});
  });

  test('schema upgrades are supported with modify', async () => {
    const {blobService, containerName} = await setupContainer(credentials);

    const containerv1 = new DataContainer({credentials, schema: schemav1, schemaVersion: 1, containerName});
    await containerv1.init();

    await containerv1.createDataBlockBlob({name: 'some-data'}, {value: 10});

    const containerv2 = new DataContainer({credentials, schema: schemav2, schemaVersion: 2, containerName});
    await containerv2.init();

    const someDataBlob = await containerv2.load('some-data', false);
    // modify the data, upgrading in the process
    await someDataBlob.modify(data => {
      data.value = 20;
      data.newValue = 40;
    });

    assume(await someDataBlob.load()).to.deeply.equal({value: 20, newValue: 40});
  });
});
