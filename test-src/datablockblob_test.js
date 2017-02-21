import DataContainer      from '../lib/datacontainer';
import {DataBlockBlob}    from '../lib/datablob';
import assume             from 'assume';
import uuid               from 'uuid';
import _debug             from 'debug';
const debug = _debug('azure-blob-storage-test:datablockblob');
import {schema, credentials} from './helpers';

describe('Azure Blob Storage - Data Block Blob', () => {
  let dataContainer;
  const containerName = 'container-blob-test-data-block-blob';
  const blobNamePrefix = 'blob';

  before(async () => {
    assume(credentials.accountName).is.ok();
    assume(credentials.accountKey).is.ok();
    dataContainer = await DataContainer({
      credentials: credentials,
      schema: schema,
      container: containerName,
    });

    assume(dataContainer).exists('Expected a data container instance');
  });

  after(async () => {
    if (dataContainer) {
      await dataContainer.removeContainer();
    }
  });

  it('should create a data block blob (no cache content), list, load and delete blob', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await dataContainer.createDataBlockBlob({
      name: blobName,
    }, {
      value: 40,
    });

    assume(blob instanceof DataBlockBlob).is.ok('Expected an instance of DataBlockBlob');
    assume(blob.content).is.a('undefined', `The blob ${blobName} content expected to be undefined`);

    debug('check if the data block blob was created');
    let list = await dataContainer.listBlobs({
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

    debug(`delete the data block blob with name ${blobName}`);
    await blob.remove();
  });

  it('try to create a data block blob with invalid data', async() => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    try {
      let blob = await dataContainer.createDataBlockBlob({
        name: blobName,
      }, {
        value: 'wrong value',
      });
    } catch (error) {
      assume(error.code).equals('SchemaValidation', 'Expected a SchemaValidationError');
      assume(error.content).exists();
      return;
    }

    assume(false).is.true('Expected a validation error.');
  });

  it('should create a data block blob, modify and load', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await dataContainer.createDataBlockBlob({
      name: blobName,
    }, {
      value: 24,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    debug(`update the content of the blob: ${blobName}`);
    let modifier = (data) => {
      data.value = 40;
      return data;
    };
    await blob.modify(modifier);

    debug('test the content updated');
    let content = await blob.load();
    assume(content.value).equals(40);
  });

  it('should create a data block blob (with cache content), list, modify', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await dataContainer.createDataBlockBlob({
      name: blobName,
      cacheContent: true,
    }, {
      value: 40,
    });

    assume(blob instanceof DataBlockBlob).is.ok();
    assume(blob.content).exists('The blob content should have been cached.');

    debug('check if the data block blob was created');
    let list = await dataContainer.listBlobs({
      prefix: blobName,
    });
    assume(list).exists();
    let blobs = list.blobs;
    assume(blobs).is.array();
    assume(blobs.length).equals(1);
    assume(blobs[0] instanceof DataBlockBlob).is.ok();

    debug(`update the content of the blob: ${blobName}`);
    let modifier = (data) => {
      data.value = 50;
      return data;
    };
    await blob.modify(modifier);
    assume(blob.content.value).equals(50, 'The content of the blob should have been updated with value 50');
  });

  it('should create a data block blob (no cache content), modify and throw an error', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await dataContainer.createDataBlockBlob({
      name: blobName,
    }, {
      value: 40,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    debug('throw an error in the modifier function');
    let err = new Error('Test the error');
    try {
      await blob.modify(() => {
        throw err;
      });
    } catch (error) {
      assume(error).equals(err, 'Expected an error from modifier function.');
      return;
    }

    assume(false).is.true('The modifier function should have been thrown an error');
  });

  it('should create a data block blob (with cache content), modify and throw an error', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await dataContainer.createDataBlockBlob({
      name: blobName,
      cacheContent: true,
    }, {
      value: 40,
    });
    assume(blob instanceof DataBlockBlob).is.ok();
    assume(blob.content.value).equals(40);

    debug('throw an error in the modifier function');
    let err = new Error('Test the error');
    try {
      await blob.modify(() => {
        throw err;
      });
    } catch (error) {
      assume(error).equals(err, 'A error should have been thrown in modifier function.');
      assume(blob.content.value).equals(40, 'The cached content shouldn\'t have been modified.');
      return;
    }
    assume(false).is.true('The modifier function should have been thrown an error');
  });

  it('should create a data block blob (no cache content), try modify a data blob blob with wrong data', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await dataContainer.createDataBlockBlob({
      name: blobName,
    }, {
      value: 24,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    debug(`try to update the content of the blob: ${blobName} with an invalid data`);
    let modifier = (data) => {
      data.value = 'wrong value';
      return data;
    };
    try {
      await blob.modify(modifier);
    } catch (error) {
      assume(error.code).equals('SchemaValidation', 'Expected a schema validation error');
      return;
    }

    assume(false).is.true('The modifier function should have been thrown an error');
  });

  it('should create a data block blob (no cache content), delete and try to modify the deleted blob', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await dataContainer.createDataBlockBlob({
      name: blobName,
    }, {
      value: 24,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    debug('delete the created blob');
    await blob.remove();

    debug('try to modify the deleted blob');
    try {
      await blob.modify((data) => {
        data.value = 80;
      });
    } catch (error) {
      assume(error.code).equals('BlobNotFound', 'Expected a `BlobNotFound` error');
      return;
    }
    assume(false).is.true('Expected an error when trying to modify a deleted blob.');
  });

  it('should create a data block blob (with cache content), modify concurrent', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await dataContainer.createDataBlockBlob({
      name: blobName,
      cacheContent: true,
    }, {
      value: 24,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    await Promise.all([
      blob.modify((data) => {
        data.value += 10;
        return data;
      }),
      blob.modify((data) => {
        data.value += 10;
        return data;
      }),
    ]);
    assume(blob.content.value).equals(44, 'The content of the blob should have been modified.');
  });

  it('should create a data block blob, modify the content, delete (ignoreChanges=true)', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await dataContainer.createDataBlockBlob({
      name: blobName,
      cacheContent: true,
    }, {
      value: 24,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    debug(`modify the content of the blob with name: ${blobName}`);
    await blob.modify((data) => {
      data.value = 80;
      return data;
    });

    debug(`remove(ignoreChanges=true) the blob with name: ${blobName}`);
    await blob.remove(true);
  });

  it('should create a data block blob, delete the blob, try to delete' +
    ' again the same blob (ignoreIfNotExists=true)', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await dataContainer.createDataBlockBlob({
      name: blobName,
      cacheContent: true,
    }, {
      value: 24,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    debug(`remove the blob with name: ${blobName}`);
    await blob.remove();

    debug(`remove (ignoreIfNotExists=true) again the blob with name: ${blobName}`);
    await blob.remove(false, true);
  });

  it('should create a data block blob, delete the blob, try to delete' +
    ' again the same blob (ignoreIfNotExists=false)', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await dataContainer.createDataBlockBlob({
      name: blobName,
      cacheContent: true,
    }, {
      value: 24,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    debug(`remove the blob with name: ${blobName}`);
    await blob.remove();

    debug(`remove (ignoreIfNotExists=false) again the blob with name: ${blobName}`);
    try {
      await blob.remove(false, false);
    } catch (error) {
      assume(error.code).equals('BlobNotFound', 'Expected a `BlobNotFound` error.');
      return;
    }
    assume(false).is.true('An error should have been thrown because the blob was already removed.');
  });
});