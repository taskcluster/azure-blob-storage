import assume        from 'assume';
import DataContainer from '../lib/DataContainer';
import _debug        from 'debug';
const debug = _debug('azure-blob-storage-test:data-container');
import {schema, credentials}      from './helpers';

describe('Azure Blob Storage - Data Container Tests', () => {
  const containerNamePrefix = 'test';
  let containerName = 'data-container-test';
  let container;

  before(() =>{
    assume(credentials.accountName).is.ok();
    assume(credentials.accountKey).is.ok();
  });

  it('create an instance of data container with azure credentials', async () => {
    container = await DataContainer({
      credentials: credentials,
      schema: schema,
      container: containerName,
    });

    assume(container).exists('Expected a data container instance.');
  });

  it('ensure an already created container', async () => {
    await container.ensureContainer();
  });

  it('should remove a blob by name', async () => {
    let blobName = 'blob-test';
    debug(`create a blob with name: ${blobName}`);
    let blob = await container.createDataBlockBlob({
      name: blobName,
    }, {
      value: 24,
    });

    await container.removeBlob(blobName);
  });

  it('should remove a blob by reference', async () => {
    let blobName = 'blob-test';
    debug(`create a blob with name: ${blobName}`);
    let blob = await container.createDataBlockBlob({
      name: blobName,
    }, {
      value: 24,
    });

    await container.removeBlob(blob);
  });

  it('should remove a non existing blob by name (ignore if not exists)', async () => {
    let blobName = 'unknown-blob';

    await container.removeBlob(blobName, true);
  });

  it('try to remove a non existing blob by name', async () => {
    let blobName = 'unknown-blob';

    try {
      await container.removeBlob(blobName);
    } catch (error) {
      assume(error.code).equals('BlobNotFound', 'Expected a `BlobNotFound` error.');
      return;
    }
    assume(false).is.true('Expected error when trying to remove a non existing blob.');
  });

  it('should create 5 blobs, scan, list blobs', async () => {
    let blobNamePrefix = 'blob-test';
    debug('create 5 data block blobs');
    for (let i = 1; i <= 5; i++) {
      await container.createDataBlockBlob({
        name: `${blobNamePrefix}${i}`,
        cacheContent: true,
      }, {
        value: i,
      });
    }

    debug('increment the `value` with 10 for every data block blob');
    let handler = async (blob) => {
      await blob.modify((content) => {
        content.value += 10;
        return content;
      });
    };
    await container.scanDataBlockBlob(handler);

    debug('list the blobs');
    let list = await container.listBlobs();
    assume(list).exists();
    let blobs = list.blobs;
    assume(blobs).is.array();
    assume(blobs.length).equals(5);

    debug('load the content of a data blob to check the value');
    let content = await blobs[0].load();
    assume(content).exists('Expected a content');
    assume(content.value - 10).is.most(10);
  });

  it('try to scan data block blobs with a handler function that throws an error', async () => {
    let scanError = new Error('scan error');
    let handler = async (blob) => {
      return Promise.reject(scanError);
    };
    try {
      await container.scanDataBlockBlob(handler);
    } catch (error) {
      assume(error).equals(scanError, 'Expected an error from scan');
      return;
    }
    assume(false).is.true('Expected error when trying to scan with a handler that throws an error');
  });

  it('should remove container', async () => {
    await container.removeContainer();
  });
});