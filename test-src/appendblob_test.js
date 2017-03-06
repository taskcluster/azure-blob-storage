import assume        from 'assume';
import DataContainer from '../lib/datacontainer';
import {AppendDataBlob}    from '../lib/datablob';
import uuid          from 'uuid';
import _debug        from 'debug';
const debug = _debug('azure-blob-storage-test:data-container');
import {logSchema, credentials}      from './helpers';
import {sleep}       from '../lib/utils';

describe('Azure Blob Storage - Append Data Blob Tests', () => {
  let dataContainer;
  const containerName = `${uuid.v4()}`;
  const blobNamePrefix = 'blob';

  before(async () => {
    assume(credentials.accountName).is.ok();
    assume(credentials.accountKey).is.ok();
    dataContainer = await DataContainer({
      credentials: credentials,
      schema: logSchema,
      container: containerName,
    });

    assume(dataContainer).exists('Expected a data container instance');
  });

  after(async () => {
    if (dataContainer) {
      await dataContainer.removeContainer();
    }
  });

  it('should create an append data blob, append content, list, load and delete', async () => {
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

  it('should create an append data blob, try to append an invalid data', async () => {
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
    assume(false).is.true('Expected an error when trying to modify a deleted blob.');
  });
});