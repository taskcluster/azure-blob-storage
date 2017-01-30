import BlobStorage from '../lib/blobstorage';
import Container from '../lib/container';
import {Blob, BlockBlob, DataBlockBlob} from '../lib/blob';
import assume from 'assume';
import config from 'typed-env-config';
import _debug from 'debug';
const debug = _debug('test:blob');
import uuid from 'uuid';

describe('Azure Blob Storage - Blob', () => {
  let accountId;
  let accessKey;
  let blobStorage;
  const containerNamePrefix = 'test';
  const blobNamePrefix = 'blob';
  let container;

  before(async () => {
    // Load configuration
    let cfg = config({});
    accountId = cfg.azureBlob.accountId;
    accessKey = cfg.azureBlob.accessKey;
    assume(accountId).is.ok();
    assume(accessKey).is.ok();
    blobStorage = new BlobStorage({
      credentials: {
        accountId,
        accessKey,
      },
    });

    let name = `${containerNamePrefix}${uuid.v4()}`;
    debug(`container name: ${name}`);

    debug('creating container with an associated schema');
    // TODO - move this in a file
    let schema = '{"$schema":      "http://json-schema.org/draft-04/schema#",' +
      '"title":        "test json schema",' +
      '"type":         "object",' +
      '"properties": {' +
      '"value": {' +
      '"type":           "integer"' +
      '}' +
      '},' +
      '"additionalProperties":   false,' +
      '"required": ["value"]' +
      '}';

    let schemaObj = JSON.parse(schema);
    container = await blobStorage.createContainer({
      name: name,
      schema: schemaObj,
    });
    assume(container instanceof Container).is.ok();
    assume(container.schemaId).equals(`http://schemas.taskcluster.net/${name}#`);
  });

  after(async () => {
    await blobStorage.deleteContainer({
      name: container.name,
    });
  });

  it('should create, list, load and delete a data block blob', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await container.createDataBlob({
      name: blobName,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    debug(`upload content to blob: ${blobName}`);
    let result = await blob.create({
      value: 40,
    });

    let list = await container.listBlobs({
      prefix: blobName,
    });
    assume(list).exists();
    let blobs = list.blobs;
    assume(blobs).is.array();
    assume(blobs.length).equals(1);
    assume(blobs[0] instanceof BlockBlob).is.ok();

    debug(`load content of blob: ${blobName}`);
    let data = await blob.load();
    assume(data.value).equals(40);
  });

  it('try create a data blob with wrong data', async() => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await container.createDataBlob({
      name: blobName,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    debug(`upload content to blob: ${blobName}`);
    let createError;
    try {
      await blob.create({
        value: 'wrong value',
      });
    } catch (error) {
      createError = error;
    }

    assume(createError).exists();
    assume(createError.code).equals('BlobDoesNotConformToSchema');
  });

  it('should create, update a data block blob', async () => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await container.createDataBlob({
      name: blobName,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    debug(`create the blob in azure storage: ${blobName}`);
    let result = await blob.create({
      value: 24,
    });

    debug(`update the content of the blob: ${blobName}`);
    let modifier = (json) => {
      json.value = 40;
      return json;
    };
    await blob.update({}, modifier);

    debug('test the content updated');
    let content = await blob.load();
    assume(content.value).equals(40);
  });

  it('try update a data blob with wrong data', async() => {
    let blobName = `${blobNamePrefix}${uuid.v4()}`;
    debug(`create a blob with name: ${blobName}`);
    let blob = await container.createDataBlob({
      name: blobName,
    });
    assume(blob instanceof DataBlockBlob).is.ok();

    debug(`upload content to blob: ${blobName}`);
    await blob.create({
      value: 24,
    });

    debug(`update the content of the blob: ${blobName}`);
    let updateError;
    let modifier = (json) => {
      json.value = 'wrong value';
      return json;
    };
    try {
      await blob.update({}, modifier);
    } catch (error) {
      updateError = error;
    }
    assume(updateError).exists();
    assume(updateError.code).equals('BlobDoesNotConformToSchema');
  });
});