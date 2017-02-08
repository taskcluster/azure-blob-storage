import BlobStorage from '../lib/blobstorage';
import Container from '../lib/container';
import assume from 'assume';
import uuid from 'uuid';
import config from 'typed-env-config';
import _debug from 'debug';
const debug = _debug('test:container');

describe('Azure Blob Storage - Container', () => {
  let accountId;
  let accessKey;
  let blobStorage;
  const containerNamePrefix = 'test';

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
  });

  after(async () => {
    // delete all containers
    let containers = await blobStorage.listContainers({
      prefix: containerNamePrefix,
    });

    await Promise.all(containers.map((container) => {
      return blobStorage.deleteContainer({
        name: container.name,
      });
    }));
  });

  it('should create, list and delete a container', async () => {
    let name = `${containerNamePrefix}${uuid.v4()}`;
    debug(`name: ${name}`);

    debug('ensuring container absence');
    let list = await blobStorage.listContainers({
      prefix: name,
    });
    assume(list).is.array();
    assume(list.length).equals(0);

    debug('creating container');
    let container = await blobStorage.createContainer({
      name,
    });
    assume(container instanceof Container).is.ok();

    debug('listing container');
    let list2 = await blobStorage.listContainers({
      prefix: name,
    });
    assume(list2).is.array();
    assume(list2.length).equals(1);
    assume(list2[0] instanceof Container).is.ok();

    debug('deleting container');
    await blobStorage.deleteContainer({
      name,
    });
    
    debug('ensuring container absence');
    let list3 = await blobStorage.listContainers({
      prefix: name,
    });
    assume(list3).is.array();
    assume(list3.length).equals(0);
  });

  it('should create, load and delete a container which has an associated schema', async () => {
    let name = `${containerNamePrefix}${uuid.v4()}`;
    debug(`container name: ${name}`);

    debug('creating container with an associated schema');
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
    let newContainer = await blobStorage.createContainer({
      name: name,
      schema: schemaObj,
    });
    let accountId = blobStorage.blobsvc.options.accountId;
    let schemaId = `http://${accountId}.blob.core.windows.net/${name}/.schema.blob.json#`;
    assume(newContainer instanceof Container).is.ok();
    assume(newContainer.schemaId).equals(schemaId);

    debug(`load the container: ${name}`);
    let container = await blobStorage.loadContainer({
      name,
    });
    assume(container instanceof Container).is.ok();
    assume(container.schemaId).equals(schemaId);
  });
});
