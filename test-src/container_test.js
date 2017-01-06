let subject = require('../lib/container');
let assume = require('assume');
let uuid = require('uuid');
let config = require('typed-env-config');
let debug = require('debug')('test:container');

describe('Azure Blob Container', () => {
  let containerName = uuid.v4();
  let accountName;
  let accountKey;
  let container;

  before(async () => {
    // Load configuration
    let cfg = config({});
    accountName = cfg.azureBlob.accountName;
    accountKey = cfg.azureBlob.accountKey;
    container = await subject(accountName, accountKey, containerName);
    assume(accountName).is.ok();
    assume(accountKey).is.ok();
  });

  after(async () => {
    await container.removeContainer();
  });

  it('should be able to create, read, update and delete a blob', async () => {
    let blobName = uuid.v4();
    debug('blob name: ' + blobName);
    let expected = {
      a: uuid.v4(),
    };

    await container.write(blobName, expected);

    let readValue = await container.read(blobName);

    assume(readValue).deeply.equals(expected);

    container.remove(blobName);
  });

  it('should be able to list blobs', async () => {
    let blobName = uuid.v4();
    let data = {
      a: uuid.v4(),
    };

    await container.write(blobName, data);
    let blobName2 = uuid.v4();
    await container.write(blobName2, data);
    let blobName3 = uuid.v4();
    await container.write(blobName3, data);

    let listResult = await container.listBlobs();
    console.dir(listResult);
    assume(listResult.entries).is.size(3);
    assume(listResult.continuationToken).equals(null);
  });

  it('should allow overwriting', async () => {
    let blobName = uuid.v4();
    debug('blob name: ' + blobName);
    let expected = {
      a: uuid.v4(),
    };
    await container.write(blobName, expected);
    expected.a = uuid.v4();
    await container.write(blobName, expected);
    let readValue = await container.read(blobName);
    assume(readValue).deeply.equals(expected);
    container.remove(blobName);
  });

  it('should cause error when reading missing blob', async done => {
    try {
      await container.read(uuid.v4());
      done(new Error('shouldnt reach here'));
    } catch (err) {
      assume(err.code).equals('BlobNotFound');
      done();
    }
  });

  it('should not fail to delete an absent blob', async () => {
    await container.remove(uuid.v4());
  });
});
