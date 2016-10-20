let subject = require('../lib/container');
let assume = require('assume');
let uuid = require('uuid');

describe('Azure Blob Container', () => {
  let containerName = uuid.v4();
  let accountName = process.env.AZURE_ACCOUNT_NAME;
  let accountKey = process.env.AZURE_ACCOUNT_KEY;
  let container;

  before(async () => {
    container = await subject(accountName, accountKey, containerName);
  });

  after(async () => {
    await container.removeContainer();
  });

  it('should be able to create, read, update and delete a blob', async () => {
    let blobName = uuid.v4();
    let expected = {
      a: uuid.v4(),
    };

    await container.write(blobName, expected);

    let readValue = await container.read(blobName);

    assume(readValue).deeply.equals(expected);

    container.remove(blobName);
  });

  it('should allow overwriting', async () => {
    let blobName = uuid.v4();
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