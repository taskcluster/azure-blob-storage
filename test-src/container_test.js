let subject = require('../lib/container');
let assume = require('assume');
let uuid = require('uuid');
let config = require('typed-env-config');
let debug = require('debug')('test:container');

describe('Azure Blob Container', () => {
  let containerName = uuid.v4();
  let accountId;
  let accessKey;
  let container;

  before(async () => {
    // Load configuration
    let cfg = config({});
    accountId = cfg.azureBlob.accountId;
    accessKey = cfg.azureBlob.accessKey;
    assume(accountId).is.ok();
    assume(accessKey).is.ok();
  });

  after(async () => {
  });

  it('should create, list and delete a container', async () => {
    let name = uuid.v4();
    debug('name: ' + name);

    let account = new subject.Account({
      accountId,
      accessKey,
    });

    debug('ensuring container absence');
    let list = await account.listContainers({
      prefix: name,
    });
    assume(list).is.array();
    assume(list.length).equals(0);

    debug('creating container');
    await account.createContainer({
      name
    });

    debug('listing container');
    let list2 = await account.listContainers({
      prefix: name,
    });
    assume(list2).is.array();
    assume(list2.length).equals(1);
    assume(list2[0] instanceof subject.Container).is.ok();

    debug('deleting container');
    await account.deleteContainer({
      name,
    });
    
    debug('ensuring container absence');
    let list3 = await account.listContainers({
      prefix: name,
    });
    assume(list3).is.array();
    assume(list3.length).equals(0);
  });
});
