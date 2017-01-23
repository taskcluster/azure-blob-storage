import Account from '../lib/account';
import Container from '../lib/container';
import assume from 'assume';
import uuid from 'uuid';
import config from 'typed-env-config';
import _debug from 'debug';
const debug = _debug('test:container');

describe('Azure Blob Container', () => {
  let containerName = uuid.v4();
  let accountId;
  let accessKey;
  let container;
  let account;

  before(async () => {
    // Load configuration
    let cfg = config({});
    accountId = cfg.azureBlob.accountId;
    accessKey = cfg.azureBlob.accessKey;
    assume(accountId).is.ok();
    assume(accessKey).is.ok();
    account = new Account({
      accountId,
      accessKey,
    });
  });

  after(async () => {
  });

  it('should create, list and delete a container', async () => {
    let name = uuid.v4();
    debug('name: ' + name);

    debug('ensuring container absence');
    let list = await account.listContainers({
      prefix: name,
    });
    assume(list).is.array();
    assume(list.length).equals(0);

    debug('creating container');
    let cont = await account.createContainer(name);
    assume(cont instanceof Container).is.ok();

    debug('listing container');
    let list2 = await account.listContainers({
      prefix: name,
    });
    debug(list2);
    assume(list2).is.array();
    assume(list2.length).equals(1);
    assume(list2[0] instanceof Container).is.ok();

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
