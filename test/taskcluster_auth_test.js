const API = require('taskcluster-lib-api');
const testing = require('taskcluster-lib-testing');
const validator = require('taskcluster-lib-validate');
const app = require('taskcluster-lib-app');
const azure = require('fast-azure-storage');
const {DataContainer} = require('../lib');
const assume = require('assume');
const {schema, credentials} = require('./helpers');
const uuid = require('uuid');
const path = require('path');

suite('Data Container - Tests for authentication with SAS from auth.taskcluster.net', () => {
  var callCount = 0;
  var returnExpiredSAS = false;
  // Create test api
  let api = new API({
    title:        'Test TC-Auth',
    description:  'Another test api',
  });

  api.declare({
    method:     'get',
    route:      '/azure/:account/containers/:container/:level',
    name:       'azureBlobSAS',
    deferAuth:  true,
    scopes:     [['auth:azure-blob:<level>:<account>/<container>']],
    title:        'Test SAS End-Point',
    description:  'Get SAS for testing',
  }, async function(req, res) {
    callCount += 1;
    let account = req.params.account;
    let container = req.params.container;
    let level = req.params.level;

    if (!(level === 'read-only' &&
      req.satisfies({account, container, level: 'read-write'}, true)) &&
      !req.satisfies({account, container, level})) {
      return;
    }

    let blobService = new azure.Blob({
      accountId:  credentials.accountName,
      accessKey:  credentials.accountKey,
    });

    // Create container ignore error, if it already exists
    if (level === 'read-write') {
      try {
        await blobService.createContainer(container);
      } catch (err) {
        if (err.code !== 'ContainerAlreadyExists') {
          throw err;
        }
      }
    }

    var expiry = new Date(Date.now() + 25 * 60 * 1000);
    // Return and old expiry, this causes a refresh on the next call
    if (returnExpiredSAS) {
      expiry = new Date(Date.now() + 15 * 60 * 1000 + 100);
    }

    let perm = level === 'read-write';

    let sas = blobService.sas(container, null, {
      start:         new Date(Date.now() - 15 * 60 * 1000),
      expiry:        expiry,
      resourceType: 'container',
      permissions: {
        read: true,
        add: perm,
        create: perm,
        write: perm,
        delete: perm,
        list: true,
      },
    });

    res.status(200).json({
      expiry:   expiry.toJSON(),
      sas:      sas,
    });
  });

  // Create servers
  let server;
  let dataContainer;
  let containerName = `container-test${uuid.v4()}`;

  suiteSetup(async () => {
    testing.fakeauth.start({
      'authed-client': ['*'],
      'read-only-client': [`auth:azure-blob:read-only:${credentials.accountName}/${containerName}`],
      'unauthed-client': ['*'],
    });

    let myvalidator = await validator({
      folder: path.join(__dirname, 'schemas_auth'),
    });

    // Create a simple app
    let myapp = app({
      port:       1208,
      env:        'development',
      forceSSL:   false,
      trustProxy: false,
    });

    // Create router for the API
    let router =  api.router({
      validator: myvalidator,
    });

    // Mount router
    myapp.use(router);

    server = await myapp.createServer();
  });

  suiteTeardown(async () => {
    await server.terminate();
    testing.fakeauth.stop();
    // delete the container created
  });

  test('should create an instance of data container', async () => {
    dataContainer = new DataContainer({
      account: credentials.accountName,
      container: containerName,
      credentials: {
        clientId: 'authed-client',
        accessToken: 'test-token',
      },
      authBaseUrl: 'http://localhost:1208',
      schema: schema,
    });
    await dataContainer.init();
    assume(dataContainer).exists('Expected a data container instance.');
  });

  test('should create an instance of data container with read-only access and try to create a blob', async () => {
    let readOnlyDataContainer = new DataContainer({
      account: credentials.accountName,
      container: containerName,
      credentials: {
        clientId: 'read-only-client',
        accessToken: 'test-token',
      },
      accessLevel: 'read-only',
      authBaseUrl: 'http://localhost:1208',
      schema: schema,
    });
    readOnlyDataContainer.init();
    assume(readOnlyDataContainer).exists('Expected a data container instance.');

    try {
      await readOnlyDataContainer.createDataBlockBlob({
        name: 'blob',
      }, {value: 20});
    } catch (error) {
      assume(error.code).equals('AuthorizationPermissionMismatch');
      return;
    }
    assume(false).is.true('It should have thrown an error because the client does not have `read-write` access.');
  });

  test('should create a data block blob', async () => {
    callCount = 0;
    await dataContainer.createDataBlockBlob({
      name: 'blobTest',
    }, {
      value: 50,
    });

    // the auth won't be called because it was already called in order to cache the schema
    assume(callCount).equals(0);
  });

  test('should call for every operation, expiry < now => refreshed SAS', async () => {
    callCount = 0;
    returnExpiredSAS = true;  // This means we call for each operation
    try {
      dataContainer = new DataContainer({
        account: credentials.accountName,
        container: containerName,
        credentials: {
          clientId: 'authed-client',
          accessToken: 'test-token',
        },
        authBaseUrl: 'http://localhost:1208',
        schema: schema,
      });
      await dataContainer.init();
      let blob = await dataContainer.createDataBlockBlob({
        name: 'blobTest',
      }, {
        value: 50,
      });

      assume(callCount).equals(2, 'azureBlobSAS should have been called twice.');

      await testing.sleep(200);
      let content = await blob.load();

      assume(callCount).equals(3, 'azureBlobSAS should have been called three times.');
    } catch (error) {
      assume(false).is.true('Expected no error.');
    }
  });
});
