import base           from 'taskcluster-base';
import azure          from 'fast-azure-storage';
import DataContainer  from '../lib/DataContainer';
import assume         from 'assume';
import path           from 'path';
import {schema, credentials}       from './helpers';

describe('Data Container - Tests for authentication with SAS from auth.taskcluster.net', () => {
  var callCount = 0;
  var returnExpiredSAS = false;
  // Create test api
  let api = new base.API({
    title:        'Test TC-Auth',
    description:  'Another test api',
  });
  api.declare({
    method:     'get',
    route:      '/azure/:account/blob/:container/read-write',
    name:       'azureBlobSAS',
    deferAuth:  true,
    scopes:     [['auth:azure-blob-access:<account>/<container>']],
    title:        'Test SAS End-Point',
    description:  'Get SAS for testing',
  }, function(req, res) {
    callCount += 1;
    let account = req.params.account;
    let container = req.params.container;
    if (!req.satisfies({account: account, container: container})) {
      return;
    }

    let blobService = new azure.Blob({
      accountId:  credentials.accountName,
      accessKey:  credentials.accountKey,
    });
    var expiry = new Date(Date.now() + 25 * 60 * 1000);
    // Return and old expiry, this causes a refresh on the next call
    if (returnExpiredSAS) {
      expiry = new Date(Date.now() + 15 * 60 * 1000 + 100);
    }

    let sas = blobService.sas(container, null, {
      start:         new Date(Date.now() - 15 * 60 * 1000),
      expiry:        expiry,
      resourceType: 'container',
      permissions: {
        read: true,
        add: true,
        create: true,
        write: true,
        delete: true,
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
  let containerName = 'container-test';

  before(async () => {
    base.testing.fakeauth.start({
      'authed-client': ['*'],
      'unauthed-client': ['*'],
    });

    let validator = await base.validator({
      folder: path.join(__dirname, 'schemas_auth'),
    });

    // Create a simple app
    let app = base.app({
      port:       1208,
      env:        'development',
      forceSSL:   false,
      trustProxy: false,
    });

    // Create router for the API
    let router =  api.router({
      validator: validator,
    });

    // Mount router
    app.use(router);

    server = await app.createServer();
  });

  after(async () => {
    await server.terminate();
    base.testing.fakeauth.stop();
  });

  it('should create an instance of data container', async () => {
    dataContainer = await DataContainer({
      account: credentials.accountName,
      container: containerName,
      credentials: {
        clientId: 'authed-client',
        accessToken: 'test-token',
      },
      authBaseUrl: 'http://localhost:1208',
      schema: schema,
    });
    assume(dataContainer).exists('Expected a data container instance.');
  });

  it('should create a data block blob', async () => {
    callCount = 0;
    await dataContainer.createDataBlockBlob({
      name: 'blobTest',
    }, {
      value: 50,
    });

    assume(callCount).equals(1);
  });

  it('should call for every operation, expiry < now => refreshed SAS', async () => {
    callCount = 0;
    returnExpiredSAS = true;  // This means we call for each operation
    dataContainer = await DataContainer({
      account: credentials.accountName,
      container: containerName,
      credentials: {
        clientId: 'authed-client',
        accessToken: 'test-token',
      },
      authBaseUrl: 'http://localhost:1208',
      schema: schema,
    });
    let blob = await dataContainer.createDataBlockBlob({
      name: 'blobTest',
    }, {
      value: 50,
    });

    assume(callCount).equals(1, 'azureBlobSAS should have been called once.');

    await base.testing.sleep(200);
    let content = await blob.load();

    assume(callCount).equals(2, 'azureBlobSAS should have been called twice.');
  });

  it('create two data block blobs in parallel, only gets SAS once', async () => {
    dataContainer = await DataContainer({
      account: credentials.accountName,
      container: containerName,
      credentials: {
        clientId: 'authed-client',
        accessToken: 'test-token',
      },
      authBaseUrl: 'http://localhost:1208',
      schema: schema,
    });
    callCount = 0;
    await Promise.all([
      dataContainer.createDataBlockBlob({
        name: 'blobTest2',
      }, {
        value: 50,
      }),
      dataContainer.createDataBlockBlob({
        name: 'blobTest3',
      }, {
        value: 50,
      }),
    ]);

    assume(callCount).equals(1);
  });
});
