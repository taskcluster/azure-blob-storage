import base           from 'taskcluster-base';
import azure          from 'fast-azure-storage';
import DataContainer  from '../lib/DataContainer';
import assume         from 'assume';
import path           from 'path';
import {schema, credentials}       from './helpers';

describe('Data Container - Tests for authentication with SAS from auth.taskcluster.net', () => {

  var callCount = 0;
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

    let credentials = {
      accountId: cfg.azureBlob.accountId,
      accessKey: cfg.azureBlob.accessKey,
    };

    let blobService = azure.Blob({
      accountId:  credentials.accountName,
      accessKey:  credentials.accountKey,
    });
    let expiry = new Date(Date.now() + 25 * 60 * 1000);

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
      // prefix: 'test/v1',
    });

    // Create a simple app
    let app = base.app({
      port:       23244,
      env:        'development',
      forceSSL:   false,
      trustProxy: false,
    });

    // Create router for the API
    var router =  api.router({
      validator: validator,
    });

    // Mount router
    app.use('/v1', router);

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
      authBaseUrl: 'http://localhost:23244',
      schema: schema,
    });
    assume(dataContainer).exists('Expected a data container instance.');
  });

  // TODO is not working - will be fixed
  it('should create a data block blob', async () => {
    callCount = 0;
    await dataContainer.createDataBlockBlob({
      name: 'blobTest',
    }, {
      value: 50,
    });

    assume(callCount).equals(1);
  });
});
