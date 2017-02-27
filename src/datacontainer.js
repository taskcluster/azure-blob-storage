import assert           from 'assert';
import azure            from 'fast-azure-storage';
import taskcluster      from 'taskcluster-client';
import constants        from './constants';
import Ajv              from 'ajv';
import {rethrowDebug}   from './utils';
import _debug           from 'debug';
const debug = _debug('azure-blob-storage:account');
import {DataBlockBlob, AppendDataBlob} from './datablob';

/**
 * This class represents an Azure Blob Storage container which stores objects in JSON format.
 * All the objects will be validated against the schema that is provided at the creation time of the container.
 */
class DataContainer {

  /**
   * Options:
   * ```js
   * {
   *   // Azure connection details for use with SAS from auth.taskcluster.net
   *   account:           '...',                  // Azure storage account name
   *   container:         'AzureContainerName',   // Azure container name
   *   // TaskCluster credentials
   *   credentials: {
   *     clientId:        '...',                  // TaskCluster clientId
   *     accessToken:     '...',                  // TaskCluster accessToken
   *   },
   *   accessLevel:       'read-write',           // The access level of the container: read-only/read-write (optional)
   *   authBaseUrl:       '...',                  // baseUrl for auth (optional)
   *   schema:            '...',                  // JSON schema object
   *
   *   // Max number of update blob request retries
   *   updateRetries:              10,
   *   // Multiplier for computation of retry delay: 2 ^ retry * delayFactor
   *   updateDelayFactor:          100,
   *
   *   // Randomization factor added as:
   *   // delay = delay * random([1 - randomizationFactor; 1 + randomizationFactor])
   *   updateRandomizationFactor:  0.25,
   *
   *   // Maximum retry delay in ms (defaults to 30 seconds)
   *   updateMaxDelay:             30 * 1000,
   * }
   * ```
   * Using the `options` format provided above a shared-access-signature will be
   * fetched from auth.taskcluster.net. The goal with this is to reduce secret
   * configuration and reduce exposure of our Azure `accountKey`. To fetch the
   * shared-access-signature the following scope is required:
   *   `auth:azure-blob:<level>:<account>/<container>`
   *
   * In case you have the Azure credentials, the options are:
   * ```js
   * {
   *    // Azure credentials
   *    credentials: {
   *      accountName: '...',         // Azure account name
   *      accountKey: '...',          // Azure account key
   *    }
   * }
   * ```
   */
  constructor(options) {
    // validate the options
    assert(options,                                 'options must be given');
    assert(options.container,                       'options.container must be given');
    assert(typeof options.container === 'string',   'options.container is not a string');
    assert(options.schema,                          'options.schema must be given');
    assert(typeof options.schema === 'object',      'options.schema is not an object');

    // create an Azure Blob Storage client
    let blobService;
    if (options.account) {
      assert(typeof options.account === 'string',
        'Expected `options.account` to be a string, or undefined.');

      // Create auth client to fetch SAS from auth.taskcluster.net
      let auth = new taskcluster.Auth({
        credentials:    options.credentials,
        baseUrl:        options.authBaseUrl,
      });

      // Create azure blob storage client with logic for fetch SAS
      blobService = new azure.Blob({
        timeout:          constants.AZURE_BLOB_TIMEOUT,
        accountId:        options.account,
        minSASAuthExpiry: 15 * 60 * 1000,
        sas: async () => {
          let level = options.accessLevel || 'read-write';
          let result = await auth.azureBlobSAS(options.account, options.container, level);
          return result.sas;
        },
      });
    } else {
      assert(options.credentials.accountName,
        'The `options.credentials.accountName` must be supplied.');
      assert(options.credentials.accountKey || options.credentials.sas,
        'The `options.credentials.accountKey` or `options.credentials.sas` must be supplied.');

      // Create azure blob storage client with accessKey
      blobService = new azure.Blob({
        timeout:    constants.AZURE_BLOB_TIMEOUT,
        accountId:  options.credentials.accountName,
        accessKey:  options.credentials.accountKey,
        sas:        options.credentials.sas,
      });
    }

    this.blobService = blobService;
    this.name        = options.container;
    this.schema      = options.schema;
    this.validator   = Ajv({
      useDefaults: true,
      format: 'full',
      verbose: true,
      allErrors: true,
    });
    // get the schema validation function
    this.schema.id = this._getSchemaId();
    // the compile method also checks if the JSON schema is a valid one
    this.validate = this.validator.compile(this.schema);

    this.updateRetries = options.updateRetries || 10;
    this.updateDelayFactor = options.updateDelayFactor || 100;
    this.updateRandomizationFactor = options.updateRandomizationFactor || 0.25;
    this.updateMaxDelay = options.updateMaxDelay || 30 * 1000;
  }

  _getSchemaId() {
    return `http://${this.blobService.options.accountId}.blob.core.windows.net/` +
      `${this.name}/${constants.JSON_SCHEMA_NAME}`;
  }

  /**
   * Ensure existence of the underlying container
   *
   * Note that this doesn't work, if authenticated with SAS.
   */
  async ensureContainer() {
    // Auth creates the container for us, so we don't do it again
    // The request will actually fail because Auth doesn't give
    // us permissions for creating containers.
    if (this.blobService.options && this.blobService.options.sas) {
      return;
    }

    try {
      await this.blobService.createContainer(this.name);
    } catch (error) {
      if (!error || error.code !== 'ContainerAlreadyExists') {
        rethrowDebug(`Failed to ensure container '${this.name}' with error: ${error}`, error);
      }
    }
  }

  /**
   * Delete the underlying container
   *
   * Note that this doesn't work, if authenticated with SAS.
   */
  async removeContainer() {
    try {
      await this.blobService.deleteContainer(this.name);
    } catch (error) {
      rethrowDebug(`Failed to delete container "${this.name}" with error: ${error}`, error);
    }
  }

  /**
   * Returns a paginated list of blobs contained by this container
   *
   * @param options
   * {
   *    prefix: '...',                // Prefix of blobs to list (optional)
   *    continuation: '...',          // Continuation token to continue from (optional)
   *    maxResults: 5000,             // The maximum number of blobs to return (optional)
   * }
   * @returns
   * {
   *    blobs: [],                    // An array of blob instances
   *    continuationToken: '...',     // Next token if not at end of list
   * }
   */
  async listBlobs(options) {
    options = options || {};
    let blobs = [];

    try {
      let result = await this.blobService.listBlobs(this.name, {
        prefix: options.prefix,
        marker: options.continuation,
        maxResults: options.maxResults,
      });

      blobs = result.blobs.map(blob => {
        let options = {
          container: this,
          name: blob.name,
          contentLanguage: blob.contentLanguage,
          contentDisposition: blob.contentDisposition,
          cacheControl: blob.cacheControl,
        };
        if (blob.type === 'BlockBlob') {
          return new DataBlockBlob(options);
        } else if (blob.type === 'AppendBlob') {
          return new AppendDataBlob(options);
        } else {
          // PageBlobs are not supported
        }
      });

      return {
        blobs: blobs || [],
        continuationToken: result.nextMarker,
      };
    } catch (error) {
      rethrowDebug(`Failed to list blobs for container "${this.name}" with error: ${error}`, error);
    }
  }

  /**
   * Execute the provided function on each Blob in this container while handling pagination.
   *
   * @param {function} handler
   * ```js
   *   function(blob) {
   *      return new Promise(...); // Do something with the blob
   *   }
   * ```
   * @param {object} options - Options on the form
   * ```js
   *    {
   *      prefix: '...',      // Prefix of blobs to list (optional)
   *      limit:  1000,       // Max number of parallel handler calls
   *    }
   * ```
   */
  async scanDataBlockBlob(handler, options) {
    assert(typeof handler === 'function', 'handler must be a function');
    options = options || {};

    try {
      let marker;
      do {
        let result = await this.blobService.listBlobs(this.name, {
          prefix: options.prefix,
          marker: marker,
          maxResults: options.limit,
        });

        await Promise.all(result.blobs.map(
          async (blob) => {
            if (blob.type === 'BlockBlob') {
              // 1. create an instance of DataBlockBlob from the result blob
              let dataBlob = new DataBlockBlob({
                name: blob.name,
                container: this,
                contentLanguage: blob.contentLanguage,
                contentDisposition: blob.contentDisposition,
                cacheControl: blob.cacheControl,
              });
              // 2. execute the handler function
              await handler(dataBlob);
            }
          }));

        marker = result.nextMarker || undefined;
      } while (marker);
    } catch (error) {
      rethrowDebug(`Failed to execute the handler with error: ${error}`, error);
    }
  }

  /**
   * Returns an instance of DataBlockBlob.
   * By using this instance of blob, a JSON file can be stored in azure storage.
   * The content will be validated against the schema defined at the container level.
   *
   * @param options - Options on the form
   * ```js
   * {
   *    name: '...',                // The name of the blob (required)
   *    metadata: '...',            // Name-value pairs associated with the blob as metadata
   *    contentEncoding: '...',     // The content encoding of the blob
   *    contentLanguage: '...',     // The content language of the blob
   *    cacheControl: '...',        // The cache control of the blob
   *    contentDisposition: '...',  // The content disposition of the blob
   *    cacheContent: true|false,   // This can be set true in order to keep a reference of the blob content.
   *                                // Default value is false
   * }
   * ```
   * @param content - content in JSON format of the blob
   * @returns {DataBlockBlob} an instance of DataBlockBlob
   */
  async createDataBlockBlob(options, content) {
    assert(content, 'The content of the blob must be provided.');
    options = options || {};
    options.container = this;

    let blob = new DataBlockBlob(options);
    await blob.create(content);

    return blob;
  }

  /**
   * Create an instance of AppendDataBlob.
   * This type is optimized for fast append operations and all writes happen at the end of the blob.
   * Each object appended must be in JSON format and must match the schema defined at container level.
   * Updating and deleting existing content is not supported.
   *
   * @param options - Options on the form
   * ```js
   * {
   *    name: '...',                // The name of the blob (required)
   *    metadata: '...',            // Name-value pairs associated with the blob as metadata
   *    contentEncoding: '...',     // The content encoding of the blob
   *    contentLanguage: '...',     // The content language of the blob
   *    cacheControl: '...',        // The cache control of the blob
   *    contentDisposition: '...',  // The content disposition of the blob
   * }
   * ```
   * @param content - the content, in JSON format, that should be appended
   */
  async createAppendDataBlob(options, content) {
    options = options || {};
    options.container = this;

    let blob = new AppendDataBlob(options);
    await blob.create(); // for append blobs, the create method should be called without content

    if (content) {
      await blob.append();
    }
    return blob;
  }

  /**
   * Returns an instance of DataBlockBlob or AppendDataBlob.
   * It makes sense to set the cacheContent to true only for DataBlockBlob, because AppendDataBlob blobs does not keep
   * the content in their instance.
   *
   * @param blobName - the name of the blob
   * @param cacheContent - true in order to cache the content
   */
  async load(blobName, cacheContent) {
    assert(blobName, 'The name of the blob must be specified.');

    try {
      let blob;
      let options = {
        name: blobName,
        container: this,
        cacheContent: cacheContent,
      };
      // find the type of the blob
      let properties = await this.blobService.getBlobProperties(this.name, blobName);
      if (properties.type === 'BlockBlob') {
        blob = new DataBlockBlob(options);
      } else if (properties.type === 'AppendBlob') {
        return new AppendDataBlob(options);
      } else {
        // PageBlob is not supported
        return null;
      }

      await blob.load();
      return blob;
    } catch (error) {
      rethrowDebug(`Failed to load the blob '${blob}' from container "${this.name}" with error: ${error}`, error);
    }
  }

  /**
   * Remove a blob
   * @param blob
   * @param ignoreIfNotExists - true in order to ignore the error that is thrown in case the blob does not exist
   */
  async remove(blob, ignoreIfNotExists) {
    assert(blob, 'The blob name must be specified.');

    try {
      await this.blobService.deleteBlob(this.name, blob);
    } catch (error) {
      if (ignoreIfNotExists && error && error.code === 'BlobNotFound') {
        return;
      }
      rethrowDebug(`Failed to remove the blob '${blob}' from container "${this.name}" with error: ${error}`, error);
    }
  }
}

async function DataContainerFactory(options) {
  let dataContainer = new DataContainer(options);
  await dataContainer.ensureContainer();
  return dataContainer;
}

export default DataContainerFactory;