const _ = require('lodash');
const assert = require('assert');
const azure = require('fast-azure-storage');
const constants = require('./constants');
const Ajv = require('ajv');
const {rethrowDebug} = require('./utils');
const _debug = require('debug');
const debug = _debug('azure-blob-storage:account');
const {DataBlockBlob, AppendDataBlob} = require('./datablob');
const {SchemaIntegrityCheckError} = require('./customerrors');

/**
 * This class represents an Azure Blob Storage container which stores objects in JSON format.
 * All the objects will be validated against the schema that is provided at the creation time of the container.
 */
class DataContainer {

  /**
   * Options:
   * ```js
   * {
   *   containerName:     'AzureContainerName',   // Azure container name
   *   credentials:                               // See README
   *     clientId:        '...',                  // TaskCluster clientId
   *     accessToken:     '...',                  // TaskCluster accessToken
   *   },
   *   schema:            '...',                  // JSON schema object
   *   schemaVersion:     1,                      // JSON schema version. (optional)
   *                                              // The default value is 1.
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
   */
  constructor(options) {
    // validate the options
    assert(options, 'options must be given');
    assert(!options.account, 'options.account is no longer allowed');
    assert(!options.container, 'options.container is now options.containerName');
    assert(!options.authBaseUrl, 'options.authBaseUrl is no longer allowed');
    assert(!options.credentials.clientId, 'Taskcluster credentials are no longer allowed');
    assert(options.containerName, 'options.containerName must be given');
    assert(typeof options.containerName === 'string', 'options.containerName is not a string');
    assert(options.schema, 'options.schema must be given');
    assert(typeof options.schema === 'object', 'options.schema is not an object');

    if (options.schemaVersion) {
      assert(typeof options.schemaVersion === 'number', 'options.schemaVersion is not a number');
    }

    // create an Azure Blob Storage client
    const blobService = new azure.Blob(_.defaults({
      timeout:          constants.AZURE_BLOB_TIMEOUT,
    }, options.credentials));

    this.blobService = blobService;
    this.name        = options.containerName;
    // _validateFunctionMap is a mapping from schema version to validation function generated
    // after the ajv schema compile
    this._validateFunctionMap = {};

    this.schema      = options.schema;
    this.schemaVersion = options.schemaVersion? options.schemaVersion : 1;
    this.schema.$id = this._getSchemaId(this.schemaVersion);

    this.validator   = Ajv({
      useDefaults: true,
      format: 'full',
      verbose: true,
      allErrors: true,
    });
    this.validator.addMetaSchema(require('ajv/lib/refs/json-schema-draft-06.json'));

    this.updateRetries = options.updateRetries || 10;
    this.updateDelayFactor = options.updateDelayFactor || 100;
    this.updateRandomizationFactor = options.updateRandomizationFactor || 0.25;
    this.updateMaxDelay = options.updateMaxDelay || 30 * 1000;
  }

  /**
   * @param schemaVersion - the schema version
   * @returns {string} - the id of the schema
   * @private
   */
  _getSchemaId(schemaVersion) {
    return `http://${this.blobService.options.accountId}.blob.core.windows.net/` +
      `${this.name}/.schema.v${schemaVersion}.json`;
  }

  /**
   * @param schemaVersion - the schema version
   * @returns {string} - the name of the schema
   * @private
   */
  _getSchemaName(schemaVersion) {
    return `.schema.v${schemaVersion}`;
  }

  /**
   * Saves the JSON schema in a BlockBlob.
   * This method will throw an 'AuthorizationPermissionMismatch', if the client has read-only rights
   * for the data container.
   *
   * @private
   */
  async _saveSchema() {
    try {
      let schemaName = this._getSchemaName(this.schemaVersion);
      await this.blobService.putBlob(this.name, schemaName, {type: 'BlockBlob'}, JSON.stringify(this.schema));
    } catch (error) {
      // Ignore the 'AuthorizationPermissionMismatch' error that will be throw if the client has read-only rights.
      // The save of the schema can be done only by the clients with read-write access.
      if (error.code !== 'AuthorizationPermissionMismatch') {
        rethrowDebug(`Failed to save the json schema '${this.schema.$id}' with error: ${error}`, error);
      }
    }
  }

  /**
   * If the schema was previously saved, this method will make an integrity check, otherwise will save the schema in
   * a blockBlob.
   * @private
   */
  async _cacheSchema() {
    let storedSchema;
    let schemaName = this._getSchemaName(this.schemaVersion);
    try {
      let schemaBlob = await this.blobService.getBlob(this.name, schemaName);
      storedSchema = schemaBlob.content;
    } catch (error) {
      if (error.code === 'BlobNotFound') {
        await this._saveSchema();
        return;
      }
      rethrowDebug(`Failed to load the json schema '${this.schema.$id}' with error: ${error}`, error);
    }

    // integrity check
    if (storedSchema !== JSON.stringify(this.schema)) {
      throw new SchemaIntegrityCheckError('The stored schema is not the same with the schema defined.');
    }
  }

  /**
   * Method that validates the content
   *
   * @param content - JSON content
   * @param schemaVersion - the schema version (optional)
   *
   * @return {object}
   * ```js
   * {
   *    valid: boolean,   // true/false if the content is valid or not
   *    errors: [],       // if the content is invalid, errors will contain an array of validation errors
   * }
   * ```
   */
  async validate(content, schemaVersion = this.schemaVersion) {
    let ajvValidate = this._validateFunctionMap[schemaVersion];
    // if the validate function is not available, this means that the schema is not yet loaded
    if (!ajvValidate) {
      if (schemaVersion === this.schemaVersion) {
        this._validateFunctionMap[this.schemaVersion] = this.validator.compile(this.schema);
      } else {
        // load the schema
        try {
          let schemaBlob = await this.blobService.getBlob(this.name, this._getSchemaName(schemaVersion));
          let schema = JSON.parse(schemaBlob.content);
          // upgrade to a v6 schema
          if (schema.id && !schema.$id) {
            schema.$id = schema.id;
            delete schema.id;
          }
          // cache the ajv validate function
          this._validateFunctionMap[schemaVersion] = this.validator.compile(schema);
        } catch (error) {
          rethrowDebug(`Failed to save the json schema '${this.schema.$id}' with error: ${error}`, error);
        }
      }
    }
    ajvValidate = this._validateFunctionMap[schemaVersion];
    let result = {
      valid: ajvValidate(content),
      errors: ajvValidate.errors,
    };

    return result;
  }

  async init() {
    // ensure the existence of the data container
    await this.ensureContainer();

    // cache the JSON schema
    await this._cacheSchema();
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
        // the list can't contain the blobs that store the JSON schema
        if (blob.type === 'BlockBlob' && !/.schema.v*/i.test(blob.name)) {
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
   * Execute the provided function on each data block blob from this container while handling pagination.
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
              // we need to take extra care for the blobs that contain the schema information.
              // the handle can't be applied on blobs that store the JSON schema
              if (!/.schema.v*/i.test(dataBlob.name)) {
                // 2. execute the handler function
                await handler(dataBlob);
              }
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
   * @param content - the content, in JSON format, that should be appended(optional)
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
   * It makes sense to set the cacheContent to true only for DataBlockBlob, because AppendDataBlob blobs do not keep
   * the content in their instance.
   *
   * @param blobName - the name of the blob
   * @param cacheContent - true in order to cache the content
   */
  async load(blobName, cacheContent) {
    assert(blobName, 'The name of the blob must be specified.');

    let properties;
    try {
      // find the type of the blob
      properties = await this.blobService.getBlobProperties(this.name, blobName);
    } catch (error) {
      /**
       * For getBlobProperties, if the blob does not exist, Azure does not send a proper BlobNotFound error.
       * Azure sends a response with statusCode: 404, statusMessage: 'The specified blob does not exists.' and
       * without any payload. Because of this, the error received here will look like this:
       *
       *  { ErrorWithoutCodeError: No error message given, in payload ''
       *     name: 'ErrorWithoutCodeError',
       *     code: 'ErrorWithoutCode',
       *     statusCode: 404,
       *     retries: 0 }
       * Probably in the future, Azure will correct the response, but, till then we will override the name and code.
       */
      if (error.statusCode === 404 && error.name === 'ErrorWithoutCodeError') {
        error.code = 'BlobNotFound';
        error.name = 'BlobNotFoundError';
        error.message = 'The specified blob does not exist.';
      }
      rethrowDebug(`Failed to load the blob '${blobName}' from container "${this.name}" with error: ${error}`, error);
    }

    let blob;
    let options = {
      name: blobName,
      container: this,
      cacheContent: cacheContent,
    };
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
  }

  /**
   * Removes a blob from Azure storage without loading it.
   * Returns true, if the blob was deleted. It makes sense to read the return value only if `ignoreIfNotExists` is set
   * to value true.
   *
   * @param blob
   * @param ignoreIfNotExists - true in order to ignore the error that is thrown in case the blob does not exist
   */
  async remove(blob, ignoreIfNotExists) {
    assert(blob, 'The blob name must be specified.');

    try {
      await this.blobService.deleteBlob(this.name, blob);
      return true;
    } catch (error) {
      if (ignoreIfNotExists && error && error.code === 'BlobNotFound') {
        return false;
      }
      rethrowDebug(`Failed to remove the blob '${blob}' from container "${this.name}" with error: ${error}`, error);
    }
  }
}

module.exports.DataContainer = DataContainer;
