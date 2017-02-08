import assert from 'assert';
import _debug from 'debug';
const debug = _debug('azure-blob-storage:container');
import {rethrowDebug} from './utils';
import {Blob, BlockBlob, AppendBlob, DataBlockBlob} from './blob';

/**
 * The name of the blob where the JSON schema is stored
 * @const
 */
const JSON_SCHEMA_BLOB_NAME = '.schema.blob.json';

class Container {

  constructor(options) {
    options = options || {};
    assert(typeof options.name === 'string', 'The name of the container, `options.name`, must be specified.');

    this.blobService = options.blobService;
    this.name = options.name;
    this.metadata = options.metadata || {};
    this.validator = options.validator;
  }

  _getSchemaId() {
    return `http://${this.blobService.options.accountId}.blob.core.windows.net/${this.name}/${JSON_SCHEMA_BLOB_NAME}#`;
  }

  async _validate(schema) {
    let validate;
    let schemaId = this._getSchemaId(this.name);
    try {
      let schemaValidation = this.validator.getSchema(schemaId);
      if (schemaValidation) {
        validate = schemaValidation;
      } else if (schema) {
        if (typeof schema !== 'object') {
          throw new Error('If specified, the `options.schema` must be a JSON schema object.');
        }
        schema.id = schemaId;

        // the compile method also checks if the JSON schema is a valid one
        validate = this.validator.compile(schema);
      } else {
        /**
         * A container can have an associated JSON schema which is store in a blob with name '.schema.blob.json'.
         */
        let data = await this.blobService.getBlob(this.name, JSON_SCHEMA_BLOB_NAME);
        let schemaObj = JSON.parse(data).content;
        schemaObj.id = schemaId;
        validate = this.validator.compile(schemaObj);
      }
    } catch (error) {
      if (!error || error.code !== 'BlobNotFound') {
        rethrowDebug(`Failed to load schema with id "${schemaId}" with error: ${error}`, error);
      }
    }
    return validate;
  }

  /**
   * This method creates this container in azure blob storage.
   * @param options - Options on the form:
   * ```js
   * {
   *    schema: object,                     // The schema object
   * }
   * ```
   */
  async create(options) {
    options = options || {};

    try {
      let createResult = await this.blobService.createContainer(this.name, {
        metadata: this.metadata,
        publicAccessLevel: this.publicAccessLevel,
      });

      this.eTag = createResult.eTag;

      // store the associated schema if it is specified
      if (options.schema) {
        // the schema will be stored in a blob named '.schema.blob.json'
        let blobOptions = {
          contentType: 'application/json',
          blobType: 'BlockBlob',
        };
        let payload = JSON.stringify({
          content: options.schema,
          version: 1,
        });
        let result = await this.blobService.putBlob(this.name,
          JSON_SCHEMA_BLOB_NAME,
          blobOptions,
          payload);
        // get the schema validation function
        this.validate = await this._validate(options.schema);
        this.schemaId = options.schema.id;
      }
    } catch (error) {
      rethrowDebug(`Failed to create container "${this.name}" with error: ${error}`, error);
    }
  }

  async load() {
    try {
      let properties = await this.blobService.getContainerProperties(this.name);
      this.eTag = properties.eTag;
      this.metadata = properties.metadata;
      // get the schema validation function if the container has an associated schema
      this.validate = await this._validate();
      this.schemaId = this.validate ? this._getSchemaId(this.name) : undefined;
    } catch (error) {
      if (!error || error.code !== 'BlobNotFound') {
        rethrowDebug(`Failed to load container "${this.name}" with error: ${error}`, error);
      }
    }
  }

  async updateMetadata(metadata) {
    metadata = metadata || {};

    try {
      await this.blobService.setContainerMetadata(this.name, metadata);
      this.metadata = metadata;
    } catch (error) {
      rethrowDebug(`Failed to update metadata for container "${this.name}" with error: ${error}`, error);
    }
  }

  /**
   * Thie method returns a paginated list of blobs
   *
   * @param options
   * {
   *    prefix: '...',
   *    delimiter: '...',
   *    marker: '...',
   *    maxResults: 5000,             // The maximum number of blobs to return (optional)
   *    include: {                    // Specifies one or more datasets to include in the result
   *        metadata: false,          // Include blob metadata in listing
   *        uncommittedBlobs: false,  // Include uncommitted blobs in listing
   *    }
   * }
   * @returns
   * {
   *    blobs: [],
   *    nextMarker: '...', // Next marker if not at end of list
   * }
   */
  async listBlobs(options) {
    let blobs = [];

    let marker;
    try {
      let result = await this.blobService.listBlobs(this.name, options);

      blobs = result.blobs.map(blob => {
        if (blob.blobType === 'BlockBlob') {
          return new BlockBlob({
            container: this,
            name: blob.name,
          });
        } else if (blob.blobType === 'AppendBlob') {
          return new AppendBlob({
            container: this,
            name: blob.name,
          });
        } else {
          // PageBlob - not implemented
          return new Blob({
            container: this,
            type: blob.blobType,
            name: blob.name,
          });
        }
      });

      return {
        blobs: blobs || [],
        nextMarker: result.nextMarker,
      };
    } catch (error) {
      rethrowDebug(`Failed to list blobs for container "${this.name}" with error: ${error}`, error);
    }
  }

  /**
   * This method returns an instance of DataBlockBlob.
   * Note that this method will not create and upload the content of the on azure blob storage.
   * In order to do this, the create() method should be called on the newly created instance of the DataBlockBlob
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
   * @returns {DataBlockBlob} an instance of DataBlockBlob
   */
  async createDataBlob(options, content) {
    options = options || {};
    options.container = this;

    let blob = new DataBlockBlob(options);
    await blob.create(content);

    return blob;
  }

  /**
   * This method returns an instance of BlockBlob.
   * Note that this method will not create and upload the content of the blob on azure blob storage.
   * In order to do this, the create() method should be called on the newly created instance of the BlockBlob
   *
   * @param options - Options on the form
   * ```js
   * {
   *    name: '...',                // The name of the blob
   *    metadata: '...',            // Name-value pairs associated with the blob as metadata
   *    contentType: '...',         // The content type of the blob
   *    contentEncoding: '...',     // The content encoding of the blob
   *    contentLanguage: '...',     // The content language of the blob
   *    cacheControl: '...',        // The cache control of the blob
   *    contentDisposition: '...',  // The content disposition of the blob
   * }
   * ```
   * @param content - content of the blob
   * @returns {BlockBlob} an instance of BlockBlob
   */
  async createBlockBlob(options, content) {
    options = options || {};
    options.container = this;

    let blob = new BlockBlob(options);
    await blob.create(content);

    return blob;
  }

  /**
   * Remove a blob by reference to the Blob object or string that's the blob's name in the Azure namespace
   * @param blob
   */
  async remove(blob) {
    assert(blob, '`blob` must be specified.');
    let blobName;
    if (typeof blob === 'string') {
      blobName = blob;
    } else if (typeof blob === 'object' && blob instanceof Blob) {
      blobName = blob.name;
    }

    try {
      await this.blobService.delete(this.container, blobName);
    } catch (error) {
      rethrowDebug(`Failed to remove the blob '${blobName}' from container "${this.name}" with error: ${error}`, error);
    }
  }
}

export default Container;
