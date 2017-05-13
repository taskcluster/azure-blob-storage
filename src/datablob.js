import assert from 'assert';
import _debug from 'debug';
import _      from 'lodash';
const debug = _debug('azure-blob-storage:blob');
import {CongestionError, SchemaValidationError, BlobSerializationError} from './customerrors';
import {rethrowDebug, sleep, computeDelay} from './utils';

/**
 * Base class for data blobs
 */
class DataBlob {

  /**
   * @param options - Options on the form
   * ```js
   * {
   *    name: '...',                  // The name of the blob (required)
   *    type: 'BlockBlob|AppendBlob', // The type of the blob (required)
   *    container: '...',             // An instance of DataContainer (required)
   *    contentEncoding: '...',       // The content encoding of the blob
   *    contentLanguage: '...',       // The content language of the blob
   *    cacheControl: '...',          // The cache control of the blob
   *    contentDisposition: '...',    // The content disposition of the blob
   *    cacheContent: true|false,     // This can be set true in order to keep a reference of the blob content.
   *                                  // Default value is false
   * }
   * ```
   */
  constructor(options) {
    options = options || {};
    assert(options, 'options must be specified.');
    assert(options.container, 'The container instance, `options.container`, must be specified.');
    assert(typeof options.name === 'string', 'The name of the blob, `options.name` must be specified.');

    this.container = options.container;
    this.blobService = this.container.blobService;
    this.cacheContent = options.cacheContent || false;

    this.name = options.name;
    this.type = options.type;
    this.contentType = 'application/json';
    this.contentLanguage = options.contentLanguage;
    this.contentDisposition = options.contentDisposition;
    this.cacheControl = options.cacheControl;
  }

  async _validateJSON(content) {
    let result = await this.container.validate(content, this.version ? this.version:this.container.schemaVersion);
    if (!result.valid) {
      debug(`Failed to validate the blob content against schema with id: 
          ${this.container.schema.id}, errors: ${result.errors}`);
      let error = new SchemaValidationError(`Failed to validate the blob content against schema with id: 
                                            ${this.container.schema.id}`);
      error.content = content;
      error.validationErrors = result.errors;
      throw error;
    }
  }

  _cache(content) {
    this.content = this.cacheContent ? content : undefined;
  }

  /**
   * Creates the blob in Azure storage
   *
   * @param content - content of the blob
   */
  async _create(content) {
    let blobOptions = {
      type: this.type,
      contentType: this.contentType,
      contentLanguage: this.contentLanguage,
      contentDisposition: this.contentDisposition,
      cacheControl: this.cacheControl,
    };
    try {
      let result = await this.blobService.putBlob(this.container.name, this.name, blobOptions, content);
      this.eTag = result.eTag;
      this._cache(content);
    } catch (error) {
      rethrowDebug(`Failed to create the blob "${this.name}" with error: ${error}`, error);
    }
  }

  /**
   * Remove this blob if the content was not modified, unless `ignoreChanges` is set
   */
  async remove(ignoreChanges, ignoreIfNotExists) {
    let options = {};
    if (!ignoreChanges) {
      options.ifMatch = this.eTag;
    }
    try {
      await this.blobService.deleteBlob(this.container.name, this.name, options);
    } catch (error) {
      if (ignoreIfNotExists && error && error.code === 'BlobNotFound') {
        return;
      }
      rethrowDebug(`Failed to remove the blob '${this.name}'` +
        ` from container "${this.container.name}" with error: ${error}`, error);
    }
  }
}

/**
 * An instance of DataBlockBlob is a reference to an azure block blob which contains a JSON file.
 * The content of the DataBlockBlob is validated against the schema stored at the container level.
 */
class DataBlockBlob extends DataBlob {

  constructor(options) {
    options.type = 'BlockBlob';
    super(options);
  }

  _serialize(json) {
    try {
      return JSON.stringify({
        content: json,
        version: this.version ? this.version : this.container.schemaVersion,
      });
    } catch (error) {
      debug(`Failed to serialize the content of the blob: ${this.name} with error: ${error}, ${error.stack}`);
      throw new BlobSerializationError(`Failed to serialize the content of the blob: ${this.name}`);
    }
  }

  /**
   * Load the content of this blob.
   */
  async load() {
    // load the content only if the eTag of our local data doesn't match the copy on the server
    let options = {};
    if (this.cacheContent) {
      options.ifNoneMatch = this.eTag;
    }
    try {
      let blob = await this.blobService.getBlob(this.container.name, this.name, options);

      // update the properties
      this.eTag = blob.eTag;
      this.contentType = blob.contentType;
      this.contentLanguage = blob.contentLanguage;
      this.contentDisposition = blob.contentDisposition;
      this.cacheControl = blob.cacheControl;

      let deserializedContent = JSON.parse(blob.content);
      let content = deserializedContent.content;
      this.version = deserializedContent.version;
      // Validate the JSON against the schema
      await this._validateJSON(content);
      this._cache(content);
      return content;
    } catch (error) {
      if (error && error.statusCode === 304) {
        return this.content; // our local data match with server data
      }
      rethrowDebug(`Failed to load the blob "${this.name}" with error: ${error}`, error);
    }
  }

  /**
   * The method creates this blob on azure blob storage.
   * The blob can be created without content. The content can be uploaded later using `modify` method.
   *
   * @param content - a JSON object
   */
  async create(content) {
    assert(content, 'content must be specified');

    // 1. Validate the content against the schema
    await this._validateJSON(content);

    // 2. store the blob
    await super._create(this._serialize(content));

    // 3. cache the raw content and not the serialized one
    this._cache(content);
  }

  /**
   * Update the content of the stored JSON.
   *
   * The JSON has the following structure:
   * ```js
   * {
   *    content: '...',
   *    version: 1
   * }
   * ```
   * @param options - Options on the form:
   * ```js
   * {
   *    contentLanguage: '...',
   *    contentDisposition: '...',
   *    cacheControl: '...'
   * }
   * ```
   * @param modifier - function that is called to update the content
   */
  async modify(modifier, options) {
    options = options || {};
    options.type = this.type;
    assert(modifier instanceof Function, 'The `modifier` must be a function.');

    // Attempt to modify this object
    let attemptsLeft = this.container.updateRetries;

    let modifiedContent;
    let attemptModify = async () => {
      try {
        // 1. load the resource
        let content = await this.load();

        // 2. run the modifier function
        let clonedContent = _.cloneDeep(content);
        modifier(clonedContent);
        modifiedContent = clonedContent;

        // 3. validate against the schema
        await this._validateJSON(clonedContent);

        // 4. update the resource
        options.ifMatch = this.eTag;

        let result = await this.blobService.putBlob(this.container.name,
                                                    this.name,
                                                    options,
                                                    this._serialize(modifiedContent));
        this.eTag = result.eTag;
      } catch (error) {
        // rethrow error, if it's not caused by optimistic concurrency
        if (!error || error.code !== 'ConditionNotMet') {
          rethrowDebug(`Failed to update blob "${this.name}" with error: ${error}`, error);
        }

        // Decrement number of attempts left
        attemptsLeft -= 1;
        if (attemptsLeft === 0) {
          debug('ERROR: the maximum number of retries exhausted, we might have congestion');
          throw new CongestionError('the maximum number of retries exhausted, check for congestion');
        }

        await sleep(computeDelay(attemptsLeft,
                                this.container.updateDelayFactor,
                                this.container.updateRandomizationFactor,
                                this.container.updateMaxDelay));
        await attemptModify();
      }
    };
    await attemptModify();
    // cache the raw content and not the one which is versioned
    this._cache(modifiedContent);
  }
}

/**
 * An instance of AppendDataBlob is a reference to an azure append blob.
 * Each appended object must be in JSON format and must match the schema defined at the container level.
 * Updating and deleting existing content is not supported.
 */
class AppendDataBlob extends DataBlob {

  constructor(options) {
    options.type = 'AppendBlob';
    super(options);
    this.cacheContent = false;
  }

  _serialize(content) {
    try {
      return JSON.stringify(content);
    } catch (error) {
      debug(`Failed to serialize the content of the blob: ${this.name} with error: ${error}, ${error.stack}`);
      throw new BlobSerializationError(`Failed to serialize the content of the blob: ${this.name}`);
    }
  }

  /**
   * Append content that should be conform to container schema
   *
   * @param content - the content that should be appended
   */
  async append(content) {
    // 1. validate the content against the schema
    await this._validateJSON(content);

    // 2. append the new content
    try {
      await this.blobService.appendBlock(this.container.name, this.name, {}, this._serialize(content));
    } catch (error) {
      rethrowDebug(`Failed to append content for blob '${this.name}' with error: ${error}`, error);
    }
  }

  /**
   * Load the content of this append blob.
   */
  async load() {
    try {
      let blob = await this.blobService.getBlob(this.container.name, this.name);

      // update the properties
      this.eTag = blob.eTag;
      this.contentType = blob.contentType;
      this.contentLanguage = blob.contentLanguage;
      this.contentDisposition = blob.contentDisposition;
      this.cacheControl = blob.cacheControl;

      return blob.content;
    } catch (error) {
      rethrowDebug(`Failed to load the blob "${this.name}" with error: ${error}`, error);
    }
  }

  /**
   * Creates the blob in Azure storage
   */
  async create() {
    await this._create();
  }
}

module.exports = {
  DataBlockBlob,
  AppendDataBlob,
};
