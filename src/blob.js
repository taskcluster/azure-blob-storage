import assert from 'assert';
import _debug from 'debug';
const debug = _debug('azure-blob-storage:blob');
import {CongestionError, SchemaValidationError, BlobSerializationError} from './customerrors';
import {rethrowDebug, sleep, computeDelay} from './utils';

const MAX_MODIFY_ATTEMPTS = 10;

class Blob {

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
    this.contentType = options.contentType;
    this.contentLanguage = options.contentLanguage;
    this.contentDisposition = options.contentDisposition;
    this.cacheControl = options.cacheControl;
  }
}

class BlockBlob extends Blob {

  constructor(options) {
    options.type = 'BlockBlob';
    super(options);
  }

  async create(content) {
    let blobOptions = {
      blobType: this.type,
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
   * Load the content of the blob
   */
  async load() {
    // load the content only if the eTag of our local data doesn't match the copy on the server
    let options = {};
    let blob;
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

      return blob.content;
    } catch (error) {
      if (error && error.statusCode === 304) {
        return this.content;
      }
      rethrowDebug(`Failed to load the blob "${this.name}" with error: ${error}`, error);
    }
  }

  async update(options, content) {
    options = options || {};
    options.blobType = this.type;

    try {
      let result = await this.blobService.putBlob(this.container.name, this.name, options, content);
      this.eTag = result.eTag;
      this._cache(content);
    } catch (error) {
      rethrowDebug(`Failed to update the blob "${this.name}" with error: ${error}`, error);
    }
  }
}

/**
 * An instance of DataBlockBlob is a reference to a azure blob which contains a JSON.
 * The content of the DataBlockBlob is validated against the schema stored at the container level.
 */
class DataBlockBlob extends BlockBlob {

  constructor(options) {
    options.contentType = 'application/json';
    super(options);
  }

  _validateJSON(content) {
    if (content && this.container.schemaId) {
      let valid = this.container.validate(content);
      if (!valid) {
        debug(`Failed to validate the blob content against schema with id: 
            ${this.container.schemaId}, errors: ${this.container.validate.errors}`);
        let error = new SchemaValidationError(`Failed to validate the blob content against schema with id: 
                                              ${this.container.schemaId}`);
        error.content = content;
        error.validationErrors = this.container.validate.errors;
        throw error;
      }
    }
  }

  _serialize(json) {
    try {
      return JSON.stringify({
        content: json,
        version: 1,
      });
    } catch (error) {
      debug(`Failed to serialize the content of the blob: ${this.name} with error: ${error}, ${error.stack}`);
      throw new BlobSerializationError(`Failed to serialize the content of the blob: ${this.name}`);
    }
  }

  _cache(content) {
    this.content = this.cacheContent ? content : undefined;
  }

  /**
   * Load the content of this blob.
   */
  async load() {
    let data = await super.load();
    // validate only if it is necessary, only if the content has been changed
    if (data !== this.content) {
      let content = JSON.parse(data).content;
      // Validate the JSON against the schema
      this._validateJSON(content);
      return content;
    }

    return this.content;
  }

  /**
   * The method creates this blob on azure blob storage.
   * The blob can be created with an empty content. The content can be uploaded later using `update` method.
   *
   * @param content - a JSON object
   */
  async create(content) {

    // 1. Validate the content against the schema
    this._validateJSON(content);

    // 2. store the blob
    await super.create(this._serialize(content));

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
  async update(options, modifier) {
    options = options || {};
    assert(modifier instanceof Function, 'The `modifier` must be a function.');

    // Attempt to modify this object
    let attemptsLeft = MAX_MODIFY_ATTEMPTS;

    let attemptModify = async () => {
      try {
        // 1. load the resource
        let content = await this.load();

        // 2. run the modifier function
        let modifiedContent = modifier(content);

        // 3. validate against the schema
        this._validateJSON(modifiedContent);

        // 4. update the resource
        options.ifMatch = this.eTag;
        super.update(options, this._serialize(modifiedContent));
        // cache the raw content and not the one
        this._cache(modifiedContent);
      } catch (error) {
        // rethrow error, if it's not caused by optimistic concurrency
        if (!error || error.code !== 'ConditionNotMet') {
          rethrowDebug(`Failed to update blob "${this.name}" with error: ${error}`, error);
        }

        // Decrement number of attempts left
        attemptsLeft -= 1;
        if (attemptsLeft === 0) {
          debug('ERROR: MAX_MODIFY_ATTEMPTS exhausted, we might have congestion');
          throw new CongestionError('MAX_MODIFY_ATTEMPTS exhausted, check for congestion');
        }

        await sleep(computeDelay(attemptsLeft,
                                this.blobService.options.delayFactor,
                                this.blobService.options.randomizationFactor,
                                this.blobService.options.maxDelay));
        return attemptModify();
      }
    };
    await attemptModify();
  }
}

// TODO AppendBlob and AppendDataBlob is in progress
class AppendBlob extends Blob {

  constructor(options) {
    options.type = 'AppendBlob';
    super(options);
  }

  async create(options) {
    super.create(options);
  }
}

class AppendDataBlob extends AppendBlob {

  constructor(options) {
    super(options);
  }

  async append(options, content) {
    // 1. validate the content against the schema
    // 2. append the new content
    await this.blobService.appendBlock(this.container.name, this.name, options, content);
  }
}

module.exports = {
  Blob,
  BlockBlob,
  AppendBlob,
  DataBlockBlob,
  AppendDataBlob,
};
