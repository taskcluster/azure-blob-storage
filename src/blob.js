import assert from 'assert';
import _debug from 'debug';
const debug = _debug('azure-blob-storage:blob');
import {CongestionError} from './customerrors';
import {rethrowDebug} from './utils';

const MAX_MODIFY_ATTEMPTS = 10;

class Blob {

  constructor(name, options) {
    options = options || {};
    this.container = options.container;
    this.blobServiceAccount = options.blobSeviceAccount;
    this.name = name;
    this.type = options.type;
  }

  async create(options, content) {
    let blobOptions = {
      blobType: this.type,
    };
    await this.blobServiceAccount.putBlob(this.container.name, this.name, blobOptions, content);
  }
}

class DataBlob extends Blob {

  constructor(options) {
    options.type = 'BlockBlob';
    super(options);
  }

  async create(options, content) {
    // 1. Validate the content against the schema
    // 2. store the blob
    super.create(options, content);
  }

  async update(options, modifier) {
    assert(modifier instanceof Function, 'The `mutator` must be a function.');

    // Attempt to modify this object
    let attemptsLeft = MAX_MODIFY_ATTEMPTS;

    let attemptModify = async () => {
      try {
        // 1. load the resource
        let blob = await this.blobServiceAccount.getBlob(this.container.name, this.name, options);

        // 2. run the modifier function
        let modifiedContent = modifier(blob.content);

        // 3. validate against the schema
        // 4. update the resource
        // TODO include in options also the blob properties
        const options = {
          ifMatch: blob.eTag,
        };
        await this.blobServiceAccount.putBlob(this.container.name, this.name, blob.properties, options, modifiedContent);

      } catch (error) {
        // rethrow error, if it's not caused by optimistic concurrency
        if (!error || error.code !== 'ConditionNotMet') {
          rethrowDebug(`Failed to update blob "${this.name}" with error: ${error}, ${error.stack}`);
        }

        // Decrement number of attempts left
        attemptsLeft -= 1;
        if (attemptsLeft === 0) {
          debug('ERROR: MAX_MODIFY_ATTEMPTS exhausted, we might have congestion');
          throw new CongestionError('MAX_MODIFY_ATTEMPTS exhausted, check for congestion');
        }
        return attemptModify();
      }
    };

    attemptModify();
  }
}

class AppendDataBlob extends Blob {

  constructor(options) {
    options.type = 'AppendData';
    super(options);
  }

  async create(options, content) {
    super.create(options);
  }

  async append(options, content) {
    // 1. validate the content against the schema
    // 2. append the new content
    this.blobServiceAccount.appendBlock(this.container.name, this.name, options, content);
  }
}

module.exports = {
  Blob,
  DataBlob,
  AppendDataBlob,
};
