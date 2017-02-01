import assert from 'assert';
import azure from 'fast-azure-storage';
import {rethrowDebug} from './utils';
import _debug from 'debug';
const debug = _debug('azure-blob-storage:account');
import Container from './container';
import Ajv from 'ajv';

/**
 * Base class for azure blob storage
 */
class BlobStorage {

  /**
   * In case you have the Azure credentials, the options are:
   * ```js
   * {
   *    // Azure credentials
   *    credentials: {
   *      accountName: '...',       // Azure account name
   *      accessKey: '...',         // Azure account key
   *    }
   *    // Max number of request retries
   *   retries:              5,
   *   // Multiplier for computation of retry delay: 2 ^ retry * delayFactor
   *   delayFactor:          100,
   *
   *   // Randomization factor added as:
   *   // delay = delay * random([1 - randomizationFactor; 1 + randomizationFactor])
   *   randomizationFactor:  0.25,
   *
   *   // Maximum retry delay in ms (defaults to 30 seconds)
   *   maxDelay:             30 * 1000,
   * }
   * ```
   *
   * In case you use SAS, the options are:
   * TODO
   */
  constructor(options) {
    assert(options, 'options must be specified.');
    assert(options.credentials, '`options.credentials` must be specified.');
    assert(typeof options.credentials.accountId === 'string',
      'The `options.accountId` must be specified and must be a string.');
    if (options.credentials.accessKey) {
      assert(typeof options.credentials.accessKey === 'string',
        'If specified, the `options.accessKey` must be a string.');
    }
    this.accountId = options.credentials.accountId;
    this.accessKey = options.credentials.accessKey || undefined;
    this.blobsvc = new azure.Blob({
      accountId: this.accountId,
      accessKey: this.accessKey,
      retries: options.retries,
      delayFactor: options.delayFactor,
      randomizationFactor: options.randomizationFactor,
      maxDelay: options.maxDelay,
    });
    this.validator = Ajv({useDefaults: true, format: 'full', verbose: true, allErrors: true});
  }

  /**
   * List all the containers under the blob storage account.
   * Note that this method handles also the pagination.
   *
   * @param {object} options - Options on the form
   * ```js
   * {
   *    prefix: '...'     // Prefix of containers to list (optional)
   * }
   * ```
   * @returns {Array} containers - An array of Container instances
   */
  async listContainers(options) {
    options = options || {};

    let containers = [];
    let marker;
    try {
      do {
        let result = await this.blobsvc.listContainers({
          prefix: options.prefix || undefined,
          marker: marker,
        });

        marker = result.nextMarker || undefined;
        result.containers.forEach(container => {
          containers.push(new Container({
            blobService: this.blobsvc,
            name: container.name,
            metadata: container.metadata,
            validator: this.validator,
          }));
        });
      } while (marker);
    } catch (error) {
      rethrowDebug(`Failed to list containers with error: ${error}`, error);
    }

    return containers;
  }

  /**
   * Load the information of a container
   *
   * @param options - Options on the form:
   * ```js
   * {
   *    name: '...',    // Name of the container (mandatory)
   * }
   * ```
   * @returns {Container} an instance of Container
   */
  async loadContainer(options) {
    assert(options, '`options` must be specified.');
    assert(typeof options.name === 'string', 'The name of the container, `options.name`, must be specified.');

    let container = new Container({
      name: options.name,
      blobService: this.blobsvc,
      validator: this.validator,
    });
    await container.load();

    return container;
  }

  /**
   * Create a new container.
   * Note that if the JSON schema will be specified, all the blobs uploaded in the container must be in JSON format and
   * will be validate against the provided schema. Otherwise, you can create all types of blobs.
   *
   * @param options - Options on the form:
   * ```js
   * {
   *    name: '...',                        // Name of the container (mandatory)
   *    schema: object,                     // The schema object
   *    metadata: '...',                    // Mapping from metadata keys to values.
   *    publicAccessLevel: container|blob   // Specifies whether data in the container may be accessed publicly
    *                                       // and the level of access.
   * }
   * ```
   * @returns {Container} an instance of Container
   */
  async createContainer(options) {
    assert(options, '`options` must be specified.');
    assert(options.name, 'The name of the container, `options.name` must be specified.');

    // create a new instance of Container
    let container = new Container({
      blobService: this.blobsvc,
      name: options.name,
      metadata: options.metadata,
      publicAccessLevel: options.publicAccessLevel,
      validator: this.validator,
    });

    // create the container in azure blob storage
    await container.create({
      schema: options.schema,
    });

    return container;
  }

  /**
   * Delete a container.
   *
   * @param options
   * @param ignoreIfNotExists - true to ignore the error that is thrown in case the container does not exists
   * @returns {boolean} - true if the container was deleted
   */
  async deleteContainer(options, ignoreIfNotExists) {
    options = options || {};
    try {
      await this.blobsvc.deleteContainer(options.name);
      return true;
    } catch (error) {
      if (!ignoreIfNotExists || !error || error.code !== 'ContainerNotFound') {
        rethrowDebug(`Failed to delete container "${this.name}" with error: ${error}`, error);
      }
      return false;
    }
  }
}

export default BlobStorage;
