import assert from 'assert';
import azure from 'fast-azure-storage';
import {rethrowDebug} from './utils';
import _debug from 'debug';
const debug = _debug('azure-blob-storage:account');
import Container from './container';

class Account {

  constructor(options) {
    options = options || {};
    assert(typeof options.accountId === 'string', 'The `options.accountId` must be specified and must be a string.');
    if (options.accessKey) {
      assert(typeof options.accessKey === 'string', 'If specified, the `options.accessKey` must be a string.');
    }
    this.accountId = options.accountId;
    this.accessKey = options.accessKey || undefined;
    this.blobsvc = new azure.Blob({
      accountId: options.accountId,
      accessKey: options.accessKey,
    });
  }

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

        marker = result.marker || undefined;
        result.containers.forEach(container => {
          containers.push(new Container({
            accountId: this.accountId,
            name: container.name,
            metadata: container.metadata,
          }));
        });
      } while (marker);
    } catch (error) {
      rethrowDebug(`Failed to list containers with error: ${error}`);
    }

    return containers;
  }

  async loadContainer(name, options) {
    try {
      let props = await this.blobsvc.getContainerProperties(name, options);

      return new Container({
        name: name,
        blobServiceAccount: this.blobsvc,
        metadata: props.metadata,
        schema: props.metadata? props.metadata.schema : '',
      });
    } catch (error) {
      rethrowDebug(`Failed to load container "${name}" with error: ${error}`);
    }
  }

  // The schema is optional ?!
  async createContainer(name, options) {
    options = options || {};
    if (options.schema) {
      assert(typeof options.schema === 'string', 'The `options.schema` must be a string.');
      options.metadata = options.metadata || {};
      options.metadata.schema = options.schema;
    }

    try {
      await this.blobsvc.createContainer(name, options);
      return new Container({
        blobServiceAccount: this.blobsvc,
        name: name,
        metadata: options.metadata,
        publicAccessLevel: options.publicAccessLevel,
      });
    } catch (error) {
      rethrowDebug(`Failed to create container "${this.name}" with error: ${error} ${error.stack}`);
    }
  }

  async deleteContainer(options, ignoreIfNotExists) {
    options = options || {};
    try {
      await this.blobsvc.deleteContainer(options.name);
      return true;
    } catch (error) {
      if (!ignoreIfNotExists || !error || error.code !== 'ContainerNotFound') {
        rethrowDebug(`Failed to delete container "${this.name}" with error: ${error}`);
      }
      return false;
    }
  }
}

export default Account;
