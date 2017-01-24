import assert from 'assert';
import _debug from 'debug';
const debug = _debug('azure-blob-storage:container');
import {rethrowDebug} from './utils';
import {Blob, DataBlockBlob} from './blob';

class Container {

  constructor(options) {
    options = options || {};
    assert(typeof options.name === 'string', 'The name of the container, `options.name`, must be specified.');
    // schema url
    // assert(typeof options.schema === 'string', 'The json schema, `options.schema`, must be specified');

    this.blobSeviceAccount = options.blobServiceAccount;
    this.name = options.name;
    this.schema = options.schema;
    this.metadata = options.metadata || {};
  }

  async updateMetadata(metadata) {
    metadata = metadata || {};

    try {
      await this.blobSeviceAccount.setContainerMetadata(this.name, metadata);
      this.metadata = metadata;
    } catch (error) {
      rethrowDebug(`Failed to update metadata for container "${this.name}" with error: ${error}`);
    }
  }

  async listBlobs(options) {
    options = options || {};

    let blobs = [];
    let marker;
    try {
      do {
        let result = await this.blobsvc.listContainers({
          prefix: options.prefix || undefined,
          marker: marker,
        });

        marker = result.marker || undefined;
        result.blobs.forEach(blob => {
          blob.push(new Blob({
            container: this,
            blobServiceAccount: this.blobSeviceAccount,
            type: blob.blobType,
            name: blob.name,
          }));
        });
      } while (marker);
    } catch (error) {
      rethrowDebug(`Failed to list blobs for container "${this.name}" with error: ${error}`);
    }

    return blobs;
  }

  async removeBlob(name, options, ignoreIfNotExists) {
    options = options || {};
    try {
      await this.blobsvc.deleteBlob(this.name, name, options);
      return true;
    } catch (error) {
      if (!ignoreIfNotExists || !error || error.code !== 'ContainerNotFound') {
        rethrowDebug(`Failed to delete blob "${this.name}" with error: ${error}, ${error.stack}`);
      }
      return false;
    }
  }

  async createDataBlob(options, content) {
    options = options || {};
    options.contentType = 'text/plain; charset="utf-8"';

    try {
      let result = await this.blobSeviceAccount.putBlob(options, content);
      let dataBlob = new DataBlockBlob(options);
      dataBlob.eTag = result.eTag;

      return dataBlob;
    } catch (error) {
      rethrowDebug(`Failed to create the data blob "${this.name}" with error: ${error}, ${error.stack}`);
    }
  }

  async scan() {
  }

  async setPermissions(permissions) {
    await this.blobSeviceAccount.setContainerAcl(permissions);
  }

  async getPermissions(permissions) {
    await this.blobSeviceAccount.getContainerAcl(permissions);
  }

  async removePermissions() {}
}

export default Container;
