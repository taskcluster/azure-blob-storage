import assert from 'assert';
import _debug from 'debug';
const debug = _debug('azure-blob-storage:container');
import {rethrowDebug} from './utils';
import {Blob, BlockBlob, AppendBlob, DataBlockBlob} from './blob';

class Container {

  constructor(options) {
    options = options || {};
    assert(typeof options.name === 'string', 'The name of the container, `options.name`, must be specified.');

    this.blobService = options.blobService;
    this.name = options.name;
    this.eTag = options.eTag;
    this.schemaId = options.schemaId;
    this.metadata = options.metadata || {};
    this.validate = options.validate;
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
   * }
   * ```
   * @returns {DataBlockBlob} an instance of DataBlockBlob
   */
  createDataBlob(options) {
    options = options || {};
    options.container = this;

    return new DataBlockBlob(options);
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
   * @returns {BlockBlob} an instance of BlockBlob
   */
  createBlockBlob(options) {
    options = options || {};
    options.container = this;

    return new BlockBlob(options);
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
