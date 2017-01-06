import assert from 'assert';
import azure from 'fast-azure-storage';

class Account {
  constructor(opts) {
    assert(typeof opts.accountId === 'string');
    if (opts.accessKey) {
      assert(typeof opts.accessKey === 'string');
    }
    this.accountId = opts.accountId;
    this.accessKey = opts.accessKey || undefined;
    this.blobsvc = new azure.Blob({
      accountId: opts.accountId, 
      accessKey: opts.accessKey,
    });
  }
    
  async listContainers(opts) {
    if (!opts) {
      opts = {};
    }
    if (opts.prefix) {
      assert(typeof opts.prefix === 'string');
    }
    let containers = [];
    let marker;
    do {
      let result = await this.blobsvc.listContainers({
        prefix: opts.prefix || undefined,
        marker: marker,
      });

      marker = result.marker || undefined;
      result.containers.forEach(container => {
        //containers.push(container);
        containers.push(new Container());
      });
    } while (marker);
    return containers;
  }

  async createContainer(opts) {
    return this.blobsvc.createContainer(opts.name);
  }

  async deleteContainer(opts) {
    return this.blobsvc.deleteContainer(opts.name);
  }

  setProperties() { }
    
  getProperties() { }
}

class Container {

}

class Blob {

}

module.exports = {
  Account,
  Container,
  Blob,
};
