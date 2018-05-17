[![Build Status](https://travis-ci.org/elenasolomon/azure-blob-storage.svg?branch=master)](https://travis-ci.org/elenasolomon/azure-blob-storage)

## azure-blob-storage

This library wraps an Azure Blob Storage container which stores objects in JSON format.

### Creating an instance of DataContainer

DataContainer is a wrapper over Azure Blob Storage container which stores only objects in JSON format.
All the objects that will be stored will be validated against the schema that is provided at the creation 
time of the container.

Create a `DataContainer` an options object, described below, then call its
async `init` method before doing anything else.

```js
let {DataContainer} = require('azure-blob-storage');

let container = new DataContainer({
  containername:     'AzureContainerName',   // Azure container name
  credentials:       ...,                    // see below
  schema:            '...',                  // JSON schema object
  schemaVersion:     1,                      // JSON schema version. (optional)
                                             // The default value is 1.

  // Max number of update blob request retries
  updateRetries:              10,
  // Multiplier for computation of retry delay: 2 ^ retry * delayFactor
  updateDelayFactor:          100,

  // Randomization factor added as:
  // delay = delay * random([1 - randomizationFactor; 1 + randomizationFactor])
  updateRandomizationFactor:  0.25,

  // Maximum retry delay in ms (defaults to 30 seconds)
  updateMaxDelay:             30 * 1000,
});
await container.init();
```

#### Credentials

Credentials can be specified to this library in a variety of ways.  Note that
these match those of the
[fast-azure-storage](https://github.com/taskcluster/fast-azure-storage)
library.

##### Raw Azure credentials

Given an accountId and accompanying accessKey, configure access like this:

```js
{
  // Azure connection details
  tableName: "AzureTableName",
  // Azure credentials
  credentials: {
    accountId: "...",
    accessKey: "...",
  },
}
```

##### SAS Function

The underlying
[fast-azure-storage](https://github.com/taskcluster/fast-azure-storage) library
allows use of SAS credentials, including dynamic generation of SAS credentials
as needed. That support can be used transparently from this library:

```js
{
  tableName: 'AzureTableName',
  credentials: {
    accountId: '...',
    sas: sas   // sas in querystring form: "se=...&sp=...&sig=..."
  };
}
```

or

```js
{
  tableName: 'AzureTableName',
  credentials: {
    accountId: '...',
    sas: function() {
      return new Promise(/* fetch SAS from somewhere */);
    },
    minSASAuthExpiry:   15 * 60 * 1000 // time before refreshing the SAS
  };
}
```

### DataContainer operations

   * _init()_ (async) -
This method must be called after construction and before any other methods.

```js
    let container = new DataContainer({ /* ... */ });
    await container.init();
```
   * _ensureContainer()_
This method will ensure that the underlying Azure container actually exists. This is an idempotent operation, and is 
called automatically by `init`, so there is never any need to call this method.

```js
    await container.ensureContainer();
```

   * _removeContainer()_
Deletes the underlying Azure container. This method will not work if you are authenticated with SAS.
Note that when the container is deleted, a container with the same name cannot be created for at least 30 seconds.

```js
    await container.removeContainer();
```

   * _listBlobs(options)_
Returns a paginated list of blobs contained by the underlying container.

```js
    let blob = await container.listBlobs({
      prefix: 'state',
      maxResults: 1000,
    });
```

   * _scanDataBlockBlob(handler, options)_
Executes the provided function on each data block blob from the container, while handling pagination.

```js
    let handler = async (blob) => {
        await blob.modify((content) => {
          content.version += 1;
        });
      };
    let options = {
      prefix: 'state',
    };
    await container.scanDataBlockBlob(handler, options);
```

   * _createDataBlockBlob(options, content)_
Creates an instance of DataBlockBlob. Using this instance of blob, a JSON file can be stored in Azure storage. 
The content will be validated against the schema defined at the container level.

This is equivalent to creating a new `DataBlockBlob` instance with the given
options (see below), then calling its `create` method.  This will
unconditionally overwrite any existing blob with the same name.

```js
    let options = {
        name: 'state-blob',
        cacheContent: true,
    };
    let content = {
      value: 30,
    };
    let dataBlob = await container.createDataBlockBlob(options, content);
```

   * _createAppendDataBlob(options, content)_
Creates an instance of AppendDataBlob. Each object appended must be in JSON format and must match the schema defined at container level.
Updating and deleting the existing content is not supported.

This is equivalent to creating a new `AppendDataBlob` instance with the given
options (see below), then calling its `create` and (if `content` is provided)
`append` methods.

```js
    let options = {
        name: 'auth-log',
    };
    let content = {
      user: 'test',
    }; 
    let appendBlob = await container.createAppendDataBlob(options, content);
```

   * _load(blobName, cacheContent)_
This method returns an instance of DataBlockBlob or AppendDataBlob that was previously created in Azure storage.
It makes sense to set the cacheContent to true only for DataBlockBlob, because AppendDataBlob blobs do not keep the content
in their instance. It will throw an error if the blob does not exist.

```js
    let blob = await container.load(blob, false);
```

   * _remove(blob, ignoreIfNotExists)_
Remove a blob from Azure storage without loading it. Set the `ignoreIfNotExists` to true to ignore the error that is 
thrown in case the blob does not exist. 
Returns true, if the blob was deleted. It makes sense to read the return value only if `ignoreIfNotExists` is set.

```js
    await container.remove('state-blob', true);
```

### Schema Versions

Each blob has an associated schema version, and all schema versions are stored
in the blob storage alongside the blobs containing user data. The version
declared to the constructor defines the "current" version, but blobs may exist
that use older versions.

When a blob is loaded, it is validated against the schema with which it was
stored.

When a blob is written (via `create`, `modify`, or `append`), it is validated
against the current schema. Thus operations that modify an existing blob are
responsible for detecting and "upgrading" any old data structures.

### DataBlockBlob and AppendDataBlob

DataBlockBlob is a wrapper over an Azure block blob which stores a JSON data which is conform with schema defined at container
level.

AppendDataBlob is a wrapper over an Azure append blob. This type is optimized for fast append operations and all writes happen
at the end of the blob. Updating and deleting the existing content is not supported. This type of blob can be used
for e.g. logging or auditing.

The constructor of the blob takes the following options:

```js
let {DataBlockBlob, AppendDataBlob} = require('azure-blob-storage');
{
   name:                '...',        // The name of the blob (required)
   container:           '...',        // An instance of DataContainer (required)
   contentEncoding:     '...',        // The content encoding of the blob
   contentLanguage:     '...',        // The content language of the blob
   cacheControl:        '...',        // The cache control of the blob
   contentDisposition:  '...',        // The content disposition of the blob
   cacheContent:        true|false,   // This can be set true in order to keep a reference of the blob content.
                                      // Default value is false
}
```
The options `cacheContent` can be set to true only for DataBlockBlob because, AppendDataBlob does not support the caching
of its content.

Note that the `createDataBlockBlob` and `createAppendDataBlob` methods of
`DataContainer` provide shortcuts to calling these constructors.

### DataBlockBlob operations

   * _create(content, options)_
Creates the blob in Azure storage having the specified content which will be
validated against container schema.  The `options`, if given are passed to
[putBlob](https://taskcluster.github.io/fast-azure-storage/classes/Blob.html#method-putBlob).

```js
    let content = {
      value: 40,
    };
    let options = {
      ifMatch: 'abcd',
    };
    let content = await dataBlob.create(content, options);
```

To conditionally create a blob, use `ifNoneMatch: '*'` and catch the `BlobAlreadyExists` error:

```js
try {
  await dataBlob.create(content, {ifNoneMatch: '*'});
} catch (e) {
  if (e.code !== 'BlobAlreadyExists') {
    throw e;
  }
  console.log('blob already exists, not overwriting..');
}
```

   * _load()_
This method returns the content of the underlying blob. After the content is loaded, it is validated and also cached,
if the `cacheContent` was set.

```js
    let content = await dataBlob.load();
```

   * _modify(modifier, options)_
This method modifies the content of the blob. The `modifier` is a function that
will be called with a clone of the blob content as first argument and it should
apply the changes to the instance of the object passed as argument.  The
`options`, if given, are passed to
[putBlob](https://taskcluster.github.io/fast-azure-storage/classes/Blob.html#method-putBlob),
with `type` and `ifMatch` used to achieve atomicity.

```js
    let modifier = (data) => {
      data.value = 'new value';
    };
    let options = {
      ifUnmodifiedSince: new Date(2017, 1, 1),
    };
    await dataBlob.modify(modifier, options);
```

This method uses ETags to ensure that modifications are atomic: if some other
process writes to the blob while `modifier` is executing, `modify` will
automatically fetch the updated blob and call `modifier` again, retrying
several times.

Note that the `modifier` function must be synchronous.

### AppendDataBlob operations

   * _create(options)_
Creates the blob in Azure storage without initial content.  The `options`, if
given are passed to
[putBlob](https://taskcluster.github.io/fast-azure-storage/classes/Blob.html#method-putBlob).

```js
    await logBlob.create();
```

   * _append(content, options)_
Appends a JSON content that must be conform to container schema.

```js
    let content = {
      user: 'test2',
    }
    await logBlob.append(content);
```

   * _load()_
Load the content of the underlying blob.

```js
    let content = await logBlob.load();
```

# Testing

To test this library, set the environment variables `AZURE_ACCOUNT_KEY` and
`AZURE_ACCOUNT_ID` appropriately before running the tests.
