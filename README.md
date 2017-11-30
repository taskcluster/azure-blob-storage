[![Build Status](https://travis-ci.org/elenasolomon/azure-blob-storage.svg?branch=master)](https://travis-ci.org/elenasolomon/azure-blob-storage)

## azure-blob-storage

This library wraps an Azure Blob Storage container which stores objects in JSON format.

### Creating an instance of DataContainer

DataContainer is a wrapper over Azure Blob Storage container which stores only objects in JSON format.
All the objects that will be stored will be validated against the schema that is provided at the creation 
time of the container.

The constructor of the DataContainer takes the following options:

```js
{
  // Azure connection details for use with SAS from auth.taskcluster.net
  account:           '...',                  // Azure storage account name
  container:         'AzureContainerName',   // Azure container name
  // TaskCluster credentials
  credentials: {
    clientId:        '...',                  // TaskCluster clientId
    accessToken:     '...',                  // TaskCluster accessToken
  },
  accessLevel:       'read-write',           // The access level of the container: read-only/read-write (optional)
  authBaseUrl:       '...',                  // baseUrl for auth (optional)
  schema:            '...',                  // JSON schema object

  // Max number of update blob request retries
  updateRetries:              10,
  // Multiplier for computation of retry delay: 2 ^ retry * delayFactor
  updateDelayFactor:          100,

  // Randomization factor added as:
  // delay = delay * random([1 - randomizationFactor; 1 + randomizationFactor])
  updateRandomizationFactor:  0.25,

  // Maximum retry delay in ms (defaults to 30 seconds)
  updateMaxDelay:             30 * 1000,
}
```

Using the `options` format provided above a shared-access-signature will be fetched from auth.taskcluster.net. To fetch the
shared-access-signature the following scope is required:  
    `auth:azure-blob:<level>:<account>/<container>`

In case you have the Azure credentials, the options are:
```js
{
   // Azure credentials
   credentials: {
     accountName: '...',         // Azure account name
     accountKey: '...',          // Azure account key
   }
}
```

### DataContainer operations

   * _ensureContainer()_
This method will ensure that the underlying Azure container actually exists. This is an idempotent operation, and is 
often called in service start-up. If you've used taskcluster-auth to get credentials rather than azure credentials, 
do not use this as taskcluster-auth has already ensured the container exists for you.

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

### DataBlockBlob and AppendDataBlob

DataBlockBlob is a wrapper over an Azure block blob which stores a JSON data which is conform with schema defined at container
level.

AppendDataBlob is a wrapper over an Azure append blob. This type is optimized for fast append operations and all writes happen
at the end of the blob. Updating and deleting the existing content is not supported. This type of blob can be used
for e.g. logging or auditing.

The constructor of the blob takes the following options:

```js
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

### DataBlockBlob operations

   * _create(content)_
Creates the blob in Azure storage having the specified content which will be validated against container schema.

```js
    let content = {
      value: 40,
    }
    let content = await dataBlob.create(content);
```

   * _load()_
This method returns the content of the underlying blob. After the content is loaded, it is validated and also cached,
if the `cacheContent` was set.

```js
    let content = await dataBlob.load();
```
   * _modify(modifier, options)_
This method modifies the content of the blob. The `modifier` is a function that will be called with a clone of the blob
content as first argument and it should apply the changes to the instance of the object passed as argument. 
```js
    let modifier = (data) => {
      data.value = 'new value';
    };
    let options = {
      prefix: 'state',
    };
    await dataBlob.modify(modifier, options);
```


### AppendDataBlob operations

   * _create()_
Creates the blob in Azure storage without initial content.

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
