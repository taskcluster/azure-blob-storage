azure-blob-storage
=====================================

The library is a wrapper of Azure blob storage which gives the possibility to:

**1. create JSON blobs that will be validated against a schema defined at container level.**

```js
// create an instance of BlobStorage
var blobStorage = new BlobStorage({
      credentials: {
        accountId,
        accessKey,
      },
    });
// Create an instance of Container specifing the JSON schema
// All the objects in the container must
// be in JSON format and must validate against the provided schema.
var schemaObject = '...';
var container = blobStorage.createContainer({
    name: containerName,
    schema: schemaObj
});
// store a JSON blob
// container.createDataBlob(options, content)
var json = ...;
var dataBlob = container.createDataBlob({
    name: blobName
}, json); // Before creating the blob, the content will be validated against the schema

```

**2. modify an existing JSON blob**

```js
// If you have a reference to the blob, you can call the modify method
// This method is similar with modify() from azure-entities
var modified = function(){}; // function that is called to update the content
dataBlob.update(option, modified);
```

**3. create a JSON blob in an already created container**

```js
// get the instance of the Container
// loadContainer() - will create a new instance of Container which have the information about the schemaId associated with it
var container = blobStorage.loadContainer({
    name: containerName
});

// store a JSON blob
var dataBlob = container.createDataBlob({
    name: blobName
}, json);
```

**4. upload a binary blob**

```js
// create an instance of Container without specifing the JSON schema
var container = blobStorage.createContainer({
    name: containerName,
});

// create a new binary blob
var blob = container.createBlockBlob(options, content)
```

**5. create an append data blob which is optimized for fast append operations**

We decided to use this type of blob for logging. And if a JSON schema is defined at container level, all the appends will be validated in order to conform with the desired format. (this is in progress)

```js
// create an instance of container with a schema attached
var container = blobStorage.createContainer({
    name: containerName,
    schema: schemaObject
});

var appendDataBlob = container.createAppendDataBlob({
    name: blobName
});

// append content
appendDataBlob.append(content); // the content will be validated
```
