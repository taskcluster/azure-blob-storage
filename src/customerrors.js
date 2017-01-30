class ExtendableError extends Error {
  constructor(message, name, code) {
    super(message);
    this.name = name;
    this.code = code;
    Error.captureStackTrace(this, ExtendableError);
  }
}

class CongestionError extends ExtendableError {
  constructor(message) {
    super(message, 'CongestionError', 'CongestionError');
  }
}

class BlobDoesNotConformToSchema extends ExtendableError {
  constructor(message) {
    super(message, 'BlobDoesNotConformToSchema', 'BlobDoesNotConformToSchema');
  }
}

class BlobNotSerializable extends ExtendableError {
  constructor(message) {
    super(message, 'BlobNotSerializable', 'BlobNotSerializable');
  }
}

module.exports = {
  CongestionError,
  BlobDoesNotConformToSchema,
  BlobNotSerializable,
};