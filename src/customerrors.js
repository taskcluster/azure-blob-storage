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

class SchemaValidationError extends ExtendableError {
  constructor(message) {
    super(message, 'SchemaValidationError', 'SchemaValidationError');
  }
}

class BlobSerializationError extends ExtendableError {
  constructor(message) {
    super(message, 'BlobSerializationError', 'BlobSerializationError');
  }
}

module.exports = {
  CongestionError,
  SchemaValidationError,
  BlobSerializationError,
};