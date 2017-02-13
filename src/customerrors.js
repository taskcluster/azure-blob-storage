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
    super(message, 'CongestionError', 'Congestion');
  }
}

class SchemaValidationError extends ExtendableError {
  constructor(message) {
    super(message, 'SchemaValidationError', 'SchemaValidation');
  }
}

class BlobSerializationError extends ExtendableError {
  constructor(message) {
    super(message, 'BlobSerializationError', 'BlobSerialization');
  }
}

module.exports = {
  CongestionError,
  SchemaValidationError,
  BlobSerializationError,
};