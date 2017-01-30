import _debug from 'debug';
const debug = _debug('azure-blob-storage:utils');

exports.rethrowDebug = (errorMessage, error) => {
  debug(errorMessage);
  throw error;
};