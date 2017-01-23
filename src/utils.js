import _debug from 'debug';
const debug = _debug('azure-blob-storage:utils');

exports.rethrowDebug = (errorMessage) => {
  debug(errorMessage);
  throw new Error(errorMessage);
};