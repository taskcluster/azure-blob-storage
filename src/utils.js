const _debug = require('debug');
const debug = _debug('azure-blob-storage:utils');

exports.rethrowDebug = (errorMessage, error) => {
  debug(errorMessage);
  throw error;
};

/*
 * Return promise to sleep for a `delay` ms
 *
 * @param   {Number} delay - Number of ms to sleep.
 * @returns {Promise} A promise that will be resolved after `delay` ms
 */
exports.sleep = (delay) => {
  return new Promise(function(resolve) {
    setTimeout(resolve, delay);
  });
};

exports.computeDelay = (retry, delayFactor, randomizationFactor, maxDelay) => {
  let delay = Math.pow(2, retry) * delayFactor;
  delay *= Math.random() * 2 * randomizationFactor + 1 - randomizationFactor;
  delay = Math.min(delay, maxDelay);

  return delay;
};
