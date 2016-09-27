let base = require('taskcluster-base');

// Create component loader

var load = base.loader({
  cfg: {
    requires: ['profile'],
    setup: ({profile}) => base.config({profile}),
  },
  stateContainer: {
    requires: ['cfg', 'profile'],
    setup: async ({cfg, profile}) => {
      // Azure Storage doesn't have promises, but we're using it in so few
      // places it doesn't make sense to write a full promise wrapper.
      // Instead, we'll just wrap as needed.
      // TODO: Use ExponentialRetryPolicyFilter
      let container = `worker-state-${profile}`;
      return Container(cfg.azureBlob.accountName, cfg.azureBlob.accountKey, container);
    },
  },
}, ['profile']);

// Export load for tests
module.exports = load;