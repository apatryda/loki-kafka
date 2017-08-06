const { createHash } = require('crypto');

const hashQuery = function hq(query) {
  const queryHash = createHash('sha256');
  queryHash.update(JSON.stringify(query));
  const queryDigest = queryHash.digest('hex');

  return queryDigest;
}

module.exports = hashQuery;
