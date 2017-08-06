const { Observable } = require('rxjs');
const { createHash } = require('crypto');

class RxCollection {
  constructor({
    collection,
  }) {
    this.collection = collection;
    this.queryTopic = 'lokiQueries';
  }

  find(query) {
    return Observable.create((observer) => {
      const queryHash = createHash('sha256');
      queryHash.update(JSON.stringify(query));
      const queryDigest = queryHash.digest('hex');

      const dv = this.collection.addDynamicView(queryDigest);
      dv.applyFind(query);
      dv.on('rebuild', ev => observer.next(dv.data()));
      // observer.next(dv.data());

      dv.data();



      return () => this.collection.removeDynamicView(queryDigest);
    });
  }

}

module.exports = RxCollection;
