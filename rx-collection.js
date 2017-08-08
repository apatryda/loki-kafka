const { Observable } = require('rxjs');

const hashQuery = require('./hashQuery');

class RxCollection {
  constructor({
    collection,
  }) {
    this.collection = collection;
    this.queryTopic = 'lokiQueries';
  }

  find(query) {
    return Observable.create((observer) => {
      const queryDigest = hashQuery(query);

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
