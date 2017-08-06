const _ = require('lodash');
const Kafka = require('no-kafka');
const Loki = require('lokijs');
const { Observable } = require('rxjs');
const Promise = require('bluebird');
const objectid = require('objectid')
const RxCollection = require('./rx-collection');
const hashQuery = require('./hashQuery');

class QueryOperator {

  queries = {};
  queryQueue = [];
  sliceSize = 10;

  constructor() {
    const query = {};
    const queryDigest = hashQuery(query);
    const dv = this.collection.addDynamicView('all');

    dv.on('rebuild', (ev) => {
      const data = dv.data().slice(0, this.sliceSize);

      if (!data.length && this.queries[]) {
        this.produceFindStop(queryDigest);
      }

      const _ids = data.map(doc => doc._id);

      this.produceData(data);

      this.findAndRemove({
        _id: { $in: _ids },
      });
    });

    dv.applyFind(query);
  }

  watch() {
    const dv = this.collection.addDynamicView('all');

    dv.on('rebuild', (ev) => {
      const data = dv.data().slice(0, this.sliceSize);

      if (!data.length && this.queries[]) {
        this.produceFindStop(queryDigest);
      }

      const _ids = data.map(doc => doc._id);

      this.produceData(data);

      this.findAndRemove({
        _id: { $in: _ids },
      });
    });
    dv.applyFind(query);
  }

  produce(messages) {
    return this.producer.send(messages, {
      batch: {
        size: 16384,
        maxWait: 25,
      },
      codec: Kafka.COMPRESSION_SNAPPY,
      partition: 0,
      topic: this.topic,
    });
  }

  produceData(data) {
    const messages = _.map(_.castArray(data), (doc) => ({
      value: {
        op: 'upsert',
        value: doc,
      },
    }));

    return this.produce(messages);
  }

  produceBegin(queryDigest) {
    return this.produce([{
      op: 'begin',
      value: queryDigest,
    }]);
  }

  produceEnd(queryDigest) {
    return this.produce([{
      op: 'end',
      value: queryDigest,
    }]);
  }

  followQuery(queryDigest) {

  }

  excludeCached(query) {
    const docs = this.collection.find(query);
    const _ids = docs.map(doc => doc._id);

    return { $and: [
      query,
      { _id: { $nin: _ids } },
    ] };
  }

  importQuery(query) {
    return Promise
      .resolve(this.queryDataStore(query))
      .each((document) => {
        const { _id } = document;
        if (this.collection.count({ _id })) {
          this.collection.findAndUpdate({ _id }, document);
        } else {
          this.collection.insert(document);
        }
      })
    ;
  }

  hybridFind(query) {
    return Promise
      .try(() => {
        const data = this.collection.find(query);
        const _ids = data.map(doc => doc._id);

        return this.queryDataStore({ $and: [
          query,
          { _id: { $nin: _ids } },
        ]})
      })
      .each((document) => {
        const { _id } = document;
        if (this.collection.count({ _id })) {
          this.collection.findAndUpdate({ _id }, document);
        } else {
          this.collection.insert(document);
        }
      })
      .then(() => this.collection.chain()
        .find(query)
        .data()
      )
    ;
  }

  queueQuery(queryDigest) {
    this.queryQueue.push(queryDigest);
  }

  addQuery(query) {
    const queryDigest = hashQuery(query);

    if (this.queries[queryDigest]) {
      return;
    }

    this.queries[queryDigest] = {
      query,
    };

    return Promise
      .try(() => this.produceBegin(queryDigest))
      .then((results) => {
        const [result] = results;
        const { offset } = result;
        this.queries[queryDigest].begin = offset;

        const data = this.collection.find(query);
        const _ids = data.map(doc => doc._id);

        return this.queryDataStore({ $and: [
          query,
          { _id: { $nin: _ids } },
        ]})
      })
      .then((docs) => {
        queryResults.forEach((doc) => {
          const { _id } = doc;

          if (!this.count({ _id })) {
            this.collection.insert(doc);
          }
        });

        this.queryQueue.push(queryDigest);
      })
    ;
  }

  initInsert(document) {
    const { _id } = document;
    const query = { _id };
    const queryDigest = hashQuery(query);

    let queryMeta = this.queries[queryDigest];
    if (!queryMeta) {
      queryMeta = this.queries[queryDigest] = { query };
    }

    return Promise
      .try(() => this.produceBegin(queryDigest))
      .then(() => this.insertIntoDataStore(query, document))
      .then(() => this.importQuery(query))
      .then(() => this.queueQuery(queryDigest))
    ;
  }

  initRemove(query) {

  }

  processQuery(queryHash) {
    const { query } = queryMeta;

    const data = this.collection.chain()
      .find(query)
      .data()
    ;

    return Promise
      .try(() => this.produceData(data))
      .then(() => this.produceEnd(queryDigest))
    ;
  }

  queryDataStore(query) {
    throw new Error('Not implemented');
  }

  findInDataStore(query) {
    throw new Error('Not implemented');
  }

  updateDataStore(query, update) {
    throw new Error('Not implemented');
  }

  removeFromDataStore(query) {
    throw new Error('Not implemented');
  }

  insertIntoDataStore(document) {
    throw new Error('Not implemented');
  }

}
