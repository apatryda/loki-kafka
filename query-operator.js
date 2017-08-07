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
  processQueue = []
  // postOperationQueue =
  opQueue = [];
  // queryQueue = [];
  // sliceSize = 10;

  constructor({
    collection,
    queryTopic,
    resultTopic,
  }) {
    this.collection = collection;
    this.queryTopic = queryTopic;
    this.resultTopic = resultTopic;
    this.consumer = new Kafka.SimpleConsumer();
    this.producer = new Kafka.Producer();

    const consumerInit = consumer.init();
    const producerInit = producer.init();

    Promise
      .resolve(producerInit)
      .tap(() => {
        Promise
          .resolve(consumerInit)
          .then(() => consumer.subscribe(
            this.queryTopic,
            [0],
            (...args) => this.processMessages(...args)
          ))
        ;
      })
      .then(() => this.processLoop())
      .catch(() => Promise.each([
        this.consumer.end(),
        this.producer.end(),
      ]))
    ;
  }

  processMessages(messageSet, topic, partition) {
    messageSet.forEach((message) => {
      const opMeta = JSON.parse(m.message.value.toString('utf8'));
      const { op } = opMeta;

      if (op === 'insert') {
        const { _id } = opMeta.document;
        const query = { _id };

        Object.assign(opMeta, { query });
      }

      const { query } = opMeta;
      const queryDigest = hashQuery(query);
      Object.assign(opMeta, { queryDigest });

      this.queueOperation(opMeta);
    });
  }

  processLoop() {
    return Promise
      .try(() => this.dequeueOperation())
      .then(opMeta => this.postprocessOperation(opMeta))
      .then(() => this.processLoop())
    ;
  }

  // watch() {
  //   const dv = this.collection.addDynamicView('all');

  //   dv.on('rebuild', (ev) => {
  //     const data = dv.data().slice(0, this.sliceSize);

  //     if (!data.length && this.queries[]) {
  //       this.produceFindStop(queryDigest);
  //     }

  //     const _ids = data.map(doc => doc._id);

  //     this.produceData(data);

  //     this.findAndRemove({
  //       _id: { $in: _ids },
  //     });
  //   });
  //   dv.applyFind(query);
  // }

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

  // produceData(data) {
  //   const messages = _.map(_.castArray(data), (doc) => ({
  //     value: {
  //       op: 'upsert',
  //       value: doc,
  //     },
  //   }));

  //   return this.produce(messages);
  // }

  produceUpserts(documents) {
    const messages = _.map(_.castArray(documents), (document) => ({
      value: {
        op: 'upsert',
        value: document,
      },
    }));

    return this.produce(messages);
  }

  produceRemoves(_ids) {
    const messages = _.map(_.castArray(_ids), (_id) => ({
      value: {
        op: 'remove',
        value: _id,
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

  // followQuery(queryDigest) {

  // }

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

  // hybridFind(query) {
  //   return Promise
  //     .try(() => {
  //       const data = this.collection.find(query);
  //       const _ids = data.map(doc => doc._id);

  //       return this.queryDataStore({ $and: [
  //         query,
  //         { _id: { $nin: _ids } },
  //       ]})
  //     })
  //     .each((document) => {
  //       const { _id } = document;
  //       if (this.collection.count({ _id })) {
  //         this.collection.findAndUpdate({ _id }, document);
  //       } else {
  //         this.collection.insert(document);
  //       }
  //     })
  //     .then(() => this.collection.chain()
  //       .find(query)
  //       .data()
  //     )
  //   ;
  // }

  dequeueOperation() {
    if (!this.opQueue.length) {
      return new Promise((resolve, reject) => {
        this.next = resolve;
        this.end = reject;
      }).then(() => this.dequeueOperation())
      ;
    }

    return Promise
      .resolve(this.opQueue.shift())
    ;
  }

  queueOperation(opMeta) {
    this.opQueue.push(opMeta);
    this.next();
  }

  // queueQuery(queryMeta) {
  //   this.queryQueue.push(queryMeta);
  // }

  // addQuery(query) {
  //   const queryDigest = hashQuery(query);

  //   if (this.queries[queryDigest]) {
  //     return;
  //   }

  //   this.queries[queryDigest] = {
  //     query,
  //   };

  //   return Promise
  //     .try(() => this.produceBegin(queryDigest))
  //     .then((results) => {
  //       const [result] = results;
  //       const { offset } = result;
  //       this.queries[queryDigest].begin = offset;

  //       const data = this.collection.find(query);
  //       const _ids = data.map(doc => doc._id);

  //       return this.queryDataStore({ $and: [
  //         query,
  //         { _id: { $nin: _ids } },
  //       ]})
  //     })
  //     .then((docs) => {
  //       queryResults.forEach((doc) => {
  //         const { _id } = doc;

  //         if (!this.count({ _id })) {
  //           this.collection.insert(doc);
  //         }
  //       });

  //       this.queryQueue.push(queryDigest);
  //     })
  //   ;
  // }

  initFind(query) {
    const queryDigest = hashQuery(query);

    let queryMeta = this.queries[queryDigest];
    if (!queryMeta) {
      queryMeta = this.queries[queryDigest] = { query };
    }

    return Promise
      .try(() => this.produceBegin(queryDigest))
      .then(() => this.queueOperation({
        promise: Promise
          .resolve(),
        query,
        queryDigest,
        type: 'find',
      }))
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
      .then(() => this.queueOperation({
        document,
        promise: Promise
          .try(() => this.insertIntoDataStore(query, document)),
        query,
        queryDigest,
        type: 'insert',
      }))
    ;
  }

  initRemove(query) {
    const queryDigest = hashQuery(query);

    return Promise
      .try(() => this.produceBegin(queryDigest))
      .then(() => this.queueOperation({
        promise: Promise
          .try(() => this.importQuery(this.excludeCached(query)))
          .then(() => this.removeFromDataStore(query)),
        query,
        queryDigest,
        type: 'remove',
      }))
    ;
  }

  initUpdate(query, update) {
    const queryDigest = hashQuery(query);

    return Promise
      .try(() => this.produceBegin(queryDigest))
      .then(() => this.queueOperation({
        promise: Promise
          .try(() => this.updateDataStore(query, update)),
        query,
        queryDigest,
        type: 'update',
        update,
      }))
    ;
  }

  processOperation(opMeta) {
    const {
      op,
      query,
      queryDigest
    } = opMeta;

    let opPromise;

    switch (op) {
      case 'find': {
        opPromise = () => Promise.resolve();
        break;
      }

      case 'insert': {
        const { document } = opMeta;
        opPromise = () => this.insertIntoDataStore(query, document);
        break;
      }

      case 'remove': {
        opPromise = () => this.removeFromDataStore(query);
        break;
      }

      case 'update': {
        opPromise = () => this.removeFromDataStore(query);
        break;
      }
    }

     Promise
      .try(() => this.produceBegin(queryDigest))
      .then(() => {
        this.postprocessOperation(opMeta);
      })
    ;

    return Promise
      .try(() => this.produceBegin(queryDigest))
      .then(() => this.queueOperation({
        document,
        promise: Promise
          .try(() => this.insertIntoDataStore(query, document)),
        query,
        queryDigest,
        type: 'insert',
      }))
    ;

    return Promise
      .try(() => this.produceBegin(queryDigest))
      .then(() => this.queueOperation({
        promise: Promise
          .resolve(),
        query,
        queryDigest,
        type: 'find',
      }))
    ;

  }

  postprocessOperation(opMeta) {
    return Promise
      // .try(() => opMeta.promise)
      .try(() => {
        const { op } = opMeta;

        switch (op) {
          case 'find':
          case 'insert':
          case 'update': {
            const {
              type,
              query,
              queryDigest,
            } = queryMeta;

            const queryToImport = type === 'find'
              ? this.excludeCached(query)
              : query
            ;

            Promise
              .try(() => this.importQuery(queryToImport))
              .then(() => {
                const docs = this.collection.chain()
                  .find(query)
                  .data({ removeMeta: true })
                ;

                this.collection.findAndRemove(query);
                return this.produceUpserts(docs);
              })
              .then(() => this.produceEnd(queryDigest))
            ;
          }

          case 'remove': {
            const {
              query,
              queryDigest,
            } = queryMeta;

            const queryToImport = this.excludeCached(query);

            Promise
              .try(() => {
                const docs = this.collection.chain()
                  .find(query)
                  .data()
                ;
                const _ids = docs.map(doc => doc._id);

                this.collection.findAndRemove(query);
                return this.produceRemoves(_ids);
              })
              .then(() => this.produceEnd(queryDigest))
            ;
          }
        }
      })
    ;
  }

  // queryDataStore(query) {
  //   throw new Error('Not implemented');
  // }

  findInDataStore(query) {
    throw new Error('Not implemented');
  }

  insertIntoDataStore(document) {
    throw new Error('Not implemented');
  }

  removeFromDataStore(query) {
    throw new Error('Not implemented');
  }

  updateDataStore(query, update) {
    throw new Error('Not implemented');
  }

}
