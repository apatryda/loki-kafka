const _ = require('lodash');
const Kafka = require('no-kafka');
const Loki = require('lokijs');
const { Observable } = require('rxjs');
const Promise = require('bluebird');
const objectid = require('objectid')
const RxCollection = require('./rx-collection');
const hashQuery = require('./hashQuery');

class QueryOperator {

  opQueue = [];

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
    this.consumerInit = this.consumer.init();
    this.producerInit = this.producer.init();

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
    messageSet.forEach((consumedMessage) => {
      const opMeta = JSON.parse(consumedMessage.message.value.toString('utf8'));
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
      .then(opMeta => this.processOperation(opMeta))
      .then(() => this.processLoop())
    ;
  }

  produce(messages) {
    const producedMessages = _.castArray(messages).map(message => ({
      message,
      partition: 0,
      topic: this.resultTopic,
    }));
    return Promise
      .try(() => this.producerInit)
      .then(() => this.producer.send(producedMessages, {
        batch: {
          size: 16384,
          maxWait: 25,
        },
        codec: Kafka.COMPRESSION_SNAPPY,
      }))
    ;
  }

  produceBegin(queryDigest) {
    return this.produce({
      value: {
        op: 'begin',
        value: queryDigest,
      },
    });
  }

  produceEnd(queryDigest) {
    return this.produce({
      value: {
        op: 'end',
        value: queryDigest,
      },
    });
  }

  produceRemoves(_ids) {
    const messages = _.castArray(_ids).map(_id => ({
      value: {
        op: 'remove',
        value: _id,
      },
    }));

    return this.produce(messages);
  }

  produceUpserts(documents) {
    const messages = _.castArray(documents).map(document => ({
      value: {
        op: 'upsert',
        value: document,
      },
    }));

    return this.produce(messages);
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
      .resolve(this.findInDataStore(query))
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

  dequeueOperation() {
    if (!this.opQueue.length) {
      const queueLock = new Promise((resolve, reject) => {
        this.next = resolve;
        this.end = reject;
      });

      return queueLock
        .then(() => this.dequeueOperation())
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
        opPromise = () => Promise
          .try(() => this.insertIntoDataStore(query, document))
        ;
        break;
      }

      case 'remove': {
        opPromise = () => Promise
          .try(() => this.importQuery(this.excludeCached(query)))
          .then(() => this.removeFromDataStore(query))
        ;
        break;
      }

      case 'update': {
        const { update } = opMeta;
        opPromise = () => Promise
          .try(() => this.updateDataStore(query, update))
        ;
        break;
      }
    }

     return Promise
      .try(() => this.produceBegin(queryDigest))
      .then(opPromise)
      .then(() => {
        this.postprocessOperation(opMeta);
      })
    ;
  }

  postprocessOperation(opMeta) {
    const { op } = opMeta;

    switch (op) {
      case 'find':
      case 'insert':
      case 'update': {
        const {
          query,
          queryDigest,
        } = opMeta;

        const queryToImport = op === 'find'
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
        } = opMeta;

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
  }

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

module.exports = QueryOperator;
