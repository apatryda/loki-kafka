const _ = require('lodash');
const jclrz = require('json-colorz');
const Kafka = require('no-kafka');
const Loki = require('lokijs');
const { Observable } = require('rxjs');
const Promise = require('bluebird');
const objectid = require('objectid')
const RxCollection = require('./rx-collection');
const hashQuery = require('./hashQuery');

class QueryOperator {
  constructor(options) {
    this.options = options;

    Promise
      .try(() => this.init())
      .then(() => this.startLooping())
    ;
  }

  init() {
    const {
      collection,
      queryTopic,
      resultTopic,
    } = this.options;

    this.collection = collection;
    this.queryTopic = queryTopic;
    this.resultTopic = resultTopic;

    this.next = () => {};
    this.opQueue = [];

    this.consumer = new Kafka.SimpleConsumer();
    this.consumerInit = this.consumer.init();

    this.producer = new Kafka.Producer();
    this.producerInit = this.producer.init();

    return Promise
      .resolve(this.producerInit)
      .tap(() => {
        Promise
          .resolve(this.consumerInit)
          .then(() => this.consumer.subscribe(
            this.queryTopic,
            [0],
            (...args) => this.processMessages(...args)
          ))
        ;
      })
    ;
  }

  startLooping() {
    return Promise
      .try(() => this.processLoop())
      .catch(() => Promise.each([
        this.consumer.end(),
        this.producer.end(),
      ]))
    ;
  }

  processLoop() {
    return Promise
      .try(() => this.dequeueOperation())
      .then(opMeta => this.processOperation(opMeta))
      .then(() => this.processLoop())
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

    const opMeta = this.opQueue.shift();
    return Promise.resolve(opMeta);
  }

  queueOperation(opMeta) {
    this.opQueue.push(opMeta);
    this.next();
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

      console.log('Recieved:')
      jclrz(opMeta);

      this.queueOperation(opMeta);
    });
  }

  produce(messages) {
    const messagesToProduce = _.castArray(messages).map(message => ({
      message,
      partition: 0,
      topic: this.resultTopic,
    }));
    return Promise
      .try(() => this.producerInit)
      .then(() => this.producer.send(messagesToProduce, {
        batch: {
          size: 16384,
          maxWait: 25,
        },
        codec: Kafka.COMPRESSION_GZIP,
      }))
    ;
  }

  produceBegin(queryDigest) {
    return this.produce({
      value: JSON.stringify({
        op: 'begin',
        value: queryDigest,
      }),
    });
  }

  produceEnd(queryDigest) {
    return this.produce({
      value: JSON.stringify({
        op: 'end',
        value: queryDigest,
      }),
    });
  }

  produceRemoves(_ids) {
    const messages = _.castArray(_ids).map(_id => ({
      value: JSON.stringify({
        op: 'remove',
        value: _id,
      }),
    }));

    return this.produce(messages);
  }

  produceUpserts(documents) {
    const messages = _.castArray(documents).map(document => ({
      value: JSON.stringify({
        op: 'upsert',
        value: document,
      }),
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

  upsertDocument(document) {
    const { _id } = document;
    if (this.collection.count({ _id })) {
      this.collection.findAndUpdate({ _id }, doc => Object.assign(doc, document));
    } else {
      this.collection.insert(document);
    }
  }

  importQuery(query) {
    return Promise
      .resolve(this.findInDataStore(query))
      .each((document) => this.upsertDocument(document))
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
        opPromise = () => Promise
          .try(() => this.insertIntoDataStore(document))
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
        break;
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
        break;
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
