const _ = require('lodash');
const jclrz = require('json-colorz');
const Kafka = require('no-kafka');
const { Observable } = require('rxjs');
const Promise = require('bluebird');

const hashQuery = require('./hashQuery');

class QueryMaker {

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
            this.resultTopic,
            [0],
            { time: Kafka.EARLIEST_OFFSET },
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

  upsertDocument(document) {
    const { _id } = document;
    console.log(document)
    if (this.collection.count({ _id })) {
      this.collection.findAndUpdate({ _id }, doc => Object.assign(doc, document));
    } else {
      this.collection.insert(document);
    }
  }

  processOperation(opMeta) {
  }

  processMessages(messageSet, topic, partition) {
    if (topic !== this.resultTopic) {
      return;
    }
    messageSet.forEach((consumedMessage) => {
      const opMeta = JSON.parse(consumedMessage.message.value.toString('utf8'));
      const { op } = opMeta;

      console.log('Recieved:')
      jclrz(opMeta);

      switch (op) {
        case 'upsert': {
          const document = opMeta.value;
          this.upsertDocument(document);
          break;
        }

        case 'remove': {
          const _id = opMeta.value;
          const query = { _id };
          this.collection.findAndRemove(query);
          break;
        }
      }
    });
  }

  produce(messages) {
    const messagesToProduce = _.castArray(messages).map(message => ({
      message,
      partition: 0,
      topic: this.queryTopic,
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

  produceFind(query) {
    return this.produce({
      value: JSON.stringify({
        op: 'find',
        query,
      }),
    });
  }

  produceInsert(document) {
    return this.produce({
      value: JSON.stringify({
        op: 'insert',
        document,
      }),
    });
  }

  produceRemove(query) {
    return this.produce({
      value: JSON.stringify({
        op: 'remove',
        query,
      }),
    });
  }

  produceUpdate(query, update) {
    return this.produce({
      value: JSON.stringify({
        op: 'update',
        query,
        update,
      }),
    });
  }

  excludeCached(query, _documents) {
    let documents = _documents;

    if (!documents) {
      documents = this.collection.find(query);
    }
    const _ids = documents.map(doc => doc._id);

    console.log('ids:', _ids);

    return { $and: [
      query,
      { _id: { $nin: _ids } },
    ] };
  }

  find(query) {
    return Observable.create((observer) => {
      const queryDigest = hashQuery(query);

      const dv = this.collection.addDynamicView(queryDigest);
      dv.applyFind(query);
      dv.on('rebuild', ev => observer.next(dv.data({ removeMeta: true })));

      this.produceFind(this.excludeCached(query, dv.data()));

      return () => this.collection.removeDynamicView(queryDigest);
    });
  }

  insert(document) {
    this.produceInsert(document);
  }
}

module.exports = QueryMaker;
