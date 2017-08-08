const _ = require('lodash');
const Kafka = require('no-kafka');
const Loki = require('lokijs');
const { Observable } = require('rxjs');
const Promise = require('bluebird');
const objectid = require('objectid')
const RxCollection = require('./rx-collection');
const hashQuery = require('./hashQuery');

class QueryOperator {
  constructor({
    collection,
    queryTopic,
    resultTopic,
  }) {
    this.collection = collection;
    this.queryTopic = queryTopic;
    this.resultTopic = resultTopic;

    this.next = () => {};
    this.opQueue = [];

    this.consumer = new Kafka.SimpleConsumer();
    this.consumerInit = this.consumer.init();

    this.producer = new Kafka.Producer();
    this.producerInit = this.producer.init();

    Promise
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
      .then(() => this.processLoop())
      .catch(() => Promise.each([
        this.consumer.end(),
        this.producer.end(),
      ]))
    ;
  }

  processMessages(messageSet, topic, partition) {
    messageSet.forEach((message) => {
      console.log(message);
      const opMeta = JSON.parse(message.message.value.toString());
      const { op } = opMeta;

      console.log(opMeta);

      if (op === 'insert') {
        const { _id } = opMeta.document;
        const query = { _id };

        Object.assign(opMeta, { query });
      }

      const { query } = opMeta;
      const queryDigest = hashQuery(query);
      Object.assign(opMeta, { queryDigest });

      console.log(opMeta);
      this.queueOperation(opMeta);
    });
  }

  processLoop() {
    return Promise
      .try(() => this.dequeueOperation())
      .tap(op => console.log('op1', op))
      .then(opMeta => this.processOperation(opMeta))
      .tap(op => console.log('op2', op))
      .then(() => this.processLoop())
    ;
  }

  produce(messages) {
    this.producer.send()
    console.log('produce');
    // return Promise
    //   .try(() => this.producerInit)
    //   .tap(() => console.log('afterPInit', typeof this.producerInit, messages))
    //   .then(() => this.producer.send(messages, {
    //     batch: {
    //       size: 16384,
    //       maxWait: 25,
    //     },
    //     codec: Kafka.COMPRESSION_SNAPPY,
    //     partition: 0,
    //     topic: this.resultTopic,
    //   }))
    //   .tap(() => console.log('after prod'))
    // ;
    const topic = this.resultTopic;
    const partition = 0;
    return Promise
      .try(() => this.producerInit)
      .tap(() => console.log('afterPInit', typeof this.producerInit, messages))
      .then(() => this.producer.send(messages.map(message => ({ message, partition, topic })), {
        batch: {
          size: 16384,
          maxWait: 25,
        },
      }))
      .tap(() => console.log('after prod'))
    ;
  }

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
    console.log('produceBegin');
    return this.produce([{
      value: {
        op: 'begin',
        value: queryDigest,
      },
    }]);
  }

  produceEnd(queryDigest) {
    return this.produce([{
      value: {
        op: 'end',
        value: queryDigest,
      },
    }]);
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

  dequeueOperation() {
    console.log('dequeueOperation');
    if (!this.opQueue.length) {
      return new Promise((resolve, reject) => {
        this.next = resolve;
        this.end = reject;
      }).then(() => this.dequeueOperation())
      ;
    }


    const opMeta = this.opQueue.shift();
    console.log('dequeueOperation:', opMeta);
    return Promise
      .resolve(opMeta)
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
        opPromise = () => this.insertIntoDataStore(query, document);
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
        opPromise = () => this.updateDataStore(query, update);
        break;
      }
    }

    console.log('sss');

    return Promise
      .try(() => this.produceBegin(queryDigest))
      .tap(() => console.log('xxx1'))
      .then(opPromise)
      .tap(() => console.log('xxx2'))
      .then(() => {
        this.postprocessOperation(opMeta);
      })
    ;
  }

  postprocessOperation(opMeta) {
    const { op } = opMeta;

    console.log('postprocess');

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
