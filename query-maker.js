const Promise = require('bluebird');
const Kafka = require('no-kafka');

class QueryMaker {
  constructor({
    queryTopic,
  }) {
    this.queryTopic = queryTopic;
    // this.consumer = new Kafka.SimpleConsumer();
    this.producer = new Kafka.Producer();
    this.producerInit = this.producer.init();

    // const consumerInit = consumer.init();
  }

  produce(messages) {
    return Promise
      .try(() => this.producerInit)
      .then(() => this.producer.send(messages, {
        batch: {
          size: 16384,
          maxWait: 25,
        },
        codec: Kafka.COMPRESSION_SNAPPY,
        partition: 0,
        topic: this.resultTopic,
      }))
    ;
  }

  produceFind(query) {
    return produce([{
      op: 'find',
      query,
    }]);
  }

  produceInsert(document) {
    return produce([{
      op: 'insert',
      document,
    }]);
  }
}

module.exports = QueryMaker;
