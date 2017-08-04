const Kafka = require('no-kafka');
const objectid = require('objectid')

const producer = new Kafka.Producer();

producer
  .init()
  // .then(() => producer.send({
  //   topic: 'kafka-test-topic',
  //   partition: 0,
  //   message: {
  //     key: 'kaffe',
  //     value: JSON.stringify({ op: 'insert', value: {
  //       _id: objectid(),
  //       n: Math.random(),
  //     } }),
  //   },
  // }))
  .then(() => producer.send({
    topic: 'kafka-test-topic',
    partition: 0,
    message: {
      key: 'kaffe',
      value: JSON.stringify({ op: 'insert', value: {
        _id: 10,
        n: 100,
      } }),
    },
  }))
  .then(() => producer.send({
    topic: 'kafka-test-topic',
    partition: 0,
    message: {
      key: 'kaffe',
      value: JSON.stringify({ op: 'insert', value: {
        _id: 11,
        n: 300,
      } }),
    },
  }))
  .then(() => producer.send({
    topic: 'kafka-test-topic',
    partition: 0,
    message: {
      key: 'kaffe',
      value: JSON.stringify({ op: 'update', value: [
        { _id: 10 },
        {
          n: 200,
        },
       ] }),
    },
  }))
  .then(() => producer.send({
    topic: 'kafka-test-topic',
    partition: 0,
    message: {
      key: 'kaffe',
      value: JSON.stringify({ op: 'remove', value: {
        _id: 10,
      } }),
    },
  }))
  .then(result => producer.end())
;
