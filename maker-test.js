const Kafka = require('no-kafka');
const Promise = require('bluebird');
const objectid = require('objectid')
const randomSentence = require('random-sentence');

const QueryMaker = require('./query-maker');

const consumer = new Kafka.SimpleConsumer();
const producer = new Kafka.Producer();

const qm = new QueryMaker({
  queryTopic: 'queries',
});

var dataHandler = function (messageSet, topic, partition) {
  messageSet.forEach((m) => {
    console.log(m);
    // const { op, value } = JSON.parse(m.message.value.toString('utf8'));

    // switch (op) {
    //   case 'insert': {
    //     items.insert(value);
    //     break
    //   }
    //   case 'update': {
    //     const [filter, update] = value;
    //     items.findAndUpdate(
    //       filter,
    //       (doc) => Object.assign(doc, update)
    //     );
    //     break
    //   }
    //   case 'remove': {
    //     items.findAndRemove(value);
    //     break
    //   }
    // }
    // console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
    // console.log(op, value);
  });
};

Promise
  .try(() => consumer.init())
  .then(() => consumer.subscribe(
    'results',
    [0],
    dataHandler
  ))
  .then(() => producer.init())
  .then(() => producer.send({
    partition: 0,
    topic: 'queries',
    message: {
      value: JSON.stringify({
        op: 'insert',
        document: {
          _id: objectid().toHexString(),
          text: randomSentence(),
        },
      }),
    },
  }))
;
