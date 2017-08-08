const jclrz = require('json-colorz');
const Loki = require('lokijs');
const QueryMaker = require('./query-maker');
const objectid = require('objectid')
const Promise = require('bluebird');
const randomSentence = require('random-sentence');

const adapter = new Loki.LokiMemoryAdapter;
const db = new Loki('test', {
  adapter,
});

const items = db.addCollection('items');

const qm = new QueryMaker({
  collection: items,
  queryTopic: 'queries2',
  resultTopic: 'results2',
});


Promise
  .delay(2000)
  .then(() => {
    qm.insert({
      _id: objectid().toHexString(),
      text: randomSentence(),
    });
  })
  .delay(2000)
  .then(() => {
    qm.find({})
      .subscribe((documents) => {
        console.log('Find Results:');
        jclrz(documents);
      })
    ;
  })
;

// const Kafka = require('no-kafka');
// const Promise = require('bluebird');
// const objectid = require('objectid')
// const randomSentence = require('random-sentence');

// const consumer = new Kafka.SimpleConsumer();
// const producer = new Kafka.Producer();

// const dataHandler = function (messageSet, topic, partition) {
//   messageSet.forEach((m) => {
//     console.log(JSON.parse(m.message.value.toString()));
//     // const { op, value } = JSON.parse(m.message.value.toString('utf8'));

//     // switch (op) {
//     //   case 'insert': {
//     //     items.insert(value);
//     //     break
//     //   }
//     //   case 'update': {
//     //     const [filter, update] = value;
//     //     items.findAndUpdate(
//     //       filter,
//     //       (doc) => Object.assign(doc, update)
//     //     );
//     //     break
//     //   }
//     //   case 'remove': {
//     //     items.findAndRemove(value);
//     //     break
//     //   }
//     // }
//     // console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
//     // console.log(op, value);
//   });
// };

// Promise
//   .try(() => consumer.init())
//   .then(() => consumer.subscribe(
//     'results',
//     [0],
//     { time: Kafka.EARLIEST_OFFSET },
//     dataHandler
//   ))
//   .then(() => producer.init())
//   .then(() => producer.send({
//     partition: 0,
//     topic: 'queries',
//     message: {
//       value: JSON.stringify({
//         op: 'insert',
//         document: {
//           _id: objectid().toHexString(),
//           text: randomSentence(),
//         },
//       }),
//     },
//   }))
// ;
