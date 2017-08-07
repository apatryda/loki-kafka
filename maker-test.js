const Kafka = require('no-kafka');
const Promise = require('bluebird');
const objectid = require('objectid')

const QueryMaker = require('./query-maker');

const consumer = new Kafka.SimpleConsumer();

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

consumer
  .init()
  .then(() => consumer.subscribe(
    'results',
    [0],
    dataHandler
  ))
;
