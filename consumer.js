const Kafka = require('no-kafka');
const Loki = require('lokijs');
const objectid = require('objectid')
const RxCollection = require('./rx-collection');

const consumer = new Kafka.SimpleConsumer();

const adapter = new Loki.LokiMemoryAdapter;
const db = new Loki('test', {
  adapter,
});

const items = db.addCollection('items');

// var mydv = items.addDynamicView('test');  // default is non-persistent
// mydv.applyFind({ _id: { $gt: 10 } });
// mydv.on('rebuild', (ev) => {
//   var results = mydv.data();
//   // console.log({ ev, rs: ev.resultset, results });
//   console.log(results);
// });

const rxItems = new RxCollection(items);

rxItems.find({ _id: { $gt: 10 } })
  .subscribe(result => console.log(result))
;

// items.insert({ name : 'mjolnir', owner: 'thor', maker: 'dwarves' });

// data handler function can return a Promise
var dataHandler = function (messageSet, topic, partition) {
  messageSet.forEach(function (m) {
    const { op, value } = JSON.parse(m.message.value.toString('utf8'));

    switch (op) {
      case 'insert': {
        items.insert(value);
        break
      }
      case 'update': {
        const [filter, update] = value;
        items.findAndUpdate(
          filter,
          (doc) => Object.assign(doc, update)
        );
        break
      }
      case 'remove': {
        items.findAndRemove(value);
        break
      }
    }
    // console.log(topic, partition, m.offset, m.message.value.toString('utf8'));
    console.log(op, value);
  });
};

consumer
  .init()
  .then(() => consumer.subscribe(
    'kafka-test-topic',
    [0, 1],
    dataHandler
  ))
;
