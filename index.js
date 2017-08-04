const Kafka = require('no-kafka');
const Loki = require('lokijs');

const adapter = new Loki.LokiMemoryAdapter;
const db = new Loki('test', {
  adapter,
});

const items = db.addCollection('items');

var mydv = items.addDynamicView('test');  // default is non-persistent
mydv.applyFind({});
mydv.on('rebuild', (ev) => {
  var results = mydv.data();
  // console.log({ ev, rs: ev.resultset, results });
  console.log(results);
});

items.insert({ name : 'mjolnir', owner: 'thor', maker: 'dwarves' });
items.insert({ name : 'gungnir', owner: 'odin', maker: 'elves' });
items.insert({ name : 'tyrfing', owner: 'Svafrlami', maker: 'dwarves' });
items.insert({ name : 'draupnir', owner: 'odin', maker: 'elves' });
// setInterval(() => {
//   items.insert({ name : 'draupnir', owner: 'odin', maker: 'elves' });
// }, 5000);

const tyrfing = items.findOne({'name': 'tyrfing'});

// console.log(tyrfing);


// console.log(results);
var producer = new Kafka.Producer();

return producer.init().then(function(){
  return producer.send({
    topic: 'kafka-test-topic',
    partition: 0,
    message: {
      key: 'kaffe',
      value: 'Hello!'
    }
  });
})
.then(function (result) {
  /*
  [ { topic: 'kafka-test-topic', partition: 0, offset: 353 } ]
  */
});
