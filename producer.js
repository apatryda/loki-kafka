const _ = require('lodash');
const Kafka = require('no-kafka');
const Promise = require('bluebird');
const objectid = require('objectid')
const randomSentence = require('random-sentence');

const producer = new Kafka.Producer();

const operations = [
  'insert',
  'update',
  'remove',
];

const data = _.times(10, _id => ({
  _id,
  val: randomSentence(),
}));

let nextId = data.length;

const looper = () => Promise
  .try(() => {
    const op = operations[Math.floor(operations.length * Math.random())];
    let value;

    switch (op) {
      case 'insert': {
        const _id = nextId++;
        const row = {
          _id,
          val: randomSentence(),
        };
        data.push(row);
        value = row;
        break;
      }
      case 'update': {
        const offset = Math.floor(data.length * Math.random());
        const { _id } = data[offset];
        const row = {
          _id,
          val: randomSentence(),
        };
        data[offset] = row;
        value = [
          { _id },
          row,
        ];
        break;
      }
      case 'remove': {
        const offset = Math.floor(data.length * Math.random());
        const { _id } = data[offset];
        data.splice(offset, 1);
        value = { _id };
        break;
      }
    }

    console.log({ op, value });

    return producer.send({
      topic: 'kafka-test-topic',
      partition: 0,
      message: {
        value: JSON.stringify({ op, value }),
      },
    });
  })
  .delay(2500)
  .then(looper)
;

producer
  .init()
  .then(looper)
  .catch(() => producer.end())
;
