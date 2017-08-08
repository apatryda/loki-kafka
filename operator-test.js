const Loki = require('lokijs');
const MongoQueryOperator = require('./mongo-query-operator');

const adapter = new Loki.LokiMemoryAdapter;
const db = new Loki('test', {
  adapter,
});

const items = db.addCollection('items');

qo = new MongoQueryOperator({
  mongoUrl: 'mongodb://localhost:27017/loki',
  collectionName: 'items',
  collection: items,
  queryTopic: 'queries2',
  resultTopic: 'results2',
});
