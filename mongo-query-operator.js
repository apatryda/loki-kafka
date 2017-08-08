const { MongoClient } = require('mongodb');
const Promise = require('bluebird');

const QueryOperator = require('./query-operator');

class MongoQueryOperator extends QueryOperator {
  init() {
    const {
      collectionName,
      mongoUrl,
    } = this.options;

    this.collectionName = collectionName;
    this.mongoUrl = mongoUrl;

    return Promise
      .try(() => MongoClient.connect(this.mongoUrl))
      .then((db) => {
        this.ds = db;
        this.dsCollection = db.collection(this.collectionName);
      })
      .then(() => super.init())
    ;
  }

  findInDataStore(query) {
    return this.dsCollection
      .find(query)
      .toArray()
    ;
  }

  insertIntoDataStore(document) {
    return this.dsCollection
      .insertOne(document)
    ;
  }

  removeFromDataStore(query) {
    return this.dsCollection
      .remove(query)
    ;
  }

  updateDataStore(query, update) {
    return this.dsCollection
      .update(query, update, { multi: true })
    ;
  }
}

module.exports = MongoQueryOperator;
