const { MongoClient } = require('mongodb');

const QueryOperator = require('./query-operator');

class MongoQueryOperator extends QueryOperator {
  constructor(options) {
    super(options);

    const {
      collectionName,
      mongoUrl,
    } = options;

    MongoClient.connect(mongoUrl, (err, db) => {
      this.ds = db;
      this.dsCollection = db.collection(collectionName);
    })
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
