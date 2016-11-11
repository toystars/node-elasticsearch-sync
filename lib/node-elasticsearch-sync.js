
/*
 *  Dependencies....
 *
 * */
var MongoOplog = require('mongo-oplog'),
  ElasticSearch = require('elasticsearch'),
  MongoDriver = require('mongodb').MongoClient,
  _ = require('underscore'),
  Validation = require('./util/validation'),
  Util = require('./util/util'),
  Crud = require('./util/crud');


/*
* private variables used for internal processes only
* */
var Oplog = {};
var EsClient = null;
var DBConnection = {
  db: {},
  connected: false
};
var Options = {
  watchedCollections: [],
  batches: [],
  documentsInBatch: 100
};


/**
 * processSingleBatch
 * @summary process batch request
 * @param {Array} currentBatch - array of documents to index (including meta-data for each document)
 * @param {Function} callBack - callBack invoked after indexing operation is complete
 * @return {null} return null
 */
var processSingleBatch = function (currentBatch, callBack) {
  var currentDocuments = currentBatch.splice(0, Options.documentsInBatch * 2);
  if (currentDocuments.length > 0) {
    var bulk = [];
    var currentCollectionName = '';
    _.each(currentDocuments, function (document) {
      if (document.index) {
        currentCollectionName = document.index._type;
      }
      Util.transform(Util.getWatcherByCollection(Options.watchedCollections, currentCollectionName), document, function (doc) {
        bulk.push(doc);
        if (bulk.length === currentDocuments.length) {
          EsClient.bulk({
            body: bulk
          }, function (error, response) {
            if (!error && !response.errors) {
              console.log('ESMongoSync: Number of documents indexed in batch - ', bulk.length / 2);
              processSingleBatch(currentBatch, callBack);
            } else {
              console.log(error);
            }
          });
        }
      });
    });
  } else {
    callBack();
  }
};


/**
 * processBatches
 * @summary process batches
 * @param {Number} currentBatchLevel - optional level of watcher to batch process, level defaults to 0 if not provided
 * @return {null} return null
 */
var processBatches = function (currentBatchLevel) {
  var batchLevel = currentBatchLevel || 0;
  if (batchLevel === 0) {
    console.log('ESMongoSync: Number of documents in batch - ', Options.documentsInBatch);
  }
  var currentBatch = Options.batches[batchLevel];
  if (currentBatch) {
    processSingleBatch(currentBatch, function () {
      processBatches(batchLevel + 1);
    });
  } else {
    console.log('ESMongoSync: Batch processing complete!');
  }
};


/**
 * createBatches
 * @summary pull documents from mongoDB and send to elastic search in batches provided by user
 * @param {Number} currentPriorityLevel - optional level of watcher to pull data from, level defaults to 0 if not provided
 * @return {null} return null
 */
var createBatches = function (currentPriorityLevel) {
  var priorityLevel = currentPriorityLevel || 0;
  if (priorityLevel === 0) {
    console.log('ESMongoSync: Beginning batch creation');
  }
  var newWatchers = Util.getWatcherAtLevel(Options.watchedCollections, priorityLevel);
  if (newWatchers.length > 0) {
    console.log('ESMongoSync: Processing watchers on priority level ', priorityLevel);
    var checker = [];
    var mainDocuments = [];
    _.each(newWatchers, function (watcher) {
      console.log('ESMongoSync: Processing ', watcher.collectionName, ' collection');
      var documents = [];
      var collection = DBConnection.db.collection(watcher.collectionName);
      collection.count(function (e, count) {
        if (count > 0) {
          collection.find({}).forEach(function (document) {
            documents.push(document);
            if (documents.length === count) {
              _.each(documents, function (doc, docIndex) {
                mainDocuments.push({
                  index: {
                    _index: watcher.index,
                    _type: watcher.type,
                    _id: doc._id
                  }
                }, doc);
                if (docIndex === documents.length - 1) {
                  checker.push(watcher.collectionName);
                  if (checker.length === newWatchers.length) {
                    Options.batches.push(mainDocuments);
                    createBatches(priorityLevel + 1);
                  }
                }
              });
            }
          });
        } else {
          checker.push(watcher.collectionName);
          if (checker.length === newWatchers.length) {
            Options.batches.push(mainDocuments);
            createBatches(priorityLevel + 1);
          }
        }
      });
    });
  } else {
    if (Util.getWatchersAtLevelSize(Options.watchedCollections, priorityLevel + 1) === 0) {
      console.log('ESMongoSync: Batch creation complete. Processing...');
      processBatches();
    } else {
      createBatches(priorityLevel + 1);
    }
  }
};


/**
 * connectDB
 * @summary connect to main database
 */
var connectDB = function () {
  MongoDriver.connect(process.env['MONGO_DATA_URL'], function (error, db) {
    if (!error) {
      console.log('ESMongoSync: Connected to MONGO server successfully.');
      DBConnection.db = db;
      DBConnection.connected = true;
      createBatches();
    } else {
      throw new Error('ESMongoSync: Connection to database: ' + process.env['MONGO_DATA_URL'] + ' failed!');
    }
  });
};


/**
 * reconnect
 * @summary try reconnecting to Mongo Oplog
 */
var reconnect = function () {
  _.delay(tail, 5000);
};


/**
 * tail
 * @summary tails mongo database for real-time events emission
 */
var tail = function () {

  Oplog = MongoOplog(process.env['MONGO_OPLOG_URL']).tail(function () {

    if (!Oplog.stream) {
      console.log('ESMongoSync: Connection to Oplog failed!');
      console.log('ESMongoSync: Retrying...');
      reconnect();
    } else {

      console.log('ESMongoSync: Oplog tailing connection successful.');

      Oplog.on('insert', function (doc) {
        var watcher = Util.getWatcher(Options.watchedCollections, Util.getCollectionName(doc.ns));
        if (watcher) {
          Crud.insert(watcher, doc.o, EsClient);
        }
      });

      Oplog.on('update', function (doc) {
        var watcher = Util.getWatcher(Options.watchedCollections, Util.getCollectionName(doc.ns));
        if (watcher) {
          Crud.update(watcher, doc.o2._id, doc.o.$set, EsClient);
        }
      });

      Oplog.on('delete', function (doc) {
        var watcher = Util.getWatcher(Options.watchedCollections, Util.getCollectionName(doc.ns));
        if (watcher) {
          Crud.remove(watcher, doc.o._id, EsClient);
        }
      });

      Oplog.on('error', function (error) {
        console.log('ESMongoSync: ', error);
        reconnect();
      });

      Oplog.on('end', function () {
        console.log('ESMongoSync: Stream ended');
        reconnect();
      });

    }
  });
};


/**
 * connectElasticSearch
 * @summary connect to elastic search if no object is passed from init
 */
var connectElasticSearch = function () {
  if (EsClient === null) {
    EsClient = new ElasticSearch.Client({
      host: process.env['ELASTIC_SEARCH_URL'],
      keepAlive: true
    });
    EsClient.ping({
      requestTimeout: Infinity
    }, function (error) {
      if (error) {
        console.log('ESMongoSync: ElasticSearch cluster is down!');
      } else {
        console.log('ESMongoSync: Connected to ElasticSearch successfully!');
        connectDB();
      }
    });
  } else {
    connectDB();
  }
};


/**
 * initialize
 * @summary initializing package and setting up major connections
 * @param {Function} callBack - callBack invoked after indexing operation is complete
 * @return {null} return null
 */
var initialize = function (callBack) {
  callBack = callBack || function(){};
  tail();
  connectElasticSearch();
  callBack();
};


/**
 * init
 * @summary initializing package and setting up major connections
 * @param {Array} watchers - array of watchers specifying Mongo Database collections to watch in real time
 * @param {Object} esClient - elasticSearch object to be used in all communications with ElasticSearch cluster
 * @param {Function} callBack - callBack invoked after indexing operation is complete
 * @return {null} return null
 */
var init = function (watchers, esClient, callback) {

  // validate arguments
  Validation.validateArgs(arguments);

  // validate environment variables
  var unsetEnvs = Validation.systemEnvsSet();
  if (esClient && unsetEnvs.length === 1 && unsetEnvs[0] === 'ELASTIC_SEARCH_URL') {
    unsetEnvs = [];
  }

  if (unsetEnvs.length > 0) {
    throw new Error('ESMongoSync: The following environment variables are not defined: ' + unsetEnvs.join(', ') + '. Set and restart server.');
  }

  // continue package init
  Options.documentsInBatch = Number(process.env['BATCH_COUNT']);
  Options.watchedCollections = watchers;
  EsClient = esClient;
  initialize(callback);
};


/**
 * addWatchers
 * @summary add watchers dynamically, documents won't be pulled from MongoDB automatically unless reIndex is called explicitly
 * @param {Array} watchers - array of watchers specifying Mongo Database collections to watch in real time
 * @return {null} return null
 */
var addWatchers = function (watchers) {
  if (_.isArray(watchers)) {
    _.each(watchers, function (watcher) {
      Options.watchedCollections.push(watcher);
    });
  } else {
    console.warn('ESMongoSync: Argument not an array. Argument must be an array.');
  }
};


/**
 * reconnectOplog
 * @summary reconnect mongoOplog
 */
var reconnectOplog = function () {
  reconnect();
};


/**
 * destroy
 * @summary destroy mongoOplog
 */
var destroy = function () {
  Oplog.destroy(function () {
    console.log('ESMongoSync: disconnected and Destroyed!');
  });
};


/**
 * disconnect
 * @summary disconnect mongoOplog
 */
var disconnect = function () {
  Oplog.stop(function () {
    console.log('ESMongoSync: tailing stopped!');
  });
};


/**
 * reIndex
 * @summary reindex all data from mongoDB to ElasticSearch
 */
var reIndex = function () {
  createBatches();
};


module.exports = {
  init: init,
  destroy: destroy,
  disconnect: disconnect,
  reconnect: reconnectOplog,
  addWatchers: addWatchers,
  reIndex: reIndex
};
