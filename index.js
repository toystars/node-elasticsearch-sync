
/*
 * NPM modules
 * */
var MongoOplog = require('mongo-oplog'),
  ElasticSearch = require('elasticsearch'),
  MongoDriver = require('mongodb').MongoClient,
  _ = require('underscore'),
  Util = require('./lib/util.js');


/*
 * Define main ESMongoSync object
 * */
var ESMongoSync = {
  Oplog: {},
  EsClient: {},
  dbConnection: {
    db: {},
    connected: false
  },
  options: {
    watchedCollections: [],
    batches: [],
    documentsInBatch: 100,
    config: {}
  }
};


/*
 * Function to verify env variables
 * */
var unsetEnv = [];
var getSetStatus = function (env, systemEnv) {
  return systemEnv[env];
};
var verifySystemEnv = function (mongoOplogUrl, elasticSearchUrl) {
  var processEnv = ['SEARCH_MONGO_URL', 'SEARCH_ELASTIC_URL'];
  var serverEnv = process.env;
  if (!mongoOplogUrl || !elasticSearchUrl) {
    _.each(processEnv, function (env) {
      if (!getSetStatus(env, serverEnv)) {
      unsetEnv.push(env);
    }
  });
    return unsetEnv.length === 0;
  } else {
    return true;
  }
};


/*
 * Function to process batch request
 * */
var processSingleBatch = function (currentBatch, callBack) {
  var currentDocuments = currentBatch.splice(0, ESMongoSync.options.documentsInBatch * 2);
  if (currentDocuments.length > 0) {
    var bulk = [];
    var currentCollectionName = '';
    _.each(currentDocuments, function (document) {
      if (document.index) {
        currentCollectionName = document.index._type;
      }
      Util.transform(Util.getWatcherByCollection(ESMongoSync.options.watchedCollections, currentCollectionName), document, function (doc) {
        bulk.push(doc);
        if (bulk.length === currentDocuments.length) {
          ESMongoSync.EsClient.bulk({
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


/*
 * Function to process batches
 * */
var processBatches = function (currentBatchLevel) {
  var batchLevel = currentBatchLevel || 0;
  if (batchLevel === 0) {
    console.log('ESMongoSync: Number of documents in batch - ', ESMongoSync.options.documentsInBatch);
  }
  var currentBatch = ESMongoSync.options.batches[batchLevel];
  if (currentBatch) {
    processSingleBatch(currentBatch, function () {
      processBatches(batchLevel + 1);
    });
  } else {
    console.log('Batch processing complete!');
  }
};


/*
 * Function to pull documents from mongoDB and send to elastic search in batches provided by user
 * */
var createBatches = function (currentPriorityLevel) {
  var priorityLevel = currentPriorityLevel || 0;
  if (priorityLevel === 0) {
    console.log('ESMongoSync: Beginning batch creation');
  }
  var newWatchers = Util.getWatcherAtLevel(ESMongoSync.options.watchedCollections, priorityLevel);
  if (newWatchers.length > 0) {
    console.log('ESMongoSync: Processing watchers on priority level ', priorityLevel);
    var checker = [];
    var mainDocuments = [];
    _.each(newWatchers, function (watcher) {
      console.log('ESMongoSync: Processing ', watcher.collectionName, ' collection');
      var documents = [];
      var collection = ESMongoSync.dbConnection.db.collection(watcher.collectionName);
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
                    ESMongoSync.options.batches.push(mainDocuments);
                    createBatches(priorityLevel + 1);
                  }
                }
              });
            }
          });
        } else {
          checker.push(watcher.collectionName);
          if (checker.length === newWatchers.length) {
            ESMongoSync.options.batches.push(mainDocuments);
            createBatches(priorityLevel + 1);
          }
        }
      });
    });
  } else {
    console.log('ESMongoSync: Batch creation complete. Processing...');
    processBatches();
  }
};


/*
 * Connect to main database
 * */
var connectDB = function () {
  MongoDriver.connect(process.env.SEARCH_MONGO_URL, function (error, db) {
    if (!error) {
      console.log('ESMongoSync: Connected to MONGO server successfully.');
      ESMongoSync.dbConnection.db = db;
      ESMongoSync.dbConnection.connected = true;
      createBatches();
    } else {
      throw new Error('ESMongoSync: Connection to database: ' + process.env.SEARCH_MONGO_URL + ' failed!');
    }
  });
};


/*
* Function to try reconnecting to Mongo Oplog
* */
var reconnect = function () {
  _.delay(tail, 5000, ESMongoSync.options.config.mongoOplogUrl);
};


/*
 * Function to tail Mongo database
 * */
var tail = function (mongoUrl) {
  ESMongoSync.Oplog = MongoOplog(mongoUrl).tail(function () {

    if (!ESMongoSync.Oplog.stream) {
      console.log('ESMongoSync: Connection to Oplog failed!');
      console.log('ESMongoSync: Retrying...');
      reconnect();
    } else {

      console.log('ESMongoSync: Oplog tailing connection successful.');

      ESMongoSync.Oplog.on('insert', function (doc) {
        var watcher = Util.getWatcher(ESMongoSync.options.watchedCollections, Util.getCollectionName(doc.ns));
        if (watcher) {
          Util.insert(watcher, doc.o, ESMongoSync.EsClient);
        }
      });

      ESMongoSync.Oplog.on('update', function (doc) {
        var watcher = Util.getWatcher(ESMongoSync.options.watchedCollections, Util.getCollectionName(doc.ns));
        if (watcher) {
          Util.update(watcher, doc.o2._id, doc.o.$set, ESMongoSync.EsClient);
        }
      });

      ESMongoSync.Oplog.on('delete', function (doc) {
        var watcher = Util.getWatcher(ESMongoSync.options.watchedCollections, Util.getCollectionName(doc.ns));
        if (watcher) {
          Util.remove(watcher, doc.o._id, ESMongoSync.EsClient);
        }
      });

      ESMongoSync.Oplog.on('error', function (error) {
        console.log('ESMongoSync: ', error);
        reconnect();
      });

      ESMongoSync.Oplog.on('end', function () {
        console.log('ESMongoSync: Stream ended');
        reconnect();
      });
    }
  });
};


/*
 * Function to connect to elastic search
 * */
var connectElasticSearch = function (elasticSearchUrl, callBack) {
  ESMongoSync.EsClient = new ElasticSearch.Client({
    host: elasticSearchUrl,
    apiVersion: '1.5',
    keepAlive: true
  });
  ESMongoSync.EsClient.ping({
    requestTimeout: Infinity
  }, function (error) {
    if (error) {
      console.log('ESMongoSync: ElasticSearch cluster is down!');
    } else {
      console.log('ESMongoSync: Connected to ElasticSearch successfully!');
      if (callBack) {
        callBack();
      }
      connectDB();
    }
  });
};


/*
 * Function to initialize all connections
 * */
var initialize = function (config) {
  tail(config.mongoOplogUrl);
  connectElasticSearch(config.elasticSearchUrl, config.callBack);
};


/*
 * Method to initialize all option values
 * */
ESMongoSync.init = function (mongoOplogUrl, elasticSearchUrl, callBack, watchers, documentsInBatch) {
  if (!verifySystemEnv(mongoOplogUrl, elasticSearchUrl)) {
    throw new Error('ESMongoSync: The following environment variables are not defined: ' + unsetEnv.join(', ') + '. Set and restart server.');
  }
  ESMongoSync.options.documentsInBatch = documentsInBatch;
  ESMongoSync.options.watchedCollections = watchers;
  ESMongoSync.options.config = {
    mongoOplogUrl: mongoOplogUrl || process.env['SEARCH_MONGO_URL'],
    elasticSearchUrl: elasticSearchUrl || process.env['SEARCH_ELASTIC_URL'],
    callBack: callBack
  };
  initialize(ESMongoSync.options.config);
};


/*
* Function to add watchers dynamically
* */
ESMongoSync.addWatcher = function (watchers) {
  if (_.isArray(watchers)) {
    _.each(watchers, function (watcher) {
      ESMongoSync.options.watchedCollections.push(watcher);
    });
  } else {
    console.warn('ESMongoSync: Argument not an array. Argument must be an array.');
  }
};


/*
 * Function to destroy mongoOplog
 * */
ESMongoSync.destroy = function () {
  ESMongoSync.Oplog.destroy(function () {
    console.log('ESMongoSync disconnected and Destroyed!');
  });
};


/*
 * Function to disconnect mongoOplog
 * */
ESMongoSync.disconnect = function () {
  ESMongoSync.Oplog.stop(function () {
    console.log('ESMongoSync tailing stopped!');
  });
};




module.exports = ESMongoSync;