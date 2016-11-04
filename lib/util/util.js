
/*!
 * node-elasticsearch-sync / utils
 * Copyright(c) 2016 Mustapha Babatunde Oluwaleke
 * MIT Licensed
 */


/*
 *  Dependencies....
 *
 * */
var _ = require('underscore');


module.exports = {

  /**
   * transform
   * @summary transform document before sending to ElasticSearch
   * @param {Object} watcher
   * @param {Object} document - document to transform
   * @param {Function} callBack - function to call after document transformation
   * @return {null} return null
   */
  transform: function (watcher, document, callBack) {
    if (document.index) {
      callBack(document);
    } else {
      // move the MongoDB-ID to the ID attribute expected by ElasticSearch (if it does not exist)
      if(!document.id) {
        document.id = document._id;
        delete document._id;
      } else {
        console.log('ESMongoSync: Warning. "id" field already exists, creating a suitable identifier "id" (default: using "_id") to enable document updates must be handled in user-defined transform function.');
      }
      if (watcher.transformFunction) {
        watcher.transformFunction(watcher, document, callBack);
      } else {
        callBack(document);
      }
    }
  },


  /**
   * getWatcherByCollection
   * @summary get watcher by collection name and index
   * @param {Array} watchers
   * @param {String} collectionName - document to transform
   * @return {Object} return watcher object
   */
  getWatcherByCollection: function (watchers, collectionName) {
    return _.find(watchers, function (watcher) {
      return watcher.collectionName === collectionName;
    });
  },


  /**
   * getWatcherAtLevel
   * @summary fetch all watchers at specified level
   * @param {Array} watchers
   * @param {Number} level - level of watchers to fetch
   * @return {Array} return watchers array
   */
  getWatcherAtLevel: function (watchers, level) {
    var newWatchers = [];
    _.each(watchers, function (watcher) {
      if (watcher.priority === level && watcher.fetchExistingDocuments) {
        newWatchers.push(watcher);
      }
    });
    return newWatchers;
  },


  /**
   * getWatchersAtLevelSize
   * @summary check for size of watchers at specific level
   * @param {Array} watchers
   * @param {Number} level - level of watchers to fetch from
   * @return {Number} return length of watchers in specified level
   */
  getWatchersAtLevelSize: function (watchers, level) {
    var newWatchers = [];
    _.each(watchers, function (watcher) {
      if (watcher.priority === level) {
        newWatchers.push(watcher);
      }
    });
    return newWatchers.length;
  },


  /**
   * getCollectionName
   * @summary get collection name
   * @param {String} ns
   * @return {String} split collection name
   */
  getCollectionName: function (ns) {
    var splitArray = ns.split('.');
    return splitArray[splitArray.length - 1];
  },


  /**
   * getWatcher
   * @summary check if collection is being watched
   * @param {Array} watchers - array of watchers
   * @param {String} collectionName - name of collection to query watcher for
   * @return {Boolean} status if watcher is available or not
   */
  getWatcher: function (watchers, collectionName) {
    return _.find(watchers, function (watcher) {
      return watcher.collectionName === collectionName;
    });
  }

};

