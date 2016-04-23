

var _ = require('underscore');

/*
* Utility methods
* */
module.exports = {


  /*
   * Function to transform document if transform function is defined
   * */
  transform: function (watcher, document, callBack) {
    if (document.index) {
      callBack(document);
    } else {
      if (watcher.transformFunction) {
        watcher.transformFunction(watcher, document, callBack);
      } else {
        document.id = document._id;
        delete document._id;
        callBack(document);
      }
    }
  },


  /*
   * Function to get watcher by collection name and index
   * */
  getWatcherByCollection: function (watchers, collectionName) {
    return _.find(watchers, function (watcher) {
      return watcher.collectionName === collectionName;
    });
  },


  /*
   * Function to fetch all watchers at specified level
   * */
  getWatcherAtLevel: function (watchers, level) {
    var newWatchers = [];
    _.each(watchers, function (watcher) {
      if (watcher.priority === level && watcher.fetchExistingDocuments) {
        newWatchers.push(watcher);
      }
    });
    return newWatchers;
  },


  /*
   * Function to get collection name
   * */
  getCollectionName: function (ns) {
    var splitArray = ns.split('.');
    return splitArray[splitArray.length - 1];
  },


  /*
   * Function to check if collection is being watched
   * */
  getWatcher: function (watchers, collectionName) {
    return _.find(watchers, function (watcher) {
      return watcher.collectionName === collectionName;
    });
  },


  /*
   * Function to insert into elastic search
   * */
  insert: function (watcher, document, esClient) {
    this.transform(watcher, document, function (document) {
      esClient.index({
        index: watcher.index,
        type: watcher.type,
        id: document.id,
        body: document
      }, function(error, response) {
        if (!error && !response.errors) {
          console.log('ESMongoSync: Inserted - ', 'Index: ', watcher.index, ' Type: ', watcher.type, ' Id: ', document.id);
        }
      });
    });
  },


  /*
   * Function to delete from elastic search
   * */
  remove: function (watcher, id, esClient) {
    esClient.delete({
      index: watcher.index,
      type: watcher.type,
      id: id
    }, function (error, response) {
      if (!response.found) {
        console.log('ESMongoSync: Deleted - ', 'Index: ', watcher.index, ' Type: ', watcher.type, ' Id: ', id);
      }
    });
  },


  /*
   * Function to update elastic search document
   * */
  update: function (watcher, id, partialDocument, esClient) {
    esClient.update({
      index: watcher.index,
      type: watcher.type,
      id: id,
      body: {
        doc: partialDocument
      }
    }, function (error) {
      if (!error) {
        console.log('ESMongoSync: Updated - ', 'Index: ', watcher.index, ' Type: ', watcher.type, ' Id: ', id);
      }
    });
  }

};
