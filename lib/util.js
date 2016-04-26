

var _ = require('underscore');


/*
* Function to verify watchers
* */
var checkWatcher = function (watcher) {
  return typeof watcher.collectionName === 'string' && typeof watcher.index === 'string'
    && typeof watcher.type === 'string' && (!watcher.transformFunction || typeof watcher.transformFunction === 'function')
    && typeof watcher.fetchExistingDocuments === 'boolean' && typeof watcher.priority === 'number' && watcher.priority >= 0;
};


var verifyWatchers = function (watchers) {
  for (var x = 0; x < watchers.length; x++) {
    if (!checkWatcher(watchers[x])) {
      throw new Error('ESMongoSync: Watcher parsing error. Watcher objects not well formatted.');
    }
  }
};

/*
* Utility methods
* */
module.exports = {


  /*
   * Function to verify init function arguments
   * */
   verifyInitArgs: function (args) {
    if (args.length !== 5) {
      throw new Error('ESMongoSync: Initialization call requires 5 parameters. ' + args.length + ' parameters provided');
    }
    if ((args[0] && typeof args[0] !== 'string') || (args[1] && typeof args[1] !== 'string')) {
      throw new Error('ESMongoSync: Expects string values as first and second parameters, got ' + typeof args[0] + ' for first and ' + typeof args[1] + ' for second.');
    }
    if (args[2] && typeof args[2] !== 'function') {
      throw new Error('ESMongoSync: Expects function as third parameter, got ' + typeof args[2] + ' instead.');
    }
    if (_.isArray(args[3])) {
      verifyWatchers(args[3]);
    } else {
      throw new Error('ESMongoSync: Expects array as fourth parameter, got ' + typeof args[3] + ' instead.');
    }
    if (typeof parseInt(args[4]) !== 'number' || parseInt(args[4]) < 0) {
      throw new Error('ESMongoSync: Expects number as fourth parameter, got ' + typeof args[3] + ' instead. Also expects number greater than or equal to zero (0).');
    }
  },


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
