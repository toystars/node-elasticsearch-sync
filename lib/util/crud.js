
/*!
 * node-elasticsearch-sync / crud
 * Copyright(c) 2016 Mustapha Babatunde Oluwaleke
 * MIT Licensed
 */


/*
 *  Dependencies....
 *
 * */
var _ = require('underscore'),
  parser = require('dot-object'),
  Util = require('./util');



// helper variables



module.exports = {


  /**
   * insert
   * @summary insert into elastic search
   * @param {Object} watcher
   * @param {Object} document - document to insert
   * @param {Object} esClient - elasticSearch client object
   * @return {null} return null
   */
  insert: function (watcher, document, esClient) {
    Util.transform(watcher, document, function (document) {
      esClient.index({
        index: watcher.index,
        type: watcher.type,
        id: document.id.toString(),
        body: parser.object(document)
      }, function(error, response) {
        if (!error && !response.errors) {
          console.log('ESMongoSync: Inserted - ', 'Index: ', watcher.index, ' Type: ', watcher.type, ' Id: ', document.id);
        } else {
          if(error) {
            console.log('ESMongoSync: NOT Inserted - ', 'Index: ', watcher.index, ' Type: ', watcher.type, ' Id: ', document.id,
              'because of "', error.message, '"\n', error.stack);
          }
          if(response.errors) {
            console.log('ESMongoSync: NOT Inserted - ', 'Index: ', watcher.index, ' Type: ', watcher.type, ' Id: ', document.id, '\n', JSON.stringify(response.errors));
          }
        }
      });
    });
  },


  /**
   * remove
   * @summary delete from elastic search
   * @param {Object} watcher
   * @param {String} id - id of document to remove from elasticSearch
   * @param {Object} esClient - elasticSearch client object
   * @return {null} return null
   */
  remove: function (watcher, id, esClient) {
    esClient.delete({
      index: watcher.index,
      type: watcher.type,
      id: id.toString()
    }, function (error, response) {
      if (error) {
        console.log('ESMongoSync: Not Found - ', 'Document with id ' + id + ' not found.');
      } else {
        console.log('ESMongoSync: Deleted - ', 'Index: ', watcher.index, ' Type: ', watcher.type, ' Id: ', id);
      }
    });
  },


  /**
   * remove
   * @summary update elastic search document
   * @param {Object} watcher
   * @param {String} id - id of document to update in elasticSearch
   * @param {Object} partialDocument - partial document containing fields to update with corresponding values
   * @param {Object} esClient - elasticSearch client object
   * @return {null} return null
   */
  update: function (watcher, id, partialDocument, esClient) {
    esClient.update({
      index: watcher.index,
      type: watcher.type,
      id: id.toString(),
      body: {
        doc: parser.object(partialDocument)
      }
    }, function (error) {
      if (!error) {
        console.log('ESMongoSync: Updated - ', 'Index: ', watcher.index, ' Type: ', watcher.type, ' Id: ', id);
      }
    });
  }

};

