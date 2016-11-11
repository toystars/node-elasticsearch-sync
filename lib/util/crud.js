
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
  Util = require('./util'),
  debugElasticsearch = require('debug')('node-elasticsearch-sync:elasticsearch'),
  debugError = require('debug')('node-elasticsearch-sync:error');


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
    debugElasticsearch('Inserting document %s', document.id.toString());
    Util.transform(watcher, document, function (document) {
      esClient.index({
        index: watcher.index,
        type: watcher.type,
        id: document.id.toString(),
        body: parser.object(document)
      }, function (error, response) {
        if (!error && !response.errors) {
          debugElasticsearch('Inserted document %s to %s/%s (index/type)', document.id, watcher.index, watcher.type);
        } else {
          if (error) {
            debugError('Document %s NOT inserted to %s/%s (index/type) because of "%s"\n%s', document.id, watcher.index, watcher.type, error.message, error.stack);
          }
          if (response.errors) {
            debugError('Document %s NOT inserted to %s/%s (index/type):\n%s', document.id, watcher.index, watcher.type, JSON.stringify(response.errors));
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
    debugElasticsearch('Deleting document %s', id.toString());
    esClient.delete({
      index: watcher.index,
      type: watcher.type,
      id: id.toString()
    }, function (error, response) {
      if (error) {
        debugError.log('Document %s to be deleted but not found', id);
      } else {
        debugElasticsearch('Deleted %s from %s/%s (index/type)', id, watcher.index, watcher.type);
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
    debugElasticsearch('Updating document %s', id.toString());
    esClient.update({
      index: watcher.index,
      type: watcher.type,
      id: id.toString(),
      body: {
        doc: parser.object(partialDocument)
      }
    }, function (error) {
      if (!error) {
        debugElasticsearch('Updatee %s in %s/%s (index/type)', id, watcher.index, watcher.type);
      }
    });
  }

};

