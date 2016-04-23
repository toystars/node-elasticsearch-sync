/*
 * SAMPLE.js
 *
 * This is to illustrate the basic use of node-elasticsearch-sync package,
 * this assumes the package has been added to the node app successfully.
 *
 * This does not cover elastic search cluster set-up and assumes the cluster is up and running,
 * and that index and types are set up correctly with the correct mappings
 *


 * Let's say we have a database with 3 collections: users, posts and comments.
 * And we want to have all the collections indexed in elasticsearch. Watchers can be created as below
 *


 * define transform functions for each watcher. Takes 3 parameters:
 * watcher, document and callBack.
 *
 * callBack is to be invoked after document transformation with the new document as the parameter
 */


var transformUser = function (watcher, user, callBack) {
  user.fullName = user.firstName + ' ' + user.lastName; // object has been transformed
  callBack(user); // callBack called with transformed object
};

var transformPost = function (watcher, post, callBack) {
  post.savedDate = new Date();
  callBack(post); // callBack called with transformed object
};

var transformComment = function (watcher, comment, callBack) {
  comment.author = 'Mustapha Babatunde Oluwaleke';
  callBack(comment); // callBack called with transformed object
};

var watchers = [];

// define watchers
var usersWatcher = {
  collectionName: 'users',
  index: 'person', // elastic search index
  type: 'users', // elastic search type
  transformFunction: transformUser, // can be null if no transformation is needed to be done
  fetchExistingDocuments: true, // this will fetch all existing document in collection and index in elastic search
  priority: 0 // defines order of watcher processing. Watchers with low priorities get processed ahead of those with high priorities
};
var postsWatcher = {
  collectionName: 'posts',
  index: 'post',
  type: 'posts',
  transformFunction: transformPost,
  fetchExistingDocuments: true,
  priority: 0
};
var commentsWatcher = {
  collectionName: 'posts',
  index: 'post',
  type: 'posts',
  transformFunction: transformComment,
  fetchExistingDocuments: true,
  priority: 0
};


// push watchers into array
watchers.push(usersWatcher, postsWatcher, commentsWatcher);


/* * Call the init method of the package as below and you are done. Parameters are as follows.
 * 1. MongoDB URL (Has to be a replica set for Oplog to be enabled), (can be null if process.env.SEARCH_MONGO_URL is set)
 * 2. Elastic Search URL, (can be null if process.env.SEARCH_ELASTIC_URL is set)
 * 3. Function to call after package init (can be null),
 * 4. Watchers,
 * 5. BatchCount - Number of documents to index for each bulk elastic search indexing (Should be set according to elastic search cluster capability)
 *
 * Note: All parameters are to be supplied in the specified order.
 **/

ESMongoSync.init('MONGO_URL', 'ELASTIC_SEARCH_URL', null, watchers, 500);


/*
 * Using process vars
 * export SEARCH_MONGO_URL="mongodb://localhost:27017/sample"
 * export SEARCH_ELASTIC_URL="localhost:9200"
 */
ESMongoSync.init(null, null, null, watchers, 500);


/*
 * Watchers can be disabled by running
 * */
ESMongoSync.disconnect();

/*
 * Watchers can be re-enabled by running
 * */
ESMongoSync.reconnect();


/*
 * Watchers can be destroyed by running
 * */
ESMongoSync.destroy(); // note that you will have to re-initialize the watchers after calling destroy.


/*
 * That's all! Now whatever CRUD operation that occur in the MongoDB database collection that are specified in the watchers will be synchronized
 * with the specified elastic search cluster in real time. You don't have to worry about your elastic search documents getting stale over time as all
 * CRUD operations are handled seamlessly.
 *
 * */
