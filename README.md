# node-elasticsearch-sync
ElasticSearch and MongoDB sync module for node


## What does it do?
elasticsearch-sync package keeps your mongoDB collections and elastic search cluster in sync. It does so by tailing the mongo oplog and replicate whatever crud operation into elastic search cluster without any overhead.
Please note that a replica set is needed for the package to tail mongoDB.


## How to use
```bash
$ npm install node-elasticsearch-sync --save
```

## Sample usage
After adding package to meteor app

```javascript

var ESMongoSync = require('node-elasticsearch-sync');

// initialize package as below

let finalCallBack = function () {
  // whatever code to run after package init
  .
  .
}

let transformFunction = function (watcher, document, callBack) {
  document.name = document.firstName + ' ' + document.lastName;
  callBack(document);
}

let sampleWatcher = {
  collectionName: 'users',
  index: 'person',
  type: 'users',
  transformFunction: transformFunction,
  fetchExistingDocuments: true,
  priority: 0
};

let watcherArray = [];
watcherArray.push(sampleWatcher);

let batchCount = 500;

ESMongoSync.init('MONGO_URL', 'ELASTIC_SEARCH_URL', finalCallBack, watcherArray, batchCount);

```

## Using environment variables
While it is possible to supply mongoDB and elastic search cluster URLs as parameters in the init() method, it is best to define them as environment variables
MongoDB url should be defined as: process.env.SEARCH_MONGO_URL, while Elastic search cluster url: process.env.SEARCH_ELASTIC_URL
Supplying the URLs as environments variables, the init method can be called like so:

```javasript
ESMongoSync.init(null, 'null, finalCallBack, watcherArray, batchCount);
```

## More usage info

The elasticsearch-sync package tries as much as possible to handle most heavy lifting, but certain checks has to be put in place, and that can be seen from the init function above. The ESMongoSync.init() takes five parameters:

- **MongoDB URL** - MongoDB database to tail and pull data from.

- **ElasticSearch URL** - URL to a running ElasticSearch cluster.

- **CallBack Function** - Function to execute after initial package setup is successful (can be null).

- **Watchers** - Array of watcher objects to signify which collection to tail and receive real-time update for.

- **Batch Count** - An integer that specifies number of documents to send to ElasticSearch in batches. (can be set to very high number. defaults to 100)


Below is more info about sample watcher:

```javascript
let sampleWatcher = {
  collectionName: 'users', 
  index: 'person',
  type: 'users',
  transformFunction: transformFunction,
  fetchExistingDocuments: true,
  priority: 0
};
```

- **collectionName** - MongoDB collection to watch.

- **index** - ElasticSearch index where documents from watcher collection is saved.

- **type** - ElasticSearch type given to documents from watcher collection

- **transformFunction** - Function that gets run each time watcher document is processed. Takes 3 parameters (watcher object, document and callBack function). The callBack function is to be called with processed document as argument. (can be null)

- **fetchExistingDocuments** - Specifies if existing documents in collection are to be pulled on initialization

- **priority** - Integer (starts from 0). Useful if certain watcher depends on other watchers. Watchers with lower priorities get processed before watchers with higher priorities.


## Dynamic watchers
You can dynamically add watchers by calling:
```javascript

ESMongoSync.addWatchers(watchersArray);  

// this takes an array of watchers as argument, even if only 1 watcher is to be included.
// note that existing documents won't be pulled for the new watcher, but oplog activities will be tailed in real time.

```


## Sample init:

Still confused? Get inspired by this [Sample Setup](SAMPLE.js)


## Contributing

Contributions are **welcome** and will be fully **credited**.

We accept contributions via Pull Requests on [Github](https://github.com/toystars/elasticsearch-sync).


### Pull Requests

- **Document any change in behaviour** - Make sure the `README.md` and any other relevant documentation are kept up-to-date.

- **Consider our release cycle** - We try to follow [SemVer v2.0.0](http://semver.org/). Randomly breaking public APIs is not an option.

- **Create feature branches** - Don't ask us to pull from your master branch.

- **One pull request per feature** - If you want to do more than one thing, send multiple pull requests.

- **Send coherent history** - Make sure each individual commit in your pull request is meaningful. If you had to make multiple intermediate commits while developing, please [squash them](http://www.git-scm.com/book/en/v2/Git-Tools-Rewriting-History#Changing-Multiple-Commit-Messages) before submitting.


## Issues

Check issues for current issues.

## Credits

- [Mustapha Babatunde](https://twitter.com/iAmToystars)

## License

The MIT License (MIT). Please see [LICENSE](LICENSE) for more information.