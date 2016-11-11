'use strict';

//var expect = require('chai').expect;
var ESMongoSync = require('../lib/node-elasticsearch-sync');
var assert = require('chai').assert;

describe('init function', () => {
  process.env.MONGO_OPLOG_URL = 'mongodb://mongo';
  process.env.MONGO_DATA_URL = 'mongodb://mongo';
  process.env.ELASTIC_SEARCH_URL = 'localhost:9200';
  process.env.BATCH_COUNT = 17;

  it('throws error if no arguments are supplied', (done) => {
    assert.throws(function () { ESMongoSync.init(); }, Error, /incorrect argument count/i);
    done();
  });
  it('throws error if one argument is supplied', (done) => {
    assert.throws(function () { ESMongoSync.init(null); }, Error, /incorrect argument count/i);
    done();
  });
  it('throws error if two arguments are supplied', (done) => {
    assert.throws(function () { ESMongoSync.init(null, ""); }, Error, /incorrect argument count/i);
    done();
  });
  it('throws error if four arguments are supplied', (done) => {
    assert.throws(function () { ESMongoSync.init(null, null, "", null); }, Error, /incorrect argument count/i);
    done();
  });

  it('throws error if first argument is not an array', (done) => {
    assert.throws(function () { ESMongoSync.init(null, {}, () => { }); }, Error, /first argument not an array/i);
    done();
  });
  it('throws error if second argument is not an object', (done) => {
    assert.throws(function () { ESMongoSync.init([], "not", () => { }); }, Error, /ElasticSearch Client object passed/i);
    done();
  });
  it('throws error if third argument is not a function', (done) => {
    assert.throws(function () { ESMongoSync.init([], {}, {}); }, Error, /callback must be a function/i);
    done();
  });

  it('throws no error if second argument is null', (done) => {
    assert.doesNotThrow(function () { ESMongoSync.init([], null, () => {}); });
    done();
  });
  it('throws no error if third argument is null', (done) => {
    assert.doesNotThrow(function () { ESMongoSync.init([], {}, null); });
    done();
  });

  it('throws no error and reaches callback if correct argument types are given', (done) => {
    ESMongoSync.init([], {}, () => {
      done();
    });
  });
});