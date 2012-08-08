'use strict';

var _      = require('underscore');
var Store  = require('socket.io').Store;
var mubsub = require('mubsub');
var util   = require('util');

var connected = false;
var instances = 0;


/**
 * Mongo store.
 * Options:
 *     - collectionPrefix (string) collection name is prefix + name, defaults to socket.io
 *     - streamCollection (string) capped collection name, defaults to stream
 *     - storageCollection (string) collection name used for key/value storage, defaults to storage
 *     - nodeId (fn) gets an id that uniquely identifies this node
 *     - url (string) database url e.g. "mongodb://localhost:27017/socketio"
 *     - pack (fn) custom packing, defaults to JSON or msgpack if installed
 *     - unpack (fn) custom packing, defaults to JSON or msgpack if installed
 *
 * @api public
 */
var Mongo = module.exports = function (opts) {
    var self = this;

    this.opts = opts = _.extend({}, Mongo.defaults, opts);
  
    // node id to uniquely identify this node
    this.nodeId = opts.nodeId || Math.abs(Math.random() * Math.random() * Date.now());
    this.subscriptions = {};
    this.persistentCollection = null;
    // packing / unpacking mechanism
    if (opts.pack) {
        this.pack = opts.pack;
        this.unpack = opts.unpack;
    } else {
        try {
            var msgpack = require('msgpack');
            this.pack = msgpack.pack;
            this.unpack = msgpack.unpack;
        } catch (e) {
            this.pack = JSON.stringify;
            this.unpack = JSON.parse;
        }
    }
  
    this.channel = mubsub.channel(opts.collectionPrefix + opts.streamCollection, { size: 100000, max: 500 });
  
    mubsub.connection.db.then(function (err, db) {
        self.emit('connect', err, db);
    });

    // all instances share one connection
    ++instances;
    if (!connected) {
        connected = true;
        mubsub.connect(opts);
    }
  
    Store.call(this, opts);

    this.setMaxListeners(0);
};

util.inherits(Mongo, Store);

/**
* Module version.
*
* @api public
*/
Mongo.version = require('../package.json').version;


 /**
* Default options.
*
* @api public
*/
Mongo.defaults = {
    collectionPrefix: 'socket.io.',
    streamCollection: 'stream',
    storageCollection: 'storage',
    size: 100000,
    max: 500,
    url: null,
    host: 'localhost',
    port: 27017,
    db: 'socketio'
};


/**
 * Publishes a message.
 *
 * @api private
 */
Mongo.prototype.publish = function (name) {
    var args = [].slice.call(arguments, 1);
    this.channel.publish({
        name    : name,
        nodeId  : this.nodeId,
        args    : args
    });
    this.emit.apply(this, ['publish', name].concat(args));
};


/**
 * Subscribes to a channel
 *
 * @api private
 */
Mongo.prototype.subscribe = function (name, cb) {
    var query = { name: name, nodeId: { $ne: this.nodeId }};

    this.subscriptions[name] = this.channel.subscribe(query, function (doc) {
        cb.apply(null, doc.args);
    });
    this.emit('subscribe', name, cb);
};


/**
 * Unsubscribes
 *
 * @api private
 */

Mongo.prototype.unsubscribe = function (name, cb) {
    if (name) {
        if (this.subscriptions[name]) {
            this.subscriptions[name].unsubscribe();
            delete this.subscriptions[name];
        }
    } else {
        _.forEach(this.subscriptions, function (subscr) {
            subscr.unsubscribe();
        });
        this.subscriptions = {};
    }

    if (typeof cb === 'function') cb();

    this.emit('unsubscribe', name, cb);

    return this;
};


/**
 * Destroys the store
 *
 * @api public
 */

Mongo.prototype.destroy = function () {
  
    Store.prototype.destroy.call(this);

    this.unsubscribe();

    this.channel.close();

    this.removeAllListeners();

    this.emit('destroy');

    --instances;

    // only close db connection if this is the only instance, because
    // all instances sharing the same connection
    if (instances <= 0) {
        connected = false;
        instances = 0;
        mubsub.connection.close();
    }
};


/**
 * Get a collection for persistent data.
 *
 * @param {Function} cb
 * @api protected
 */
Mongo.prototype.getPersistentCollection = function (cb) {
    var self = this;
  
    if (self.persistentCollection) return cb(null, this.persistentCollection);

    mubsub.connection.db.then(function (err, db) {
        if (err) return cb(err);

        var name = self.opts.collectionPrefix + self.opts.storageCollection;

        db.collection(name, { safe: true }, function (err, collection) {
            if (err) return cb(err);

            self.persistentCollection = collection;
            cb(null, self.persistentCollection);
        });
    });
    return this;
};


/**
 * Client constructor
 *
 * @api private
 */

var Client = Mongo.Client = function () {
    Store.Client.apply(this, arguments);
};
util.inherits(Mongo.Client, Store.Client);

/**
 * MongoDB doc find
 *
 * @api private
 */
Client.prototype.get = function (key, fn) {
    var self = this;
    self.store.getPersistentCollection(function (err, collection) {
        if (err) return fn(err);

        collection.findOne({ _id: self.id + key }, function (err, data) {
            if (err) return fn(err);
            fn(null, data ? data.value : null);
        });
    });
    return this;
};

/**
 * MongoDB doc upsert
 *
 * @api private
 */

Client.prototype.set = function (key, value, fn) {
    var self = this;

    this.store.getPersistentCollection(function (err, collection) {
        if (err) return fn && fn(err);

        collection.update(
            { _id: self.id + key },
            { $set: { value: value, clientId: self.id } },
            { upsert: true, safe: true },
            fn
        );
    });
    return this;
};

/**
 * MongoDB doc delete
 *
 * @api private
 */

Client.prototype.del = function (key, fn) {
    var self = this;

    this.store.getPersistentCollection(function (err, collection) {
        if (err) return fn && fn(err);

        collection.remove({ _id: self.id + key }, function (err, data) {
            if (err) return fn && fn(err);

            if (fn) return fn();
        });
    });

    return this;
};

/**
 * MongoDB doc findOne for existence
 *
 * @api private
 */

Client.prototype.has = function (key, fn) {
    var self = this;

    this.store.getPersistentCollection(function (err, collection) {
        if (err) return fn && fn(err);

        collection.findOne({ _id: self.id + key }, { fields: { _id: 1 } }, function (err, data) {
            if (err) return fn && fn(err);

            if (fn) fn(null, Boolean(data));
        });
    });

    return this;
};

/**
 * Destroys client
 *
 * @param {Number} number of seconds to expire data
 * @api private
 */

Client.prototype.destroy = function (expiration, fn) {
    var self = this;

    if (typeof expiration === 'number') {
        setTimeout(function () {
            self.destroy(null, fn);
        }, expiration * 1000);
        return this;
    }
    this.store.unsubscribe();
    this.store.getPersistentCollection(function (err, collection) {
        if (err) return fn && fn(err);
        collection.remove({ clientId: self.id }, fn || function () {});
    });

    return this;
};