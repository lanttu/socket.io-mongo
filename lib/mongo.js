
/*!
 * socket.io-mongo
 */

/**
 * Module dependencies.
 */

var Store = require('socket.io').Store,
    mubsub = require('mubsub');


/**
 * Global vars
 */
 
var connected = false,
    instances = 0;

/**
 * Exports the constructor.
 */

exports = module.exports = Mongo;
Mongo.Client = Client;


/**
* Module version.
*
* @api public
*/
Mongo.version = require('../package.json').version;

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


Object.defineProperty(Object.prototype, "extend", {
  enumerable: false,
  value: function() {
    var destination = this, args = [].slice.call(arguments, 0), props, descriptor;
    args.forEach(function(source){
      props = Object.getOwnPropertyNames(source);
      props.forEach(function(name) {
        descriptor = Object.getOwnPropertyDescriptor(source, name);
        Object.defineProperty(destination, name, descriptor);
      });
    });
    return this;
  }
});

/*
Misc
// max size in bytes for capped collection
size: 100000,
// max number of documents inside of capped collection
num: null,
// 
// optionally you can pass everything separately
host: 'localhost',
port: 27017,
db: 'socketio'


Not applicable
*     - redisPub (object) options to pass to the pub redis client
*     - redisSub (object) options to pass to the sub redis client
*     - redisClient (object) options to pass to the general redis client
*/
function Mongo (opts) {
  var opts = {}.extend(Mongo.defaults, opts),
      self = this;
  
  // node id to uniquely identify this node
  var nodeId = opts.nodeId || function () {
    // by default, we generate a random id 
    return Math.abs(Math.random() * Math.random() * Date.now() | 0);
  };

  this.nodeId = nodeId();
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
  
  /*
  // initialize a pubsub client and a regular client
  if (opts.pub instanceof Channel) { // instanceof RedisClient 
    this.pub = opts.pub;
  } else {
    opts.pub || (opts.pub = {});
    this.pub = pubsub.channel(opts.collectionPrefix + opts.streamCollection, { size: 100000, max: 500 });
  }
  if (opts.sub instanceof Channel) {
    this.sub = opts.sub;
  } else {
    opts.sub || (opts.sub = {});
    this.sub = pubsub.channel(opts.collectionPrefix + opts.streamCollection, { size: 100000, max: 500 });
  }
  if (opts.client instanceof Channel) {
    this.cmd = opts.client;
  } else {
    opts.client || (opts.client = {});
    this.cmd = pubsub.channel(opts.collectionPrefix + opts.streamCollection, { size: 100000, max: 500 });
  }
  */
  this.pub = this.sub = this.cmd = mubsub.channel(opts.collectionPrefix + opts.streamCollection, { size: 100000, max: 500 });
  
  mubsub.connection.db.then(function(err, db) {
    self.emit('connect', err, db);
  });

  // all instances share one connection
  ++instances;
  if (!connected) {
    connected = true;
    mubsub.connect(opts);
  }
  
  Store.call(this, opts);

  //this.sub.setMaxListeners(0);
  this.setMaxListeners(0);
};

/**
 * Inherits from Store.
 */

Mongo.prototype.__proto__ = Store.prototype;

/**
 * Publishes a message.
 *
 * @api private
 */

Mongo.prototype.publish = function (name) {
  var args = [].slice.call(arguments, 1);
  this.pub.publish({ name: name/*, nodeId: {$ne: this.nodeId}*/, data: this.pack({ nodeId: this.nodeId, args: args }) }); //, this._error);
  this.emit.apply(this, ['publish', name].concat(args));
};

/**
 * Subscribes to a channel
 *
 * @api private
 */

Mongo.prototype.subscribe = function (name, consumer, fn) {
  //this.sub.subscribe(name);
  
  if (consumer || fn) {
    var self = this, subscription;
    self.subscriptions[name] = subscription = self.sub.subscribe({ name: name/*, nodeId: {$ne: this.nodeId}*/ }, function(err, doc) {
      if(err) throw err;
      if (name == doc.name) {
        var msg = self.unpack(doc.data);
        // we check that the message consumed wasnt emitted by this node
        if (self.nodeId != msg.nodeId) {
          consumer.apply(null, msg.args);
          //subscription.unsubscribe();
        }
      }
      
      // self.sub.removeListener('subscribe', subscribe);

      fn && fn();
      //}
    });
  }

  this.emit('subscribe', name, consumer, fn);

};

/**
 * Unsubscribes
 *
 * @api private
 */

Mongo.prototype.unsubscribe = function (name, fn) {
  //this.sub.unsubscribe(name);
  if (this.subscriptions[name]) {
      this.subscriptions[name].unsubscribe();
  }

  fn && fn();
  //var client = this.sub;
  //client.on('unsubscribe', function (ch) {
  //  if (name == ch) {
  //    fn();
  //    this.unsubscribe();
  //  }
  //});

  this.emit('unsubscribe', name, fn);
};

/**
 * Unsubscribes all
 *
 * @api private
 */

Mongo.prototype.unsubscribeAll = function (){
  // Unsubscribing all events
  for(var subscriptionName in this.subscriptions){
    this.subscriptions[subscriptionName].unsubscribe();
  }
  this.subscriptions = {};
}

/**
 * Destroys the store
 *
 * @api public
 */

Mongo.prototype.destroy = function () {
  
  Store.prototype.destroy.call(this);
  
  this.unsubscribeAll();
  
  this.pub.close();
  this.sub.close();
  this.cmd.close();
  
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
 * @param {Function} callback.
 * @api protected
 */
Mongo.prototype.getPersistentCollection = function(callback) {
  var self = this,
      opts = this.options;
  
  if (self.persistentCollection) {
    return callback && callback(null, self.persistentCollection);
  }
  mubsub.connection.db.then(function(err, db) {
    var name = opts.collectionPrefix + opts.storageCollection;

    if (err) {
      return callback && callback(err);
    }

    db.collection(name, function(err, collection) {
      if (err) {
        return callback && callback(err);
      }

      self.persistentCollection = collection;
      callback(null, self.persistentCollection);
    });
  });
//  
//  return this;
};


/**
 * Client constructor
 *
 * @api private
 */

function Client (store, id) {
  Store.Client.call(this, store, id);
};

/**
 * Inherits from Store.Client
 */

Client.prototype.__proto__ = Store.Client;

/**
 * MongoDB doc find
 *
 * @api private
 */

Client.prototype.get = function (key, fn) {
  var self = this;
  self.store.getPersistentCollection(function(err, collection) {
    if (err) {
      return fn(err);
    }

    collection.findOne({_id: self.id + key}, function(err, data) {
      if (err) {
        return fn(err);
      }
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

  this.store.getPersistentCollection(function(err, collection) {
    if (err) {
      return fn && fn(err);
    }

    collection.update(
      {_id: self.id + key},
      {$set: {value: value, clientId: self.id}},
      {upsert: true},
      fn || function(){}
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

  this.store.getPersistentCollection(function(err, collection) {
    if (err) {
      return fn && fn(err);
    }

    collection.remove({_id: self.id + key}, function(err, data) {
      if (err) {
        return fn && fn(err);
      }
      fn && fn();
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

  this.store.getPersistentCollection(function(err, collection) {
      if (err) {
        return fn && fn(err);
      }

      collection.findOne({_id: self.id + key}, {fields: {_id: 1}}, function(err, data) {
        if (err) {
          return fn && fn(err);
        }

        fn && fn(null, Boolean(data));
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

  if (typeof expiration == 'number') {
    setTimeout(function() {
      self.destroy(null, fn);
    }, expiration * 1000);
    return this;
  }
  this.store.unsubscribeAll();
  this.store.getPersistentCollection(function(err, collection) {
    if (err) {
      return fn && fn(err);
    }
    collection.remove({clientId: self.id}, fn || function(){});
  });

  return this;

};