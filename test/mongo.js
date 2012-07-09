var chainer = require('chainer'),
    should = require('should'),
    Mongo = require("../lib/mongo");

function getInstance() {
  var store;
  store = new Mongo({
    url: process.env.MONGODB_URI || 'mongodb://localhost:27017/socketio_mongo'
  });
  store.on('error', console.error);
  return store;
}

// Always keep one instance
getInstance();

describe('Mongo', function(){
  describe('tests subscribing and unsubscribing', function(){
    var a, b, c;
    
    beforeEach(function(){
      a = getInstance();
      b = getInstance();
      c = getInstance();
    });

    afterEach(function(){
      a.destroy();
      b.destroy();
      c.destroy();
    });

    it('should only receive events not send by self', function(next){
      a.subscribe('myevent', function(arg) {
        arg.should.equal('foo');
        next();
      });
      
      a.publish('myevent', 'bar');
      b.publish('myevent', 'foo');
      
    });

    it('should receive subscribed event before unsubscribe', function(next){
      a.subscribe('myevent', function(arg) {
        arg.should.equal( 'foobar' );
        a.unsubscribe('myevent', function() {
          b.publish('myevent');
          next();
        });
      });
      b.publish('myevent', 'foobar');
    });
    
    it('should publish to multiple subscribers', function(next) {
      var messages = 0;
      function subscription(arg1, arg2, arg3) {
        arg1.should.equal( 'foo' );
        arg2.should.equal( 'bar' );
        arg3.should.equal( 'baz' );
        messages++;

        if (messages == 6) {
          messages.should.equal( 6 );
          next();
        }
      }
      a.subscribe('myevent', subscription);
      b.subscribe('myevent', subscription);
      c.subscribe('myevent', subscription);

      a.publish('myevent', 'foo', 'bar', 'baz');
      a.publish('myevent', 'foo', 'bar', 'baz');
      a.publish('myevent', 'foo', 'bar', 'baz');
    });
  });
  
  describe('test storing and retrieving data for a client', function() {
    var store, clientId, client;
    before(function(){
      store = getInstance();
      clientId = 'client-' + Date.now();
      client = store.client(clientId);
    });
    
    it('should have client id', function(){
      client.id.should.equal(clientId);
    })
    

    it('should set, without errors', function(next){
      client.set('a', 'b', function(err) {
        should.not.exist(err);
        next();
      });
    });

    it('should get correct values, without errors', function(next){
      client.get('a', function(err, val) {
        should.not.exist(err);
        val.should.equal('b');
        next();
      });
    });

    it('should has correct values, without errors', function(next){
      client.has('a', function(err, has) {
        should.not.exist(err);
        has.should.equal(true);
        next();
      });
    });

    it('should has `false`, without errors', function(next){
      client.has('b', function(err, has) {
        should.not.exist(err);
        has.should.equal(false);
        next();
      });
    });

    it('should del, without errors', function(next){
      client.del('a', function(err) {
        should.not.exist(err);
        next();
      });
    });

    it('should has `false` after del, without errors', function(next){
      client.has('a', function(err, has) {
        should.not.exist(err);
        has.should.equal(false);
        next();
      });
    });

    it('should set object, without errors', function(next){
      client.set('c', {a: 1}, function(err) {
        should.not.exist(err);
        next();
      });
    });

    it('should modify object, without errors', function(next){
      client.set('c', {a: 3}, function(err) {
        should.not.exist(err);
        next();
      });
    });

    it('should get modified object with correct values, without errors', function(next){
      client.get('c', function(err, val) {
        should.not.exist(err);
        val.should.eql( {a: 3} );
        next();
      });
    });

    after(function(){
      store.destroy();
    });
    
  });

  describe('test cleaning up clients data', function() {
    var chain, store, client1, client2;
    
    chain = chainer();
    store = getInstance();
    client1 = store.client('client1-' + Date.now());
    client2 = store.client('client2-' + Date.now());
  
    chain.add(function() {
      it('should set client1, without errors', function(){
        client1.set('a', 'b', function(err) {
          should.not.exist(err);
          chain.next();
        });
      });
    });
    chain.add(function() {
      it('should set client2, without errors', function(){
        client2.set('c', 'd', function(err) {
          should.not.exist(err);
          chain.next();
        });
      });
    });
    chain.add(function() {
      it('should has correct value in client1, without errors', function(){
        client1.has('a', function(err, has) {
          should.not.exist(err);
          has.should.equal(true);
          chain.next();
        });
      });
    });
    chain.add(function() {
      it('should should has correct value in client2, without errors', function(){
        client2.has('c', function(err, has) {
          should.not.exist(err);
          has.should.equal(true);
          chain.next();
        });
      });
    });
    chain.add(function() {
      it('should destroy and initiate new store', function(){
        store.destroy();
        store = getInstance();
        client1 = store.client(Date.now());
        client2 = store.client(Date.now() + 1);
        chain.next();
      });
    });
    chain.add(function() {
      it('should has correct value in client1 after destroy, without errors', function(){
        client1.has('a', function(err, has) {
          should.not.exist(err);
          has.should.equal(false);
          chain.next();
        });
      });
    });
    chain.add(function() {
      it('should has correct value in client2 after destroy', function(){
        client2.has('c', function(err, has) {
          should.not.exist(err);
          has.should.equal(false);
          chain.next();
        });
      });
    });
    chain.start();
  });

  describe('test cleaning up a particular client', function() {
    var chain, store, clientId1, clientId2, client1, client2;
    
    chain = chainer();
    store = getInstance()
    clientId1 = 'client1-' + Date.now();
    clientId2 = 'client2-' + Date.now();
    client1 = store.client(clientId1);
    client2 = store.client(clientId2);
    
    chain.add(function() {
      it('should set client1, without errors', function(){
        client1.set('a', 'b', function(err) {
          should.not.exist(err); // '');
          chain.next();
        });
      });
    });
    chain.add(function() {
      it('should set client2, without errors', function(){
        client2.set('c', 'd', function(err) {
          should.not.exist(err);
          chain.next();
        });
      });
    });
    chain.add(function() {
      it('should has correct value in client1, without errors', function(){
        client1.has('a', function(err, has) {
          should.not.exist(err);
          has.should.equal(true);
          chain.next();
        });
      });
    });
    chain.add(function() {
      it('should has correct value in client2, without errors', function(){
        client2.has('c', function(err, has) {
          should.not.exist(err);
          has.should.equal(true);
          chain.next();
        });
      });
    });
    chain.add(function() {
      it('should have client1 and client2 in clients', function(){
        store.clients.should.have.property(clientId1);
        store.clients.should.have.property(clientId2);

        store.destroyClient(clientId1);

        store.clients.should.not.have.property(clientId1);
        store.clients.should.have.property(clientId2);
        chain.next();
      });
    });
    chain.add(function() {
      it('shouldn\'t has a value in client1 after destroy, without errors', function(){
        client1.has('a', function(err, has) {
          should.not.exist(err);
          has.should.equal(false);
          chain.next();
        });
      });
    });
    chain.add(function() {
      store.destroy();
    });
    chain.start();
  });

  describe('test destroy expiration', function() {
    var store, clientId, client;
    chain = chainer();
    store = getInstance()
    clientId = 'client-' + Date.now();
    client = store.client(clientId);
    
    chain.add(function() {
      it('should set, without errors', function(){
        client.set('foo', 'bar', function(err) {
          should.not.exist(err);
          chain.next();
        });
      });
    });
    chain.add(function() {
      it('should destroy client with 1 second expiration', function(){
        store.destroyClient(clientId, 2);
        setTimeout(function(){
          chain.next()
        }, 1500);
      });
    });
    chain.add(function() {
      it('should get correct value, without errors', function(){
        client.get('foo', function(err, val) {
          should.not.exist(err);
          console.log(val);
          //val.should.equal('bar');
          setTimeout(function(){
            console.log(client.get('foo'));
            chain.next();
          }, 12000);
        });
      });
    });
    chain.add(function() {
      it('should get correct value after expiration, without errors', function(){
        client.get('foo', function(err, val) {
          should.not.exist(err);
          should.not.exist(val);
          chain.next();
        });
      });
    });

    chain.add(function() {
      store.destroy();
    });
    
    chain.start();
  });
  
});