var Engine = function (server, options) {
  this._server = server;
  this._options = options || {};

  var couchbase = require('couchbase'),
    cluster = this._options.cluster || this.DEFAULT_CLUSTER,
    password = this._options.password || this.DEFAULT_PASSWORD,
    bucket = this._options.bucket || this.DEFAULT_BUCKET,
    gc = this._options.gc || this.DEFAULT_GC;

  this._ns = this._options.namespace || '';
  this._gc = gc < (60 * 60) ? gc : this.DEFAULT_GC;

  if (couchbase) {
    this._couchbase = new couchbase.Cluster(cluster).openBucket(bucket, password);
  }

  this._messageChannel = this._ns + '/notifications/messages';
  this._closeChannel = this._ns + '/notifications/close';

  var self = this;
  /*this._subscriber.subscribe(this._messageChannel);
  this._subscriber.subscribe(this._closeChannel);
  this._subscriber.on('message', function (topic, message) {
    if (topic === self._messageChannel) self.emptyQueue(message);
    if (topic === self._closeChannel) self._server.trigger('close', message);
  });
  */
};

Engine.create = function (server, options) {
  return new this(server, options);
};

Engine.prototype = {
  DEFAULT_CLUSTER: 'localhost',
  DEFAULT_PASSWORD: '',
  DEFAULT_BUCKET: 'default',
  DEFAULT_SESSION_EXPIRY: (60 * 60 * 1),
  DEFAULT_GC: 60,
  LOCK_TIMEOUT: 120,

  disconnect: function () {
    this._couchbase.disconnect(); // Close connection
  },

  createClient: function (callback, context) {
    var clientId = this._server.generateId(), self = this;
    var sessionExpiry = 1.6 * this._server.timeout;
    this._couchbase.insert(this._ns + '/clients/' + clientId, true, { 'expiry': sessionExpiry }, function (error, res) {
      //this._redis.zadd(this._ns + '/clients', 0, clientId, function (error, added) {
      if (error) return self.createClient(callback, context);
      self._server.debug('Created new client ?', clientId);
      self.ping(clientId);
      self._server.trigger('handshake', clientId);
      callback.call(context, clientId);
    });
  },

  clientExists: function (clientId, callback, context) {
    //var cutoff = new Date().getTime() - (1000 * 1.6 * this._server.timeout);
    this._couchbase.get(this._ns + '/clients/' + clientId, function (error, result) {
      callback.call(context, !error && result);
    });
  },

  destroyClient: function (clientId, callback, context) {
    var self = this;
    self._couchbase.get(self._ns + '/clients/' + clientId + '/channels', function (error, result) {
      if (result && result.value) {
        var channels = result.value.channels || [];
        channels.forEach(function (channel) { // Suppress client from channel clients list
          self._couchbase.get(self._ns + '/channels' + channel, function (error, result) {
            var dataClients = {
              clients: []
            };
            if (result && result.value) {
              dataClients.clients = result.value && Array.isArray(result.value.clients) && result.value.clients || [];
            }
            var indexC = dataClients.clients.indexOf(clientId);
            if (indexC !== -1) {
              dataClients.clients.splice(indexC, 1);
              self._couchbase.upsert(self._ns + '/channels' + channel, dataClients, { 'expiry': self._gc }, function(error, result){});
            }
            if (dataClients.clients.length === 0) {
              self._couchbase.remove(self._ns + '/channels' + channel, function(error, result){});
            }
          });
        });
      }
      self._couchbase.remove(self._ns + '/clients/' + clientId + '/channels', function(error, result){});
      self._couchbase.remove(self._ns + '/clients/' + clientId + '/messages', function(error, result){});
      self._couchbase.remove(this._ns + '/clients/' + clientId, function(error, result){});

      self._server.debug('Destroyed client ?', clientId);
      self._server.trigger('disconnect', clientId);

      if (callback) callback.call(context);
    });
    
    // ---

    this._redis.smembers(this._ns + '/clients/' + clientId + '/channels', function (error, channels) {
      var multi = self._redis.multi();

      multi.zadd(self._ns + '/clients', 0, clientId);

      channels.forEach(function (channel) {
        multi.srem(self._ns + '/clients/' + clientId + '/channels', channel);
        multi.srem(self._ns + '/channels' + channel, clientId);
      });
      multi.del(self._ns + '/clients/' + clientId + '/messages');
      multi.zrem(self._ns + '/clients', clientId);
      multi.publish(self._closeChannel, clientId);

      multi.exec(function (error, results) {
        channels.forEach(function (channel, i) {
          if (results[2 * i + 1] !== 1) return;
          self._server.trigger('unsubscribe', clientId, channel);
          self._server.debug('Unsubscribed client ? from channel ?', clientId, channel);
        });

        self._server.debug('Destroyed client ?', clientId);
        self._server.trigger('disconnect', clientId);

        if (callback) callback.call(context);
      });
    });
  },

  ping: function (clientId) {
    var timeout = this._server.timeout;
    if (typeof timeout !== 'number') return;

    this._server.debug('Ping ?, ?', clientId, timeout);
    var sessionExpiry = 1.6 * timeout;
    this._couchbase.upsert(this._ns + '/clients/' + clientId, true, { 'expiry': sessionExpiry }, function(error, result){});
  },

  subscribe: function (clientId, channel, callback, context) {
    var self = this;
    self._couchbase.get(self._ns + '/clients/' + clientId + '/channels', function (error, result) {
      var dataChannels = {
        channels: []
      };
      if (result && result.value) {
        dataChannels.channels = result.value && Array.isArray(result.value.channels) && result.value.channels || [];
      }
      if (dataChannels.channels.indexOf(channel) === -1) {
        dataChannels.channels.push(channel);
        self._couchbase.upsert(self._ns + '/clients/' + clientId + '/channels', dataChannels, { 'expiry': self._gc }, function (error, result) {
          //self._redis.sadd(self._ns + '/clients/' + clientId + '/channels', channel, function (error, added) {
          if (!error) self._server.trigger('subscribe', clientId, channel);
        });
      } else {
        self._server.trigger('subscribe', clientId, channel);
      }
      self._couchbase.get(self._ns + '/channels' + channel, function (error, result) {
        var dataClients = {
          clients: []
        };
        if (result && result.value) {
          dataClients.clients = result.value && Array.isArray(result.value.clients) && result.value.clients || [];
        }
        if (dataClients.clients.indexOf(clientId) === -1) {
          dataClients.clients.push(clientId);
          self._couchbase.upsert(self._ns + '/channels' + channel, dataClients, { 'expiry': self._gc }, function (error, result) {
            //self._redis.sadd(self._ns + '/channels' + channel, clientId, function () {
            self._server.debug('Subscribed client ? to channel ?', clientId, channel);
            if (callback) callback.call(context);
          });
        } else {
          self._server.debug('Subscribed client ? to channel ?', clientId, channel);
          if (callback) callback.call(context);
        }
      });
    });
  },

  unsubscribe: function (clientId, channel, callback, context) {
    var self = this;
    self._couchbase.get(self._ns + '/clients/' + clientId + '/channels', function (error, result) {
      var dataChannels = {
        channels: []
      };
      if (result && result.value) {
        dataChannels.channels = result.value && Array.isArray(result.value.channels) && result.value.channels || [];
      }
      var index = dataChannels.channels.indexOf(channel);
      if (index !== -1) {
        dataChannels.channels.splice(index, 1);
        self._couchbase.upsert(self._ns + '/clients/' + clientId + '/channels', dataChannels, { 'expiry': self._gc }, function (error, result) {
          //self._redis.sadd(self._ns + '/clients/' + clientId + '/channels', channel, function (error, added) {
          if (!error) self._server.trigger('unsubscribe', clientId, channel);
        });
      } else {
        self._server.trigger('unsubscribe', clientId, channel);
      }
      self._couchbase.get(self._ns + '/channels' + channel, function (error, result) {
        var dataClients = {
          clients: []
        };
        if (result && result.value) {
          dataClients.clients = result.value && Array.isArray(result.value.clients) && result.value.clients || [];
        }
        var indexC = dataClients.clients.indexOf(clientId);
        if (indexC !== -1) {
          dataClients.clients.splice(indexC, 1);
          if (dataClients.clients.length === 0) {
            self._couchbase.remove(self._ns + '/channels' + channel, function(error, result){});
          } else {
            self._couchbase.upsert(self._ns + '/channels' + channel, dataClients, { 'expiry': self._gc }, function (error, result) {
              //this._redis.sadd(this._ns + '/channels' + channel, clientId, function () {
              self._server.debug('Unsubscribed client ? from channel ?', clientId, channel);
              if (callback) callback.call(context);
            });
          }
        } else {
          self._server.debug('Unsubscribed client ? from channel ?', clientId, channel);
          if (callback) callback.call(context);
        }
      });
    });
  },

  publish: function (message, channels) {
    this._server.debug('Publishing message ?', message);

    var self = this,
      keys = channels.map(function (c) { return self._ns + '/channels' + c });

    self._couchbase.getMulti(keys, function (error, result) {
      if (!error && result) {
        var clients = [];
        for (var i in result) {
          clients.concat(result[i].value.clients);
        }

        clients.forEach(function (clientId) {
          var queue = self._ns + '/clients/' + clientId + '/messages';
          self._server.debug('Queueing for client ?: ?', clientId, message);

          self._couchbase.get(queue, function (error, result) {
            var dataMsg = {
              messages: []
            };
            if (result && result.value) {
              dataMsg.messages = result.value && Array.isArray(result.value.messages) && result.value.messages || [];
            }
            dataMsg.messages.push(message);
            self._couchbase.upsert(queue, dataMsg, { 'expiry': self._gc }, function (error, result) {
              self.emptyQueue(clientId);
            });
          });
        });
      } else {
        self._server.error('Error getting channels for publish', error, message, channels);
        return;
      }
    });

    this._server.trigger('publish', message.clientId, message.channel, message.data);
  },

  emptyQueue: function (clientId) {
    if (!this._server.hasConnection(clientId)) return;

    var key = this._ns + '/clients/' + clientId + '/messages',
      self = this;

    self._couchbase.get(key, function (error, result) {
      if (result && result.value) {
        var messages = result.value.messages || [];
        self._server.deliver(clientId, messages);
      } else {
        return;
      }
    });
  }
};

module.exports = Engine;
