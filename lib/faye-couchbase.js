var async = require('neo-async');

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
    this._server.debug('Disconnect');
    this._couchbase.disconnect(); // Close connection
  },

  createClient: function (callback, context) {
    var clientId = this._server.generateId(), self = this;
    var sessionExpiry = 1.6 * this._server.timeout;
    self._server.debug('Client creation, sessionExpiry:  ?', sessionExpiry);
    this._couchbase.insert(this._ns + '/clients/' + clientId, true, { 'expiry': sessionExpiry }, function (error, res) {
      if (error) return self.createClient(callback, context);
      self._server.debug('Created new client ?', clientId);
      self.ping(clientId);
      self._server.trigger('handshake', clientId);
      callback.call(context, clientId);
    });
  },

  clientExists: function (clientId, callback, context) {
    var self = this;
    this._couchbase.get(this._ns + '/clients/' + clientId, function (error, result) {
      if (error) {
        self._server.error('Client exists [GET]:  ?,  ?', error, clientId);
      }
      callback.call(context, !error && result);
    });
  },

  destroyClient: function (clientId, callback, context) {
    var self = this;
    self._couchbase.get(self._ns + '/clients/' + clientId + '/channels', function (error, result) {
      if (error) {
        self._server.warn('Client destroy :  ?,  ?', error, clientId);
      }
      if (result && result.value) {
        var channels = result.value.channels || [];
        async.each(channels, function (channel, callbackEach) {
          self._couchbase.get(self._ns + '/channels' + channel, function (error, result) {
            if (error) {
              self._server.warn('Client destroyClient [GET]:  ?,  ?', error, clientId);
            }
            var dataClients = {
              clients: []
            };
            if (result && result.value) {
              dataClients.clients = result.value && Array.isArray(result.value.clients) && result.value.clients || [];
            }
            var indexC = dataClients.clients.indexOf(clientId);
            if (indexC !== -1) {
              dataClients.clients.splice(indexC, 1);
              self._couchbase.upsert(self._ns + '/channels' + channel, dataClients, { 'expiry': self._gc }, function (error, result) {
                if (error) {
                  self._server.error('Client destroyClient [UPSERT]:  ?,  ?,  ?,  ?', error, clientId, channel, dataClients.clients);
                }
                if (dataClients.clients.length === 0) {
                  self._couchbase.remove(self._ns + '/channels' + channel, function (error, result) {
                    if (error) {
                      self._server.error('Client destroyClient [REMOVE]:  ?,  ?,  ?', error, clientId, channel);
                    }
                    callbackEach();
                  });
                } else {
                  callbackEach();
                }
              });
            } else {
              if (dataClients.clients.length === 0) {
                self._couchbase.remove(self._ns + '/channels' + channel, function (error, result) {
                  if (error) {
                    self._server.error('Client destroyClient [REMOVE]:  ?,  ?,  ?', error, clientId, channel);
                  }
                  callbackEach();
                });
              } else {
                callbackEach();
              }
            }
          });
        }, function (err) {
          if (err) {
            self._server.error('Client destroyClient [EACH]:', err);
          }
          self._couchbase.remove(self._ns + '/clients/' + clientId + '/channels', function (error, result) {
            if (error) {
              self._server.error('Client destroyClient /clients/?/channels [REMOVE]:  ?', clientId, error);
            }
          });
          self._couchbase.remove(self._ns + '/clients/' + clientId + '/messages', function (error, result) {
            if (error) {
              self._server.error('Client destroyClient /clients/?/messages [REMOVE]:  ?', clientId, error);
            }
          });
          self._couchbase.remove(this._ns + '/clients/' + clientId, function (error, result) {
            if (error) {
              self._server.error('Client destroyClient /clients/? [REMOVE]:  ?', clientId, error);
            }
          });

          self._server.debug('Destroyed client ?', clientId);
          self._server.trigger('disconnect', clientId);

          if (callback) callback.call(context);
        });
      } else { // No result from channel list
        self._couchbase.remove(self._ns + '/clients/' + clientId + '/channels', function (error, result) {
          if (error) {
            self._server.error('Client destroyClient /clients/?/channels [REMOVE]:  ?', clientId, error);
          }
        });
        self._couchbase.remove(self._ns + '/clients/' + clientId + '/messages', function (error, result) {
          if (error) {
            self._server.error('Client destroyClient /clients/?/messages [REMOVE]:  ?', clientId, error);
          }
        });
        self._couchbase.remove(this._ns + '/clients/' + clientId, function (error, result) {
          if (error) {
            self._server.error('Client destroyClient /clients/? [REMOVE]:  ?', clientId, error);
          }
        });

        self._server.debug('Destroyed client ?', clientId);
        self._server.trigger('disconnect', clientId);

        if (callback) callback.call(context);
      }
    });
  },

  ping: function (clientId) {
    var self = this;
    var timeout = this._server.timeout;
    if (typeof timeout !== 'number') return;

    this._server.debug('Ping ?, ?', clientId, timeout);
    var sessionExpiry = 1.6 * timeout;
    this._couchbase.upsert(this._ns + '/clients/' + clientId, true, { 'expiry': sessionExpiry }, function (error, result) {
      if (error) {
        self._server.error('Client ping [UPSERT]:  ?,  ?', error, clientId);
      }
    });
  },

  subscribe: function (clientId, channel, callback, context) {
    var self = this;
    async.waterfall([
      function (callbackW) {
        self._couchbase.get(self._ns + '/clients/' + clientId + '/channels', function (error, result) {
          if (error) {
            self._server.warn('Client subscribe [GET]:  ?,  ?,  ?', error, clientId, channel);
          }
          var dataChannels = {
            channels: []
          };
          if (result && result.value) {
            dataChannels.channels = result.value && Array.isArray(result.value.channels) && result.value.channels || [];
          }
          if (dataChannels.channels.indexOf(channel) === -1) {
            dataChannels.channels.push(channel);
            self._couchbase.upsert(self._ns + '/clients/' + clientId + '/channels', dataChannels, { 'expiry': self._gc }, function (error, result) {
              if (error) {
                self._server.error('Client subscribe [UPSERT]:  ?,  ?,  ?,  ?', error, clientId, channel, dataChannels.channels);
              }
              if (!error) self._server.trigger('subscribe', clientId, channel);
              callbackW(null, 'done');
            });
          } else {
            self._server.trigger('subscribe', clientId, channel);
            callbackW(null, 'done');
          }
        });
      },
      function (arg, callbackW) {
        self._couchbase.get(self._ns + '/channels' + channel, function (error, result) {
          if (error) {
            self._server.warn('Client subscribe [GET]:  ?,  ?', error, channel);
          }
          var dataClients = {
            clients: []
          };
          if (result && result.value) {
            dataClients.clients = result.value && Array.isArray(result.value.clients) && result.value.clients || [];
          }
          if (dataClients.clients.indexOf(clientId) === -1) {
            dataClients.clients.push(clientId);
            self._couchbase.upsert(self._ns + '/channels' + channel, dataClients, { 'expiry': self._gc }, function (error, result) {
              if (error) {
                self._server.error('Client subscribe [UPSERT]:  ?,  ?,  ?', error, channel, dataClients.clients);
              }
              callbackW(null, 'done');
            });
          } else {
            callbackW(null, 'done');
          }
        });
      }
    ], function (err, result) {
      if (err) {
        self._server.error('Client subscribe [EACH]:  ?', err);
      }
      self._server.debug('Subscribed client ? to channel ?', clientId, channel);
      if (callback) callback.call(context);
    });
  },

  unsubscribe: function (clientId, channel, callback, context) {
    var self = this;

    async.waterfall([
      function (callbackW) {
        self._couchbase.get(self._ns + '/clients/' + clientId + '/channels', function (error, result) {
          if (error) {
            self._server.warn('Client unsubscribe [GET]:  ?,  ?,  ?', error, clientId, channel);
          }
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
              if (error) {
                self._server.error('Client unsubscribe [UPSERT]:  ?,  ?,  ?,  ?', error, clientId, channel, dataChannels.channels);
              }
              if (!error) self._server.trigger('unsubscribe', clientId, channel);
              callbackW(null, 'done');
            });
          } else {
            self._server.trigger('unsubscribe', clientId, channel);
            callbackW(null, 'done');
          }
        });
      },
      function (arg, callbackW) {
        self._couchbase.get(self._ns + '/channels' + channel, function (error, result) {
          if (error) {
            self._server.warn('Client unsubscribe [GET]:  ?,  ?,  ?', error, clientId, channel);
          }
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
              self._couchbase.remove(self._ns + '/channels' + channel, function (error, result) {
                if (error) {
                  self._server.warn('Client unsubscribe [DELETE]:  ?,  ?', error, channel);
                }
                callbackW(null, 'done');
              });
            } else {
              self._couchbase.upsert(self._ns + '/channels' + channel, dataClients, { 'expiry': self._gc }, function (error, result) {
                if (error) {
                  self._server.error('Client unsubscribe [UPSERT]:  ?,  ?,  ?', error, channel, dataClients.clients);
                }
                callbackW(null, 'done');
              });
            }
          } else {
            callbackW(null, 'done');
          }
        });
      }
    ], function (err, result) {
      if (err) {
        self._server.error('Client unsubscribe [EACH]:  ?', err);
      }
      self._server.debug('Unsubscribed client ? from channel ?', clientId, channel);
      if (callback) callback.call(context);
    });

  },

  publish: function (message, channels) {
    this._server.debug('Publishing message ?', message);

    var self = this,
      keys = channels.map(function (c) { return self._ns + '/channels' + c; });
    this._server.debug('Channel for message publication ?', channels);

    self._couchbase.getMulti(keys, function (error, result) {
      self._server.debug('Channels\' clients for message publication ?', result);
      if (result) {
        var clients = [];
        for (var i in result) {
          if (result[i] && result[i].value && Array.isArray(result[i].value.clients)) { // result may have property error instead of value in not exist
            clients = clients.concat(result[i].value.clients);
          }
        }
        async.each(clients, function (clientId, callback) {
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
              if (error) {
                self._server.error('Client publish [UPSERT]:  ?,  ?,  ?', error, message, channels);
              }
              self.clientExists(clientId, function (exists) {
                if (!exists) {
                  self._couchbase.remove(queue, function (error, result) {
                    if (error) {
                      self._server.warn('Client publish [DELETE]:  ?,  ?', error, queue);
                    }
                    callback();
                  });
                } else {
                  self.emptyQueue(clientId,callback);
                }
              });
            });
          });
        }, function (err) {
          self._server.trigger('publish', message.clientId, message.channel, message.data);
        });
      } else {
        self._server.warn('No channels for publish:  ?,  ?,  ?', result, message, channels);
        self._server.trigger('publish', message.clientId, message.channel, message.data);
      }
    });

  },

  emptyQueue: function (clientId, callback) {
    if (!this._server.hasConnection(clientId)) {
      if(typeof callback === 'function'){
        callback();
      }
      return;
    }
    var key = this._ns + '/clients/' + clientId + '/messages',
      self = this;

    self._couchbase.get(key, function (error, result) {
      if (error) {
        self._server.warn('Client emptyQueue [GET]:  ?', error);
      }
      var messages = result && result.value && result.value.messages || [];
      self._server.deliver(clientId, messages);
      if(typeof callback === 'function'){
        callback();
      }
    });
  }
};

module.exports = Engine;
