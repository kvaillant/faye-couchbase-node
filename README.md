# faye-couchbase-node
Add couchbase support for node-faye (based on faye-redid-node)

This plugin provides a Couchbase-based backend for the
[Faye](http://faye.jcoglan.com) messaging server. It allows a single Faye
service to be distributed across many front-end web servers by storing state and
routing messages through a [Couchbase](http://couchbase.com) database server.


## Usage

Pass in the engine and any settings you need when setting up your Faye server.

```js
var faye  = require('faye'),
    fayecb = require('faye-couchbase'),
    http  = require('http');

var server = http.createServer();

var bayeux = new faye.NodeAdapter({
  mount:    '/ws',
  timeout:  25,
  engine: {
    type:   fayecb,
    cluster:   'couchbase://cb.example.com',
    password:   'default',
    bucket:   'default',
    server_username:   'user',
    server_password:   'password',
    namespace:   '',
    // more options
  }
});

bayeux.attach(server);
server.listen(8000);
```

The full list of settings is as follows.

* <b>`cluster`</b> - Url of the couchbase instance + port
* <b>`password`</b> - bucket password
* <b>`bucket`</b> - bucket name, default is `default`
* <b>`server_username`</b> - Couchbase server username
* <b>`server_password`</b> - Couchbase server password
* <b>`namespace`</b> - prefix applied to all keys, default is `''`


## License

(The MIT License)

Copyright (c) 2015-2016 Karl Vaillant

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the 'Software'), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
