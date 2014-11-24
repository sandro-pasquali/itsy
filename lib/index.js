var zmq = require('zmq');
var uuid = require('node-uuid');
var path = require('path');
var Promise = require('bluebird');
var winston = require('winston');

winston
	.add(winston.transports.File, { 
		filename: 'itsy.log' 
	})
	.remove(winston.transports.Console);

process.on('uncaughtException', function(err) {
	winston.log('uncaught', err)
})

var _completePort = 'tcp://127.0.0.1:25000';

//	Stores the references to flighted objects
//	@see 	Publisher#send
//	@see	_completeSub#on#message
//
var _flightManifests = {};

var _completePub = zmq.socket('pub')
var _completeSub = zmq.socket('sub');
_completePub.bindSync(_completePort);
_completeSub.connect(_completePort)

_completeSub.on('message', function(payload) {

	payload = payload.toString();

    var id = payload.substring(0, payload.indexOf(' '));
	var manifest = _flightManifests[id];

	if(!manifest) {
		//	Log this when in Optimization mode
		//
		return;
	}
	
	try {
		payload = JSON.parse(payload.substring(payload.indexOf('{')));
	} catch(e) {
		//	todo: Log when we receive a non-parseable response
		//
		return manifest.error(e);
	}
	
	var complete = true;
	var nullsRemaining = false;
	var k;
	
	manifest.nulls.forEach(function(key) {
		if(manifest.payload[key] === null) {
			if(payload[key] !== null) {
				manifest.payload[key] = payload[key];
			} else {
				nullsRemaining = true;
			}
		}
	});

	if(nullsRemaining === false) {

		//	Do something better here
		//
		//delete _flightManifests[id];
		
		_completeSub.unsubscribe(id);
		manifest.callback(manifest.payload);
	}
});
	
function Publisher(port) {

	this.port = port;
	this.pub = zmq.socket('pub');
	
	try {
		this.pub.bindSync(port);
	} catch(e) {
		throw e;
	}
}

Publisher.prototype.send = function(topic, obj) {
	return new Promise(function(resolve, reject) {
		if(typeof topic !== "string") {
			throw new TypeError('Must send topic String as first argument to Publisher.send()');
		}

		obj = obj instanceof Object ? obj : {};
		
		//	A unique identifier used as prefix when returning result
		//
		var completeChannel = uuid.v4();
		
		//	Collect any fields with null values in the sent packet and
		//	sort them alphabetically.
		//
		var nulls = Object.keys(obj).filter(function(k) {
			return obj[k] === null;
		}).sort();
		
		//	Need at least one...
		//
		if(nulls.length === 0) {
			nulls = ['_'];
		}
		
		nulls.forEach(function(key) {
			_flightManifests[completeChannel] = {
				callback : resolve,
				error : reject,
				payload : obj,
				nulls : nulls,
				start : Date.now()
			};
			
			_completeSub.subscribe(completeChannel);

			process.nextTick(function() { 
				this.pub.send([
					topic,
					key,
					JSON.stringify(obj),
					completeChannel
				].join(' '));
			}.bind(this));
		}.bind(this));
	}.bind(this));
};
	
function Subscriber(publisher, topic) {
	
	var port = publisher.port;
	
	this._prefixes = {};
	this._lastKeyGroup = null;
	
	if(!topic) {
		throw new Error('No topic sent to Subscriber.subscribe(topicHere)');
	}
	
	this.topic = topic;
	
	this.sub = zmq.socket('sub');
	this.sub.identity = 'subscriber_' + process.pid + '_' + uuid.v4();
	  
	this.sub.connect(port, function(err) {
		if(err) {
			throw new Error(err);
		}	
	});
	
	this.sub.on('message', function(response) {
		//	Response is a space-separated string. Split out the data.
		//  [0] = topic
		//	[n-1] = response payload
		//	[n] = completeChannel
		//	[1] = null key subscriber is listening on (optional)
		//
		var sp = response.toString().split(' ');

		this.runHandlers(sp.shift(), sp.shift(), sp.pop(), JSON.parse(sp.join(' ')));
		
	}.bind(this));
}

Subscriber.prototype.runHandlers = function(topic, key, completeChannel, payload) {
	
	var using = this._prefixes[key];

	//	No handlers. Nothing to do. Probably should log/report this
	//
	if(using.length === 0) {
		//	Report a non-handled receiver
		//
		return;
	}
	
	var run = function(resolvedValue, handlers, resolve) {
		var fn;
		if(fn = handlers.shift()) {
			fn.call(this, function(res) {
				run(res, handlers, resolve);
			})
		} else {
			resolve(resolvedValue);
		}
	}.bind(payload);
	
	run(null, using.slice(), function(val) {
		this[key] = val;
		_completePub.send(completeChannel + ' ' + JSON.stringify(this));
	}.bind(payload));
};
	
Subscriber.prototype.close = function() {
	this.sub.close();
	return this;
};
	
Subscriber.prototype.pause = function() {
	return this;
};
	
Subscriber.prototype.resume = function() {
	return this;
};
	
Subscriber.prototype.fulfill = function(nullField) {

	var key = nullField || '_';
	
	this._lastKeyGroup = key;
	
	if(this._prefixes[key]) {
		return this;
	}
	
	this._prefixes[key] = [];
	this.sub.subscribe(this.topic + ' ' + key);
	
	return this;
};
	
Subscriber.prototype.use = function(fn) {
	//	a path to a MODULE, or a function
	if(typeof fn === "string") {
		fn = require(path.resolve(fn));
		if(typeof fn !== 'function') {
			throw new Error('String sent to Subscriber.using() not a valid package path, or non-function exported by package. Received: ' + fn);
		}
	} else if(typeof fn !== "function") {
		throw new Error('Must send a String or Function to Subscriber.using(). Received: ' + (typeof fn));
	}
	
	if(!this._lastKeyGroup) {
		throw new Error('No Subscriber.fulfill(key) statement to work from. Subscriber.using() unable to bind to key group.');
	}
	
	this._prefixes[this._lastKeyGroup].push(fn);
	
	return this;
};

module.exports = function(port) {
	var publisher = new Publisher(port);
	publisher.identity = 'publisher_' + process.pid + '_' + uuid.v4();
	var api = {
		receive: function(serviceName) {
			return new Subscriber(publisher, serviceName)
		},
		send: function(serviceName, data, cb) {
			
			return publisher.send(serviceName, data);
		},
		profile: winston.profile
	};
	return api;
}
