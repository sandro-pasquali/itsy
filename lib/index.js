var zmq = require('zmq');
var uuid = require('node-uuid');
var path = require('path');

process.on('uncaughtException', function(err) {
	console.log("THREW: ", err);
	console.trace()
})

var _completePort = 'tcp://127.0.0.1:25000';

var _completionCallbacks = {};

var _completePub = zmq.socket('pub')
var _completeSub = zmq.socket('sub');
_completePub.bindSync(_completePort);
_completeSub.connect(_completePort)

_completeSub.on('message', function(payload) {

	payload = payload.toString();
	
    var id = payload.substring(0, payload.indexOf(' '));
	var cb = _completionCallbacks[id];
	
	payload = JSON.parse(payload.substring(payload.indexOf('{')));
	
	var complete = true;
	var nullsRemaining = false;
	var k;
	
	cb.nulls.forEach(function(key) {
		if(cb.payload[key] === null) {
			if(payload[key] !== null) {
				cb.payload[key] = payload[key];
			} else {
				nullsRemaining = true;
			}
		}
	});

	if(!nullsRemaining) {
		delete _completionCallbacks[id];
		_completeSub.unsubscribe(id);
		cb.callback(cb.payload);
	}
});
	
function Publisher(topic, port) {

	var binding;

	this.pub = zmq.socket('pub');
	
	//try {
		binding = this.pub.bindSync(port);
	//} finally {
		//if(!binding) {
		//	return;
		//}
	//}
	
	this.send = function(obj, cb) {
	
		obj = obj instanceof Object ? obj : {};
		
		//	An identifier that we use to return a final response.
		//
		var completeChannel = uuid.v4();
		
		var sig = Object.keys(obj).filter(function(k) {
			return obj[k] === null;
		}).sort();
		
		var nulls = sig.slice();
		
		sig.unshift(topic);
		sig.push(JSON.stringify(obj));
		sig.push(completeChannel);
		
		_completionCallbacks[completeChannel] = {
			callback : cb || function() {},
			payload : obj,
			nulls : nulls,
			start : Date.now()
		};
		_completeSub.subscribe(completeChannel);
		
		this.pub.send(sig.join(' '));
	}
}
	
function Subscriber(topic, port) {
	
	var prefixes = {};
	var using = [];
	
	if(!topic) {
		throw new Error('No topic sent to Subscriber.subscribe(topicHere)');
	}
	
	this.sub = zmq.socket('sub');
	  
	this.sub.connect(port, function(err) {
		if(err) {
			throw new Error(err);
		}	
	});
	
	function runHandlers(payload, topic, keys, completeChannel) {

		var completed = false;
		var len = using.length;
		
		//	No handlers. Nothing to do.
		//
		if(len === 0) {
			reuturn;
		}
		
		using.forEach(function(fn) {
			fn.call(payload, function(updatedPayload) {

				_completePub.send(completeChannel + ' ' + JSON.stringify(updatedPayload));

			})
		})
	};
	
	this.sub.on('message', function(response) {
		//	Response is a space-separated string. Split out the data.
		//  [0] = service name
		//	[n-1] = response payload
		//	[n] = completeChannel
		//	[1...n-2] = null keys subscriber was listening on.
		//
		var sp = response.toString().split(' ');
		
		var completeChannel = sp.pop();
		
		//	What remains are null keys
		//
		runHandlers(JSON.parse(sp.pop()), sp.shift(), sp, completeChannel);
	});
	
	this.close = function() {
		this.sub.close();
		return this;
	};
	
	this.pause = function() {
		return this;
	};
	
	this.resume = function() {
		return this;
	};
	
	this.fulfill = function() {

		var fields = Array.prototype.slice.call(arguments);
		
		var prefix = topic + fields.reduce(function(prev, cur) {
			return prev + ' ' + cur;
		}, "");
		
		if(prefixes[prefix]) {
			return this;
		}
		
		prefixes[prefix] = 1;

		this.sub.subscribe(prefix);
		
		return this;
	};
	
	this.using = function(fn) {
		//	a path to a MODULE, or a function
		if(typeof fn === "string") {
			fn = require(path.resolve(fn));
			if(typeof fn !== 'function') {
				throw new Error('String sent to Subscriber.using() not a valid module path, or non-function exported. Received: ' + fn);
			}
		} else if(typeof fn !== "function") {
			throw new Error('Must send a String or Function to Subscriber.using(). Received: ' + (typeof fn));
		}
		
		using.push(fn);
		
		return this;
	};
}

module.exports.Subscriber = Subscriber;
module.exports.Publisher = Publisher;

