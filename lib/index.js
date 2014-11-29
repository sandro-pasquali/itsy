var zmq		= require('zmq');
var uuid	= require('node-uuid');
var path	= require('path');
var Promise	= require('bluebird');
var logger	= require('./logger');
var exec	= require('child_process').exec;
var fs		= require('fs');
var Joi		= require('joi');
var util	= require('util');
var _		= require('lodash');

logger.handleExceptions();

var IS_NULL_PROP = function(obj, key) {
	var pk = obj[key];
	if(	obj.hasOwnProperty(key) 
		&& (pk === "" || pk == null || pk === void 0)) {
		return true;
	}
	return false;
}

//	Creates a new publisher, setting up completion handlers.
//
function Publisher(port) {

	this.port = port;
	this.pub = zmq.socket('pub');

	this.identity = 'publisher_' + process.pid + '_' + port;

	//	#value on init == default
	//
	this.options = {
		//	Whether or not to log debug info to the console.
		//
		debug : {
			schema : Joi.boolean(),
			value : true
		},
		//	How long to wait for a flighted request to return.
		//
		latencyThreshold : {
			schema : Joi.number().min(100).max(5000),
			value : 1000
		},
		//	The number of keys in flighted map to be checked per sweep.
		//	Set to 0 for 100% agressiveness (clean all in one sweep)
		//
		gcAggressiveness : {
			schema : Joi.number().min(10),
			value : 100
		}
	};
	
	try {
		this.pub.bindSync(port);
	} catch(e) {
		return logger.error('Unable to bind >> ' + port);
	}
	
	//////////////////////////////////////////////////////////////////////
	//																	//
	//					Set up flighted request tracking				//
	//																	//
	//////////////////////////////////////////////////////////////////////
	//	Stores the references to flighted objects
	//	@see 	Publisher#send
	//	@see	completeSub#on#message
	//
	this.flightManifests = {};
	
	this.completeSub = zmq.socket('sub');
	this.completeSub.connect(this.port)

	//	The receiver for messages being sent along a completion channel.
	//	#payload is the response by a service.
	//
	this.completeSub.on('message', function(payload) {

		payload = payload.toString();

		var id = payload.substring(0, payload.indexOf(' '));
		var json = payload.substring(payload.indexOf('{'), payload.lastIndexOf('}') +1);
		var manifest = this.flightManifests[id];
		var nullsRemaining = false;
		var sweep;
		var k;

		if(!manifest) {
			return logger.error('No manifest available for returned message', payload);
		}

		try {
			payload = JSON.parse(json);
		} finally {
			if(typeof payload !== 'object') {
				return logger.error('Unable to parse message payload on topic >> ' + id);
			}
		}
		
		Object.keys(manifest.payload).forEach(function(key) {
			if(IS_NULL_PROP(manifest.payload, key)) {
				if(payload[key]) {
					manifest.payload[key] = payload[key];
				} else {
					nullsRemaining = true;
				}
			}
		});
	
		if(nullsRemaining === false) {		

			//	Do something more here
			//
			delete this.flightManifests[id];		
		
			//	Completion callbacks are one-time-only
			//
			this.completeSub.unsubscribe(id);
			
			//	The callback here is a Promise resolver for the 
			//	Promise returned by publisher.send() (see api)
			//
			manifest.callback(manifest.payload);
		}
	}.bind(this));
	
	//	Periodically sweep #gcAgressiveness items
	//
	(sweep = function() {
		var manifests = this.flightManifests;
		var threshold = this.options.latencyThreshold.value;
		var now = Date.now();
		var aggressiveness = this.options.gcAggressiveness.value || Infinity;
		var m;
		for(m in manifests) {
			if((now - manifests[m].start) > threshold) {
				console.log(">>>>>> EXPIRED >>>>>>>", m)
				console.log(manifests[m])
				delete manifests[m];
			}
			if(--aggressiveness < 0) {
				break;
			}
		}
		setTimeout(sweep, threshold);
	}.bind(this))();
};

Publisher.prototype.send = function(topic, obj) {
	return new Promise(function(resolve, reject) {
		if(typeof topic !== "string") {
			throw new TypeError('Must send topic String as first argument to Publisher.send()');
		}

		obj = obj instanceof Object ? obj : {};
		
		//	A unique identifier used as prefix when returning result
		//
		var completeChannel = uuid.v4();
		
		//	Validate UUID v4 
		//
		var isUUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

		//	Collect any fields that are empty strings or nulls or undefined.
		//	Note how Boolean false and Number 0 are not caught by this.
		//
		var nulls = Object.keys(obj).filter(function(k) {
			return IS_NULL_PROP(obj, k);
		});
		
		//	Need at least one...
		//
		if(nulls.length === 0) {
			nulls = ['_'];
		}

		nulls.forEach(function(key) {	

			//	If we are publishing a completion id as the topic it will be 
			//	a UUID(v4). In those cases we do not need to set up further
			//	completion listeners, as they will never be called. Otherwise
			//	we are publishing to the service listener pool, which uses
			//	the completion id to return their results.
			//
			if(!isUUID.test(topic)) {
				this.flightManifests[completeChannel] = {
					callback : resolve,
					payload : obj,
					start : Date.now()
				};
				this.completeSub.subscribe(completeChannel);
			} else {
				completeChannel = "";
			}

			//	Publish String. Any listeners on this topic/[key] will publish 
			//	their result on #completeChannel when they are done.
			//
			this.pub.send([
				topic,
				key,
				JSON.stringify(obj),
				completeChannel
			].join(' '));

		}.bind(this));
	}.bind(this));
};
	
function Subscriber(publisher, topic) {
	
	var port = publisher.port;

	this.publisher = publisher;
	
	this.prefixes = {};
	this.lastKeyGroup = null;
	this.isPaused = false;
	
	if(!topic) {
		throw new Error('No topic sent to Subscriber.subscribe(topicHere)');
	}
	
	this.topic = topic;
	
	this.sub = zmq.socket('sub');
	this.sub.identity = 'subscriber_' + process.pid + '_' + port;
	  
	this.sub.connect(port, function(err) {
		if(err) {
			throw new Error(err);
		}	
	});
	
	//	When this publisher publishes on a topic here is where the subscriber
	//	handles the message.
	//
	this.sub.on('message', function(response) {

		//	Response is a space-separated string. Split out the data.
		//  [0] = topic
		//	[n] = completeChannel
		//	[n-1] = response payload
		//	[1] = null key subscriber is listening on (optional)
		//
		var sp = response.toString().split(' ');

		var topic = sp.shift();
		var key = sp.shift();
		var completeChannel = sp.pop();
		var payload = JSON.parse(sp.join(' '));
		var publisher = this.publisher;
console.log("KEY:::: ", key)
		var using = this.prefixes[key];

		//	No handlers. Nothing to do. Probably should log/report this
		//
		if(using.length === 0) {
			//	Report a non-handled receiver
			//
			return;
		}

		//	Each #use handler can itself call services.
		//	TODO: deal with circular calls!!!!
		//
		payload.$send = function(_topic, _payload) {
		
			_payload = _payload || {};
			
			var toss = function() {
				throw new Error("Circular call via property #" + key + " in object >> " + util.inspect(_payload, {depth:1}) + " for topic " + _topic)	
			};
			
			//	This call will lead us back here (circular) if same 
			//	#topic AND one of:
			//		a) No null properties in #_payload and #use calling $send
			//		is part of an empty #fulfill() (key === "_")
			//		b) #_payload contains a null key that #use calling $send
			//		responds to as well.
			//	In either case, throw.
			//
			if(topic === _topic) {
				if(key === "_") {
					if(!_.findKey(_payload, function(k) {
						return IS_NULL_PROP(_payload, k);
					})) {
						toss();
					}
				} else if(IS_NULL_PROP(_payload, key)) {
					toss();
				}
			}
			return publisher.send(_topic, _payload);
		};
	
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
			publisher.pub.send(completeChannel + ' ' + JSON.stringify(this));
		}.bind(payload));
	
	}.bind(this));
}
	
Subscriber.prototype.close = function() {
	this.sub.close();
	return this;
};
	
Subscriber.prototype.pause = function() {
	this.isPaused = true;
	return this;
};
	
Subscriber.prototype.resume = function() {
	this.isPaused = false;
	return this;
};
	
Subscriber.prototype.fulfill = function(nullField) {

	var key = nullField || '_';
	
	this.lastKeyGroup = key;
	
	if(this.prefixes[key]) {
		return this;
	}
	
	this.prefixes[key] = [];
	this.sub.subscribe(this.topic + ' ' + key);
	
	return this;
};
	
Subscriber.prototype.use = function(fn) {

	var mpath;
	
	//	a path to a MODULE, or a function
	//
	if(typeof fn === "string") {

		fn = require(mpath = path.resolve(fn));
		
		if(typeof fn !== 'function') {
			throw new Error('String sent to Subscriber.using() not a valid package path, or non-function exported by package. Received: ' + fn);
		}
		
		fs.stat(mpath, function(err, stats) {
			if(stats.isDirectory()) {
				exec('(cd ' + mpath + '; npm i; npm test)', function(error, stdout) {
					if(error) {
						logger.error('SERVICE_TEST_FAILURE ' + mpath + ' >> ' + error);
					} else {
						logger.info('SERVICE_TEST_OK ' + mpath);
						console.log(stdout);
					}
				});
			} else {
				//	Services must be packaged
				//
				throw new Error('SERVICE_NOT_PACKAGED ' + mpath);
			}
		});
		
	} else if(typeof fn !== "function") {
		throw new Error('Must send a String or Function to Subscriber.using(). Received: ' + (typeof fn));
	}
	
	if(!this.lastKeyGroup) {
		throw new Error('No Subscriber.fulfill(key) statement to work from. Subscriber.using() unable to bind to key group.');
	}
	
	this.prefixes[this.lastKeyGroup].push(fn);
	
	return this;
};

module.exports = function(port) {

	var publisher = new Publisher(port);

	return {
		receive : function(serviceName) {
			return new Subscriber(publisher, serviceName)
		},
		send : function(serviceName, data) {
			return publisher.send(serviceName, data);
		},
		profile : function(k) {
			logger.profile(k);
		},
		set : function(k, v) {
			publisher.options[k] && publisher.options[k].schema.validate(v, function(err, value) {
				if(err) {
					return logger.error('Invalid value for option >> ' + k);
				}
				publisher.options[k].value = v;
			});
		},
		get : function(k) {
			var opt = publisher.options[k];
			if(opt) {
				return opt.value;
			}
			logger.error('Request for non-existent option >> ' + k);
		},
		http : function(opts) {
			opts = opts || {};
			host = opts.host || '127.0.0.1';
			port = opts.port || '8080';
			require('./httpServer.js')({
				host : host,
				port : port, 
				publisher : publisher,
				backlog : opts.backlog || 1024,
				onReady : opts.onReady
			});
		}	
	};
}