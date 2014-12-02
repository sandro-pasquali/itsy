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
var Server	= require('./Server');
var pm2		= require('pm2');
var fork	= require('child_process').fork;

logger.handleExceptions();

var GRACEFUL_DEATH = function() {
	Object.keys(SERVICES).forEach(function(s) {
		//s.available.kill();
	});
	process.exit(1);
}
				
process
	.on('exit', GRACEFUL_DEATH)
	.on('SIGINT', GRACEFUL_DEATH)
	.on('SIGTERM', GRACEFUL_DEATH) 

var SERVICES = {};

//	Validate UUID v4 
//
var IS_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function IS_NULL_PROP(obj, key) {
	var pk = obj[key];
	if(	obj.hasOwnProperty(key) 
		&& (pk === "" || pk == null || pk === void 0)) {
		return true;
	}
	return false;
}

function START_SWEEPING(publisher) {
	//	Periodically sweep #gcAgressiveness items
	//
	var sweep;
	(sweep = function() {
		var manifests = this.flightManifests;
		var threshold = this.options.latencyThreshold.value;
		var now = Date.now();
		var aggressiveness = this.options.gcAggressiveness.value || Infinity;
		var mid;
		var man;
		for(mid in manifests) {
			man = manifests[mid]
			if((now - man.start) > threshold) {

				man.reject('Expired payload : ' + util.inspect(man.payload, {depth:1}));
				
				delete manifests[mid];
			}
			if(--aggressiveness < 0) {
				break;
			}
		}
		setTimeout(sweep, threshold);
	}.bind(publisher))();
};

function SET_COMPLETION_HANDLER(publisher) {

	//	The receiver for messages being sent along a completion channel.
	//	#payload is the response by a service.
	//
	publisher.completeSub.on('message', function(pack, payload) {
		pack = pack.toString('hex');
		payload = payload.toString('utf8');

		var id = payload.substring(0, payload.indexOf(' '));
		var json = payload.substring(payload.indexOf('{'), payload.lastIndexOf('}') +1);
		var manifest = this.flightManifests[id];
		var nullsRemaining = false;
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

			delete this.flightManifests[id];	
			
			//	The callback here is a Promise resolver for the 
			//	Promise returned by publisher.send() (see api)
			//
			manifest.resolve(manifest.payload);
		}
	}.bind(publisher));
}

//	Creates a new publisher, setting up completion handlers.
//
function Publisher(port, router) {

	this.port = port;
	this.router = router;
	
	this.pub = zmq.socket('pub');

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
	
	//	Stores the references to flighted objects
	//	@see 	Publisher#send
	//	@see	completeSub#on#message
	//
	this.flightManifests = {};
	
	this.completeSub = zmq.socket('router');
	this.completeSub.bindSync(router)

	SET_COMPLETION_HANDLER(this);
	START_SWEEPING(this);
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
		
		var _send = function() {
			nulls.forEach(function(key) {	
	
				//	If we are publishing a completion id as the topic it will be 
				//	a UUID(v4). In those cases we do not need to set up further
				//	completion listeners, as they will never be called.
				//	Otherwise we are publishing to the service listener pool
				//	which uses the completion id to return their results.
				//
				if(!IS_UUID.test(topic)) {
					this.flightManifests[completeChannel] = {
						resolve : resolve,
						reject : reject,
						payload : obj,
						start : Date.now()
					};
				} else {
					completeChannel = "";
				}
				
				//	Publish String. Any listeners on this topic/[key] will
				//	publish their result on #completeChannel when they are done.
				//
				this.pub.send([
					topic,
					key,
					JSON.stringify(obj),
					completeChannel
				].join(' '));
			}.bind(this));
		}.bind(this);
		
		_send.displayName = [topic].concat(nulls).join('-');
		
		if(SERVICES[topic].loading) {
			return SERVICES[topic].sendQueue.push(_send);
		}
		_send();
		
	}.bind(this));
};
	
function Subscriber(publisher, topic) {
	
	var port = publisher.port;
	
	this.publisher = publisher;
	this.prefixes = {};
	this.lastKeyGroup = null;

	if(!topic) {
		throw new Error('No topic sent to Subscriber.subscribe(topicHere)');
	}
	
	this.topic = topic;
}
	
Subscriber.prototype.fulfill = function(nullField) {

	var key = nullField || '_';
	
	this.lastKeyGroup = key;
	
	if(this.prefixes[key]) {
		return this;
	}
	
	this.prefixes[key] = [];
	
	return this;
};
	
Subscriber.prototype.use = function(fn) {

	var mpath;
	
	if(!this.lastKeyGroup) {
		throw new Error('No Subscriber.fulfill(key) statement to work from. Subscriber.using() unable to bind to key group.');
	}

	var lkg = this.lastKeyGroup;
	
	//	a path to a MODULE, or a function
	//
	if(typeof fn === "string") {

		mpath = path.resolve(fn);
				
		var stats = fs.statSync(mpath);
		
		if(stats.isDirectory()) {
			
			if(!SERVICES[this.topic]) {
				SERVICES[this.topic] = {
					loading : 0,
					sendQueue : [],
					started : Date.now()
				}
			}
			
			SERVICES[this.topic].loading++;
			
			exec('(cd ' + mpath + '; npm i; npm test)', function(error, stdout) {
				if(error) {
					return logger.error('SERVICE_TEST_FAILURE ' + mpath + ' >> ' + error);
				} 
				
				//	Print test harness output
				//
				logger.info(stdout);

				pm2.connect(function(err) {
					pm2.start(mpath, { 
						name: mpath.split('/').splice(-3).join('/'),
						scriptArgs: [
							this.publisher.port,
							this.publisher.router,
							this.topic,
							lkg
						],
						watch : path.join(mpath, 'index.js'),
						instances: 1,
						execMode : 'fork_mode'
					}, function(err, proc) {
						if(err) {
							throw err;
						}
						
						var stt = SERVICES[this.topic];
				
						setTimeout(function(pid) {
				
							if(--stt.loading === 0) {
								stt.sendQueue.forEach(function(f) {
									console.log(f.displayName);
									f();
								});
								stt.sendQueue = [];
							}
							
						}.bind(this), 2000);	
					}.bind(this));
				}.bind(this))
			}.bind(this));
		} else {
			//	Services must be packaged
			//
			throw new Error('SERVICE_NOT_PACKAGED ' + mpath);
		}
		
	} else {
		throw new Error('Must send a service module path (as String) to Subscriber.using(). Received: ' + (typeof fn));
	}
	
	return this;
};

module.exports = function(opts) {

	if(typeof opts !== 'object' || !opts.publisher || !opts.router) {
		throw new Error("Invalid config set to Itsy factory. Received " + util.inspect(opts, {depth: 1}));
	}

	var publisher = new Publisher(opts.publisher, opts.router);
	var server;

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
		serve : function(opts) {

			opts.host = opts.host || '127.0.0.1';
			opts.port =  opts.port || '8080';
			opts.publisher = publisher;
			opts.backlog = opts.backlog || 1024;
			opts.onReady = opts.onReady;

			Server(opts);
		}
	};
}