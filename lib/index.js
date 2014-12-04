var zmq		= require('zmq');
var uuid	= require('node-uuid');
var path	= require('path');
var Promise	= require('bluebird');
var fs		= require('fs');
var Joi		= require('joi');
var util	= require('util');
var _		= require('lodash');
var pm2		= require('pm2');
var exec	= require('child_process').exec;
var fork	= require('child_process').fork;

var logger	= require('./logger');
var Server = require('./Server');
var ServiceFactory = require('./ServiceFactory');

logger.handleExceptions();

var CALLER_PATH = path.dirname(module.parent.filename);

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

var SERVICES = {
	topics : [],
	sendOrQueue : function(topic, fn) {
		if(this.topics[topic].loading) {
			return this.topics[topic].sendQueue.push(fn);
		}
		fn();
	},
	startLoading : function(modulePath, topic, key) {
	
		//	#use is adding services (existing in packages) to #fulfill payloads
		//	#receive-d on (published to) this topic. Packages need to be loaded,
		//	tested, and so forth prior to being so bound. SERVICES 
		if(!this.topics[topic]) {
			this.topics[topic] = {
				loading : 0,
				sendQueue : [],
				started : Date.now()
			}
		}
		this.topics[topic].loading++;
	},
	completeLoading : function(topic, key, pid) {
		var stt = this.topics[topic];
		if(--stt.loading === 0) {
			stt.sendQueue.forEach(function(f) {
				f();
			});
			stt.sendQueue = [];
		}
	},
	handleServiceReady : function(router, identity, payload) {
		
		//	id, topic, key, pid
		//
		var sp = payload.split(' ');
		
		//	Tell the service which channel to subscribe to.
		//	The service should then send back a SERVICE_SUBSCRIBED
		//	response (see below).
		//
		return router.send([
			identity, 
			'subscribe ' + sp[1] + ' ' + sp[2]
		]);	
	},
	handleServiceSubscribed : function(payload) {
		//	id, topic, key, pid
		//
		var sp = payload.split(' ');
		return this.completeLoading(sp[1], sp[2], sp[3]);	
	},
	isDuplicate : function(list, procName) {
		var i = 0;
		for(; i < list.length; i++) {
			if(list[i].name === procName) {
				return {
					duplicate: list[i]
				};
			}
		}
		return { duplicate: false };
	}
};

//	#value on init == default
//
var OPTIONS = {
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
		var threshold = OPTIONS.latencyThreshold.value;
		var now = Date.now();
		var aggressiveness = OPTIONS.gcAggressiveness.value || Infinity;
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
	publisher.router.on('message', function(identity, payload) {

		payload = payload.toString('utf8');

		var id = payload.substring(0, payload.indexOf(' '));
		
		if(id === "SERVICE_READY") {
			return SERVICES.handleServiceReady(publisher.router, identity, payload);
		}
		
		if(id === "SERVICE_SUBSCRIBED") {
			return SERVICES.handleServiceSubscribed(payload);
		}
		
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
function Publisher(port, routerSocket) {

	this.port = port;
	this.routerSocket = routerSocket;
	
	this.pub = zmq.socket('pub');
	
	try {
		this.pub.bindSync(port);
	} catch(e) {
		return logger.error('Unable to bind >> ' + port);
	}
	
	//	Stores the references to flighted objects
	//	@see 	Publisher#send
	//	@see	router#on#message
	//
	this.flightManifests = {};
	
	this.router = zmq.socket('router');
	this.router.bindSync(routerSocket)
	
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
		
		SERVICES.sendOrQueue(topic, _send);
		
	}.bind(this));
};
	
function Subscriber(socketConfig, topic, router) {
	
	this.socketConfig = socketConfig;
	
	//	Within execution of Subscriber methods (#fulfill, #use) we may
	//	need to communicate with the Publisher router endpoint.
	//
	this.dealer = zmq.socket('dealer').connect(socketConfig.router);
	
	this.prefixes = {};
	this.lastKeyGroup = null;

	if(!topic) {
		throw new Error('No topic sent to Subscriber.subscribe(topicHere)');
	}
	
	this.topic = topic;
}

//	Assert that a #receive-r will fulfill field #nullField.
//	@example	itsy.receive('/translate')
//					.fulfill('french')
//					.use('/services/translateToFrench')
//
Subscriber.prototype.fulfill = function(nullField) {

	var key = nullField || '_';
	
	this.lastKeyGroup = key;
	
	if(this.prefixes[key]) {
		return this;
	}
	
	this.prefixes[key] = [];
	
	return this;
};

//	#use a service to help #fulfill a call to #receive
//	@example	itsy.receive('/translate')
//					.fulfill('french')
//					.use('/services/translateToFrench')
//
Subscriber.prototype.use = function(servicePath) {

	var self = this;
	
	if(!self.lastKeyGroup) {
		throw new Error('No Subscriber.fulfill(key) statement to work from. Subscriber.using() unable to bind to key group.');
	}
	
	//	A path to an npm package
	//
	if(typeof servicePath !== "string") {
		throw new Error('Must send a service module path (as String) to Subscriber.using(). Received: ' + (typeof servicePath));
	}
	
	//	The value passed as the service path is resolved relative to the
	//	cwd of the script which require-d this package.
	//
	var mpath	= path.resolve(CALLER_PATH, servicePath);
	var lkg 	= self.lastKeyGroup;
	
	ServiceFactory({
		path : mpath
	})

	//	Identifies the unique route/key + service/path combination, which key
	//	is used to uniquely identify the process pm2 will spawn (see > pm2 list)
	//
	var procName = [
		self.topic,
		lkg
	].concat(mpath.split('/').splice(-4)).join('/');
	
	SERVICES.startLoading(mpath, self.topic, lkg);

	exec('(cd ' + mpath + '; npm i; npm test)', function(error, stdout) {
		if(error) {
			return logger.error('SERVICE_TEST_FAILURE ' + mpath + ' >> ' + error);
		} 
		
		//	Print test harness output (mocha reporter)
		//
		logger.info(stdout);

		pm2.connect(function(err) {
			pm2.list(function(err, list) {
			
				//	Ensure that unique route/key + service/path combinations
				//	are allotted only one process. Note that the same
				//	service/path can be used for different route/key combos.
				//
				//	e.g 	.receive('/foo/bar')		// unique route
				//			.fulfill('key')
				//			.use('services/baz')		// identical service
				//
				//			.receive('/foo/bar/baz')	// unique route
				//			.fulfill('key')
				//			.use('services/baz')		// identical service
				//
				//			.receive('/foo/bar/baz')	// same route
				//			.fulfill('key')
				//			.use('services/baz')		// identical service
				//	...will result in TWO spawned /services/baz processes, with
				//	the third group being ignored (/foo/bar/baz is already being
				//	handled by the correct process /servics/baz). Uniqueness of
				//	#key has the same effect.
				//
				var dup = SERVICES.isDuplicate(list, procName);
				if(dup.duplicate) {
					console.log("DUPLICATE: ", procName, dup.duplicate.pid);
					self.dealer.send('SERVICE_SUBSCRIBED ' + self.topic + ' ' + lkg + ' ' + dup.duplicate.pid);
					return;
				}
				pm2.start(mpath, { 
					name: procName,
					scriptArgs: [
						self.socketConfig.publisher,
						self.socketConfig.router,
						self.topic,
						lkg
					],
					watch : path.join(mpath, 'index.js'),
					instances: 1,
					execMode : 'fork_mode',
					force : true
				}, function(err, proc) {
					if(err) {
						throw new Error(err);
					}
				});
			})
		});
	});

	return self;
};

module.exports = function(opts) {

	if(typeof opts !== 'object' || !opts.publisher || !opts.router) {
		throw new Error("Invalid config sent to Itsy factory. Received " + util.inspect(opts, {depth: 1}));
	}

	var publisher = new Publisher(opts.publisher, opts.router);
	var server;

	return {
		receive : function(serviceName) {
			return new Subscriber(opts, serviceName, publisher.router)
		},
		send : function(serviceName, data) {
			if(!publisher) {
				return logger.error('No publisher defined. Unable to send on ' + serviceName);
			}
			return publisher.send(serviceName, data);
		},
		profile : function(k) {
			logger.profile(k);
		},
		set : function(k, v) {
			OPTIONS[k] && OPTIONS[k].schema.validate(v, function(err, value) {
				if(err) {
					return logger.error('Invalid value for option >> ' + k);
				}
				OPTIONS[k].value = v;
			});
		},
		get : function(k) {
			var opt = OPTIONS[k];
			if(opt) {
				return opt.value;
			}
			logger.error('Request for non-existent option >> ' + k);
		},
		serve : function(opts) {

			if(!publisher) {
				return logger.error('No publisher defined. Unable to create Server');
			}
			
			opts.host = opts.host || '127.0.0.1';
			opts.port =  opts.port || '8080';
			opts.publisher = publisher;
			opts.backlog = opts.backlog || 1024;
			opts.onReady = opts.onReady;

			Server(opts);
		}
	};
}