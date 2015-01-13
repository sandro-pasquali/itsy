var zmq		= require('zmq');
var path	= require('path');
var Promise	= require('bluebird');
var fs		= require('fs');
var Joi		= require('joi');
var util	= require('util');
var _		= require('lodash');
var pm2		= require('pm2');
var exec	= require('child_process').exec;
var fork	= require('child_process').fork;

var Server = require('./Server');
var Services = require('./Services');
var Helpers = require('./ServiceHelpers');

var CALLER_PATH = path.dirname(module.parent.filename);

Helpers.setDeathHandlers();

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

//	Creates a new publisher, setting up completion handlers.
//
function Publisher(port, routerSocket) {

	var sweep;
	
	this.port = port;
	this.routerSocket = routerSocket;
	this.lastId = 0;
		
	//	Stores the references to flighted objects
	//	@see 	Publisher#send
	//	@see	router#on#message
	//
	this.flightManifests = {};
	
	this.router = zmq.socket('router');
	this.pub = zmq.socket('pub');
	
	this.router.bindSync(routerSocket);
	this.pub.bindSync(port);
	
	Helpers.setCompletionHandlers(this);

	//	Periodically sweep #gcAgressiveness items
	//
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
		var completeChannel = '@@' + ++this.lastId;

		//	Collect any fields that are empty strings or nulls or undefined.
		//	Note how Boolean false and Number 0 are not caught by this.
		//
		var nulls = Object.keys(obj).filter(function(k) {
			return Helpers.isNullProp(obj, k);
		});
		
		//	Need at least one...
		//
		if(nulls.length === 0) {
			nulls = ['_'];
		}
		
		var _send = function() {
			nulls.forEach(function(key) {	
	
				//	If we are publishing a completion id as the topic it will be 
				//	prefixed with @@. In those cases we do not need to set up
				//	further completion listeners, as they will never be called.
				//	Otherwise we are publishing to the service listener pool
				//	which uses the completion id to return their results.
				//
				if(topic.indexOf('@@') !== 0) {
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
		
		Helpers.sendOrQueue(topic, _send);
		
	}.bind(this));
};
	
function Subscriber(socketConfig, topic, router) {
	
	this.socketConfig = socketConfig;
	
	//	Within execution of Subscriber methods (#fulfill, #use) we may
	//	need to communicate with the Publisher router endpoint.
	//
	this.dealer = zmq.socket('dealer').connect(socketConfig.routerUri);
	
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

	if(!~['string','function'].indexOf(typeof servicePath)) {
		throw new Error('Must send a service module path (as String) or a service Function to Subscriber.using(). Received: ' + (typeof servicePath));
	}
	
	//	The value passed as the service path is resolved relative to the
	//	cwd of the script which require-d this package.
	//
	var mpath	= path.resolve(CALLER_PATH, servicePath);
	var lkg 	= self.lastKeyGroup;
	
	Helpers.startLoading(self.topic);
	
	Services.build(mpath);

	exec('(cd ' + mpath + '; tar -zxf modules.tgz; rm -f modules.tgz; npm test)', function(error, stdout) {
		if(error) {
			throw new Error('SERVICE_TEST_FAILURE ' + mpath + ' >> ' + error);
		} 
		
		//	Primarily, this will contain service test results
		//
		console.log(stdout);

		pm2.connect(function(err) {
			pm2.list(function(err, list) {
			
				//	Identifies the unique route/key + service/path combination,
				//	which key is used to uniquely identify the process pm2 will
				//	spawn (see > pm2 list)
				//
				var procName = [
					self.topic,
					lkg
				].concat(mpath.split('/').splice(-4)).join('/');
			
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
				var dup = Helpers.isDuplicate(list, procName);
				if(dup.duplicate) {
					console.log("DUPLICATE: ", procName, dup.duplicate.pid);
					return self.dealer.send([
						'SERVICE_SUBSCRIBED',
						self.topic,
						lkg,
						dup.duplicate.pid
					].join(' '))
				}
				pm2.start(mpath, { 
					name: procName,
					scriptArgs: [
						self.socketConfig.publisherUri,
						self.socketConfig.routerUri,
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
	}).stdout.on('data', function(chunk) {
		console.log('.');
	});

	return self;
};

module.exports = function(opts) {

	if(typeof opts !== 'object' || !opts.publisherUri || !opts.routerUri) {
		throw new Error("Invalid config sent to Itsy factory. Received " + util.inspect(opts, {depth: 1}));
	}

	var publisher = new Publisher(opts.publisherUri, opts.routerUri);
	var server;

	var api = {
		receive : function(serviceName) {
			return new Subscriber(opts, serviceName, publisher.router)
		},
		send : function(serviceName, data) {
			if(!publisher) {
				throw new Error('No publisher defined. Unable to send on ' + serviceName);
			}
			return publisher.send(serviceName, data);
		},
		set : function(k, v) {
			OPTIONS[k] && OPTIONS[k].schema.validate(v, function(err, value) {
				if(err) {
					throw new Error('Invalid value for option >> ' + k);
				}
				OPTIONS[k].value = v;
			});
		},
		get : function(k) {
			var opt = OPTIONS[k];
			if(opt) {
				return opt.value;
			}
			throw new Error('Request for non-existent option >> ' + k);
		},
		serve : function(opts) {

			if(!publisher) {
				throw new Error('No publisher defined. Unable to create Server');
			}
			
			opts.host = opts.host || '127.0.0.1';
			opts.port =  opts.port || '8080';
			opts.publisher = publisher;
			opts.backlog = opts.backlog || 1024;
			opts.onReady = opts.onReady;

			Server(opts);
		}
	};
	
	return api;
}