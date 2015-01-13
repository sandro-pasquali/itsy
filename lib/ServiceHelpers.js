module.exports = {
	topics : [],
	sendOrQueue : function(topic, fn) {
		if(this.topics[topic].loading) {
			return this.topics[topic].sendQueue.push(fn);
		}
		fn();
	},
	startLoading : function(topic) {
	
		//	#use is adding services (existing in packages) to #fulfill payloads
		//	#receive-d on (published to) this topic. Packages need to be loaded,
		//	tested, and so forth prior to being so bound.
		//
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
	//	Payloads are interesting for their "null" keys -- keys that are not
	//	fulfilled. These keys are what #fulfill targets. Within Itsy, "null"
	//	keys are understood to contain either true null, undefined(void 0), 
	//	or the empty string ("").  Other falsy values (0, false)
	//
	isNullProp : function(obj, key) {
		var pk = obj[key];
		if(	obj.hasOwnProperty(key) 
			&& (pk === "" || pk == null || pk === void 0)) {
			return true;
		}
		return false;
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
			identity, [
				'subscribe',
				sp[1],
				sp[2]
			].join(' ')
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
	},
	setDeathHandlers : function() {
		(function(die) {	
			process
				.on('exit', die)
				.on('SIGINT', die)
				.on('SIGTERM', die) 
		})(function() {
			Object.keys(this).forEach(function(s) {
				//s.available.kill();
			});
			process.exit(1);
		}.bind(this));
		
		process.on('uncaughtException', function(err) {
			throw err;
		});
	},
	setCompletionHandlers : function(publisher) {
		//	The receiver for messages being sent along a completion channel.
		//	#payload is the response by a service.
		//
		publisher.router.on('message', function(identity, payload) {
	
			payload = payload.toString('utf8');
	
			var id = payload.substring(0, payload.indexOf(' '));
			
			if(id === "SERVICE_READY") {
				return this.handleServiceReady(publisher.router, identity, payload);
			}
			
			if(id === "SERVICE_SUBSCRIBED") {
				return this.handleServiceSubscribed(payload);
			}
			
			var json = payload.substring(payload.indexOf('{'), payload.lastIndexOf('}') +1);
			var manifest = publisher.flightManifests[id];
			var nullsRemaining = false;
			var k;
	
			if(!manifest) {
				throw new Error('No manifest available for returned message', payload);
			}
	
			try {
				payload = JSON.parse(json);
			} finally {
				if(typeof payload !== 'object') {
					throw new Error('Unable to parse message payload on topic >> ' + id);
				}
			}
			
			Object.keys(manifest.payload).forEach(function(key) {
				if(this.isNullProp(manifest.payload, key)) {
					if(!this.isNullProp(payload, key)) {
						manifest.payload[key] = payload[key];
					} else {
						nullsRemaining = true;
					}
				}
			}.bind(this));
		
			if(nullsRemaining === false) {		
	
				delete publisher.flightManifests[id];	
				
				//	The callback here is a Promise resolver for the 
				//	Promise returned by publisher.send() (see api)
				//
				manifest.resolve(manifest.payload);
			}
		}.bind(this));
	}
}