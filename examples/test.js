var Promise = require('bluebird');
var Itsy = require('../lib');

var itsy = Itsy('tcp://127.0.0.1:12345');

itsy.receive('/some/route')

	//	Use more than one handler to fulfill a request.
	//	The second handler receives the response of the first
	//	handler.
	//
	.fulfill('a')
	.use(function(cb) {
		//	You can change properties of the call package -- it is a copy.
		//	This 'blackboard' is `this` in subsequent #use calls
		//
		this.useThisAsFinal = 'used as final';
		cb("aaaa");
	})
	.use(function(cb) {
		//	While you can always use <itsy instance>.send(), when you don't 
		//	have access to the instance ref within #use (such as when this 
		//	handler is exported by a module) use this.$send
		//
		this.$send('/some/route', {
			w: null,
			ww: 'woot'
		})
		.then(function(fulfilledObject) {
			console.log("Fulfilled :", fulfilledObject);
			cb(this.useThisAsFinal)
		}.bind(this))
	})
	.fulfill('w')
	.use(function(cb) {
		cb(".....WWW.....")
	})

itsy.profile('*test exec time*');
itsy.send('/some/route', {
	a : null,
	c : "aaaa",
	d : "bbbb"
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
	return new Promise(function(resolve, reject) {
		setTimeout(function() {
			resolve('eventually, this was resolved')
		}, 1000)
	})
})
.then(function(last) {
	console.log(last)
})
.then(function(res, boo) {
	//	#send returns a Promise, so the chain waits for its resolve.
	//	This make fulfillment chains a little easier to construct.
	//
	return itsy.send('/some/route', {
		a : null,
		c : "cccc",
		d : "dddd"
	})
})
.then(function(last) {
	console.log("A chained call result: ", last);
	itsy.profile('*test exec time*');
})
.catch(TypeError, function(err) {
	console.log("type", err);
})


itsy.receive('/some/route2')

	//	Fullfill any request with a null #a
	//
	.fulfill('a')
	.use(function(cb) {
		cb("This is the A result");
	})
	.fulfill('b')
	.use(function(cb) {
		cb("This is the B result");
	})	
		
	.fulfill('f')
	.use(function(cb) {
		cb("FFF-->F")
	})
	
	//	Fulfill any request passed to this topic
	//
	.fulfill()
	.use(function(cb) {
		cb("final result FOoOOOOOOOOOOoO");
	})

itsy.send('/some/route2', {
	a : null,
	b : null,
	f : null,
	c : "x xxx xx",
	d : "yyyyyy"
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
});

itsy.send('/some/route2', {
	a : null,
	c : "xxxxxx2",
	d : "yyyyyy2"
}).then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
});

itsy.send('/some/route2', {
	c : "xxxxxx2",
	d : "yyyyyy2"
}).then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
});


//	This will expire (never resolves as there is no matching service)
//
itsy.send('/frig', {
	wtf : "frig"
}).then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
}).catch(function(err) {
	console.log(err);
})
