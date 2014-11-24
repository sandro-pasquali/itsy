var Promise = require('bluebird');

var Itsy = require('./lib');
var itsy = Itsy('tcp://127.0.0.1:12345');
var itsy2 = Itsy('tcp://127.0.0.1:12356');

itsy.receive('/some/route')

	//	Use more than one handler to fulfill a request.
	//	The second handler receives the response of the first
	//	handler.
	//
	.fulfill('a')
	.use(function(cb) {
		//	You can change properties of the call package -- it is a copy.
		//
		this.useThisAsFinal = 123;
		cb("aaaa");
	})
	.use(function(cb) {
		cb(this.useThisAsFinal);
	})
	
itsy.receive('/some/route2')

	//	Fullfill any request with a null #a
	//
	.fulfill('a')
	.use(function(cb) {
		cb("final result 2");
	})
	
	//	Fulfill any request passed to this topic
	//
	.fulfill()
	.use(function(cb) {
		cb("final result FOoOOOOOOOOOOoO");
	})

itsy.profile('test');
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
		}, 2000)
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
	itsy.profile('test');
})
.catch(TypeError, function(err) {
	console.log("type", err);
})


itsy.send('/some/route2', {
	a : null,
	c : "x xxx xx",
	d : "yyyyyy"
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
});

itsy.send('/some/route2', {
	c : "xxxxxx2",
	d : "yyyyyy2"
}).then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
});






itsy2.receive('/some/route')
	.fulfill('a')
	.use(function(cb) {
		cb("aaaa@@@@");
	})
	.use(function(cb) {
		cb("final result");
	})

itsy2.send('/some/route', {
	a : null,
	c : "@@@@@@@",
	d : "$$$$$$$"
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
})

itsy2.send('/some/route', {
	a : null,
	c : "!!!!!!!!!!",
	d : "%%%%%%%%%"
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
})
