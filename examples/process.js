var Promise = require('bluebird');
var Itsy = require('../lib');

//	Use modules for a clean organizational structure
//
var itsy = Itsy({
	publisher : 'tcp://127.0.0.1:12345',
	router : 'tcp://127.0.0.1:12346',
	onServiceReady : function(topic, key, pid) {
	
	},
	onServiceSubscribed : function(topic, key, pid) {
	
	},
	onServiceDuplicate : function(topic, key) {
	
	}
});

itsy.receive('both/a/and/b')
	.fulfill('a')
	.use('services/solveForA')
	.fulfill('b')
	.use('services/solveForB')

itsy.send('both/a/and/b', {
	a : null,
	b : null,
	c : "the first one"
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
	itsy.profile('foo');
	itsy.send('both/a/and/b', {
		a : null,
		b : null,
		c : "subprocess to first one"
	})
	.then(function(fulfilledObject) {
		console.log("Fulfilled : ", fulfilledObject);
		itsy.profile('foo');
	})
})

itsy.receive('another/a/and/b')
	.fulfill('a')
	.use('services/solveForA')
	.fulfill('b')
	.use('services/solveForB')

itsy.send('another/a/and/b', {
	a : null,
	b : null,
	c : "the other one"
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
})

