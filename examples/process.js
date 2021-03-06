var Promise = require('bluebird');
var Itsy = require('../lib');
var timer = require('microtimer');

//	Use modules for a clean organizational structure
//
var itsy = Itsy({
	publisherUri : 'tcp://127.0.0.1:12345',
	routerUri : 'tcp://127.0.0.1:12346',
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
	timer.set('tester');
	itsy.send('both/a/and/b', {
		a : null,
		b : null,
		c : "subprocess to first one"
	})
	.then(function(fulfilledObject) {
		console.log("Fulfilled : ", fulfilledObject);
		timer.get('tester');
	})
})