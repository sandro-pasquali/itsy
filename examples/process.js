var Promise = require('bluebird');
var Itsy = require('../lib');

//	Use modules for a clean organizational structure
//
var itsy = Itsy({
	publisher : 'tcp://127.0.0.1:12345',
	router : 'tcp://127.0.0.1:12346'
});

itsy.receive('both/a/and/b')
	.fulfill('a')
	.use('services/solveForA')
	.fulfill('b')
	.use('services/solveForB')


itsy.send('both/a/and/b', {
	a : null,
	b : null,
	c : "cccccffffcc"
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
	itsy.profile('foo');
	itsy.send('both/a/and/b', {
		a : null,
		b : null,
		c : "cccccffffcc"
	})
	.then(function(fulfilledObject) {
		console.log("Fulfilled : ", fulfilledObject);
		itsy.profile('foo');
	})
})
