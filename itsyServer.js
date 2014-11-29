var Itsy = require('./lib');

//	Use modules for a clean organizational structure
//
var itsy = Itsy('tcp://127.0.0.1:12356');

itsy.receive('both/a/and/b')
	.fulfill('a')
	.use('services/solveForA')
	.fulfill('b')
	.use('services/solveForB')
	

itsy.send('both/a/and/b', {
	a : null,
	b : null,
	c : "@@@@@@@",
	d : "$$$$$$$"
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
})

itsy.http({
	port : 8080,
	host : '69.41.161.46',
	backlog: 1024,
	onReady : function() {
		console.log('server ready');
	}
})

// curl --data "a=&b=&c=ccccccc" http://69.41.161.46:8080/both/a/and/b