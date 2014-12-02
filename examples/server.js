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
	
var port = process.argv[3] || 8000;
var host = process.argv[2] || '127.0.0.1';

itsy.send('both/a/and/b', {
	a : null,
	b : null,
	c : "ccccccc"
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
})

itsy.serve({
	port : port,
	host : host,
	backlog: 1024,
	onReady : function() {
		console.log('server ready');
	}
})

console.log('\n\n** Serving http://' + host + ':' + port);
console.log('Try > curl --data "a=&b=&c=ccccccc" http://' + host + ':' + port + '/both/a/and/b\n\n');