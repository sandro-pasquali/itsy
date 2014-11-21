var itsy = require('./lib');

var port = 'tcp://127.0.0.1:12345';

var publisher = new itsy.Publisher('serviceName', port);
var service = new itsy.Subscriber('serviceName', port);

service
	.fulfill('a', 'b')
	.using(function(cb) {
		this.a = "AAAAAA!";
		cb(this);
	})
	.using(function(cb) {
		this.b = "BBBBB!";
		cb(this);
	})

publisher.send({
	a : null,
	b : null,
	c : "aaaa",
	d : "bbbb"
}, function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
});