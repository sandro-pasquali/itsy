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

//
//	MONGO
//

itsy.receive('config/get')
	.fulfill('values')
	.use('services/config/get')

itsy.send('config/get', {
	fields : [
		'firstname',
		'lastname'
	],
	values : null
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
})

itsy.receive('config/set')
	.fulfill()
	.use('services/config/set')

itsy.send('config/set', {
	fields : {
		firstname : "sandro",
		lastname : "pasquali"
	}
})
.then(function(fulfilledObject) {
	console.log("Fulfilled : ", fulfilledObject);
})

