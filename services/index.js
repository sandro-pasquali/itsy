var zmq = require('zmq');

module.exports = function(main) {

	if(!process.send) {
		return main;
	}
	
	var sub = zmq.socket('sub');
	var dealer = zmq.socket('dealer');
	
	var publisher = process.argv[2];
	var router = process.argv[3];
	var topic = process.argv[4];
	var key = process.argv[5];

	dealer.connect(router);
	
	sub.connect(publisher);
	sub.subscribe(topic + ' ' + key);
	sub.on('message', function(response) {

		//	Response is a space-separated string. Split out the data.
		//  [0] = topic
		//	[n] = completeChannel
		//	[n-1] = response payload
		//	[1] = null key subscriber is listening on (optional)
		//
		var sp = response.toString().split(' ');
	
		var completeChannel = sp.pop();
		var payload = JSON.parse(sp.pop());
		
		//	ADD #SEND method here. Use dealer to tell publisher to send some 
		//	message, passing some value for the publisher router to send back
		//	here (replicating the Promise interface of Itsy)
		
		main.call(payload, function(resolvedValue) {
			payload[key] = resolvedValue;
			dealer.send(completeChannel +' '+ JSON.stringify(payload));
		})
	});
	
	process.send(process.pid);
};