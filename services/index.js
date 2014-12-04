var zmq = require('zmq');

module.exports = function(main) {

	if(!process.send) {
		return main;
	}
	
	var sub = zmq.socket('sub');
	var dealer = zmq.socket('dealer');
	
	var publisher = process.argv[2];
	var router = process.argv[3];
	var initTopic = process.argv[4];
	var initKey = process.argv[5];

	dealer.connect(router);
	sub.connect(publisher);
	
	var addSubscription = function(topic, key) {
		this.subscribe(topic + ' ' + key);
		this.on('message', function(response) {
	
			//	Response is a space-separated string. Split out the data.
			//  [0] = topic
			//	[n] = completeChannel
			//	[n-1] = response payload
			//	[1] = null key subscriber is listening on (optional)
			//
			var sp = response.toString().split(' ');
			var completeChannel = sp.pop();
			var topic = sp.shift();
			var key = sp.shift();
			var payload = JSON.parse(sp.join(' '));
			
			//	ADD #SEND method here. Use dealer to tell publisher to send some 
			//	message, passing some value for the publisher router to send back
			//	here (replicating the Promise interface of Itsy)
			
			main.call(payload, function(resolvedValue) {
				if(key && resolvedValue !== void 0) {
					payload[key] = resolvedValue;
				}
				dealer.send(completeChannel +' '+ JSON.stringify(payload));
			})
		});
	}.bind(sub);
	
	dealer.send('SERVICE_READY ' + initTopic + ' ' + initKey);
	
	//	The "RPC" gateway that router can use to communicate with 
	//	service processes.
	//
	dealer.on('message', function(msg) {
		msg = msg.toString('utf8');
		var args = msg.split(' ');
		switch(args[0]) {
			case 'subscribe':
				//	1:topic, 2:key
				//
				addSubscription(args[1], args[2]);
				dealer.send('SERVICE_SUBSCRIBED ' + args[1] + ' ' + args[2] + ' ' + process.pid);
			break;
			
			default:
			break;
		}
	});
};