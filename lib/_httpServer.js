var http = require('http');
var util = require('util');

console.log('HIHIIHIHH')
console.log(util.inspect(process, {depth:10}));

process.exit(0);

var serverHandler = function(request, response) {
		
	//	Leading and trailing slashes are removed
	//
	var url = request.url.replace(/^\/+|\/+$/g,"");
	var body = "";

	if(url.indexOf('favicon.ico') === 0) {
		return response.end();
	}
	
	response.writeHeader(200, {
		"content-type" : "application/json"
	})

	request.on('data', function(chunk) {
		body += chunk;
	})
	
	request.on('end', function() {
		opts.publisher.send(url, qs.parse(body))
		.then(function(fulfilledObject) {
			response.end(JSON.stringify(fulfilledObject || {}));
		})
		.catch(function(error) {
			response.end(JSON.stringify({
				error : error
			}))
		})
	})
};

http.createServer(serverHandler).listen(opts.port, opts.host, opts.backlog);

