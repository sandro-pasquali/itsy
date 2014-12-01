var qs		= require('querystring');
var Itsy 	= require('./');
var pm2		= require('pm2');


/*
var pm2 = require('pm2');


*/



function createServer(opts) {




// Connect or launch PM2
return pm2.connect(function(err) {

  // Start a script on the current folder
  pm2.start('./_httpServer.js', { 
  	name: 'testerino',
	host : opts.host || '127.0.0.1',
	port :  opts.port || '8080',
	publishTo : opts.publishTo || 'tcp://127.0.0.1:12345',
	backlog : opts.backlog || 1024
  
  }, function(err, proc) {
    if (err) throw new Error('err');

    // Get all processes running
    pm2.list(function(err, process_list) {
      console.log(process_list);

      // Disconnect to PM2
      pm2.disconnect(function() { process.exit(0) });
    });
  });
})

};

module.exports = createServer;