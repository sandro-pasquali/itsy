var fs = require('fs-extra');
var Promise = require('bluebird');
var colors = require('colors');

module.exports = function(opts) {
	var path = opts.path;
	var stats;
	
	if(typeof path !== "string" || path === "") {
		throw new Error("Unable to create new service. Invalid #path > " + path);
	}
console.log(path);
	try { 
		stats = fs.statSync(path);
	} catch(e) {} 
	
	finally {
		//	Exists. All good.
		//
		if(stats) {
			return;
		} 
	}
	
	fs.copySync('./lib/serviceBlueprint', path);
	
	console.log(colors.green('New service created at path >'), colors.blue(path));
	
}