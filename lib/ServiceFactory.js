var fs = require('fs-extra');
var Promise = require('bluebird');
var colors = require('colors');

module.exports = function(mpath) {

	var stats;
	
	if(typeof mpath !== "string" || mpath === "") {
		throw new Error("ServiceFactory unable to create new service. Invalid path > " + mpath);
	}

	try { 
		stats = fs.statSync(mpath);
	} catch(e) {} 
	

	if(stats) {
		return;
	} 
	
	fs.copySync('./lib/serviceBlueprint', mpath);

	console.log(colors.green('New service created at path >'), colors.blue(mpath));
}