var winston = require('winston');
var fs = require('fs');

var _handleExceptions = winston.handleExceptions.bind(winston);
winston.handleExceptions = function() {
	_handleExceptions(new winston.transports.File({ 
		filename: './logs/exceptions.log' 
	}));
}

module.exports = winston;