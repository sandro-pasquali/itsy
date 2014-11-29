var chai = require('chai');

global.sinon = require('sinon');
global.service = require('../../');
global.path = require('path');

global.description = 'service ' + __dirname.replace('/test/helpers','')

chai.config.includeStack = true;

global.expect = chai.expect;
global.AssertionError = chai.AssertionError;
global.Assertion = chai.Assertion;
global.assert = chai.assert;