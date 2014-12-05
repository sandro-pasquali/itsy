describe(description, function() {
	it('exports a Function', function(done) {
		assert.equal('function', typeof service);
		done();
	}),
	it('returns a Boolean false value', function(done) {
		service.call({}, function(val) {
			expect(val).to.be.false;
			done();
		})
	})
});