test:
	node_modules/.bin/mocha --require should ./test/mongo
	
test_dep:
	node_modules/.bin/mocha --require should ./test/mubsub


.PHONY: test test_dep
