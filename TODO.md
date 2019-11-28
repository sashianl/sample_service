# Design
* What are samples called? SampleReplicates?

# Functionality
* Admin flags on ops

# Reliability
* Transactions in clustered arango are
  [not atomic](https://www.arangodb.com/docs/stable/transactions-limitations.html#atomicity).
  Currently the arango error will be passed on if the transaction fails, but that may leave the db
  in an inconsistent state.
  * Improvement: trap the error and roll back. Or just don't use transactions

# Documentation
* Document error codes
* note RocksDB is required for Arango
* minimum arango ver is 3.5.1
* how to run both types of tests
* document collections for server. Do not auto create as admins will want to specify shard count

# Testing
* Make kb-sdk test run in travis and test full stack.
* flake8 and bandit on test-sdkless (generated code is poopy)
* When https://github.com/python/mypy/issues/6385 is implemented, ditch all the stupid 
  `__init__.py` files

# Misc
* https://github.com/kbaseIncubator/samples/pull/1/files
* The ~10 documents that have been written about this
* compile html spec