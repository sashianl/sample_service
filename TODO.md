# Design
* What are samples called? SampleReplicates?
* Do we need replicate groups?
  * Case where samples are functionally subsamples but cannot be produced from the same sample
    as the experimental techniques don't allow it.

# Functionality
* Admin flags on ops
* Search integration
* Logging

# Documentation
* Document error codes
* note RocksDB is required for Arango
* minimum arango ver is 3.5.1
* how to run both types of tests
* document collections for server. Do not auto create as admins will want to specify shard count

# Testing
* flake8 and bandit on test-sdkless (generated code is poopy)
* When https://github.com/python/mypy/issues/6385 is implemented, ditch all the stupid 
  `__init__.py` files

# Misc
* https://github.com/kbaseIncubator/samples/pull/1/files
* The ~10 documents that have been written about this
* compile html spec

# Obsolete
* Make kb-sdk test run in travis
  * full stack is tested without it so nevermind