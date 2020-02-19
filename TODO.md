# Design
* What are samples called? SampleReplicates?
* Do we need replicate groups?
  * Case where samples are functionally subsamples but cannot be produced from the same sample
    as the experimental techniques don't allow it.

# Functionality
* Admin flags on ops
* List / find samples, possibly with...
* Search integration
* Logging
* Link data to samples
* Workspace @sample integration
  * If user has access to sample set, should have access to embedded samples (?)
* ACLs:
  * cache known good users
  * remove self from acls (read/write)

# Concerns:
* Searching for samples could get very expensive based on the queries.
* Searching for samples may be difficult as the metadata is embedded in the sample node
  document which limits the possible indexed queries to some extent.
  * Separating out the metadata documents means that traversals querying metadata would be
    more complicated or impossible.
* Linking data may be complicated depending on the constraints and features we want

# Testing
* flake8 and bandit on test-sdkless (generated code is poopy)
* When https://github.com/python/mypy/issues/6385 is implemented, ditch all the stupid 
  `__init__.py` files

# Misc
* The ~10 documents that have been written about this

# Obsolete
* Make kb-sdk test run in travis
  * full stack is tested without it so nevermind