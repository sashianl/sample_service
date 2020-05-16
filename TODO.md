# Design
* What are samples called? SampleReplicates?
* Do we need replicate groups?
  * Case where samples are functionally subsamples but cannot be produced from the same sample
    as the experimental techniques don't allow it.

# Functionality
* List / find samples, possibly with...
* Search integration
* Logging
* Workspace @sample integration
  * If user has access to sample set, should have access to embedded samples (?)
* ACLs:
  * change ACLs with more granularity. Right now it's replace all
  * cache known good users
  * cache user roles
  * remove self from acls (read/write)
  * change sample owner
    * Probably needs request / accept multistep flow
* Cache workspace reponses
* Stand alone validator CLI
  * Validate without sending data to server
* Versioning scheme for validator config
* Kafka events for create sample version, update ACLS, create link, expire link
  * get link by ID

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