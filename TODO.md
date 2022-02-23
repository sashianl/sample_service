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
  * cache known good users
  * cache user roles
  * remove self from acls (read/write)
  * change sample owner
    * Probably needs request / accept multistep flow
* Cache workspace reponses
* Stand alone validator CLI
  * Validate without sending data to server
* Versioning scheme for validator config
* Kafka events
  * Improve reliability
    * Currently if the service goes down between DB modification for a new link/sample and kafka
      reciept of the message the message is lost.
    * Could improve reliability of messaging by putting a `sent` field or something like that on
      samples / links in the DB, and not updating the field until the kafka send succeeds.
    * On startup, look for unsent, older messages and resend.
  * Tools to recreate events from the DB (backfill new external DBs, handle cases where
    Kafka messages were lost)
* Currently there's no way to list expired links other than setting an effective time on
  the from-sample search and from-data search
  * Maybe that's enough?
    * No way to see the history of the links
  * Listing expired links for a sample or data would require sort & paging, but it's not clear
    how to page. The obvious fields are the creation / expiration date, but since those are
    not necessarily unique, could return up to 10k documents. That makes paging smaller sizes
    problematic/impossible.
  * Listing expired links for a given sample *and* data is definitely possible since the links
    don't overlap in time and can be sorted / paged by the creation date.
      * Needs a sample/data/creation index.
* Lots of opportunities for performance improvements if neccessary (bulk reads [and writes,
  which are a lot harder])

# Concerns:
* Searching for samples could get very expensive based on the queries.
  * Maybe just using a Lucene based solution is the way to go.
* Searching for samples may be difficult as the metadata is embedded in the sample node
  document which limits the possible indexed queries to some extent.
  * Separating out the metadata documents means that traversals querying metadata would be
    more complicated or impossible.
  * Lucene as above
* If we want more features or constraints, linking data may get more complicated than is already is

# Testing
* flake8 and bandit on test-sdkless (generated code is poopy)
* When https://github.com/python/mypy/issues/6385 is implemented, ditch all the stupid 
  `__init__.py` files

# Misc
* The ~10 documents that have been written about samples

# Obsolete
* Make kb-sdk test run in travis
  * full stack is tested without it so nevermind