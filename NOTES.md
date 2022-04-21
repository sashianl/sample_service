# NOTES on current testing setup

auth and workspace binaries provided by:

authjars, wsjars

This means that these are fixed versions, will not reflect changes in these services.

They also require a specific version range of java. Which ones?

Requires special "controller" files to bring the jars to life.

Requires hardcoded path and class in the workspace_controller.py

## Summary of Changes


### reorganize test directory

The test directory mixes test files and test utilities. While refactoring tests, and in general any time working on tests, it is much easier of test utilities and test specs are separated.

To this end, all tests are moved to test/lib/specs and test utilities to test/lib/test_support

Moving all test python code into test/lib creates a nice parallel to the top level lib directory, and makes python path setup easier to reason about.

### Rename test/core test/lib/test_support

The `core` top level directory/namespace is quite confusing in test imports, as there is also a `core` directory in the main source. The solution is to move all such utilities into the `test_support` directory/namespace. This required touching all test files which utilize test support utilities

### Move constants into ... `constants.py`  

Many tests repeat certain test data setup, such as the names of arangodb collections, arangodb username and password.

Moving all such values into a module dedicated to testing constants helps reduce this duplication, and the possibility of divergence in tests.

### Move binaries into test/bin

The wsjars and authjars service jars are provided with the repo, and located in `test/bin`.

The mongo and KBase jars are installed prior to tests in `test/bin/mongo` and `test/bin/jars`.

This cleans up the test directory.

One day, hopefully, the bin directory will no longer be required and disappear.

### make test works again

Whereas the previous make test was just an echo to say to run test-sdkless, it is now operational again. It is a container task for test setup, running, and cleanup.

### Remove controllers for arango, kafka

Since arangodb and kafka were previously used directly as binaries, they had "controller" classes to wrap them and provide some controlled access. When replacing these binaries with their equivalent containers, the controllers are no longer necessary. The separate clients are sufficient.

### 