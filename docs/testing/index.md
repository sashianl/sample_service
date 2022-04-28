# Setup and test

The Sample Service requires ArangoDB 3.5.1+ with RocksDB as the storage engine.

If Kafka notifications are enabled, the Sample Service requires Kafka 2.5.0+.

To run tests, MongoDB 3.6+ and the KBase Jars file repo are also required. Kafka is always required to run tests.

These dependencies will be installed (for macOS only), with `make test-setup`.

From start to finish, this is what it takes to run tests:

```
make test-setup
make test
```

Please note that `test-setup` will look for required host application dependencies. If you are missing any of the dependencies listed below, you will need to install them with your favorite package manager:

- python 3.7
- pipenv 
- wget
- java

> Note: `kb-sdk test` does not currently pass.

