# Setup and test

The Sample Service requires ArangoDB 3.5.1+ with RocksDB as the storage engine.

If Kafka notifications are enabled, the Sample Service requires Kafka 2.5.0+.

To run tests, MongoDB 3.6+ and the KBase Jars file repo are also required. Kafka is always
required to run tests.

See `.travis.yml` for an example of how to set up tests, including creating a `test.cfg` file
from the `test/test.cfg.example` file.

Once the dependencies are installed, run:

```
pipenv install --dev
pipenv shell
make test-sdkless
```

`kb-sdk test` does not currently pass.

