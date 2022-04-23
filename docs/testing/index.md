# Testing

- [Quick Start](./quick-start.md)
- [Installing Test Dependencies](./installing-test-dependencies.md)
- [Testing Services with Docker Compose](./testing-services-with-docker-compose.md)
- [MyPy Type Verification](./mypy.md)

Tests for the Sample Service are composed of mixed unit and integration tests and mypy type verification. Tests do not require configuration, but test running does have a few [host system prerequisites](./quick-start.md#prerequisites). The host machine running the tests should have a good amount of resources, because the integration tests require running a half dozen services. Some of these resources are run in docker containers, others are run directly on the host machine. All of them are automated and may be invoked via a single `make` task.


## OLD DOCS BELOW

The Sample Service requires ArangoDB 3.5.1+ with RocksDB as the storage engine.

If Kafka notifications are enabled, the Sample Service requires Kafka 2.5.0+.

To run tests, MongoDB 3.6+ and the KBase Jars file repo are also required. Kafka is always required to run tests.

Once the dependencies are installed, run:

```
pipenv install --dev
pipenv shell
make test-sdkless
```

`kb-sdk test` does not currently pass.

