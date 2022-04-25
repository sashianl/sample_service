# Testing

Tests for the Sample Service are composed of mixed unit and integration tests and mypy type verification. Tests do not require configuration, but test running does have a few [host system prerequisites](./quick-start.md#prerequisites). The host machine running the tests should have a good amount of resources available (RAM, cores), because the integration tests require running a half dozen running services. Some of these resources are run in docker containers, others are run directly on the host machine. All of them are automated and may be invoked via a single `make` task.

- [Quick Start](./quick-start.md)
- [Test Dependencies](./dependencies.md)
- [Running Required Services with Docker Compose](./testing-services-with-docker-compose.md)
- [Test Coverage](./coverage.md)
- [MyPy Type Verification](./mypy.md)
- [Troubleshooting](./troubleshooting.md)

> Note: Although this service is based on [kb_sdk](https://github.com/kbase/kb_sdk), tests are not run by `kb-sdk`.
