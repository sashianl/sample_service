# Testing Overview

### Dependent Services

Integration tests require several services which are normally available in a KBase deployment environment.

#### KBase Services

The KBase services `workspace` and `auth` are required by integration-level tests. These services are run and managed directly by the tests themselves. They are provided within the repo as simple jar files, located in the `test` directory:

- `test`
  - `bin`
    - `authjars`
    - `wsjars`

There is currently no formal procedure for replacing these files when their respective services are updated.

### Installation Required

Some test dependencies require installation and setup prior to testing. These tasks include:

- ensure that host level dependencies are present
- install kbase "jars" (required for auth and ws services)
- install and populate test configuration
- install python dependencies

Fortunately, a single make task takes care of this

```shell
make setup-test-dependencies
```

### Prerequisites

#### Host System

In order to run tests, the host system requires the following host-level dependencies:

- Docker
- Python
- Java
- wget
- pipenv

> There may be more requirements here, but I don't have a bare system to test on at the moment.

Prior to running tests, the test automation script will check to ensure the required programs are present. If not, an error will be printed to the terminal, and the tests terminated.

If you are missing dependencies, it may be useful to [consult the appropriate documentation](./installing-test-dependencies.md).


### Run Tests


Jeeze, that was hard!

Behind the scenes, `make test` runs four make tasks:

- `make host-start-test-services` starts arangodb, kafka, and zookeeper in the background
- `make wait-for-arango` monitors arangodb, continuing if it is detected, and failing if not after after 60s 
- `make test-sdkless` runs the tests
- `make host-stop-test-services` stops the test services started in the first step
