# Testing

## Quick Start

### Install Prerequisites

#### Host System

Your host system requires the following host-level dependencies:

- Docker
- Python
- Java
- wget
- set
- pipenv

##### macOS

> TODO

##### Linux

> TODO

##### Windows

> TODO


#### Testing Binaries

The tests themselves rely upon executable programs being available. Some of these are java "jar" files  (and require java to run), others are directly executable binaries.

#### Included in Repo

The KBase services "workspace" and "auth" are required by integration-level tests. These services are run and managed directly by the tests themselves. They are provided within the repo as simple jar files, located in the `test` directory:

- `test`
  - `authjars`
  - `wsjars`

#### Installation Required

Finally, some test dependencies require manual installation and setup. These tasks include:

- ensure that host level dependencies are present
- install mongo binary
- install kbase "jars" (required for auth and ws services)
- install and populate test configuration
- install python dependencies

Fortunately, a single make task takes care of this

```shell
make setup-test-dependencies
```

### Run Tests

```shell
make test
```

Jeeze, that was hard!

Behind the scenes, `make test` runs four make tasks:

- `make host-start-test-services` starts arangodb, kafka, and zookeeper in the background
- `make wait-for-arango` monitors arangodb, continuing if it is detected, and failing if not after after 60s 
- `make test-sdkless` runs the tests
- `make host-stop-test-services` stops the test services started in the first step
    
## Common Functionality

> TODO
 
### pytest fixtures

> TODO

### Utilities

> TODO

### Constants

> TODO

## Test Data

> TODO

## Coverage

> TODO

### Reporting

> TODO
