# Using Docker Compose for Local Development

Docker compose can be very useful as a development platform. It obviates the need to install the sample service dependencies directly on your host machine.

These dependencies include:

- arangodb
- 

PORT=5001 MOCK_DATASET_PATH=`pwd`/dev/data/SampleService make host-start-dev-server


## Requirements

Although this service is written in Python, in order to test it you only need `docker`, `make`, and an `sh`-compatible shell available on your host system.

The automation tools use `make` and `sh`, which configure and run docker containers via `docker-compose`. Test output is written to the console, and also produces html coverage reports

Dependencies:

- docker
- make
- sh

## Docker Images

The required docker images are noted in the `docker-compose.yml` file in the `dev/` directory of the repo.

The service images in the docker compose file should match those that are used in actual deployments (or perhaps the relationship is reversed - one should only deploy with versions that match the testing images.)

Generally, the required services are:

- ArangoDB 3.5.1+ with RocksDB as the storage engine
- Kafa 2.5.0+
- KBase Mock Services

## Starting the containers

```sh
make host-start-dev-server
```

This uses three environment variables set to default values.

where:

- `PORT` is the port to expose on your host machine; optional - defaults to 5000.

    On macOS should specify the port as other than 5000, as macOS Monterey uses it by default for AirPlay.

    E.g. `PORT=5001 make host-start-dev-server`

    Generally 5000 is a reserved port, but it is the KBase default for services.

- `MOCK_DATASET_PATH` is a filesystem path to the parent directory for data provided for the mock services server; optional, defaults to `"${PWD}/dev/data/SampleService"` which contains just a little bit of mock data as an example.

- `VALIDATION_SPEC_URL` is a url to the validation spec file; optional, defaults to the production validation specs `https://raw.githubusercontent.com/kbase/sample_service_validator_config/master/metadata_validation.yml`

    Note this can be directed to a local file (within the container) using the `file://`  protocol. (An example of this will be in future work.)

Any of these environment variables may set from the shell and will override defaults. 

Note that within `dev/docker-compose.yml` the environment variables are prefixed by `DC_`. The `scripts/dev-server-env.sh` script contains the default values, and sets up the `DC_` environment variables. This script is sourced within the `Makefile`.

## Stopping the containers

There are two associated make tasks `host-stop-dev-server` and `host-remove-dev-server` which will stop and remove the containers, respectively.

## Using the sample service in the container

To use the sample service in the container, simply invoke the api at `http://localhost:$PORT` where `$PORT` is either the default port of `5000` or the port set in the `PORT` environment variable.

E.g.

```sh
curl -X POST http://localhost:5001/ \
    -d '{
"version": "1.1",
"id": "123",
"method": "SampleService.status",
"params": []
}'
```

If you pipe that through `json_pp`:

```sh
curl -s -X POST http://localhost:5001/ \
    -d '{
"version": "1.1",
"id": "123",
"method": "SampleService.status",
"params": []
}' | json_pp
```

it should return something like:

```sh
{
    "version": "1.1",
    "result": [
        {
            "state": "OK",
            "message": "",
            "version": "0.1.0",
            "git_url": "git@github.com:kbase/sample_service.git",
            "git_commit_hash": "b362ec800344f7c527ace52d0cc0127d006a731c",
            "servertime": 1641516040447
        }
    ],
    "id": "123"
}
```

## Iterating with the container

At present you must stop and start the container with code changes. When docker compose is stopped with `[Control]C` only the sample service container is stopped. The entire suite must be stopped and removed before restarting, otherwise the startup script will attempt to re-initialize the database, and fail to start.

Future work will add auto-restart upon code code change.
