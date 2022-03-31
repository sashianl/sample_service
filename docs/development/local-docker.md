# Using Docker Compose for Local Development

Docker compose can be very useful as a development platform. It obviates the need to install the sample service dependencies directly on your host machine.

The Sample Service depends on the following services:

- arangodb
- kafka
- zookeeper
- KBase Workspace Services
- KBase Auth Service

## Requirements

Although this service is written in Python, in order to test it you only need `docker`, `make`, and an `sh`-compatible shell available on your host system.

The automation tools use `make` and `sh`, which configure and run docker containers via `docker-compose`. Test output is written to the console, and also produces html coverage reports

Dependencies:

- docker
- make
- sh

### Docker Resources

The `docker compose` configuration has some minimal resources which need to be configured in your version of Docker. On a modern machine, a fresh Docker installation should work fine out of the box.

It has been used with Docker assigned as little as 2G memory, 2 cpus, 1G swap, and 16G disk space (macOS - resource requirements may be even lower).

Note that this is with ArangoDB only lightly populated, and Java services with memory requirements lowered from their defaults. Real-world usage scenarios will require more resources to either operate or be performant, and the heap memory assigned to the Java services (kafka, zookeeper) may need to be increased (they are lowered from their default 1G to 256M).

A more performant and pleasant experience will be had with 4+G memory, 4+ cpus, 1G swap, and 20+G disk space.

Note that the memory resources assigned are in addition to other concurrent docker work you may be conducting.

## Docker Images

The required docker images are noted in the `docker-compose.yml` file in the `dev/` directory of the repo.

The service images in the docker compose file should match those that are used in actual deployments (or perhaps the relationship is reversed - one should only deploy with versions that match the testing images.)

Generally, the required services are:

- ArangoDB 3.5.1+ with RocksDB as the storage engine
- Kafka 2.5.0+
- Zookeeper latest (not sure why this isn't pinned)
- KBase Mock Services, which provides:
  - workspace
  - auth

## Starting the containers

```sh
make host-start-dev-server
```

This uses four environment variables set to default values.

where:

- `PORT` is the port to expose on your host machine; optional - defaults to 5006.

    The default exposed port is 5006, even though the internal port is 5000, the standard port for KBase services.

    This is so because macOS Monterey utilizes port 5000 for Airplay; generally there are many extant services, most of which would not interfere with KBase development, on port 5000, both [defacto](https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers) and [official](https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml?&page=88).

    Generally you should be prepare to select another port if you receive an error message that something else is using the port:

    E.g. `PORT=5001 make host-start-dev-server`

- `MOCK_DATASET_PATH` is a filesystem path to the parent directory for data provided for the mock services server; optional, defaults to `"${PWD}/dev/data/mock"` which contains just a little bit of mock data as an example.

- `VALIDATION_SPEC_URL` is a url to the validation spec file; optional, defaults to the production validation specs `https://raw.githubusercontent.com/kbase/sample_service_validator_config/master/metadata_validation.yml`

    Note this can be directed to a local file (within the container) using the `file://`  protocol. (An example of this will be in future work.)

- `DETACH` determines whether the docker compose service runs "detached", in the background, or not; defaults to "no". A value of "yes" indicates that it will run detached; "no" or any other value will cause it to run in the foreground. Foreground can be handy for monitoring log entries as they are produced, which can help with debugging. In detached mode you can use Docker Desktop to easily monitor the logs of individual services.

Any of these environment variables may set from the shell and will override defaults.

Note that within `dev/docker-compose.yml` the environment variables are prefixed by `DC_`. The `scripts/dev-server-env.sh` script contains the default values, and sets up the `DC_` environment variables. This script is sourced within the `Makefile`.

## Starting and sending to background

If you prefer to have the server run in the background without using the docker compose detached mode, and perhaps have standard output and standard error redirected to files, have a gander at this.

### Run in background, watch output

This mode lets you monitor the startup of services, but leaves the command line free, although not really usable until it is completed.

```shell
make host-start-dev-server &
```

### Run in background, send output to files

```shell
make host-start-dev-server > out.txt 2> err.txt & 
```

Returns immediately, starting and running the server in the background, and sending standard output to `out.txt` and standard error to `err.txt`.

### Run in background, ignore output

```shell
make host-start-dev-server > /dev/null 2> /dev/null &
```

You can always access the logs via docker, e.g. select the running container in Docker Desktop to view the logs for that container.

## Stopping and removing the containers

An associated make task `host-stop-dev-server` stop and remove the containers. You can run this task in the same window, if you started the server in the background, or in another window if the server is running in the foreground (or you may kill the primary server process with `[Control]C` and then run the `host-stop-dev-server` task to clean up.)

## Using the sample service in the container

To use the sample service running in the container, simply invoke the api at `http://localhost:$PORT` where `$PORT` is either the default port of `5006` or the port you set in the `PORT` environment variable.

E.g.

```sh
curl -X POST http://localhost:5006/ \
    -d '{
"version": "1.1",
"id": "123",
"method": "SampleService.status",
"params": []
}'
```

If you pipe that through `json_pp`:

```sh
curl -s -X POST http://localhost:5006/ \
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
