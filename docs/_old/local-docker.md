# Local Docker Development and Deployment

Due to the sample service's dependencies on other services (Arango DB, Kafka, Zookeeper, and KBase's auth and workspace), it can be rather complex, time-consuming, and unreliable to install and run these dependencies on the host machine.

To ease this burden, you may use a docker-compose based workflow.

This workflow:

- runs real external 3rd party services in containers (Arango, Kafka, Zookeeper)
- runs mock KBase services

Here is the basic workflow:

```shell
export MOCK_DATASET_PATH=`pwd`/test/data/mock_services
make host-start-dev-server
```

After a few seconds, you should have an operational Sample Service running on http://localhost:5000.

To close up shop, halt the services with `Ctrl-C` and issue:

```shell
make host-stop-dev-server
```

and then

```shell
make host-remove-dev-server
```

to clean up the containers

## Port

The standard port for KBase service containers is `5000`. This is also the default port exposed by the docker container. However, there are cases in which port 5000 is already used. E.g. in macOS Monterey port 5000 is used by AirPlay, in previous versions it may have been used by Bonjour;  generally this port is claimed by the "commplex-main" (https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml?search=5000).

To use a specific port other than 5000, set the `PORT` environment variable.

E.g.

```shell
export MOCK_DATASET_PATH=`pwd`/test/data/mock_services
export PORT=5001
make host-start-dev-server
```

## Accessing the Service

As mentioned above, the development server will not do very much without populating ArangoDB (including some aspects of the Relation Engine) and complementary data for the associated mock services (Workspace, Auth). The dev server was originally set up for testing new methods which provide data from the validation specs, and thus doesn't require the database.

These methods are not available presently in the Sample Service (they are being prepared in a subsequent pull request). However, the status endpoint does not require the database, and may be successfully invoked:

```sh
curl -X POST http://localhost:5001/ -s \
    -d '{
"version": "1.1",
"id": "123",
"method": "SampleService.status",
"params": []
}' | json_pp
```

which results in 

```json
{
   "result" : [
      {
         "servertime" : 1641222955761,
         "git_commit_hash" : "b362ec800344f7c527ace52d0cc0127d006a731c",
         "message" : "",
         "version" : "0.1.0",
         "git_url" : "git@github.com:kbase/sample_service.git",
         "state" : "OK"
      }
   ],
   "id" : "123",
   "version" : "1.1"
}
```

> if `json_pp` is not available it may be omitted - in this case the JSON will be displayed in a single line, i.e. without indentation.


## Status

This currently works for methods which do not access ArangoDB. That excludes most of the API, so we have work to do!. 

Primarily, the database needs to be populated with example data, and supporting mock data for the workspace and auth as well.
