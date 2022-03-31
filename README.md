# Sample Service  

Build status (master):
![Build Status](https://github.com/kbase/sample_service/actions/workflows/build-test-push.yml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/kbase/sample_service/badge.svg?branch=master)](https://coveralls.io/github/kbase/sample_service?branch=master)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/kbase/sample_service.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/kbase/sample_service/context:python)
[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

The Sample Service is a KBase "core service" which supports storage and retrieval of experimental sample data. It supports validation, access control, versioning, and linking to KBase data objects. 


## Table of Contents

- [Security](#security)
- [Background](#background)
- [Installation](#installation)
- [Usage](#usage)
- [Documentation](#documentation)
- [API](#api)
- [Maintainers](#maintainers)
- [Thanks](#thanks)
- [Contributing](#contributing)
- [License](#license)

## Security

> TODO


## Background

The Sample Service provides a means of getting "samples" into the KBase environment. It is therefore tightly bound to the KBase infrastructure; it is not an independently running system.

Sample raw data is "uploaded" to KBase, is "imported" via a Narrative app into the Sample Service, may be "linked" to existing data objects, may contribute specific data to data objects.

A sample is a set of key-value pairs, or fields. If the key matches a pre-defined sample "metadata field", called "controlled" fields, validation constraints will be applied and the value may be transformed. Fields which do not match a controlled field are "user fields", have no constraints, and are not transformed.

The sample service does not capture relations between samples. There is mention in the documentation of sample "trees", but that feature of the sample service has never been fully developed or utilized. Rather sets of samples are maintained in the KBase Workspace in the form of the SampleSet object.

 Samples are stored in ArangoDB, a database shared with the KBase "Relation Engine". The sample service interacts with other KBase and KBase-managed services such as Kafka, Workspace, Auth, and User Profile.
  

## Installation

The sample service may be run either locally for development or in a supported runtime environment, e.g. in a KBase environment.

In a development context, the Sample Service may be [run locally](./docs/development/local-docker.md) as a service target or within tests for running integration tests.

Within a KBase environment

## Usage

> TODO

## Documentation

Please see the [Documentation](./docs/index.md) for detailed information on development, testing and design.

## API

> TODO

## Maintainers

> TODO

## Thanks

> TODO 

## Contributing

> TODO 

## License

SEE LICENSE IN [LICENSE](./LICENSE)

