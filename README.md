# Sample Service  

[![tests](https://github.com/kbase/sample_service/actions/workflows/non_sdk_test.yml/badge.svg)](https://github.com/kbase/sample_service/actions/workflows/non_sdk_test.yml)
[![Coverage Status](https://coveralls.io/repos/github/kbase/sample_service/badge.svg?branch=master)](https://coveralls.io/github/kbase/sample_service?branch=master)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/kbase/sample_service.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/kbase/sample_service/context:python)
[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

The Sample Service is a KBase "core service" which supports storage and retrieval of experimental sample metadata. It supports validation, access control, versioning, and linking of samples to KBase data objects. 

## Table of Contents

- [Security](#security)
- [Background](#background)
- [Contributing](#Contributing)
- [Installation](#installation)
- [Usage](#usage)
- [Documentation](#documentation)
- [API](#api)
- [Maintainers](#maintainers)
- [Contributing](#contributing)
- [License](#license)

## Security

- Private samples require a KBase auth token be provided in the API; public samples do not require an auth token.
- Must operate over https or within an isolated environment, as it receives and sends auth tokens. If operated over https within a protected environment, the proxy to it must provide only https access to the API endpoint.
- All upstream services (ArangoDB, Kafka, KBase Workspace, KBase auth) must be similarly protected

## Background

The Sample Service provides a means of getting samples "metadata" into the KBase environment. It is tightly bound to the KBase infrastructure; it is not an independently running system.

Sample raw data is "uploaded" to KBase, is "imported" via a Narrative app into the Sample Service, may be "linked" to existing data objects, may contribute specific data to data objects.

A sample is a set of key-value pairs, or fields, containing measurements and observations. They may be gathered concurrently with physical samples e.g. to represent environment conditions, or afterwards representing measurements of the sample itself. In the context of KBase, samples do not contain reads, which are captured elsewhere in KBase.
 
If the a sample field matches a pre-defined sample field, called "controlled" fields, validation constraints will be applied and the value may be transformed. Fields which do not match a controlled field are "user fields", have no constraints, and are not transformed.

The sample service does not capture relations between samples. There is mention in the documentation of sample "trees", but that feature of the sample service has never been fully developed or utilized. Rather sets of samples are maintained in the KBase Workspace in the form of the SampleSet object.

 Samples are stored in ArangoDB, a database shared with the KBase "Relation Engine". The sample service interacts with other KBase and KBase-managed services such as Kafka, Workspace, Auth, and User Profile.
 
See the following resources for additional information regarding the design of the KBase samples system:
- [Linking Metadata](https://docs.kbase.us/data/samples)
- [SampleSet](https://docs.kbase.us/data/upload-download-guide/sampleset)

For additional information, including specific usage scenarios, please visit the [KBase Documentation Site](https://docs.kbase.us) and search for "sample".

![Docs Site](./docs/images/kbase-docs-site.png)

## Contributing

### When is `develop` merged into `master`?

The `develop` branch is merged to `master` in anticipation of a new release.
This merge should rename the `Unreleased` section of `RELEASE_NOTES.md` to the
version of the release. Ideally, that merge results in a tag and release from
`master`.

### When should `RELEASE_NOTES.md` be updated?

Release notes should be updated in any Pull Request (PR) to the `develop`
branch in the `Unreleased` section. These updated notes are the author's
responsibility. If this PR is the first after a release, the author should add
the `Unreleased` section to the `RELEASE_NOTES.md`.

When `develop` is merged into `master` the `RELEASE_NOTES.md` should also be
updated, updating the `Unreleased` header to reflect the version released.

### What if `develop` and `master` diverge?

This should be a rare occurence. Bring it up during the stand up. The usual
solution in this case is to merge `master` into `develop` in some way. Say you
have PR for a branch named `feature`.

1. Merge `master` into `feature`.
2. Merge `develop` into `feature`.
3. Merge `feature` into `develop` via the PR.

### What if the PR is too big or complex?
 
If you think your PR is too big then you should raise this concern in the daily
standup as early as possible. There may be various outcomes depending on what
the team decides.

## Installation

The sample service may be run either locally for development or in a supported runtime environment, e.g. in a KBase environment.

In a development context, the Sample Service may be [run locally](./docs/development/local-docker.md) as a service target or within tests for running integration tests.

For deploying within a KBase environment, please consult the [deployment guide](./docs/deployment/index.md).

## Usage

The Sample Service is used as an API endpoint for various other services, such as the Sample Importer, Narrative, Sample and Sample Set landing pages, and other apps.

It is roughly "kb-sdk compatible", and as such uses the KBase "JSON-RPC-1.1-like" interface over https. 

For API usage consult the generated [api document](http://htmlpreview.github.io/?https://github.com/kbase/sample_service/blob/master/SampleService.html).

For example, here is a call to get a single sample:

```json
{
	"version": "1.1",
	"id": "123",
	"method": "SampleService.get_sample",
	"params": [{
		"id": "4c72858c-5d61-45fb-b47e-974005c1b074",
		"version": 1
	}]
}
```

> Note that an `Authentication: KBASETOKEN` header (where `KBASETOKEN` is a KBase login or other auth token), is required for private samples, and a `Content-Type: application/json` is recommended.

Which results in (elided to shorten):

```json
{
	"version": "1.1",
	"result": [{
		"id": "4c72858c-5d61-45fb-b47e-974005c1b074",
		"user": "eapearson",
		"name": "0408-FW021.46.11.27.12.02",
		"node_tree": [{
			"id": "0408-FW021.46.11.27.12.02",
			"type": "BioReplicate",
			"parent": null,
			"meta_controlled": {
				"enigma:experiment_name": {
					"value": "100 Well"
				},
				"enigma:area_name": {
					"value": "Area 1"
				},
				"latitude": {
					"value": 35.97774706,
					"units": "degrees"
				}
			},
			"meta_user": {
				"redox_potential_?": {
					"value": 431.5
				}
			},
			"source_meta": [{
					"key": "enigma:experiment_name",
					"skey": "Experiment Name",
					"svalue": {
						"value": "100 Well"
					}
				},
				{
					"key": "enigma:area_name",
					"skey": "Area Name",
					"svalue": {
						"value": "Area 1"
					}
				},
				{
					"key": "latitude",
					"skey": "Latitude",
					"svalue": {
						"value": 35.97774706
					}
				}
			]
		}],
		"save_date": 1642712508378,
		"version": 1
	}],
	"id": "123"
}
```

## Documentation

Please see the [Documentation](./docs/index.md) for detailed information on development, testing and design.

## API

The API is documented in the generated [api document](http://htmlpreview.github.io/?https://github.com/kbase/sample_service/blob/master/SampleService.html).

## Maintainers

This project is maintained by KBase staff.

## Contributing

Contributions to this repo roughly follow gitflow:

- create a feature branch from the develop branch
  - feature branches should be created to satisfy a JIRA ticket; include the JIRA ticket id in the branch name
- develop in that feature branch locally
- create a Pull Request against develop from this feature branch
- the PR then enters a review/change cycle until the review is complete
- when complete, the PR is merged into the develop branch 
- when a release is called for, the develop branch is merged into master, a release is created off of master.

## License

SEE LICENSE IN [LICENSE](https://raw.githubusercontent.com/kbase/sample_service/master/LICENSE)
