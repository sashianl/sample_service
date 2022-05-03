# SampleService release notes

## Unreleased

## 0.2.5

* organize documentation under a single `docs` directory; add some additional documentation and placeholders  [SAM-209]
* improve test config generate, automate installation of service deps for local testing, update python deps, improve coverage reporting in GHA [SAM-236]
* Improve performance of the `get_samples` method.

## 0.2.4
* Changes GitHub actions: creates images from releases off master, adds test running on develop branch
* Bugfix for write-write error
* Bugfix for incorrectly thrown `owner unexpectedly changed` errors
* Improved error handling/messages for get_samples method

## 0.2.1

* Add `get_data_links_from_sample_set` method - allows for getting many links from a list of samples
from a single call.

## 0.1.1

* Add `update_samples_acls` method - allows for updating many samples ACLs with
  a single call.

## 0.1.0-2alpha

* Add propagate_data_links method - propagates data links from a previous sample to the current (latest) version

## 0.1.0

* To fix temperature unit conversion check which breaks for celsius and (gasp) Fahrenheit,
  use the quantity api rather than multiplication (SAM-91).
* Pin flake8 and pipenv in requirements.txt for usage by GHA workflows or local development.

## 0.1.0-alpha28

* Add metadata (sub)key information to returned error detail

## 0.1.0-alpha27

* Adding auth-read-exempt-roles to config file so that read privileges are removed from get_sample_acls

## 0.1.0-alpha26

* Adding get_samples function with accompanying tests. First bulk function

## 0.1.0-alpha24

* Adding validate_samples method. Adds endpoint for checking if a list of samples will pass the validation step.

## 0.1.0

* Module created by kb-sdk init
* Initial release
