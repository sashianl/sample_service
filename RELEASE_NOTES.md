# SampleService release notes

## Unreleased

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
