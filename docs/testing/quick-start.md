# Testing Quick Start

## Development

1. If developing, clone this repo from the `develop` branch; otherwise the `master` branch represents the most recent release.

```text
git clone -b develop https://github.com/kbase/sample_service
```

3. Setup host environment for testing:

```shell
cd sample_service
make test-setup
```

If any errors are reported, consult [the troubleshooting document](./troubleshooting.md).

Testing requires the following host programs:
- wget
- java 1.8 (other versions?)
- docker
- python 3.7
- pipenv

2. Run tests:

```shell
make test
```

3. When tests are successfully completed, a coverage summary will be displayed. A human-readable coverage report will be available in [`htmlcov/index.html`](../../htmlcov/index.html).


4. Coverage data end reports are excluded from git, so you can safely ignore any test artifacts.
