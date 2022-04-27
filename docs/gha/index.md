# GitHub Action (GHA) Workflow

This project uses a GitHub Action workflow to:

- run unit and integration tests
- build an image containing the service
- tag the image appropriatelyn
- publish the image to GitHub Container Registry

This is accomplished with a single GHA workflow file `./github/workflows/build-test-push.yml`.

The workflow is triggered by the following conditions:

- push to the branches:
  - master
  - develop
  - fix-*
  - feature-*
- a pull request against:
  - develop
  - master
- a release
- a manual workflow dispatch

The workflow is based on "ubuntu-latest". It proceeds to:

- install Python 3
- install pipenv
- install test dependencies
- run tests
- print a coverage summary
- build image
- tag image (see rules below)
- publish image to GHCR


## Image name and tag

The image will be named `kbase/sample_service:TAG`, where `TAG` is one of:

- the branch name `develop`, `master`, `fix-X`, or `feature-X` when the trigger is a push-to-branch (more on fix and feature branches below)
- `pr-#` for a pull request against master or develop, where `#` is the pull request number,
- `v#.#.#` when the trigger is a release, where `v#.#.#` is the release tag, e.g. `v1.2.3`.

The image may be addressed at `ghcr.io/kbase/sample_service:TAG`, for example

```shell
docker pull ghcr.io/kbase/sample_service:pull_request-462
```

## Fix and Feature Branches

This workflow supports fix and feature branches. These are simply branches prefixed by `fix-` and `feature-`. The intention is that often when producing a fix or a new feature, the iterative process involves periodic releases of the fix or feature for evaluation, prior to the fix or feature being merged in the main code line (in this case the develop branch).

If these "feature" is not desirable, it may be disabled by removing those triggering conditions.
