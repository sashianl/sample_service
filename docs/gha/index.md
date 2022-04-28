# GitHub Action (GHA) Workflow

This project uses a GitHub Action workflow to

- run unit and integration tests,
- build an image containing the service,
- tag the image appropriately, and 
- publish the image to GitHub Container Registry

under a variety of conditions

This is accomplished with a set of 6 workflow files located in `./github/workflows`.

Of these workflow files, 2 are "reusable workflows" containing the primary workflow logic, and 4 are "controlling workflows" which invoke the reusable workflows and indicate triggering conditions and, optionally, an image tag.

GitHub Actions supports a type of workflow termed a "reusable workflow". Such workflows may be included in another workflow. They differ from a normal workflow in that they can only use a special triggering condition "on.workflow_call", and they may define a set of input parameters.

The reusable workflows are:

- `test.yml` - sets up and runs all tests
  - parameters: none
- `build-push-image.yml` - builds the service image, and pushes it to GHCR 
  - parameters
    - tag - the tag for the image
  - secrets
    - ghcr_username - the username for the GHCR push
    - ghcr_token - the token associated with the username to authorize GHCR push


The controlling workflows are: 

- `pull-request.yml` - triggered by pull request activity (open, reopen, synchronize) on the master or develop branches, creates an image tage like "pr-#". This only runs tests. (See below for generating images for PRs)
- `close-merge-pr.yml` - triggered by a pull request against master or develop which is closed and merged. It runs both tests and build/push image.  It generates a tag which is the branch name (develop or master). This supports the case of always having an up-to-date develop and master image to use, e.g. in CI.
* `release.yml` - triggered when a release is published against the master branch. It executes both the test and build/push workflows, using an image tag which is the git tag associated with the release. This supports automatic creation of release images during the release process.
* `manual.yml` - triggered by workflow_dispatch, so can be run by the GitHub UI button. It runs both test and image build/push, and a tag which is the branch name. The supports the use case of generating an image from any branch. E.g. in order to preview changes in a feature or fix branch, one may run this workflow specifying a branch which is either the source for a PR or may become one, generating an image that may be previewed and verifying through shared test results that the changes are non-breaking.

## Image name and tag

Most workflows will generate and publish an image for the service.

The image will be named `kbase/sample_service:TAG`, where `TAG` is one of:

- `pr-#` for a pull request against master or develop, where `#` is the pull request number
- the branch name `develop` or `master` when the trigger is a push-to-branch (more on fix and feature branches below)
- `v#.#.#` when the trigger is a release, where `v#.#.#` is the release tag, e.g. `v1.2.3`.

The image may be addressed at `ghcr.io/kbase/sample_service:TAG`, for example

```shell
docker pull ghcr.io/kbase/sample_service:pull_request-462
```
