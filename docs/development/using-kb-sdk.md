# Using `kb_sdk` with this repo

The `sample_service` is based on the code layout and structure established by initializing and managing a codebase with the [KBase App SDK (`kb_sdk`)](https://github.com/kbase/kb_sdk).

It does not adhere strictly to `kb_sdk` practices.

This document describes, in a nutshell, current practice.

> Note that the "KBase App SDK" is known, interchangeably, as `kb_sdk`, which is the name of the repo, and `kb-sdk`, which is the name of the command and the image.

## Differences

### Compilation

This is a Python project, so there is no real code compilation. However, the "compile" command, as configured in the project's `Makefile` and implemented in `kb-sdk`, does more than compile a project which requires compilation. It also validates the api spec and generates code.

Typically, a `kb_sdk` service is maintained by updating the "spec file" (`SampleService.spec`) and then running `make compile`. This would:

1. create core utility library files `lib/SampleService/authclient.py`, `lib/SampleService/baseclient.py`
2. add, modify, or remove method stubs to `lib/SampleService/SampleServiceImpl.py`, depending on changes to the spec file
3. create the main entrypoint `lib/SampleServiceServer.py`, overwriting the existing file if any
4. create the spec documentation page `SampleService.html`, overwriting the existing file if any

In this repo, (3) is omitted. See the `Makefile` for how this was done. The Server file needs to be maintained by hand.

The main server entrypoint is `lib/SampleService/SampleServiceServer.py`, rather than `lib/SampleServiceServer.py`.

### Testing

Typically a `kb-sdk` project runs tests through `kb-sdk`, which manages invocation of the tests via a container using an image built from the project's `Dockerfile` and utilizes conventions for directories, configuration, etc.

In this project, the limitations of that test environment preclude using that test framework.

Rather, tests are run via `make test-sdkless`.

## Using `kb-sdk`

The `kb-sdk` command is made available through a docker container, which uses an image based on the latest release of `kb_sdk`.

You may consult the [KBase SDK Docs](https://kbase.github.io/kb_sdk_docs/tutorial/2_install.html) for the official installation procedure, or follow these somewhat simpler and less intrusive instructions:

- install `kb-sdk`:
  - `make install-sdk`
- following the instructions printed to the console, adjust the path in your shell:
  - export PATH=$PWD:$PATH

The typical workflow does not use `kb-sdk` directly.

The primary current usage of `kb-sdk` in this project is compilation, as briefly described above.

The following command:

```sh
make compile
```

will run the compilation, emitting errors if there are problems with the spec file, or generating new or updated method stubs for any newly added or updated function definitions.
