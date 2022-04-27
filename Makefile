SERVICE_CAPS = SampleService
SPEC_FILE = SampleService.spec
LIB_DIR = lib
SCRIPTS_DIR = scripts
TEST_DIR = test
TEST_CONFIG_FILE = test.cfg
LBIN_DIR = bin
WORK_DIR = /kb/module/work/tmp
# see https://stackoverflow.com/a/23324703/643675
MAKEFILE_DIR:=$(strip $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))

# Override TEST_SPEC when running make test-sdkless to run different tests
TEST_SPEC := $(TEST_DIR)  

PYPATH=$(MAKEFILE_DIR)/$(LIB_DIR):$(MAKEFILE_DIR)/$(TEST_DIR)
TSTFL=$(MAKEFILE_DIR)/$(TEST_DIR)/$(TEST_CONFIG_FILE)

.PHONY: test

default: compile

all: compile build build-startup-script build-executable-script build-test-script

compile:
# Don't compile server automatically, overwrites fixes to error handling
# Temporarily add the next line to the command line args if recompiliation is needed to add
# methods.
#		--pysrvname $(SERVICE_CAPS).$(SERVICE_CAPS)Server \

	kb-sdk compile $(SPEC_FILE) \
		--out $(LIB_DIR) \
		--pyclname $(SERVICE_CAPS).$(SERVICE_CAPS)Client \
		--dynservver release \
		--pyimplname $(SERVICE_CAPS).$(SERVICE_CAPS)Impl;
	- rm $(LIB_DIR)/$(SERVICE_CAPS)Server.py

	kb-sdk compile $(SPEC_FILE) \
		--out . \
		--html \

test: test-sdkless coverage-reports

test-sdkless:
	# TODO flake8 and bandit
	# TODO check tests run with kb-sdk test - will need to install mongo and update config
	MYPYPATH=$(MAKEFILE_DIR)/$(LIB_DIR) pipenv run mypy --namespace-packages $(LIB_DIR)/$(SERVICE_CAPS)/core $(TEST_DIR)
	PYTHONPATH=$(PYPATH) SAMPLESERV_TEST_FILE=$(TSTFL) pipenv run pytest --verbose --cov $(LIB_DIR)/$(SERVICE_CAPS) --cov-config=$(TEST_DIR)/coveragerc $(TEST_SPEC)
	# to print test output immediately: --capture=tee-sys

clean:
	rm -rfv $(LBIN_DIR)

# Managing development container orchestration

host-start-dev-server:
	source scripts/dev-server-env.sh && sh scripts/start-dev-server.sh

host-stop-dev-server:
	source scripts/dev-server-env.sh && sh scripts/stop-dev-server.sh

# Test support

test-setup:
	bash test/scripts/test-setup.sh

coverage-reports:
	@echo "Creating html coverage report"
	pipenv run coverage html
	@echo "Converting coverage to lcov"
	pipenv run coverage lcov --data-file .coverage -o cov_profile.lcov

coverage-summary:
	@echo "Coverage summary:"
	pipenv run coverage report