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
TEST_SPEC := $(TEST_DIR)/lib/specs

PYPATH=$(MAKEFILE_DIR)/$(LIB_DIR):$(MAKEFILE_DIR)/$(TEST_DIR)/lib
TSTFL=$(MAKEFILE_DIR)/$(TEST_DIR)/$(TEST_CONFIG_FILE)
TEST_PYPATH=$(MAKEFILE_DIR)/$(LIB_DIR):$(MAKEFILE_DIR)/$(TEST_DIR)/lib

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

test: host-test-types
	make -s host-start-test-services && \
	ARANGO_URL=http://localhost:8529 make -s wait-for-arango && \
	MONGO_HOST=localhost:27017 make -s wait-for-mongo && \
	make -s test-sdkless && \
	make -s host-stop-test-services && \
	make -s coverage-reports


test-sdkless:
	# TODO flake8 and bandit
	# TODO check tests run with kb-sdk test - will need to install mongo and update config
	PYTHONPATH=$(PYPATH) SAMPLESERV_TEST_FILE=$(TSTFL) pipenv run pytest \
		--verbose \
		--cov $(LIB_DIR)/$(SERVICE_CAPS) \
		--cov-config=$(TEST_DIR)/coveragerc \
		$(TEST_SPEC)
# to print test output immediately: --capture=tee-sys

host-test-types:
	MYPYPATH="${LIB_DIR}" pipenv run python -m mypy --namespace-packages "${LIB_DIR}/SampleService/core" "${TEST_DIR}/lib"

clean:
	rm -rfv $(LBIN_DIR)

# Managing development containers

host-start-dev-server:
	source scripts/dev-server-env.sh && sh scripts/start-dev-server.sh

host-stop-dev-server:
	source scripts/dev-server-env.sh && sh scripts/stop-dev-server.sh

# Managing test containers

host-start-test-services:
	sh scripts/start-test-services.sh &>test/test-services.log &

host-stop-test-services:
	sh scripts/stop-test-services.sh

test-setup:
	sh scripts/test-setup.sh

coverage-reports:
	@echo "Creating html coverage report"
	pipenv run coverage html
	@echo "Converting coverage to lcov"
	# TODO: the below should work, and would simplify things if it did,
	# but at last try it did not.
	pipenv run coverage lcov --data-file .coverage -o cov_profile.lcov

# Wait for ...

wait-for-arango:
	@echo "Waiting for ArangoDB to be available"
	@[ "${ARANGO_URL}" ] || (echo "! Environment variable ARANGO_URL must be set"; exit 1)
	PYTHONPATH=$(TEST_PYPATH) pipenv run python -c "import sys; from test_support.wait_for import wait_for_arangodb; wait_for_arangodb('$(ARANGO_URL)', 60, 1) or sys.exit(1)"


wait-for-mongo:
	@echo "Waiting for MongoDB to be available"
	@[ "${MONGO_HOST}" ] || (echo "! Environment variable MONGO_HOST must be set"; exit 1)
	PYTHONPATH=$(TEST_PYPATH) pipenv run python -c "import sys; from test_support.wait_for import wait_for_mongodb; wait_for_mongodb('$(MONGO_HOST)', 60, 1) or sys.exit(1)"
