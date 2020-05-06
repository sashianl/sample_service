SERVICE_CAPS = SampleService
SPEC_FILE = SampleService.spec
LIB_DIR = lib
SCRIPTS_DIR = scripts
TEST_DIR = test
TEST_CONFIG_FILE = test.cfg
TEST_CONFIG_FILE_SDK = test-sdk.cfg
LBIN_DIR = bin
WORK_DIR = /kb/module/work/tmp
EXECUTABLE_SCRIPT_NAME = run_$(SERVICE_CAPS)_async_job.sh
STARTUP_SCRIPT_NAME = start_server.sh
TEST_SCRIPT_NAME = run_tests.sh
# see https://stackoverflow.com/a/23324703/643675
MAKEFILE_DIR:=$(strip $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST)))))

PYPATH=$(MAKEFILE_DIR)/$(LIB_DIR):$(MAKEFILE_DIR)/$(TEST_DIR)
TSTFL=$(MAKEFILE_DIR)/$(TEST_DIR)/$(TEST_CONFIG_FILE)
TSTFL_SDK=$(MAKEFILE_DIR)/$(TEST_DIR)/$(TEST_CONFIG_FILE_SDK)

.PHONY: test

default: compile

all: compile build build-startup-script build-executable-script build-test-script

compile:
	# Don't compile server automatically, overwrites fixes to error handling
	# Temporarily add the next line to the command line args if recompiliation is needed to add
	# methods.
	# --pysrvname $(SERVICE_CAPS).$(SERVICE_CAPS)Server \
	kb-sdk compile $(SPEC_FILE) \
		--out $(LIB_DIR) \
		--pyclname $(SERVICE_CAPS).$(SERVICE_CAPS)Client \
		--dynservver release \
		--pyimplname $(SERVICE_CAPS).$(SERVICE_CAPS)Impl;
	- rm $(LIB_DIR)/$(SERVICE_CAPS)Server.py

	kb-sdk compile $(SPEC_FILE) \
		--out . \
		--html \

build:
	chmod +x $(SCRIPTS_DIR)/entrypoint.sh

build-executable-script:
	mkdir -p $(LBIN_DIR)
	echo '#!/bin/bash' > $(LBIN_DIR)/$(EXECUTABLE_SCRIPT_NAME)
	echo 'script_dir=$$(dirname "$$(readlink -f "$$0")")' >> $(LBIN_DIR)/$(EXECUTABLE_SCRIPT_NAME)
	echo 'export PYTHONPATH=$$script_dir/../$(LIB_DIR):$$PATH:$$PYTHONPATH' >> $(LBIN_DIR)/$(EXECUTABLE_SCRIPT_NAME)
	echo 'python -u $$script_dir/../$(LIB_DIR)/$(SERVICE_CAPS)/$(SERVICE_CAPS)Server.py $$1 $$2 $$3' >> $(LBIN_DIR)/$(EXECUTABLE_SCRIPT_NAME)
	chmod +x $(LBIN_DIR)/$(EXECUTABLE_SCRIPT_NAME)

build-startup-script:
	mkdir -p $(LBIN_DIR)
	echo '#!/bin/bash' > $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)
	echo 'script_dir=$$(dirname "$$(readlink -f "$$0")")' >> $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)
	echo 'export KB_DEPLOYMENT_CONFIG=$$script_dir/../deploy.cfg' >> $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)
	echo 'export PYTHONPATH=$$script_dir/../$(LIB_DIR):$$PATH:$$PYTHONPATH' >> $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)
	echo 'gunicorn --worker-class gevent --timeout 30 --workers 17 --bind :5000 --log-level info $(SERVICE_CAPS).$(SERVICE_CAPS)Server:application' >> $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)
	chmod +x $(SCRIPTS_DIR)/$(STARTUP_SCRIPT_NAME)

build-test-script:
	echo '#!/bin/bash' > $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'script_dir=$$(dirname "$$(readlink -f "$$0")")' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'lib=$$script_dir/../$(LIB_DIR)/$(SERVICE_CAPS)' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'export KB_DEPLOYMENT_CONFIG=$$script_dir/../deploy.cfg' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'export KB_AUTH_TOKEN=`cat /kb/module/work/token`' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'echo "Removing temp files..."' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'rm -rf $(WORK_DIR)/*' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'echo "...done removing temp files."' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'export PYTHONPATH=$$script_dir/../$(LIB_DIR):$$PATH:$$PYTHONPATH' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'cd $$script_dir/../$(TEST_DIR)' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'SAMPLESERV_TEST_FILE=$(TSTFL_SDK) pytest  --verbose --cov=$$lib --cov-config=coveragerc --cov-report=html .' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'mv .coverage /kb/module/work/' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'rm -r /kb/module/work/test_coverage' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	echo 'mv htmlcov /kb/module/work/test_coverage' >> $(TEST_DIR)/$(TEST_SCRIPT_NAME)
	chmod +x $(TEST_DIR)/$(TEST_SCRIPT_NAME)

test:
	if [ ! -f /kb/module/work/token ]; then echo -e '\nOutside a docker container please run "kb-sdk test" rather than "make test"\n' && exit 1; fi
	bash $(TEST_DIR)/$(TEST_SCRIPT_NAME)

test-sdkless:
	# TODO flake8 and bandit
	# TODO check tests run with kb-sdk test - will need to install mongo and update config
	MYPYPATH=$(MAKEFILE_DIR)/$(LIB_DIR) mypy --namespace-packages $(LIB_DIR)/$(SERVICE_CAPS)/core $(TEST_DIR)
	PYTHONPATH=$(PYPATH) SAMPLESERV_TEST_FILE=$(TSTFL) pytest --verbose --cov $(LIB_DIR)/$(SERVICE_CAPS) --cov-config=$(TEST_DIR)/coveragerc $(TEST_DIR)

clean:
	rm -rfv $(LBIN_DIR)
