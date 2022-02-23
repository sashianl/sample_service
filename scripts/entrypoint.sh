#!/bin/bash

echo '[ENTRYPOINT] starting'

# Create deploy.cfg
if [ ! -z "$CONFIG_URL" ] ; then
    EXTRA=" -env ${CONFIG_URL} -env-header /run/secrets/auth_data"
fi
# TODO: dockerize is usually used as the entrypoint and the template substitution
# specified in the Dockerfile.
dockerize ${EXTRA} -template deploy.cfg.tmpl:deploy.cfg

echo "[ENTRYPOINT] using option '${1}'"

script_dir=$(dirname "$(readlink -f "$0")")
export KB_DEPLOYMENT_CONFIG=$script_dir/../deploy.cfg
export PYTHONPATH=$script_dir/../lib:$PATH:$PYTHONPATH

echo "[ENTRYPOINT] Python path: ${PYTHONPATH}"

if [ "${1}" = "bash" ] ; then
  echo "[ENTRYPOINT] shell mode with Python path: ${PYTHONPATH}"
  bash
else
  if [ $# -eq 0 ] ; then
    workers=17
    log_level=info
  elif [ "${1}" = "develop" ] ; then
    # Python path must include the test directory in order for the test validators
    # to be loadable.
    # Test directory is included in the python path so that we can use
    # test validators defined therein. 
    # TODO: we may want to establish a dev directory in which to place
    # development validators, data for loading, etc.
    export PYTHONPATH="$script_dir/../test:$PYTHONPATH"
    echo "[ENTRYPOINT] develop mode with Python path: ${PYTHONPATH}"
    python "${script_dir}/../lib/cli/prepare-arango.py"
    workers=1
    log_level=debug
  else
    echo "Unknown entrypoint option: ${1}"
    exit 1
  fi

  echo "[ENTRYPOINT] Starting gunicorn with Python path: ${PYTHONPATH}"
  gunicorn --worker-class gevent \
      --timeout 30 \
      --reload \
      --workers $workers \
      --bind :5000 \
      --log-level $log_level  \
      --capture-output \
      SampleService.SampleServiceServer:application
fi
