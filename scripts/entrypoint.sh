#!/bin/bash

#. /kb/deployment/user-env.sh
#
#python ./scripts/prepare_deploy_cfg.py ./deploy.cfg ./work/config.properties

# Create deploy.cfg
if [ ! -z "$CONFIG_URL" ] ; then
    EXTRA=" -env ${CONFIG_URL} -env-header /run/secrets/auth_data"
fi
dockerize ${EXTRA} -template deploy.cfg.tmpl:deploy.cfg

if [ $# -eq 0 ] ; then
  script_dir=$(dirname "$(readlink -f "$0")")
  export KB_DEPLOYMENT_CONFIG=$script_dir/../deploy.cfg
  export PYTHONPATH=$script_dir/../lib:$PATH:$PYTHONPATH
  gunicorn --worker-class gevent --timeout 30 --workers 17 --bind :5000 --log-level info SampleService.SampleServiceServer:application
elif [ "${1}" = "bash" ] ; then
  bash
else
  echo Unknown
fi
