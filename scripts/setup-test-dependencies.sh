# export MONGODB_VER=mongodb-osx-x86_64-3.6.16
export MONGODB_VER=mongodb-osx-ssl-x86_64-3.6.23
export MONGODB_VER_UNPACKED=mongodb-osx-x86_64-3.6.23
# https://fastdl.mongodb.org/osx/mongodb-osx-ssl-x86_64-3.6.23.tgz
# https://downloads.mongodb.com/osx/mongodb-osx-x86_64-enterprise-3.6.22.tgz
export ARANGODB_VER=3.5.1
export ARANGODB_V=35
export KAFKA_VER=2.8.1
export SCALA_VER=2.12

set -e

# Utilities

log()
{
   printf '%s' "$1"
}

logn()
{
   printf '%s\n' "$1"
}

ensure_dependency()
{
  if ! hash "$1" 2>/dev/null
  then
    logn "error: host dependency '$1' not found"
    exit 1
  fi

  if [ -n "$2" ]
  then
    logn "(will check version $2)"
  fi
}

ensure_host_dependencies()
{
  ensure_dependency wget
  ensure_dependency java
  ensure_dependency docker
  ensure_dependency sed
  ensure_dependency python 3.7
  ensure_dependency pipenv
}

prepare_bin_dir()
{
  rm -rf test/bin/jars
  rm -rf test/bin/mongo
  mkdir -p test/bin/temp
  rm -f test/test.cfg
}

cleanup_bin_dir()
{
  rm -rf test/bin/temp
}

# MongoDB

export RETVAL=""

install_mongo()
{
  log "Installing mongo..."
  cd test/bin/temp

  # TODO: need switch for the os platform:
  # TODO: this only works for macosx currently; this will be
  # replaced with a mongodb container shortly.

  wget --quiet https://fastdl.mongodb.org/osx/$MONGODB_VER.tgz

  tar xvfz $MONGODB_VER.tgz
  mv $MONGODB_VER_UNPACKED mongo
  mv mongo ..
  rm $MONGODB_VER.tgz
  cd ../../..
  RETVAL="${PWD}/test/bin/mongo/bin/mongod"
  logn "done."
}


install_jars()
{
  log "Installing jars..."
  cd test/bin
  git clone --quiet --depth 1 https://github.com/kbase/jars jars_repo
  mv jars_repo/lib/jars .
  rm -rf jars_repo
  cd ../..
  RETVAL="$(PWD)/test/bin/jars"
  logn "done."
}

install_test_config()
{
  log "Installing test config..."
  cd test
  cp test.cfg.example test.cfg
  # TODO: This is a rather limited manner of processing the config template; a more generic
  # method which can utilize environment variables directly, e.g. dockerize, would be better
  sed -i ".bak" "s#^test.jars.dir=.*#test.jars.dir=$JARS_PATH#" test.cfg
  sed -i ".bak" "s#^test.temp.dir=.*#test.temp.dir=$PWD/temp_test_dir#" test.cfg
  sed -i ".bak" "s#^test.mongo.exe.*#test.mongo.exe=$MONGO_PATH#" test.cfg
  sed -i ".bak" "s#^test.mongo.wired_tiger.*#test.mongo.wired_tiger=true#" test.cfg
  cd ..
  logn "done."
}

install_python_dependencies()
{
  log "Installing Python dependencies..."
  # note that pipenv must already be available
  # the reason not to automate this is that it is a bit tricky to
  # install on host machines, and the technique to install it may
  # vary by host system type and the developers preferred package manager
  pipenv install --dev
  logn "done."
}

# MAIN

ensure_host_dependencies

prepare_bin_dir

install_mongo
export MONGO_PATH=$RETVAL

install_jars
export JARS_PATH=$RETVAL

cleanup_bin_dir

install_test_config

install_python_dependencies
