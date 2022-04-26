set -e

# Utilities

ensure_os()
{
  if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "macos"
  elif [[ "$OSTYPE" == "linux-gnu"*  ]]; then
    echo "linux"
  else
    echo "Not a supported OS: ${OSTYPE}"
    exit 1
  fi
}

# Not OS-specific
ARANGODB_VER="3.5.1"
ARANGODB_V="35"
KAFKA_VER="2.8.1"
SCALA_VER="2.12"

if [[ "$(ensure_os)" == "macos" ]]; then
  MONGODB_VER=mongodb-osx-ssl-x86_64-3.6.23
  MONGODB_VER_UNPACKED=mongodb-osx-x86_64-3.6.23
else
  MONGODB_VER=mongodb-linux-x86_64-3.6.23
  MONGODB_VER_UNPACKED=mongodb-linux-x86_64-3.6.23
fi

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
  ensure_dependency python 3.7
  ensure_dependency pipenv
}

prepare_bin_dir()
{
  rm -rf test/bin/jars
  rm -rf test/bin/mongo
  rm -rf test/bin/arangodb
  rm -rf test/bin/kafka
  mkdir -p test/bin/temp
  rm -f test/test.cfg
}

cleanup_bin_dir()
{
  rm -rf test/bin/temp
}

# MongoDB

install_jars()
{
  log "Installing jars..."
  cd test/bin
  git clone --quiet --depth 1 https://github.com/kbase/jars jars_repo
  mv jars_repo/lib/jars .
  rm -rf jars_repo
  cd ../..

  export JARS="${PWD}/test/bin/jars"

  logn "done."
}


install_mongo()
{
  log "Installing mongo..."
  cd test/bin/temp

  # TODO: need switch for the os platform:
  # TODO: this only works for macosx currently; this will be
  # replaced with a mongodb container shortly.

  os=$(ensure_os)
  if [[ $os == "macos" ]]; then
    path_element="osx"
  else
    path_element="linux"
  fi

  wget --quiet https://fastdl.mongodb.org/${path_element}/$MONGODB_VER.tgz

  tar xvfz $MONGODB_VER.tgz
  mv $MONGODB_VER_UNPACKED mongo
  mv mongo ..
  rm $MONGODB_VER.tgz
  cd ../../..

  export MONGOD="${PWD}/test/bin/mongo/bin/mongod"

  logn "done."
}

install_arango()
{
  log "Installing arango..."
  cd test/bin/temp

  os=$(ensure_os)

  if [[ $os == "macos" ]]; then
    path_element="MacOSX"
  else
    path_element="Linux"
  fi

  export ARANGO_ARCHIVE="arangodb3-${os}-$ARANGODB_VER.tar.gz"
  curl -O "https://download.arangodb.com/arangodb$ARANGODB_V/Community/${path_element}/$ARANGO_ARCHIVE"
  tar -xf "$ARANGO_ARCHIVE"
  mv "arangodb3-$ARANGODB_VER" arangodb
  mv arangodb ..
  rm "$ARANGO_ARCHIVE"
  cd ../../..

  export ARANGO_EXE=${PWD}/test/bin/arangodb/usr/sbin/arangod
  export ARANGO_JS=${PWD}/test/bin/arangodb/usr/share/arangodb3/js/

  logn "done."
}

install_kafka()
{
  log "Installing kafka..."
  cd test/bin/temp

  export KAFKA_ARCHIVE="kafka_$SCALA_VER-$KAFKA_VER.tgz"
  curl -O "http://mirror.metrocast.net/apache/kafka/$KAFKA_VER/$KAFKA_ARCHIVE"
  tar -xzf $KAFKA_ARCHIVE
  mv "kafka_$SCALA_VER-$KAFKA_VER" kafka
  mv kafka ..
  rm $KAFKA_ARCHIVE
  cd ../../..

  export KAFKA_BIN_DIR="${PWD}/test/bin/kafka/bin"

  logn "done."
}


install_test_config()
{
  log "Installing test config..."
  export TEMP_DIR="$PWD/temp_test_dir"
  pipenv run python test/scripts/render-template.py "$PWD/test/test.cfg.template"  "$PWD/test/test.cfg"
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

os=$(ensure_os)
logn "Supported OS detected: $os"

ensure_host_dependencies

install_python_dependencies

prepare_bin_dir

install_mongo
install_arango
install_kafka
install_jars

install_test_config
