export MONGODB_VER=mongodb-linux-x86_64-3.6.16
export ARANGODB_VER=3.5.1
export ARANGODB_V=35
export KAFKA_VER=2.8.1
export SCALA_VER=2.12

set -e


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
    logn "host dependency '$1' not found"
    exit 1
  fi

  if [ -n "$2" ]
  then
    logn "will check version $2"
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
  rm -rf test/bin
  mkdir -p test/bin/temp
  rm -f test/test.cfg
}

# MongoDB

export RETVAL=""

install_mongo()
{
  log 'Installing mongo...'
  cd test/bin/temp
  wget --quiet http://fastdl.mongodb.org/linux/$MONGODB_VER.tgz
  gzip -d $MONGODB_VER.tgz
  tar xf $MONGODB_VER.tar
  mv $MONGODB_VER mongo
  mv mongo ..
  rm ./*.tar
  cd ../../..
  logn "done"
  RETVAL="${PWD}/test/bin/mongo/bin/mongod"
}


install_jars()
{
  cd test/bin
  git clone --depth 1 https://github.com/kbase/jars jars_repo
  mv jars_repo/lib/jars .
  rm -rf jars_repo
  cd ../..
  RETVAL="$(PWD)/test/bin/jars/lib/jars"
}

install_test_config()
{
  cd test
  cp test.cfg.example test.cfg
  # TODO: ensure this works on Linux. If not, need to find another means of
  sed -i ".bak" "s#^test.jars.dir=.*#test.jars.dir=$JARS_PATH#" test.cfg
  sed -i ".bak" "s#^test.temp.dir=.*#test.temp.dir=temp_test_dir#" test.cfg
  sed -i ".bak" "s#^test.mongo.exe.*#test.mongo.exe=$MONGO_PATH#" test.cfg
  sed -i ".bak" "s#^test.mongo.wired_tiger.*#test.mongo.wired_tiger=true#" test.cfg
  cd ..
}

install_python_dependencies()
{
  # note that pipenv must already be available
  # the reason not to automate this is that it is a bit tricky to
  # install on host machines, and the technique to install it may
  # vary by host system type and the developers preferred package manager
  pipenv install --dev
}

# MAIN

ensure_host_dependencies

prepare_bin_dir

install_mongo
export MONGO_PATH=$RETVAL

install_jars
export JARS_PATH=${PWD}/temp/bin/jars

install_test_config

install_python_dependencies
