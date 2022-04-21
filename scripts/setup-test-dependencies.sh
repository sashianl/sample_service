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

install_jars()
{
  log "Installing jars..."
  cd test/bin
  git clone --quiet --depth 1 https://github.com/kbase/jars jars_repo
  mv jars_repo/lib/jars .
  rm -rf jars_repo
  cd ../..
  RETVAL="${PWD}/test/bin/jars"
  logn "done."
}

install_test_config()
{
  log "Installing test config..."
  cd test
  cp test.cfg.example test.cfg
  # TODO: This is a rather limited manner of processing the config template; a more generic
  # method which can utilize environment variables directly, e.g. dockerize, would be better
  sed -i "s#^test.jars.dir=.*#test.jars.dir=$JARS_PATH#" test.cfg
  sed -i "s#^test.temp.dir=.*#test.temp.dir=$PWD/temp_test_dir#" test.cfg
  sed -i "s#^test.mongo.exe.*#test.mongo.exe=$MONGO_PATH#" test.cfg
  sed -i "s#^test.mongo.wired_tiger.*#test.mongo.wired_tiger=true#" test.cfg
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

install_jars
export JARS_PATH=$RETVAL

cleanup_bin_dir

install_test_config

install_python_dependencies
