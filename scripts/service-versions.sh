# TODO: these are copied from .github/workflows/non_sdk_test.yml
# Not sure this is worth centralizing as direct binary installation
# will soon be obsolete.
# Also note that mongo is platform-specific,so the values below are,
# in this case, specific to macOS
#export MONGODB_VER="mongodb-linux-x86_64-3.6.16"
export MONGODB_VER=mongodb-osx-ssl-x86_64-3.6.23
export MONGODB_VER_UNPACKED=mongodb-osx-x86_64-3.6.23
export ARANGODB_VER="3.5.1"
export ARANGODB_V="35"
export KAFKA_VER="2.8.1"
export SCALA_VER="2.12"