import os
import shutil

from arango import ArangoClient
from pytest import fixture

from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage
from test_support import test_utils
from test_support.constants import (
    TEST_COL_DATA_LINK,
    TEST_COL_NODE_EDGE,
    TEST_COL_NODES,
    TEST_COL_SAMPLE,
    TEST_COL_SCHEMA,
    TEST_COL_VER_EDGE,
    TEST_COL_VERSION,
    TEST_COL_WS_OBJ_VER,
    TEST_PWD,
    TEST_USER,
    ARANGODB_URL,
    ARANGODB_PORT,
    KAFKA_HOST,
    KAFKA_PORT, TEST_COL_EDGE, MONGO_PORT,
)
from test_support.constants import TEST_DB_NAME


def delete_test_db(arango_client):
    system_db = arango_client.db("_system")  # default access to _system db
    system_db.delete_database(TEST_DB_NAME)


def create_test_db(arango_client):
    system_db = arango_client.db("_system")  # default access to _system db
    try:
        system_db.delete_database(TEST_DB_NAME)
    except Exception:
        # we don't care if we had to zap a previous test database
        pass
    system_db.create_database(
        TEST_DB_NAME, [{"username": TEST_USER, "password": TEST_PWD}]
    )
    db = arango_client.db(TEST_DB_NAME, TEST_USER, TEST_PWD)
    reset_collections(db)
    return db


def reset_collections(db):
    drop_collections(db)
    create_collections(db)
    return db


def drop_collections(db):
    db.delete_collection(TEST_COL_EDGE, ignore_missing=True)
    db.delete_collection(TEST_COL_SAMPLE, ignore_missing=True)
    db.delete_collection(TEST_COL_VERSION, ignore_missing=True)
    db.delete_collection(TEST_COL_VER_EDGE, ignore_missing=True)
    db.delete_collection(TEST_COL_NODES, ignore_missing=True)
    db.delete_collection(TEST_COL_NODE_EDGE, ignore_missing=True)
    db.delete_collection(TEST_COL_DATA_LINK, ignore_missing=True)
    db.delete_collection(TEST_COL_WS_OBJ_VER, ignore_missing=True)
    db.delete_collection(TEST_COL_SCHEMA, ignore_missing=True)
    return db


def create_collections(db):
    db.create_collection(TEST_COL_SAMPLE)
    db.create_collection(TEST_COL_VERSION)
    db.create_collection(TEST_COL_VER_EDGE, edge=True)
    db.create_collection(TEST_COL_NODES)
    db.create_collection(TEST_COL_NODE_EDGE, edge=True)
    db.create_collection(TEST_COL_DATA_LINK, edge=True)
    db.create_collection(TEST_COL_WS_OBJ_VER)
    db.create_collection(TEST_COL_SCHEMA)
    return db


# def remove_all_files(directory):
#     shutil.rmtree(directory)
#     # for filename in os.listdir(directory):
#     #     file_path = os.path.join(directory, filename)
#     #     try:
#     #         if os.path.isfile(file_path) or os.path.islink(file_path):
#     #             os.unlink(file_path)
#     #         elif os.path.isdir(file_path):
#     #             shutil.rmtree(file_path)
#     #     except Exception as e:
#     #         print("Failed to delete %s. Reason: %s" % (file_path, e))


# TODO: Weird -- the ArangoSampleStorage class is just used for the
# side effects of the constructor.
def samplestorage_client(arango_client):
    return ArangoSampleStorage(
        arango_client.db(TEST_DB_NAME, TEST_USER, TEST_PWD),
        TEST_COL_SAMPLE,
        TEST_COL_VERSION,
        TEST_COL_VER_EDGE,
        TEST_COL_NODES,
        TEST_COL_NODE_EDGE,
        TEST_COL_WS_OBJ_VER,
        TEST_COL_DATA_LINK,
        TEST_COL_SCHEMA
    )


#
# Fixtures
#
# Generally, some fixtures can be reset for each test module, others need to
# be clean at the beginning of each test.
#
# We may be able to move some tests
#


@fixture(scope='session')
def temp_dir():
    """
       A session fixture, returns a temporary directory, as defined in the testing configuration
       (see test_utils.py).
       Upon testing session startup, ensures that it does not initially exist.
       Upon testing session teardown ensures that it is removed.
       """
    temporary_directory = test_utils.get_temp_dir()
    shutil.rmtree(temporary_directory, ignore_errors=True)
    os.mkdir(temporary_directory)
    yield temporary_directory

    if test_utils.get_delete_temp_files():
        shutil.rmtree(temporary_directory)

#
# @fixture(scope="function")
# def ensure_clean_temp_dir(temp_dir):
#     shutil.rmtree(temp_dir, ignore_errors=False)
#     os.mkdir(temp_dir)
#     yield temp_dir

# @fixture(scope="session")
# def temp_dir():
#     """
#     A session fixture, returns a temporary directory, as defined in the testing configuration
#     (see test_utils.py).
#     Upon testing session startup, ensures that it does not initially exist.
#     Upon testing session teardown ensures that it is removed.
#     """
#     tempdir = test_utils.get_temp_dir()
#     print('[temp_dir]', test_utils.get_temp_dir())
#     shutil.rmtree(test_utils.get_temp_dir(), ignore_errors=False)
#     yield tempdir
#
#     if test_utils.get_delete_temp_files():
#         shutil.rmtree(test_utils.get_temp_dir())


@fixture(scope="session")
def kafka_host():
    yield f"{KAFKA_HOST}"


@fixture(scope="session")
def kafka_port():
    yield KAFKA_PORT


@fixture(scope="session")
def mongo_port():
    yield MONGO_PORT


@fixture(scope="session")
def arango_client():
    client = ArangoClient(hosts=f"{ARANGODB_URL}")
    yield client


@fixture(scope="session")
def arango_port():
    yield ARANGODB_PORT



# @fixture(scope="session")
# def workspace_url():
#     yield f"{MOCK_SERVICES_URL}/services/ws"
#
#
# @fixture(scope="session")
# def auth_url():
#     yield f"{MOCK_SERVICES_URL}/services/auth"


@fixture(scope="session")
def testing_db(arango_client):
    yield create_test_db(arango_client)


# @fixture(scope="function")
# def sample_service(testing_db):
#     db = reset_collections(testing_db)
#     yield {"url": SAMPLE_SERVICE_URL, "db": db}


@fixture(scope="function")
def sample_service_db(testing_db):
    yield reset_collections(testing_db)


@fixture(scope="function")
def samplestorage(sample_service_db, arango_client):
    return samplestorage_client(arango_client)

