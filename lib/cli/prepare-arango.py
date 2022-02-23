import datetime
import time

import arango

ARANGO_HOST = "http://arangodb:8529"
TEST_DB_NAME = "test_db"
TEST_COL_SAMPLE = "samples_sample"
TEST_COL_VERSION = "samples_version"
TEST_COL_VER_EDGE = "samples_ver_edge"
TEST_COL_NODES = "samples_nodes"
TEST_COL_NODE_EDGE = "samples_nodes_edge"
TEST_COL_WS_OBJ_VER = "ws_object_version"
TEST_COL_DATA_LINK = "samples_data_link"
TEST_COL_SCHEMA = "samples_schema"
TEST_USER = "test"
TEST_PWD = "test123"

TIMEOUT = 60


def create_collections(db):
    db.create_collection(TEST_COL_SAMPLE)
    db.create_collection(TEST_COL_VERSION)
    db.create_collection(TEST_COL_VER_EDGE, edge=True)
    db.create_collection(TEST_COL_NODES)
    db.create_collection(TEST_COL_NODE_EDGE, edge=True)
    db.create_collection(TEST_COL_WS_OBJ_VER)
    db.create_collection(TEST_COL_DATA_LINK, edge=True)
    db.create_collection(TEST_COL_SCHEMA)


def create_test_db(arango_host, test_db_name, test_user, test_user_password):
    arango_client = arango.ArangoClient(hosts=arango_host)
    systemdb = arango_client.db(verify=True)  # default access to _system db
    systemdb.create_database(
        test_db_name, [{"username": test_user, "password": test_user_password}]
    )
    return arango_client.db(test_db_name, test_user, test_user_password)


def try_wait(fun, timeout):
    start = datetime.datetime.now().timestamp()
    while datetime.datetime.now().timestamp() - start < timeout:
        try:
            return fun()
        except Exception as ex:
            print("not ready, retrying")
            print(ex)
            time.sleep(1)
    raise Exception(
        f"Arango not ready after {datetime.datetime.now().timestamp() - start}s"
    )


def main():
    print("[prepare-arango] Preparing ArangoDB...")
    print("[prepare-arango] Waiting for ArangoDB to start...")

    def create_db():
        return create_test_db(ARANGO_HOST, TEST_DB_NAME, TEST_USER, TEST_PWD)

    db = try_wait(create_db, 60)

    try:
        create_collections(db)
    except Exception as ex:
        print("[prepare-arango] collections: I guess not :(", ex)

    print("[prepare-arango] DONE!")


if __name__ == "__main__":
    main()
