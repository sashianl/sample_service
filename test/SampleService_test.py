# These tests cover the integration of the entire system and do not go into details - that's
# what unit tests are for. As such, typically each method will get a single happy path test and
# a single unhappy path test unless otherwise warranted.

import os
import shutil
import tempfile
import requests
import time
from configparser import ConfigParser
from pytest import fixture
from threading import Thread

from core import test_utils
from core.test_utils import assert_ms_epoch_close_to_now
from arango_controller import ArangoController
from mongo_controller import MongoController
from auth_controller import AuthController

VER = '0.1.0-alpha1'

_AUTH_DB = 'test_auth_db'

TEST_DB_NAME = 'test_sample_service'
TEST_COL_SAMPLE = 'samples'
TEST_COL_VERSION = 'versions'
TEST_COL_VER_EDGE = 'ver_to_sample'
TEST_COL_NODES = 'nodes'
TEST_COL_NODE_EDGE = 'node_edges'
TEST_COL_SCHEMA = 'schema'
TEST_USER = 'user1'
TEST_PWD = 'password1'


def create_deploy_cfg(auth_port, arango_port):
    cfg = ConfigParser()
    ss = 'SampleService'
    cfg.add_section(ss)

    cfg[ss]['auth-service-url'] = (f'http://localhost:{auth_port}/testmode/' +
                                   'api/legacy/KBase/Sessions/Login')
    cfg[ss]['auth-service-url-allow-insecure'] = 'true'

    cfg[ss]['arango-url'] = f'http://localhost:{arango_port}'
    cfg[ss]['arango-db'] = TEST_DB_NAME
    cfg[ss]['arango-user'] = TEST_USER
    cfg[ss]['arango-pwd'] = TEST_PWD

    cfg[ss]['sample-collection'] = TEST_COL_SAMPLE
    cfg[ss]['version-collection'] = TEST_COL_VERSION
    cfg[ss]['version-edge-collection'] = TEST_COL_VER_EDGE
    cfg[ss]['node-collection'] = TEST_COL_NODES
    cfg[ss]['node-edge-collection'] = TEST_COL_NODE_EDGE
    cfg[ss]['schema-collection'] = TEST_COL_SCHEMA

    _, path = tempfile.mkstemp('.cfg', 'deploy-', dir=test_utils.get_temp_dir(), text=True)

    with open(path, 'w') as handle:
        cfg.write(handle)

    return path


USER1 = 'user1'
TOKEN1 = None


@fixture(scope='module')
def temp_file():
    tempdir = test_utils.get_temp_dir()
    yield tempdir

    if test_utils.get_delete_temp_files():
        shutil.rmtree(test_utils.get_temp_dir())


@fixture(scope='module')
def mongo(temp_file):
    mongoexe = test_utils.get_mongo_exe()
    tempdir = test_utils.get_temp_dir()
    wt = test_utils.get_use_wired_tiger()
    mongo = MongoController(mongoexe, tempdir, wt)
    wttext = ' with WiredTiger' if wt else ''
    print(f'running mongo {mongo.db_version}{wttext} on port {mongo.port} in dir {mongo.temp_dir}')

    yield mongo

    del_temp = test_utils.get_delete_temp_files()
    print(f'shutting down mongo, delete_temp_files={del_temp}')
    mongo.destroy(del_temp)


@fixture(scope='module')
def auth(mongo):
    global TOKEN1
    jd = test_utils.get_jars_dir()
    tempdir = test_utils.get_temp_dir()
    auth = AuthController(jd, f'localhost:{mongo.port}', _AUTH_DB, tempdir)
    print(f'running KBase Auth2 {auth.version} on port {auth.port} in dir {auth.temp_dir}')
    url = f'http://localhost:{auth.port}'
    test_utils.create_auth_user(url, USER1, 'display1')
    TOKEN1 = test_utils.create_auth_login_token(url, USER1)

    yield auth

    del_temp = test_utils.get_delete_temp_files()
    print(f'shutting down auth, delete_temp_files={del_temp}')
    auth.destroy(del_temp)


@fixture(scope='module')
def arango():
    arangoexe = test_utils.get_arango_exe()
    arangojs = test_utils.get_arango_js()
    tempdir = test_utils.get_temp_dir()
    arango = ArangoController(arangoexe, arangojs, tempdir)
    create_test_db(arango)
    print('running arango on port {} in dir {}'.format(arango.port, arango.temp_dir))
    yield arango

    del_temp = test_utils.get_delete_temp_files()
    print('shutting down arango, delete_temp_files={}'.format(del_temp))
    arango.destroy(del_temp)


def create_test_db(arango):
    systemdb = arango.client.db(verify=True)  # default access to _system db
    systemdb.create_database(TEST_DB_NAME, [{'username': TEST_USER, 'password': TEST_PWD}])
    return arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD)


def clear_db_and_recreate(arango):
    arango.clear_database(TEST_DB_NAME, drop_indexes=True)
    db = create_test_db(arango)
    db.create_collection(TEST_COL_SAMPLE)
    db.create_collection(TEST_COL_VERSION)
    db.create_collection(TEST_COL_VER_EDGE, edge=True)
    db.create_collection(TEST_COL_NODES)
    db.create_collection(TEST_COL_NODE_EDGE, edge=True)
    db.create_collection(TEST_COL_SCHEMA)
    return db


@fixture(scope='module')
def service(auth, arango):
    portint = test_utils.find_free_port()
    clear_db_and_recreate(arango)
    # this is completely stupid. The state is calculated on import so there's no way to
    # test the state creation normally.
    cfgpath = create_deploy_cfg(auth.port, arango.port)
    os.environ['KB_DEPLOYMENT_CONFIG'] = cfgpath
    from SampleService import SampleServiceServer
    Thread(target=SampleServiceServer.start_server, kwargs={'port': portint}, daemon=True).start()
    time.sleep(0.05)
    port = str(portint)
    print('running sample service at localhost:' + port)
    yield port

    # shutdown the server
    # SampleServiceServer.stop_server()  <-- this causes an error. the start & stop methods are
    # bugged. _proc is only set if newprocess=True


@fixture
def sample_port(service, arango):
    clear_db_and_recreate(arango)
    yield service


def test_status(sample_port):
    res = requests.post('http://localhost:' + sample_port, json={
        'method': 'SampleService.status',
        'params': [],
        'version': 1.1,
        'id': 1   # don't do this. This is bad practice
    })
    assert res.status_code == 200
    s = res.json()
    # print(s)
    assert len(s['result']) == 1  # results are always in a list
    assert s['result'][0]['state'] == 'OK'
    assert s['result'][0]['message'] == ""
    assert s['result'][0]['version'] == VER
    # ignore git url and hash, can change


def get_authorized_headers():
    return {'authorization': TOKEN1, 'accept': 'application/json'}


def test_create_and_get_sample_with_version(sample_port):
    url = f'http://localhost:{sample_port}'

    # verison 1
    ret = requests.post(url, headers=get_authorized_headers(), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      'meta_controlled': {'foo': {'bar': 'baz'}},
                                      'meta_user': {'a': {'b': 'c'}}
                                      }
                                     ]
                       }
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id_ = ret.json()['result'][0]['id']

    # version 2
    ret = requests.post(url, headers=get_authorized_headers(), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '68',
        'params': [{
            'sample': {'name': 'mysample2',
                       'id': id_,
                       'node_tree': [{'id': 'root2',
                                      'type': 'BioReplicate',
                                      'meta_controlled': {'foo': {'bar': 'bat'}},
                                      'meta_user': {'a': {'b': 'd'}}
                                      }
                                     ]
                       },
            'prior_version': 1
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 2

    # get version 1
    ret = requests.post(url, headers=get_authorized_headers(), json={
        'method': 'SampleService.get_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_, 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True
    j = ret.json()['result'][0]
    assert_ms_epoch_close_to_now(j['save_date'])
    del j['save_date']
    assert j == {
        'id': id_,
        'version': 1,
        'name': 'mysample',
        'node_tree': [{'id': 'root',
                       'parent': None,
                       'type': 'BioReplicate',
                       'meta_controlled': {'foo': {'bar': 'baz'}},
                       'meta_user': {'a': {'b': 'c'}}}]
    }

    # get version 2
    ret = requests.post(url, headers=get_authorized_headers(), json={
        'method': 'SampleService.get_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_}]
    })
    # print(ret.text)
    assert ret.ok is True
    j = ret.json()['result'][0]
    assert_ms_epoch_close_to_now(j['save_date'])
    del j['save_date']
    assert j == {
        'id': id_,
        'version': 2,
        'name': 'mysample2',
        'node_tree': [{'id': 'root2',
                       'parent': None,
                       'type': 'BioReplicate',
                       'meta_controlled': {'foo': {'bar': 'bat'}},
                       'meta_user': {'a': {'b': 'd'}}}]
    }
