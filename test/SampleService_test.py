# These tests cover the integration of the entire system and do not go into details - that's
# what unit tests are for. As such, typically each method will get a single happy path test and
# a single unhappy path test unless otherwise warranted.

# Tests of the auth user lookup and workspace wrapper code are at the bottom of the file.

import datetime
import json
import os
import tempfile
import requests
import time
import uuid
import yaml
import copy
from configparser import ConfigParser
from pytest import fixture, raises
from threading import Thread

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from SampleService.SampleServiceImpl import SampleService
from SampleService.core.errors import (
    MissingParameterError, NoSuchWorkspaceDataError, IllegalParameterError)
from SampleService.core.notification import KafkaNotifier
from SampleService.core.user_lookup import KBaseUserLookup, AdminPermission
from SampleService.core.user_lookup import InvalidTokenError, InvalidUserError
from SampleService.core.workspace import WS, WorkspaceAccessType, UPA
from SampleService.core.errors import UnauthorizedError, NoSuchUserError
from SampleService.core.user import UserID

from installed_clients.WorkspaceClient import Workspace as Workspace

from core import test_utils
from core.test_utils import (
    assert_ms_epoch_close_to_now,
    assert_exception_correct,
    find_free_port
)
from arango_controller import ArangoController
from mongo_controller import MongoController
from workspace_controller import WorkspaceController
from auth_controller import AuthController
from kafka_controller import KafkaController

# TODO should really test a start up for the case where the metadata validation config is not
# supplied, but that's almost never going to be the case and the code is trivial, so YAGNI

VER = '0.2.5'

_AUTH_DB = 'test_auth_db'
_WS_DB = 'test_ws_db'
_WS_TYPE_DB = 'test_ws_type_db'

TEST_DB_NAME = 'test_sample_service'
TEST_COL_SAMPLE = 'samples'
TEST_COL_VERSION = 'versions'
TEST_COL_VER_EDGE = 'ver_to_sample'
TEST_COL_NODES = 'nodes'
TEST_COL_NODE_EDGE = 'node_edges'
TEST_COL_DATA_LINK = 'data_link'
TEST_COL_WS_OBJ_VER = 'ws_obj_ver_shadow'
TEST_COL_SCHEMA = 'schema'
TEST_USER = 'user1'
TEST_PWD = 'password1'

USER_WS_READ_ADMIN = 'wsreadadmin'
TOKEN_WS_READ_ADMIN = None
USER_WS_FULL_ADMIN = 'wsfulladmin'
TOKEN_WS_FULL_ADMIN = None
WS_READ_ADMIN = 'WS_READ_ADMIN'
WS_FULL_ADMIN = 'WS_FULL_ADMIN'

USER_SERVICE = 'serviceuser'
TOKEN_SERVICE = None

USER1 = 'user1'
TOKEN1 = None
USER2 = 'user2'
TOKEN2 = None
USER3 = 'user3'
TOKEN3 = None
USER4 = 'user4'
TOKEN4 = None
USER5 = 'user5'
TOKEN5 = None

USER_NO_TOKEN1 = 'usernt1'
USER_NO_TOKEN2 = 'usernt2'
USER_NO_TOKEN3 = 'usernt3'

KAFKA_TOPIC = 'sampleservice'


def create_deploy_cfg(auth_port, arango_port, workspace_port, kafka_port):
    cfg = ConfigParser()
    ss = 'SampleService'
    cfg.add_section(ss)

    cfg[ss]['auth-service-url'] = (f'http://localhost:{auth_port}/testmode/' +
                                   'api/legacy/KBase/Sessions/Login')
    cfg[ss]['auth-service-url-allow-insecure'] = 'true'

    cfg[ss]['auth-root-url'] = f'http://localhost:{auth_port}/testmode'
    cfg[ss]['auth-token'] = TOKEN_SERVICE
    cfg[ss]['auth-read-admin-roles'] = 'readadmin1'
    cfg[ss]['auth-full-admin-roles'] = 'fulladmin2'

    cfg[ss]['arango-url'] = f'http://localhost:{arango_port}'
    cfg[ss]['arango-db'] = TEST_DB_NAME
    cfg[ss]['arango-user'] = TEST_USER
    cfg[ss]['arango-pwd'] = TEST_PWD

    cfg[ss]['workspace-url'] = f'http://localhost:{workspace_port}'
    cfg[ss]['workspace-read-admin-token'] = TOKEN_WS_READ_ADMIN

    cfg[ss]['kafka-bootstrap-servers'] = f'localhost:{kafka_port}'
    cfg[ss]['kafka-topic'] = KAFKA_TOPIC

    cfg[ss]['sample-collection'] = TEST_COL_SAMPLE
    cfg[ss]['version-collection'] = TEST_COL_VERSION
    cfg[ss]['version-edge-collection'] = TEST_COL_VER_EDGE
    cfg[ss]['node-collection'] = TEST_COL_NODES
    cfg[ss]['node-edge-collection'] = TEST_COL_NODE_EDGE
    cfg[ss]['data-link-collection'] = TEST_COL_DATA_LINK
    cfg[ss]['workspace-object-version-shadow-collection'] = TEST_COL_WS_OBJ_VER
    cfg[ss]['schema-collection'] = TEST_COL_SCHEMA

    metacfg = {
        'validators': {
            'foo': {'validators': [{'module': 'SampleService.core.validator.builtin',
                                    'callable_builder': 'noop'
                                    }],
                    'key_metadata': {'a': 'b', 'c': 'd'}
                    },
            'stringlentest': {'validators': [{'module': 'SampleService.core.validator.builtin',
                                              'callable_builder': 'string',
                                              'parameters': {'max-len': 5}
                                              },
                                             {'module': 'SampleService.core.validator.builtin',
                                              'callable_builder': 'string',
                                              'parameters': {'keys': 'spcky', 'max-len': 2}
                                              }],
                              'key_metadata': {'h': 'i', 'j': 'k'}
                              }
        },
        'prefix_validators': {
            'pre': {'validators': [{'module': 'core.config_test_vals',
                                    'callable_builder': 'prefix_validator_test_builder',
                                    'parameters': {'fail_on_arg': 'fail_plz'}
                                    }],
                    'key_metadata': {'1': '2'}
                    }
        }
    }
    metaval = tempfile.mkstemp('.cfg', 'metaval-', dir=test_utils.get_temp_dir(), text=True)
    os.close(metaval[0])

    with open(metaval[1], 'w') as handle:
        yaml.dump(metacfg, handle)

    cfg[ss]['metadata-validator-config-url'] = f'file://{metaval[1]}'

    deploy = tempfile.mkstemp('.cfg', 'deploy-', dir=test_utils.get_temp_dir(), text=True)
    os.close(deploy[0])

    with open(deploy[1], 'w') as handle:
        cfg.write(handle)

    return deploy[1]


@fixture(scope='module')
def mongo():
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
    global TOKEN_SERVICE
    global TOKEN_WS_FULL_ADMIN
    global TOKEN_WS_READ_ADMIN
    global TOKEN1
    global TOKEN2
    global TOKEN3
    global TOKEN4
    global TOKEN5
    jd = test_utils.get_jars_dir()
    tempdir = test_utils.get_temp_dir()
    auth = AuthController(jd, f'localhost:{mongo.port}', _AUTH_DB, tempdir)
    print(f'Started KBase Auth2 {auth.version} on port {auth.port} ' +
          f'in dir {auth.temp_dir} in {auth.startup_count}s')
    url = f'http://localhost:{auth.port}'

    test_utils.create_auth_role(url, 'fulladmin1', 'fa1')
    test_utils.create_auth_role(url, 'fulladmin2', 'fa2')
    test_utils.create_auth_role(url, 'readadmin1', 'ra1')
    test_utils.create_auth_role(url, 'readadmin2', 'ra2')
    test_utils.create_auth_role(url, WS_READ_ADMIN, 'wsr')
    test_utils.create_auth_role(url, WS_FULL_ADMIN, 'wsf')

    test_utils.create_auth_user(url, USER_SERVICE, 'serv')
    TOKEN_SERVICE = test_utils.create_auth_login_token(url, USER_SERVICE)

    test_utils.create_auth_user(url, USER_WS_READ_ADMIN, 'wsra')
    TOKEN_WS_READ_ADMIN = test_utils.create_auth_login_token(url, USER_WS_READ_ADMIN)
    test_utils.set_custom_roles(url, USER_WS_READ_ADMIN, [WS_READ_ADMIN])

    test_utils.create_auth_user(url, USER_WS_FULL_ADMIN, 'wsrf')
    TOKEN_WS_FULL_ADMIN = test_utils.create_auth_login_token(url, USER_WS_FULL_ADMIN)
    test_utils.set_custom_roles(url, USER_WS_FULL_ADMIN, [WS_FULL_ADMIN])

    test_utils.create_auth_user(url, USER1, 'display1')
    TOKEN1 = test_utils.create_auth_login_token(url, USER1)
    test_utils.set_custom_roles(url, USER1, ['fulladmin1'])

    test_utils.create_auth_user(url, USER2, 'display2')
    TOKEN2 = test_utils.create_auth_login_token(url, USER2)
    test_utils.set_custom_roles(url, USER2, ['fulladmin1', 'fulladmin2', 'readadmin2'])

    test_utils.create_auth_user(url, USER3, 'display3')
    TOKEN3 = test_utils.create_auth_login_token(url, USER3)
    test_utils.set_custom_roles(url, USER3, ['readadmin1'])

    test_utils.create_auth_user(url, USER4, 'display4')
    TOKEN4 = test_utils.create_auth_login_token(url, USER4)

    test_utils.create_auth_user(url, USER5, 'display5')
    TOKEN5 = test_utils.create_auth_login_token(url, USER5)
    test_utils.set_custom_roles(url, USER5, ['fulladmin2'])

    test_utils.create_auth_user(url, USER_NO_TOKEN1, 'displaynt1')
    test_utils.create_auth_user(url, USER_NO_TOKEN2, 'displaynt2')
    test_utils.create_auth_user(url, USER_NO_TOKEN3, 'displaynt3')

    yield auth

    del_temp = test_utils.get_delete_temp_files()
    print(f'shutting down auth, delete_temp_files={del_temp}')
    auth.destroy(del_temp)


@fixture(scope='module')
def workspace(auth, mongo):
    jd = test_utils.get_jars_dir()
    tempdir = test_utils.get_temp_dir()
    ws = WorkspaceController(
        jd,
        mongo,
        _WS_DB,
        _WS_TYPE_DB,
        f'http://localhost:{auth.port}/testmode',
        tempdir)
    print(f'Started KBase Workspace {ws.version} on port {ws.port} ' +
          f'in dir {ws.temp_dir} in {ws.startup_count}s')

    wsc = Workspace(f'http://localhost:{ws.port}', token=TOKEN_WS_FULL_ADMIN)
    wsc.request_module_ownership('Trivial')
    wsc.administer({'command': 'approveModRequest', 'module': 'Trivial'})
    wsc.register_typespec({
        'spec': '''
                module Trivial {

                    /* @optional dontusethisfieldorifyoudomakesureitsastring */
                    typedef structure {
                        string dontusethisfieldorifyoudomakesureitsastring;
                    } Object;

                    /* @optional dontusethisfieldorifyoudomakesureitsastring */
                    typedef structure {
                        string dontusethisfieldorifyoudomakesureitsastring;
                    } Object2;
                };
                ''',
        'dryrun': 0,
        'new_types': ['Object', 'Object2']
    })
    wsc.release_module('Trivial')

    yield ws

    del_temp = test_utils.get_delete_temp_files()
    print(f'shutting down workspace, delete_temp_files={del_temp}')
    ws.destroy(del_temp, False)


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
    db.create_collection(TEST_COL_DATA_LINK, edge=True)
    db.create_collection(TEST_COL_WS_OBJ_VER)
    db.create_collection(TEST_COL_SCHEMA)
    return db


@fixture(scope='module')
def kafka():
    kafka_bin_dir = test_utils.get_kafka_bin_dir()
    tempdir = test_utils.get_temp_dir()
    kc = KafkaController(kafka_bin_dir, tempdir)
    print('running kafka on port {} in dir {}'.format(kc.port, kc.temp_dir))
    yield kc

    del_temp = test_utils.get_delete_temp_files()
    print('shutting down kafka, delete_temp_files={}'.format(del_temp))
    kc.destroy(del_temp, dump_logs_to_stdout=False)


@fixture(scope='module')
def service(auth, arango, workspace, kafka):
    portint = test_utils.find_free_port()
    clear_db_and_recreate(arango)
    # this is completely stupid. The state is calculated on import so there's no way to
    # test the state creation normally.
    cfgpath = create_deploy_cfg(auth.port, arango.port, workspace.port, kafka.port)
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
def sample_port(service, arango, workspace, kafka):
    clear_db_and_recreate(arango)
    workspace.clear_db()
    # _clear_kafka_messages(kafka)  # too expensive to run after every test
    # kafka.clear_all_topics()  # too expensive to run after every test
    yield service


def test_init_fail():
    # init success is tested via starting the server
    init_fail(None, ValueError('config is empty, cannot start service'))
    cfg = {}
    init_fail(cfg, ValueError('config is empty, cannot start service'))
    cfg['arango-url'] = None
    init_fail(cfg, MissingParameterError('config param arango-url'))
    cfg['arango-url'] = 'crap'
    init_fail(cfg, MissingParameterError('config param arango-db'))
    cfg['arango-db'] = 'crap'
    init_fail(cfg, MissingParameterError('config param arango-user'))
    cfg['arango-user'] = 'crap'
    init_fail(cfg, MissingParameterError('config param arango-pwd'))
    cfg['arango-pwd'] = 'crap'
    init_fail(cfg, MissingParameterError('config param sample-collection'))
    cfg['sample-collection'] = 'crap'
    init_fail(cfg, MissingParameterError('config param version-collection'))
    cfg['version-collection'] = 'crap'
    init_fail(cfg, MissingParameterError('config param version-edge-collection'))
    cfg['version-edge-collection'] = 'crap'
    init_fail(cfg, MissingParameterError('config param node-collection'))
    cfg['node-collection'] = 'crap'
    init_fail(cfg, MissingParameterError('config param node-edge-collection'))
    cfg['node-edge-collection'] = 'crap'
    init_fail(cfg, MissingParameterError('config param data-link-collection'))
    cfg['data-link-collection'] = 'crap'
    init_fail(cfg, MissingParameterError(
        'config param workspace-object-version-shadow-collection'))
    cfg['workspace-object-version-shadow-collection'] = 'crap'
    init_fail(cfg, MissingParameterError('config param schema-collection'))
    cfg['schema-collection'] = 'crap'
    init_fail(cfg, MissingParameterError('config param auth-root-url'))
    cfg['auth-root-url'] = 'crap'
    init_fail(cfg, MissingParameterError('config param auth-token'))
    cfg['auth-token'] = 'crap'
    init_fail(cfg, MissingParameterError('config param workspace-url'))
    cfg['workspace-url'] = 'crap'
    init_fail(cfg, MissingParameterError('config param workspace-read-admin-token'))
    cfg['workspace-read-admin-token'] = 'crap'
    cfg['kafka-bootstrap-servers'] = 'crap'
    init_fail(cfg, MissingParameterError('config param kafka-topic'))
    cfg['kafka-topic'] = 'crap'
    # get_validators is tested elsewhere, just make sure it'll error out
    cfg['metadata-validator-config-url'] = 'https://kbase.us/services'
    init_fail(cfg, ValueError(
        'Failed to open validator configuration file at https://kbase.us/services: Not Found'))


def init_fail(config, expected):
    with raises(Exception) as got:
        SampleService(config)
    assert_exception_correct(got.value, expected)


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
    assert_ms_epoch_close_to_now(s['result'][0]['servertime'])
    assert s['result'][0]['state'] == 'OK'
    assert s['result'][0]['message'] == ""
    assert s['result'][0]['version'] == VER
    # ignore git url and hash, can change


def get_authorized_headers(token):
    headers = {'accept': 'application/json'}
    if token is not None:
        headers['authorization'] = token
    return headers


def _check_kafka_messages(kafka, expected_msgs, topic=KAFKA_TOPIC, print_res=False):
    kc = KafkaConsumer(
        topic,
        bootstrap_servers=f'localhost:{kafka.port}',
        auto_offset_reset='earliest',
        group_id='foo')  # quiets warnings

    try:
        res = kc.poll(timeout_ms=2000)  # 1s not enough? Seems like a lot
        if print_res:
            print(res)
        assert len(res) == 1
        assert next(iter(res.keys())).topic == topic
        records = next(iter(res.values()))
        assert len(records) == len(expected_msgs)
        for i, r in enumerate(records):
            assert json.loads(r.value) == expected_msgs[i]
        # Need to commit here? doesn't seem like it
    finally:
        kc.close()


def _clear_kafka_messages(kafka, topic=KAFKA_TOPIC):
    kc = KafkaConsumer(
        topic,
        bootstrap_servers=f'localhost:{kafka.port}',
        auto_offset_reset='earliest',
        group_id='foo')  # quiets warnings

    try:
        kc.poll(timeout_ms=2000)  # 1s not enough? Seems like a lot
        # Need to commit here? doesn't seem like it
    finally:
        kc.close()


def test_create_and_get_sample_with_version(sample_port, kafka):
    _clear_kafka_messages(kafka)
    url = f'http://localhost:{sample_port}'

    # version 1
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      'meta_controlled': {'foo': {'bar': 'baz'},
                                                          'stringlentest': {'foooo': 'barrr',
                                                                            'spcky': 'fa'},
                                                          'prefixed': {'safe': 'args'}
                                                          },
                                      'meta_user': {'a': {'b': 'c'}},
                                      'source_meta': [
                                          {'key': 'foo', 'skey': 'bar', 'svalue': {'whee': 'whoo'}},
                                          {'key': 'stringlentest',
                                           'skey': 'ya fer sure',
                                           'svalue': {'just': 'some', 'data': 42}}
                                          ]
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
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
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
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
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
        'user': USER1,
        'name': 'mysample',
        'node_tree': [{'id': 'root',
                       'parent': None,
                       'type': 'BioReplicate',
                       'meta_controlled': {'foo': {'bar': 'baz'},
                                           'stringlentest': {'foooo': 'barrr',
                                                             'spcky': 'fa'},
                                           'prefixed': {'safe': 'args'}
                                           },
                       'meta_user': {'a': {'b': 'c'}},
                       'source_meta': [
                            {'key': 'foo', 'skey': 'bar', 'svalue': {'whee': 'whoo'}},
                            {'key': 'stringlentest',
                             'skey': 'ya fer sure',
                             'svalue': {'just': 'some', 'data': 42}}
                             ],
                       }]
    }

    # get version 2
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.get_sample',
        'version': '1.1',
        'id': '43',
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
        'user': USER1,
        'name': 'mysample2',
        'node_tree': [{'id': 'root2',
                       'parent': None,
                       'type': 'BioReplicate',
                       'meta_controlled': {'foo': {'bar': 'bat'}},
                       'meta_user': {'a': {'b': 'd'}},
                       'source_meta': [],
                       }]
    }

    _check_kafka_messages(
        kafka,
        [
            {'event_type': 'NEW_SAMPLE', 'sample_id': id_, 'sample_ver': 1},
            {'event_type': 'NEW_SAMPLE', 'sample_id': id_, 'sample_ver': 2}
        ])


def test_create_and_get_samples(sample_port, kafka):
    _clear_kafka_messages(kafka)
    url = f'http://localhost:{sample_port}'

    # first sample
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      'meta_controlled': {'foo': {'bar': 'baz'},
                                                          'stringlentest': {'foooo': 'barrr',
                                                                            'spcky': 'fa'},
                                                          'prefixed': {'safe': 'args'}
                                                          },
                                      'meta_user': {'a': {'b': 'c'}},
                                      'source_meta': [
                                          {'key': 'foo', 'skey': 'bar', 'svalue': {'whee': 'whoo'}},
                                          {'key': 'stringlentest',
                                           'skey': 'ya fer sure',
                                           'svalue': {'just': 'some', 'data': 42}}
                                          ]
                                      }
                                     ]
                       }
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id1_ = ret.json()['result'][0]['id']

    # second sample
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '68',
        'params': [{
            'sample': {'name': 'mysample2',
                       'node_tree': [{'id': 'root2',
                                      'type': 'BioReplicate',
                                      'meta_controlled': {'foo': {'bar': 'bat'}},
                                      'meta_user': {'a': {'b': 'd'}}
                                      }
                                     ]
                       }
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id2_ = ret.json()['result'][0]['id']

    # get both samples
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.get_samples',
        'version': '1.1',
        'id': '42',
        'params': [{'samples': [{'id': id1_, 'version': 1}, {'id': id2_, 'version': 1}]}]
    })
    # print(ret.text)
    assert ret.ok is True
    j = ret.json()['result'][0]
    for s in j:
        assert_ms_epoch_close_to_now(s['save_date'])
        del s['save_date']
    print('-'*80)
    import json
    print(json.dumps(j))
    print('-'*80)

    assert j == [{
        'id': id1_,
        'version': 1,
        'user': USER1,
        'name': 'mysample',
        'node_tree': [{
           'id': 'root',
           'parent': None,
           'type': 'BioReplicate',
           'meta_controlled': {'foo': {'bar': 'baz'},
                               'stringlentest': {'foooo': 'barrr',
                                                 'spcky': 'fa'},
                               'prefixed': {'safe': 'args'}
                               },
           'meta_user': {'a': {'b': 'c'}},
           'source_meta': [
                {'key': 'foo', 'skey': 'bar', 'svalue': {'whee': 'whoo'}},
                {'key': 'stringlentest',
                 'skey': 'ya fer sure',
                 'svalue': {'just': 'some', 'data': 42}}
                 ],
        }]
    }, {
        'id': id2_,
        'version': 1,
        'user': USER1,
        'name': 'mysample2',
        'node_tree': [{'id': 'root2',
            'parent': None,
            'type': 'BioReplicate',
            'meta_controlled': {'foo': {'bar': 'bat'}},
            'meta_user': {'a': {'b': 'd'}},
            'source_meta': []
        }]
    }]
    _check_kafka_messages(
        kafka,
        [
            {'event_type': 'NEW_SAMPLE', 'sample_id': id1_, 'sample_ver': 1},
            {'event_type': 'NEW_SAMPLE', 'sample_id': id2_, 'sample_ver': 1}
        ])


def test_create_sample_as_admin(sample_port):
    _create_sample_as_admin(sample_port, None, TOKEN2, USER2)


def test_create_sample_as_admin_impersonate_user(sample_port):
    _create_sample_as_admin(sample_port, '     ' + USER4 + '      ', TOKEN4, USER4)


def _create_sample_as_admin(sample_port, as_user, get_token, expected_user):
    url = f'http://localhost:{sample_port}'

    # verison 1
    ret = requests.post(url, headers=get_authorized_headers(TOKEN2), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      'meta_controlled': {'foo': {'bar': 'baz'}
                                                          },
                                      'meta_user': {'a': {'b': 'c'}}
                                      }
                                     ]
                       },
            'as_admin': 1,
            'as_user': as_user
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id_ = ret.json()['result'][0]['id']

    # get
    ret = requests.post(url, headers=get_authorized_headers(get_token), json={
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
        'user': expected_user,
        'name': 'mysample',
        'node_tree': [{'id': 'root',
                       'parent': None,
                       'type': 'BioReplicate',
                       'meta_controlled': {'foo': {'bar': 'baz'}
                                           },
                       'meta_user': {'a': {'b': 'c'}},
                       'source_meta': [],
                       }]
    }


def test_create_sample_version_as_admin(sample_port):
    _create_sample_version_as_admin(sample_port, None, USER2)


def test_create_sample_version_as_admin_impersonate_user(sample_port):
    _create_sample_version_as_admin(sample_port, USER3, USER3)


def _create_sample_version_as_admin(sample_port, as_user, expected_user):
    url = f'http://localhost:{sample_port}'

    # verison 1
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      'meta_controlled': {'foo': {'bar': 'baz'},
                                                          'stringlentest': {'foooo': 'barrr',
                                                                            'spcky': 'fa'},
                                                          'prefixed': {'safe': 'args'}
                                                          },
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
    ret = requests.post(url, headers=get_authorized_headers(TOKEN2), json={
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
            'as_admin': 1,
            'as_user': as_user
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 2

    # get version 2
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.get_sample',
        'version': '1.1',
        'id': '43',
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
        'user': expected_user,
        'name': 'mysample2',
        'node_tree': [{'id': 'root2',
                       'parent': None,
                       'type': 'BioReplicate',
                       'meta_controlled': {'foo': {'bar': 'bat'}},
                       'meta_user': {'a': {'b': 'd'}},
                       'source_meta': [],
                       }]
    }


def test_get_samples_fail_no_samples(sample_port):
    _test_get_samples_fail(sample_port, None,
        'Missing or incorrect "samples" field - must provide a list of samples to retrieve.')

    _test_get_samples_fail(sample_port, "im a random sample id string!",
        'Missing or incorrect "samples" field - must provide a list of samples to retrieve.')

    _test_get_samples_fail(sample_port, [],
        'Cannot provide empty list of samples - must provide at least one sample to retrieve.')


def _test_get_samples_fail(sample_port, samples, message):
    params = {'samples': samples}
    _request_fail(sample_port, 'get_samples', TOKEN1, params, message)

def test_get_sample_public_read(sample_port):
    url = f'http://localhost:{sample_port}'
    id_ = _create_generic_sample(url, TOKEN1)

    _replace_acls(url, id_, TOKEN1, {'public_read': 1})

    for token in [TOKEN4, None]:  # unauthed user and anonymous user
        s = _get_sample(url, token, id_)
        assert_ms_epoch_close_to_now(s['save_date'])
        del s['save_date']
        assert s == {
            'id': id_,
            'version': 1,
            'user': 'user1',
            'name': 'mysample',
            'node_tree': [{'id': 'root',
                           'parent': None,
                           'type': 'BioReplicate',
                           'meta_controlled': {},
                           'meta_user': {},
                           'source_meta': [],
                           },
                          {'id': 'foo',
                           'parent': 'root',
                           'type': 'TechReplicate',
                           'meta_controlled': {},
                           'meta_user': {},
                           'source_meta': [],
                           }
                          ]
        }


def _get_sample(url, token, id_):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_sample',
        'version': '1.1',
        'id': '43',
        'params': [{'id': str(id_)}]
    })
    # print(ret.text)
    assert ret.ok is True
    return ret.json()['result'][0]


def test_get_sample_as_admin(sample_port):
    url = f'http://localhost:{sample_port}'

    # verison 1
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      'meta_controlled': {'foo': {'bar': 'baz'}
                                                          },
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

    # token3 has read admin but not full admin
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_, 'version': 1, 'as_admin': 1}]
    })
    print(ret.text)
    assert ret.ok is True
    j = ret.json()['result'][0]
    assert_ms_epoch_close_to_now(j['save_date'])
    del j['save_date']
    assert j == {
        'id': id_,
        'version': 1,
        'user': USER1,
        'name': 'mysample',
        'node_tree': [{'id': 'root',
                       'parent': None,
                       'type': 'BioReplicate',
                       'meta_controlled': {'foo': {'bar': 'baz'},
                                           },
                       'meta_user': {'a': {'b': 'c'}},
                       'source_meta': [],
                       }]
    }


def test_create_sample_fail_no_nodes(sample_port):
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': None
                       }
        }]
    })
    # print(ret.text)
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        'Sample service error code 30001 Illegal input parameter: sample node tree ' +
        'must be present and a list')


def test_create_sample_fail_bad_metadata(sample_port):
    _create_sample_fail_bad_metadata(
        sample_port, {'stringlentest': {}},
        'Sample service error code 30001 Illegal input parameter: Error for node at index 0: ' +
        'Controlled metadata value associated with metadata key stringlentest is null or empty')
    _create_sample_fail_bad_metadata(
        sample_port, {'stringlentest': {'foooo': 'barrrr'}},
        'Sample service error code 30010 Metadata validation failed: Node at index 0: ' +
        'Key stringlentest: Metadata value at key foooo is longer than max length of 5')
    _create_sample_fail_bad_metadata(
        sample_port, {'stringlentest': {'foooo': 'barrr', 'spcky': 'baz'}},
        'Sample service error code 30010 Metadata validation failed: Node at index 0: Key ' +
        'stringlentest: Metadata value at key spcky is longer than max length of 2')
    _create_sample_fail_bad_metadata(
        sample_port, {'prefix': {'fail_plz': 'yes, or principal sayof'}},
        "Sample service error code 30010 Metadata validation failed: Node at index 0: " +
        "Prefix validator pre, key prefix: pre, prefix, {'fail_plz': 'yes, or principal sayof'}")

    _create_sample_fail_bad_metadata(
        sample_port, {'prefix': {'foo': 'bar'}},
        'Sample service error code 30001 Illegal input parameter: Error for node at ' +
        'index 0: Duplicate source metadata key: prefix',
        sourcemeta=[
            {'key': 'prefix', 'skey': 'a', 'svalue': {'a': 'b'}},
            {'key': 'prefix', 'skey': 'b', 'svalue': {'c': 'd'}}
            ])


def _create_sample_fail_bad_metadata(sample_port, meta, expected, sourcemeta=None):
    url = f'http://localhost:{sample_port}'
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      'meta_controlled': meta,
                                      'source_meta': sourcemeta
                                      }
                                     ]
                       }
        }]
    })

    # print(ret.text)
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


def test_create_sample_fail_permissions(sample_port):
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      }
                                     ]
                       }
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id_ = ret.json()['result'][0]['id']

    _replace_acls(url, id_, TOKEN1, {'read': [USER2]})

    ret = requests.post(url, headers=get_authorized_headers(TOKEN2), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'id': id_,
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      }
                                     ]
                       }
        }]
    })

    # print(ret.text)
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        f'Sample service error code 20000 Unauthorized: User user2 cannot write to sample {id_}')


def test_create_sample_fail_admin_bad_user_name(sample_port):
    _create_sample_fail_admin_as_user(
        sample_port, 'bad\tuser',
        'Sample service error code 30001 Illegal input parameter: userid contains ' +
        'control characters')


def test_create_sample_fail_admin_no_such_user(sample_port):
    _create_sample_fail_admin_as_user(
        sample_port, USER4 + 'impostor',
        'Sample service error code 50000 No such user: user4impostor')


def _create_sample_fail_admin_as_user(sample_port, user, expected):
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, headers=get_authorized_headers(TOKEN2), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      }
                                     ]
                       },
            'as_admin': 'true',
            'as_user': user
        }]
    })

    # print(ret.text)
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


def test_create_sample_fail_admin_permissions(sample_port):
    url = f'http://localhost:{sample_port}'

    # token 3 only has read permissions
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      }
                                     ]
                       },
            'as_admin': 1,
            'as_user': USER4
        }]
    })

    # print(ret.text)
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        'Sample service error code 20000 Unauthorized: User user3 does not have the ' +
        'necessary administration privileges to run method create_sample')


def test_get_sample_fail_bad_id(sample_port):
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      }
                                     ]
                       }
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id_ = ret.json()['result'][0]['id']

    ret = requests.post(url, headers=get_authorized_headers(TOKEN2), json={
        'method': 'SampleService.get_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_[:-1]}]
    })

    # print(ret.text)
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        'Sample service error code 30001 Illegal input parameter: ' +
        f'id {id_[:-1]} must be a UUID string')


def test_get_sample_fail_permissions(sample_port):
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      }
                                     ]
                       }
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id_ = ret.json()['result'][0]['id']

    _get_sample_fail(
        url, TOKEN2, {'id': id_},
        f'Sample service error code 20000 Unauthorized: User user2 cannot read sample {id_}')

    _get_sample_fail(
        url, None, {'id': id_},
        f'Sample service error code 20000 Unauthorized: Anonymous users cannot read sample {id_}')

    _get_sample_fail(
        url, None, {'id': id_, 'as_admin': 1},
        'Sample service error code 20000 Unauthorized: Anonymous users ' +
        'may not act as service administrators.')


def test_get_sample_fail_admin_permissions(sample_port):
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      }
                                     ]
                       }
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id_ = ret.json()['result'][0]['id']

    _get_sample_fail(
        url, TOKEN4, {'id': id_, 'as_admin': 1},
        'Sample service error code 20000 Unauthorized: User user4 does not have the ' +
        'necessary administration privileges to run method get_sample')


def _get_sample_fail(url, token, params, expected):

    # user 4 has no admin permissions
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_sample',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })

    # print(ret.text)
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


def test_get_and_replace_acls(sample_port, kafka):
    _clear_kafka_messages(kafka)
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      }
                                     ]
                       }
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id_ = ret.json()['result'][0]['id']

    _assert_acl_contents(url, id_, TOKEN1, {
        'owner': USER1,
        'admin': [],
        'write': [],
        'read': [],
        'public_read': 0
    })

    _replace_acls(url, id_, TOKEN1, {
        'admin': [USER2],
        'write': [USER_NO_TOKEN1, USER_NO_TOKEN2, USER3],
        'read': [USER_NO_TOKEN3, USER4]
    })

    # test that people in the acls can read
    for token in [TOKEN2, TOKEN3, TOKEN4]:
        _assert_acl_contents(url, id_, token, {
            'owner': USER1,
            'admin': [USER2],
            'write': [USER3, USER_NO_TOKEN1, USER_NO_TOKEN2],
            'read': [USER4, USER_NO_TOKEN3],
            'public_read': 0
        })

        ret = requests.post(url, headers=get_authorized_headers(token), json={
            'method': 'SampleService.get_sample',
            'version': '1.1',
            'id': '42',
            'params': [{'id': id_}]
        })
        # print(ret.text)
        assert ret.ok is True
        j = ret.json()['result'][0]
        del j['save_date']
        assert j == {
            'id': id_,
            'version': 1,
            'user': USER1,
            'name': 'mysample',
            'node_tree': [{
                'id': 'root',
                'type': 'BioReplicate',
                'parent': None,
                'meta_controlled': {},
                'meta_user': {},
                'source_meta': [],
                }]
        }

    # test admins and writers can write
    for token, version in ((TOKEN2, 2), (TOKEN3, 3)):
        ret = requests.post(url, headers=get_authorized_headers(token), json={
            'method': 'SampleService.create_sample',
            'version': '1.1',
            'id': '68',
            'params': [{
                'sample': {'name': f'mysample{version}',
                           'id': id_,
                           'node_tree': [{'id': f'root{version}',
                                          'type': 'BioReplicate',
                                          }
                                         ]
                           }
            }]
        })
        # print(ret.text)
        assert ret.ok is True
        assert ret.json()['result'][0]['version'] == version

    # check one of the writes
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.get_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_, 'version': 2}]
    })
    # print(ret.text)
    assert ret.ok is True
    j = ret.json()['result'][0]
    assert_ms_epoch_close_to_now(j['save_date'])
    del j['save_date']
    assert j == {
        'id': id_,
        'version': 2,
        'user': USER2,
        'name': 'mysample2',
        'node_tree': [{'id': 'root2',
                       'parent': None,
                       'type': 'BioReplicate',
                       'meta_controlled': {},
                       'meta_user': {},
                       'source_meta': [],
                       }]
    }

    # test that an admin can replace ACLs
    _replace_acls(url, id_, TOKEN2, {
        'admin': [USER_NO_TOKEN2],
        'write': [],
        'read': [USER2],
        'public_read': 1
    })

    _assert_acl_contents(url, id_, TOKEN1, {
        'owner': USER1,
        'admin': [USER_NO_TOKEN2],
        'write': [],
        'read': [USER2],
        'public_read': 1
    })

    _check_kafka_messages(
        kafka,
        [
            {'event_type': 'NEW_SAMPLE', 'sample_id': id_, 'sample_ver': 1},
            {'event_type': 'ACL_CHANGE', 'sample_id': id_},
            {'event_type': 'NEW_SAMPLE', 'sample_id': id_, 'sample_ver': 2},
            {'event_type': 'NEW_SAMPLE', 'sample_id': id_, 'sample_ver': 3},
            {'event_type': 'ACL_CHANGE', 'sample_id': id_},
        ])


def test_get_acls_public_read(sample_port):
    url = f'http://localhost:{sample_port}'
    id_ = _create_generic_sample(url, TOKEN1)

    _replace_acls(url, id_, TOKEN1, {'public_read': 1})

    for token in [TOKEN4, None]:  # user with no explicit perms and anon user
        _assert_acl_contents(url, id_, token, {
            'owner': USER1,
            'admin': [],
            'write': [],
            'read': [],
            'public_read': 1
        })


def test_get_acls_as_admin(sample_port):
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      }
                                     ]
                       }
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id_ = ret.json()['result'][0]['id']

    # user 3 has admin read rights only
    _assert_acl_contents(url, id_, TOKEN3, {
        'owner': USER1,
        'admin': [],
        'write': [],
        'read': [],
        'public_read': 0
        },
        as_admin=1)


def test_replace_acls_as_admin(sample_port):
    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _assert_acl_contents(url, id_, TOKEN1, {
        'owner': USER1,
        'admin': [],
        'write': [],
        'read': [],
        'public_read': 0
    })

    _replace_acls(url, id_, TOKEN2, {
        'admin': [USER2],
        'write': [USER_NO_TOKEN1, USER_NO_TOKEN2, USER3],
        'read': [USER_NO_TOKEN3, USER4],
        'public_read': 1
        },
        as_admin=1)

    _assert_acl_contents(url, id_, TOKEN1, {
        'owner': USER1,
        'admin': [USER2],
        'write': [USER3, USER_NO_TOKEN1, USER_NO_TOKEN2],
        'read': [USER4, USER_NO_TOKEN3],
        'public_read': 1
    })


def _replace_acls(url, id_, token, acls, as_admin=0, print_resp=False):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.replace_sample_acls',
        'version': '1.1',
        'id': '67',
        'params': [{'id': id_, 'acls': acls, 'as_admin': as_admin}]
    })
    if print_resp:
        print(ret.text)
    assert ret.ok is True
    assert ret.json() == {'version': '1.1', 'id': '67', 'result': None}


def _assert_acl_contents(url, id_, token, expected, as_admin=0, print_resp=False):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_sample_acls',
        'version': '1.1',
        'id': '47',
        'params': [{'id': id_, 'as_admin': as_admin}]
    })
    if print_resp:
        print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0] == expected


def test_get_acls_fail_no_id(sample_port):

    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      }
                                     ]
                       }
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id_ = ret.json()['result'][0]['id']

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.get_sample_acls',
        'version': '1.1',
        'id': '42',
        'params': [{'ids': id_}]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        'Sample service error code 30000 Missing input parameter: id')


def test_get_acls_fail_permissions(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _get_acls_fail_permissions(
        url, TOKEN2, {'id': id_},
        f'Sample service error code 20000 Unauthorized: User user2 cannot read sample {id_}')

    _get_acls_fail_permissions(
        url, None, {'id': id_},
        f'Sample service error code 20000 Unauthorized: Anonymous users cannot read sample {id_}')

    _get_acls_fail_permissions(
        url, None, {'id': id_, 'as_admin': 1},
        'Sample service error code 20000 Unauthorized: Anonymous users ' +
        'may not act as service administrators.')


def _get_acls_fail_permissions(url, token, params, expected):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_sample_acls',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


def test_get_acls_fail_admin_permissions(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    # user 4 has no admin perms
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.get_sample_acls',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_, 'as_admin': 1}]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        'Sample service error code 20000 Unauthorized: User user4 does not have the ' +
        'necessary administration privileges to run method get_sample_acls')


def _create_generic_sample(url, token):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      },
                                     {'id': 'foo',
                                      'parent': 'root',
                                      'type': 'TechReplicate',
                                      }
                                     ]
                       }
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    return ret.json()['result'][0]['id']


def test_replace_acls_fail_no_id(sample_port):
    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.replace_sample_acls',
        'version': '1.1',
        'id': '42',
        'params': [{'ids': id_}]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        'Sample service error code 30000 Missing input parameter: id')


def test_replace_acls_fail_bad_acls(sample_port):
    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.replace_sample_acls',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_, 'acls': ['foo']}]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        'Sample service error code 30001 Illegal input parameter: ' +
        'ACLs must be supplied in the acls key and must be a mapping')


def test_replace_acls_fail_permissions(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _replace_acls(url, id_, TOKEN1, {
        'admin': [USER2],
        'write': [USER3],
        'read': [USER4]
    })

    for user, token in ((USER3, TOKEN3), (USER4, TOKEN4)):
        ret = requests.post(url, headers=get_authorized_headers(token), json={
            'method': 'SampleService.replace_sample_acls',
            'version': '1.1',
            'id': '42',
            'params': [{'id': id_, 'acls': {}}]
        })
        assert ret.status_code == 500
        assert ret.json()['error']['message'] == (
            f'Sample service error code 20000 Unauthorized: User {user} cannot ' +
            f'administrate sample {id_}')


def test_replace_acls_fail_admin_permissions(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    for user, token in ((USER1, TOKEN1), (USER3, TOKEN3), (USER4, TOKEN4)):
        ret = requests.post(url, headers=get_authorized_headers(token), json={
            'method': 'SampleService.replace_sample_acls',
            'version': '1.1',
            'id': '42',
            'params': [{'id': id_, 'acls': {}, 'as_admin': 1}]
        })
        assert ret.status_code == 500
        assert ret.json()['error']['message'] == (
            f'Sample service error code 20000 Unauthorized: User {user} does not have the ' +
            'necessary administration privileges to run method replace_sample_acls')


def test_replace_acls_fail_bad_user(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.replace_sample_acls',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_,
                    'acls': {
                        'admin': [USER2, 'a'],
                        'write': [USER3],
                        'read': [USER4, 'philbin_j_montgomery_iii']
                        }
                    }]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        'Sample service error code 50000 No such user: a, philbin_j_montgomery_iii')


def test_replace_acls_fail_user_in_2_acls(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.replace_sample_acls',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_, 'acls': {'write': [USER2, USER3], 'read': [USER2]}}]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        'Sample service error code 30001 Illegal input parameter: ' +
        f'User {USER2} appears in two ACLs')


def test_replace_acls_fail_owner_in_another_acl(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.replace_sample_acls',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_, 'acls': {'write': [USER1]}}]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        'Sample service error code 30001 Illegal input parameter: ' +
        'The owner cannot be in any other ACL')


def test_update_acls(sample_port, kafka):
    _update_acls_tst(sample_port, kafka, TOKEN1, False)  # owner
    _update_acls_tst(sample_port, kafka, TOKEN2, False)  # admin
    _update_acls_tst(sample_port, kafka, TOKEN5, True)  # as_admin = True

def _update_acls_tst(sample_port, kafka, token, as_admin):
    _clear_kafka_messages(kafka)
    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _replace_acls(url, id_, TOKEN1, {
        'admin': [USER2],
        'write': [USER_NO_TOKEN1, USER_NO_TOKEN2, USER3],
        'read': [USER_NO_TOKEN3, USER4],
        'public_read': 0
        })

    _update_acls(url, token, {
        'id': str(id_),
        'admin': [USER4],
        'write': [USER2],
        'read': [USER_NO_TOKEN2],
        'remove': [USER3],
        'public_read': 390,
        'as_admin': 1 if as_admin else 0,
    })

    _assert_acl_contents(url, id_, TOKEN1, {
        'owner': USER1,
        'admin': [USER4],
        'write': [USER2, USER_NO_TOKEN1],
        'read': [USER_NO_TOKEN2, USER_NO_TOKEN3],
        'public_read': 1
    })

    _check_kafka_messages(
        kafka,
        [
            {'event_type': 'NEW_SAMPLE', 'sample_id': id_, 'sample_ver': 1},
            {'event_type': 'ACL_CHANGE', 'sample_id': id_},
            {'event_type': 'ACL_CHANGE', 'sample_id': id_},
        ])


def test_update_acls_with_at_least(sample_port, kafka):
    _update_acls_tst_with_at_least(sample_port, kafka, TOKEN1, False)  # owner
    _update_acls_tst_with_at_least(sample_port, kafka, TOKEN2, False)  # admin
    _update_acls_tst_with_at_least(sample_port, kafka, TOKEN5, True)  # as_admin = True


def _update_acls_tst_with_at_least(sample_port, kafka, token, as_admin):
    _clear_kafka_messages(kafka)
    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _replace_acls(url, id_, TOKEN1, {
        'admin': [USER2],
        'write': [USER_NO_TOKEN1, USER_NO_TOKEN2, USER3],
        'read': [USER_NO_TOKEN3, USER4],
        'public_read': 0
        })

    _update_acls(url, token, {
        'id': str(id_),
        'admin': [USER4],
        'write': [USER2, USER_NO_TOKEN3],
        'read': [USER_NO_TOKEN2, USER5],
        'remove': [USER3],
        'public_read': 390,
        'as_admin': 1 if as_admin else 0,
        'at_least': 1,
    })

    _assert_acl_contents(url, id_, TOKEN1, {
        'owner': USER1,
        'admin': [USER2, USER4],
        'write': [USER_NO_TOKEN1, USER_NO_TOKEN2, USER_NO_TOKEN3],
        'read': [USER5],
        'public_read': 1
    }, print_resp=True)

    _check_kafka_messages(
        kafka,
        [
            {'event_type': 'NEW_SAMPLE', 'sample_id': id_, 'sample_ver': 1},
            {'event_type': 'ACL_CHANGE', 'sample_id': id_},
            {'event_type': 'ACL_CHANGE', 'sample_id': id_},
        ])


def test_update_acls_fail_no_id(sample_port):
    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _update_acls_fail(
        url, TOKEN1, {'ids': id_},
        'Sample service error code 30000 Missing input parameter: id')


def test_update_acls_fail_bad_pub(sample_port):
    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _update_acls_fail(
        url, TOKEN1, {'id': id_, 'public_read': 'thingy'},
        'Sample service error code 30001 Illegal input parameter: ' +
        'public_read must be an integer if present')


def test_update_acls_fail_permissions(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _replace_acls(url, id_, TOKEN1, {
        'admin': [USER2],
        'write': [USER3],
        'read': [USER4]
    })

    for user, token in ((USER3, TOKEN3), (USER4, TOKEN4)):
        _update_acls_fail(url, token, {'id': id_}, 'Sample service error code 20000 ' +
                          f'Unauthorized: User {user} cannot administrate sample {id_}')


def test_update_acls_fail_admin_permissions(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    for user, token in ((USER1, TOKEN1), (USER3, TOKEN3), (USER4, TOKEN4)):
        _update_acls_fail(
            url, token, {'id': id_, 'as_admin': 1},
            f'Sample service error code 20000 Unauthorized: User {user} does not have the ' +
            'necessary administration privileges to run method update_sample_acls')


def test_update_acls_fail_bad_user(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _update_acls_fail(
        url,
        TOKEN1,
        {'id': id_,
         'admin': [USER2, 'a'],
         'write': [USER3],
         'read': [USER4, 'philbin_j_montgomery_iii'],
         'remove': ['someguy']
         },
        'Sample service error code 50000 No such user: a, philbin_j_montgomery_iii, someguy')


def test_update_acls_fail_user_2_acls(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _update_acls_fail(
        url,
        TOKEN1,
        {'id': id_,
         'admin': [USER2],
         'write': [USER3],
         'read': [USER4, USER2],
         },
        'Sample service error code 30001 Illegal input parameter: User user2 appears in two ACLs')


def test_update_acls_fail_user_in_acl_and_remove(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _update_acls_fail(
        url,
        TOKEN1,
        {'id': id_,
         'admin': [USER2],
         'write': [USER3],
         'read': [USER4],
         'remove': [USER2]
         },
        'Sample service error code 30001 Illegal input parameter: Users in the remove list ' +
        'cannot be in any other ACL')


def test_update_acls_fail_owner_in_another_acl(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _update_acls_fail(
        url, TOKEN1, {'id': id_, 'write': [USER1]},
        'Sample service error code 20000 Unauthorized: ' +
        'ACLs for the sample owner user1 may not be modified by a delta update.')


def test_update_acls_fail_owner_in_remove_acl(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _update_acls_fail(
        url, TOKEN1, {'id': id_, 'remove': [USER1]},
        'Sample service error code 20000 Unauthorized: ' +
        'ACLs for the sample owner user1 may not be modified by a delta update.')


def _update_acls_fail(url, token, params, expected):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.update_sample_acls',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


def _update_acls(url, token, params, print_resp=False):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.update_sample_acls',
        'version': '1.1',
        'id': '67',
        'params': [params]
    })
    if print_resp:
        print(ret.text)
    assert ret.ok is True
    assert ret.json() == {'version': '1.1', 'id': '67', 'result': None}


def _update_samples_acls(url, token, params, print_resp=False):
    resp = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.update_samples_acls',
        'version': '1.1',
        'id': '1729',
        'params': [params]
    })
    if print_resp:
        print(resp.text)
    return resp


def test_update_acls_many(sample_port):
    url = f'http://localhost:{sample_port}'
    # create samples
    n_samples = 2 # 1000
    ids = _create_samples(url, TOKEN1, n_samples, 1)
    for id_ in ids:
        _update_acls(
            url,
            TOKEN1,
            {
                'id': str(id_),
                'admin': [],
                'write': [],
                'read': [USER2],
                'remove': [],
                'public_read': 1,
                'as_admin': 0,
            },
            print_resp=True,
        )


def test_update_acls_many_bulk(sample_port):
    url = f'http://localhost:{sample_port}'
    # create samples
    n_samples = 2 # 1000
    ids = _create_samples(url, TOKEN1, n_samples, 1)
    resp = _update_samples_acls(
        url,
        TOKEN1,
        {
            'ids': ids,
            'admin': [],
            'write': [],
            'read': [USER2],
            'remove': [],
            'public_read': 1,
            'as_admin': 0,
        },
        print_resp=True,
    )
    assert resp.ok
    assert resp.json()['result'] is None

def test_update_acls_many_bulk_fail(sample_port):
    url = f'http://localhost:{sample_port}'
    sample_bad_id = str(uuid.UUID('0'*32))
    resp = _update_samples_acls(
        url,
        TOKEN1,
        {
            'ids': [sample_bad_id],
            'admin': [],
            'write': [],
            'read': [USER2],
            'remove': [],
            'public_read': 1,
            'as_admin': 0,
        },
        print_resp=True,
    )
    assert resp.status_code == 500
    msg = f"Sample service error code 50010 No such sample: {sample_bad_id}"
    assert resp.json()['error']['message'] == msg

def test_get_metadata_key_static_metadata(sample_port):
    _get_metadata_key_static_metadata(
        sample_port, {'keys': ['foo']}, {'foo': {'a': 'b', 'c': 'd'}})
    _get_metadata_key_static_metadata(
        sample_port,
        {'keys': ['foo', 'stringlentest'], 'prefix': 0},
        {'foo': {'a': 'b', 'c': 'd'}, 'stringlentest': {'h': 'i', 'j': 'k'}})
    _get_metadata_key_static_metadata(
        sample_port, {'keys': ['pre'], 'prefix': 1}, {'pre': {'1': '2'}})
    _get_metadata_key_static_metadata(
        sample_port, {'keys': ['premature'], 'prefix': 2}, {'pre': {'1': '2'}})


def _get_metadata_key_static_metadata(sample_port, params, expected):
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, json={
        'method': 'SampleService.get_metadata_key_static_metadata',
        'version': '1.1',
        'id': '67',
        'params': [params]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0] == {'static_metadata': expected}


def test_get_metadata_key_static_metadata_fail_bad_args(sample_port):
    _get_metadata_key_static_metadata_fail(
        sample_port,
        {},
        'Sample service error code 30001 Illegal input parameter: keys must be a list')
    _get_metadata_key_static_metadata_fail(
        sample_port,
        {'keys': ['foo', 'stringlentestage'], 'prefix': 0},
        'Sample service error code 30001 Illegal input parameter: No such metadata key: ' +
        'stringlentestage')
    _get_metadata_key_static_metadata_fail(
        sample_port,
        {'keys': ['premature'], 'prefix': 1},
        'Sample service error code 30001 Illegal input parameter: No such prefix metadata key: ' +
        'premature')
    _get_metadata_key_static_metadata_fail(
        sample_port,
        {'keys': ['somekey'], 'prefix': 2},
        'Sample service error code 30001 Illegal input parameter: No prefix metadata keys ' +
        'matching key somekey')


def _get_metadata_key_static_metadata_fail(sample_port, params, error):
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, json={
        'method': 'SampleService.get_metadata_key_static_metadata',
        'version': '1.1',
        'id': '67',
        'params': [params]
    })
    # print(ret.text)
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == error


def _create_sample(url, token, sample, expected_version):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{'sample': sample}]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == expected_version
    return ret.json()['result'][0]['id']

def _sample_factory(name):
    return {
        "sample": {
            "name": name,
            "node_tree": [{
                    "id": "root",
                    "type": "BioReplicate",
                },
                {
                    "id": "foo",
                    "parent": "root",
                    "type": "TechReplicate",
                }
            ]
        }
    }


def _create_samples(url, token, n, expected_version, sample_factory=None):
    if sample_factory is None:
        sample_factory = _sample_factory

    ids = []
    for i in range(n):
        sample = sample_factory(f"sample-{i}")
        resp = requests.post(url, headers=get_authorized_headers(token), json={
            'method': 'SampleService.create_sample',
            'version': '1.1',
            'id': '67',
            'params': [sample]
        })
        assert resp.ok
        data = resp.json()["result"][0]
        assert data["version"] == expected_version
        ids.append(data["id"])
    return ids


def _create_link(url, token, expected_user, params, print_resp=False):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.create_data_link',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    if print_resp:
        print(ret.text)
    assert ret.ok is True
    link = ret.json()['result'][0]['new_link']
    id_ = link['linkid']
    uuid.UUID(id_)  # check the ID is a valid UUID
    del link['linkid']
    created = link['created']
    assert_ms_epoch_close_to_now(created)
    del link['created']
    assert link == {
        'id': params['id'],
        'version': params['version'],
        'node': params['node'],
        'upa': params['upa'],
        'dataid': params.get('dataid'),
        'createdby': expected_user,
        'expiredby': None,
        'expired': None
    }
    return id_


def _create_sample_and_links_for_propagate_links(url, token, user):
    # create samples
    sid = _create_sample(
        url,
        token,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )
    # ver 2
    _create_sample(
        url,
        token,
        {'id': sid,
         'name': 'mysample2',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        2
        )

    # create links
    lid1 = _create_link(
        url, token, user,
        {'id': sid, 'version': 1, 'node': 'root', 'upa': '1/1/1', 'dataid': 'column1'})
    lid2 = _create_link(
        url, token, user,
        {'id': sid, 'version': 1, 'node': 'root', 'upa': '1/2/1', 'dataid': 'column2'})

    return sid, lid1, lid2


def _check_data_links(links, expected_links):

    assert len(links) == len(expected_links)
    for link in links:
        assert_ms_epoch_close_to_now(link['created'])
        del link['created']

    for link in expected_links:
        assert link in links


def _check_sample_data_links(url, sample_id, version, expected_links, token):

    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': sample_id, 'version': version}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 2
    assert_ms_epoch_close_to_now(ret.json()['result'][0]['effective_time'])
    links = ret.json()['result'][0]['links']

    _check_data_links(links, expected_links)


def test_create_and_propagate_data_links(sample_port, workspace, kafka):

    _clear_kafka_messages(kafka)

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        {'name': 'baz', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    sid, lid1, lid2 = _create_sample_and_links_for_propagate_links(url, TOKEN3, USER3)

    # check initial links for both version
    expected_links = [
        {
            'linkid': lid1,
            'id': sid,
            'version': 1,
            'node': 'root',
            'upa': '1/1/1',
            'dataid': 'column1',
            'createdby': USER3,
            'expiredby': None,
            'expired': None
        },
        {
            'linkid': lid2,
            'id': sid,
            'version': 1,
            'node': 'root',
            'upa': '1/2/1',
            'dataid': 'column2',
            'createdby': USER3,
            'expiredby': None,
            'expired': None
        }
    ]
    _check_sample_data_links(url, sid, 1, expected_links, TOKEN3)
    _check_sample_data_links(url, sid, 2, [], TOKEN3)

    # propagate data links from sample version 1 to version 2
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.propagate_data_links',
        'version': '1.1',
        'id': '38',
        'params': [{'id': sid, 'version': 2, 'previous_version': 1}]
    })

    # print(ret.text)
    assert ret.ok is True
    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 1
    links = ret.json()['result'][0]['links']

    new_link_ids = [i['linkid'] for i in links]
    expected_new_links = copy.deepcopy(expected_links)

    # propagated links should have new link id, dataid and version
    for idx, expected_link in enumerate(expected_new_links):
        expected_link['linkid'] = new_link_ids[idx]
        expected_link['dataid'] = expected_link['dataid'] + '_2'
        expected_link['version'] = 2

    _check_data_links(links, expected_new_links)

    # check links again for sample version 1 and 2
    _check_sample_data_links(url, sid, 1, expected_links, TOKEN3)
    _check_sample_data_links(url, sid, 2, expected_new_links, TOKEN3)


def test_create_and_propagate_data_links_type_specific(sample_port, workspace, kafka):

    _clear_kafka_messages(kafka)

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        {'name': 'baz', 'data': {}, 'type': 'Trivial.Object2-1.0'},
        ]})

    sid, lid1, lid2 = _create_sample_and_links_for_propagate_links(url, TOKEN3, USER3)

    # check initial links for both version
    expected_links = [
        {
            'linkid': lid1,
            'id': sid,
            'version': 1,
            'node': 'root',
            'upa': '1/1/1',
            'dataid': 'column1',
            'createdby': USER3,
            'expiredby': None,
            'expired': None
        },
        {
            'linkid': lid2,
            'id': sid,
            'version': 1,
            'node': 'root',
            'upa': '1/2/1',
            'dataid': 'column2',
            'createdby': USER3,
            'expiredby': None,
            'expired': None
        }
    ]
    _check_sample_data_links(url, sid, 1, expected_links, TOKEN3)
    _check_sample_data_links(url, sid, 2, [], TOKEN3)

    # propagate data links from sample version 1 to version 2
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.propagate_data_links',
        'version': '1.1',
        'id': '38',
        'params': [{'id': sid, 'version': 2, 'previous_version': 1,
                    'ignore_types': ['Trivial.Object2']}]
    })

    # print(ret.text)
    assert ret.ok is True
    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 1
    links = ret.json()['result'][0]['links']

    new_link_ids = [i['linkid'] for i in links]
    expected_new_links = copy.deepcopy(expected_links)
    expected_new_links.pop()
    assert len(expected_new_links) == 1

    # propagated links should have new link id, dataid and version
    for idx, expected_link in enumerate(expected_new_links):
        expected_link['linkid'] = new_link_ids[idx]
        expected_link['dataid'] = expected_link['dataid'] + '_2'
        expected_link['version'] = 2

    _check_data_links(links, expected_new_links)

    # check links again for sample version 1 and 2
    _check_sample_data_links(url, sid, 1, expected_links, TOKEN3)
    _check_sample_data_links(url, sid, 2, expected_new_links, TOKEN3)


def test_create_links_and_get_links_from_sample_basic(sample_port, workspace, kafka):
    '''
    Also tests that the 'as_user' key is ignored if 'as_admin' is falsy.
    '''
    _clear_kafka_messages(kafka)

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        {'name': 'baz', 'data': {}, 'type': 'Trivial.Object-1.0'},
        {'name': 'baz', 'data': {}, 'type': 'Trivial.Object-1.0'}
        ]})
    wscli.set_permissions({'id': 1, 'new_permission': 'w', 'users': [USER4]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN3,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )

    id2 = _create_sample(
        url,
        TOKEN4,
        {'name': 'myothersample',
         'node_tree': [{'id': 'root2', 'type': 'BioReplicate'},
                       {'id': 'foo2', 'type': 'TechReplicate', 'parent': 'root2'}
                       ]
         },
        1
        )
    # ver 2
    _create_sample(
        url,
        TOKEN4,
        {'id': id2,
         'name': 'myothersample3',
         'node_tree': [{'id': 'root3', 'type': 'BioReplicate'},
                       {'id': 'foo3', 'type': 'TechReplicate', 'parent': 'root3'}
                       ]
         },
        2
        )

    # create links
    # as_user should be ignored unless as_admin is true
    lid1 = _create_link(url, TOKEN3, USER3,
                        {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/2/2', 'as_user': USER1})
    lid2 = _create_link(
        url, TOKEN3, USER3,
        {'id': id1, 'version': 1, 'node': 'root', 'upa': '1/1/1', 'dataid': 'column1'})
    lid3 = _create_link(
        url, TOKEN4, USER4,
        {'id': id2, 'version': 1, 'node': 'foo2', 'upa': '1/2/1', 'dataid': 'column2'})

    # get links from sample 1
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id1, 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 2
    assert_ms_epoch_close_to_now(ret.json()['result'][0]['effective_time'])
    res = ret.json()['result'][0]['links']
    expected_links = [
        {
            'linkid': lid1,
            'id': id1,
            'version': 1,
            'node': 'foo',
            'upa': '1/2/2',
            'dataid': None,
            'createdby': USER3,
            'expiredby': None,
            'expired': None
         },
        {
            'linkid': lid2,
            'id': id1,
            'version': 1,
            'node': 'root',
            'upa': '1/1/1',
            'dataid': 'column1',
            'createdby': USER3,
            'expiredby': None,
            'expired': None
        }
    ]

    assert len(res) == len(expected_links)
    for link in res:
        assert_ms_epoch_close_to_now(link['created'])
        del link['created']

    for link in expected_links:
        assert link in res

    # get links from sample 2
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id2, 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 2
    assert_ms_epoch_close_to_now(ret.json()['result'][0]['effective_time'])
    res = ret.json()['result'][0]['links']
    assert_ms_epoch_close_to_now(res[0]['created'])
    del res[0]['created']
    assert res == [
        {
            'linkid': lid3,
            'id': id2,
            'version': 1,
            'node': 'foo2',
            'upa': '1/2/1',
            'dataid': 'column2',
            'createdby': USER4,
            'expiredby': None,
            'expired': None
         }
    ]

    # get links from ver 2 of sample 2
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id2, 'version': 2}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 2
    assert_ms_epoch_close_to_now(ret.json()['result'][0]['effective_time'])
    assert ret.json()['result'][0]['links'] == []

    _check_kafka_messages(
        kafka,
        [
            {'event_type': 'NEW_SAMPLE', 'sample_id': id1, 'sample_ver': 1},
            {'event_type': 'NEW_SAMPLE', 'sample_id': id2, 'sample_ver': 1},
            {'event_type': 'NEW_SAMPLE', 'sample_id': id2, 'sample_ver': 2},
            {'event_type': 'NEW_LINK', 'link_id': lid1},
            {'event_type': 'NEW_LINK', 'link_id': lid2},
            {'event_type': 'NEW_LINK', 'link_id': lid3},
        ])


def test_update_and_get_links_from_sample(sample_port, workspace, kafka):
    '''
    Also tests getting links from a sample using an effective time
    '''
    _clear_kafka_messages(kafka)

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli.set_permissions({'id': 1, 'new_permission': 'w', 'users': [USER4]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN3,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )
    _replace_acls(url, id1, TOKEN3, {'admin': [USER4]})

    # create links
    lid1 = _create_link(url, TOKEN3, USER3,
                        {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

    oldlinkactive = datetime.datetime.now()
    time.sleep(1)

    # update link node
    lid2 = _create_link(
        url,
        TOKEN4,
        USER4,
        {'id': id1,
         'version': 1,
         'node': 'root',
         'upa': '1/1/1',
         'dataid': 'yay',
         'update': 1})

    # get current link
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id1, 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    res = ret.json()['result'][0]
    assert len(res) == 2
    assert_ms_epoch_close_to_now(res['effective_time'])
    del res['effective_time']
    created = res['links'][0]['created']
    assert_ms_epoch_close_to_now(created)
    del res['links'][0]['created']
    assert res == {'links': [
        {
            'linkid': lid2,
            'id': id1,
            'version': 1,
            'node': 'root',
            'upa': '1/1/1',
            'dataid': 'yay',
            'createdby': USER4,
            'expiredby': None,
            'expired': None
         }
    ]}

    # get expired link
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [{
            'id': id1,
            'version': 1,
            'effective_time': round(oldlinkactive.timestamp() * 1000)}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    res = ret.json()['result'][0]
    assert res['links'][0]['expired'] == created - 1
    assert_ms_epoch_close_to_now(res['links'][0]['created'] + 1000)
    del res['links'][0]['created']
    del res['links'][0]['expired']
    assert res == {
        'effective_time': round(oldlinkactive.timestamp() * 1000),
        'links': [
            {
                'linkid': lid1,
                'id': id1,
                'version': 1,
                'node': 'foo',
                'upa': '1/1/1',
                'dataid': 'yay',
                'createdby': USER3,
                'expiredby': USER4,
            }
        ]}

    _check_kafka_messages(
        kafka,
        [
            {'event_type': 'NEW_SAMPLE', 'sample_id': id1, 'sample_ver': 1},
            {'event_type': 'ACL_CHANGE', 'sample_id': id1},
            {'event_type': 'NEW_LINK', 'link_id': lid1},
            {'event_type': 'NEW_LINK', 'link_id': lid2},
            {'event_type': 'EXPIRED_LINK', 'link_id': lid1},
        ])


def test_create_data_link_as_admin(sample_port, workspace):

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN3,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )

    # create links
    lid1 = _create_link(
        url,
        TOKEN2,
        USER2,
        {'id': id1,
         'version': 1,
         'node': 'root',
         'upa': '1/1/1',
         'dataid': 'yeet',
         'as_admin': 1})
    lid2 = _create_link(
        url,
        TOKEN2,
        USER4,
        {'id': id1,
         'version': 1,
         'node': 'foo',
         'upa': '1/1/1',
         'as_admin': 1,
         'as_user': f'     {USER4}     '})

    # get link
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id1, 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 2
    assert_ms_epoch_close_to_now(ret.json()['result'][0]['effective_time'])
    res = ret.json()['result'][0]['links']
    expected_links = [
        {
            'linkid': lid1,
            'id': id1,
            'version': 1,
            'node': 'root',
            'upa': '1/1/1',
            'dataid': 'yeet',
            'createdby': USER2,
            'expiredby': None,
            'expired': None
         },
        {
            'linkid': lid2,
            'id': id1,
            'version': 1,
            'node': 'foo',
            'upa': '1/1/1',
            'dataid': None,
            'createdby': USER4,
            'expiredby': None,
            'expired': None
        }
    ]

    assert len(res) == len(expected_links)
    for link in res:
        assert_ms_epoch_close_to_now(link['created'])
        del link['created']

    for link in expected_links:
        assert link in res


def test_get_links_from_sample_exclude_workspaces(sample_port, workspace):
    '''
    Tests that unreadable workspaces are excluded from link results
    '''
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli3 = Workspace(wsurl, token=TOKEN3)
    wscli4 = Workspace(wsurl, token=TOKEN4)

    # create workspace & objects
    wscli3.create_workspace({'workspace': 'foo'})
    wscli3.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    wscli4.create_workspace({'workspace': 'bar'})
    wscli4.save_objects({'id': 2, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli4.set_permissions({'id': 2, 'new_permission': 'r', 'users': [USER3]})

    wscli4.create_workspace({'workspace': 'baz'})
    wscli4.save_objects({'id': 3, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli4.set_global_permission({'id': 3, 'new_permission': 'r'})

    wscli4.create_workspace({'workspace': 'bat'})  # unreadable
    wscli4.save_objects({'id': 4, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    # create sample
    id_ = _create_generic_sample(url, TOKEN3)
    _replace_acls(url, id_, TOKEN3, {'admin': [USER4]})

    # create links
    lid1 = _create_link(
        url, TOKEN3, USER3, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1'})
    lid2 = _create_link(
        url, TOKEN4, USER4, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '2/1/1'})
    lid3 = _create_link(url, TOKEN4, USER4,
                        {'id': id_, 'version': 1, 'node': 'foo', 'upa': '3/1/1', 'dataid': 'whee'})
    _create_link(
        url, TOKEN4, USER4,  {'id': id_, 'version': 1, 'node': 'foo', 'upa': '4/1/1'})

    # check correct links are returned
    ret = _get_links_from_sample(url, TOKEN3, {'id': id_, 'version': 1})

    assert_ms_epoch_close_to_now(ret['effective_time'])
    res = ret['links']
    expected_links = [
        {
            'linkid': lid1,
            'id': id_,
            'version': 1,
            'node': 'foo',
            'upa': '1/1/1',
            'dataid': None,
            'createdby': USER3,
            'expiredby': None,
            'expired': None
         },
        {
            'linkid': lid2,
            'id': id_,
            'version': 1,
            'node': 'foo',
            'upa': '2/1/1',
            'dataid': None,
            'createdby': USER4,
            'expiredby': None,
            'expired': None
         },
        {
            'linkid': lid3,
            'id': id_,
            'version': 1,
            'node': 'foo',
            'upa': '3/1/1',
            'dataid': 'whee',
            'createdby': USER4,
            'expiredby': None,
            'expired': None
         }
    ]

    assert len(res) == len(expected_links)
    for link in res:
        assert_ms_epoch_close_to_now(link['created'])
        del link['created']

    for link in expected_links:
        assert link in res

    # test with anon user
    _replace_acls(url, id_, TOKEN3, {'public_read': 1})
    ret = _get_links_from_sample(url, None, {'id': id_, 'version': 1})

    assert_ms_epoch_close_to_now(ret['effective_time'])
    res = ret['links']
    expected_links = [
        {
            'linkid': lid3,
            'id': id_,
            'version': 1,
            'node': 'foo',
            'upa': '3/1/1',
            'dataid': 'whee',
            'createdby': USER4,
            'expiredby': None,
            'expired': None
         }
    ]

    assert len(res) == len(expected_links)
    for link in res:
        assert_ms_epoch_close_to_now(link['created'])
        del link['created']

    for link in expected_links:
        assert link in res


def _get_links_from_sample(url, token, params, print_resp=False):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    if print_resp:
        print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 2
    return ret.json()['result'][0]


def test_get_links_from_sample_as_admin(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN4)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    # create sample
    id_ = _create_generic_sample(url, TOKEN4)

    # create links
    lid = _create_link(url, TOKEN4, USER4, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1'})

    # check correct links are returned, user 3 has read admin perms, but not full
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_, 'version': 1, 'as_admin': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 2
    assert_ms_epoch_close_to_now(ret.json()['result'][0]['effective_time'])
    assert len(ret.json()['result'][0]['links']) == 1
    link = ret.json()['result'][0]['links'][0]
    assert_ms_epoch_close_to_now(link['created'])
    del link['created']

    assert link == {
            'linkid': lid,
            'id': id_,
            'version': 1,
            'node': 'foo',
            'upa': '1/1/1',
            'dataid': None,
            'createdby': USER4,
            'expiredby': None,
            'expired': None
         }


def test_get_links_from_sample_public_read(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN1)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli.set_global_permission({'id': 1, 'new_permission': 'r'})

    # create sample
    id_ = _create_generic_sample(url, TOKEN1)

    # create links
    lid = _create_link(url, TOKEN1, USER1, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1'})

    _replace_acls(url, id_, TOKEN1, {'public_read': 1})

    for token in [None, TOKEN4]:  # anon user & user without explicit permission
        # check correct links are returned
        ret = requests.post(url, headers=get_authorized_headers(token), json={
            'method': 'SampleService.get_data_links_from_sample',
            'version': '1.1',
            'id': '42',
            'params': [{'id': id_, 'version': 1}]
        })
        # print(ret.text)
        assert ret.ok is True

        assert len(ret.json()['result']) == 1
        assert len(ret.json()['result'][0]) == 2
        assert_ms_epoch_close_to_now(ret.json()['result'][0]['effective_time'])
        assert len(ret.json()['result'][0]['links']) == 1
        link = ret.json()['result'][0]['links'][0]
        assert_ms_epoch_close_to_now(link['created'])
        del link['created']

        assert link == {
                'linkid': lid,
                'id': id_,
                'version': 1,
                'node': 'foo',
                'upa': '1/1/1',
                'dataid': None,
                'createdby': USER1,
                'expiredby': None,
                'expired': None
            }

def test_get_links_from_sample_set(sample_port, workspace):

    """
        test timing for fetching batch of links from list of samples
    """

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN1)

    N_SAMPLES = 100

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'} for _ in range(N_SAMPLES)
    ]})
    wscli.set_global_permission({'id': 1, 'new_permission': 'r'})

    ids_ = [_create_generic_sample(url, TOKEN1) for _ in range(N_SAMPLES)]
    lids = [_create_link(url, TOKEN1, USER1, {
        'id': id_,
        'version': 1,
        'node': 'foo',
        'upa': f'1/1/{i+1}'}) for i, id_ in enumerate(ids_)]
    start = time.time()
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.get_data_links_from_sample_set',
        'version': '1.1',
        'id': '42',
        'params': [{
            'sample_ids': [{'id': id_, 'version': 1} for id_ in ids_],
            'as_admin': False,
            'effective_time': _get_current_epochmillis()
        }]
    })
    end = time.time()
    elapsed = end - start
    # getting 500 sample links should take about 5 seconds (1 second per 100 samples)
    print(f"retrieved data links from {N_SAMPLES} samples in {elapsed} seconds.")
    assert ret.ok
    # assuming twice the amound of expected time elasped should raise concern
    assert elapsed < 10
    assert len(ret.json()['result'][0]['links']) == N_SAMPLES

def test_create_link_fail(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)
    id_ = _create_generic_sample(url, TOKEN3)

    _create_link_fail(
        sample_port, TOKEN3, {'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'},
        'Sample service error code 30000 Missing input parameter: id')
    _create_link_fail(
        sample_port, TOKEN3, {'id': id_, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'},
        'Sample service error code 30000 Missing input parameter: version')
    _create_link_fail(
        sample_port, TOKEN3,
        {'id': id_, 'version': 1, 'node': 'foo', 'upa': 'upalupa', 'dataid': 'yay'},
        'Sample service error code 30001 Illegal input parameter: upalupa is not a valid UPA')
    _create_link_fail(
        sample_port, TOKEN3, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1'},
        'Sample service error code 50040 No such workspace data: No workspace with id 1 exists')

    wscli.create_workspace({'workspace': 'foo'})
    _create_link_fail(
        sample_port, TOKEN3, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1'},
        'Sample service error code 50040 No such workspace data: Object 1/1/1 does not exist')

    _replace_acls(url, id_, TOKEN3, {'write': [USER4]})
    _create_link_fail(  # fails if permission granted is admin
        sample_port, TOKEN4, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1'},
        'Sample service error code 20000 Unauthorized: User user4 cannot ' +
        f'administrate sample {id_}')

    _replace_acls(url, id_, TOKEN3, {'admin': [USER4]})
    wscli.set_permissions({'id': 1, 'new_permission': 'r', 'users': [USER4]})
    _create_link_fail(  # fails if permission granted is write
        sample_port, TOKEN4, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1'},
        'Sample service error code 20000 Unauthorized: User user4 cannot write to upa 1/1/1')

    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    _create_link_fail(
        sample_port, TOKEN3, {'id': id_, 'version': 1, 'node': 'fake', 'upa': '1/1/1'},
        f'Sample service error code 50030 No such sample node: {id_} ver 1 fake')

    # admin tests
    _create_link_fail(
        sample_port, TOKEN2,
        {'id': id_,
         'version': 1,
         'node': 'foo',
         'upa': '1/1/1',
         'as_admin': 1,
         'as_user': 'foo\bbar'},
        'Sample service error code 30001 Illegal input parameter: ' +
        'userid contains control characters')
    _create_link_fail(
        sample_port, TOKEN3,
        {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'as_user': USER4, 'as_admin': 'f'},
        'Sample service error code 20000 Unauthorized: User user3 does not have ' +
        'the necessary administration privileges to run method create_data_link')
    _create_link_fail(
        sample_port,
        TOKEN2,
        {'id': id_,
         'version': 1,
         'node': 'foo',
         'upa': '1/1/1',
         'as_user': 'fake',
         'as_admin': 'f'},
        'Sample service error code 50000 No such user: fake')


def test_create_link_fail_link_exists(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    id_ = _create_generic_sample(url, TOKEN3)

    _create_link(url, TOKEN3, USER3,
                 {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

    _create_link_fail(
        sample_port, TOKEN3,
        {'id': id_, 'version': 1, 'node': 'root', 'upa': '1/1/1', 'dataid': 'yay'},
        'Sample service error code 60000 Data link exists for data ID: 1/1/1:yay')


def _create_link_fail(sample_port, token, params, expected):
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.create_data_link',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })

    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


def test_get_links_from_sample_fail(sample_port):
    url = f'http://localhost:{sample_port}'
    id_ = _create_generic_sample(url, TOKEN3)

    _get_link_from_sample_fail(
        sample_port, TOKEN3, {},
        'Sample service error code 30000 Missing input parameter: id')
    _get_link_from_sample_fail(
        sample_port, TOKEN3, {'id': id_},
        'Sample service error code 30000 Missing input parameter: version')
    _get_link_from_sample_fail(
        sample_port, TOKEN3, {'id': id_, 'version': 1, 'effective_time': 'foo'},
        "Sample service error code 30001 Illegal input parameter: key 'effective_time' " +
        "value of 'foo' is not a valid epoch millisecond timestamp")
    _get_link_from_sample_fail(
        sample_port, TOKEN4, {'id': id_, 'version': 1},
        f'Sample service error code 20000 Unauthorized: User user4 cannot read sample {id_}')
    _get_link_from_sample_fail(
        sample_port, None, {'id': id_, 'version': 1},
        f'Sample service error code 20000 Unauthorized: Anonymous users cannot read sample {id_}')
    badid = uuid.uuid4()
    _get_link_from_sample_fail(
        sample_port, TOKEN3, {'id': str(badid), 'version': 1},
        f'Sample service error code 50010 No such sample: {badid}')

    # admin tests
    _get_link_from_sample_fail(
        sample_port, TOKEN4, {'id': id_, 'version': 1, 'as_admin': 1},
        'Sample service error code 20000 Unauthorized: User user4 does not have the ' +
        'necessary administration privileges to run method get_data_links_from_sample')
    _get_link_from_sample_fail(
        sample_port, None, {'id': id_, 'version': 1, 'as_admin': 1},
        'Sample service error code 20000 Unauthorized: Anonymous users ' +
        'may not act as service administrators.')


def _get_link_from_sample_fail(sample_port, token, params, expected):
    url = f'http://localhost:{sample_port}'
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


def test_get_links_from_sample_set_fail(sample_port):
    url = f'http://localhost:{sample_port}'
    id_ = _create_generic_sample(url, TOKEN3)

    _get_links_from_sample_set_fail(
        sample_port, TOKEN3, {},
        'Missing "sample_ids" field - Must provide a list of valid sample ids.')
    _get_links_from_sample_set_fail(
        sample_port, TOKEN3, {
            'sample_ids': [{'id': id_}]
        },
        "Malformed sample accessor - each sample must provide both an id and a version.")
    _get_links_from_sample_set_fail(
        sample_port, TOKEN3, {
            'sample_ids': [{'id': id_, 'version': 1}]
        },
        'Missing "effective_time" parameter.')
    _get_links_from_sample_set_fail(
        sample_port, TOKEN3, {
            'sample_ids': [{'id': id_, 'version': 1}],
            'effective_time': 'foo'
        },
        "Sample service error code 30001 Illegal input parameter: key 'effective_time' " +
        "value of 'foo' is not a valid epoch millisecond timestamp")
    _get_links_from_sample_set_fail(
        sample_port, TOKEN4, {
            'sample_ids': [{'id': id_, 'version': 1}],
            'effective_time': _get_current_epochmillis() - 500
        },
        f'Sample service error code 20000 Unauthorized: User user4 cannot read sample {id_}')
    _get_links_from_sample_set_fail(
        sample_port, None, {
            'sample_ids': [{'id': id_, 'version': 1}],
            'effective_time': _get_current_epochmillis() - 500
        },
        f'Sample service error code 20000 Unauthorized: Anonymous users cannot read sample {id_}')
    badid = uuid.uuid4()
    _get_links_from_sample_set_fail(
        sample_port, TOKEN3, {
            'sample_ids': [{'id': str(badid), 'version': 1}],
            'effective_time': _get_current_epochmillis() - 500
        },
        'Sample service error code 50010 No such sample:'
        f" Could not complete search for samples: ['{badid}']")

    # admin tests
    _get_links_from_sample_set_fail(
        sample_port, TOKEN4, {
            'sample_ids': [{'id': id_, 'version': 1}],
            'effective_time': _get_current_epochmillis() - 500,
            'as_admin': 1,
        },
        'Sample service error code 20000 Unauthorized: User user4 does not have the ' +
        'necessary administration privileges to run method get_data_links_from_sample')
    _get_links_from_sample_set_fail(
        sample_port, None, {
            'sample_ids': [{'id': id_, 'version': 1}],
            'effective_time': _get_current_epochmillis() - 500,
            'as_admin': 1
        },
        'Sample service error code 20000 Unauthorized: Anonymous users ' +
        'may not act as service administrators.')


def _get_links_from_sample_set_fail(sample_port, token, params, expected):
    url = f'http://localhost:{sample_port}'
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_data_links_from_sample_set',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


def _get_current_epochmillis():
    return round(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)


def test_expire_data_link(sample_port, workspace, kafka):
    _expire_data_link(sample_port, workspace, None, kafka)


def test_expire_data_link_with_data_id(sample_port, workspace, kafka):
    _expire_data_link(sample_port, workspace, 'whee', kafka)


def _expire_data_link(sample_port, workspace, dataid, kafka):
    ''' also tests that 'as_user' is ignored if 'as_admin' is false '''
    _clear_kafka_messages(kafka)

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli.set_permissions({'id': 1, 'new_permission': 'w', 'users': [USER4]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN3,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'},
                       {'id': 'bar', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )
    _replace_acls(url, id1, TOKEN3, {'admin': [USER4]})

    # create links
    lid1 = _create_link(url, TOKEN3, USER3,
                        {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': dataid})
    lid2 = _create_link(url, TOKEN3, USER3,
                        {'id': id1, 'version': 1, 'node': 'bar', 'upa': '1/1/1', 'dataid': 'fake'})

    time.sleep(1)  # need to be able to set a resonable effective time to fetch links

    # expire link
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.expire_data_link',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'dataid': dataid, 'as_user': USER1}]
    })
    # print(ret.text)
    assert ret.ok is True

    # check links
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.get_data_links_from_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'effective_time': _get_current_epochmillis() - 500}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 2
    assert_ms_epoch_close_to_now(ret.json()['result'][0]['effective_time'])
    links = ret.json()['result'][0]['links']
    assert len(links) == 2
    for link in links:
        if link['dataid'] == 'fake':
            current_link = link
        else:
            expired_link = link
    assert_ms_epoch_close_to_now(expired_link['expired'])
    assert_ms_epoch_close_to_now(expired_link['created'] + 1000)
    del expired_link['created']
    del expired_link['expired']

    assert expired_link == {
            'linkid': lid1,
            'id': id1,
            'version': 1,
            'node': 'foo',
            'upa': '1/1/1',
            'dataid': dataid,
            'createdby': USER3,
            'expiredby': USER4,
         }

    assert_ms_epoch_close_to_now(current_link['created'] + 1000)
    del current_link['created']

    assert current_link == {
            'linkid': lid2,
            'id': id1,
            'version': 1,
            'node': 'bar',
            'upa': '1/1/1',
            'dataid': 'fake',
            'createdby': USER3,
            'expiredby': None,
            'expired': None
         }

    _check_kafka_messages(
        kafka,
        [
            {'event_type': 'NEW_SAMPLE', 'sample_id': id1, 'sample_ver': 1},
            {'event_type': 'ACL_CHANGE', 'sample_id': id1},
            {'event_type': 'NEW_LINK', 'link_id': lid1},
            {'event_type': 'NEW_LINK', 'link_id': lid2},
            {'event_type': 'EXPIRED_LINK', 'link_id': lid1},
        ])


def test_expire_data_link_as_admin(sample_port, workspace, kafka):
    _expire_data_link_as_admin(sample_port, workspace, None, USER2, kafka)


def test_expire_data_link_as_admin_impersonate_user(sample_port, workspace, kafka):
    _expire_data_link_as_admin(sample_port, workspace, USER4, USER4, kafka)


def _expire_data_link_as_admin(sample_port, workspace, user, expected_user, kafka):
    _clear_kafka_messages(kafka)

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli.set_permissions({'id': 1, 'new_permission': 'w', 'users': [USER4]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN3,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'},
                       {'id': 'bar', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )

    # create links
    lid = _create_link(url, TOKEN3, USER3,
                       {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'duidy'})

    time.sleep(1)  # need to be able to set a resonable effective time to fetch links

    # expire link
    ret = requests.post(url, headers=get_authorized_headers(TOKEN2), json={
        'method': 'SampleService.expire_data_link',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'dataid': 'duidy', 'as_admin': 1, 'as_user': user}]
    })
    # print(ret.text)
    assert ret.ok is True

    # check links
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.get_data_links_from_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'effective_time': _get_current_epochmillis() - 500}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 2
    assert_ms_epoch_close_to_now(ret.json()['result'][0]['effective_time'])
    links = ret.json()['result'][0]['links']
    assert len(links) == 1
    link = links[0]
    assert_ms_epoch_close_to_now(link['expired'])
    assert_ms_epoch_close_to_now(link['created'] + 1000)
    del link['created']
    del link['expired']

    assert link == {
            'linkid': lid,
            'id': id1,
            'version': 1,
            'node': 'foo',
            'upa': '1/1/1',
            'dataid': 'duidy',
            'createdby': USER3,
            'expiredby': expected_user,
         }

    _check_kafka_messages(
        kafka,
        [
            {'event_type': 'NEW_SAMPLE', 'sample_id': id1, 'sample_ver': 1},
            {'event_type': 'NEW_LINK', 'link_id': lid},
            {'event_type': 'EXPIRED_LINK', 'link_id': lid},
        ])


def test_expire_data_link_fail(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN3,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )

    # create links
    _create_link(url, TOKEN3, USER3,
                 {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

    _expire_data_link_fail(
        sample_port, TOKEN3, {}, 'Sample service error code 30000 Missing input parameter: upa')
    _expire_data_link_fail(
        sample_port, TOKEN3, {'upa': '1/0/1'},
        'Sample service error code 30001 Illegal input parameter: 1/0/1 is not a valid UPA')
    _expire_data_link_fail(
        sample_port, TOKEN3, {'upa': '1/1/1', 'dataid': 'foo\nbar'},
        'Sample service error code 30001 Illegal input parameter: ' +
        'dataid contains control characters')
    _expire_data_link_fail(
        sample_port, TOKEN4, {'upa': '1/1/1', 'dataid': 'yay'},
        'Sample service error code 20000 Unauthorized: User user4 cannot write to workspace 1')

    wscli.delete_workspace({'id': 1})
    _expire_data_link_fail(
        sample_port, TOKEN3, {'upa': '1/1/1', 'dataid': 'yay'},
        'Sample service error code 50040 No such workspace data: Workspace 1 is deleted')

    wsadmin = Workspace(wsurl, token=TOKEN_WS_FULL_ADMIN)
    wsadmin.administer({'command': 'undeleteWorkspace', 'params': {'id': 1}})
    _expire_data_link_fail(
        sample_port, TOKEN3, {'upa': '1/1/2', 'dataid': 'yay'},
        'Sample service error code 50050 No such data link: 1/1/2:yay')
    _expire_data_link_fail(
        sample_port, TOKEN3, {'upa': '1/1/1', 'dataid': 'yee'},
        'Sample service error code 50050 No such data link: 1/1/1:yee')

    wscli.set_permissions({'id': 1, 'new_permission': 'w', 'users': [USER4]})
    _expire_data_link_fail(
        sample_port, TOKEN4, {'upa': '1/1/1', 'dataid': 'yay'},
        'Sample service error code 20000 Unauthorized: User user4 cannot ' +
        f'administrate sample {id1}')

    # admin tests
    _expire_data_link_fail(
        sample_port, TOKEN2,
        {'upa': '1/1/1', 'dataid': 'yay', 'as_admin': ['t'], 'as_user': 'foo\tbar'},
        'Sample service error code 30001 Illegal input parameter: ' +
        'userid contains control characters')
    _expire_data_link_fail(
        sample_port, TOKEN3,
        {'upa': '1/1/1', 'dataid': 'yay', 'as_admin': ['t'], 'as_user': USER4},
        'Sample service error code 20000 Unauthorized: User user3 does not have ' +
        'the necessary administration privileges to run method expire_data_link')
    _expire_data_link_fail(
        sample_port, TOKEN2,
        {'upa': '1/1/1', 'dataid': 'yay', 'as_admin': ['t'], 'as_user': 'fake'},
        'Sample service error code 50000 No such user: fake')


def _expire_data_link_fail(sample_port, token, params, expected):
    _request_fail(sample_port, 'expire_data_link', token, params, expected)


def _request_fail(sample_port, method, token, params, expected):
    url = f'http://localhost:{sample_port}'
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.' + method,
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


def test_get_links_from_data(sample_port, workspace):

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        {'name': 'baz', 'data': {}, 'type': 'Trivial.Object-1.0'},
        {'name': 'baz', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli.set_permissions({'id': 1, 'new_permission': 'w', 'users': [USER4]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN3,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )

    id2 = _create_sample(
        url,
        TOKEN4,
        {'name': 'myothersample',
         'node_tree': [{'id': 'root2', 'type': 'BioReplicate'},
                       {'id': 'foo2', 'type': 'TechReplicate', 'parent': 'root2'}
                       ]
         },
        1
        )
    # ver 2
    _create_sample(
        url,
        TOKEN4,
        {'id': id2,
         'name': 'myothersample3',
         'node_tree': [{'id': 'root3', 'type': 'BioReplicate'},
                       {'id': 'foo3', 'type': 'TechReplicate', 'parent': 'root3'}
                       ]
         },
        2
        )

    # create links
    lid1 = _create_link(
        url, TOKEN3, USER3, {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/2/2'})
    lid2 = _create_link(
        url, TOKEN4, USER4,
        {'id': id2, 'version': 1, 'node': 'root2', 'upa': '1/1/1', 'dataid': 'column1'})
    lid3 = _create_link(
        url, TOKEN4, USER4,
        {'id': id2, 'version': 2, 'node': 'foo3', 'upa': '1/2/2', 'dataid': 'column2'})

    # get links from object 1/2/2
    ret = _get_links_from_data(url, TOKEN3, {'upa': '1/2/2'})

    assert_ms_epoch_close_to_now(ret['effective_time'])
    res = ret['links']
    expected_links = [
        {
            'linkid': lid1,
            'id': id1,
            'version': 1,
            'node': 'foo',
            'upa': '1/2/2',
            'dataid': None,
            'createdby': USER3,
            'expiredby': None,
            'expired': None
         },
        {
            'linkid': lid3,
            'id': id2,
            'version': 2,
            'node': 'foo3',
            'upa': '1/2/2',
            'dataid': 'column2',
            'createdby': USER4,
            'expiredby': None,
            'expired': None
        }
    ]

    assert len(res) == len(expected_links)
    for link in res:
        assert_ms_epoch_close_to_now(link['created'])
        del link['created']

    for link in expected_links:
        assert link in res

    # get links from object 1/1/1
    ret = _get_links_from_data(url, TOKEN3, {'upa': '1/1/1'})

    assert_ms_epoch_close_to_now(ret['effective_time'])
    res = ret['links']
    assert_ms_epoch_close_to_now(res[0]['created'])
    del res[0]['created']
    assert res == [
        {
            'linkid': lid2,
            'id': id2,
            'version': 1,
            'node': 'root2',
            'upa': '1/1/1',
            'dataid': 'column1',
            'createdby': USER4,
            'expiredby': None,
            'expired': None
         }
    ]

    # get links from object 1/2/1
    ret = _get_links_from_data(url, TOKEN3, {'upa': '1/2/1'})

    assert_ms_epoch_close_to_now(ret['effective_time'])
    assert ret['links'] == []


def _get_links_from_data(url, token, params, print_resp=False):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_data_links_from_data',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    if print_resp:
        print(ret.text)
    assert ret.ok is True
    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 2
    return ret.json()['result'][0]


def test_get_links_from_data_expired(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli.set_permissions({'id': 1, 'new_permission': 'w', 'users': [USER4]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN3,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )
    _replace_acls(url, id1, TOKEN3, {'admin': [USER4]})

    # create links
    lid1 = _create_link(url, TOKEN3, USER3,
                        {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

    oldlinkactive = datetime.datetime.now()
    time.sleep(1)

    # update link node
    lid2 = _create_link(url, TOKEN4, USER4, {
        'id': id1,
        'version': 1,
        'node': 'root',
        'upa': '1/1/1',
        'dataid': 'yay',
        'update': 1
    })

    # get current link
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1'}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    res = ret.json()['result'][0]
    assert len(res) == 2
    assert_ms_epoch_close_to_now(res['effective_time'])
    del res['effective_time']
    created = res['links'][0]['created']
    assert_ms_epoch_close_to_now(created)
    del res['links'][0]['created']
    assert res == {'links': [
        {
            'linkid': lid2,
            'id': id1,
            'version': 1,
            'node': 'root',
            'upa': '1/1/1',
            'dataid': 'yay',
            'createdby': USER4,
            'expiredby': None,
            'expired': None
         }
    ]}

    # get expired link
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_data',
        'version': '1.1',
        'id': '42',
        'params': [{
            'upa': '1/1/1',
            'effective_time': round(oldlinkactive.timestamp() * 1000)}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    res = ret.json()['result'][0]
    assert res['links'][0]['expired'] == created - 1
    assert_ms_epoch_close_to_now(res['links'][0]['created'] + 1000)
    del res['links'][0]['created']
    del res['links'][0]['expired']
    assert res == {
        'effective_time': round(oldlinkactive.timestamp() * 1000),
        'links': [
            {
                'linkid': lid1,
                'id': id1,
                'version': 1,
                'node': 'foo',
                'upa': '1/1/1',
                'dataid': 'yay',
                'createdby': USER3,
                'expiredby': USER4,
            }
        ]}


def test_get_links_from_data_public_read(sample_port, workspace):

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN1)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli.set_global_permission({'id': 1, 'new_permission': 'r'})

    # create samples
    id_ = _create_generic_sample(url, TOKEN1)

    # create links
    lid = _create_link(url, TOKEN1, USER1, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1'})

    for token in [None, TOKEN4]:  # anon user, user 4 has no explicit perms
        ret = _get_links_from_data(url, token, {'upa': '1/1/1'})

        assert_ms_epoch_close_to_now(ret['effective_time'])
        assert len(ret['links']) == 1
        link = ret['links'][0]
        assert_ms_epoch_close_to_now(link['created'])
        del link['created']
        assert link == {
                'linkid': lid,
                'id': id_,
                'version': 1,
                'node': 'foo',
                'upa': '1/1/1',
                'dataid': None,
                'createdby': USER1,
                'expiredby': None,
                'expired': None
            }


def test_get_links_from_data_as_admin(sample_port, workspace):

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN4)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN4,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )

    # create links
    lid = _create_link(url, TOKEN4, USER4, {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1'})

    # get links from object, user 3 has admin read perms
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'as_admin': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    assert len(ret.json()['result'][0]) == 2
    assert_ms_epoch_close_to_now(ret.json()['result'][0]['effective_time'])
    assert len(ret.json()['result'][0]['links']) == 1
    link = ret.json()['result'][0]['links'][0]
    assert_ms_epoch_close_to_now(link['created'])
    del link['created']
    assert link == {
            'linkid': lid,
            'id': id1,
            'version': 1,
            'node': 'foo',
            'upa': '1/1/1',
            'dataid': None,
            'createdby': USER4,
            'expiredby': None,
            'expired': None
         }


def test_get_links_from_data_fail(sample_port, workspace):
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    _get_link_from_data_fail(
        sample_port, TOKEN3, {},
        'Sample service error code 30000 Missing input parameter: upa')
    _get_link_from_data_fail(
        sample_port, TOKEN3, {'upa': '1/1/1', 'effective_time': 'foo'},
        "Sample service error code 30001 Illegal input parameter: key 'effective_time' " +
        "value of 'foo' is not a valid epoch millisecond timestamp")
    _get_link_from_data_fail(
        sample_port, TOKEN4, {'upa': '1/1/1'},
        'Sample service error code 20000 Unauthorized: User user4 cannot read upa 1/1/1')
    _get_link_from_data_fail(
        sample_port, None, {'upa': '1/1/1'},
        'Sample service error code 20000 Unauthorized: Anonymous users cannot read upa 1/1/1')
    _get_link_from_data_fail(
        sample_port, TOKEN3, {'upa': '1/2/1'},
        'Sample service error code 50040 No such workspace data: Object 1/2/1 does not exist')

    # admin tests (also tests missing / deleted objects)
    _get_link_from_data_fail(
        sample_port, TOKEN4, {'upa': '1/1/1', 'as_admin': 1},
        'Sample service error code 20000 Unauthorized: User user4 does not have the necessary ' +
        'administration privileges to run method get_data_links_from_data')
    _get_link_from_data_fail(
        sample_port, None, {'upa': '1/1/1', 'as_admin': 1},
        'Sample service error code 20000 Unauthorized: Anonymous users may not act ' +
        'as service administrators.')
    _get_link_from_data_fail(
        sample_port, TOKEN3, {'upa': '1/1/2', 'as_admin': 1},
        'Sample service error code 50040 No such workspace data: Object 1/1/2 does not exist')
    _get_link_from_data_fail(
        sample_port, TOKEN3, {'upa': '2/1/1', 'as_admin': 1},
        'Sample service error code 50040 No such workspace data: No workspace with id 2 exists')

    wscli.delete_objects([{'ref': '1/1'}])
    _get_link_from_data_fail(
        sample_port, TOKEN3, {'upa': '1/1/1', 'as_admin': 1},
        'Sample service error code 50040 No such workspace data: Object 1/1/1 does not exist')

    wscli.delete_workspace({'id': 1})
    _get_link_from_data_fail(
        sample_port, TOKEN3, {'upa': '1/1/1', 'as_admin': 1},
        'Sample service error code 50040 No such workspace data: Workspace 1 is deleted')


def _get_link_from_data_fail(sample_port, token, params, expected):
    url = f'http://localhost:{sample_port}'
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_data_links_from_data',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


def test_get_sample_via_data(sample_port, workspace):

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli.set_permissions({'id': 1, 'new_permission': 'r', 'users': [USER4]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN3,
        {'name': 'mysample',
         'node_tree': [{'id': 'root',
                        'type': 'BioReplicate',
                        'meta_user': {'a': {'b': 'f', 'e': 'g'}, 'c': {'d': 'h'}},
                        'meta_controlled': {'foo': {'bar': 'baz'}, 'premature': {'e': 'fakeout'}},
                        'source_meta': [{'key': 'foo', 'skey': 'b', 'svalue': {'x': 'y'}}]
                        },
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )

    id2 = _create_sample(
        url,
        TOKEN3,
        {'name': 'unused', 'node_tree': [{'id': 'unused', 'type': 'BioReplicate'}]},
        1
        )
    # ver 2
    _create_sample(
        url,
        TOKEN3,
        {'id': id2,
         'name': 'myothersample3',
         'node_tree': [{'id': 'root3', 'type': 'BioReplicate'},
                       {'id': 'foo3', 'type': 'TechReplicate', 'parent': 'root3'}
                       ]
         },
        2
        )

    # create links
    _create_link(url, TOKEN3, USER3, {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1'})
    _create_link(
        url, TOKEN3, USER3,
        {'id': id2, 'version': 2, 'node': 'root3', 'upa': '1/1/1', 'dataid': 'column1'})

    # get first sample via link from object 1/1/1 using a token that has no access
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.get_sample_via_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'id': str(id1), 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    res = ret.json()['result'][0]
    assert_ms_epoch_close_to_now(res['save_date'])
    del res['save_date']

    expected = {
        'id': id1,
        'version': 1,
        'name': 'mysample',
        'user': USER3,
        'node_tree': [{'id': 'root',
                       'type': 'BioReplicate',
                       'parent': None,
                       'meta_user': {'a': {'b': 'f', 'e': 'g'}, 'c': {'d': 'h'}},
                       'meta_controlled': {'foo': {'bar': 'baz'}, 'premature': {'e': 'fakeout'}},
                       'source_meta': [{'key': 'foo', 'skey': 'b', 'svalue': {'x': 'y'}}],
                       },
                      {'id': 'foo',
                       'type': 'TechReplicate',
                       'parent': 'root',
                       'meta_controlled': {},
                       'meta_user': {},
                       'source_meta': [],
                       },
                      ]
        }
    assert res == expected

    # get second sample via link from object 1/1/1 using a token that has no access
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.get_sample_via_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'id': str(id2), 'version': 2}]
    })
    # print(ret.text)
    assert ret.ok is True

    res = ret.json()['result'][0]
    assert_ms_epoch_close_to_now(res['save_date'])
    del res['save_date']

    expected = {
        'id': id2,
        'version': 2,
        'name': 'myothersample3',
        'user': USER3,
        'node_tree': [{'id': 'root3',
                       'type': 'BioReplicate',
                       'parent': None,
                       'meta_controlled': {},
                       'meta_user': {},
                       'source_meta': [],
                       },
                      {'id': 'foo3',
                       'type': 'TechReplicate',
                       'parent': 'root3',
                       'meta_controlled': {},
                       'meta_user': {},
                       'source_meta': [],
                       },
                      ]
        }
    assert res == expected


def test_get_sample_via_data_expired_with_anon_user(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli.set_global_permission({'id': 1, 'new_permission': 'r'})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN3,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )
    id2 = _create_sample(
        url,
        TOKEN3,
        {'name': 'myothersample',
         'node_tree': [{'id': 'root2', 'type': 'BioReplicate'},
                       {'id': 'foo2', 'type': 'TechReplicate', 'parent': 'root2'}
                       ]
         },
        1
        )

    # create links
    _create_link(url, TOKEN3, USER3,
                 {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

    # update link node
    _create_link(url, TOKEN3, USER3, {
        'id': id2,
        'version': 1,
        'node': 'root2',
        'upa': '1/1/1',
        'dataid': 'yay',
        'update': 1,
    })
    # pulled link from server to check the old link was expired

    # get sample via current link
    ret = requests.post(url, headers=get_authorized_headers(None), json={
        'method': 'SampleService.get_sample_via_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'id': str(id2), 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    res = ret.json()['result'][0]
    assert_ms_epoch_close_to_now(res['save_date'])
    del res['save_date']

    expected = {
        'id': id2,
        'version': 1,
        'name': 'myothersample',
        'user': USER3,
        'node_tree': [{'id': 'root2',
                       'type': 'BioReplicate',
                       'parent': None,
                       'meta_user': {},
                       'meta_controlled': {},
                       'source_meta': [],
                       },
                      {'id': 'foo2',
                       'type': 'TechReplicate',
                       'parent': 'root2',
                       'meta_controlled': {},
                       'meta_user': {},
                       'source_meta': [],
                       },
                      ]
        }
    assert res == expected

    # get sample via expired link
    ret = requests.post(url, headers=get_authorized_headers(None), json={
        'method': 'SampleService.get_sample_via_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'id': str(id1), 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    res = ret.json()['result'][0]
    assert_ms_epoch_close_to_now(res['save_date'])
    del res['save_date']

    expected = {
        'id': id1,
        'version': 1,
        'name': 'mysample',
        'user': USER3,
        'node_tree': [{'id': 'root',
                       'type': 'BioReplicate',
                       'parent': None,
                       'meta_user': {},
                       'meta_controlled': {},
                       'source_meta': [],
                       },
                      {'id': 'foo',
                       'type': 'TechReplicate',
                       'parent': 'root',
                       'meta_controlled': {},
                       'meta_user': {},
                       'source_meta': [],
                       },
                      ]
        }
    assert res == expected


def test_get_sample_via_data_public_read(sample_port, workspace):

    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN1)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    wscli.set_global_permission({'id': 1, 'new_permission': 'r'})

    # create samples
    id_ = _create_generic_sample(url, TOKEN1)

    # create links
    _create_link(url, TOKEN1, USER1, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1'})

    # get sample via link from object 1/1/1 using a token that has no explicit access
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.get_sample_via_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'id': str(id_), 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    res = ret.json()['result'][0]
    assert_ms_epoch_close_to_now(res['save_date'])
    del res['save_date']

    expected = {
        'id': id_,
        'version': 1,
        'name': 'mysample',
        'user': USER1,
        'node_tree': [{'id': 'root',
                       'type': 'BioReplicate',
                       'parent': None,
                       'meta_user': {},
                       'meta_controlled': {},
                       'source_meta': [],
                       },
                      {'id': 'foo',
                       'type': 'TechReplicate',
                       'parent': 'root',
                       'meta_controlled': {},
                       'meta_user': {},
                       'source_meta': [],
                       },
                      ]
        }
    assert res == expected


def test_get_sample_via_data_fail(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN3,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )

    # create links
    _create_link(url, TOKEN3, USER3,
                 {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

    _get_sample_via_data_fail(
        sample_port, TOKEN3, {},
        'Sample service error code 30000 Missing input parameter: upa')
    _get_sample_via_data_fail(
        sample_port, TOKEN3, {'upa': '1/1/1'},
        'Sample service error code 30000 Missing input parameter: id')
    _get_sample_via_data_fail(
        sample_port, TOKEN3, {'upa': '1/1/1', 'id': id1},
        'Sample service error code 30000 Missing input parameter: version')
    _get_sample_via_data_fail(
        sample_port, TOKEN4, {'upa': '1/1/1', 'id': id1, 'version': 1},
        'Sample service error code 20000 Unauthorized: User user4 cannot read upa 1/1/1')
    _get_sample_via_data_fail(
        sample_port, None, {'upa': '1/1/1', 'id': id1, 'version': 1},
        'Sample service error code 20000 Unauthorized: Anonymous users cannot read upa 1/1/1')
    _get_sample_via_data_fail(
        sample_port, TOKEN3, {'upa': '1/2/1', 'id': id1, 'version': 1},
        'Sample service error code 50040 No such workspace data: Object 1/2/1 does not exist')
    badid = uuid.uuid4()
    _get_sample_via_data_fail(
        sample_port, TOKEN3, {'upa': '1/1/1', 'id': str(badid), 'version': 1},
        'Sample service error code 50050 No such data link: There is no link from UPA 1/1/1 ' +
        f'to sample {badid}')
    _get_sample_via_data_fail(
        sample_port, TOKEN3, {'upa': '1/1/1', 'id': str(id1), 'version': 2},
        f'Sample service error code 50020 No such sample version: {id1} ver 2')


def _get_sample_via_data_fail(sample_port, token, params, expected):
    # could make a single method that just takes the service method name to DRY things up a bit
    url = f'http://localhost:{sample_port}'
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_sample_via_data',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


def test_get_data_link(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN4)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN4,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )

    # create link
    lid = _create_link(url, TOKEN4, USER4,
                       {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

    # get link, user 3 has admin read perms
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_link',
        'version': '1.1',
        'id': '42',
        'params': [{'linkid': lid}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    link = ret.json()['result'][0]
    created = link.pop('created')
    assert_ms_epoch_close_to_now(created)
    assert link == {
            'linkid': lid,
            'id': id1,
            'version': 1,
            'node': 'foo',
            'upa': '1/1/1',
            'dataid': 'yay',
            'createdby': USER4,
            'expiredby': None,
            'expired': None
         }

    # expire link
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.expire_data_link',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'dataid': 'yay'}]
    })
    # print(ret.text)
    assert ret.ok is True

    # get link, user 5 has full perms
    ret = requests.post(url, headers=get_authorized_headers(TOKEN5), json={
        'method': 'SampleService.get_data_link',
        'version': '1.1',
        'id': '42',
        'params': [{'linkid': lid}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result']) == 1
    link = ret.json()['result'][0]
    assert_ms_epoch_close_to_now(link['expired'])
    del link['expired']
    assert link == {
            'linkid': lid,
            'id': id1,
            'version': 1,
            'node': 'foo',
            'upa': '1/1/1',
            'dataid': 'yay',
            'created': created,
            'createdby': USER4,
            'expiredby': USER4,
         }


def test_get_data_link_fail(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN4)

    # create workspace & objects
    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    # create samples
    id1 = _create_sample(
        url,
        TOKEN4,
        {'name': 'mysample',
         'node_tree': [{'id': 'root', 'type': 'BioReplicate'},
                       {'id': 'foo', 'type': 'TechReplicate', 'parent': 'root'}
                       ]
         },
        1
        )

    # create link
    lid = _create_link(url, TOKEN4, USER4,
                       {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

    _get_data_link_fail(
        sample_port, TOKEN3, {}, 'Sample service error code 30000 Missing input parameter: linkid')
    _get_data_link_fail(
        sample_port, TOKEN4, {'linkid': lid},
        'Sample service error code 20000 Unauthorized: User user4 does not have the necessary ' +
        'administration privileges to run method get_data_link')
    oid = uuid.uuid4()
    _get_data_link_fail(
        sample_port, TOKEN3, {'linkid': str(oid)},
        f'Sample service error code 50050 No such data link: {oid}')


def _get_data_link_fail(sample_port, token, params, expected):
    # could make a single method that just takes the service method name to DRY things up a bit
    url = f'http://localhost:{sample_port}'
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_data_link',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == expected


# ###########################
# Auth user lookup tests
# ###########################

# for some reason having sample_port along with auth in the test fn args prevents a tear down
# error, not quite sure why

def test_user_lookup_build_fail_bad_args():
    _user_lookup_build_fail(
        '', 'foo', ValueError('auth_url cannot be a value that evaluates to false'))
    _user_lookup_build_fail(
        'http://foo.com', '', ValueError('auth_token cannot be a value that evaluates to false'))


def test_user_lookup_build_fail_bad_token(sample_port, auth):
    _user_lookup_build_fail(
        f'http://localhost:{auth.port}/testmode',
        'tokentokentoken!',
        InvalidTokenError('KBase auth server reported token is invalid.'))


def test_user_lookup_build_fail_bad_auth_url(sample_port, auth):
    _user_lookup_build_fail(
        f'http://localhost:{auth.port}/testmode/foo',
        TOKEN1,
        IOError('Error from KBase auth server: HTTP 404 Not Found'))


def test_user_lookup_build_fail_not_auth_url(auth):
    _user_lookup_build_fail(
        'https://httpbin.org/status/404',
        TOKEN1,
        IOError('Non-JSON response from KBase auth server, status code: 404'))

def _user_lookup_build_fail(url, token, expected):
    with raises(Exception) as got:
        KBaseUserLookup(url, token)
    assert_exception_correct(got.value, expected)


def test_user_lookup(sample_port, auth):
    ul = KBaseUserLookup(f'http://localhost:{auth.port}/testmode', TOKEN1)
    assert ul.invalid_users([]) == []
    assert ul.invalid_users([UserID(USER1), UserID(USER2), UserID(USER3)]) == []

def test_user_lookup_cache(sample_port, auth):
    ul = KBaseUserLookup(f'http://localhost:{auth.port}/testmode', TOKEN1)
    assert ul._valid_cache.get(USER1, default=False) is False
    assert ul._valid_cache.get(USER2, default=False) is False
    ul.invalid_users([UserID(USER1)])
    assert ul._valid_cache.get(USER1, default=False) is True
    assert ul._valid_cache.get(USER2, default=False) is False

def test_user_lookup_bad_users(sample_port, auth):
    ul = KBaseUserLookup(f'http://localhost:{auth.port}/testmode/', TOKEN1)
    assert ul.invalid_users(
        [UserID('nouserhere'), UserID(USER1), UserID(USER2), UserID('whooptydoo'),
         UserID(USER3)]) == [UserID('nouserhere'), UserID('whooptydoo')]


def test_user_lookup_fail_bad_args(sample_port, auth):
    ul = KBaseUserLookup(f'http://localhost:{auth.port}/testmode/', TOKEN1)
    _user_lookup_fail(ul, None, ValueError('usernames cannot be None'))
    _user_lookup_fail(ul, [UserID('foo'), UserID('bar'), None], ValueError(
        'Index 2 of iterable usernames cannot be a value that evaluates to false'))


def test_user_lookup_fail_bad_username(sample_port, auth):
    ul = KBaseUserLookup(f'http://localhost:{auth.port}/testmode/', TOKEN1)
    # maybe possibly this error should be shortened
    # definitely clear the user name is illegal though, there's no question about that
    _user_lookup_fail(ul, [UserID('1')], InvalidUserError(
        'The KBase auth server is being very assertive about one of the usernames being ' +
        'illegal: 30010 Illegal user name: Illegal user name [1]: 30010 Illegal user name: ' +
        'Username must start with a letter'))


def _user_lookup_fail(userlookup, users, expected):
    with raises(Exception) as got:
        userlookup.invalid_users(users)
    assert_exception_correct(got.value, expected)


def test_is_admin(sample_port, auth):
    n = AdminPermission.NONE
    r = AdminPermission.READ
    f = AdminPermission.FULL

    _check_is_admin(auth.port, [n, n, n, n])
    _check_is_admin(auth.port, [f, f, n, n], ['fulladmin1'])
    _check_is_admin(auth.port, [n, f, n, n], ['fulladmin2'])
    _check_is_admin(auth.port, [n, n, r, n], None, ['readadmin1'])
    _check_is_admin(auth.port, [n, r, n, n], None, ['readadmin2'])
    _check_is_admin(auth.port, [n, f, n, n], ['fulladmin2'], ['readadmin2'])
    _check_is_admin(auth.port, [n, f, r, n], ['fulladmin2'], ['readadmin1'])


def _check_is_admin(port, results, full_roles=None, read_roles=None):
    ul = KBaseUserLookup(
        f'http://localhost:{port}/testmode/',
        TOKEN_SERVICE,
        full_roles,
        read_roles)

    for t, u, r in zip([TOKEN1, TOKEN2, TOKEN3, TOKEN4], [USER1, USER2, USER3, USER4], results):
        assert ul.is_admin(t) == (r, u)

def test_is_admin_cache(sample_port, auth):
    ul = KBaseUserLookup(f'http://localhost:{auth.port}/testmode/', TOKEN_SERVICE)
    assert ul._admin_cache.get(TOKEN1, default=False) is False
    assert ul._admin_cache.get(TOKEN2, default=False) is False
    ul.is_admin(TOKEN1)
    assert ul._admin_cache.get(TOKEN1, default=False) is not False
    assert ul._admin_cache.get(TOKEN2, default=False) is False

def test_is_admin_fail_bad_input(sample_port, auth):
    ul = KBaseUserLookup(f'http://localhost:{auth.port}/testmode/', TOKEN_SERVICE)

    _is_admin_fail(ul, None, ValueError('token cannot be a value that evaluates to false'))
    _is_admin_fail(ul, '', ValueError('token cannot be a value that evaluates to false'))


def test_is_admin_fail_bad_token(sample_port, auth):
    ul = KBaseUserLookup(f'http://localhost:{auth.port}/testmode/', TOKEN_SERVICE)

    _is_admin_fail(ul, 'bad token here', InvalidTokenError(
        'KBase auth server reported token is invalid.'))


def _is_admin_fail(userlookup, user, expected):
    with raises(Exception) as got:
        userlookup.is_admin(user)
    assert_exception_correct(got.value, expected)


# ###########################
# Workspace wrapper tests
# ###########################


def test_workspace_wrapper_has_permission(sample_port, workspace):
    url = f'http://localhost:{workspace.port}'
    wscli = Workspace(url, token=TOKEN_WS_READ_ADMIN)
    ws = WS(wscli)

    wscli2 = Workspace(url, token=TOKEN2)
    wscli2.create_workspace({'workspace': 'foo'})
    wscli2.save_objects({'id': 1,
                         'objects': [{'name': 'bar', 'type': 'Trivial.Object-1.0', 'data': {}}]})
    wscli2.save_objects({'id': 1,
                         'objects': [{'name': 'foo', 'type': 'Trivial.Object-1.0', 'data': {}}]})
    wscli2.save_objects({'id': 1,
                         'objects': [{'name': 'foo', 'type': 'Trivial.Object-1.0', 'data': {}}]})

    ws.has_permission(UserID(USER2), WorkspaceAccessType.ADMIN, 1)  # Shouldn't fail
    ws.has_permission(UserID(USER2), WorkspaceAccessType.ADMIN, upa=UPA('1/2/2'))  # Shouldn't fail


def test_workspace_wrapper_has_permission_fail_bad_args(sample_port, workspace):
    url = f'http://localhost:{workspace.port}'
    wscli2 = Workspace(url, token=TOKEN2)
    wscli2.create_workspace({'workspace': 'foo'})
    wscli2.save_objects({'id': 1,
                         'objects': [{'name': 'bar', 'type': 'Trivial.Object-1.0', 'data': {}}]})
    wscli2.save_objects({'id': 1,
                         'objects': [{'name': 'foo', 'type': 'Trivial.Object-1.0', 'data': {}}]})

    _workspace_wrapper_has_permission_fail(
        workspace.port, UserID(USER1), 1, None, UnauthorizedError(
            'User user1 cannot read workspace 1'))
    _workspace_wrapper_has_permission_fail(
        workspace.port, UserID(USER1), None, UPA('1/2/1'),
        UnauthorizedError('User user1 cannot read upa 1/2/1'))
    _workspace_wrapper_has_permission_fail(
        workspace.port, UserID('fakeuser'), 1, None, UnauthorizedError(
            'User fakeuser cannot read workspace 1'))
    _workspace_wrapper_has_permission_fail(
        workspace.port, UserID('fakeuser'), None, UPA('1/2/1'),
        UnauthorizedError('User fakeuser cannot read upa 1/2/1'))
    _workspace_wrapper_has_permission_fail(
        workspace.port, UserID(USER2), 2, None,
        NoSuchWorkspaceDataError('No workspace with id 2 exists'))
    _workspace_wrapper_has_permission_fail(
        workspace.port, UserID(USER2), None, UPA('2/1/1'),
        NoSuchWorkspaceDataError('No workspace with id 2 exists'))
    _workspace_wrapper_has_permission_fail(
        workspace.port, UserID(USER2), None, UPA('1/2/2'),
        NoSuchWorkspaceDataError('Object 1/2/2 does not exist'))
    _workspace_wrapper_has_permission_fail(
        workspace.port, UserID(USER2), None, UPA('1/3/1'),
        NoSuchWorkspaceDataError('Object 1/3/1 does not exist'))

    wscli2.delete_objects([{'ref': '1/2'}])
    _workspace_wrapper_has_permission_fail(
        workspace.port, UserID(USER2), None, UPA('1/2/1'),
        NoSuchWorkspaceDataError('Object 1/2/1 does not exist'))

    wscli2.delete_workspace({'id': 1})
    _workspace_wrapper_has_permission_fail(
        workspace.port, UserID(USER2), None, UPA('1/1/1'),
        NoSuchWorkspaceDataError('Workspace 1 is deleted'))
    _workspace_wrapper_has_permission_fail(
        workspace.port, UserID(USER2), 1, None, NoSuchWorkspaceDataError('Workspace 1 is deleted'))


def _workspace_wrapper_has_permission_fail(ws_port, user, wsid, upa, expected):
    url = f'http://localhost:{ws_port}'
    wscli = Workspace(url, token=TOKEN_WS_READ_ADMIN)
    ws = WS(wscli)

    with raises(Exception) as got:
        ws.has_permission(user, WorkspaceAccessType.READ, wsid, upa)
    assert_exception_correct(got.value, expected)


def test_workspace_wrapper_get_workspaces(sample_port, workspace):
    url = f'http://localhost:{workspace.port}'
    wscli = Workspace(url, token=TOKEN_WS_READ_ADMIN)
    ws = WS(wscli)

    wscli1 = Workspace(url, token=TOKEN1)
    wscli1.create_workspace({'workspace': 'baz'})

    wscli2 = Workspace(url, token=TOKEN2)
    wscli2.create_workspace({'workspace': 'foo'})
    wscli2.set_global_permission({'id': 2, 'new_permission': 'r'})

    wscli3 = Workspace(url, token=TOKEN3)
    wscli3.create_workspace({'workspace': 'bar'})
    wscli3.set_permissions({'id': 3, 'users': [USER1], 'new_permission': 'r'})
    wscli3.create_workspace({'workspace': 'invisible'})

    assert ws.get_user_workspaces(UserID(USER1)) == [1, 2, 3]  # not 4


def test_workspace_wrapper_get_workspaces_fail_no_user(sample_port, workspace):
    url = f'http://localhost:{workspace.port}'
    wscli = Workspace(url, token=TOKEN_WS_READ_ADMIN)
    ws = WS(wscli)

    with raises(Exception) as got:
        ws.get_user_workspaces(UserID('fakeuser'))
    assert_exception_correct(got.value, NoSuchUserError('User fakeuser is not a valid user'))


# ###########################
# Kafka notifier tests
# ###########################

def test_kafka_notifier_init_fail():
    _kafka_notifier_init_fail(None, 't', MissingParameterError('bootstrap_servers'))
    _kafka_notifier_init_fail('   \t   ', 't', MissingParameterError('bootstrap_servers'))
    _kafka_notifier_init_fail('localhost:10000', None, MissingParameterError('topic'))
    _kafka_notifier_init_fail('localhost:10000', '   \t   ', MissingParameterError('topic'))
    _kafka_notifier_init_fail(
        'localhost:10000', 'mytopic' + 243 * 'a',
        IllegalParameterError('topic exceeds maximum length of 249'))
    _kafka_notifier_init_fail(f'localhost:{find_free_port()}', 'mytopic', NoBrokersAvailable())

    for c in ['Ѽ', '_', '.', '*']:
        _kafka_notifier_init_fail('localhost:10000', f'topic{c}topic', ValueError(
            f'Illegal character in Kafka topic topic{c}topic: {c}'))


def _kafka_notifier_init_fail(servers, topic, expected):
    with raises(Exception) as got:
        KafkaNotifier(servers, topic)
    assert_exception_correct(got.value, expected)


def test_kafka_notifier_new_sample(sample_port, kafka):
    topic = 'abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ-' + 186 * 'a'
    kn = KafkaNotifier(f'localhost:{kafka.port}', topic)
    try:
        id_ = uuid.uuid4()

        kn.notify_new_sample_version(id_, 6)

        _check_kafka_messages(
            kafka,
            [{'event_type': 'NEW_SAMPLE', 'sample_id': str(id_), 'sample_ver': 6}],
            topic)
    finally:
        kn.close()


def test_kafka_notifier_notify_new_sample_version_fail(sample_port, kafka):
    kn = KafkaNotifier(f'localhost:{kafka.port}', 'mytopic')

    _kafka_notifier_notify_new_sample_version_fail(kn, None, 1, ValueError(
        'sample_id cannot be a value that evaluates to false'))
    _kafka_notifier_notify_new_sample_version_fail(kn, uuid.uuid4(), 0, ValueError(
        'sample_ver must be > 0'))
    _kafka_notifier_notify_new_sample_version_fail(kn, uuid.uuid4(), -3, ValueError(
        'sample_ver must be > 0'))

    kn.close()
    _kafka_notifier_notify_new_sample_version_fail(kn, uuid.uuid4(), 1, ValueError(
        'client is closed'))


def _kafka_notifier_notify_new_sample_version_fail(notifier, sample, version, expected):
    with raises(Exception) as got:
        notifier.notify_new_sample_version(sample, version)
    assert_exception_correct(got.value, expected)


def test_kafka_notifier_acl_change(sample_port, kafka):
    kn = KafkaNotifier(f'localhost:{kafka.port}', 'topictopic')
    try:
        id_ = uuid.uuid4()

        kn.notify_sample_acl_change(id_)

        _check_kafka_messages(
            kafka,
            [{'event_type': 'ACL_CHANGE', 'sample_id': str(id_)}],
            'topictopic')
    finally:
        kn.close()


def test_kafka_notifier_notify_acl_change_fail(sample_port, kafka):
    kn = KafkaNotifier(f'localhost:{kafka.port}', 'mytopic')

    _kafka_notifier_notify_acl_change_fail(kn, None, ValueError(
        'sample_id cannot be a value that evaluates to false'))

    kn.close()
    _kafka_notifier_notify_acl_change_fail(kn, uuid.uuid4(), ValueError(
        'client is closed'))


def _kafka_notifier_notify_acl_change_fail(notifier, sample, expected):
    with raises(Exception) as got:
        notifier.notify_sample_acl_change(sample)
    assert_exception_correct(got.value, expected)


def test_kafka_notifier_new_link(sample_port, kafka):
    kn = KafkaNotifier(f'localhost:{kafka.port}', 'topictopic')
    try:
        id_ = uuid.uuid4()

        kn.notify_new_link(id_)

        _check_kafka_messages(
            kafka,
            [{'event_type': 'NEW_LINK', 'link_id': str(id_)}],
            'topictopic')
    finally:
        kn.close()


def test_kafka_notifier_new_link_fail(sample_port, kafka):
    kn = KafkaNotifier(f'localhost:{kafka.port}', 'mytopic')

    _kafka_notifier_new_link_fail(kn, None, ValueError(
        'link_id cannot be a value that evaluates to false'))

    kn.close()
    _kafka_notifier_new_link_fail(kn, uuid.uuid4(), ValueError(
        'client is closed'))


def _kafka_notifier_new_link_fail(notifier, sample, expected):
    with raises(Exception) as got:
        notifier.notify_new_link(sample)
    assert_exception_correct(got.value, expected)


def test_kafka_notifier_expired_link(sample_port, kafka):
    kn = KafkaNotifier(f'localhost:{kafka.port}', 'topictopic')
    try:
        id_ = uuid.uuid4()

        kn.notify_expired_link(id_)

        _check_kafka_messages(
            kafka,
            [{'event_type': 'EXPIRED_LINK', 'link_id': str(id_)}],
            'topictopic')
    finally:
        kn.close()


def test_kafka_notifier_expired_link_fail(sample_port, kafka):
    kn = KafkaNotifier(f'localhost:{kafka.port}', 'mytopic')

    _kafka_notifier_expired_link_fail(kn, None, ValueError(
        'link_id cannot be a value that evaluates to false'))

    kn.close()
    _kafka_notifier_expired_link_fail(kn, uuid.uuid4(), ValueError(
        'client is closed'))


def _kafka_notifier_expired_link_fail(notifier, sample, expected):
    with raises(Exception) as got:
        notifier.notify_expired_link(sample)
    assert_exception_correct(got.value, expected)


def test_validate_sample(sample_port):
    _validate_sample_as_admin(sample_port, None, TOKEN2, USER2)


def _validate_sample_as_admin(sample_port, as_user, get_token, expected_user):
    url = f'http://localhost:{sample_port}'

    ret = requests.post(url, headers=get_authorized_headers(TOKEN2), json={
        'method': 'SampleService.validate_samples',
        'version': '1.1',
        'id': '67',
        'params': [{
            'samples': [{
                'name': 'mysample',
                'node_tree': [{
                    'id': 'root',
                    'type': 'BioReplicate',
                    'meta_controlled': {'foo': {'bar': 'baz'}},
                    'meta_user': {'a': {'b': 'c'}}
                }]
            }]
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    ret_json = ret.json()['result'][0]
    assert 'mysample' not in ret_json['errors']
