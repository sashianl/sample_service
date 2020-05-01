# These tests cover the integration of the entire system and do not go into details - that's
# what unit tests are for. As such, typically each method will get a single happy path test and
# a single unhappy path test unless otherwise warranted.

# Tests of the auth user lookup and workspace wrapper code are at the bottom of the file.

import datetime
import os
import tempfile
import requests
import time
import uuid
import yaml
from configparser import ConfigParser
from pytest import fixture, raises
from threading import Thread

from SampleService.SampleServiceImpl import SampleService
from SampleService.core.errors import MissingParameterError, NoSuchWorkspaceDataError
from SampleService.core.user_lookup import KBaseUserLookup, AdminPermission
from SampleService.core.user_lookup import InvalidTokenError, InvalidUserError
from SampleService.core.workspace import WS, WorkspaceAccessType, UPA
from SampleService.core.errors import UnauthorizedError, NoSuchUserError
from SampleService.core.user import UserID

from installed_clients.WorkspaceClient import Workspace as Workspace

from core import test_utils
from core.test_utils import assert_ms_epoch_close_to_now, assert_exception_correct
from arango_controller import ArangoController
from mongo_controller import MongoController
from workspace_controller import WorkspaceController
from auth_controller import AuthController

# TODO should really test a start up for the case where the metadata validation config is not
# supplied, but that's almost never going to be the case and the code is trivial, so YAGNI

VER = '0.1.0-alpha9'

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

USER_NO_TOKEN1 = 'usernt1'
USER_NO_TOKEN2 = 'usernt2'
USER_NO_TOKEN3 = 'usernt3'


def create_deploy_cfg(auth_port, arango_port, workspace_port):
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
                                    'callable-builder': 'noop'
                                    }],
                    'key_metadata': {'a': 'b', 'c': 'd'}
                    },
            'stringlentest': {'validators': [{'module': 'SampleService.core.validator.builtin',
                                              'callable-builder': 'string',
                                              'parameters': {'max-len': 5}
                                              },
                                             {'module': 'SampleService.core.validator.builtin',
                                              'callable-builder': 'string',
                                              'parameters': {'keys': 'spcky', 'max-len': 2}
                                              }],
                              'key_metadata': {'h': 'i', 'j': 'k'}
                              }
        },
        'prefix_validators': {
            'pre': {'validators': [{'module': 'core.config_test_vals',
                                    'callable-builder': 'prefix_validator_test_builder',
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
                };
                ''',
        'dryrun': 0,
        'new_types': ['Object']
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
def service(auth, arango, workspace):
    portint = test_utils.find_free_port()
    clear_db_and_recreate(arango)
    # this is completely stupid. The state is calculated on import so there's no way to
    # test the state creation normally.
    cfgpath = create_deploy_cfg(auth.port, arango.port, workspace.port)
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
def sample_port(service, arango, workspace):
    clear_db_and_recreate(arango)
    workspace.clear_db()
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
    assert s['result'][0]['state'] == 'OK'
    assert s['result'][0]['message'] == ""
    assert s['result'][0]['version'] == VER
    # ignore git url and hash, can change


def get_authorized_headers(token):
    return {'authorization': token, 'accept': 'application/json'}


def test_create_and_get_sample_with_version(sample_port):
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
                       'meta_user': {'a': {'b': 'c'}}}]
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
                       'meta_user': {'a': {'b': 'd'}}}]
    }


def test_create_sample_as_admin(sample_port):
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
            'as_user': '     ' + USER4 + '   '
        }]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0]['version'] == 1
    id_ = ret.json()['result'][0]['id']

    # get
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
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
        'user': USER4,
        'name': 'mysample',
        'node_tree': [{'id': 'root',
                       'parent': None,
                       'type': 'BioReplicate',
                       'meta_controlled': {'foo': {'bar': 'baz'}
                                           },
                       'meta_user': {'a': {'b': 'c'}}}]
    }


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
                       'meta_user': {'a': {'b': 'c'}}}]
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
        f'Sample service error code 30001 Illegal input parameter: sample node tree ' +
        'must be present and a list')


def test_create_sample_fail_bad_metadata(sample_port):
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


def _create_sample_fail_bad_metadata(sample_port, meta, expected):
    url = f'http://localhost:{sample_port}'
    ret = requests.post(url, headers=get_authorized_headers(TOKEN1), json={
        'method': 'SampleService.create_sample',
        'version': '1.1',
        'id': '67',
        'params': [{
            'sample': {'name': 'mysample',
                       'node_tree': [{'id': 'root',
                                      'type': 'BioReplicate',
                                      'meta_controlled': meta
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
        f'Sample service error code 30001 Illegal input parameter: userid contains ' +
        'control characters')


def test_create_sample_fail_admin_no_such_user(sample_port):
    _create_sample_fail_admin_as_user(
        sample_port, USER4 + 'impostor',
        f'Sample service error code 50000 No such user: user4impostor')


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
            'as_user': USER4
        }]
    })

    # print(ret.text)
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        f'Sample service error code 20000 Unauthorized: User user3 does not have the ' +
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
        f'Sample ID {id_[:-1]} must be a UUID string')


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

    ret = requests.post(url, headers=get_authorized_headers(TOKEN2), json={
        'method': 'SampleService.get_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_}]
    })

    # print(ret.text)
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        f'Sample service error code 20000 Unauthorized: User user2 cannot read sample {id_}')


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

    # user 4 has no admin permissions
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.get_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_, 'as_admin': 1}]
    })

    # print(ret.text)
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        f'Sample service error code 20000 Unauthorized: User user4 does not have the ' +
        'necessary administration privileges to run method get_sample')


def test_get_and_replace_acls(sample_port):
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
        'read': []
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
            'write': [USER_NO_TOKEN1, USER_NO_TOKEN2, USER3],
            'read': [USER_NO_TOKEN3, USER4]
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
                'meta_user': {}}]
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
                       'meta_user': {}
                       }]
    }

    # test that an admin can replace ACLs
    _replace_acls(url, id_, TOKEN2, {
        'admin': [USER_NO_TOKEN2],
        'write': [],
        'read': [USER2]
    })

    _assert_acl_contents(url, id_, TOKEN1, {
        'owner': USER1,
        'admin': [USER_NO_TOKEN2],
        'write': [],
        'read': [USER2]
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
        'read': []
        },
        as_admin=1)


def test_replace_acls_as_admin(sample_port):
    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    _assert_acl_contents(url, id_, TOKEN1, {
        'owner': USER1,
        'admin': [],
        'write': [],
        'read': []
    })

    _replace_acls(url, id_, TOKEN2, {
        'admin': [USER2],
        'write': [USER_NO_TOKEN1, USER_NO_TOKEN2, USER3],
        'read': [USER_NO_TOKEN3, USER4],
        },
        as_admin=1)

    _assert_acl_contents(url, id_, TOKEN1, {
        'owner': USER1,
        'admin': [USER2],
        'write': [USER_NO_TOKEN1, USER_NO_TOKEN2, USER3],
        'read': [USER_NO_TOKEN3, USER4],
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


def _assert_acl_contents(url, id_, token, expected, as_admin=0):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_sample_acls',
        'version': '1.1',
        'id': '47',
        'params': [{'id': id_, 'as_admin': as_admin}]
    })
    # print(ret.text)
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
        'Sample service error code 30000 Missing input parameter: Sample ID')


def test_get_acls_fail_permissions(sample_port):

    url = f'http://localhost:{sample_port}'

    id_ = _create_generic_sample(url, TOKEN1)

    ret = requests.post(url, headers=get_authorized_headers(TOKEN2), json={
        'method': 'SampleService.get_sample_acls',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_}]
    })
    assert ret.status_code == 500
    assert ret.json()['error']['message'] == (
        f'Sample service error code 20000 Unauthorized: User user2 cannot read sample {id_}')


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
        f'Sample service error code 20000 Unauthorized: User user4 does not have the ' +
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
        'Sample service error code 30000 Missing input parameter: Sample ID')


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
        f'Sample service error code 50000 No such user: a, philbin_j_montgomery_iii')


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


def _create_link(url, token, params, print_resp=False):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.create_data_link',
        'version': '1.1',
        'id': '42',
        'params': [params]
    })
    if print_resp:
        print(ret.text)
    assert ret.ok is True


def test_create_links_and_get_links_from_sample_basic(sample_port, workspace):
    '''
    Also tests that the 'as_user' key is ignored if 'as_admin' is falsy.
    '''

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
    _create_link(
        url, TOKEN3, {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/2/2', 'as_user': USER1})
    _create_link(
        url, TOKEN3,
        {'id': id1, 'version': 1, 'node': 'root', 'upa': '1/1/1', 'dataid': 'column1'})
    _create_link(
        url, TOKEN4,
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

    assert len(ret.json()['result'][0]) == 1
    res = ret.json()['result'][0]['links']
    expected_links = [
        {
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
    for l in res:
        assert_ms_epoch_close_to_now(l['created'])
        del l['created']

    for l in expected_links:
        assert l in res

    # get links from sample 2
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id2, 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result'][0]) == 1
    res = ret.json()['result'][0]['links']
    assert_ms_epoch_close_to_now(res[0]['created'])
    del res[0]['created']
    assert res == [
        {
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
    assert ret.json()['result'][0] == {'links': []}


def test_update_and_get_links_from_sample(sample_port, workspace):
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
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.create_data_link',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'}]
    })
    # print(ret.text)
    assert ret.ok is True

    oldlinkactive = datetime.datetime.now()
    time.sleep(1)

    # update link node
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.create_data_link',
        'version': '1.1',
        'id': '42',
        'params': [
            {'id': id1,
             'version': 1,
             'node': 'root',
             'upa': '1/1/1',
             'dataid': 'yay',
             'update': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    # get current link
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id1, 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    res = ret.json()['result'][0]
    created = res['links'][0]['created']
    assert_ms_epoch_close_to_now(created)
    del res['links'][0]['created']
    assert res == {'links': [
        {
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

    res = ret.json()['result'][0]
    assert res['links'][0]['expired'] == created - 1
    assert_ms_epoch_close_to_now(res['links'][0]['created'] + 1000)
    del res['links'][0]['created']
    del res['links'][0]['expired']
    assert res == {'links': [
        {
            'id': id1,
            'version': 1,
            'node': 'foo',
            'upa': '1/1/1',
            'dataid': 'yay',
            'createdby': USER3,
            'expiredby': USER4,
         }
    ]}


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
    _create_link(
        url,
        TOKEN2,
        {'id': id1,
         'version': 1,
         'node': 'root',
         'upa': '1/1/1',
         'dataid': 'yeet',
         'as_admin': 1})
    _create_link(
        url,
        TOKEN2,
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

    assert len(ret.json()['result'][0]) == 1
    res = ret.json()['result'][0]['links']
    expected_links = [
        {
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
    for l in res:
        assert_ms_epoch_close_to_now(l['created'])
        del l['created']

    for l in expected_links:
        assert l in res


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
    _create_link(url, TOKEN3, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1'})
    _create_link(url, TOKEN4, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '2/1/1'})
    _create_link(
        url, TOKEN4, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '3/1/1', 'dataid': 'whee'})
    _create_link(url, TOKEN4, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '4/1/1'})

    # check correct links are returned
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_sample',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_, 'version': 1}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result'][0]) == 1
    res = ret.json()['result'][0]['links']
    print(res)
    expected_links = [
        {
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
    for l in res:
        assert_ms_epoch_close_to_now(l['created'])
        del l['created']

    for l in expected_links:
        assert l in res


def test_create_link_fail(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)
    id_ = _create_generic_sample(url, TOKEN3)

    _create_link_fail(
        sample_port, TOKEN3, {'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'},
        'Sample service error code 30000 Missing input parameter: Sample ID')
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
        f'Sample service error code 20000 Unauthorized: User user4 cannot ' +
        f'administrate sample {id_}')

    _replace_acls(url, id_, TOKEN3, {'admin': [USER4]})
    wscli.set_permissions({'id': 1, 'new_permission': 'r', 'users': [USER4]})
    _create_link_fail(  # fails if permission granted is write
        sample_port, TOKEN4, {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1'},
        f'Sample service error code 20000 Unauthorized: User user4 cannot write to upa 1/1/1')

    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})
    _create_link_fail(
        sample_port, TOKEN3, {'id': id_, 'version': 1, 'node': 'fake', 'upa': '1/1/1'},
        f'Sample service error code 50030 No such sample node: {id_} ver 1 fake')

    # admin tests
    _create_link_fail(
        sample_port, TOKEN2,
        {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'as_user': 'foo\bbar'},
        f'Sample service error code 30001 Illegal input parameter: ' +
        'userid contains control characters')
    _create_link_fail(
        sample_port, TOKEN3,
        {'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'as_user': USER4, 'as_admin': 'f'},
        f'Sample service error code 20000 Unauthorized: User user3 does not have ' +
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
        f'Sample service error code 50000 No such user: fake')


def test_create_link_fail_link_exists(sample_port, workspace):
    url = f'http://localhost:{sample_port}'
    wsurl = f'http://localhost:{workspace.port}'
    wscli = Workspace(wsurl, token=TOKEN3)

    wscli.create_workspace({'workspace': 'foo'})
    wscli.save_objects({'id': 1, 'objects': [
        {'name': 'bar', 'data': {}, 'type': 'Trivial.Object-1.0'},
        ]})

    id_ = _create_generic_sample(url, TOKEN3)

    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.create_data_link',
        'version': '1.1',
        'id': '42',
        'params': [{'id': id_, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'}]
    })
    # print(ret.text)
    assert ret.ok is True

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
        'Sample service error code 30000 Missing input parameter: Sample ID')
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
    badid = uuid.uuid4()
    _get_link_from_sample_fail(
        sample_port, TOKEN3, {'id': str(badid), 'version': 1},
        f'Sample service error code 50010 No such sample: {badid}')


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


def _get_current_epochmillis():
    return round(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)


def test_expire_data_link(sample_port, workspace):
    _expire_data_link(sample_port, workspace, None)


def test_expire_data_link_with_data_id(sample_port, workspace):
    _expire_data_link(sample_port, workspace, 'whee')


def _expire_data_link(sample_port, workspace, dataid):
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
    _create_link(
        url, TOKEN3, {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': dataid})
    _create_link(
        url, TOKEN3, {'id': id1, 'version': 1, 'node': 'bar', 'upa': '1/1/1', 'dataid': 'fake'})

    time.sleep(1)  # need to be able to set a resonable effective time to fetch links

    # expire link
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
        'method': 'SampleService.expire_data_link',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1', 'dataid': dataid}]
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

    assert len(ret.json()['result'][0]) == 1
    links = ret.json()['result'][0]['links']
    assert len(links) == 2
    for l in links:
        if l['dataid'] == 'fake':
            current_link = l
        else:
            expired_link = l
    assert_ms_epoch_close_to_now(expired_link['expired'])
    assert_ms_epoch_close_to_now(expired_link['created'] + 1000)
    del expired_link['created']
    del expired_link['expired']

    assert expired_link == {
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
            'id': id1,
            'version': 1,
            'node': 'bar',
            'upa': '1/1/1',
            'dataid': 'fake',
            'createdby': USER3,
            'expiredby': None,
            'expired': None
         }


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
    _create_link(
        url, TOKEN3, {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

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
        f'Sample service error code 20000 Unauthorized: User user4 cannot ' +
        f'administrate sample {id1}')


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
    _create_link(url, TOKEN3, {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/2/2'})
    _create_link(
        url, TOKEN4,
        {'id': id2, 'version': 1, 'node': 'root2', 'upa': '1/1/1', 'dataid': 'column1'})
    _create_link(
        url, TOKEN4,
        {'id': id2, 'version': 2, 'node': 'foo3', 'upa': '1/2/2', 'dataid': 'column2'})

    # get links from object 1/2/2
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/2/2'}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result'][0]) == 1
    res = ret.json()['result'][0]['links']
    expected_links = [
        {
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
    for l in res:
        assert_ms_epoch_close_to_now(l['created'])
        del l['created']

    for l in expected_links:
        assert l in res

    # get links from object 1/1/1
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/1/1'}]
    })
    # print(ret.text)
    assert ret.ok is True

    assert len(ret.json()['result'][0]) == 1
    res = ret.json()['result'][0]['links']
    assert_ms_epoch_close_to_now(res[0]['created'])
    del res[0]['created']
    assert res == [
        {
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
    ret = requests.post(url, headers=get_authorized_headers(TOKEN3), json={
        'method': 'SampleService.get_data_links_from_data',
        'version': '1.1',
        'id': '42',
        'params': [{'upa': '1/2/1'}]
    })
    # print(ret.text)
    assert ret.ok is True
    assert ret.json()['result'][0] == {'links': []}


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
    _create_link(
        url, TOKEN3, {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

    oldlinkactive = datetime.datetime.now()
    time.sleep(1)

    # update link node
    _create_link(url, TOKEN4, {
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

    res = ret.json()['result'][0]
    created = res['links'][0]['created']
    assert_ms_epoch_close_to_now(created)
    del res['links'][0]['created']
    assert res == {'links': [
        {
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

    res = ret.json()['result'][0]
    assert res['links'][0]['expired'] == created - 1
    assert_ms_epoch_close_to_now(res['links'][0]['created'] + 1000)
    del res['links'][0]['created']
    del res['links'][0]['expired']
    assert res == {'links': [
        {
            'id': id1,
            'version': 1,
            'node': 'foo',
            'upa': '1/1/1',
            'dataid': 'yay',
            'createdby': USER3,
            'expiredby': USER4,
         }
    ]}


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
        f'Sample service error code 20000 Unauthorized: User user4 cannot read upa 1/1/1')
    _get_link_from_data_fail(
        sample_port, TOKEN3, {'upa': '1/2/1'},
        f'Sample service error code 50040 No such workspace data: Object 1/2/1 does not exist')


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
                        'meta_controlled': {'foo': {'bar': 'baz'}, 'premature': {'e': 'fakeout'}}},
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
    _create_link(url, TOKEN3, {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1'})
    _create_link(
        url, TOKEN3,
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
                       },
                      {'id': 'foo',
                       'type': 'TechReplicate',
                       'parent': 'root',
                       'meta_controlled': {},
                       'meta_user': {}},
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
                       'meta_user': {}},
                      {'id': 'foo3',
                       'type': 'TechReplicate',
                       'parent': 'root3',
                       'meta_controlled': {},
                       'meta_user': {}},
                      ]
        }
    assert res == expected


def test_get_sample_via_from_data_expired(sample_port, workspace):
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
    _create_link(
        url, TOKEN3, {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

    # update link node
    _create_link(url, TOKEN3, {
        'id': id2,
        'version': 1,
        'node': 'root2',
        'upa': '1/1/1',
        'dataid': 'yay',
        'update': 1,
    })
    # pulled link from server to check the old link was expired

    # get sample via current link
    ret = requests.post(url, headers=get_authorized_headers(TOKEN4), json={
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
                       },
                      {'id': 'foo2',
                       'type': 'TechReplicate',
                       'parent': 'root2',
                       'meta_controlled': {},
                       'meta_user': {}},
                      ]
        }
    assert res == expected

    # get sample via expired link
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
                       'meta_user': {},
                       'meta_controlled': {},
                       },
                      {'id': 'foo',
                       'type': 'TechReplicate',
                       'parent': 'root',
                       'meta_controlled': {},
                       'meta_user': {}},
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
    _create_link(
        url, TOKEN3, {'id': id1, 'version': 1, 'node': 'foo', 'upa': '1/1/1', 'dataid': 'yay'})

    _get_sample_via_data_fail(
        sample_port, TOKEN3, {},
        'Sample service error code 30000 Missing input parameter: upa')
    _get_sample_via_data_fail(
        sample_port, TOKEN3, {'upa': '1/1/1'},
        'Sample service error code 30000 Missing input parameter: Sample ID')
    _get_sample_via_data_fail(
        sample_port, TOKEN3, {'upa': '1/1/1', 'id': id1},
        'Sample service error code 30000 Missing input parameter: version')
    _get_sample_via_data_fail(
        sample_port, TOKEN4, {'upa': '1/1/1', 'id': id1, 'version': 1},
        f'Sample service error code 20000 Unauthorized: User user4 cannot read upa 1/1/1')
    _get_sample_via_data_fail(
        sample_port, TOKEN3, {'upa': '1/2/1', 'id': id1, 'version': 1},
        f'Sample service error code 50040 No such workspace data: Object 1/2/1 does not exist')
    badid = uuid.uuid4()
    _get_sample_via_data_fail(
        sample_port, TOKEN3, {'upa': '1/1/1', 'id': str(badid), 'version': 1},
        f'Sample service error code 50050 No such data link: There is no link from UPA 1/1/1 ' +
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


def test_user_lookup_build_fail_not_auth_url():
    _user_lookup_build_fail(
        f'https://ci.kbase.us/services',
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
