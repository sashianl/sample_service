# These tests cover the integration of the entire system and do not go into details - that's
# what unit tests are for. As such, typically each method will get a single happy path test and
# a single unhappy path test unless otherwise warranted.

# Tests of the auth user lookup code is at the bottom of the file.

import os
import tempfile
import requests
import time
import yaml
from configparser import ConfigParser
from pytest import fixture, raises
from threading import Thread

from SampleService.SampleServiceImpl import SampleService
from SampleService.core.errors import MissingParameterError
from SampleService.core.user_lookup import KBaseUserLookup, AdminPermission
from SampleService.core.user_lookup import InvalidTokenError, InvalidUserError

from core import test_utils
from core.test_utils import assert_ms_epoch_close_to_now, assert_exception_correct
from arango_controller import ArangoController
from mongo_controller import MongoController
from auth_controller import AuthController

# TODO should really test a start up for the case where the metadata validation config is not
# supplied, but that's almost never going to be the case and the code is trivial, so YAGNI

VER = '0.1.0-alpha4'

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


def create_deploy_cfg(auth_port, arango_port):
    cfg = ConfigParser()
    ss = 'SampleService'
    cfg.add_section(ss)

    cfg[ss]['auth-service-url'] = (f'http://localhost:{auth_port}/testmode/' +
                                   'api/legacy/KBase/Sessions/Login')
    cfg[ss]['auth-service-url-allow-insecure'] = 'true'

    cfg[ss]['auth-root-url'] = f'http://localhost:{auth_port}/testmode'
    cfg[ss]['auth-token'] = TOKEN_SERVICE

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

    metacfg = {
        'foo': [{'module': 'SampleService.core.validator.builtin',
                 'callable-builder': 'noop'
                 }],
        'stringlentest': [{'module': 'SampleService.core.validator.builtin',
                           'callable-builder': 'string',
                           'parameters': {'max-len': 5}
                           },
                          {'module': 'SampleService.core.validator.builtin',
                           'callable-builder': 'string',
                           'parameters': {'keys': 'spcky', 'max-len': 2}
                           }],
        'pre': [{'module': 'core.config_test_vals',
                 'callable-builder': 'prefix_validator_test_builder',
                 'prefix': True,
                 'parameters': {'fail_on_arg': 'fail_plz'}
                 }]
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
    global TOKEN1
    global TOKEN2
    global TOKEN3
    global TOKEN4
    jd = test_utils.get_jars_dir()
    tempdir = test_utils.get_temp_dir()
    auth = AuthController(jd, f'localhost:{mongo.port}', _AUTH_DB, tempdir)
    print(f'running KBase Auth2 {auth.version} on port {auth.port} in dir {auth.temp_dir}')
    url = f'http://localhost:{auth.port}'

    test_utils.create_auth_role(url, 'fulladmin1', 'fa1')
    test_utils.create_auth_role(url, 'fulladmin2', 'fa2')
    test_utils.create_auth_role(url, 'readadmin1', 'ra1')
    test_utils.create_auth_role(url, 'readadmin2', 'ra2')

    test_utils.create_auth_user(url, USER_SERVICE, 'serv')
    TOKEN_SERVICE = test_utils.create_auth_login_token(url, USER_SERVICE)

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
    init_fail(cfg, MissingParameterError('config param schema-collection'))
    cfg['schema-collection'] = 'crap'
    init_fail(cfg, MissingParameterError('config param auth-root-url'))
    cfg['auth-root-url'] = 'crap'
    init_fail(cfg, MissingParameterError('config param auth-token'))
    cfg['auth-token'] = 'crap'
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


def _replace_acls(url, id_, token, acls):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.replace_sample_acls',
        'version': '1.1',
        'id': '67',
        'params': [{'id': id_, 'acls': acls}]
    })
    assert ret.ok is True
    assert ret.json() == {'version': '1.1', 'id': '67', 'result': None}


def _assert_acl_contents(url, id_, token, expected):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
        'method': 'SampleService.get_sample_acls',
        'version': '1.1',
        'id': '47',
        'params': [{'id': id_}]
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


def _create_generic_sample(url, token):
    ret = requests.post(url, headers=get_authorized_headers(token), json={
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
    assert ul.are_valid_users([]) == []
    assert ul.are_valid_users([USER1, USER2, USER3]) == []


def test_user_lookup_bad_users(sample_port, auth):
    ul = KBaseUserLookup(f'http://localhost:{auth.port}/testmode/', TOKEN1)
    assert ul.are_valid_users(
        ['nouserhere', USER1, USER2, 'whooptydoo', USER3]) == ['nouserhere', 'whooptydoo']


def test_user_lookup_fail_bad_args(sample_port, auth):
    ul = KBaseUserLookup(f'http://localhost:{auth.port}/testmode/', TOKEN1)
    _user_lookup_fail(ul, None, ValueError('usernames cannot be None'))
    _user_lookup_fail(ul, ['foo', 'bar', ''], ValueError(
        'Index 2 of iterable usernames cannot be a value that evaluates to false'))


def test_user_lookup_fail_bad_username(sample_port, auth):
    ul = KBaseUserLookup(f'http://localhost:{auth.port}/testmode/', TOKEN1)
    # maybe possibly this error should be shortened
    # definitely clear the user name is illegal though, there's no question about that
    _user_lookup_fail(ul, ['1'], InvalidUserError(
        'The KBase auth server is being very assertive about one of the usernames being ' +
        'illegal: 30010 Illegal user name: Illegal user name [1]: 30010 Illegal user name: ' +
        'Username must start with a letter'))


def _user_lookup_fail(userlookup, users, expected):
    with raises(Exception) as got:
        userlookup.are_valid_users(users)
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

    for t, r in zip([TOKEN1, TOKEN2, TOKEN3, TOKEN4], results):
        assert ul.is_admin(t) == r


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
