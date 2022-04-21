import time

import requests

JARS_DIR = 'test.jars.dir'
TEST_TEMP_DIR = 'test.temp.dir'
KEEP_TEMP_DIR = 'test.temp.dir.keep'

TEST_CONFIG_FILE_SECTION = 'sampleservicetest'

TEST_FILE_LOC_ENV_KEY = 'SAMPLESERV_TEST_FILE'

_CONFIG = None



def assert_exception_correct(got: Exception, expected: Exception):
    assert got.args == expected.args
    assert type(got) == type(expected)


def assert_ms_epoch_close_to_now(time_):
    now_ms = time.time() * 1000
    assert now_ms + 1000 > time_
    assert now_ms - 1000 < time_


def create_auth_user(auth_url, username, displayname):
    ret = requests.post(
        auth_url + '/testmode/api/V2/testmodeonly/user',
        headers={'accept': 'application/json'},
        json={'user': username, 'display': displayname})
    if not ret.ok:
        ret.raise_for_status()


def create_auth_login_token(auth_url, username):
    ret = requests.post(
        auth_url + '/testmode/api/V2/testmodeonly/token',
        headers={'accept': 'application/json'},
        json={'user': username, 'type': 'Login'})
    if not ret.ok:
        ret.raise_for_status()
    return ret.json()['token']


def create_auth_role(auth_url, role, description):
    ret = requests.post(
        auth_url + '/testmode/api/V2/testmodeonly/customroles',
        headers={'accept': 'application/json'},
        json={'id': role, 'desc': description})
    if not ret.ok:
        ret.raise_for_status()


def set_custom_roles(auth_url, user, roles):
    ret = requests.put(
        auth_url + '/testmode/api/V2/testmodeonly/userroles',
        headers={'accept': 'application/json'},
        json={'user': user, 'customroles': roles})
    if not ret.ok:
        ret.raise_for_status()


def get_authorized_headers(token):
    headers = {"accept": "application/json"}
    if token is not None:
        headers["authorization"] = token
    return headers
