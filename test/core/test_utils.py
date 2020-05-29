import os
import requests
import socket
from contextlib import closing
from pathlib import Path
import configparser
from logging import Formatter
from typing import List
from logging import LogRecord
import time

ARANGO_EXE = 'test.arango.exe'
ARANGO_JS = 'test.arango.js'
KAFKA_BIN_DIR = 'test.kafka.bin.dir'
MONGO_EXE = 'test.mongo.exe'
MONGO_USE_WIRED_TIGER = 'test.mongo.wired_tiger'
JARS_DIR = 'test.jars.dir'
TEST_TEMP_DIR = 'test.temp.dir'
KEEP_TEMP_DIR = 'test.temp.dir.keep'

TEST_CONFIG_FILE_SECTION = 'sampleservicetest'

TEST_FILE_LOC_ENV_KEY = 'SAMPLESERV_TEST_FILE'

_CONFIG = None


def get_arango_exe() -> Path:
    return Path(os.path.abspath(_get_test_property(ARANGO_EXE)))


def get_arango_js() -> Path:
    return Path(os.path.abspath(_get_test_property(ARANGO_JS)))


def get_kafka_bin_dir() -> Path:
    return Path(os.path.abspath(_get_test_property(KAFKA_BIN_DIR)))


def get_mongo_exe() -> Path:
    return Path(os.path.abspath(_get_test_property(MONGO_EXE)))


def get_use_wired_tiger() -> bool:
    return _get_test_property(MONGO_USE_WIRED_TIGER) == 'true'


def get_jars_dir() -> Path:
    return Path(os.path.abspath(_get_test_property(JARS_DIR)))


def get_temp_dir() -> Path:
    return Path(os.path.abspath(_get_test_property(TEST_TEMP_DIR)))


def get_delete_temp_files() -> bool:
    return _get_test_property(KEEP_TEMP_DIR) != 'true'


def _get_test_config_file_path() -> Path:
    p = os.environ.get(TEST_FILE_LOC_ENV_KEY)
    if not p:
        raise TestException("Can't find key {} in environment".format(TEST_FILE_LOC_ENV_KEY))
    return Path(p)


def _get_test_property(prop: str) -> str:
    global _CONFIG
    if not _CONFIG:
        test_cfg = _get_test_config_file_path()
        config = configparser.ConfigParser()
        config.read(test_cfg)
        if TEST_CONFIG_FILE_SECTION not in config:
            raise TestException('No section {} found in test config file {}'
                                .format(TEST_CONFIG_FILE_SECTION, test_cfg))
        sec = config[TEST_CONFIG_FILE_SECTION]
        # a section is not a real map and is missing methods
        _CONFIG = {x: sec[x] for x in sec.keys()}
    if prop not in _CONFIG:
        test_cfg = _get_test_config_file_path()
        raise TestException('Property {} in section {} of test file {} is missing'
                            .format(prop, TEST_CONFIG_FILE_SECTION, test_cfg))
    return _CONFIG[prop]


def find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        return s.getsockname()[1]


def assert_exception_correct(got: Exception, expected: Exception):
    assert got.args == expected.args
    assert type(got) == type(expected)


def assert_ms_epoch_close_to_now(time_):
    now_ms = time.time() * 1000
    assert now_ms + 1000 > time_
    assert now_ms - 1000 < time_


class TerstFermerttr(Formatter):

    logs: List[LogRecord] = []

    def __init__(self):
        pass

    def format(self, record):
        self.logs.append(record)
        return 'no logs here, no sir'


class TestException(Exception):
    __test__ = False


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
