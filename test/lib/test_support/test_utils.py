import configparser
import datetime
import os
import socket
import uuid
from contextlib import closing
from logging import Formatter
from logging import LogRecord
from pathlib import Path
from typing import List

from SampleService.core.user import UserID

JARS_DIR = 'test.jars.dir'
TEST_TEMP_DIR = 'test.temp.dir'
KEEP_TEMP_DIR = 'test.temp.dir.keep'

TEST_CONFIG_FILE_SECTION = 'sampleservicetest'

TEST_FILE_LOC_ENV_KEY = 'SAMPLESERV_TEST_FILE'

_CONFIG = None


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


def get_current_epochmillis():
    return round(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)


class TerstFermerttr(Formatter):

    logs: List[LogRecord] = []

    def __init__(self):
        pass

    def format(self, record):
        self.logs.append(record)
        return 'no logs here, no sir'


class TestException(Exception):
    __test__ = False


def u(user):
    return UserID(user)

def dt(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)


def make_uuid():
    return uuid.uuid4()


def nw():
    return datetime.datetime.fromtimestamp(1, tz=datetime.timezone.utc)