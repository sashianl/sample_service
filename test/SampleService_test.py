import shutil
import tempfile
from configparser import ConfigParser
from pytest import fixture

from core import test_utils
from mongo_controller import MongoController
from auth_controller import AuthController

_AUTH_DB = 'test_auth_db'


def create_deploy_cfg(mongo_port):
    cfg = ConfigParser()
    ss = 'SampleService'
    cfg.add_section(ss)
    cfg[ss]['arango-url'] = 'foo'
    cfg[ss]['arango-db'] = 'foo'
    cfg[ss]['arango-user'] = 'foo'
    cfg[ss]['arango-pwd'] = 'foo'

    cfg[ss]['sample-collection'] = 'foo'
    cfg[ss]['version-collection'] = 'foo'
    cfg[ss]['version-edge-collection'] = 'foo'
    cfg[ss]['node-collection'] = 'foo'
    cfg[ss]['node-edge-collection'] = 'foo'
    cfg[ss]['schema-collection'] = 'foo'

    _, path = tempfile.mkstemp('.cfg', 'deploy-', dir=test_utils.get_temp_dir(), text=True)

    with open(path, 'w') as handle:
        cfg.write(handle)

    return path


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
    jd = test_utils.get_jars_dir()
    tempdir = test_utils.get_temp_dir()
    auth = AuthController(jd, f'localhost:{mongo.port}', _AUTH_DB, tempdir)
    print(f'running KBase Auth2 {auth.version} on port {auth.port} in dir {auth.temp_dir}')

    yield auth

    del_temp = test_utils.get_delete_temp_files()
    print(f'shutting down auth, delete_temp_files={del_temp}')
    auth.destroy(del_temp)


def test_fake(auth):
    pass
