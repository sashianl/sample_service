import shutil
import tempfile
from configparser import ConfigParser
from pytest import fixture

from core import test_utils
from mongo_controller import MongoController


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
def mongo():
    mongoexe = test_utils.get_mongo_exe()
    tempdir = test_utils.get_temp_dir()
    wt = test_utils.get_use_wired_tiger()
    mongo = MongoController(mongoexe, tempdir, wt)
    print('running mongo {}{} on port {} in dir {}'.format(
        mongo.db_version, ' with WiredTiger' if wt else '', mongo.port, mongo.temp_dir))

    yield mongo

    del_temp = test_utils.get_delete_temp_files()
    print('shutting down mongo, delete_temp_files={}'.format(del_temp))
    mongo.destroy(del_temp)
    if del_temp:
        shutil.rmtree(test_utils.get_temp_dir())


def test_fake(mongo):
    pass
