from pytest import raises, fixture
from core import test_utils
from core.test_utils import assert_exception_correct
from core.arango_controller import ArangoController
from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage

TEST_DB_NAME = 'test_sample_service'
TEST_COL_SAMPLE = 'samples'
TEST_USER = 'user1'
TEST_PWD = 'password1'


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


@fixture
def idstorage(arango):
    arango.clear_database(TEST_DB_NAME, drop_indexes=True)
    create_test_db(arango)
    return ArangoSampleStorage(arango.client.db())


def test_fail_startup(arango):
    db = arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD)
    with raises(Exception) as got:
        ArangoSampleStorage(None, 'foo')
    assert_exception_correct(got.value, ValueError('db cannot be a value that evaluates to false'))

    with raises(Exception) as got:
        ArangoSampleStorage(db, '')
    assert_exception_correct(
        got.value, ValueError('sample_collection cannot be a value that evaluates to false'))
