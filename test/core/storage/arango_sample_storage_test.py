import uuid

from pytest import raises, fixture
from core import test_utils
from core.test_utils import assert_exception_correct
from core.arango_controller import ArangoController
from SampleService.core.sample import SampleWithID
from SampleService.core.errors import MissingParameterError, NoSuchSampleError
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
    return arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD)


@fixture
def samplestorage(arango):
    arango.clear_database(TEST_DB_NAME, drop_indexes=True)
    db = create_test_db(arango)
    db.create_collection(TEST_COL_SAMPLE)
    return ArangoSampleStorage(
        arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD),
        TEST_COL_SAMPLE)


def test_fail_startup(arango):
    db = arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD)
    with raises(Exception) as got:
        ArangoSampleStorage(None, 'foo')
    assert_exception_correct(got.value, ValueError('db cannot be a value that evaluates to false'))

    with raises(Exception) as got:
        ArangoSampleStorage(db, '')
    assert_exception_correct(
        got.value, MissingParameterError('sample_collection'))


def test_save_and_get_sample(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')

    assert samplestorage.save_sample('auser', SampleWithID(id_, 'foo')) is True

    assert samplestorage.get_sample(id_) == SampleWithID(id_, 'foo')

    assert samplestorage.get_sample_acls(id_) == {
        'owner': 'auser', 'admin': [], 'write': [], 'read': []}


def test_save_sample_fail_bad_input(samplestorage):
    s = SampleWithID(uuid.UUID('1234567890abcdef1234567890abcdef'), 'foo')

    with raises(Exception) as got:
        samplestorage.save_sample('', s)
    assert_exception_correct(
        got.value, ValueError('user_name cannot be a value that evaluates to false'))

    with raises(Exception) as got:
        samplestorage.save_sample('a', None)
    assert_exception_correct(
        got.value, ValueError('sample cannot be a value that evaluates to false'))


def test_save_sample_fail_duplicate(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, 'foo')) is True

    assert samplestorage.save_sample('user1', SampleWithID(id_, 'bar')) is False


def test_get_sample_fail_bad_input(samplestorage):
    with raises(Exception) as got:
        samplestorage.get_sample(None)
    assert_exception_correct(
        got.value, ValueError('id_ cannot be a value that evaluates to false'))


def test_get_sample_fail_no_sample(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, 'foo')) is True

    with raises(Exception) as got:
        samplestorage.get_sample(uuid.UUID('1234567890abcdef1234567890abcdea'))
    assert_exception_correct(
        got.value, NoSuchSampleError('12345678-90ab-cdef-1234-567890abcdea'))


def test_get_sample_acls_fail_bad_input(samplestorage):
    with raises(Exception) as got:
        samplestorage.get_sample_acls(None)
    assert_exception_correct(
        got.value, ValueError('id_ cannot be a value that evaluates to false'))


def test_get_sample_acls_fail_no_sample(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, 'foo')) is True

    with raises(Exception) as got:
        samplestorage.get_sample_acls(uuid.UUID('1234567890abcdef1234567890abcdea'))
    assert_exception_correct(
        got.value, NoSuchSampleError('12345678-90ab-cdef-1234-567890abcdea'))
