import uuid

from pytest import raises, fixture
from core import test_utils
from core.test_utils import assert_exception_correct
from core.arango_controller import ArangoController
from SampleService.core.sample import SampleWithID
from SampleService.core.errors import MissingParameterError, NoSuchSampleError
from SampleService.core.errors import NoSuchSampleVersionError
from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage
from SampleService.core.storage.errors import SampleStorageError, StorageInitException

TEST_DB_NAME = 'test_sample_service'
TEST_COL_SAMPLE = 'samples'
TEST_COL_VERSION = 'versions'
TEST_COL_VER_EDGE = 'ver_to_sample'
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
    return samplestorage_method(arango)


def samplestorage_method(arango):
    arango.clear_database(TEST_DB_NAME, drop_indexes=True)
    db = create_test_db(arango)
    db.create_collection(TEST_COL_SAMPLE)
    db.create_collection(TEST_COL_VERSION)
    db.create_collection(TEST_COL_VER_EDGE, edge=True)
    return ArangoSampleStorage(
        arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD),
        TEST_COL_SAMPLE,
        TEST_COL_VERSION,
        TEST_COL_VER_EDGE)


def test_fail_startup_bad_args(arango):
    samplestorage_method(arango)
    db = arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD)

    s = TEST_COL_SAMPLE
    v = TEST_COL_VERSION
    ve = TEST_COL_VER_EDGE
    _fail_startup(None, s, v, ve, ValueError('db cannot be a value that evaluates to false'))
    _fail_startup(db, '', v, ve, MissingParameterError('sample_collection'))
    _fail_startup(db, s, '', ve, MissingParameterError('version_collection'))
    _fail_startup(db, s, v, '', MissingParameterError('version_edge_collection'))


def test_fail_startup_incorrect_collection_type(arango):
    samplestorage_method(arango)
    db = arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD)
    db.create_collection('sampleedge', edge=True)

    s = TEST_COL_SAMPLE
    v = TEST_COL_VERSION
    ve = TEST_COL_VER_EDGE
    _fail_startup(
        db, 'sampleedge', v, ve, StorageInitException('sampleedge is not a vertex collection'))
    _fail_startup(db, s, ve, ve, StorageInitException('ver_to_sample is not a vertex collection'))
    _fail_startup(db, s, v, v, StorageInitException('versions is not an edge collection'))


def _fail_startup(db, colsample, colver, colveredge, expected):
    with raises(Exception) as got:
        ArangoSampleStorage(db, colsample, colver, colveredge)
    assert_exception_correct(got.value, expected)


def test_save_and_get_sample(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')

    assert samplestorage.save_sample('auser', SampleWithID(id_, 'foo')) is True

    assert samplestorage.get_sample(id_) == SampleWithID(id_, 'foo', 1)

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


def test_save_sample_fail_duplicate_race_condition(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, 'foo')) is True

    # this is a very bad and naughty thing to do
    assert samplestorage._save_sample_pt2('user1', SampleWithID(id_, 'bar')) is False


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


def test_get_sample_fail_no_such_version(samplestorage):
    # TODO test after saving multiple versions as well.
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, 'foo')) is True

    with raises(Exception) as got:
        samplestorage.get_sample(uuid.UUID('1234567890abcdef1234567890abcdef'), version=2)
    assert_exception_correct(
        got.value, NoSuchSampleVersionError('12345678-90ab-cdef-1234-567890abcdef ver 2'))


def test_get_sample_fail_no_version_doc(samplestorage):
    # TODO test after saving multiple versions as well.
    # This should be impossible in practice unless someone actively deletes records from the db.
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, 'foo')) is True

    # this is very naughty
    verdoc_filters = {'id': '12345678-90ab-cdef-1234-567890abcdef', 'ver': 1}
    verdoc = samplestorage._col_version.find(verdoc_filters).next()
    samplestorage._col_version.delete_match(verdoc_filters)

    with raises(Exception) as got:
        samplestorage.get_sample(uuid.UUID('1234567890abcdef1234567890abcdef'), version=1)
    assert_exception_correct(
        got.value, SampleStorageError(f'Corrupt DB: Missing version {verdoc["uuidver"]} ' +
                                      'for sample 12345678-90ab-cdef-1234-567890abcdef'))


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
