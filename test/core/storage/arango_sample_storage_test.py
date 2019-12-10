
import datetime
import uuid

from pytest import raises, fixture
from core import test_utils
from core.test_utils import assert_exception_correct
from core.arango_controller import ArangoController
from SampleService.core.sample import SampleWithID, SampleNode, SubSampleType
from SampleService.core.errors import MissingParameterError, NoSuchSampleError, ConcurrencyError
from SampleService.core.errors import NoSuchSampleVersionError
from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage
from SampleService.core.storage.errors import SampleStorageError, StorageInitException

TEST_NODE = SampleNode('foo')

TEST_DB_NAME = 'test_sample_service'
TEST_COL_SAMPLE = 'samples'
TEST_COL_VERSION = 'versions'
TEST_COL_VER_EDGE = 'ver_to_sample'
TEST_COL_NODES = 'nodes'
TEST_COL_NODE_EDGE = 'node_edges'
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
    db.create_collection(TEST_COL_NODES)
    db.create_collection(TEST_COL_NODE_EDGE, edge=True)
    return ArangoSampleStorage(
        arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD),
        TEST_COL_SAMPLE,
        TEST_COL_VERSION,
        TEST_COL_VER_EDGE,
        TEST_COL_NODES,
        TEST_COL_NODE_EDGE)


def test_fail_startup_bad_args(arango):
    samplestorage_method(arango)
    db = arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD)

    s = TEST_COL_SAMPLE
    v = TEST_COL_VERSION
    ve = TEST_COL_VER_EDGE
    n = TEST_COL_NODES
    ne = TEST_COL_NODE_EDGE
    _fail_startup(None, s, v, ve, n, ne,
                  ValueError('db cannot be a value that evaluates to false'))
    _fail_startup(db, '', v, ve, n, ne, MissingParameterError('sample_collection'))
    _fail_startup(db, s, '', ve, n, ne, MissingParameterError('version_collection'))
    _fail_startup(db, s, v, '', n, ne, MissingParameterError('version_edge_collection'))
    _fail_startup(db, s, v, ve, '', ne, MissingParameterError('node_collection'))
    _fail_startup(db, s, v, ve, n, '', MissingParameterError('node_edge_collection'))


def test_fail_startup_incorrect_collection_type(arango):
    samplestorage_method(arango)
    db = arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD)
    db.create_collection('sampleedge', edge=True)

    s = TEST_COL_SAMPLE
    v = TEST_COL_VERSION
    ve = TEST_COL_VER_EDGE
    n = TEST_COL_NODES
    ne = TEST_COL_NODE_EDGE
    _fail_startup(db, 'sampleedge', v, ve, n, ne, StorageInitException(
        'sample collection sampleedge is not a vertex collection'))
    _fail_startup(db, s, ve, ve, n, ne, StorageInitException(
                  'version collection ver_to_sample is not a vertex collection'))
    _fail_startup(db, s, v, v, n, ne, StorageInitException(
                  'version edge collection versions is not an edge collection'))
    _fail_startup(db, s, v, ve, ne, ne, StorageInitException(
                  'node collection node_edges is not a vertex collection'))
    _fail_startup(db, s, v, ve, n, n, StorageInitException(
                  'node edge collection nodes is not an edge collection'))


def _fail_startup(db, colsample, colver, colveredge, colnode, colnodeedge, expected):
    with raises(Exception) as got:
        ArangoSampleStorage(db, colsample, colver, colveredge, colnode, colnodeedge)
    assert_exception_correct(got.value, expected)


def test_indexes_created(samplestorage):
    # test that any non-standard indexes are created.
    print(samplestorage._col_nodes.indexes())
    indexes = samplestorage._col_nodes.indexes()
    assert len(indexes) == 2
    assert indexes[0]['fields'] == ['_key']
    assert indexes[1]['fields'] == ['uuidver']
    assert indexes[1]['deduplicate'] is True
    assert indexes[1]['sparse'] is False
    assert indexes[1]['type'] == 'persistent'
    assert indexes[1]['unique'] is False


def dt(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)


def test_save_and_get_sample(samplestorage):
    n1 = SampleNode('root')
    n2 = SampleNode('kid1', SubSampleType.TECHNICAL_REPLICATE, 'root')
    n3 = SampleNode('kid2', SubSampleType.SUB_SAMPLE, 'kid1')
    n4 = SampleNode('kid3', SubSampleType.TECHNICAL_REPLICATE, 'root')

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')

    assert samplestorage.save_sample(
        'auser', SampleWithID(id_, [n1, n2, n3, n4], dt(8), 'foo')) is True

    assert samplestorage.get_sample(id_) == SampleWithID(id_, [n1, n2, n3, n4], dt(8), 'foo', 1)

    assert samplestorage.get_sample_acls(id_) == {
        'owner': 'auser', 'admin': [], 'write': [], 'read': []}


def test_save_sample_fail_bad_input(samplestorage):
    s = SampleWithID(uuid.UUID('1234567890abcdef1234567890abcdef'), [TEST_NODE], dt(1), 'foo')

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
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True

    assert samplestorage.save_sample(
        'user1', SampleWithID(id_, [TEST_NODE], dt(1), 'bar')) is False


def test_save_sample_fail_duplicate_race_condition(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True

    # this is a very bad and naughty thing to do
    assert samplestorage._save_sample_pt2(
        'user1', SampleWithID(id_, [TEST_NODE], dt(1), 'bar')) is False


def test_get_sample_with_non_updated_version_doc(samplestorage):
    # simulates the case where a save failed part way through. The version UUID was added to the
    # sample doc but the node and version doc updates were not completed
    n1 = SampleNode('root')
    n2 = SampleNode('kid1', SubSampleType.TECHNICAL_REPLICATE, 'root')
    n3 = SampleNode('kid2', SubSampleType.SUB_SAMPLE, 'kid1')
    n4 = SampleNode('kid3', SubSampleType.TECHNICAL_REPLICATE, 'root')

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')

    assert samplestorage.save_sample(
        'auser', SampleWithID(id_, [n1, n2, n3, n4], dt(1), 'foo')) is True

    # this is very naughty
    # checked that these modifications actually work by viewing the db contents
    samplestorage._col_version.update_match({}, {'ver': -1})
    samplestorage._col_nodes.update_match({'name': 'kid2'}, {'ver': -1})

    assert samplestorage.get_sample(id_) == SampleWithID(id_, [n1, n2, n3, n4], dt(1), 'foo', 1)

    for v in samplestorage._col_version.all():
        assert v['ver'] == 1

    for v in samplestorage._col_nodes.all():
        assert v['ver'] == 1


def test_get_sample_with_non_updated_node_doc(samplestorage):
    # simulates the case where a save failed part way through. The version UUID was added to the
    # sample doc but the node doc updates were not completed
    # the version doc update *must* have been updated for this test to exercise the
    # node checking logic because a non-updated version doc will cause the nodes to be updated
    # immediately.
    n1 = SampleNode('root')
    n2 = SampleNode('kid1', SubSampleType.TECHNICAL_REPLICATE, 'root')
    n3 = SampleNode('kid2', SubSampleType.SUB_SAMPLE, 'kid1')
    n4 = SampleNode('kid3', SubSampleType.TECHNICAL_REPLICATE, 'root')

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')

    assert samplestorage.save_sample(
        'auser', SampleWithID(id_, [n1, n2, n3, n4], dt(1), 'foo')) is True

    # this is very naughty
    # checked that these modifications actually work by viewing the db contents
    samplestorage._col_nodes.update_match({'name': 'kid1'}, {'ver': -1})

    assert samplestorage.get_sample(id_) == SampleWithID(id_, [n1, n2, n3, n4], dt(1), 'foo', 1)

    for v in samplestorage._col_nodes.all():
        assert v['ver'] == 1


def test_get_sample_fail_bad_input(samplestorage):
    with raises(Exception) as got:
        samplestorage.get_sample(None)
    assert_exception_correct(
        got.value, ValueError('id_ cannot be a value that evaluates to false'))


def test_get_sample_fail_no_sample(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True

    with raises(Exception) as got:
        samplestorage.get_sample(uuid.UUID('1234567890abcdef1234567890abcdea'))
    assert_exception_correct(
        got.value, NoSuchSampleError('12345678-90ab-cdef-1234-567890abcdea'))


def test_get_sample_fail_no_such_version(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True

    with raises(Exception) as got:
        samplestorage.get_sample(uuid.UUID('1234567890abcdef1234567890abcdef'), version=2)
    assert_exception_correct(
        got.value, NoSuchSampleVersionError('12345678-90ab-cdef-1234-567890abcdef ver 2'))

    assert samplestorage.save_sample_version(SampleWithID(id_, [TEST_NODE], dt(1), 'bar')) == 2

    assert samplestorage.get_sample(id_) == SampleWithID(id_, [TEST_NODE], dt(1), 'bar', 2)

    with raises(Exception) as got:
        samplestorage.get_sample(uuid.UUID('1234567890abcdef1234567890abcdef'), version=3)
    assert_exception_correct(
        got.value, NoSuchSampleVersionError('12345678-90ab-cdef-1234-567890abcdef ver 3'))


def test_get_sample_fail_no_version_doc_1_version(samplestorage):
    # This should be impossible in practice unless someone actively deletes records from the db.
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True

    # this is very naughty
    verdoc_filters = {'id': '12345678-90ab-cdef-1234-567890abcdef', 'ver': 1}
    verdoc = samplestorage._col_version.find(verdoc_filters).next()
    samplestorage._col_version.delete_match(verdoc_filters)

    with raises(Exception) as got:
        samplestorage.get_sample(uuid.UUID('1234567890abcdef1234567890abcdef'), version=1)
    assert_exception_correct(
        got.value, SampleStorageError(f'Corrupt DB: Missing version {verdoc["uuidver"]} ' +
                                      'for sample 12345678-90ab-cdef-1234-567890abcdef'))


def test_get_sample_fail_no_version_doc_2_versions(samplestorage):
    # This should be impossible in practice unless someone actively deletes records from the db.
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True
    assert samplestorage.save_sample_version(SampleWithID(id_, [TEST_NODE], dt(1), 'bar')) == 2

    # this is very naughty
    verdoc_filters = {'id': '12345678-90ab-cdef-1234-567890abcdef', 'ver': 2}
    verdoc = samplestorage._col_version.find(verdoc_filters).next()
    samplestorage._col_version.delete_match(verdoc_filters)

    assert samplestorage.get_sample(id_, version=1) == SampleWithID(
        id_, [TEST_NODE], dt(1), 'foo', 1)

    with raises(Exception) as got:
        samplestorage.get_sample(uuid.UUID('1234567890abcdef1234567890abcdef'), version=2)
    assert_exception_correct(
        got.value, SampleStorageError(f'Corrupt DB: Missing version {verdoc["uuidver"]} ' +
                                      'for sample 12345678-90ab-cdef-1234-567890abcdef'))


def test_get_sample_fail_no_node_docs_1_version(samplestorage):
    # This should be impossible in practice unless someone actively deletes records from the db.
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True

    # this is very naughty
    nodedoc_filters = {'id': '12345678-90ab-cdef-1234-567890abcdef', 'ver': 1}
    nodedoc = samplestorage._col_nodes.find(nodedoc_filters).next()
    samplestorage._col_nodes.delete_match(nodedoc_filters)

    with raises(Exception) as got:
        samplestorage.get_sample(uuid.UUID('1234567890abcdef1234567890abcdef'), version=1)
    assert_exception_correct(
        got.value, SampleStorageError(
            f'Corrupt DB: Missing nodes for version {nodedoc["uuidver"]} of sample ' +
            '12345678-90ab-cdef-1234-567890abcdef'))


def test_get_sample_fail_no_node_docs_2_versions(samplestorage):
    # This should be impossible in practice unless someone actively deletes records from the db.
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True
    assert samplestorage.save_sample_version(SampleWithID(id_, [TEST_NODE], dt(1), 'bar')) == 2

    # this is very naughty
    nodedoc_filters = {'id': '12345678-90ab-cdef-1234-567890abcdef', 'ver': 2}
    nodedoc = samplestorage._col_nodes.find(nodedoc_filters).next()
    samplestorage._col_nodes.delete_match(nodedoc_filters)

    assert samplestorage.get_sample(id_, version=1) == SampleWithID(
        id_, [TEST_NODE], dt(1), 'foo', 1)

    with raises(Exception) as got:
        samplestorage.get_sample(uuid.UUID('1234567890abcdef1234567890abcdef'), version=2)
    assert_exception_correct(
        got.value, SampleStorageError(
            f'Corrupt DB: Missing nodes for version {nodedoc["uuidver"]} of sample ' +
            '12345678-90ab-cdef-1234-567890abcdef'))


def test_save_and_get_sample_version(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(42), 'foo')) is True

    n1 = SampleNode('root')
    n2 = SampleNode('kid1', SubSampleType.TECHNICAL_REPLICATE, 'root')
    n3 = SampleNode('kid2', SubSampleType.SUB_SAMPLE, 'kid1')
    n4 = SampleNode('kid3', SubSampleType.TECHNICAL_REPLICATE, 'root')

    assert samplestorage.save_sample_version(
        SampleWithID(id_, [n1, n2, n3, n4], dt(86), 'bar')) == 2
    assert samplestorage.save_sample_version(
        SampleWithID(id_, [n1], dt(7), 'whiz', version=6)) == 3

    assert samplestorage.get_sample(id_, version=1) == SampleWithID(
        id_, [TEST_NODE], dt(42), 'foo', 1)

    assert samplestorage.get_sample(id_, version=2) == SampleWithID(
        id_, [n1, n2, n3, n4], dt(86), 'bar', 2)

    expected = SampleWithID(id_, [n1], dt(7), 'whiz', 3)
    assert samplestorage.get_sample(id_) == expected
    assert samplestorage.get_sample(id_, version=3) == expected


def test_save_sample_version_fail_bad_input(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    s = SampleWithID(id_, [TEST_NODE], dt(1), 'foo')

    _save_sample_version_fail(samplestorage, None, None, ValueError(
        'sample cannot be a value that evaluates to false'))
    _save_sample_version_fail(samplestorage, s, 0, ValueError(
        'prior_version must be > 0'))


def _save_sample_version_fail(samplestorage, sample, prior_version, expected):
    with raises(Exception) as got:
        samplestorage.save_sample_version(sample, prior_version)
    assert_exception_correct(got.value, expected)


def test_save_sample_version_fail_no_sample(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True

    id2 = uuid.UUID('1234567890abcdef1234567890abcdea')
    with raises(Exception) as got:
        samplestorage.save_sample_version(SampleWithID(id2, [TEST_NODE], dt(1), 'whiz'))
    assert_exception_correct(got.value, NoSuchSampleError('12345678-90ab-cdef-1234-567890abcdea'))


def test_save_sample_version_fail_prior_version(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True
    assert samplestorage.save_sample_version(
        SampleWithID(id_, [SampleNode('bat')], dt(1), 'bar')) == 2

    with raises(Exception) as got:
        samplestorage.save_sample_version(
            SampleWithID(id_, [TEST_NODE], dt(1), 'whiz'), prior_version=1)
    assert_exception_correct(got.value, ConcurrencyError(
        'Version required for sample ' +
        '12345678-90ab-cdef-1234-567890abcdef is 1, but current version is 2'))

    # this is naughty, but need to check race condition
    with raises(Exception) as got:
        samplestorage._save_sample_version_pt2(SampleWithID(id_, [TEST_NODE], dt(1), 'whiz'), 1)
    assert_exception_correct(got.value, ConcurrencyError(
        'Version required for sample ' +
        '12345678-90ab-cdef-1234-567890abcdef is 1, but current version is 2'))


def test_sample_version_update(samplestorage):
    # tests that the versions on node and version documents are updated correctly
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample(
        'user', SampleWithID(id_, [SampleNode('baz')], dt(1), 'foo')) is True

    assert samplestorage.save_sample_version(
        SampleWithID(id_, [SampleNode('bat')], dt(1), 'bar')) == 2

    assert samplestorage.get_sample(id_, version=1) == SampleWithID(
        id_, [SampleNode('baz')], dt(1), 'foo', 1)

    assert samplestorage.get_sample(id_) == SampleWithID(id_, [SampleNode('bat')], dt(1), 'bar', 2)

    idstr = '12345678-90ab-cdef-1234-567890abcdef'
    vers = set()
    # this is naughty
    for n in samplestorage._col_version.find({'id': idstr}):
        vers.add((n['name'], n['ver']))
    assert vers == {('foo', 1), ('bar', 2)}

    nodes = set()
    # this is naughty
    for n in samplestorage._col_nodes.find({'id': idstr}):
        nodes.add((n['name'], n['ver']))
    assert nodes == {('baz', 1), ('bat', 2)}


def test_get_sample_acls_fail_bad_input(samplestorage):
    with raises(Exception) as got:
        samplestorage.get_sample_acls(None)
    assert_exception_correct(
        got.value, ValueError('id_ cannot be a value that evaluates to false'))


def test_get_sample_acls_fail_no_sample(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True

    with raises(Exception) as got:
        samplestorage.get_sample_acls(uuid.UUID('1234567890abcdef1234567890abcdea'))
    assert_exception_correct(
        got.value, NoSuchSampleError('12345678-90ab-cdef-1234-567890abcdea'))
