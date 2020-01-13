import datetime
import uuid
import time

from pytest import raises, fixture
from core import test_utils
from core.test_utils import assert_exception_correct
from arango_controller import ArangoController
from SampleService.core.acls import SampleACL
from SampleService.core.sample import SampleWithID, SampleNode, SubSampleType
from SampleService.core.errors import MissingParameterError, NoSuchSampleError, ConcurrencyError
from SampleService.core.errors import NoSuchSampleVersionError
from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage
from SampleService.core.storage.errors import SampleStorageError, StorageInitException
from SampleService.core.storage.errors import OwnerChangedException

TEST_NODE = SampleNode('foo')

TEST_DB_NAME = 'test_sample_service'
TEST_COL_SAMPLE = 'samples'
TEST_COL_VERSION = 'versions'
TEST_COL_VER_EDGE = 'ver_to_sample'
TEST_COL_NODES = 'nodes'
TEST_COL_NODE_EDGE = 'node_edges'
TEST_COL_SCHEMA = 'schema'
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


def samplestorage_method(arango):
    clear_db_and_recreate(arango)
    return ArangoSampleStorage(
        arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD),
        TEST_COL_SAMPLE,
        TEST_COL_VERSION,
        TEST_COL_VER_EDGE,
        TEST_COL_NODES,
        TEST_COL_NODE_EDGE,
        TEST_COL_SCHEMA)


def nw():
    return datetime.datetime.fromtimestamp(1, tz=datetime.timezone.utc)


def test_startup_and_check_config_doc(samplestorage):
    # this is very naughty
    assert samplestorage._col_schema.count() == 1
    cfgdoc = samplestorage._col_schema.find({}).next()
    print(cfgdoc)
    assert cfgdoc['_key'] == 'schema'
    assert cfgdoc['schemaver'] == 1
    assert cfgdoc['inupdate'] is False

    # check startup works with cfg object in place
    # this is also very naughty
    ss = ArangoSampleStorage(
        samplestorage._db,
        samplestorage._col_sample.name,
        samplestorage._col_version.name,
        samplestorage._col_ver_edge.name,
        samplestorage._col_nodes.name,
        samplestorage._col_node_edge.name,
        samplestorage._col_schema.name)

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    n = SampleNode('rootyroot')
    assert ss.save_sample('auser', SampleWithID(id_, [n], dt(1), 'foo')) is True
    assert ss.get_sample(id_) == SampleWithID(id_, [n], dt(1), 'foo', version=1)


def test_startup_with_extra_config_doc(arango):
    db = clear_db_and_recreate(arango)

    scol = db.collection('schema')
    scol.insert_many([{'_key': 'schema', 'schemaver': 1, 'inupdate': False},
                      {'schema': 'schema', 'schemaver': 2, 'inupdate': False}])

    s = TEST_COL_SAMPLE
    v = TEST_COL_VERSION
    ve = TEST_COL_VER_EDGE
    n = TEST_COL_NODES
    ne = TEST_COL_NODE_EDGE
    sc = TEST_COL_SCHEMA

    _fail_startup(db, s, v, ve, n, ne, sc, nw, StorageInitException(
        'Multiple config objects found ' +
        'in the database. This should not happen, something is very wrong.'))


def test_startup_with_bad_schema_version(arango):
    db = clear_db_and_recreate(arango)
    col = db.collection(TEST_COL_SCHEMA)
    col.insert({'_key': 'schema', 'schemaver': 4, 'inupdate': False})

    s = TEST_COL_SAMPLE
    v = TEST_COL_VERSION
    ve = TEST_COL_VER_EDGE
    n = TEST_COL_NODES
    ne = TEST_COL_NODE_EDGE
    sc = TEST_COL_SCHEMA

    _fail_startup(db, s, v, ve, n, ne, sc, nw, StorageInitException(
        'Incompatible database schema. Server is v1, DB is v4'))


def test_startup_in_update(arango):
    db = clear_db_and_recreate(arango)
    col = db.collection(TEST_COL_SCHEMA)
    col.insert({'_key': 'schema', 'schemaver': 1, 'inupdate': True})

    s = TEST_COL_SAMPLE
    v = TEST_COL_VERSION
    ve = TEST_COL_VER_EDGE
    n = TEST_COL_NODES
    ne = TEST_COL_NODE_EDGE
    sc = TEST_COL_SCHEMA

    _fail_startup(db, s, v, ve, n, ne, sc, nw, StorageInitException(
        'The database is in the middle of an update from v1 of the schema. Aborting startup.'))


def test_startup_with_unupdated_version_and_node_docs(samplestorage):
    # this test simulates a server coming up after a dirty shutdown, where version and
    # node doc integer versions have not been updated
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

    # this is also very naughty
    ArangoSampleStorage(
        samplestorage._db,
        samplestorage._col_sample.name,
        samplestorage._col_version.name,
        samplestorage._col_ver_edge.name,
        samplestorage._col_nodes.name,
        samplestorage._col_node_edge.name,
        samplestorage._col_schema.name)

    assert samplestorage._col_version.count() == 1
    assert samplestorage._col_ver_edge.count() == 1
    assert samplestorage._col_nodes.count() == 4
    assert samplestorage._col_node_edge.count() == 4

    for v in samplestorage._col_version.all():
        assert v['ver'] == 1

    for v in samplestorage._col_nodes.all():
        assert v['ver'] == 1


def test_startup_with_unupdated_node_docs(samplestorage):
    # this test simulates a server coming up after a dirty shutdown, where
    # node doc integer versions have not been updated
    # version doc cannot be modified such that ver = -1 or the version check will also correct the
    # node docs, negating the point of this test
    n1 = SampleNode('root')
    n2 = SampleNode('kid1', SubSampleType.TECHNICAL_REPLICATE, 'root')
    n3 = SampleNode('kid2', SubSampleType.SUB_SAMPLE, 'kid1')
    n4 = SampleNode('kid3', SubSampleType.TECHNICAL_REPLICATE, 'root')

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')

    assert samplestorage.save_sample(
        'auser', SampleWithID(id_, [n1, n2, n3, n4], dt(1), 'foo')) is True

    assert samplestorage.save_sample_version(
        SampleWithID(id_, [n1, n2, n3, n4], dt(1), 'bar')) == 2

    # this is very naughty
    sample = samplestorage._col_sample.find({}).next()
    uuidver2 = sample['vers'][1]

    # checked that these modifications actually work by viewing the db contents
    samplestorage._col_nodes.update_match({'uuidver': uuidver2, 'name': 'kid2'}, {'ver': -1})

    # this is also very naughty
    ArangoSampleStorage(
        samplestorage._db,
        samplestorage._col_sample.name,
        samplestorage._col_version.name,
        samplestorage._col_ver_edge.name,
        samplestorage._col_nodes.name,
        samplestorage._col_node_edge.name,
        samplestorage._col_schema.name)

    assert samplestorage._col_version.count() == 2
    assert samplestorage._col_ver_edge.count() == 2
    assert samplestorage._col_nodes.count() == 8
    assert samplestorage._col_node_edge.count() == 8

    for v in samplestorage._col_version.all():
        assert v['ver'] == 2 if v['uuidver'] == uuidver2 else 1

    for v in samplestorage._col_nodes.all():
        assert v['ver'] == 2 if v['uuidver'] == uuidver2 else 1


def test_startup_with_no_sample_doc(samplestorage):
    # this test simulates a server coming up after a dirty shutdown, where version and
    # node docs were saved but the sample document was not while saving the first version of
    # a sample
    n1 = SampleNode('root')
    n2 = SampleNode('kid1', SubSampleType.TECHNICAL_REPLICATE, 'root')
    n3 = SampleNode('kid2', SubSampleType.SUB_SAMPLE, 'kid1')
    n4 = SampleNode('kid3', SubSampleType.TECHNICAL_REPLICATE, 'root')

    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdea')

    assert samplestorage.save_sample(
        'auser', SampleWithID(id1, [n1, n2, n3, n4], dt(1), 'foo')) is True

    assert samplestorage.save_sample(
        'auser', SampleWithID(id2, [n1, n2, n3, n4], dt(1000), 'foo')) is True

    # this is very naughty
    assert samplestorage._col_version.count() == 2
    assert samplestorage._col_ver_edge.count() == 2
    assert samplestorage._col_nodes.count() == 8
    assert samplestorage._col_node_edge.count() == 8

    samplestorage._col_sample.delete({'_key': str(id2)})
    # if the sample document hasn't been saved, then none of the integer versions for the
    # sample can have been updated to 1
    samplestorage._col_version.update_match({'id': str(id2)}, {'ver': -1})
    samplestorage._col_nodes.update_match({'id': str(id2)}, {'ver': -1})

    # first test that bringing up the server before the 1hr deletion time limit doesn't change the
    # db:
    # this is also very naughty
    ArangoSampleStorage(
        samplestorage._db,
        samplestorage._col_sample.name,
        samplestorage._col_version.name,
        samplestorage._col_ver_edge.name,
        samplestorage._col_nodes.name,
        samplestorage._col_node_edge.name,
        samplestorage._col_schema.name,
        now=lambda: datetime.datetime.fromtimestamp(4600, tz=datetime.timezone.utc))

    assert samplestorage._col_version.count() == 2
    assert samplestorage._col_ver_edge.count() == 2
    assert samplestorage._col_nodes.count() == 8
    assert samplestorage._col_node_edge.count() == 8

    # now test that bringing up the server after the limit deletes the docs:
    ArangoSampleStorage(
        samplestorage._db,
        samplestorage._col_sample.name,
        samplestorage._col_version.name,
        samplestorage._col_ver_edge.name,
        samplestorage._col_nodes.name,
        samplestorage._col_node_edge.name,
        samplestorage._col_schema.name,
        now=lambda: datetime.datetime.fromtimestamp(4601, tz=datetime.timezone.utc))

    assert samplestorage._col_sample.count() == 1
    assert samplestorage._col_version.count() == 1
    assert samplestorage._col_ver_edge.count() == 1
    assert samplestorage._col_nodes.count() == 4
    assert samplestorage._col_node_edge.count() == 4

    sample = samplestorage._col_sample.find({}).next()
    assert sample['id'] == str(id1)
    uuidver = sample['vers'][0]

    assert len(list(samplestorage._col_version.find({'uuidver': uuidver}))) == 1
    assert len(list(samplestorage._col_ver_edge.find({'uuidver': uuidver}))) == 1
    assert len(list(samplestorage._col_nodes.find({'uuidver': uuidver}))) == 4
    assert len(list(samplestorage._col_node_edge.find({'uuidver': uuidver}))) == 4


def test_startup_with_no_version_in_sample_doc(samplestorage):
    # this test simulates a server coming up after a dirty shutdown, where version and
    # node docs were saved but the sample document was not updated while saving the second
    # version of # a sample
    n1 = SampleNode('root')
    n2 = SampleNode('kid1', SubSampleType.TECHNICAL_REPLICATE, 'root')
    n3 = SampleNode('kid2', SubSampleType.SUB_SAMPLE, 'kid1')
    n4 = SampleNode('kid3', SubSampleType.TECHNICAL_REPLICATE, 'root')

    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')

    assert samplestorage.save_sample(
        'auser', SampleWithID(id1, [n1, n2, n3, n4], dt(1), 'foo')) is True

    assert samplestorage.save_sample_version(
        SampleWithID(id1, [n1, n2, n3, n4], dt(2000), 'foo')) == 2

    # this is very naughty
    assert samplestorage._col_sample.count() == 1
    assert samplestorage._col_version.count() == 2
    assert samplestorage._col_ver_edge.count() == 2
    assert samplestorage._col_nodes.count() == 8
    assert samplestorage._col_node_edge.count() == 8

    sample = samplestorage._col_sample.find({}).next()
    samplestorage._col_sample.update_match({}, {'vers': sample['vers'][:1]})
    uuidver2 = sample['vers'][1]

    # if the sample document hasn't been updated, then none of the integer versions for the
    # sample can have been updated to 1
    samplestorage._col_version.update_match({'uuidver': uuidver2}, {'ver': -1})
    samplestorage._col_nodes.update_match({'uuidver': uuidver2}, {'ver': -1})

    # first test that bringing up the server before the 1hr deletion time limit doesn't change the
    # db:
    # this is also very naughty
    ArangoSampleStorage(
        samplestorage._db,
        samplestorage._col_sample.name,
        samplestorage._col_version.name,
        samplestorage._col_ver_edge.name,
        samplestorage._col_nodes.name,
        samplestorage._col_node_edge.name,
        samplestorage._col_schema.name,
        now=lambda: datetime.datetime.fromtimestamp(5600, tz=datetime.timezone.utc))

    assert samplestorage._col_version.count() == 2
    assert samplestorage._col_ver_edge.count() == 2
    assert samplestorage._col_nodes.count() == 8
    assert samplestorage._col_node_edge.count() == 8

    # now test that bringing up the server after the limit deletes the docs:
    ArangoSampleStorage(
        samplestorage._db,
        samplestorage._col_sample.name,
        samplestorage._col_version.name,
        samplestorage._col_ver_edge.name,
        samplestorage._col_nodes.name,
        samplestorage._col_node_edge.name,
        samplestorage._col_schema.name,
        now=lambda: datetime.datetime.fromtimestamp(5601, tz=datetime.timezone.utc))

    assert samplestorage._col_version.count() == 1
    assert samplestorage._col_ver_edge.count() == 1
    assert samplestorage._col_nodes.count() == 4
    assert samplestorage._col_node_edge.count() == 4

    uuidver1 = sample['vers'][0]

    assert len(list(samplestorage._col_version.find({'uuidver': uuidver1}))) == 1
    assert len(list(samplestorage._col_ver_edge.find({'uuidver': uuidver1}))) == 1
    assert len(list(samplestorage._col_nodes.find({'uuidver': uuidver1}))) == 4
    assert len(list(samplestorage._col_node_edge.find({'uuidver': uuidver1}))) == 4


def test_fail_startup_bad_args(arango):
    samplestorage_method(arango)
    db = arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD)

    s = TEST_COL_SAMPLE
    v = TEST_COL_VERSION
    ve = TEST_COL_VER_EDGE
    n = TEST_COL_NODES
    ne = TEST_COL_NODE_EDGE
    sc = TEST_COL_SCHEMA

    def nw():
        datetime.datetime.fromtimestamp(1, tz=datetime.timezone.utc)

    _fail_startup(None, s, v, ve, n, ne, sc, nw,
                  ValueError('db cannot be a value that evaluates to false'))
    _fail_startup(db, '', v, ve, n, ne, sc, nw, MissingParameterError('sample_collection'))
    _fail_startup(db, s, '', ve, n, ne, sc, nw, MissingParameterError('version_collection'))
    _fail_startup(db, s, v, '', n, ne, sc, nw, MissingParameterError('version_edge_collection'))
    _fail_startup(db, s, v, ve, '', ne, sc, nw, MissingParameterError('node_collection'))
    _fail_startup(db, s, v, ve, n, '', sc, nw, MissingParameterError('node_edge_collection'))
    _fail_startup(db, s, v, ve, n, ne, '', nw, MissingParameterError('schema_collection'))
    _fail_startup(db, s, v, ve, n, ne, sc, None,
                  ValueError('now cannot be a value that evaluates to false'))


def test_fail_startup_incorrect_collection_type(arango):
    samplestorage_method(arango)
    db = arango.client.db(TEST_DB_NAME, TEST_USER, TEST_PWD)
    db.create_collection('sampleedge', edge=True)

    s = TEST_COL_SAMPLE
    v = TEST_COL_VERSION
    ve = TEST_COL_VER_EDGE
    n = TEST_COL_NODES
    ne = TEST_COL_NODE_EDGE
    sc = TEST_COL_SCHEMA

    def nw():
        datetime.datetime.fromtimestamp(1, tz=datetime.timezone.utc)

    _fail_startup(db, 'sampleedge', v, ve, n, ne, sc, nw, StorageInitException(
        'sample collection sampleedge is not a vertex collection'))
    _fail_startup(db, s, ve, ve, n, ne, sc, nw, StorageInitException(
                  'version collection ver_to_sample is not a vertex collection'))
    _fail_startup(db, s, v, v, n, ne, sc, nw, StorageInitException(
                  'version edge collection versions is not an edge collection'))
    _fail_startup(db, s, v, ve, ne, ne, sc, nw, StorageInitException(
                  'node collection node_edges is not a vertex collection'))
    _fail_startup(db, s, v, ve, n, n, sc, nw, StorageInitException(
                  'node edge collection nodes is not an edge collection'))
    _fail_startup(db, s, v, ve, n, ne, ne, nw, StorageInitException(
                  'schema collection node_edges is not a vertex collection'))


def _fail_startup(
        db,
        colsample,
        colver,
        colveredge,
        colnode,
        colnodeedge,
        colschema,
        now,
        expected):

    with raises(Exception) as got:
        ArangoSampleStorage(
            db, colsample, colver, colveredge, colnode, colnodeedge, colschema, now=now)
    assert_exception_correct(got.value, expected)


def test_indexes_created(samplestorage):
    # test that any non-standard indexes are created.
    indexes = samplestorage._col_nodes.indexes()
    assert len(indexes) == 3
    assert indexes[0]['fields'] == ['_key']
    _check_index(indexes[1], ['uuidver'])
    _check_index(indexes[2], ['ver'])

    indexes = samplestorage._col_version.indexes()
    assert len(indexes) == 3
    assert indexes[0]['fields'] == ['_key']
    _check_index(indexes[1], ['uuidver'])
    _check_index(indexes[2], ['ver'])

    indexes = samplestorage._col_node_edge.indexes()
    print(indexes)
    assert len(indexes) == 3
    assert indexes[0]['fields'] == ['_key']
    assert indexes[1]['fields'] == ['_from', '_to']
    _check_index(indexes[2], ['uuidver'])

    indexes = samplestorage._col_ver_edge.indexes()
    assert len(indexes) == 3
    assert indexes[0]['fields'] == ['_key']
    assert indexes[1]['fields'] == ['_from', '_to']
    _check_index(indexes[2], ['uuidver'])


def _check_index(index, fields):
    assert index['fields'] == fields
    assert index['deduplicate'] is True
    assert index['sparse'] is False
    assert index['type'] == 'persistent'
    assert index['unique'] is False


def test_start_consistency_checker_fail_bad_args(samplestorage):
    with raises(Exception) as got:
        samplestorage.start_consistency_checker(interval_sec=0)
    assert_exception_correct(got.value, ValueError('interval_sec must be > 0'))


def test_consistency_checker_run(samplestorage):
    # here we just test that stopping and starting the checker will clean up the db.
    # The cleaning functionality is tested thoroughly above.
    # The db could be in an unclean state if a sample server does down mid save and doesn't
    # come back up.
    n1 = SampleNode('root')
    n2 = SampleNode('kid1', SubSampleType.TECHNICAL_REPLICATE, 'root')
    n3 = SampleNode('kid2', SubSampleType.SUB_SAMPLE, 'kid1')
    n4 = SampleNode('kid3', SubSampleType.TECHNICAL_REPLICATE, 'root')

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')

    assert samplestorage.save_sample(
        'auser', SampleWithID(id_, [n1, n2, n3, n4], dt(1), 'foo')) is True

    assert samplestorage.save_sample_version(
        SampleWithID(id_, [n1, n2, n3, n4], dt(1), 'bar')) == 2

    # this is very naughty
    sample = samplestorage._col_sample.find({}).next()
    uuidver2 = sample['vers'][1]

    samplestorage._col_nodes.update_match({'uuidver': uuidver2, 'name': 'kid2'}, {'ver': -1})

    samplestorage.start_consistency_checker(interval_sec=1)
    samplestorage.start_consistency_checker(interval_sec=1)  # test that running twice does nothing

    time.sleep(0.5)

    assert samplestorage._col_nodes.find({'uuidver': uuidver2, 'name': 'kid2'}).next()['ver'] == -1

    time.sleep(1)

    assert samplestorage._col_version.count() == 2
    assert samplestorage._col_ver_edge.count() == 2
    assert samplestorage._col_nodes.count() == 8
    assert samplestorage._col_node_edge.count() == 8

    for v in samplestorage._col_version.all():
        assert v['ver'] == 2 if v['uuidver'] == uuidver2 else 1

    for v in samplestorage._col_nodes.all():
        assert v['ver'] == 2 if v['uuidver'] == uuidver2 else 1

    # test that pausing stops updating
    samplestorage.stop_consistency_checker()
    samplestorage.stop_consistency_checker()  # test that running twice in a row does nothing

    samplestorage._col_nodes.update_match({'uuidver': uuidver2, 'name': 'kid2'}, {'ver': -1})

    time.sleep(1.5)
    assert samplestorage._col_nodes.find({'uuidver': uuidver2, 'name': 'kid2'}).next()['ver'] == -1

    samplestorage.start_consistency_checker(1)

    time.sleep(1.5)

    assert samplestorage._col_nodes.find({'uuidver': uuidver2, 'name': 'kid2'}).next()['ver'] == 2

    # leaving the checker running can occasionally interfere with other tests, deleting documents
    # that are in the middle of the save process. Stop the checker and wait until the job must've
    # run.
    samplestorage.stop_consistency_checker()
    time.sleep(1)


def dt(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)


def test_save_and_get_sample(samplestorage):
    n1 = SampleNode('root')
    n2 = SampleNode(
        'kid1', SubSampleType.TECHNICAL_REPLICATE, 'root',
        {'a': {'b': 'c', 'd': 'e'}, 'f': {'g': 'h'}},
        {'m': {'n': 'o'}})
    n3 = SampleNode('kid2', SubSampleType.SUB_SAMPLE, 'kid1', {'a': {'b': 'c'}})
    n4 = SampleNode('kid3', SubSampleType.TECHNICAL_REPLICATE, 'root',
                    uncontrolled_metadata={'f': {'g': 'h'}})

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')

    assert samplestorage.save_sample(
        'auser', SampleWithID(id_, [n1, n2, n3, n4], dt(8), 'foo')) is True

    assert samplestorage.get_sample(id_) == SampleWithID(id_, [n1, n2, n3, n4], dt(8), 'foo', 1)

    assert samplestorage.get_sample_acls(id_) == SampleACL('auser')


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
    n2 = SampleNode(
        'kid1', SubSampleType.TECHNICAL_REPLICATE, 'root',
        {'a': {'b': 'c', 'd': 'e'}, 'f': {'g': 'h'}},
        {'m': {'n': 'o'}})
    n3 = SampleNode('kid2', SubSampleType.SUB_SAMPLE, 'kid1', {'a': {'b': 'c'}})
    n4 = SampleNode('kid3', SubSampleType.TECHNICAL_REPLICATE, 'root',
                    uncontrolled_metadata={'f': {'g': 'h'}})

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


def test_replace_sample_acls(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True

    samplestorage.replace_sample_acls(id_, SampleACL(
        'user', ['foo', 'bar'], ['baz', 'bat'], ['whoo']))

    assert samplestorage.get_sample_acls(id_) == SampleACL(
        'user', ['foo', 'bar'], ['baz', 'bat'], ['whoo'])

    samplestorage.replace_sample_acls(id_, SampleACL('user', write=['baz']))

    assert samplestorage.get_sample_acls(id_) == SampleACL('user', write=['baz'])


def test_replace_sample_acls_fail_bad_args(samplestorage):
    with raises(Exception) as got:
        samplestorage.replace_sample_acls(None, SampleACL('user'))
    assert_exception_correct(got.value, ValueError(
        'id_ cannot be a value that evaluates to false'))

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    with raises(Exception) as got:
        samplestorage.replace_sample_acls(id_, None)
    assert_exception_correct(got.value, ValueError(
        'acls cannot be a value that evaluates to false'))


def test_replace_sample_acls_fail_no_sample(samplestorage):
    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id1, [TEST_NODE], dt(1), 'foo')) is True

    id2 = uuid.UUID('1234567890abcdef1234567890abcdea')

    with raises(Exception) as got:
        samplestorage.replace_sample_acls(id2, SampleACL('user'))
    assert_exception_correct(got.value, NoSuchSampleError(str(id2)))


def test_replace_sample_acls_fail_owner_changed(samplestorage):
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    assert samplestorage.save_sample('user', SampleWithID(id_, [TEST_NODE], dt(1), 'foo')) is True

    # this is naughty
    samplestorage._db.aql.execute(
        '''
        FOR s IN @@col
            UPDATE s WITH {acls: MERGE(s.acls, @acls)} IN @@col
            RETURN s
        ''',
        bind_vars={'@col': 'samples', 'acls': {'owner': 'user2'}})

    with raises(Exception) as got:
        samplestorage.replace_sample_acls(id_, SampleACL('user', write=['foo']))
    assert_exception_correct(got.value, OwnerChangedException())
