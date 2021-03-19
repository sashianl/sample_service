import datetime
import uuid
import time

from pytest import raises, fixture
from core import test_utils
from core.test_utils import assert_exception_correct
from arango_controller import ArangoController
from SampleService.core.acls import SampleACL, SampleACLDelta
from SampleService.core.data_link import DataLink
from SampleService.core.sample import (
    SavedSample,
    SampleNode,
    SubSampleType,
    SampleNodeAddress,
    SampleAddress,
    SourceMetadata,
)
from SampleService.core.errors import (
    MissingParameterError, NoSuchSampleError, ConcurrencyError, UnauthorizedError,
    NoSuchSampleVersionError, DataLinkExistsError, TooManyDataLinksError, NoSuchLinkError,
    NoSuchSampleNodeError
)
from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage
from SampleService.core.storage.errors import SampleStorageError, StorageInitError
from SampleService.core.storage.errors import OwnerChangedError
from SampleService.core.user import UserID
from SampleService.core.workspace import UPA, DataUnitID

TEST_NODE = SampleNode('foo')

TEST_DB_NAME = 'test_sample_service'
TEST_COL_SAMPLE = 'samples'
TEST_COL_VERSION = 'samples_version'
TEST_COL_VER_EDGE = 'ver_to_sample'
TEST_COL_NODES = 'samples_nodes'
TEST_COL_NODE_EDGE = 'node_edges'
TEST_COL_WS_OBJ_VER = 'ws_obj_ver'
TEST_COL_DATA_LINK = 'samples_data_link'
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
    db.create_collection(TEST_COL_WS_OBJ_VER)
    db.create_collection(TEST_COL_DATA_LINK, edge=True)
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
        TEST_COL_WS_OBJ_VER,
        TEST_COL_DATA_LINK,
        TEST_COL_SCHEMA)

def dt(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)

def _create_and_expire_data_link(samplestorage, link, expired, user):
    samplestorage.create_data_link(link)
    samplestorage.expire_data_link(expired, user, link.id)

def test_timestamp_seconds_to_milliseconds(samplestorage):
    ts1=1614958000000 # milliseconds
    ts2=1614958000    # seconds
    ts3=1614958       # seconds
    ts4=9007199254740.991 # seconds

    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdee')
    assert samplestorage.save_sample(
        SavedSample(id1, UserID('user'), [SampleNode('mynode')], dt(ts3), 'foo')) is True
    assert samplestorage.save_sample_version(
        SavedSample(id1, UserID('user'), [SampleNode('mynode1')], dt(ts2), 'foo')) == 2
    assert samplestorage.save_sample(
        SavedSample(id2, UserID('user'), [SampleNode('mynode2')], dt(ts3), 'foo')) is True

    lid1=uuid.UUID('1234567890abcdef1234567890abcde2')
    lid2=uuid.UUID('1234567890abcdef1234567890abcde3')
    lid3=uuid.UUID('1234567890abcdef1234567890abcde4')
    samplestorage.create_data_link(DataLink(
        lid1,
        DataUnitID(UPA('42/42/42'), 'dataunit1'),
        SampleNodeAddress(SampleAddress(id1, 1), 'mynode'),
        dt(ts2),
        UserID('user'))
    )

    samplestorage.create_data_link(DataLink(
        lid2,
        DataUnitID(UPA('5/89/32'), 'dataunit2'),
        SampleNodeAddress(SampleAddress(id2, 1), 'mynode2'),
        dt(ts3),
        UserID('user'))
    )

    _create_and_expire_data_link(
        samplestorage,
        DataLink(
            lid3,
            DataUnitID(UPA('5/89/33'), 'dataunit1'),
            SampleNodeAddress(SampleAddress(id1, 1), 'mynode'),
            dt(ts3),
            UserID('user')),
        dt(ts3+100),
        UserID('user')
    )

    assert samplestorage.get_sample(id1, 1).savetime == dt(ts3)
    assert samplestorage.get_sample(id1, 2).savetime == dt(ts2)
    assert samplestorage.get_sample(id2).savetime == dt(ts3)
    assert samplestorage.get_data_link(lid1).created == dt(ts2)
    assert samplestorage.get_data_link(lid2).created == dt(ts3)
    assert samplestorage.get_data_link(lid3).created == dt(ts3)
    assert samplestorage.get_data_link(lid3).expired == dt(ts3+100)

    threshold=1000000000000 # current timestamp in milliseconds is above 1600000000000
    query="""
        FOR sample1 IN samples_nodes
            FILTER sample1.saved < @threshold
            UPDATE sample1 WITH { saved: ROUND(sample1.saved * 1000) } IN samples_nodes
        FOR sample2 IN samples_version
            FILTER sample2.saved < @threshold
            UPDATE sample2 WITH { saved: ROUND(sample2.saved * 1000) } IN samples_version
        FOR link IN samples_data_link
            FILTER link.expired < @threshold OR link.created < @threshold
            UPDATE link WITH { 
                expired: link.expired < @threshold ? ROUND(link.expired * 1000) : link.expired,
                created: link.created < @threshold ? ROUND(link.created * 1000) : link.created
            } IN samples_data_link
        """

    samplestorage._db.aql.execute(query, bind_vars={'threshold': threshold})

    assert samplestorage.get_sample(id1, 1).savetime == dt(ts2)
    assert samplestorage.get_sample(id1, 2).savetime == dt(ts2)
    assert samplestorage.get_sample(id2).savetime == dt(ts2)
    assert samplestorage.get_data_link(lid1).created == dt(ts2)
    assert samplestorage.get_data_link(lid2).created == dt(ts2)
    assert samplestorage.get_data_link(lid3).created == dt(ts2)
    assert samplestorage.get_data_link(lid3).expired == dt((ts3+100) * 1000)

    samplestorage._db.aql.execute(query, bind_vars={'threshold': threshold})

    assert samplestorage.get_sample(id1, 1).savetime == dt(ts2)
    assert samplestorage.get_sample(id1, 2).savetime == dt(ts2)
    assert samplestorage.get_sample(id2).savetime == dt(ts2)
    assert samplestorage.get_data_link(lid1).created == dt(ts2)
    assert samplestorage.get_data_link(lid2).created == dt(ts2)
    assert samplestorage.get_data_link(lid3).created == dt(ts2)
    assert samplestorage.get_data_link(lid3).expired == dt((ts3+100) * 1000)

