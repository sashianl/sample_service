import uuid

from SampleService.core.data_link import DataLink
from SampleService.core.sample import (
    SavedSample,
    SampleNode,
    SampleNodeAddress,
    SampleAddress,
)
from SampleService.core.user import UserID
from SampleService.core.workspace import UPA, DataUnitID
from test_support.test_utils import dt

TEST_NODE = SampleNode('foo')

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

