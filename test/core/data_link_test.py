import datetime
import uuid

from pytest import raises

from core.test_utils import assert_exception_correct
from SampleService.core.data_link import DataLink
from SampleService.core.sample import SampleNodeAddress, SampleAddress
from SampleService.core.workspace import DataUnitID, UPA


def dt(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)


def test_init_no_expire():
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')

    dl = DataLink(
        DataUnitID(UPA('2/3/4')),
        SampleNodeAddress(SampleAddress(id_, 5), 'foo'),
        dt(500)
    )

    assert dl.duid == DataUnitID(UPA('2/3/4'))
    assert dl.sample_node_address == SampleNodeAddress(SampleAddress(id_, 5), 'foo')
    assert dl.create == dt(500)
    assert dl.expire is None
    assert str(dl) == ('duid=[2/3/4] ' +
                       'sample_node_address=[12345678-90ab-cdef-1234-567890abcdef:5:foo] ' +
                       'create=500.0 expire=None')


def test_init_with_expire():
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')

    dl = DataLink(
        DataUnitID(UPA('2/6/4'), 'whee'),
        SampleNodeAddress(SampleAddress(id_, 7), 'bar'),
        dt(400),
        dt(800)
    )

    assert dl.duid == DataUnitID(UPA('2/6/4'), 'whee')
    assert dl.sample_node_address == SampleNodeAddress(SampleAddress(id_, 7), 'bar')
    assert dl.create == dt(400)
    assert dl.expire == dt(800)
    assert str(dl) == ('duid=[2/6/4:whee] ' +
                       'sample_node_address=[12345678-90ab-cdef-1234-567890abcdef:7:bar] ' +
                       'create=400.0 expire=800.0')


def test_init_fail():
    d = DataUnitID(UPA('1/1/1'))
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    s = SampleNodeAddress(SampleAddress(id_, 1), 'a')
    t = dt(1)
    bt = datetime.datetime.now()

    _init_fail(None, s, t, t, ValueError('duid cannot be a value that evaluates to false'))
    _init_fail(d, None, t, t, ValueError(
        'sample_node_address cannot be a value that evaluates to false'))
    _init_fail(d, s, None, t, ValueError('create cannot be a value that evaluates to false'))
    _init_fail(d, s, bt, t, ValueError('create cannot be a naive datetime'))
    _init_fail(d, s, t, bt, ValueError('expire cannot be a naive datetime'))


def _init_fail(duid, sna, cr, ex, expected):
    with raises(Exception) as got:
        DataLink(duid, sna, cr, ex)
    assert_exception_correct(got.value, expected)


def test_equals():
    d1 = DataUnitID(UPA('1/1/1'))
    d1a = DataUnitID(UPA('1/1/1'))
    d2 = DataUnitID(UPA('1/1/2'))
    d2a = DataUnitID(UPA('1/1/2'))
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    s1 = SampleNodeAddress(SampleAddress(id_, 1), 'foo')
    s1a = SampleNodeAddress(SampleAddress(id_, 1), 'foo')
    s2 = SampleNodeAddress(SampleAddress(id_, 2), 'foo')
    s2a = SampleNodeAddress(SampleAddress(id_, 2), 'foo')
    t1 = dt(500)
    t1a = dt(500)
    t2 = dt(600)
    t2a = dt(600)

    assert DataLink(d1, s1, t1) == DataLink(d1a, s1a, t1a)
    assert DataLink(d1, s1, t1, None) == DataLink(d1a, s1a, t1a, None)
    assert DataLink(d2, s2, t1, t2) == DataLink(d2a, s2a, t1a, t2a)

    assert DataLink(d1, s1, t1) != (d1, s1, t1)
    assert DataLink(d1, s1, t1, t2) != (d1, s1, t1, t2)

    assert DataLink(d1, s1, t1) != DataLink(d2, s1a, t1a)
    assert DataLink(d1, s1, t1) != DataLink(d1a, s2, t1a)
    assert DataLink(d1, s1, t1) != DataLink(d1a, s1a, t2)
    assert DataLink(d1, s1, t1, t2) != DataLink(d1a, s1a, t1a, t1)
    assert DataLink(d1, s1, t1, t1) != DataLink(d1a, s1a, t1a)
    assert DataLink(d1, s1, t1) != DataLink(d1a, s1a, t1a, t1a)


def test_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__
    d1 = DataUnitID(UPA('1/1/1'))
    d1a = DataUnitID(UPA('1/1/1'))
    d2 = DataUnitID(UPA('1/1/2'))
    d2a = DataUnitID(UPA('1/1/2'))
    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    s1 = SampleNodeAddress(SampleAddress(id_, 1), 'foo')
    s1a = SampleNodeAddress(SampleAddress(id_, 1), 'foo')
    s2 = SampleNodeAddress(SampleAddress(id_, 2), 'foo')
    s2a = SampleNodeAddress(SampleAddress(id_, 2), 'foo')
    t1 = dt(500)
    t1a = dt(500)
    t2 = dt(600)
    t2a = dt(600)

    assert hash(DataLink(d1, s1, t1)) == hash(DataLink(d1a, s1a, t1a))
    assert hash(DataLink(d1, s1, t1, None)) == hash(DataLink(d1a, s1a, t1a, None))
    assert hash(DataLink(d2, s2, t1, t2)) == hash(DataLink(d2a, s2a, t1a, t2a))

    assert hash(DataLink(d1, s1, t1)) != hash(DataLink(d2, s1a, t1a))
    assert hash(DataLink(d1, s1, t1)) != hash(DataLink(d1a, s2, t1a))
    assert hash(DataLink(d1, s1, t1)) != hash(DataLink(d1a, s1a, t2))
    assert hash(DataLink(d1, s1, t1, t2)) != hash(DataLink(d1a, s1a, t1a, t1))
    assert hash(DataLink(d1, s1, t1, t1)) != hash(DataLink(d1a, s1a, t1a))
    assert hash(DataLink(d1, s1, t1)) != hash(DataLink(d1a, s1a, t1a, t1a))
