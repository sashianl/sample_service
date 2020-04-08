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
    sid = uuid.UUID('1234567890abcdef1234567890abcdef')

    dl = DataLink(
        uuid.UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('2/3/4')),
        SampleNodeAddress(SampleAddress(sid, 5), 'foo'),
        dt(500)
    )

    assert dl.id == uuid.UUID('1234567890abcdef1234567890abcdee')
    assert dl.duid == DataUnitID(UPA('2/3/4'))
    assert dl.sample_node_address == SampleNodeAddress(SampleAddress(sid, 5), 'foo')
    assert dl.created == dt(500)
    assert dl.expired is None
    assert str(dl) == ('id=12345678-90ab-cdef-1234-567890abcdee ' +
                       'duid=[2/3/4] ' +
                       'sample_node_address=[12345678-90ab-cdef-1234-567890abcdef:5:foo] ' +
                       'created=500.0 expired=None')


def test_init_with_expire1():
    sid = uuid.UUID('1234567890abcdef1234567890abcdef')

    dl = DataLink(
        uuid.UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('2/6/4'), 'whee'),
        SampleNodeAddress(SampleAddress(sid, 7), 'bar'),
        dt(400),
        dt(800)
    )

    assert dl.id == uuid.UUID('1234567890abcdef1234567890abcdee')
    assert dl.duid == DataUnitID(UPA('2/6/4'), 'whee')
    assert dl.sample_node_address == SampleNodeAddress(SampleAddress(sid, 7), 'bar')
    assert dl.created == dt(400)
    assert dl.expired == dt(800)
    assert str(dl) == ('id=12345678-90ab-cdef-1234-567890abcdee ' +
                       'duid=[2/6/4:whee] ' +
                       'sample_node_address=[12345678-90ab-cdef-1234-567890abcdef:7:bar] ' +
                       'created=400.0 expired=800.0')


def test_init_with_expire2():
    sid = uuid.UUID('1234567890abcdef1234567890abcdef')

    dl = DataLink(
        uuid.UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('2/6/4'), 'whee'),
        SampleNodeAddress(SampleAddress(sid, 7), 'bar'),
        dt(400),
        dt(400)
    )

    assert dl.id == uuid.UUID('1234567890abcdef1234567890abcdee')
    assert dl.duid == DataUnitID(UPA('2/6/4'), 'whee')
    assert dl.sample_node_address == SampleNodeAddress(SampleAddress(sid, 7), 'bar')
    assert dl.created == dt(400)
    assert dl.expired == dt(400)
    assert str(dl) == ('id=12345678-90ab-cdef-1234-567890abcdee ' +
                       'duid=[2/6/4:whee] ' +
                       'sample_node_address=[12345678-90ab-cdef-1234-567890abcdef:7:bar] ' +
                       'created=400.0 expired=400.0')


def test_init_fail():
    lid = uuid.UUID('1234567890abcdef1234567890abcdee')
    d = DataUnitID(UPA('1/1/1'))
    sid = uuid.UUID('1234567890abcdef1234567890abcdef')
    s = SampleNodeAddress(SampleAddress(sid, 1), 'a')
    t = dt(1)
    bt = datetime.datetime.now()

    _init_fail(None, d, s, t, t, ValueError('id cannot be a value that evaluates to false'))
    _init_fail(lid, None, s, t, t, ValueError('duid cannot be a value that evaluates to false'))
    _init_fail(lid, d, None, t, t, ValueError(
        'sample_node_address cannot be a value that evaluates to false'))
    _init_fail(lid, d, s, None, t, ValueError('created cannot be a value that evaluates to false'))
    _init_fail(lid, d, s, bt, t, ValueError('created cannot be a naive datetime'))
    _init_fail(lid, d, s, t, bt, ValueError('expired cannot be a naive datetime'))
    _init_fail(lid, d, s, dt(100), dt(99), ValueError('link cannot expire before it is created'))


def _init_fail(lid, duid, sna, cr, ex, expected):
    with raises(Exception) as got:
        DataLink(lid, duid, sna, cr, ex)
    assert_exception_correct(got.value, expected)


def test_expire1():
    sid = uuid.UUID('1234567890abcdef1234567890abcdef')

    dl = DataLink(
        uuid.UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('2/3/4')),
        SampleNodeAddress(SampleAddress(sid, 5), 'foo'),
        dt(500)
    )

    assert dl.expired is None

    dl = dl.expire(dt(800))
    assert dl.id == uuid.UUID('1234567890abcdef1234567890abcdee')
    assert dl.duid == DataUnitID(UPA('2/3/4'))
    assert dl.sample_node_address == SampleNodeAddress(SampleAddress(sid, 5), 'foo')
    assert dl.created == dt(500)
    assert dl.expired == dt(800)


def test_expire2():
    sid = uuid.UUID('1234567890abcdef1234567890abcdef')

    dl = DataLink(
        uuid.UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('2/3/4')),
        SampleNodeAddress(SampleAddress(sid, 5), 'foo'),
        dt(500)
    )

    assert dl.expired is None

    dl = dl.expire(dt(500))
    assert dl.id == uuid.UUID('1234567890abcdef1234567890abcdee')
    assert dl.duid == DataUnitID(UPA('2/3/4'))
    assert dl.sample_node_address == SampleNodeAddress(SampleAddress(sid, 5), 'foo')
    assert dl.created == dt(500)
    assert dl.expired == dt(500)


def test_expire_fail():
    sid = uuid.UUID('1234567890abcdef1234567890abcdef')
    dl = DataLink(
        uuid.UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('2/3/4')),
        SampleNodeAddress(SampleAddress(sid, 5), 'foo'),
        dt(500)
    )
    bt = datetime.datetime.now()

    _expire_fail(dl, None, ValueError('expired cannot be a value that evaluates to false'))
    _expire_fail(dl, bt, ValueError('expired cannot be a naive datetime'))
    _expire_fail(dl, dt(499), ValueError('link cannot expire before it is created'))


def _expire_fail(dl, expired, expected):
    with raises(Exception) as got:
        dl.expire(expired)
    assert_exception_correct(got.value, expected)


def test_equals():
    lid1 = uuid.UUID('1234567890abcdef1234567890abcdee')
    lid1a = uuid.UUID('1234567890abcdef1234567890abcdee')
    lid2 = uuid.UUID('1234567890abcdef1234567890abcdec')
    lid2a = uuid.UUID('1234567890abcdef1234567890abcdec')
    d1 = DataUnitID(UPA('1/1/1'))
    d1a = DataUnitID(UPA('1/1/1'))
    d2 = DataUnitID(UPA('1/1/2'))
    d2a = DataUnitID(UPA('1/1/2'))
    sid = uuid.UUID('1234567890abcdef1234567890abcdef')
    s1 = SampleNodeAddress(SampleAddress(sid, 1), 'foo')
    s1a = SampleNodeAddress(SampleAddress(sid, 1), 'foo')
    s2 = SampleNodeAddress(SampleAddress(sid, 2), 'foo')
    s2a = SampleNodeAddress(SampleAddress(sid, 2), 'foo')
    t1 = dt(500)
    t1a = dt(500)
    t2 = dt(600)
    t2a = dt(600)

    assert DataLink(lid1, d1, s1, t1) == DataLink(lid1a, d1a, s1a, t1a)
    assert DataLink(lid1, d1, s1, t1, None) == DataLink(lid1a, d1a, s1a, t1a, None)
    assert DataLink(lid2, d2, s2, t1, t2) == DataLink(lid2a, d2a, s2a, t1a, t2a)

    assert DataLink(lid1, d1, s1, t1) != (lid1, d1, s1, t1)
    assert DataLink(lid1, d1, s1, t1, t2) != (lid1, d1, s1, t1, t2)

    assert DataLink(lid1, d1, s1, t1) != DataLink(lid2, d1a, s1a, t1a)
    assert DataLink(lid1, d1, s1, t1) != DataLink(lid1a, d2, s1a, t1a)
    assert DataLink(lid1, d1, s1, t1) != DataLink(lid1a, d1a, s2, t1a)
    assert DataLink(lid1, d1, s1, t1) != DataLink(lid1a, d1a, s1a, t2)
    assert DataLink(lid1, d1, s1, t1, t2) != DataLink(lid1a, d1a, s1a, t1a, t1)
    assert DataLink(lid1, d1, s1, t1, t1) != DataLink(lid1a, d1a, s1a, t1a)
    assert DataLink(lid1, d1, s1, t1) != DataLink(lid1a, d1a, s1a, t1a, t1a)


def test_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__
    lid1 = uuid.UUID('1234567890abcdef1234567890abcdee')
    lid1a = uuid.UUID('1234567890abcdef1234567890abcdee')
    lid2 = uuid.UUID('1234567890abcdef1234567890abcdec')
    lid2a = uuid.UUID('1234567890abcdef1234567890abcdec')
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

    assert hash(DataLink(lid1, d1, s1, t1)) == hash(DataLink(lid1a, d1a, s1a, t1a))
    assert hash(DataLink(lid1, d1, s1, t1, None)) == hash(DataLink(lid1a, d1a, s1a, t1a, None))
    assert hash(DataLink(lid2, d2, s2, t1, t2)) == hash(DataLink(lid2a, d2a, s2a, t1a, t2a))

    assert hash(DataLink(lid1, d1, s1, t1)) != hash(DataLink(lid2, d1a, s1a, t1a))
    assert hash(DataLink(lid1, d1, s1, t1)) != hash(DataLink(lid1a, d2, s1a, t1a))
    assert hash(DataLink(lid1, d1, s1, t1)) != hash(DataLink(lid1a, d1a, s2, t1a))
    assert hash(DataLink(lid1, d1, s1, t1)) != hash(DataLink(lid1a, d1a, s1a, t2))
    assert hash(DataLink(lid1, d1, s1, t1, t2)) != hash(DataLink(lid1a, d1a, s1a, t1a, t1))
    assert hash(DataLink(lid1, d1, s1, t1, t1)) != hash(DataLink(lid1a, d1a, s1a, t1a))
    assert hash(DataLink(lid1, d1, s1, t1)) != hash(DataLink(lid1a, d1a, s1a, t1a, t1a))
