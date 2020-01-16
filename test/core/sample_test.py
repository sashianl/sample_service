import datetime
import uuid
from pytest import raises
from core.test_utils import assert_exception_correct
from SampleService.core.sample import Sample, SavedSample, SampleNode, SubSampleType
from SampleService.core.errors import IllegalParameterError, MissingParameterError


def test_sample_node_build():
    sn = SampleNode('foo', SubSampleType.BIOLOGICAL_REPLICATE)
    assert sn.name == 'foo'
    assert sn.type == SubSampleType.BIOLOGICAL_REPLICATE
    assert sn.parent is None
    assert sn.controlled_metadata == {}
    assert sn.uncontrolled_metadata == {}

    sn = SampleNode('a' * 255, SubSampleType.TECHNICAL_REPLICATE, 'b' * 255)
    assert sn.name == 'a' * 255
    assert sn.type == SubSampleType.TECHNICAL_REPLICATE
    assert sn.parent == 'b' * 255
    assert sn.controlled_metadata == {}
    assert sn.uncontrolled_metadata == {}

    sn = SampleNode('a' * 255, SubSampleType.TECHNICAL_REPLICATE, 'b' * 255,
                    {'foo': {'bar': 'baz', 'bat': 'whee'}, 'wugga': {'a': 'b'}},
                    {'a': {'b': 'foo'}})
    assert sn.name == 'a' * 255
    assert sn.type == SubSampleType.TECHNICAL_REPLICATE
    assert sn.parent == 'b' * 255
    assert sn.controlled_metadata == {'foo': {'bar': 'baz', 'bat': 'whee'}, 'wugga': {'a': 'b'}}
    assert sn.uncontrolled_metadata == {'a': {'b': 'foo'}}


def test_sample_node_build_fail():
    # not testing every permutation of failing check_string here, just one test to make sure
    # it's there
    _sample_node_build_fail('', SubSampleType.BIOLOGICAL_REPLICATE, None,
                            MissingParameterError('subsample name'))
    _sample_node_build_fail('a' * 256, SubSampleType.BIOLOGICAL_REPLICATE, None,
                            IllegalParameterError('subsample name exceeds maximum length of 255'))
    _sample_node_build_fail('a', None, None,
                            ValueError('type cannot be a value that evaluates to false'))
    _sample_node_build_fail('a', SubSampleType.TECHNICAL_REPLICATE, 'b' * 256,
                            IllegalParameterError('parent exceeds maximum length of 255'))
    _sample_node_build_fail(
        'a', SubSampleType.BIOLOGICAL_REPLICATE, 'badparent', IllegalParameterError(
            'Node a is of type BioReplicate and therefore cannot have a parent'))
    _sample_node_build_fail(
        'a', SubSampleType.TECHNICAL_REPLICATE, None, IllegalParameterError(
            'Node a is of type TechReplicate and therefore must have a parent'))
    _sample_node_build_fail(
        'a', SubSampleType.SUB_SAMPLE, None, IllegalParameterError(
            'Node a is of type SubSample and therefore must have a parent'))


def _sample_node_build_fail(name, type_, parent, expected):
    with raises(Exception) as got:
        SampleNode(name, type_, parent)
    assert_exception_correct(got.value, expected)


def test_sample_node_eq():
    t = SubSampleType.TECHNICAL_REPLICATE
    s = SubSampleType.SUB_SAMPLE
    r = SubSampleType.BIOLOGICAL_REPLICATE

    assert SampleNode('foo') == SampleNode('foo')
    assert SampleNode('foo') != SampleNode('bar')

    assert SampleNode('foo', r) == SampleNode('foo', r)
    assert SampleNode('foo', r) != SampleNode('foo', s, 'baz')

    assert SampleNode('foo', s, 'bar') == SampleNode('foo', s, 'bar')
    assert SampleNode('foo', s, 'bar') != SampleNode('foo', t, 'bar')
    assert SampleNode('foo', s, 'bar') != SampleNode('foo', s, 'bat')

    assert SampleNode('foo', s, 'bar', {'foo': {'a': 'b'}}) == SampleNode(
        'foo', s, 'bar', {'foo': {'a': 'b'}})
    assert SampleNode('foo', s, 'bar', {'foo': {'a': 'b'}}) != SampleNode(
        'foo', s, 'bar', {'foo': {'a': 'c'}})
    assert SampleNode('foo', s, 'bar', {'foo': {'a': 'b'}}) != SampleNode(
        'foo', s, 'bar', {'foo': {'z': 'b'}})
    assert SampleNode('foo', s, 'bar', {'foo': {'a': 'b'}}) != SampleNode(
        'foo', s, 'bar', {'fo': {'a': 'b'}})

    assert SampleNode('foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'b'}}) == SampleNode(
        'foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'b'}})
    assert SampleNode('foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'b'}}) != SampleNode(
        'foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'c'}})
    assert SampleNode('foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'b'}}) != SampleNode(
        'foo', s, 'bar', uncontrolled_metadata={'foo': {'z': 'b'}})
    assert SampleNode('foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'b'}}) != SampleNode(
        'foo', s, 'bar', uncontrolled_metadata={'fo': {'a': 'b'}})

    assert SampleNode('foo') != 'foo'
    assert 'foo' != SampleNode('foo')


def test_sample_node_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__

    t = SubSampleType.TECHNICAL_REPLICATE
    s = SubSampleType.SUB_SAMPLE
    r = SubSampleType.BIOLOGICAL_REPLICATE

    assert hash(SampleNode('foo')) == hash(SampleNode('foo'))
    assert hash(SampleNode('bar')) == hash(SampleNode('bar'))
    assert hash(SampleNode('foo')) != hash(SampleNode('bar'))
    assert hash(SampleNode('foo', r)) == hash(SampleNode('foo', r))
    assert hash(SampleNode('foo', r)) != hash(SampleNode('foo', s, 'baz'))
    assert hash(SampleNode('foo', s, 'bar')) == hash(SampleNode('foo', s, 'bar'))
    assert hash(SampleNode('foo', t, 'bat')) == hash(SampleNode('foo', t, 'bat'))
    assert hash(SampleNode('foo', s, 'bar')) != hash(SampleNode('foo', t, 'bar'))
    assert hash(SampleNode('foo', s, 'bar')) != hash(SampleNode('foo', s, 'bat'))

    assert hash(SampleNode('foo', s, 'bar', {'foo': {'a': 'b'}})) == hash(SampleNode(
        'foo', s, 'bar', {'foo': {'a': 'b'}}))
    assert hash(SampleNode('foo', s, 'bar', {'foo': {'a': 'b'}})) != hash(SampleNode(
        'foo', s, 'bar', {'foo': {'a': 'c'}}))
    assert hash(SampleNode('foo', s, 'bar', {'foo': {'a': 'b'}})) != hash(SampleNode(
        'foo', s, 'bar', {'foo': {'z': 'b'}}))
    assert hash(SampleNode('foo', s, 'bar', {'foo': {'a': 'b'}})) != hash(SampleNode(
        'foo', s, 'bar', {'fo': {'a': 'b'}}))

    assert hash(SampleNode('foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'b'}})) == hash(
        SampleNode('foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'b'}}))
    assert hash(SampleNode('foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'b'}})) != hash(
        SampleNode('foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'c'}}))
    assert hash(SampleNode('foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'b'}})) != hash(
        SampleNode('foo', s, 'bar', uncontrolled_metadata={'foo': {'z': 'b'}}))
    assert hash(SampleNode('foo', s, 'bar', uncontrolled_metadata={'foo': {'a': 'b'}})) != hash(
        SampleNode('foo', s, 'bar', uncontrolled_metadata={'fo': {'a': 'b'}}))


def dt(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)


def test_sample_build():
    sn = SampleNode('foo')
    sn2 = SampleNode('bat')
    sn3 = SampleNode('bar', type_=SubSampleType.TECHNICAL_REPLICATE, parent='foo')
    sn4 = SampleNode('baz', type_=SubSampleType.SUB_SAMPLE, parent='foo')
    sndup = SampleNode('foo')

    s = Sample([sn])
    assert s.nodes == (sndup,)
    assert s.name is None

    s = Sample([sn, sn2, sn4, sn3], '   \t   foo    ')
    assert s.nodes == (sndup, sn2, sn4, sn3)
    assert s.name == 'foo'

    s = Sample([sn], 'a' * 255)
    assert s.nodes == (sndup,)
    assert s.name == 'a' * 255

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    s = SavedSample(id_, 'user', [sn], dt(6))
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == 'user'
    assert s.nodes == (sndup,)
    assert s.savetime == dt(6)
    assert s.name is None
    assert s.version is None

    s = SavedSample(id_, 'user2', [sn], dt(6), 'foo')
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == 'user2'
    assert s.nodes == (sndup,)
    assert s.savetime == dt(6)
    assert s.name == 'foo'
    assert s.version is None

    s = SavedSample(id_, 'user', [sn], dt(6), 'foo', 1)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == 'user'
    assert s.nodes == (sndup,)
    assert s.savetime == dt(6)
    assert s.name == 'foo'
    assert s.version == 1

    s = SavedSample(id_, 'user', [sn], dt(6), 'foo', 8)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == 'user'
    assert s.nodes == (sndup,)
    assert s.savetime == dt(6)
    assert s.name == 'foo'
    assert s.version == 8

    s = SavedSample(id_, 'user', [sn], dt(6), version=8)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == 'user'
    assert s.nodes == (sndup,)
    assert s.savetime == dt(6)
    assert s.name is None
    assert s.version == 8


def test_sample_build_fail():
    # not testing every permutation of failing check_string here, just one test to make sure
    # it's there

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    u = 'user'
    sn = SampleNode('foo')
    tn = SampleNode('bar', SubSampleType.TECHNICAL_REPLICATE, 'foo')
    sn2 = SampleNode('baz')
    dup = SampleNode('foo')
    d = dt(8)

    _sample_build_fail(
        [sn], 'a' * 256, IllegalParameterError('name exceeds maximum length of 255'))
    _sample_build_fail([], None, MissingParameterError('At least one node per sample is required'))
    _sample_build_fail(
        [tn, sn], 'a', IllegalParameterError('The first node in a sample must be a BioReplicate'))
    _sample_build_fail([sn, tn, sn2], 'a', IllegalParameterError(
                       'BioReplicates must be the first nodes in the list of sample nodes.'))
    _sample_build_fail([sn, sn2, dup], 'a', IllegalParameterError(
                       'Duplicate sample node name: foo'))
    _sample_build_fail([sn2, tn], 'a', IllegalParameterError(
                        'Parent foo of node bar does not appear in node list prior to node.'))

    _sample_with_id_build_fail(None, u, [sn], d, None, None,
                               ValueError('id_ cannot be a value that evaluates to false'))
    _sample_with_id_build_fail(id_, '', [sn], d, None, None,
                               ValueError('user cannot be a value that evaluates to false'))
    _sample_with_id_build_fail(id_, u, [sn], None, None, None, ValueError(
                               'savetime cannot be a value that evaluates to false'))
    _sample_with_id_build_fail(id_, u, [sn], datetime.datetime.now(), None, None, ValueError(
                               'savetime cannot be a naive datetime'))
    _sample_with_id_build_fail(id_, u, [sn], d, None, 0, ValueError('version must be > 0'))


def test_sample_build_fail_sample_count():
    nodes = [SampleNode('s' + str(i)) for i in range(10000)]

    s = Sample(nodes)
    assert s.nodes == tuple(nodes)
    assert s.name is None

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    s = SavedSample(id_, 'u', nodes, dt(8))
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == 'u'
    assert s.nodes == tuple(nodes)
    assert s.savetime == dt(8)
    assert s.name is None
    assert s.version is None

    nodes.append(SampleNode('s10000'))
    _sample_build_fail(nodes, None, IllegalParameterError(
                       'At most 10000 nodes are allowed per sample'))


def _sample_build_fail(nodes, name, expected):
    with raises(Exception) as got:
        Sample(nodes, name)
    assert_exception_correct(got.value, expected)

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    with raises(Exception) as got:
        SavedSample(id_, 'u', nodes, dt(8), name)
    assert_exception_correct(got.value, expected)


def _sample_with_id_build_fail(id_, user, nodes, savetime, name, version, expected):
    with raises(Exception) as got:
        SavedSample(id_, user, nodes, savetime, name, version)
        Sample(nodes, name)
    assert_exception_correct(got.value, expected)


def test_sample_eq():
    sn = SampleNode('foo')
    sn2 = SampleNode('bar')

    assert Sample([sn], 'yay') == Sample([sn], 'yay')
    assert Sample([sn], 'yay') != Sample([sn2], 'yay')
    assert Sample([sn], 'yay') != Sample([sn], 'yooo')

    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdea')
    dt1 = dt(5)
    dt2 = dt(8)

    assert SavedSample(id1, 'u', [sn], dt1) == SavedSample(id1, 'u', [sn], dt(5))
    assert SavedSample(id1, 'u', [sn], dt1) != SavedSample(id2, 'u', [sn], dt1)
    assert SavedSample(id1, 'u', [sn], dt1) != SavedSample(id1, 'u2', [sn], dt1)
    assert SavedSample(id1, 'u', [sn], dt1) != SavedSample(id1, 'u', [sn2], dt1)
    assert SavedSample(id1, 'u', [sn], dt1) != SavedSample(id1, 'u', [sn], dt2)

    assert SavedSample(id1, 'u', [sn], dt1, 'yay') == SavedSample(id1, 'u', [sn], dt1, 'yay')
    assert SavedSample(id1, 'u', [sn], dt1, 'yay') != SavedSample(id1, 'u', [sn], dt1, 'yooo')

    assert SavedSample(id1, 'u', [sn], dt2, 'yay', 6) == SavedSample(id1, 'u', [sn], dt2, 'yay', 6)
    assert SavedSample(id1, 'u', [sn], dt1, 'yay', 6) != SavedSample(id1, 'u', [sn], dt1, 'yay', 7)

    assert SavedSample(id1, 'u', [sn], dt1, 'yay') != Sample([sn], 'yay')
    assert Sample([sn], 'yay') != SavedSample(id1, 'u', [sn], dt1, 'yay')


def test_sample_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__

    sn = SampleNode('foo')
    sn2 = SampleNode('bar')
    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdea')
    dt1 = dt(5)
    dt2 = dt(8)

    assert hash(Sample([sn], 'yay')) == hash(Sample([sn], 'yay'))
    assert hash(Sample([sn], 'foo')) == hash(Sample([sn], 'foo'))
    assert hash(Sample([sn], 'yay')) != hash(Sample([sn2], 'yay'))
    assert hash(Sample([sn], 'yay')) != hash(Sample([sn], 'yo'))

    assert hash(SavedSample(id1, 'u', [sn], dt1, 'yay')) == hash(SavedSample(
                                                                 id1, 'u', [sn], dt(5), 'yay'))
    assert hash(SavedSample(id2, 'u', [sn], dt1, 'foo')) == hash(SavedSample(
                                                                 id2, 'u', [sn], dt1, 'foo'))
    assert hash(SavedSample(id1, 'u', [sn], dt1, 'foo')) != hash(SavedSample(
                                                                 id2, 'u', [sn], dt1, 'foo'))
    assert hash(SavedSample(id1, 'u', [sn], dt1, 'foo')) != hash(SavedSample(
                                                                 id1, 'u2', [sn], dt1, 'foo'))
    assert hash(SavedSample(id2, 'u', [sn], dt1, 'foo')) != hash(SavedSample(
                                                                 id2, 'u', [sn2], dt1, 'foo'))
    assert hash(SavedSample(id2, 'u', [sn], dt1, 'foo')) != hash(SavedSample(
                                                                 id2, 'u', [sn], dt2, 'foo'))
    assert hash(SavedSample(id2, 'u', [sn], dt1, 'foo')) != hash(SavedSample(
                                                                 id2, 'u', [sn], dt1, 'bar'))
    assert hash(SavedSample(id1, 'u', [sn], dt1, 'foo', 6)) == hash(SavedSample(
                                                                    id1, 'u', [sn], dt1, 'foo', 6))
    assert hash(SavedSample(id1, 'u', [sn], dt1, 'foo', 6)) != hash(SavedSample(
                                                                    id1, 'u', [sn], dt1, 'foo', 7))
