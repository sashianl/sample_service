import uuid
from pytest import raises
from core.test_utils import assert_exception_correct
from SampleService.core.sample import Sample, SampleWithID, SampleNode, SubSampleType
from SampleService.core.errors import IllegalParameterError, MissingParameterError


def test_sample_node_build():
    sn = SampleNode('foo', SubSampleType.BIOLOGICAL_REPLICATE)
    assert sn.name == 'foo'
    assert sn.type == SubSampleType.BIOLOGICAL_REPLICATE
    assert sn.parent is None

    sn = SampleNode('a' * 255, SubSampleType.TECHNICAL_REPLICATE, 'b' * 255)
    assert sn.name == 'a' * 255
    assert sn.type == SubSampleType.TECHNICAL_REPLICATE
    assert sn.parent == 'b' * 255


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
    s = SampleWithID(id_, [sn])
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.nodes == (sndup,)
    assert s.name is None
    assert s.version is None

    s = SampleWithID(id_, [sn], 'foo')
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.nodes == (sndup,)
    assert s.name == 'foo'
    assert s.version is None

    s = SampleWithID(id_, [sn], 'foo', 1)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.nodes == (sndup,)
    assert s.name == 'foo'
    assert s.version == 1

    s = SampleWithID(id_, [sn], 'foo', 8)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.nodes == (sndup,)
    assert s.name == 'foo'
    assert s.version == 8

    s = SampleWithID(id_, [sn], version=8)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.nodes == (sndup,)
    assert s.name is None
    assert s.version == 8


def test_sample_build_fail():
    # not testing every permutation of failing check_string here, just one test to make sure
    # it's there

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    sn = SampleNode('foo')
    tn = SampleNode('bar', SubSampleType.TECHNICAL_REPLICATE, 'foo')
    sn2 = SampleNode('baz')
    dup = SampleNode('foo')

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

    _sample_with_id_build_fail(None, [sn], None, None,
                               ValueError('id_ cannot be a value that evaluates to false'))
    _sample_with_id_build_fail(id_, [sn], None, 0, ValueError('version must be > 0'))


def _sample_build_fail(nodes, name, expected):
    with raises(Exception) as got:
        Sample(nodes, name)
    assert_exception_correct(got.value, expected)

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    with raises(Exception) as got:
        SampleWithID(id_, nodes, name)
    assert_exception_correct(got.value, expected)


def _sample_with_id_build_fail(id_, nodes, name, version, expected):
    with raises(Exception) as got:
        SampleWithID(id_, nodes, name, version)
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

    assert SampleWithID(id1, [sn]) == SampleWithID(id1, [sn])
    assert SampleWithID(id1, [sn]) != SampleWithID(id2, [sn])
    assert SampleWithID(id1, [sn]) != SampleWithID(id1, [sn2])

    assert SampleWithID(id1, [sn], 'yay') == SampleWithID(id1, [sn], 'yay')
    assert SampleWithID(id1, [sn], 'yay') != SampleWithID(id1, [sn], 'yooo')

    assert SampleWithID(id1, [sn], 'yay', 6) == SampleWithID(id1, [sn], 'yay', 6)
    assert SampleWithID(id1, [sn], 'yay', 6) != SampleWithID(id1, [sn], 'yay', 7)

    assert SampleWithID(id1, [sn], 'yay') != Sample([sn], 'yay')
    assert Sample([sn], 'yay') != SampleWithID(id1, [sn], 'yay')


def test_sample_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__

    sn = SampleNode('foo')
    sn2 = SampleNode('bar')
    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdea')

    assert hash(Sample([sn], 'yay')) == hash(Sample([sn], 'yay'))
    assert hash(Sample([sn], 'foo')) == hash(Sample([sn], 'foo'))
    assert hash(Sample([sn], 'yay')) != hash(Sample([sn2], 'yay'))
    assert hash(Sample([sn], 'yay')) != hash(Sample([sn], 'yo'))

    assert hash(SampleWithID(id1, [sn], 'yay')) == hash(SampleWithID(id1, [sn], 'yay'))
    assert hash(SampleWithID(id2, [sn], 'foo')) == hash(SampleWithID(id2, [sn], 'foo'))
    assert hash(SampleWithID(id1, [sn], 'foo')) != hash(SampleWithID(id2, [sn], 'foo'))
    assert hash(SampleWithID(id2, [sn], 'foo')) != hash(SampleWithID(id2, [sn2], 'foo'))
    assert hash(SampleWithID(id2, [sn], 'foo')) != hash(SampleWithID(id2, [sn], 'bar'))
    assert hash(SampleWithID(id1, [sn], 'foo', 6)) == hash(SampleWithID(id1, [sn], 'foo', 6))
    assert hash(SampleWithID(id1, [sn], 'foo', 6)) != hash(SampleWithID(id1, [sn], 'foo', 7))
