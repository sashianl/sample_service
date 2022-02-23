import datetime
import uuid
from pytest import raises
from core.test_utils import assert_exception_correct
from SampleService.core.sample import (
    Sample,
    SavedSample,
    SampleNode,
    SubSampleType,
    SampleAddress,
    SourceMetadata,
)
from SampleService.core.sample import SampleNodeAddress
from SampleService.core.errors import IllegalParameterError, MissingParameterError
from SampleService.core.user import UserID


def test_source_metadata_build():
    sm = SourceMetadata('k' * 256, 'f' * 256, {
        'foo': 1,
        'a' * 256: 'whee\twhoo',
        'k': 'b\n' + 'b' * 1022,
        'f': 1.4,
        'g': True})

    assert sm.key == 'k' * 256
    assert sm.sourcekey == 'f' * 256
    assert sm.sourcevalue == {
        'foo': 1,
        'a' * 256: 'whee\twhoo',
        'k': 'b\n' + 'b' * 1022,
        'f': 1.4,
        'g': True}

    sm = SourceMetadata('k', 'f', {'x': 'y'})

    assert sm.key == 'k'
    assert sm.sourcekey == 'f'
    assert sm.sourcevalue == {'x': 'y'}


def test_source_metadata_build_fail():
    _source_metadata_build_fail(None, 's', {}, IllegalParameterError(
        'Controlled metadata keys may not be null or whitespace only'))
    _source_metadata_build_fail('   \n  \t ', 's', {}, IllegalParameterError(
        'Controlled metadata keys may not be null or whitespace only'))
    _source_metadata_build_fail(
        'z' * 255 + 'ff', 'skey', {},
        IllegalParameterError(f"Controlled metadata has key starting with {'z' * 255 + 'f'} " +
                              'that exceeds maximum length of 256'))
    _source_metadata_build_fail(
        '\twhee', 'skey', {},
        IllegalParameterError(
            "Controlled metadata key \twhee's character at index 0 is a control character."))

    _source_metadata_build_fail('k', None, {}, IllegalParameterError(
        'Source metadata keys may not be null or whitespace only'))
    _source_metadata_build_fail('k', '   \n  \t ', {}, IllegalParameterError(
        'Source metadata keys may not be null or whitespace only'))
    _source_metadata_build_fail(
        'k', 'b' * 255 + 'ff', {},
        IllegalParameterError(f"Source metadata has key starting with {'b' * 255 + 'f'} " +
                              'that exceeds maximum length of 256'))
    _source_metadata_build_fail(
        'k', 'thingy\n', {},
        IllegalParameterError(
            "Source metadata key thingy\n's character at index 6 is a control character."))

    _source_metadata_build_fail('k', 'sk', None, IllegalParameterError(
        'Source metadata value associated with metadata key k is null or empty'))
    _source_metadata_build_fail('k', 'sk', {}, IllegalParameterError(
        'Source metadata value associated with metadata key k is null or empty'))
    _source_metadata_build_fail('k', 'skey', {'a' * 255 + 'ff': 'whee'}, IllegalParameterError(
        'Source metadata has a value key associated with metadata key k starting with ' +
        f"{'a' * 255 + 'f'} that exceeds maximum length of 256"))
    _source_metadata_build_fail('k2', 'skey', {'\twhee': {}}, IllegalParameterError(
        'Source metadata value key \twhee associated with metadata key k2 has a character at ' +
        'index 0 that is a control character.'))
    _source_metadata_build_fail(
        'somekey', 'skey', {'whee': 'a' * 255 + 'f' * 770},
        IllegalParameterError(
            'Source metadata has a value associated with metadata key somekey and value key ' +
            f"whee starting with {'a' * 255 + 'f'} that exceeds maximum length of 1024"))
    _source_metadata_build_fail('k3', 'skey', {'whee': 'whoop\bbutt'}, IllegalParameterError(
        'Source metadata value associated with metadata key k3 and value key whee has a ' +
        'character at index 5 that is a control character.'))


def _source_metadata_build_fail(key, skey, value, expected):
    with raises(Exception) as got:
        SourceMetadata(key, skey, value)
    assert_exception_correct(got.value, expected)


def test_source_metadata_eq():

    assert SourceMetadata('k', 'f', {'x': 'y'}) == SourceMetadata('k', 'f', {'x': 'y'})
    assert SourceMetadata('k', 'f', {'x': 'y'}) != SourceMetadata('k1', 'f', {'x': 'y'})
    assert SourceMetadata('k', 'f', {'x': 'y'}) != SourceMetadata('k', 'f1', {'x': 'y'})
    assert SourceMetadata('k', 'f', {'a': 'b'}) != SourceMetadata('k', 'f', {'a': 'c'})

    assert SourceMetadata('k', 'f', {'x': 'y'}) != 'k'
    assert {} != SourceMetadata('k', 'f', {'x': 'y'})


def test_source_metadata_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__

    assert hash(SourceMetadata('k', 'f', {'x': 'y'})) == hash(SourceMetadata('k', 'f', {'x': 'y'}))
    assert hash(SourceMetadata('k', 'f', {'x': 'y'})) != hash(SourceMetadata('k1', 'f', {'x': 'y'}))
    assert hash(SourceMetadata('k', 'f', {'x': 'y'})) != hash(SourceMetadata('k', 'f1', {'x': 'y'}))
    assert hash(SourceMetadata('k', 'f', {'a': 'b'})) != hash(SourceMetadata('k', 'f', {'a': 'c'}))


def test_sample_node_build():
    sn = SampleNode('foo', SubSampleType.BIOLOGICAL_REPLICATE)
    assert sn.name == 'foo'
    assert sn.type == SubSampleType.BIOLOGICAL_REPLICATE
    assert sn.parent is None
    assert sn.controlled_metadata == {}
    assert sn.user_metadata == {}
    assert sn.source_metadata == ()

    sn = SampleNode('a' * 256, SubSampleType.TECHNICAL_REPLICATE, 'b' * 256)
    assert sn.name == 'a' * 256
    assert sn.type == SubSampleType.TECHNICAL_REPLICATE
    assert sn.parent == 'b' * 256
    assert sn.controlled_metadata == {}
    assert sn.user_metadata == {}
    assert sn.source_metadata == ()

    sn = SampleNode('a' * 256, SubSampleType.TECHNICAL_REPLICATE, 'b' * 256,
                    {'a' * 256: {'bar': 'baz', 'bat': 'wh\tee'},
                     'wugga': {'a': 'b' * 1024},
                     # tests that having a controlled key doesn't force a source key
                     'z': {'u': 'v'}},
                    {'a': {'b' * 256: 'fo\no', 'c': 1, 'd': 1.5, 'e': False}},
                    [SourceMetadata('a' * 256, 'sk', {'a': 'b'}),
                     SourceMetadata('wugga', 'sk', {'a': 'b'})])
    assert sn.name == 'a' * 256
    assert sn.type == SubSampleType.TECHNICAL_REPLICATE
    assert sn.parent == 'b' * 256
    assert sn.controlled_metadata == {'a' * 256: {'bar': 'baz', 'bat': 'wh\tee'},
                                      'wugga': {'a': 'b' * 1024},
                                      'z': {'u': 'v'}}
    assert sn.user_metadata == {'a': {'b' * 256: 'fo\no', 'c': 1, 'd': 1.5, 'e': False}}
    assert sn.source_metadata == (SourceMetadata('a' * 256, 'sk', {'a': 'b'}),
                                  SourceMetadata('wugga', 'sk', {'a': 'b'}))

    # 100KB when serialized to json
    meta = {str(i): {'b': '𐎦' * 25} for i in range(848)}
    meta['a'] = {'b': 'c' * 30}

    # Also 100KB when the size calculation routine is run
    smeta = [SourceMetadata(str(i), 'sksksk', {'x': '𐎦' * 25}) for i in range(848)]
    smeta.append(SourceMetadata('a', 'b' * 35, {'u': 'v'}))

    sn = SampleNode('a', SubSampleType.SUB_SAMPLE, 'b', meta, meta, smeta)
    assert sn.name == 'a'
    assert sn.type == SubSampleType.SUB_SAMPLE
    assert sn.parent == 'b'
    assert sn.controlled_metadata == meta
    assert sn.user_metadata == meta
    assert sn.source_metadata == tuple(smeta)


def test_sample_node_build_fail():
    # not testing every permutation of failing check_string here, just one test to make sure
    # it's there
    _sample_node_build_fail('', SubSampleType.BIOLOGICAL_REPLICATE, None,
                            MissingParameterError('subsample name'))
    _sample_node_build_fail('a' * 257, SubSampleType.BIOLOGICAL_REPLICATE, None,
                            IllegalParameterError('subsample name exceeds maximum length of 256'))
    _sample_node_build_fail('a', None, None,
                            ValueError('type cannot be a value that evaluates to false'))
    _sample_node_build_fail('a', SubSampleType.TECHNICAL_REPLICATE, 'b' * 257,
                            IllegalParameterError('parent exceeds maximum length of 256'))
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


def test_sample_node_build_fail_metadata():
    _sample_node_build_fail_metadata(
        {None: {}},
        '{} metadata keys may not be null or whitespace only')

    _sample_node_build_fail_metadata(
        {'  \t  \n  ': {}},
        '{} metadata keys may not be null or whitespace only')

    _sample_node_build_fail_metadata(
        {'foo': None},
        '{} metadata value associated with metadata key foo is null or empty')
    _sample_node_build_fail_metadata(
        {'foo': {}},
        '{} metadata value associated with metadata key foo is null or empty')

    _sample_node_build_fail_metadata(
        {'a' * 255 + 'ff': {}},
        f"{{}} metadata has key starting with {'a' * 255 + 'f'} " +
        "that exceeds maximum length of 256")

    _sample_node_build_fail_metadata(
        {'wh\tee': {}},
        "{} metadata key wh\tee's character at index 2 is a control character.")

    _sample_node_build_fail_metadata(
        {'bat': {'a' * 255 + 'ff': 'whee'}},
        '{} metadata has a value key associated with metadata key bat starting with ' +
        f"{'a' * 255 + 'f'} that exceeds maximum length of 256")

    _sample_node_build_fail_metadata(
        {'wugga': {'wh\tee': {}}},
        '{} metadata value key wh\tee associated with metadata key wugga has a character at ' +
        'index 2 that is a control character.')

    _sample_node_build_fail_metadata(
        {'bat': {'whee': 'a' * 255 + 'f' * 770}},
        '{} metadata has a value associated with metadata key bat and value key whee starting ' +
        f"with {'a' * 255 + 'f'} that exceeds maximum length of 1024")

    _sample_node_build_fail_metadata(
        {'bat': {'whee': '\bwhoopbutt'}},
        '{} metadata value associated with metadata key bat and value key whee has a ' +
        'character at index 0 that is a control character.')

    # 100001B when serialized to json
    meta = {str(i): {'b': '𐎦' * 25} for i in range(848)}
    meta['a'] = {'b': 'c' * 31}
    _sample_node_build_fail_metadata(meta, "{} metadata is larger than maximum of 100000B")


def _sample_node_build_fail_metadata(meta, expected):
    with raises(Exception) as got:
        SampleNode('n', SubSampleType.BIOLOGICAL_REPLICATE, controlled_metadata=meta)
    assert_exception_correct(got.value, IllegalParameterError(expected.format('Controlled')))
    with raises(Exception) as got:
        SampleNode('n', SubSampleType.BIOLOGICAL_REPLICATE, user_metadata=meta)
    assert_exception_correct(got.value, IllegalParameterError(expected.format('User')))


def test_sample_node_build_fail_source_metadata():
    _sample_node_build_fail_source_metadata(
        [SourceMetadata('k', 'k1', {'a': 'b'}), None], ValueError(
            'Index 1 of iterable source_metadata cannot be a value that evaluates to false'))
    _sample_node_build_fail_source_metadata(
        [SourceMetadata('f', 'k1', {'a': 'b'}), SourceMetadata('k', 'k1', {'c': 'd'})],
        IllegalParameterError(
            'Source metadata key k does not appear in the controlled metadata'),
        cmeta={'f': {'x': 'y'}})
    _sample_node_build_fail_source_metadata(
        [SourceMetadata('k', 'k1', {'a': 'b'}), SourceMetadata('k', 'k2', {'a': 2})],
        IllegalParameterError('Duplicate source metadata key: k'))

    # 100001KB when the size calculation routine is run
    smeta = [SourceMetadata(str(i), 'sksksk', {'x': '𐎦' * 25}) for i in range(848)]
    smeta.append(SourceMetadata('a', 'b' * 36, {'x': 'y'}))
    _sample_node_build_fail_source_metadata(smeta, IllegalParameterError(
        'Source metadata is larger than maximum of 100000B'))


def _sample_node_build_fail_source_metadata(meta, expected, cmeta=None):
    if cmeta is None:
        cmeta = {sm.key: {'x': 'y'} for sm in meta if sm is not None}
    with raises(Exception) as got:
        SampleNode(
            'n',
            SubSampleType.BIOLOGICAL_REPLICATE,
            controlled_metadata=cmeta,
            source_metadata=meta)
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

    assert SampleNode('foo', s, 'bar', user_metadata={'foo': {'a': 'b'}}) == SampleNode(
        'foo', s, 'bar', user_metadata={'foo': {'a': 'b'}})
    assert SampleNode('foo', s, 'bar', user_metadata={'foo': {'a': 'b'}}) != SampleNode(
        'foo', s, 'bar', user_metadata={'foo': {'a': 'c'}})
    assert SampleNode('foo', s, 'bar', user_metadata={'foo': {'a': 'b'}}) != SampleNode(
        'foo', s, 'bar', user_metadata={'foo': {'z': 'b'}})
    assert SampleNode('foo', s, 'bar', user_metadata={'foo': {'a': 'b'}}) != SampleNode(
        'foo', s, 'bar', user_metadata={'fo': {'a': 'b'}})

    assert SampleNode(
        'foo',
        s,
        'bar',
        controlled_metadata={'k': {'a': 'b'}},
        source_metadata=[SourceMetadata('k', 'k', {'c': 'd'})]) == SampleNode(
            'foo',
            s,
            'bar',
            controlled_metadata={'k': {'a': 'b'}},
            source_metadata=[SourceMetadata('k', 'k', {'c': 'd'})])
    assert SampleNode(
        'foo',
        s,
        'bar',
        controlled_metadata={'k': {'a': 'b'}},
        source_metadata=[SourceMetadata('k', 'k', {'c': 'd'})]) != SampleNode(
            'foo',
            s,
            'bar',
            controlled_metadata={'k': {'a': 'b'}},
            source_metadata=[SourceMetadata('k', 'v', {'c': 'd'})])

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

    assert hash(SampleNode('foo', s, 'bar', user_metadata={'foo': {'a': 'b'}})) == hash(
        SampleNode('foo', s, 'bar', user_metadata={'foo': {'a': 'b'}}))
    assert hash(SampleNode('foo', s, 'bar', user_metadata={'foo': {'a': 'b'}})) != hash(
        SampleNode('foo', s, 'bar', user_metadata={'foo': {'a': 'c'}}))
    assert hash(SampleNode('foo', s, 'bar', user_metadata={'foo': {'a': 'b'}})) != hash(
        SampleNode('foo', s, 'bar', user_metadata={'foo': {'z': 'b'}}))
    assert hash(SampleNode('foo', s, 'bar', user_metadata={'foo': {'a': 'b'}})) != hash(
        SampleNode('foo', s, 'bar', user_metadata={'fo': {'a': 'b'}}))

    assert hash(SampleNode(
        'foo',
        s,
        'bar',
        controlled_metadata={'k': {'a': 'b'}},
        source_metadata=[SourceMetadata('k', 'k', {'c': 'd'})])) == hash(SampleNode(
            'foo',
            s,
            'bar',
            controlled_metadata={'k': {'a': 'b'}},
            source_metadata=[SourceMetadata('k', 'k', {'c': 'd'})]))
    assert hash(SampleNode(
        'foo',
        s,
        'bar',
        controlled_metadata={'k': {'a': 'b'}},
        source_metadata=[SourceMetadata('k', 'k', {'c': 'd'})])) != hash(SampleNode(
            'foo',
            s,
            'bar',
            controlled_metadata={'k': {'a': 'b'}},
            source_metadata=[SourceMetadata('k', 'v', {'c': 'd'})]))


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

    s = Sample([sn], 'a' * 256)
    assert s.nodes == (sndup,)
    assert s.name == 'a' * 256

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    s = SavedSample(id_, UserID('user'), [sn], dt(6))
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == UserID('user')
    assert s.nodes == (sndup,)
    assert s.savetime == dt(6)
    assert s.name is None
    assert s.version is None

    s = SavedSample(id_, UserID('user2'), [sn], dt(6), 'foo')
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == UserID('user2')
    assert s.nodes == (sndup,)
    assert s.savetime == dt(6)
    assert s.name == 'foo'
    assert s.version is None

    s = SavedSample(id_, UserID('user'), [sn], dt(6), 'foo', 1)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == UserID('user')
    assert s.nodes == (sndup,)
    assert s.savetime == dt(6)
    assert s.name == 'foo'
    assert s.version == 1

    s = SavedSample(id_, UserID('user'), [sn], dt(6), 'foo', 8)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == UserID('user')
    assert s.nodes == (sndup,)
    assert s.savetime == dt(6)
    assert s.name == 'foo'
    assert s.version == 8

    s = SavedSample(id_, UserID('user'), [sn], dt(6), version=8)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == UserID('user')
    assert s.nodes == (sndup,)
    assert s.savetime == dt(6)
    assert s.name is None
    assert s.version == 8


def test_sample_build_fail():
    # not testing every permutation of failing check_string here, just one test to make sure
    # it's there

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    u = UserID('user')
    sn = SampleNode('foo')
    tn = SampleNode('bar', SubSampleType.TECHNICAL_REPLICATE, 'foo')
    sn2 = SampleNode('baz')
    dup = SampleNode('foo')
    d = dt(8)

    _sample_build_fail(
        [sn], 'a' * 257, IllegalParameterError('name exceeds maximum length of 256'))
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
    _sample_with_id_build_fail(id_, None, [sn], d, None, None,
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
    s = SavedSample(id_, UserID('u'), nodes, dt(8))
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.user == UserID('u')
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
        SavedSample(id_, UserID('u'), nodes, dt(8), name)
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
    u = UserID('u')
    u2 = UserID('u2')

    assert SavedSample(id1, u, [sn], dt1) == SavedSample(id1, u, [sn], dt(5))
    assert SavedSample(id1, u, [sn], dt1) != SavedSample(id2, u, [sn], dt1)
    assert SavedSample(id1, u, [sn], dt1) != SavedSample(id1, u2, [sn], dt1)
    assert SavedSample(id1, u, [sn], dt1) != SavedSample(id1, u, [sn2], dt1)
    assert SavedSample(id1, u, [sn], dt1) != SavedSample(id1, u, [sn], dt2)

    assert SavedSample(id1, u, [sn], dt1, 'yay') == SavedSample(id1, u, [sn], dt1, 'yay')
    assert SavedSample(id1, u, [sn], dt1, 'yay') != SavedSample(id1, u, [sn], dt1, 'yooo')

    assert SavedSample(id1, u, [sn], dt2, 'yay', 6) == SavedSample(id1, u, [sn], dt2, 'yay', 6)
    assert SavedSample(id1, u, [sn], dt1, 'yay', 6) != SavedSample(id1, u, [sn], dt1, 'yay', 7)

    assert SavedSample(id1, u, [sn], dt1, 'yay') != Sample([sn], 'yay')
    assert Sample([sn], 'yay') != SavedSample(id1, u, [sn], dt1, 'yay')


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
    u = UserID('u')
    u2 = UserID('u2')

    assert hash(Sample([sn], 'yay')) == hash(Sample([sn], 'yay'))
    assert hash(Sample([sn], 'foo')) == hash(Sample([sn], 'foo'))
    assert hash(Sample([sn], 'yay')) != hash(Sample([sn2], 'yay'))
    assert hash(Sample([sn], 'yay')) != hash(Sample([sn], 'yo'))

    assert hash(SavedSample(id1, u, [sn], dt1, 'yay')) == hash(SavedSample(
                                                                 id1, u, [sn], dt(5), 'yay'))
    assert hash(SavedSample(id2, u, [sn], dt1, 'foo')) == hash(SavedSample(
                                                                 id2, u, [sn], dt1, 'foo'))
    assert hash(SavedSample(id1, u, [sn], dt1, 'foo')) != hash(SavedSample(
                                                                 id2, u, [sn], dt1, 'foo'))
    assert hash(SavedSample(id1, u, [sn], dt1, 'foo')) != hash(SavedSample(
                                                                 id1, u2, [sn], dt1, 'foo'))
    assert hash(SavedSample(id2, u, [sn], dt1, 'foo')) != hash(SavedSample(
                                                                 id2, u, [sn2], dt1, 'foo'))
    assert hash(SavedSample(id2, u, [sn], dt1, 'foo')) != hash(SavedSample(
                                                                 id2, u, [sn], dt2, 'foo'))
    assert hash(SavedSample(id2, u, [sn], dt1, 'foo')) != hash(SavedSample(
                                                                 id2, u, [sn], dt1, 'bar'))
    assert hash(SavedSample(id1, u, [sn], dt1, 'foo', 6)) == hash(SavedSample(
                                                                    id1, u, [sn], dt1, 'foo', 6))
    assert hash(SavedSample(id1, u, [sn], dt1, 'foo', 6)) != hash(SavedSample(
                                                                    id1, u, [sn], dt1, 'foo', 7))


def test_sample_address_init():
    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdef')

    sa = SampleAddress(id1, 1)
    assert sa.sampleid == id2
    assert sa.version == 1
    assert str(sa) == '12345678-90ab-cdef-1234-567890abcdef:1'

    sa = SampleAddress(id1, 394)
    assert sa.sampleid == id2
    assert sa.version == 394
    assert str(sa) == '12345678-90ab-cdef-1234-567890abcdef:394'


def test_sample_address_init_fail():
    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')

    _sample_address_init_fail(None, 6, ValueError(
        'sampleid cannot be a value that evaluates to false'))
    _sample_address_init_fail(id1, None, IllegalParameterError('version must be > 0'))
    _sample_address_init_fail(id1, 0, IllegalParameterError('version must be > 0'))
    _sample_address_init_fail(id1, -5, IllegalParameterError('version must be > 0'))


def _sample_address_init_fail(sid, ver, expected):
    with raises(Exception) as got:
        SampleAddress(sid, ver)
    assert_exception_correct(got.value, expected)


def test_sample_address_equals():
    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdef')
    idd1 = uuid.UUID('1234567890abcdef1234567890abcded')
    idd2 = uuid.UUID('1234567890abcdef1234567890abcded')

    assert SampleAddress(id1, 7) == SampleAddress(id2, 7)
    assert SampleAddress(idd1, 89) == SampleAddress(idd2, 89)

    assert SampleAddress(id1, 6) != (id1, 6)

    assert SampleAddress(id1, 42) != SampleAddress(idd1, 42)
    assert SampleAddress(id1, 42) != SampleAddress(id1, 46)


def test_sample_address_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__
    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdef')
    idd1 = uuid.UUID('1234567890abcdef1234567890abcded')
    idd2 = uuid.UUID('1234567890abcdef1234567890abcded')

    assert hash(SampleAddress(id1, 7)) == hash(SampleAddress(id2, 7))
    assert hash(SampleAddress(idd1, 89)) == hash(SampleAddress(idd2, 89))

    assert hash(SampleAddress(id1, 42)) != hash(SampleAddress(idd1, 42))
    assert hash(SampleAddress(id1, 42)) != hash(SampleAddress(id1, 46))


def test_sample_node_address_init():
    id_a1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id_a2 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id_b1 = uuid.UUID('1234567890abcdef1234567890abcdee')
    id_b2 = uuid.UUID('1234567890abcdef1234567890abcdee')

    sa = SampleNodeAddress(SampleAddress(id_a1, 5), 'somenode')
    assert sa.sampleid == id_a2
    assert sa.version == 5
    assert sa.node == 'somenode'
    assert str(sa) == '12345678-90ab-cdef-1234-567890abcdef:5:somenode'

    sa = SampleNodeAddress(SampleAddress(id_b1, 1), 'e' * 256)
    assert sa.sampleid == id_b2
    assert sa.version == 1
    assert sa.node == 'e' * 256
    assert str(sa) == ('12345678-90ab-cdef-1234-567890abcdee:1:' + 'e' * 256)


def test_sample_node_address_init_fail():
    sa = SampleAddress(uuid.UUID('1234567890abcdef1234567890abcdef'), 5)

    _sample_node_address_init_fail(None, 'f',  ValueError(
        'sample cannot be a value that evaluates to false'))
    _sample_node_address_init_fail(sa, None, MissingParameterError('node'))
    _sample_node_address_init_fail(sa, '  \t \n  ', MissingParameterError('node'))
    _sample_node_address_init_fail(sa, '3' * 257, IllegalParameterError(
        'node exceeds maximum length of 256'))


def _sample_node_address_init_fail(sample, node, expected):
    with raises(Exception) as got:
        SampleNodeAddress(sample, node)
    assert_exception_correct(got.value, expected)


def test_sample_node_address_equals():
    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdef')
    idd1 = uuid.UUID('1234567890abcdef1234567890abcded')
    idd2 = uuid.UUID('1234567890abcdef1234567890abcded')

    sa_a1 = SampleAddress(id1, 6)
    sa_a2 = SampleAddress(id2, 6)
    sa_ad = SampleAddress(id1, 7)
    sa_b1 = SampleAddress(idd1, 6)
    sa_b2 = SampleAddress(idd2, 6)

    assert SampleNodeAddress(sa_a1, 'n') == SampleNodeAddress(sa_a2, 'n')
    assert SampleNodeAddress(sa_b1, 'this is a node') == SampleNodeAddress(sa_b2, 'this is a node')

    assert SampleNodeAddress(sa_a1, 'z') != (sa_a1, 'z')

    assert SampleNodeAddress(sa_a1, 'n') != SampleNodeAddress(sa_b1, 'n')
    assert SampleNodeAddress(sa_a1, 'n') != SampleNodeAddress(sa_ad, 'n')
    assert SampleNodeAddress(sa_a1, 'n') != SampleNodeAddress(sa_a2, 'z')


def test_sample_node_address_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__
    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdef')
    idd1 = uuid.UUID('1234567890abcdef1234567890abcded')
    idd2 = uuid.UUID('1234567890abcdef1234567890abcded')

    sa_a1 = SampleAddress(id1, 6)
    sa_a2 = SampleAddress(id2, 6)
    sa_ad = SampleAddress(id1, 7)
    sa_b1 = SampleAddress(idd1, 6)
    sa_b2 = SampleAddress(idd2, 6)

    assert hash(SampleNodeAddress(sa_a1, 'n')) == hash(SampleNodeAddress(sa_a2, 'n'))
    assert hash(SampleNodeAddress(sa_b1, 'this is a node')) == hash(
        SampleNodeAddress(sa_b2, 'this is a node'))

    assert hash(SampleNodeAddress(sa_a1, 'n')) != hash(SampleNodeAddress(sa_b1, 'n'))
    assert hash(SampleNodeAddress(sa_a1, 'n')) != hash(SampleNodeAddress(sa_ad, 'n'))
    assert hash(SampleNodeAddress(sa_a1, 'n')) != hash(SampleNodeAddress(sa_a2, 'z'))
