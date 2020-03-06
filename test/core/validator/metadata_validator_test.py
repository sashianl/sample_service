import maps
from pytest import raises

from core.test_utils import assert_exception_correct
from SampleService.core.validator.metadata_validator import MetadataValidatorSet
from SampleService.core.errors import MetadataValidationError


def _noop(_, __):
    return None


def _noop3(_, __, ___):
    return None


def test_empty():
    mv = MetadataValidatorSet()

    assert mv.keys() == {}.keys()
    assert mv.prefix_keys() == []


def test_with_validators():
    mv = MetadataValidatorSet({
        # this is vile
        'key1': [lambda k, v: exec('assert k == "key1"'),
                 lambda k, v: exec('assert v == {"a": "b"}')
                 ],
        'key2': [lambda k, v: exec('assert k == "key2"')]
    })

    md = {'key1': {'a': 'b'}, 'key2': {'foo': 'bar'}}
    mv.validate_metadata(md)
    mv.validate_metadata(maps.FrozenMap(md))

    assert mv.keys() == {'key1': 1, 'key2': 2}.keys()
    assert mv.prefix_keys() == []
    assert mv.validator_count('key1') == 2
    assert mv.validator_count('key2') == 1


def test_with_prefix_validators():
    mv = MetadataValidatorSet(prefix_validators={
        # this is vile
        'pre1': [lambda p, k, v: exec('assert p == "pre1"'),
                 lambda p, k, v: exec('assert k == "pre1stuff"'),
                 lambda p, k, v: exec('assert v == {"a": "b"}')
                 ],
        'pre2': [lambda p, k, v: exec('assert p == "pre2"')]
    })

    md = {'pre1stuff': {'a': 'b'}, 'pre2thingy': {'foo': 'bar'}}
    mv.validate_metadata(md)
    mv.validate_metadata(maps.FrozenMap(md))

    assert mv.keys() == {}.keys()
    assert mv.prefix_keys() == ['pre1', 'pre2']
    assert mv.prefix_validator_count('pre1') == 3
    assert mv.prefix_validator_count('pre2') == 1


def test_with_prefix_validators_multiple_matches():
    results = []
    mv = MetadataValidatorSet(
        validators={'somekey': [lambda k, v: results.append((k, v))]},
        prefix_validators={
            'somekeya': [lambda p, k, v: exec('raise ValueError("test failed somekeya")')],
            'somekex': [lambda p, k, v: exec('raise ValueError("test failed somekex")')],
            'somekey': [lambda p, k, v: results.append((p, k, v))],
            'somekez': [lambda p, k, v: exec('raise ValueError("test failed somekez")')],
            'someke': [lambda p, k, v: results.append((p, k, v))],
            's': [lambda p, k, v: results.append((p, k, v))],
            't': [lambda p, k, v: exec('raise ValueError("test failed t")')],
            }
        )
    md = {'somekey': {'x', 'y'}}
    mv.validate_metadata(md)

    print(results)
    assert results == [
        ('somekey', {'x', 'y'}),
        ('s', 'somekey', {'x', 'y'}),
        ('someke', 'somekey', {'x', 'y'}),
        ('somekey', 'somekey', {'x', 'y'}),
    ]


def test_call_validator():
    mv = MetadataValidatorSet({
        'key1': [lambda k, v: (k, v, 1), lambda k, v: (k, v, 2)],
        'key2': [lambda k, v: (k, v, 3)]
    })
    assert mv.call_validator('key1', 0, {'foo', 'bar'}) == ('key1', {'foo', 'bar'}, 1)
    assert mv.call_validator('key1', 1, {'foo', 'bat'}) == ('key1', {'foo', 'bat'}, 2)
    assert mv.call_validator('key2', 0, {'foo', 'baz'}) == ('key2', {'foo', 'baz'}, 3)


def test_call_prefix_validator():
    mv = MetadataValidatorSet({}, {
        'p1': [lambda p, k, v: (p, k, v, 1), lambda p, k, v: (p, k, v, 2)],
        'p2': [lambda p, k, v: (p, k, v, 3)]
    })
    assert mv.call_prefix_validator(
        'p1', 0, 'key1', {'foo', 'bar'}) == ('p1', 'key1', {'foo', 'bar'}, 1)
    assert mv.call_prefix_validator(
        'p1', 1, 'key11', {'foo', 'bat'}) == ('p1', 'key11', {'foo', 'bat'}, 2)
    assert mv.call_prefix_validator(
        'p2', 0, 'key2', {'foo', 'baz'}) == ('p2', 'key2', {'foo', 'baz'}, 3)


def test_validator_count_fail():
    _validator_count_fail(
        {'key1': [_noop], 'key2': [_noop]}, 'key3', ValueError('No validators for key key3'))
    _validator_count_fail(
        {'key1': [_noop], 'key3': []}, 'key3', ValueError('No validators for key key3'))


def _validator_count_fail(vals, key, expected):
    mv = MetadataValidatorSet(vals)
    with raises(Exception) as got:
        mv.validator_count(key)
    assert_exception_correct(got.value, expected)


def test_prefix_validator_count_fail():
    _prefix_validator_count_fail(
        {'key1': [_noop], 'key2': [_noop]}, 'key3',
        ValueError('No prefix validators for prefix key3'))
    _prefix_validator_count_fail(
        {'key1': [_noop], 'key3': []}, 'key3',
        ValueError('No prefix validators for prefix key3'))
    _prefix_validator_count_fail(
        {'key1': [_noop], 'key': [_noop]}, 'key3',
        ValueError('No prefix validators for prefix key3'))
    _prefix_validator_count_fail(
        {'key1': [_noop], 'key3': [_noop]}, 'key',
        ValueError('No prefix validators for prefix key'))


def _prefix_validator_count_fail(vals, prefix, expected):
    mv = MetadataValidatorSet({}, vals)
    with raises(Exception) as got:
        mv.prefix_validator_count(prefix)
    assert_exception_correct(got.value, expected)


def test_call_validator_fail():
    _call_validator_fail({'key1': [_noop], 'key2': [_noop]}, 'key3', 0,
                         ValueError('No validators for key key3'))
    _call_validator_fail({'key1': [_noop], 'key2': [_noop], 'key3': []}, 'key3', 0,
                         ValueError('No validators for key key3'))
    _call_validator_fail(
        {'key1': [_noop], 'key2': [_noop, _noop]}, 'key2', 2,
        IndexError('Requested validator index 2 for key key2 but maximum index is 1'))


def _call_validator_fail(vals, key, index, expected):
    mv = MetadataValidatorSet(vals)
    with raises(Exception) as got:
        mv.call_validator(key, index, {})
    assert_exception_correct(got.value, expected)


def test_call_prefix_validator_fail():
    _call_prefix_validator_fail(
        {'key1': [_noop], 'key2': [_noop]}, 'key3', 0, 'key3stuff',
        ValueError('No prefix validators for prefix key3'))
    _call_prefix_validator_fail(
        {'key1': [_noop], 'key2': [_noop], 'key3': []}, 'key3', 0, 'key3stuff',
        ValueError('No prefix validators for prefix key3'))
    _call_prefix_validator_fail(
        {'key1': [_noop], 'key2': [_noop], 'key': [_noop]}, 'key3', 0, 'key3stuff',
        ValueError('No prefix validators for prefix key3'))
    _call_prefix_validator_fail(
        {'key1': [_noop], 'key2': [_noop], 'key3': [_noop]}, 'key', 0, 'key3stuff',
        ValueError('No prefix validators for prefix key'))
    _call_prefix_validator_fail(
        {'key1': [_noop], 'key2': [_noop, _noop]}, 'key2', 2, 'key2stuff',
        IndexError('Requested validator index 2 for prefix key2 but maximum index is 1'))


def _call_prefix_validator_fail(vals, prefix, index, key, expected):
    mv = MetadataValidatorSet(prefix_validators=vals)
    with raises(Exception) as got:
        mv.call_prefix_validator(prefix, index, key, {})
    assert_exception_correct(got.value, expected)


def test_validate_metadata_fail():
    _validate_metadata_fail(
        {'key1': [_noop], 'key2': [_noop]}, {}, [], ValueError('metadata must be a dict'))
    _validate_metadata_fail(
        {'key1': [_noop], 'key2': [_noop]},
        {'key': [_noop3]},
        {'key1': 'a', 'key2': 'b', 'kex': 'c'},
        MetadataValidationError('No validator available for metadata key kex'))
    _validate_metadata_fail(
        {'key1': [_noop], 'key2': [_noop], 'key3': []},
        {'ke': []},
        {'key1': 'a', 'key2': 'b', 'key3': 'c'},
        MetadataValidationError('No validator available for metadata key key3'))
    _validate_metadata_fail(
        {},
        {'keyx': [_noop3]},
        {'keyx1': 'a', 'keyx2': 'b', 'key': 'c'},
        MetadataValidationError('No validator available for metadata key key'))
    _validate_metadata_fail(
        {'key1': [_noop],
         'key2': [_noop],
         'key3': [_noop, lambda _, __: 'oh poop']},
        {},
        {'key1': 'a', 'key2': 'b', 'key3': 'c'},
        MetadataValidationError('Key key3: oh poop'))
    _validate_metadata_fail(
        None,
        {'key1': [_noop3],
         'key2': [_noop3],
         'key3': [_noop3, lambda _, __, ___: 'oh poop']},
        {'key1stuff': 'a', 'key2': 'b', 'key3yay': 'c'},
        MetadataValidationError('Prefix validator key3, key key3yay: oh poop'))


def _validate_metadata_fail(vals, prevals, meta, expected):
    mv = MetadataValidatorSet(vals, prevals)
    with raises(Exception) as got:
        mv.validate_metadata(meta)
    assert_exception_correct(got.value, expected)
