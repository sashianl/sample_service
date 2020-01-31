import maps
from pytest import raises

from core.test_utils import assert_exception_correct
from SampleService.core.validator.metadata_validator import MetadataValidator
from SampleService.core.errors import MetadataValidationError


def _noop(_, __):
    return None


def test_empty():
    mv = MetadataValidator()

    assert mv.keys() == {}.keys()


def test_with_validators():
    mv = MetadataValidator({
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
    assert mv.validator_count('key1') == 2
    assert mv.validator_count('key2') == 1


def test_call_validator():
    mv = MetadataValidator({
        'key1': [lambda k, v: (k, v, 1), lambda k, v: (k, v, 2)],
        'key2': [lambda k, v: (k, v, 3)]
    })
    assert mv.call_validator('key1', 0, {'foo', 'bar'}) == ('key1', {'foo', 'bar'}, 1)
    assert mv.call_validator('key1', 1, {'foo', 'bat'}) == ('key1', {'foo', 'bat'}, 2)
    assert mv.call_validator('key2', 0, {'foo', 'baz'}) == ('key2', {'foo', 'baz'}, 3)


def test_validator_count_fail():
    _validator_count_fail(
        {'key1': [_noop], 'key2': [_noop]}, 'key3', ValueError('No validators for key key3'))
    _validator_count_fail(
        {'key1': [_noop], 'key3': []}, 'key3', ValueError('No validators for key key3'))


def _validator_count_fail(vals, key, expected):
    mv = MetadataValidator(vals)
    with raises(Exception) as got:
        mv.validator_count(key)
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
    mv = MetadataValidator(vals)
    with raises(Exception) as got:
        mv.call_validator(key, index, {})
    assert_exception_correct(got.value, expected)


def test_validate_metadata_fail():
    _validate_metadata_fail(
        {'key1': [_noop], 'key2': [_noop]}, [], ValueError('metadata must be a dict'))
    _validate_metadata_fail(
        {'key1': [_noop], 'key2': [_noop]},
        {'key1': 'a', 'key2': 'b', 'key3': 'c'},
        MetadataValidationError('No validator available for metadata key key3'))
    _validate_metadata_fail(
        {'key1': [_noop], 'key2': [_noop], 'key3': []},
        {'key1': 'a', 'key2': 'b', 'key3': 'c'},
        MetadataValidationError('No validator available for metadata key key3'))
    _validate_metadata_fail(
        {'key1': [_noop],
         'key2': [_noop],
         'key3': [_noop, lambda _, __: 'oh poop']},
        {'key1': 'a', 'key2': 'b', 'key3': 'c'},
        MetadataValidationError('Key key3: oh poop'))


def _validate_metadata_fail(vals, meta, expected):
    mv = MetadataValidator(vals)
    with raises(Exception) as got:
        mv.validate_metadata(meta)
    assert_exception_correct(got.value, expected)
