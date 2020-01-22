from pytest import raises
from core.test_utils import assert_exception_correct

from SampleService.core.validators import builtin
from SampleService.core.errors import MetadataValidationError


def test_noop():
    # not much to test here.
    n = builtin.noop({})

    assert n({}) is None


def test_string_length():
    sl = builtin.string_length({'max_len': 2})
    assert sl({
        'fo': 'b',
        'ba': 'f',
        'a': True,
        'b': 1111111111,
        'c': 1.23456789}) is None


def test_string_length_fail_bad_constructor_args():
    _string_length_fail_construct(None, ValueError('max_len parameter required'))
    _string_length_fail_construct({'foo': 'bar'}, ValueError('max_len parameter required'))
    _string_length_fail_construct({'max_len': 'shazzbat'},
                                  ValueError('max_len must be an integer'))
    _string_length_fail_construct({'max_len': '0'}, ValueError('max_len must be > 0'))


def _string_length_fail_construct(d, expected):
    with raises(Exception) as got:
        builtin.string_length(d)
    assert_exception_correct(got.value, expected)


def test_string_length_fail_bad_metadata_values():
    _string_length_fail_validate(2, {'foo': 'ba', 'ba': 'f'}, MetadataValidationError(
        'Metadata contains key longer than max length of 2'))
    _string_length_fail_validate(4, {'foo': 'ba', 'ba': 'fudge'}, MetadataValidationError(
        'Metadata value at key ba is longer than max length of 4'))


def _string_length_fail_validate(max_len, meta, expected):
    sl = builtin.string_length({'max_len': max_len})
    with raises(Exception) as got:
        sl(meta)
    assert_exception_correct(got.value, expected)
