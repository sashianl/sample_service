from pytest import raises
from core.test_utils import assert_exception_correct

from SampleService.core.validators import builtin


def test_noop():
    # not much to test here.
    n = builtin.noop({})

    assert n({}) is None


def test_string_general():
    sl = builtin.string({'max-len': 2})
    assert sl({
        'fo': 'b',
        'e': 'fb',
        'a': True,
        'b': 1111111111,
        'c': 1.23456789}) is None


def test_string_single_keys():
    sl = builtin.string({'keys': 'whee', 'required': False})
    assert sl({
        'fo': 'b',
        'e': 'fb',
        'whee': 'whooooooooooooo',
        'a': True,
        'b': 1111111111,
        'c': 1.23456789}) is None


def test_string_multiple_keys_max_len():
    sl = builtin.string({'keys': ['whee', 'whoo', 'whoa'], 'max-len': 9})
    # missing whoo key
    assert sl({
        'fo': 'b',
        'e': 'fb',
        'whee': 'whoo',
        'a': True,
        'b': 1111111111,
        'whoa': 'free mind',
        'c': 1.23456789}) is None


def test_string_multiple_keys_required():
    sl = builtin.string({'keys': ['whee', 'whoo', 'whoa'], 'required': True})
    assert sl({
        'fo': 'b',
        'e': 'fb',
        'whee': None,  # test that none is allowed as a value, even w/ required
        'a': True,
        'whoo': 'whoopty doo',
        'b': 1111111111,
        'whoa': 'free mind',
        'c': 1.23456789}) is None


def test_string_fail_bad_constructor_args():
    _string_fail_construct(None, ValueError('d must be a dict'))
    _string_fail_construct({'foo': 'bar'}, ValueError(
        'If the keys parameter is not specified, max-len must be specified'))
    _string_fail_construct({'max-len': 'shazzbat'}, ValueError('max-len must be an integer'))
    _string_fail_construct({'max-len': '0'}, ValueError('max-len must be > 0'))
    _string_fail_construct({'keys': 356}, ValueError('keys parameter must be a string or list'))
    _string_fail_construct({'keys': ['foo', 'bar', 356, 'baz']}, ValueError(
        'keys parameter contains a non-string entry at index 2'))


def _string_fail_construct(d, expected):
    with raises(Exception) as got:
        builtin.string(d)
    assert_exception_correct(got.value, expected)


def test_string_fail_bad_metadata_values():
    _string_fail_validate(
        {'max-len': 2}, {'foo': 'ba', 'ba': 'f'},
        'Metadata contains key longer than max length of 2')
    _string_fail_validate(
        {'max-len': 4}, {'foo': 'ba', 'ba': 'fudge'},
        'Metadata value at key ba is longer than max length of 4')
    _string_fail_validate(
        {'keys': ['foo', 'bar'], 'required': True}, {'foo': 'ba', 'ba': 'fudge'},
        'Required key bar is missing')
    _string_fail_validate(
        {'keys': 'foo'}, {'foo': ['a'], 'ba': 'fudge'},
        'Metadata value at key foo is not a string')
    _string_fail_validate(
        {'keys': ['foo', 'bar'], 'required': True, 'max-len': 6}, {'foo': 'ba', 'bar': 'fudgera'},
        'Metadata value at key bar is longer than max length of 6')


def _string_fail_validate(cfg, meta, expected):
    assert builtin.string(cfg)(meta) == expected
