# Most of the tests for the config code are in the integration tests as they require running
# arango and auth instances

from pytest import raises

from SampleService.core.config import get_validators
from core.test_utils import assert_exception_correct


def test_config_get_validators():
    cfg = {'ignore-me': 'foo',
           'metaval-key1-module': 'core.config_test_vals',
           'metaval-key1-callable_builder': 'val1',
           'metaval-key2-module': 'core.config_test_vals',
           'metaval-key2-callable_builder': 'val2',
           'metaval-key2-param-max_len': '7',
           'metaval-key2-param-foo': 'bar',
           'metaval-key3-module': 'core.config_test_vals',
           'metaval-key3-callable_builder': 'val1',
           'metaval-key3-param-foo': 'bat',
           }
    vals = get_validators(cfg)
    # validators always fail
    assert vals['key1']({'a': 'b'}) == "1, {}, {'a': 'b'}"
    assert vals['key2']({'a': 'd'}) == "2, {'foo': 'bar', 'max_len': '7'}, {'a': 'd'}"
    assert vals['key3']({'a': 'c'}) == "1, {'foo': 'bat'}, {'a': 'c'}"


def test_config_get_validators_fail_bad_params():
    _config_get_validators_fail(
        {'metaval-key-bad_param-x': 'y'},
        ValueError('invalid configuration key: metaval-key-bad_param-x'))
    _config_get_validators_fail(
        {'metaval-': 'y'},
        ValueError('invalid configuration key: metaval-'))
    _config_get_validators_fail(
        {'metaval-x': 'y'},
        ValueError('invalid configuration key: metaval-x'))
    _config_get_validators_fail(
        {'metaval-x-param-y-z': 'y'},
        ValueError('invalid configuration key: metaval-x-param-y-z'))
    _config_get_validators_fail(
        {'metaval-x-mdule': 'foo',
         'metaval-x-param-y': 'y'},
        ValueError('Missing config param metaval-x-module'))
    _config_get_validators_fail(
        {'metaval-x-module': 'foo',
         'metaval-x-param-y': 'y'},
        ValueError('Missing config param metaval-x-callable_builder'))


def test_config_get_validators_fail_no_module():
    _config_get_validators_fail(
        {'metaval-x-module': 'no_modules_here',
         'metaval-x-callable_builder': 'foo'},
        ModuleNotFoundError("No module named 'no_modules_here'"))


def test_config_get_validators_fail_no_function():
    _config_get_validators_fail(
        {'metaval-x-module': 'core.config_test_vals',
         'metaval-x-callable_builder': 'foo'},
        ValueError("Metadata validator callable build failed for key x: " +
                   "module 'core.config_test_vals' has no attribute 'foo'"))


def test_config_get_validators_fail_function_exception():
    _config_get_validators_fail(
        {'metaval-x-module': 'core.config_test_vals',
         'metaval-x-callable_builder': 'fail_val'},
        ValueError("Metadata validator callable build failed for key x: " +
                   "we've no functions 'ere"))


def _config_get_validators_fail(cfg, expected):
    with raises(Exception) as got:
        get_validators(cfg)
    assert_exception_correct(got.value, expected)
