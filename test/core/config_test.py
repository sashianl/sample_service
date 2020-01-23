# Most of the tests for the config code are in the integration tests as they require running
# arango and auth instances

import os
import shutil
import tempfile
import yaml
from pytest import raises, fixture
from jsonschema.exceptions import ValidationError

from core import test_utils
from core.test_utils import assert_exception_correct
from SampleService.core.config import get_validators


@fixture(scope='module')
def temp_dir():
    tempdir = test_utils.get_temp_dir()
    yield tempdir

    if test_utils.get_delete_temp_files():
        shutil.rmtree(test_utils.get_temp_dir())


def _write_config(cfg, temp_dir):
    tf = tempfile.mkstemp('.tmp.cfg', 'config_test_', dir=temp_dir)
    os.close(tf[0])
    with open(tf[1], 'w') as temp:
        yaml.dump(cfg, temp)
    return tf[1]


def test_config_get_validators(temp_dir):
    cfg = {'key1': [{'module': 'core.config_test_vals',
                     'callable-builder': 'val1'
                     }],
           'key2': [{'module': 'core.config_test_vals',
                     'callable-builder': 'val2',
                     'parameters': {'max-len': 7, 'foo': 'bar'}
                     },
                    {'module': 'core.config_test_vals',
                     'callable-builder': 'val2',
                     'parameters': {'max-len': 5, 'foo': 'bar'}
                     }],
           'key3': [{'module': 'core.config_test_vals',
                     'callable-builder': 'val1',
                     'parameters': {'foo': 'bat'}
                     }]
           }
    tf = _write_config(cfg, temp_dir)
    vals = get_validators('file://' + tf)
    assert len(vals) == 3
    # the test validators always fail
    assert len(vals['key1']) == 1
    assert vals['key1'][0]({'a': 'b'}) == "1, {}, {'a': 'b'}"
    assert len(vals['key2']) == 2
    assert vals['key2'][0]({'a': 'd'}) == "2, {'foo': 'bar', 'max-len': 7}, {'a': 'd'}"
    assert vals['key2'][1]({'a': 'd'}) == "2, {'foo': 'bar', 'max-len': 5}, {'a': 'd'}"
    assert len(vals['key3']) == 1
    assert vals['key3'][0]({'a': 'c'}) == "1, {'foo': 'bat'}, {'a': 'c'}"

    # noop entry
    cfg = {}
    tf = _write_config(cfg, temp_dir)
    assert get_validators('file://' + tf) == {}


def test_config_get_validators_fail_bad_file(temp_dir):
    tf = _write_config({}, temp_dir)
    os.remove(tf)
    with raises(Exception) as got:
        get_validators('file://' + tf)
    assert_exception_correct(got.value, ValueError(
        f"Failed to open validator configuration file at file://{tf}: " +
        f"[Errno 2] No such file or directory: '{tf}'"))


def test_config_get_validators_fail_bad_yaml(temp_dir):
    # calling str() on ValidationErrors returns more detailed into about the error
    tf = tempfile.mkstemp('.tmp.cfg', 'config_test_bad_yaml', dir=temp_dir)
    os.close(tf[0])
    with open(tf[1], 'w') as temp:
        temp.write('[bad yaml')
    with raises(Exception) as got:
        get_validators('file://' + tf[1])
    assert_exception_correct(got.value, ValueError(
        f'Failed to open validator configuration file at file://{tf[1]}: while parsing a ' +
        'flow sequence\n  in "<urllib response>", line 1, column 1\nexpected \',\' or \']\', ' +
        'but got \'<stream end>\'\n  in "<urllib response>", line 1, column 10'
    ))


def test_config_get_validators_fail_bad_params(temp_dir):
    # calling str() on ValidationErrors returns more detailed into about the error
    _config_get_validators_fail(
        '', temp_dir,
        ValidationError("'' is not of type 'object'"))
    _config_get_validators_fail(
        ['foo', 'bar'], temp_dir,
        ValidationError("['foo', 'bar'] is not of type 'object'"))
    _config_get_validators_fail(
        {'key': 'y'}, temp_dir,
        ValidationError("'y' is not of type 'array'"))
    _config_get_validators_fail(
        {'key': ['foo']}, temp_dir,
        ValidationError("'foo' is not of type 'object'"))
    _config_get_validators_fail(
        {'key': [{}]}, temp_dir,
        ValidationError("'module' is a required property"))
    _config_get_validators_fail(
        {'key': [{'module': 'foo'}]}, temp_dir,
        ValidationError("'callable-builder' is a required property"))
    _config_get_validators_fail(
        {'key': [{'module': 'foo', 'callable-builder': 'bar', 'xtraprop': 1}]}, temp_dir,
        ValidationError("Additional properties are not allowed ('xtraprop' was unexpected)"))
    _config_get_validators_fail(
        {'key': [{'module': ['foo'], 'callable-builder': 'bar'}]}, temp_dir,
        ValidationError("['foo'] is not of type 'string'"))
    _config_get_validators_fail(
        {'key': [{'module': 'foo', 'callable-builder': ['bar']}]}, temp_dir,
        ValidationError("['bar'] is not of type 'string'"))
    _config_get_validators_fail(
        {'key': [{'module': 'foo', 'callable-builder': 'bar', 'parameters': 'foo'}]}, temp_dir,
        ValidationError("'foo' is not of type 'object'"))


def test_config_get_validators_fail_no_module(temp_dir):
    _config_get_validators_fail(
        {'key': [{'module': 'no_modules_here', 'callable-builder': 'foo'}]}, temp_dir,
        ModuleNotFoundError("No module named 'no_modules_here'"))


def test_config_get_validators_fail_no_function(temp_dir):
    _config_get_validators_fail(
        {'x': [{'module': 'core.config_test_vals', 'callable-builder': 'foo'}]}, temp_dir,
        ValueError("Metadata validator callable build #0 failed for key x: " +
                   "module 'core.config_test_vals' has no attribute 'foo'"))


def test_config_get_validators_fail_function_exception(temp_dir):
    _config_get_validators_fail(
        {'x': [{'module': 'core.config_test_vals', 'callable-builder': 'val1'},
               {'module': 'core.config_test_vals', 'callable-builder': 'fail_val'}]}, temp_dir,
        ValueError("Metadata validator callable build #1 failed for key x: " +
                   "we've no functions 'ere"))


def _config_get_validators_fail(cfg, temp_dir, expected):
    tf = _write_config(cfg, temp_dir)
    with raises(Exception) as got:
        get_validators('file://' + tf)
    assert_exception_correct(got.value, expected)
