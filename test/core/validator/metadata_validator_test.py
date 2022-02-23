import maps
from pytest import raises

from core.test_utils import assert_exception_correct
from SampleService.core.validator.metadata_validator import MetadataValidatorSet, MetadataValidator
from SampleService.core.errors import MetadataValidationError, IllegalParameterError


def _noop(_, __):
    return None


def _noop3(_, __, ___):
    return None


def val1(key, value):
    return f'{key} {dict(sorted(value.items()))} 1'


def val2(key, value):
    return f'{key} {dict(sorted(value.items()))} 2'


def test_construct_val_std():
    mv = MetadataValidator('mykey', [val1, val2])

    assert mv.key == 'mykey'
    assert mv.metadata == {}
    assert mv.is_prefix_validator() is False
    assert len(mv.validators) == 2
    assert len(mv.prefix_validators) == 0
    assert mv.validators[0]('foo', {'a': 'b'}) == "foo {'a': 'b'} 1"
    assert mv.validators[1]('bar', {'c': 'd'}) == "bar {'c': 'd'} 2"


def test_construct_val_prefix_and_meta():
    mv = MetadataValidator(
        'my other key', prefix_validators=[val2], metadata={'foo': 'bar', 'baz': 'bat'})

    assert mv.key == 'my other key'
    assert mv.metadata == {'foo': 'bar', 'baz': 'bat'}
    assert mv.is_prefix_validator() is True
    assert len(mv.validators) == 0
    assert len(mv.prefix_validators) == 1
    assert mv.prefix_validators[0]('foo', {'a': 'b'}) == "foo {'a': 'b'} 2"


def test_construct_val_fail_bad_args():
    _fail_construct_val(None, [val1], None, ValueError(
        'key cannot be a value that evaluates to false'))
    _fail_construct_val('', [val1], None, ValueError(
        'key cannot be a value that evaluates to false'))
    _fail_construct_val('k', None, None, ValueError(
        'Exactly one of validators or prefix_validators must be supplied and must contain ' +
        'at least one validator'))
    _fail_construct_val('k', [], [], ValueError(
        'Exactly one of validators or prefix_validators must be supplied and must contain ' +
        'at least one validator'))
    _fail_construct_val('k', [val1], [val2], ValueError(
        'Exactly one of validators or prefix_validators must be supplied and must contain ' +
        'at least one validator'))


def _fail_construct_val(key, validators, prefix_validators, expected):
    with raises(Exception) as got:
        MetadataValidator(key, validators, prefix_validators)
    assert_exception_correct(got.value, expected)


def test_construct_set_fail_bad_args():
    _fail_construct_set([
        MetadataValidator('key1', [_noop]),
        MetadataValidator('key3', [_noop]),
        MetadataValidator('key1', [_noop]),
        ],
        ValueError('Duplicate validator: key1'))
    _fail_construct_set([
        MetadataValidator('key1', prefix_validators=[_noop]),
        MetadataValidator('key3', prefix_validators=[_noop]),
        MetadataValidator('key1', prefix_validators=[_noop]),
        ],
        ValueError('Duplicate prefix validator: key1'))


def _fail_construct_set(validators, expected):
    with raises(Exception) as got:
        MetadataValidatorSet(validators)
    assert_exception_correct(got.value, expected)


def test_empty_set():
    mv = MetadataValidatorSet()

    assert mv.keys() == []
    assert mv.prefix_keys() == []


def test_set_with_validators():
    mv = MetadataValidatorSet([
        # this is vile
        MetadataValidator(
            'key1',
            [lambda k, v: exec('assert k == "key1"'),
             lambda k, v: exec('assert v == {"a": "b"}')
             ]),
        MetadataValidator('key2', [lambda k, v: exec('assert k == "key2"')])
        ])

    md = {'key1': {'a': 'b'}, 'key2': {'foo': 'bar'}}
    mv.validate_metadata(md)
    mv.validate_metadata(maps.FrozenMap(md))

    assert mv.keys() == ['key1', 'key2']
    assert mv.prefix_keys() == []
    assert mv.validator_count('key1') == 2
    assert mv.validator_count('key2') == 1


def test_set_with_prefix_validators():
    mv = MetadataValidatorSet([
        # this is vile
        MetadataValidator('pre1', prefix_validators=[
            lambda p, k, v: exec('assert p == "pre1"'),
            lambda p, k, v: exec('assert k == "pre1stuff"'),
            lambda p, k, v: exec('assert v == {"a": "b"}')
            ]),
        MetadataValidator('pre2', prefix_validators=[lambda p, k, v: exec('assert p == "pre2"')])
        ])

    md = {'pre1stuff': {'a': 'b'}, 'pre2thingy': {'foo': 'bar'}}
    mv.validate_metadata(md)
    mv.validate_metadata(maps.FrozenMap(md))

    assert mv.keys() == []
    assert mv.prefix_keys() == ['pre1', 'pre2']
    assert mv.prefix_validator_count('pre1') == 3
    assert mv.prefix_validator_count('pre2') == 1


def test_set_with_prefix_validators_and_standard_validator():
    mv = MetadataValidatorSet([
        # this is vile
        MetadataValidator('pre1', prefix_validators=[
            lambda p, k, v: exec('assert p == "pre1"'),
            lambda p, k, v: exec('assert k == "pre1stuff"'),
            lambda p, k, v: exec('assert v == {"a": "b"}')
            ]),
        MetadataValidator('pre2', prefix_validators=[lambda p, k, v: exec('assert p == "pre2"')]),
        # test that non-prefix validator with same name is ok
        MetadataValidator('pre2', [lambda k, v: exec('raise ValueError()')])]
    )

    md = {'pre1stuff': {'a': 'b'}, 'pre2thingy': {'foo': 'bar'}}
    mv.validate_metadata(md)
    mv.validate_metadata(maps.FrozenMap(md))

    assert mv.keys() == ['pre2']
    assert mv.prefix_keys() == ['pre1', 'pre2']
    assert mv.prefix_validator_count('pre1') == 3
    assert mv.prefix_validator_count('pre2') == 1


def test_set_with_prefix_validators_multiple_matches():
    results = []
    mv = MetadataValidatorSet([
        MetadataValidator('somekey', [lambda k, v: results.append((k, v))]),
        MetadataValidator(
            'somekeya',
            prefix_validators=[lambda p, k, v: exec('raise ValueError("test failed somekeya")')]),
        MetadataValidator(
            'somekex',
            prefix_validators=[lambda p, k, v: exec('raise ValueError("test failed somekex")')]),
        MetadataValidator(
            'somekey',
            prefix_validators=[lambda p, k, v: results.append((p, k, v))]),
        MetadataValidator(
            'somekez',
            prefix_validators=[lambda p, k, v: exec('raise ValueError("test failed somekez")')]),
        MetadataValidator('someke', prefix_validators=[lambda p, k, v: results.append((p, k, v))]),
        MetadataValidator('s', prefix_validators=[lambda p, k, v: results.append((p, k, v))]),
        MetadataValidator(
            't',
            prefix_validators=[lambda p, k, v: exec('raise ValueError("test failed t")')]),
        ])
    md = {'somekey': {'x', 'y'}}
    mv.validate_metadata(md)

    print(results)
    assert results == [
        ('somekey', {'x', 'y'}),
        ('s', 'somekey', {'x', 'y'}),
        ('someke', 'somekey', {'x', 'y'}),
        ('somekey', 'somekey', {'x', 'y'}),
    ]


def test_set_with_key_metadata():
    mv = MetadataValidatorSet([
        MetadataValidator('pre1', prefix_validators=[_noop], metadata={'a': 'b', 'c': 'd'}),
        MetadataValidator('pre2', prefix_validators=[_noop]),
        MetadataValidator('pre3', prefix_validators=[_noop], metadata={'c': 'd'}),
        MetadataValidator('pre1', [_noop]),
        MetadataValidator('pre2', [_noop], metadata={'e': 'f', 'h': 'i'}),
        MetadataValidator('pre3', [_noop], metadata={'h': 'i'})
        ])

    assert mv.key_metadata([]) == {}
    assert mv.prefix_key_metadata([]) == {}

    assert mv.key_metadata(['pre1']) == {'pre1': {}}
    assert mv.prefix_key_metadata(['pre1'], exact_match=True) == {'pre1': {'a': 'b', 'c': 'd'}}

    assert mv.key_metadata(['pre1', 'pre3']) == {'pre1': {},
                                                 'pre3': {'h': 'i'}}
    assert mv.prefix_key_metadata(['pre1', 'pre2']) == {'pre1': {'a': 'b', 'c': 'd'},
                                                        'pre2': {}}

    assert mv.key_metadata(['pre1', 'pre2', 'pre3']) == {'pre1': {},
                                                         'pre2': {'e': 'f', 'h': 'i'},
                                                         'pre3': {'h': 'i'}}
    assert mv.prefix_key_metadata(['pre1', 'pre2', 'pre3']) == {'pre1': {'a': 'b', 'c': 'd'},
                                                                'pre2': {},
                                                                'pre3': {'c': 'd'}}


def test_set_with_prefix_match_key_metadata():
    mv = MetadataValidatorSet([
        MetadataValidator('a', prefix_validators=[_noop], metadata={'a': 'b'}),
        MetadataValidator('abc', prefix_validators=[_noop], metadata={'c': 'd'}),
        MetadataValidator('abcdef', prefix_validators=[_noop], metadata={'f': 'g'}),
        MetadataValidator('abcdefhi', prefix_validators=[_noop], metadata={'f': 'g'}),
        MetadataValidator('abzhi', prefix_validators=[_noop], metadata={'z': 'w'}),
        MetadataValidator('abzhijk', prefix_validators=[_noop], metadata={'q': 'q'}),
        MetadataValidator('b', prefix_validators=[_noop], metadata={'bbb': 'bbb'}),
    ])

    assert mv.prefix_key_metadata(['abcdef']) == {'abcdef': {'f': 'g'}}
    assert mv.prefix_key_metadata(['abcdef'], exact_match=False) == {
        'a': {'a': 'b'}, 'abc': {'c': 'd'}, 'abcdef': {'f': 'g'}}
    assert mv.prefix_key_metadata(['abcdefh'], exact_match=False) == {
        'a': {'a': 'b'}, 'abc': {'c': 'd'}, 'abcdef': {'f': 'g'}}
    assert mv.prefix_key_metadata(['abzhij'], exact_match=False) == {
        'a': {'a': 'b'}, 'abzhi': {'z': 'w'}}
    assert mv.prefix_key_metadata(['abcdef', 'abzhij'], exact_match=False) == {
        'a': {'a': 'b'},
        'abc': {'c': 'd'},
        'abcdef': {'f': 'g'},
        'abzhi': {'z': 'w'}}


def test_set_key_metadata_fail_bad_args():
    _key_metadata_fail_([], None, ValueError('keys cannot be None'))
    _key_metadata_fail_(
        [MetadataValidator('key1', [_noop]), MetadataValidator('key3', [_noop])],
        ['key1', 'key2', 'key3'],
        IllegalParameterError('No such metadata key: key2'))


def _key_metadata_fail_(vals, keys, expected):
    mv = MetadataValidatorSet(vals)
    with raises(Exception) as got:
        mv.key_metadata(keys)
    assert_exception_correct(got.value, expected)


def test_set_prefix_key_metadata_fail_bad_args():
    _prefix_key_metadata_fail_([], None, ValueError('keys cannot be None'))
    _prefix_key_metadata_fail_(
        [MetadataValidator('key1', prefix_validators=[_noop]),
         MetadataValidator('key2', prefix_validators=[_noop])],
        ['key1', 'key2', 'key3'],
        IllegalParameterError('No such prefix metadata key: key3'))


def _prefix_key_metadata_fail_(vals, keys, expected):
    mv = MetadataValidatorSet(vals)
    with raises(Exception) as got:
        mv.prefix_key_metadata(keys)
    assert_exception_correct(got.value, expected)

    with raises(Exception) as got:
        mv.prefix_key_metadata(keys, exact_match=True)
    assert_exception_correct(got.value, expected)


def test_prefix_key_metadata_fail_prefix_match():
    mv = MetadataValidatorSet([
        MetadataValidator('abcdef', prefix_validators=[_noop], metadata={'f': 'g'}),
        MetadataValidator('abcdefhi', prefix_validators=[_noop], metadata={'f': 'g'})
    ])

    with raises(Exception) as got:
        mv.prefix_key_metadata(None, exact_match=False)
    assert_exception_correct(got.value, ValueError('keys cannot be None'))

    with raises(Exception) as got:
        mv.prefix_key_metadata(['abcde'], exact_match=False)
    assert_exception_correct(got.value, IllegalParameterError(
        'No prefix metadata keys matching key abcde'))


def test_set_call_validator():
    mv = MetadataValidatorSet([
        MetadataValidator('key1', [lambda k, v: (k, v, 1), lambda k, v: (k, v, 2)]),
        MetadataValidator('key2', [lambda k, v: (k, v, 3)])
        ])
    assert mv.call_validator('key1', 0, {'foo', 'bar'}) == ('key1', {'foo', 'bar'}, 1)
    assert mv.call_validator('key1', 1, {'foo', 'bat'}) == ('key1', {'foo', 'bat'}, 2)
    assert mv.call_validator('key2', 0, {'foo', 'baz'}) == ('key2', {'foo', 'baz'}, 3)


def test_set_call_prefix_validator():
    mv = MetadataValidatorSet([
        MetadataValidator(
            'p1', prefix_validators=[lambda p, k, v: (p, k, v, 1), lambda p, k, v: (p, k, v, 2)]),
        MetadataValidator('p2', prefix_validators=[lambda p, k, v: (p, k, v, 3)])
        ])
    assert mv.call_prefix_validator(
        'p1', 0, 'key1', {'foo', 'bar'}) == ('p1', 'key1', {'foo', 'bar'}, 1)
    assert mv.call_prefix_validator(
        'p1', 1, 'key11', {'foo', 'bat'}) == ('p1', 'key11', {'foo', 'bat'}, 2)
    assert mv.call_prefix_validator(
        'p2', 0, 'key2', {'foo', 'baz'}) == ('p2', 'key2', {'foo', 'baz'}, 3)


def test_set_validator_count_fail():
    _validator_count_fail([
        MetadataValidator('key1', [_noop]), MetadataValidator('key2', [_noop])],
        'key3', ValueError('No validators for key key3'))


def _validator_count_fail(vals, key, expected):
    mv = MetadataValidatorSet(vals)
    with raises(Exception) as got:
        mv.validator_count(key)
    assert_exception_correct(got.value, expected)


def test_set_prefix_validator_count_fail():
    _prefix_validator_count_fail([
        MetadataValidator('key1', prefix_validators=[_noop]),
        MetadataValidator('key2', prefix_validators=[_noop])],
        'key3', ValueError('No prefix validators for prefix key3'))
    _prefix_validator_count_fail([  # exact match required
        MetadataValidator('key1', prefix_validators=[_noop]),
        MetadataValidator('key', prefix_validators=[_noop])],
        'key3', ValueError('No prefix validators for prefix key3'))
    _prefix_validator_count_fail([
        MetadataValidator('key1', prefix_validators=[_noop]),
        MetadataValidator('key3', prefix_validators=[_noop])],
        'key', ValueError('No prefix validators for prefix key'))


def _prefix_validator_count_fail(vals, prefix, expected):
    mv = MetadataValidatorSet(vals)
    with raises(Exception) as got:
        mv.prefix_validator_count(prefix)
    assert_exception_correct(got.value, expected)


def test_set_call_validator_fail():
    _call_validator_fail([
        MetadataValidator('key1', [_noop]),
        MetadataValidator('key2', [_noop])],
        'key3', 0, ValueError('No validators for key key3'))
    _call_validator_fail([
        MetadataValidator('key1', [_noop]),
        MetadataValidator('key2', [_noop, _noop])],
        'key2', 2, IndexError('Requested validator index 2 for key key2 but maximum index is 1'))


def _call_validator_fail(vals, key, index, expected):
    mv = MetadataValidatorSet(vals)
    with raises(Exception) as got:
        mv.call_validator(key, index, {})
    assert_exception_correct(got.value, expected)


def test_set_call_prefix_validator_fail():
    _call_prefix_validator_fail([
        MetadataValidator('key1', prefix_validators=[_noop]),
        MetadataValidator('key2', prefix_validators=[_noop])],
        'key3', 0, 'key3stuff', ValueError('No prefix validators for prefix key3'))
    _call_prefix_validator_fail([  # exact match required
        MetadataValidator('key1', prefix_validators=[_noop]),
        MetadataValidator('key2', prefix_validators=[_noop]),
        MetadataValidator('key', prefix_validators=[_noop])],
        'key3', 0, 'key3stuff', ValueError('No prefix validators for prefix key3'))
    _call_prefix_validator_fail([
        MetadataValidator('key1', prefix_validators=[_noop]),
        MetadataValidator('key2', prefix_validators=[_noop]),
        MetadataValidator('key3', prefix_validators=[_noop])],
        'key', 0, 'key3stuff', ValueError('No prefix validators for prefix key'))
    _call_prefix_validator_fail([
        MetadataValidator('key1', prefix_validators=[_noop]),
        MetadataValidator('key2', prefix_validators=[_noop, _noop])],
        'key2', 2, 'key2stuff',
        IndexError('Requested validator index 2 for prefix key2 but maximum index is 1'))


def _call_prefix_validator_fail(vals, prefix, index, key, expected):
    mv = MetadataValidatorSet(vals)
    with raises(Exception) as got:
        mv.call_prefix_validator(prefix, index, key, {})
    assert_exception_correct(got.value, expected)

def test_set_validate_metadata_return_errors():
    _validate_metadata_errors(
        [MetadataValidator('key1', [_noop]),
         MetadataValidator('key2', [_noop]),
         MetadataValidator('key', prefix_validators=[_noop3])],
        {'key1': 'a', 'key2': 'b', 'kex': 'c'},
        'kex', 'Cannot validate controlled field "kex", no matching validator found')
    _validate_metadata_errors(
        [MetadataValidator('key1', [_noop]), MetadataValidator('key2', [_noop])],
        {'key1': 'a', 'key2': 'b', 'key3': 'c'},
        'key3', 'Cannot validate controlled field "key3", no matching validator found')
    _validate_metadata_errors(
        [MetadataValidator('keyx', prefix_validators=[_noop3])],
        {'keyx1': 'a', 'keyx2': 'b', 'key': 'c'},
        'key', 'Cannot validate controlled field "key", no matching validator found')
    _validate_metadata_errors(
        [MetadataValidator('key1', [_noop]),
         MetadataValidator('key2', [_noop]),
         MetadataValidator('key3', [_noop, lambda _, __: 'oh poop'])],
        {'key1': 'a', 'key2': 'b', 'key3': 'c'},
        'key3', 'Key key3: oh poop')
    _validate_metadata_errors(
        [MetadataValidator('key1', prefix_validators=[_noop3]),
         MetadataValidator('key2', prefix_validators=[_noop3]),
         MetadataValidator('key3', prefix_validators=[_noop3, lambda _, __, ___: 'oh poop'])],
        {'key1stuff': 'a', 'key2': 'b', 'key3yay': 'c'},
        'key3yay', 'Prefix validator key3, key key3yay: oh poop')

def test_set_validate_metadata_return_errors_with_subkey():
    _validate_metadata_errors(
        [MetadataValidator('key1', [_noop]),
         MetadataValidator('key2', [_noop]),
         MetadataValidator('key3', [_noop, lambda _, __: {'subkey': 'somekey','message':'oh poop'}])],
        {'key1': 'a', 'key2': 'b', 'key3': 'c'},
        'key3', 'Key key3: oh poop', 'somekey')

def _validate_metadata_errors(vals, meta, expected_key, expected_dev_message, expected_subkey=None):
    mv = MetadataValidatorSet(vals)
    # with raises(Exception) as got:
    errors = mv.validate_metadata(meta, return_error_detail=True)
    assert len(errors) == 1
    assert str(errors[0]['key']) == str(expected_key)
    assert str(errors[0]['dev_message']) == str(expected_dev_message)
    assert str(errors[0]['subkey']) == str(expected_subkey)


def test_set_validate_metadata_fail():
    _validate_metadata_fail(
        [MetadataValidator('key1', [_noop]), MetadataValidator('key2', [_noop])],
        [],
        ValueError('metadata must be a dict'))
    _validate_metadata_fail(
        [MetadataValidator('key1', [_noop]),
         MetadataValidator('key2', [_noop]),
         MetadataValidator('key', prefix_validators=[_noop3])],
        {'key1': 'a', 'key2': 'b', 'kex': 'c'},
        MetadataValidationError('No validator available for metadata key kex'))
    _validate_metadata_fail(
        [MetadataValidator('key1', [_noop]), MetadataValidator('key2', [_noop])],
        {'key1': 'a', 'key2': 'b', 'key3': 'c'},
        MetadataValidationError('No validator available for metadata key key3'))
    _validate_metadata_fail(
        [MetadataValidator('keyx', prefix_validators=[_noop3])],
        {'keyx1': 'a', 'keyx2': 'b', 'key': 'c'},
        MetadataValidationError('No validator available for metadata key key'))
    _validate_metadata_fail(
        [MetadataValidator('key1', [_noop]),
         MetadataValidator('key2', [_noop]),
         MetadataValidator('key3', [_noop, lambda _, __: 'oh poop'])],
        {'key1': 'a', 'key2': 'b', 'key3': 'c'},
        MetadataValidationError('Key key3: oh poop'))
    _validate_metadata_fail(
        [MetadataValidator('key1', prefix_validators=[_noop3]),
         MetadataValidator('key2', prefix_validators=[_noop3]),
         MetadataValidator('key3', prefix_validators=[_noop3, lambda _, __, ___: 'oh poop'])],
        {'key1stuff': 'a', 'key2': 'b', 'key3yay': 'c'},
        MetadataValidationError('Prefix validator key3, key key3yay: oh poop'))


def _validate_metadata_fail(vals, meta, expected):
    mv = MetadataValidatorSet(vals)
    with raises(Exception) as got:
        mv.validate_metadata(meta)
    assert_exception_correct(got.value, expected)
