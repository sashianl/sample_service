from pytest import raises
from core.test_utils import assert_exception_correct

from SampleService.core.validator import builtin


def test_noop():
    # not much to test here.
    n = builtin.noop({})

    assert n('key', {}) is None


def test_noop_fail_bad_input():
    _noop_fail_build(None, ValueError('d must be a dict'))
    _noop_fail_build([], ValueError('d must be a dict'))
    _noop_fail_build({'key4': 'a', 'key86': 'a', 'key23': 'b', 'key6': 'c', 'key3': 'd'},
                     ValueError('Unexpected configuration parameter: key23'))


def _noop_fail_build(cfg, expected):
    with raises(Exception) as got:
        builtin.noop(cfg)
    assert_exception_correct(got.value, expected)


def test_string_general():
    sl = builtin.string({'max-len': 2})
    assert sl('key', {
        'fo': 'b',
        'e': 'fb',
        'a': True,
        'b': 1111111111,
        'c': 1.23456789}) is None


def test_string_single_keys():
    sl = builtin.string({'keys': 'whee', 'required': False})
    assert sl('key', {
        'fo': 'b',
        'e': 'fb',
        'whee': 'whooooooooooooo',
        'a': True,
        'b': 1111111111,
        'c': 1.23456789}) is None


def test_string_multiple_keys_max_len():
    sl = builtin.string({'keys': ['whee', 'whoo', 'whoa'], 'max-len': 9})
    # missing whoo key
    assert sl('key', {
        'fo': 'b',
        'e': 'fb',
        'whee': 'whoo',
        'a': True,
        'b': 1111111111,
        'whoa': 'free mind',
        'c': 1.23456789}) is None


def test_string_multiple_keys_required():
    sl = builtin.string({'keys': ['whee', 'whoo', 'whoa'], 'required': True})
    assert sl('key', {
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
    _string_fail_construct({}, ValueError(
        'If the keys parameter is not specified, max-len must be specified'))
    _string_fail_construct({'keys': 'foo', 'foo': 'bar', 'max-len': 25}, ValueError(
        'Unexpected configuration parameter: foo'))
    _string_fail_construct({'max-len': 'shazzbat'}, ValueError('max-len must be an integer'))
    _string_fail_construct({'max-len': '0'}, ValueError('max-len must be > 0'))
    _string_fail_construct({'keys': 356}, ValueError('keys parameter must be a string or list'))
    _string_fail_construct({'keys': ['foo', 'bar', 356, 'baz']}, ValueError(
        'keys parameter contains a non-string entry at index 2'))


def _string_fail_construct(d, expected):
    with raises(Exception) as got:
        builtin.string(d)
    assert_exception_correct(got.value, expected)


def test_string_validate_fail_bad_metadata_values():
    _string_validate_fail(
        {'max-len': 2}, {'foo': 'ba', 'ba': 'f'},
        'Metadata contains key longer than max length of 2')
    _string_validate_fail(
        {'max-len': 4}, {'foo': 'ba', 'ba': 'fudge'},
        'Metadata value at key ba is longer than max length of 4')
    _string_validate_fail(
        {'keys': ['foo', 'bar'], 'required': True}, {'foo': 'ba', 'ba': 'fudge'},
        'Required key bar is missing')
    _string_validate_fail(
        {'keys': 'foo'}, {'foo': ['a'], 'ba': 'fudge'},
        'Metadata value at key foo is not a string')
    _string_validate_fail(
        {'keys': ['foo', 'bar'], 'required': True, 'max-len': 6}, {'foo': 'ba', 'bar': 'fudgera'},
        'Metadata value at key bar is longer than max length of 6')


def _string_validate_fail(cfg, meta, expected):
    assert builtin.string(cfg)('key', meta)['message'] == expected


def test_enum():
    en = builtin.enum({'allowed-values': ['a', 1, 3.1, True]})

    assert en('key', {'z': 'a', 'w': 1, 'x': 3.1, 'y': True}) is None


def test_enum_with_single_key():
    en = builtin.enum({'keys': '4', 'allowed-values': ['b', 2, 3.2, False]})

    assert en('key', {'4': 2}) is None


def test_enum_with_keys():
    en = builtin.enum({'keys': ['1', '2', '3', '4'], 'allowed-values': ['b', 2, 3.2, False]})

    assert en('key', {'1': 'b', '2': 2, '3': 3.2, '4': False}) is None


def test_enum_build_fail():
    _enum_build_fail(None, ValueError('d must be a dict'))
    _enum_build_fail(['foo'], ValueError('d must be a dict'))
    _enum_build_fail({'keys': ['foo']}, ValueError('allowed-values is a required parameter'))
    _enum_build_fail({'keys': ['foo'], 'allowed-values': [], 'bar': 'bat'},
                     ValueError('Unexpected configuration parameter: bar'))
    _enum_build_fail({'allowed-values': {'a': 'b'}}, ValueError(
        'allowed-values parameter must be a list'))
    _enum_build_fail({'allowed-values': ['a', True, []]}, ValueError(
        'allowed-values parameter contains a non-primitive type entry at index 2'))
    _enum_build_fail({'allowed-values': ['a', True, 1, {}]}, ValueError(
        'allowed-values parameter contains a non-primitive type entry at index 3'))
    _enum_build_fail({'keys': {'a': 'b'}, 'allowed-values': [1]}, ValueError(
        'keys parameter must be a string or list'))
    _enum_build_fail({'keys': ['a', 1], 'allowed-values': [1]}, ValueError(
        'keys parameter contains a non-string entry at index 1'))


def test_enum_validate_fail():
    _enum_validate_fail(
        {'allowed-values': ['a', 1]}, {'foo': 'a', 'bar': 1, 'baz': 'whee'},
        'Metadata value at key baz is not in the allowed list of values')
    _enum_validate_fail(
        {'allowed-values': ['a', 1]}, {'foo': 'a', 'bar': 1, 'bat': None},
        'Metadata value at key bat is not in the allowed list of values')
    _enum_validate_fail(
        {'allowed-values': ['a', 1], 'keys': ['bat']}, {'foo': 'b', 'bar': 'b', 'bat': 'whoo'},
        'Metadata value at key bat is not in the allowed list of values')
    _enum_validate_fail(
        {'allowed-values': ['a', 1], 'keys': ['bat']}, {'foo': 'b', 'bar': 'b', 'bat': None},
        'Metadata value at key bat is not in the allowed list of values')


def _enum_build_fail(cfg, expected):
    with raises(Exception) as got:
        builtin.enum(cfg)
    assert_exception_correct(got.value, expected)


def _enum_validate_fail(cfg, meta, expected):
    assert builtin.enum(cfg)('key', meta)['message'] == expected


def test_units():
    _units_good({'key': 'u', 'units': 'N'}, {'u': 'lb * ft/ s^2'})
    _units_good({'key': 'y', 'units': 'degF'}, {'y': 'K', 'u': 'not a unit'})
    _units_good({'key': 'u', 'units': '(lb * ft^2) / (s^3 * A^2)'}, {'u': 'ohm'})
    _units_good({'key': 'y', 'units': 'cells'}, {'y': "cell"})
    _units_good({'key': 'u', 'units': 'cells / gram'}, {'u': "cells / g"})
    _units_good({'key': 'y', 'units': 'percent'}, {'y': "percent"})

def _units_good(cfg, meta):
    assert builtin.units(cfg)('key', meta) is None


def test_units_build_fail():
    _units_build_fail(None, ValueError('d must be a dict'))
    _units_build_fail([], ValueError('d must be a dict'))
    _units_build_fail({}, ValueError('key is a required parameter'))
    _units_build_fail({'key': None}, ValueError('key is a required parameter'))
    _units_build_fail({'key': ['foo']}, ValueError('the key parameter must be a string'))
    _units_build_fail({'key': 'foo'}, ValueError('units is a required parameter'))
    _units_build_fail({'key': 'foo', 'units': None}, ValueError('units is a required parameter'))
    _units_build_fail({'key': 'foo', 'units': 'm', 'whee': 'whoo'},
                      ValueError('Unexpected configuration parameter: whee'))
    _units_build_fail(
        {'key': 'foo', 'units': ['N']}, ValueError('the units parameter must be a string'))
    _units_build_fail(
        {'key': 'foo', 'units': 'not a unit'}, ValueError(
            "unable to parse units 'not a unit': undefined unit: not"))
    _units_build_fail(
        {'key': 'foo', 'units': 'm ^'}, ValueError(
            'unable to parse units \'m ^\': syntax error: missing unary operator "**"'))


def test_units_validate_fail():
    _units_validate_fail({'key': 'u', 'units': 'm'}, {}, 'metadata value key u is required')
    _units_validate_fail({'key': 'u', 'units': 'm'}, {'u': None},
                         'metadata value key u is required')
    _units_validate_fail({'key': 'u', 'units': 'm'}, {'u': ['ft']},
                         'metadata value key u must be a string')
    _units_validate_fail({'key': 'u', 'units': 'm'}, {'u': 'no_units_here'},
                         "unable to parse units 'm' at key u: undefined unit: no_units_here")
    _units_validate_fail(
        {'key': 'u', 'units': 'm'}, {'u': 'ft / '},
        'unable to parse units \'m\' at key u: syntax error: missing unary operator "/"')
    _units_validate_fail(
        {'key': 'u', 'units': 'm'}, {'u': 's'},
        "Units at key u, 's', are not equivalent to required units, 'm': Cannot convert " +
        "from 'second' ([time]) to 'meter' ([length])")


def _units_build_fail(cfg, expected):
    with raises(Exception) as got:
        builtin.units(cfg)
    assert_exception_correct(got.value, expected)


def _units_validate_fail(cfg, meta, expected):
    assert builtin.units(cfg)('key', meta)['message'] == expected


def test_number():
    _number_success({}, {'whee': 1, '2': 2.3, '3': 10312.5677, '4': 682})
    # null type should allow floats
    _number_success({'type': None}, {'whee': 1, '2': 2.3, '3': 10312.5677, '4': 682})
    # float type should allow floats
    _number_success({'type': 'float'}, {'whee': 1, '2': 2.3, '3': 10312.5677, '4': 682})
    # required should be ignored
    _number_success({'required': True}, {'whee': 1, '2': 2.3, '3': 10312.567, '4': 682, '5': None})
    # test none is ok and ints are ok
    _number_success({'type': 'int'}, {'whee': 1, '2': 2, '3': 10312, '4': 682, '5': None})


def test_number_with_ranges():
    _number_success({'gt': 4.6}, {'whee': 4.6000000000000001, '2': 4.7, '3': 10312.5677, '4': 682})
    _number_success({'gt': 4}, {'whee': 5, '2': 6, '3': 10312.5677, '4': 682})
    _number_success({'gte': 4.6}, {'whee': 4.6, '2': 4.7, '3': 10312.5677, '4': 682})
    _number_success({'gte': 4}, {'whee': 4, '2': 4.7, '3': 10312.5677, '4': 682})

    _number_success({'lt': 10312.5678}, {'whee': 4.600000001, '2': 4.7, '3': 10312.5677, '4': 682})
    _number_success({'lt': 10312}, {'whee': 5, '2': 6, '3': 10311, '4': 682})
    _number_success({'lte': 10312.5678}, {'whee': 4.6, '2': 4.7, '3': 10312.5678, '4': 682})
    _number_success({'lte': 10312}, {'whee': 4, '2': 4.7, '3': 10312, '4': 682})

    _number_success({'lt': 10312.5678, 'gte': 4}, {'whee': 4, '2': 4.7, '3': 10312.5677, '4': 682})
    _number_success({'lte': 10.1, 'gt': 10}, {'whee': 10.1, '2': 10.0000000000001, '3': 10.01})


def test_number_with_keys():
    _number_success({'keys': 'whee'}, {'whee': 1, '2': 7.4, '3': True, '4': 'nan'})
    # test keys are not required
    _number_success({'keys': ['whee', '2', 'a']}, {'whee': 1, '2': 2.3, '3': True, '4': 'nan'})
    _number_success({'required': True, 'keys': ['whee', '2', '5']},
                    {'whee': 64, '2': 18.3, '3': 'foo', '4': False, '5': None})
    # test int only & None is ok
    _number_success({'type': 'int', 'keys': ['whee', '2', '5']},
                    {'whee': 1, '2': 2, '3': 10312.3, '4': 'bar', '5': None})


def test_number_with_keys_and_ranges():
    _number_success({'gt': 4.6, 'keys': ['whee', '2']},
                    {'whee': 4.6000000000000001, '2': 4.7, '3': 0, '4': 4})
    _number_success({'gt': 4, 'keys': ['whee', '2']},
                    {'whee': 5, '2': 6, '3': 4, '4': -12})
    _number_success({'gte': 4.6, 'keys': ['whee', '2']},
                    {'whee': 4.6, '2': 4.7, '3': 4.5999, '4': -300})
    _number_success({'gte': 4, 'keys': ['whee', '2']},
                    {'whee': 4, '2': 4.7, '3': 3, '4': 682})

    _number_success({'lt': 10312.5678, 'keys': ['whee', '3']},
                    {'whee': 4.600000001, '2': 10321.5678, '3': 10312.5677, '4': 100000})
    _number_success({'lt': 10312, 'keys': ['whee', '3']},
                    {'whee': 5, '2': 10312, '3': 10311, '4': 777777})
    _number_success({'lte': 10312.5678, 'keys': ['whee', '3']},
                    {'whee': 4.6, '2': 10312.5679, '3': 10312.5678, '4': 1000000000})
    _number_success({'lte': 10312, 'keys': ['whee', '3']},
                    {'whee': 4, '2': 10313, '3': 10312, '4': 1000000})

    _number_success({'lt': 10312.5678, 'gte': 4, 'keys': ['whee', '2', '3']},
                    {'whee': 4, '2': 4.7, '3': 10312.5677, '4': 3, '5': 10312.5679})
    _number_success({'lte': 10.1, 'gt': 10, 'keys': ['whee', '2', '3']},
                    {'whee': 10.1, '2': 10.0000000000001, '3': 10.01, '4': 10.11, '5': 10})


def _number_success(cfg, params):
    assert builtin.number(cfg)('key', params) is None


def test_number_build_fail():
    _number_build_fail(None, ValueError('d must be a dict'))
    _number_build_fail([], ValueError('d must be a dict'))
    _number_build_fail({'keys': {'a': 'b'}}, ValueError('keys parameter must be a string or list'))
    _number_build_fail({'keys': ['a', 'b', 7]}, ValueError(
        'keys parameter contains a non-string entry at index 2'))
    _number_build_fail({'type': 'foo'}, ValueError('Illegal value for type parameter: foo'))
    _number_build_fail({'type': 'int', 'keys': ['bar'], 'required': True,
                        'gt': 1, 'lte': 4, 'fakekey': 'yay'},
                       ValueError('Unexpected configuration parameter: fakekey'))
    _number_build_fail({'type': []}, ValueError('Illegal value for type parameter: []'))

    for r in ['gt', 'gte', 'lt', 'lte']:
        _number_build_fail({r: 'foo'}, ValueError(f'Value for {r} parameter is not a number'))
        _number_build_fail({r: []}, ValueError(f'Value for {r} parameter is not a number'))

    _number_build_fail({'gt': 7, 'gte': 8}, ValueError('Cannot specify both gt and gte'))
    _number_build_fail({'lt': 5, 'lte': 89}, ValueError('Cannot specify both lt and lte'))


def test_number_validate_fails():
    _number_validate_fail(
        {}, {'a': 'foo'}, 'Metadata value at key a is not an accepted number type')
    _number_validate_fail(
        {}, {'a': {}}, 'Metadata value at key a is not an accepted number type')
    _number_validate_fail(
        {'type': 'int'}, {'a': 3.1}, 'Metadata value at key a is not an accepted number type')
    _number_validate_fail(
        {'gt': 7, 'lt': 9}, {'a': 7},
        'Metadata value at key a is not within the range (7, 9)')
    _number_validate_fail(
        {'gt': 7, 'lt': 9}, {'a': 9},
        'Metadata value at key a is not within the range (7, 9)')
    _number_validate_fail(
        {'gte': 0, 'lte': 100}, {'a': -0.00000000000000000001},
        'Metadata value at key a is not within the range [0, 100]')
    _number_validate_fail(
        {'gte': 0, 'lte': 100}, {'a': 100.01},
        'Metadata value at key a is not within the range [0, 100]')
    # 0 is falsy, which is dumb and could cause problems here
    _number_validate_fail(
        {'gte': 0.1, 'lte': 100}, {'a': 0},
        'Metadata value at key a is not within the range [0.1, 100]')


def test_number_validate_with_keys_fails():
    _number_validate_fail(
        {'required': True, 'keys': ['a', 'b']}, {'a': 1},
        'Required key b is missing')
    _number_validate_fail(
        {'keys': 'a'}, {'a': True}, 'Metadata value at key a is not an accepted number type')
    _number_validate_fail(
        {'type': 'int', 'keys': 'a'}, {'a': 3.1},
        'Metadata value at key a is not an accepted number type')
    _number_validate_fail(
        {'keys': 'a', 'gt': 7, 'lt': 9}, {'a': 7},
        'Metadata value at key a is not within the range (7, 9)')
    _number_validate_fail(
        {'keys': 'a', 'gt': 7, 'lt': 9}, {'a': 9},
        'Metadata value at key a is not within the range (7, 9)')
    _number_validate_fail(
        {'keys': 'a', 'gte': 0, 'lte': 100}, {'a': -0.00000000000000000001},
        'Metadata value at key a is not within the range [0, 100]')
    _number_validate_fail(
        {'keys': 'a', 'gte': 0, 'lte': 100}, {'a': 100.01},
        'Metadata value at key a is not within the range [0, 100]')
    # 0 is falsy, which is dumb and could cause problems here
    _number_validate_fail(
        {'gte': 0.1, 'lte': 100, 'keys': 'a'}, {'a': 0},
        'Metadata value at key a is not within the range [0.1, 100]')


def _number_build_fail(cfg, expected):
    with raises(Exception) as got:
        builtin.number(cfg)
    assert_exception_correct(got.value, expected)


def _number_validate_fail(cfg, meta, expected):
    assert builtin.number(cfg)('key', meta)['message'] == expected

def test_ontology_has_ancestor():
  _ontology_has_ancestor_success(
      {'ontology': 'envo_ontology', 'ancestor_term':'ENVO:00010483' },
      {'Material': 'ENVO:00002041', 'ENVO:Material': 'ENVO:00002006'})

def _ontology_has_ancestor_success(cfg, meta):
    assert builtin.ontology_has_ancestor(cfg)('key', meta) is None

def test_ontology_has_ancestor_build_fail():
    _ontology_has_ancestor_build_fail(None, ValueError('d must be a dict'))
    _ontology_has_ancestor_build_fail({'whee': 'whoo'}, 
        ValueError('Unexpected configuration parameter: whee'))
    _ontology_has_ancestor_build_fail({'ontology': None}, 
        ValueError('ontology is a required parameter'))
    _ontology_has_ancestor_build_fail({'ontology': ['foo']}, 
        ValueError('ontology must be a string'))
    _ontology_has_ancestor_build_fail({'ontology': 'foo', 'ancestor_term': None}, 
        ValueError('ancestor_term is a required parameter'))
    _ontology_has_ancestor_build_fail({'ontology': 'foo', 'ancestor_term': ['foo']}, 
        ValueError('ancestor_term must be a string'))
    _ontology_has_ancestor_build_fail(
        {'ontology': 'foo', 'ancestor_term':'ENVO:00010483'},
        ValueError('ontology foo doesn\'t exist'))
    _ontology_has_ancestor_build_fail(
        {'ontology': 'envo_ontology', 'ancestor_term':'baz' },
        ValueError('ancestor_term baz is not found in envo_ontology'))

def _ontology_has_ancestor_build_fail(cfg, expected):
    with raises(Exception) as got:
        builtin.ontology_has_ancestor(cfg)
    assert_exception_correct(got.value, expected)

def test_ontology_has_ancestor_validate_fail():
    _ontology_has_ancestor_validate_fail(
        {'ontology': 'envo_ontology', 'ancestor_term':'ENVO:00010483' }, 
        {'a': None}, 'Metadata value at key a is None')
    _ontology_has_ancestor_validate_fail(
        {'ontology': 'envo_ontology', 'ancestor_term':'ENVO:00010483' }, 
        {'a': 'foo'}, 'Metadata value at key a does not have envo_ontology ancestor term ENVO:00010483')
    _ontology_has_ancestor_validate_fail(
        {'ontology': 'envo_ontology', 'ancestor_term':'ENVO:00002010' }, 
        {'a': 'ENVO:00002041'}, 'Metadata value at key a does not have envo_ontology ancestor term ENVO:00002010')

def _ontology_has_ancestor_validate_fail(cfg, meta, expected):
    assert builtin.ontology_has_ancestor(cfg)('key', meta)['message'] == expected
