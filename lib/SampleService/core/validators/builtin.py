'''
Contains metadata validation callable builder functions for the Sample service.

The builder functions are expected to take a dict of configuration parameters
from which they can configure themselves and return a validation callable.

Validation callables must accept a metadata value for their key, a dict where the keys are
strings and the values are strings, integers, floats, or booleans. A non-None return value
indicates the metadata failed validation.
For errors that are not under control of the user, any other appropriate exception should be
thrown.

If an exception is not thrown, and a falsy value is returned, the validation succeeds.
'''

from typing import Dict, Any, Callable, Optional, cast as _cast
from SampleService.core.core_types import PrimitiveType

# TODO float / int w/ ranges
# TODO units (pint)


def noop(d: Dict[str, Any]) -> Callable[[Dict[str, PrimitiveType]], Optional[str]]:
    '''
    Build a validation callable that allows any value for the metadata key.
    :params d: The configuration parameters for the callable. Unused.
    '''
    return lambda _: None


def string(d: Dict[str, Any]) -> Callable[[Dict[str, PrimitiveType]], Optional[str]]:
    '''
    Build a validation callable that performs string checking based on the following rules:

    If the 'keys' parameter is specified it must contain a string or a list of strings. The
    provided string(s) are used by the returned callable to query the metadata map.
    If any of the values for the provided keys are not strings, an error is returned. If the
    `max-len` parameter is provided, the value of which must be an integer, the values' lengths
    must be less than 'max-len'. If the 'required' parameter's value is truthy, an error is
    thrown if any of the keys in the 'keys' parameter do not exist in the map, athough the
    values may be None.

    If the 'keys' parameter is not provided, 'max-len' must be provided, in which case all
    the keys and string values in the metadata value map are checked against the max-value.
    Non-string values are ignored.

    :param d: the configuration map for the callable.
    :returns: a callable that validates metadata maps.
    '''
    if type(d) != dict:
        raise ValueError('d must be a dict')
    if 'max-len' not in d:
        maxlen = None
    else:
        try:
            maxlen = int(d['max-len'])
        except ValueError:
            raise ValueError('max-len must be an integer')
        if maxlen < 1:
            raise ValueError('max-len must be > 0')

    required = d.get('required')
    keys = _get_keys(d)
    if keys:

        def strlen(d1: Dict[str, PrimitiveType]):
            for k in keys:
                if required and k not in d1:
                    return f'Required key {k} is missing'
                v = d1.get(k)
                if v is not None and type(v) != str:
                    return f'Metadata value at key {k} is not a string'
                if v and maxlen and len(v) > maxlen:
                    return f'Metadata value at key {k} is longer than max length of {maxlen}'
    elif maxlen:
        def strlen(d1: Dict[str, PrimitiveType]):
            for k, v in d1.items():
                if len(k) > maxlen:
                    return f'Metadata contains key longer than max length of {maxlen}'
                if type(v) == str:
                    if len(_cast(str, v)) > maxlen:
                        return f'Metadata value at key {k} is longer than max length of {maxlen}'
    else:
        raise ValueError('If the keys parameter is not specified, max-len must be specified')
    return strlen


def enum(d: Dict[str, Any]) -> Callable[[Dict[str, PrimitiveType]], Optional[str]]:
    '''
    Build a validation callable that checks that values are one of a set of specified values.

    The 'allowed-values' parameter is required and is a list of the allowed values for the
    metadata values. Any primitive value is allowed. By default, all the metadata values will
    be checked against the allowed values.

    If the keys parameter is provided, it must be either a string or a list of strings. In this
    case, only the specified keys are checked.
    '''
    if type(d) != dict:
        raise ValueError('d must be a dict')
    allowed = d.get('allowed-values')
    if not allowed:
        raise ValueError('allowed-values is a required parameter')
    if type(allowed) != list:
        raise ValueError('allowed-values parameter must be a list')
    for i, a in enumerate(allowed):
        if _not_primitive(a):
            raise ValueError(
                f'allowed-values parameter contains a non-primitive type entry at index {i}')
    allowed = set(allowed)
    keys = _get_keys(d)
    if keys:

        def enumval(d1: Dict[str, PrimitiveType]):
            for k in keys:
                if d1.get(k) not in allowed:
                    return f'Metadata value at key {k} is not in the allowed list of values'
    else:

        def enumval(d1: Dict[str, PrimitiveType]):
            for k, v in d1.items():
                if v not in allowed:
                    return f'Metadata value at key {k} is not in the allowed list of values'
    return enumval


def _not_primitive(value):
    return (type(value) != str and type(value) != int and
            type(value) != float and type(value) != bool)


def _get_keys(d):
    keys = d.get('keys')
    if keys:
        if type(keys) == str:
            keys = [keys]
        elif type(keys) != list:
            raise ValueError('keys parameter must be a string or list')
        for i, k in enumerate(keys):
            if type(k) != str:
                raise ValueError(f'keys parameter contains a non-string entry at index {i}')
    return keys
