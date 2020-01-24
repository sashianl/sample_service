'''
Contains metadata validation callable builder functions for the Sample service.

The builder functions are expected to take a dict of configuration parameters
from which they can configure themselves and return a validation callable.

Validation callables must accept a metadata value for their key, a dict where the keys are
strings and the values are strings, integers, floats, or booleans. A non-None return value
indicates the metadata failed validation.
For error that are not under control of the user, any other appropriate error should be thrown.

If an error is not thrown, and a falsy value is returned, the validation succeeds.
'''

from typing import Dict, cast as _cast
from SampleService.core.core_types import PrimitiveType


def noop(d: Dict[str, str]):
    '''
    Build a validation callable that allows any value for the metadata key.
    :params d: The configuration parameters for the callable. Unused.
    '''
    return lambda _: None


def string(d: Dict[str, str]):
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

    keys = d.get('keys')
    required = d.get('required')
    if keys:
        if type(keys) == str:
            keys = [keys]
        elif type(keys) != list:
            raise ValueError('keys parameter must be a string or list')
        for i, k in enumerate(keys):
            if type(k) != str:
                raise ValueError(f'keys parameter contains a non-string entry at index {i}')

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
