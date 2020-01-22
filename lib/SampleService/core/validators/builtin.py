'''
Contains metadata validation callable builder functions for the Sample service.

The builder functions are expected to take a string -> string dict of configuration parameters
from which they can configure themselves and return a validation callable.

Validation callables must accept a metadata value for their key, a dict where the keys are
strings and the values are strings, integers, floats, or booleans. The return value, if any,
is ignored. On a validation failure that is caused by invalid metadata, the function must raise
SampleService.core.errors.MetadataValidationError to denote that the failure is user-caused.
For error that are not under control of the user, any other appropriate error should be thrown.

If an error is not thrown, the validation succeeds.
'''

from typing import Dict, cast as _cast
from SampleService.core.core_types import PrimitiveType
from SampleService.core.errors import MetadataValidationError


def noop(d: Dict[str, str]):
    '''
    Build a validation callable that allows any value for the metadata key.
    :params d: The configuration parameters for the callable. Unused.
    '''
    return lambda _: None


def string_length(d: Dict[str, str]):
    '''
    Build a validation callable that ensures all the keys and string values in the metadata
    value are less than a given length. Non-string values are ignored.

    :param d: the configuration map for the callable. Expects a max_len key that must be parseable
        to an integer greater than 0.
    :raises MetadataValidationError: if any keys' or values' length are greater than the
        maximum.
    '''
    if not d or 'max_len' not in d:
        raise ValueError('max_len parameter required')
    try:
        maxlen = int(d['max_len'])
    except ValueError:
        raise ValueError('max_len must be an integer')
    if maxlen < 1:
        raise ValueError('max_len must be > 0')

    def strlen(d1: Dict[str, PrimitiveType]):
        for k, v in d1.items():
            if len(k) > maxlen:
                raise MetadataValidationError(
                    f'Metadata contains key longer than max length of {maxlen}')
            if type(v) == str:
                if len(_cast(str, v)) > maxlen:
                    raise MetadataValidationError(
                        f'Metadata value at key {k} is longer than max length of {maxlen}')

    return strlen
