'''
Contains helper functions for translating between the SDK API and the core Samples code.
'''

# TODO TEST

from uuid import UUID
from typing import Dict, Any, Optional
import datetime

from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.errors import IllegalParameterError as _IllegalParameterError

ID = 'id'
''' The ID of a sample. '''


def get_id_from_object(obj: Dict[str, Any]) -> Optional[UUID]:
    '''
    Given a dict, get a sample ID from the dict if it exists, using the key 'id'.

    If None or an empty dict is passed to the method, None is returned.

    :params obj: The dict wherein the ID can be found.
    :returns: The ID, if it exists, or None.
    :raises IllegalParameterError: If the ID is provided but is invalid.
    '''
    id_ = None
    if obj and obj.get(ID):
        if type(obj[ID]) != str:
            raise _IllegalParameterError(f'Sample ID {obj[ID]} must be a UUID string')
        try:
            id_ = UUID(obj[ID])
        except ValueError as _:  # noqa F841
            raise _IllegalParameterError(f'Sample ID {obj[ID]} must be a UUID string')
    return id_


def datetime_to_epochmilliseconds(d: datetime.datetime) -> int:
    '''
    Convert a datetime object to epoch milliseconds.

    :param d: The datetime.
    :returns: The date in epoch milliseconds.
    '''
    return round(_not_falsy(d, 'd').timestamp() * 1000)
