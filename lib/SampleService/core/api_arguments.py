'''
Contains helper functions for translating between the SDK API and the core Samples code.
'''

# TODO TEST

from uuid import UUID
from typing import Dict, Any, Optional

from SampleService.core.errors import IllegalParameterError as _IllegalParameterError

ID = 'id'
''' The ID of a sample. '''


def get_id_from_object(obj: Dict[str, Any]) -> Optional[UUID]:
    '''
    Given a dict, get a sample ID from the dict if it exists, using the key 'id'.

    :params obj: The dict wherein the ID can be found.
    :returns: The ID, if it exists.
    :raises IllegalParameterError: If the ID is provided but is invalid.
    '''
    id_ = None
    if obj.get(ID):
        if type(obj[ID]) != str:
            raise _IllegalParameterError('If a sample ID is provided it must be a UUID')
        try:
            id_ = UUID(obj[ID])
        except ValueError as _:  # noqa F841
            raise _IllegalParameterError(f'{obj[ID]} is not a valid sample ID')
    return id_
