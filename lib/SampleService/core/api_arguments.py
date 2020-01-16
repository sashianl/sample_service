'''
Contains helper functions for translating between the SDK API and the core Samples code.
'''


from uuid import UUID
from typing import Dict, Any, Optional, Tuple, cast as _cast
import datetime

from SampleService.core.core_types import PrimitiveType
from SampleService.core.sample import Sample, SampleNode as _SampleNode, SavedSample
from SampleService.core.sample import SubSampleType as _SubSampleType
from SampleService.core.acls import SampleACLOwnerless, SampleACL
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.errors import IllegalParameterError as _IllegalParameterError
from SampleService.core.errors import MissingParameterError as _MissingParameterError

ID = 'id'
''' The ID of a sample. '''


def get_id_from_object(obj: Dict[str, Any], required=False) -> Optional[UUID]:
    '''
    Given a dict, get a sample ID from the dict if it exists, using the key 'id'.

    If None or an empty dict is passed to the method, None is returned.

    :param obj: The dict wherein the ID can be found.
    :param required: If no ID is present, throw an exception.
    :returns: The ID, if it exists, or None.
    :raises MissingParameterError: If the ID is required but not present.
    :raises IllegalParameterError: If the ID is provided but is invalid.
    '''
    id_ = None
    if required and (not obj or not obj.get(ID)):
        raise _MissingParameterError('Sample ID')
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


def create_sample_params(params: Dict[str, Any]) -> Tuple[Sample, Optional[UUID], Optional[int]]:
    '''
    Process the input from the create_sample API call and translate it into standard types.

    :param params: The unmarshalled JSON recieved from the API as part of the create_sample
        call.
    :returns: A tuple of the sample to save, the UUID of the sample for which a new version should
        be created or None if an entirely new sample should be created, and the previous version
        of the sample expected when saving a new version.
    :raises IllegalParameterError: if any of the arguments are illegal.
    '''
    _check_params(params)
    if type(params.get('sample')) != dict:
        raise _IllegalParameterError('params must contain sample key that maps to a structure')
    s = params['sample']
    if type(s.get('node_tree')) != list:
        raise _IllegalParameterError('sample node tree must be present and a list')
    if s.get('name') is not None and type(s.get('name')) != str:
        raise _IllegalParameterError('sample name must be omitted or a string')
    nodes = []
    for i, n in enumerate(s['node_tree']):
        if type(n) != dict:
            raise _IllegalParameterError(f'Node at index {i} is not a structure')
        if type(n.get('id')) != str:
            raise _IllegalParameterError(
                f'Node at index {i} must have an id key that maps to a string')
        try:
            type_ = _SubSampleType(n.get('type'))
        except ValueError:
            raise _IllegalParameterError(
                f'Node at index {i} has an invalid sample type: {n.get("type")}')
        if n.get('parent') and type(n.get('parent')) != str:
            raise _IllegalParameterError(
                f'Node at index {i} has a parent entry that is not a string')
        nodes.append(_SampleNode(
            n.get('id'),
            type_,
            n.get('parent'),
            _check_meta(n.get('meta_controlled'), i, 'controlled metadata'),
            _check_meta(n.get('meta_user'), i, 'user metadata')))

    id_ = get_id_from_object(s)

    pv = params.get('prior_version')
    if pv is not None and type(pv) != int:
        raise _IllegalParameterError('prior_version must be an integer if supplied')
    s = Sample(nodes, s.get('name'))
    return (s, id_, pv)


def _check_meta(m, index, name) -> Optional[Dict[str, Dict[str, PrimitiveType]]]:
    if not m:
        return None
    if type(m) != dict:
        raise _IllegalParameterError(f"Node at index {index}'s {name} entry must be a mapping")
    # since this is coming from JSON we assume keys are strings
    for k1 in m:
        if type(m[k1]) != dict:
            raise _IllegalParameterError(f"Node at index {index}'s {name} entry does " +
                                         f"not have a dict as a value at key {k1}")
        for k2 in m[k1]:
            v = m[k1][k2]
            if type(v) != str and type(v) != int and type(v) != float and type(v) != bool:
                raise _IllegalParameterError(
                    f"Node at index {index}'s {name} entry does " +
                    f"not have a primitive type as the value at {k1}/{k2}")
    return m


def _check_params(params):
    if params is None:
        raise ValueError('params cannot be None')


def get_version_from_object(params: Dict[str, Any]) -> Optional[int]:
    '''
    Given a dict, get a sample version from the dict if it exists, using the key 'version'.

    :param params: The unmarshalled JSON recieved from the API as part of the API call.
    :returns: the version or None if no version was provided.
    :raises IllegalParameterError: if the version is not an integer or < 1.
    '''
    _check_params(params)
    ver = params.get('version')
    if ver is not None and (type(ver) != int or ver < 1):
        raise _IllegalParameterError(f'Illegal version argument: {ver}')
    return ver


def get_sample_address_from_object(params: Dict[str, Any]) -> Tuple[UUID, Optional[int]]:
    '''
    Given a dict, get a sample ID and version from the dict. The sample ID is required but
    the version is not. The keys 'id' and 'version' are used.

    :param params: The unmarshalled JSON recieved from the API as part of the API call.
    :returns: A tuple containing the ID and the version or None if no version was provided.
    :raises MissingParameterError: If the ID is missing.
    :raises IllegalParameterError: if the ID is malformed or if the version is not an
        integer or < 1.
    '''
    return (_cast(UUID, get_id_from_object(params, required=True)),
            get_version_from_object(params))


def sample_to_dict(sample: SavedSample) -> Dict[str, Any]:
    '''
    Convert a sample to a JSONable structure to return to the SDK API.

    :param sample: The sample to convert.
    :return: The sample as a dict.
    '''
    nodes = [{'id': n.name,
              'type': n.type.value,
              'parent': n.parent,
              'meta_controlled': _unfreeze_meta(n.controlled_metadata),
              'meta_user': _unfreeze_meta(n.uncontrolled_metadata)
              }
             for n in _not_falsy(sample, 'sample').nodes]
    return {'id': str(sample.id),
            'user': sample.user,
            'name': sample.name,
            'node_tree': nodes,
            'save_date': datetime_to_epochmilliseconds(sample.savetime),
            'version': sample.version
            }


def _unfreeze_meta(m):
    ret = {}
    for k in m:
        ret[k] = {ik: m[k][ik] for ik in m[k]}
    return ret


def acls_to_dict(acls: SampleACL) -> Dict[str, Any]:
    '''
    Convert sample ACLs to a JSONable structure to return to the SDK API.

    :param acls: The ACLs to convert.
    :return: the ACLs as a dict.
    '''
    return {'owner': _not_falsy(acls, 'acls').owner,
            'admin': acls.admin,
            'write': acls.write,
            'read': acls.read,
            }


def acls_from_dict(d: Dict[str, Any]) -> SampleACLOwnerless:
    '''
    Given a dict, create a SampleACLOwnerless object from the contents of the acls key.

    :param params: The dict containing the ACLS.
    :returns: the ACLs.
    :raises IllegalParameterError: if any of the arguments are illegal.
    '''
    _not_falsy(d, 'd')
    if d.get('acls') is None or type(d['acls']) != dict:
        raise _IllegalParameterError('ACLs must be supplied in the acls key and must be a mapping')
    acls = d['acls']
    _check_acl(acls, 'admin')
    _check_acl(acls, 'write')
    _check_acl(acls, 'read')

    return SampleACLOwnerless(acls.get('admin'), acls.get('write'), acls.get('read'))


def _check_acl(acls, type_):
    if acls.get(type_) is not None:
        acl = acls[type_]
        if not type(acl) == list:
            raise _IllegalParameterError(f'{type_} ACL must be a list')
        for i, item, in enumerate(acl):
            if not type(item) == str:
                raise _IllegalParameterError(f'Index {i} of {type_} ACL does not contain a string')
