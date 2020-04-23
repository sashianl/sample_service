'''
Contains helper functions for translating between the SDK API and the core Samples code.
'''


from uuid import UUID
from typing import Dict, Any, Optional, Tuple, List, Callable, cast as _cast
import datetime

from SampleService.core.core_types import PrimitiveType
from SampleService.core.sample import (
    Sample,
    SampleNode as _SampleNode,
    SavedSample,
    SampleAddress as _SampleAddress,
    SampleNodeAddress,
    SubSampleType as _SubSampleType
)
from SampleService.core.acls import SampleACLOwnerless, SampleACL, AdminPermission
from SampleService.core.user_lookup import KBaseUserLookup
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.errors import IllegalParameterError as _IllegalParameterError
from SampleService.core.errors import MissingParameterError as _MissingParameterError
from SampleService.core.errors import UnauthorizedError as _UnauthorizedError
from SampleService.core.user import UserID as _UserID
from SampleService.core.workspace import DataUnitID, UPA as _UPA

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


def get_datetime_from_epochmilliseconds_in_object(
        params: Dict[str, Any], key: str) -> Optional[datetime.datetime]:
    '''
    Get a non-naive datetime from a field in an object where the value is epoch milliseconds.

    :param params: the object.
    :param key: the key in the object for which the value is an epoch millisecond timestamp.
    :returns: the datatime or None if none was provided.
    :raises IllegalParameterError: if the timestamp is missing or illegal.
    '''
    t = _check_params(params).get(key)
    if t is None:
        return None
    if type(t) != int:
        raise _IllegalParameterError(
            f"key '{key}' value of '{t}' is not a valid epoch millisecond timestamp")
    return datetime.datetime.fromtimestamp(t / 1000, tz=datetime.timezone.utc)


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
        mc = _check_meta(n.get('meta_controlled'), i, 'controlled metadata')
        mu = _check_meta(n.get('meta_user'), i, 'user metadata')
        try:
            nodes.append(_SampleNode(n.get('id'), type_, n.get('parent'), mc, mu))
            # already checked for the missing param error above, for id
        except _IllegalParameterError as e:
            raise _IllegalParameterError(
                f'Error for node at index {i}: ' + _cast(str, e.message)) from e

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
        if type(k1) != str:
            raise _IllegalParameterError(
                f"Node at index {index}'s {name} entry contains a non-string key")
        if type(m[k1]) != dict:
            raise _IllegalParameterError(f"Node at index {index}'s {name} entry does " +
                                         f"not have a dict as a value at key {k1}")
        for k2 in m[k1]:
            if type(k2) != str:
                raise _IllegalParameterError(f"Node at index {index}'s {name} entry contains " +
                                             f'a non-string key under key {k1}')
            v = m[k1][k2]
            if type(v) != str and type(v) != int and type(v) != float and type(v) != bool:
                raise _IllegalParameterError(
                    f"Node at index {index}'s {name} entry does " +
                    f"not have a primitive type as the value at {k1}/{k2}")
    return m


def _check_params(params):
    if params is None:
        raise ValueError('params cannot be None')
    return params


def get_version_from_object(params: Dict[str, Any], required: bool = False) -> Optional[int]:
    '''
    Given a dict, get a sample version from the dict if it exists, using the key 'version'.

    :param params: the unmarshalled JSON recieved from the API as part of the API call.
    :param required: if True, throw and exception if the version is not supplied.
    :returns: the version or None if no version was provided.
    :raises IllegalParameterError: if the version is not an integer or < 1.
    '''
    _check_params(params)
    ver = params.get('version')
    if ver is None and required:
        raise _IllegalParameterError('version is required')
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
              'meta_user': _unfreeze_meta(n.user_metadata)
              }
             for n in _not_falsy(sample, 'sample').nodes]
    return {ID: str(sample.id),
            'user': sample.user.id,
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
    return {'owner': _not_falsy(acls, 'acls').owner.id,
            'admin': tuple(u.id for u in acls.admin),
            'write': tuple(u.id for u in acls.write),
            'read': tuple(u.id for u in acls.read),
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

    return SampleACLOwnerless(
        _get_acl(acls, 'admin'),
        _get_acl(acls, 'write'),
        _get_acl(acls, 'read'))


def _get_acl(acls, type_):
    ret = []
    if acls.get(type_) is not None:
        acl = acls[type_]
        if not type(acl) == list:
            raise _IllegalParameterError(f'{type_} ACL must be a list')
        for i, item, in enumerate(acl):
            if not type(item) == str:
                raise _IllegalParameterError(f'Index {i} of {type_} ACL does not contain a string')
            ret.append(_UserID(item))
    return ret


def check_admin(
        user_lookup: KBaseUserLookup,
        token: str,
        perm: AdminPermission,
        method: str,
        log_fn: Callable[[str], None],
        as_user: str = None,
        skip_check: bool = False) -> bool:
    '''
    Check whether a user has admin privileges.
    The request is logged.

    :param user_lookup: the service to use to look up user information.
    :param token: the user's token.
    :param perm: the required administration permission.
    :param method: the method the user is trying to run. This is used in logging and error
      messages.
    :param logger: a function that logs information when called with a string.
    :param as_user: if the admin is impersonating another user, the username of that user.
    :param skip_check: Skip the administration permission check and return false.
    :returns: true if the user has the required administration permission, false if skip_check
        is true.
    :raises UnauthorizedError: if the user does not have the permission required.
    '''
    if skip_check:
        return False
    _not_falsy(method, 'method')
    _not_falsy(log_fn, 'log_fn')
    if _not_falsy(perm, 'perm') == AdminPermission.NONE:
        raise ValueError('what are you doing calling this method with no permission ' +
                         'requirement? That totally makes no sense. Get a brain moran')
    if as_user and perm != AdminPermission.FULL:
        raise ValueError('as_user is supplied, but permission is not FULL')
    p, user = _not_falsy(user_lookup, 'user_lookup').is_admin(_not_falsy(token, 'token'))
    if p < perm:
        err = (f'User {user} does not have the necessary administration ' +
               f'privileges to run method {method}')
        log_fn(err)
        raise _UnauthorizedError(err)
    log_fn(f'User {user} is running method {method} with administration permission {p.name}' +
           (f' as user {as_user}' if as_user else ''))
    return True


def get_static_key_metadata_params(params: Dict[str, Any]) -> Tuple[List[str], Optional[bool]]:
    '''
    Given a dict, extract the parameters to interrogate metadata key static metadata.

    :param params: The parameters.
    :returns: A tuple consisting of, firstly, the list of keys to interrogate, and secondly,
        a trinary value where False indicates that standard keys should be interrogated, None
        indicates that prefix keys should be interrogated but only exact matches should be
        considered, and True indicates that prefix keys should be interrogated but prefix matches
        should be included in the results.
    :raises IllegalParameterError: if any of the arguments are illegal.
    '''
    _check_params(params)
    keys = params.get('keys')
    if type(keys) != list:
        raise _IllegalParameterError('keys must be a list')
    for i, k in enumerate(_cast(List[Any], keys)):
        if type(k) != str:
            raise _IllegalParameterError(f'index {i} of keys is not a string')
    prefix = params.get('prefix')
    pre: Optional[bool]
    if not prefix:
        pre = False
    elif prefix == 1:
        pre = None
    elif prefix == 2:
        pre = True
    else:
        raise _IllegalParameterError(f'Unexpected value for prefix: {prefix}')
    return _cast(List[str], keys), pre


def create_data_link_params(params: Dict[str, Any]) -> Tuple[DataUnitID, SampleNodeAddress, bool]:
    '''
    Given a dict, extract the parameters to create parameters for creating a data link.

    Expected keys:
    id - sample id
    version - sample version
    node - sample node
    upa - workspace object UPA
    dataid - ID of the data within the workspace object
    update - whether the link should be updated
    The keys 'id' and 'version' are used.

    :param params: the parameters.
    :returns: a tuple consisting of:
        1) The data unit ID that is the target of the link,
        2) The sample node that is the target of the link,
        3) A boolean that indicates whether the link should be updated if it already exists.
    :raises IllegalParameterError: if any of the arguments are illegal.
    '''
    _check_params(params)
    sna = SampleNodeAddress(
        _SampleAddress(
            _cast(UUID, get_id_from_object(params, required=True)),
            _cast(int, get_version_from_object(params, required=True))),
        _cast(str, _check_string(params, 'node', True))
    )
    duid = DataUnitID(
        _UPA(_cast(str, _check_string(params, 'upa', True))),
        _check_string(params, 'dataid'))
    return (duid, sna, bool(params.get('update')))


def _check_string(params: Dict[str, Any], key: str, required=False) -> Optional[str]:
    v = params.get(key)
    if v is None and not required:
        return None
    if type(v) != str:
        raise _IllegalParameterError(f'{key} key is not a string as required')
    return _cast(str, v)
