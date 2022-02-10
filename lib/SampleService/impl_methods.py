'''
Module containing SampleService methods.

Use this module to write methods for use in SampleServiceImpl.py.
'''
from SampleService.core.api_translation import (
    acl_delta_from_dict as _acl_delta_from_dict,
    check_admin as _check_admin,
    validate_sample_id as _validate_sample_id,
)
from SampleService.core.user import UserID as _UserID


def update_samples_acls(
    params, samples_client, user_lookup, user, token, perms, log_info
):
    '''
    Completely replace the ACLs for a list of samples.

    :param id_: the sample's ID.
    :param params: the update_samples_acls parameters
    :param samples_client: the samples client instance
    :param user_lookup: the KBaseUserLookup client method
    :param user: client username
    :param token: client token
    :param perms: client permissions
    :param log_info: logger
    :returns: None
    :raises IllegalParameterError: if the ID is provided but is invalid.
    :raises InvalidUserError: if any of the user names are invalid.
    :raises NoSuchSampleError: if the sample does not exist.
    :raises NoSuchUserError: if any of the users in the ACLs do not exist.
    :raises SampleStorageError: if the sample could not be retrieved.
    :raises UnauthorizedError: if any of the users names are valid but do not
        exist in the system.
    :raises UnauthorizedError: if the user does not have the permission
        required.
    :raises UnauthorizedError: if the user does not have admin permission for
        the sample or the request attempts to alter the owner.
    '''
    acldelta = _acl_delta_from_dict(params)
    admin = _check_admin(
        user_lookup,
        token,
        perms,
        'update_sample_acls',
        log_info,
        skip_check=not params.get('as_admin'),
    )
    ids = params.get('ids')
    for id_ in ids:
        _validate_sample_id(id_, '')
        samples_client.update_sample_acls(
            id_, _UserID(user), acldelta, as_admin=admin
        )
