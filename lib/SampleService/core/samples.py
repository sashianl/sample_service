'''
Core class for saving and getting samples.
'''

import datetime
import uuid as _uuid  # lgtm [py/import-and-import-from]
from uuid import UUID

from typing import Optional, Callable, Tuple, List, Dict, Union, Any, cast as _cast

from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import check_timestamp as _check_timestamp
from SampleService.core.acls import SampleAccessType as _SampleAccessType
from SampleService.core.acls import SampleACL, SampleACLOwnerless, SampleACLDelta
from SampleService.core.core_types import PrimitiveType
from SampleService.core.data_link import DataLink
from SampleService.core.errors import (
    UnauthorizedError as _UnauthorizedError,
    IllegalParameterError as _IllegalParameterError,
    MetadataValidationError as _MetadataValidationError,
    NoSuchUserError as _NoSuchUserError,
    NoSuchLinkError as _NoSuchLinkError
)
from SampleService.core.notification import KafkaNotifier
from SampleService.core.sample import Sample, SavedSample, SampleAddress, SampleNodeAddress
from SampleService.core.user_lookup import KBaseUserLookup
from SampleService.core import user_lookup as _user_lookup_mod
from SampleService.core.validator.metadata_validator import MetadataValidatorSet
from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage
from SampleService.core.storage.errors import OwnerChangedError as _OwnerChangedError
from SampleService.core.user import UserID
from SampleService.core.workspace import WS, WorkspaceAccessType as _WorkspaceAccessType
from SampleService.core.workspace import DataUnitID, UPA


# TODO remove own acls.

class Samples:
    '''
    Class implementing sample manipulation operations.
    '''

    def __init__(
            self,
            storage: ArangoSampleStorage,
            user_lookup: KBaseUserLookup,  # make an interface? YAGNI
            metadata_validator: MetadataValidatorSet,
            workspace: WS,
            # may want to support multiple notifiers. YAGNI for now
            notifier: Optional[KafkaNotifier] = None,
            now: Callable[[], datetime.datetime] = lambda: datetime.datetime.now(
                tz=datetime.timezone.utc),
            uuid_gen: Callable[[], UUID] = lambda: _uuid.uuid4()):
        '''
        Create the class.

        :param storage: the storage system to use.
        :param user_lookup: a service to verify usernames are valid and exist.
        :param metadata_validator: A validator for metadata.
        '''
        # don't publicize these params
        # :param now: A callable that returns the current time. Primarily used for testing.
        # :param uuid_gen: A callable that returns a random UUID. Primarily used for testing.
        # extract an interface from ASS if needed.
        self._storage = _not_falsy(storage, 'storage')
        self._user_lookup = _not_falsy(user_lookup, 'user_lookup')
        self._metaval = _not_falsy(metadata_validator, 'metadata_validator')
        self._ws = _not_falsy(workspace, 'workspace')
        self._kafka = notifier  # can be None
        self._now = _not_falsy(now, 'now')
        self._uuid_gen = _not_falsy(uuid_gen, 'uuid_gen')

    def save_sample(
            self,
            sample: Sample,
            user: UserID,
            id_: UUID = None,
            prior_version: Optional[int] = None,
            as_admin: bool = False) -> Tuple[UUID, int]:
        '''
        Save a sample.

        :param sample: the sample to save.
        :param user: the username of the user saving the sample.
        :param id_: if the sample is a new version of a sample, the ID of the sample which will
            get a new version.
        :param prior_version: if id_ is included, specifying prior_version will ensure that the new
            sample is saved with version prior_version + 1 or not at all.
        :param as_admin: skip ACL checks for new versions.
        :returns a tuple of the sample ID and version.
        :raises IllegalParameterError: if the prior version is < 1
        :raises UnauthorizedError: if the user does not have write permission to the sample when
            saving a new version.
        :raises NoSuchSampleError: if the sample does not exist when saving a new version.
        :raises SampleStorageError: if the sample could not be retrieved when saving a new version
            or if the sample fails to save.
        :raises ConcurrencyError: if the sample's version is not equal to prior_version.
        '''
        _not_falsy(sample, 'sample')
        _not_falsy(user, 'user')
        _ = self._validate_metadata(sample)
        if id_:
            if prior_version is not None and prior_version < 1:
                raise _IllegalParameterError('Prior version must be > 0')
            self._check_perms(id_, user, _SampleAccessType.WRITE, as_admin=as_admin)
            swid = SavedSample(id_, user, list(sample.nodes), self._now(), sample.name)
            ver = self._storage.save_sample_version(swid, prior_version)
        else:
            id_ = self._uuid_gen()
            swid = SavedSample(id_, user, list(sample.nodes), self._now(), sample.name)
            # don't bother checking output since we created uuid
            self._storage.save_sample(swid)
            ver = 1
        if self._kafka:
            self._kafka.notify_new_sample_version(id_, ver)
        return (id_, ver)

    def _validate_metadata(self, sample: Sample, return_error_detail: bool=False):
        '''
        :params sample: sample to be validated
        :params return_exception: default=False, whether to return all errors found as a list exceptions.

        :returns: list of excpetions
        '''
        for i, n in enumerate(sample.nodes):
            try:
                error_detail = self._metaval.validate_metadata(n.controlled_metadata, return_error_detail)
                if return_error_detail:
                    for e in error_detail:
                        e['node'] = n.name
                    return error_detail
            except _MetadataValidationError as e:
                raise _MetadataValidationError(f'Node at index {i}: {e.message}') from e

    def _check_perms(
            self,
            id_: UUID,
            user: Optional[UserID],
            access: _SampleAccessType,
            acls: SampleACL = None,
            as_admin: bool = False):
        if as_admin:
            return
        if not acls:
            acls = self._storage.get_sample_acls(id_)
        level = self._get_access_level(acls, user)
        if level < access:
            uerr = f'User {user}' if user else 'Anonymous users'
            errmsg = f'{uerr} {self._unauth_errmap[access]} sample {id_}'
            raise _UnauthorizedError(errmsg)

    _unauth_errmap = {_SampleAccessType.OWNER: 'does not own',
                      _SampleAccessType.ADMIN: 'cannot administrate',
                      _SampleAccessType.WRITE: 'cannot write to',
                      _SampleAccessType.READ: 'cannot read'}

    def _check_batch_perms(
        self,
        ids_: List[UUID],
        user: Optional[UserID],
        access: _SampleAccessType,
        acls: List[SampleACL] = None,
        as_admin: bool = False):
            if as_admin:
                return
            if not acls:
                acls = self._storage.get_sample_set_acls(ids_)
            levels = [self._get_access_level(acl, user) for acl in acls]
            for i, level in enumerate(levels):
                if level < access:
                    uerr = f'User {user}' if user else 'Anonymous users'
                    errmsg = f'{uerr} {self._unauth_errmap[access]} sample {ids_[i]}'
                    raise _UnauthorizedError(errmsg)

    def _get_access_level(self, acls: SampleACL, user: Optional[UserID]):
        if user == acls.owner:
            return _SampleAccessType.OWNER
        if user in acls.admin:
            return _SampleAccessType.ADMIN
        if user in acls.write:
            return _SampleAccessType.WRITE
        if user in acls.read or acls.public_read:
            return _SampleAccessType.READ
        return _SampleAccessType.NONE

    def get_sample(
            self,
            id_: UUID,
            user: Optional[UserID],
            version: int = None,
            as_admin: bool = False) -> SavedSample:
        '''
        Get a sample.
        :param id_: the ID of the sample.
        :param user: the username of the user getting the sample, or None for an anonymous user.
        :param version: The version of the sample to retrieve. Defaults to the latest version.
        :param as_admin: Skip ACL checks.
        :returns: the sample.
        :raises IllegalParameterError: if version is supplied and is < 1
        :raises UnauthorizedError: if the user does not have read permission for the sample.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises NoSuchSampleVersionError: if the sample version does not exist.
        :raises SampleStorageError: if the sample could not be retrieved.
        '''
        if version is not None and version < 1:
            raise _IllegalParameterError('Version must be > 0')
        self._check_perms(_not_falsy(id_, 'id_'), user, _SampleAccessType.READ, as_admin=as_admin)
        return self._storage.get_sample(id_, version)

    def get_samples(
        self,
        ids_: List[Dict[str, Any]],
        user: Optional[UserID],
        as_admin: bool = False) -> List[SavedSample]:
        '''
        '''
        for id_ in ids_:
            self._check_perms(_not_falsy(id_['id'], 'id_'), user, _SampleAccessType.READ, as_admin=as_admin)
        return self._storage.get_samples(ids_)

    def get_sample_acls(
            self, id_: UUID, user: Optional[UserID], as_admin: bool = False) -> SampleACL:
        '''
        Get a sample's acls.
        :param id_: the ID of the sample.
        :param user: the username of the user getting the acls or None if the user is anonymous.
        :param as_admin: Skip ACL checks.
        :returns: the sample acls.
        :raises UnauthorizedError: if the user does not have read permission for the sample.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises SampleStorageError: if the sample could not be retrieved.
        '''
        acls = self._storage.get_sample_acls(_not_falsy(id_, 'id_'))
        self._check_perms(id_, user, _SampleAccessType.READ, acls, as_admin=as_admin)
        return acls

    def replace_sample_acls(
            self,
            id_: UUID,
            user: UserID,
            new_acls: SampleACLOwnerless,
            as_admin: bool = False) -> None:
        '''
        Completely replace a sample's ACLs. The owner cannot be changed.

        :param id_: the sample's ID.
        :param user: the user changing the ACLs.
        :param new_acls: the new ACLs.
        :param as_admin: Skip ACL checks.
        :raises NoSuchUserError: if any of the users in the ACLs do not exist.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises UnauthorizedError: if the user does not have admin permission for the sample or
            the request attempts to change the owner.
        :raises SampleStorageError: if the sample could not be retrieved.
        '''
        _not_falsy(id_, 'id_')
        _not_falsy(user, 'user')
        _not_falsy(new_acls, 'new_acls')
        self._check_for_bad_users(_cast(List[UserID], []) + list(new_acls.admin) +
                                  list(new_acls.write) + list(new_acls.read))
        count = 0
        while count >= 0:
            if count >= 5:
                raise ValueError(f'Failed setting ACLs after 5 attempts for sample {id_}')
            acls = self._storage.get_sample_acls(id_)
            self._check_perms(id_, user, _SampleAccessType.ADMIN, acls, as_admin=as_admin)
            new_acls = SampleACL(
                acls.owner,
                self._now(),
                new_acls.admin,
                new_acls.write,
                new_acls.read,
                new_acls.public_read)
            try:
                self._storage.replace_sample_acls(id_, new_acls)
                count = -1
            except _OwnerChangedError:
                count += 1
        if self._kafka:
            self._kafka.notify_sample_acl_change(id_)

    def update_sample_acls(
            self,
            id_: UUID,
            user: UserID,
            update: SampleACLDelta,
            as_admin: bool = False) -> None:
        '''
        Completely replace a sample's ACLs.

        :param id_: the sample's ID.
        :param user: the user changing the ACLs.
        :param update: the ACL update. Note the update time is ignored. If the sample owner is
            in any of the lists in the update, the update will fail.
        :param as_admin: Skip ACL checks.
        :raises NoSuchUserError: if any of the users in the ACLs do not exist.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises UnauthorizedError: if the user does not have admin permission for the sample or
            the request attempts to alter the owner.
        :raises SampleStorageError: if the sample could not be retrieved.
        '''
        # could make yet another ACL class that's a delta w/o an update time - probably not
        # worth it. If people get confused do it.
        _not_falsy(id_, 'id_')
        _not_falsy(user, 'user')
        _not_falsy(update, 'update')
        self._check_for_bad_users(_cast(List[UserID], []) + list(update.admin) +
                                  list(update.write) + list(update.read) + list(update.remove))

        self._check_perms(id_, user, _SampleAccessType.ADMIN, as_admin=as_admin)

        self._storage.update_sample_acls(id_, update, self._now())
        if self._kafka:
            self._kafka.notify_sample_acl_change(id_)

    # TODO change owner. Probably needs a request/accept flow.

    def _check_for_bad_users(self, users: List[UserID]):
        try:
            bad_users = self._user_lookup.invalid_users(users)
            # let authentication errors propagate, not much to do
            # could add retries to the client
        except _user_lookup_mod.InvalidUserError as e:
            raise _NoSuchUserError(e.args[0]) from e
        except _user_lookup_mod.InvalidTokenError:
            raise ValueError('user lookup token for KBase auth server is invalid, cannot continue')
        if bad_users:
            raise _NoSuchUserError(', '.join([u.id for u in bad_users[:5]]))

    def get_key_static_metadata(
            self,
            keys: List[str],
            prefix: Union[bool, None] = False
            ) -> Dict[str, Dict[str, PrimitiveType]]:
        '''
        Get any static metadata associated with the provided list of keys.

        :param keys: The keys to query.
        :param prefix: True to query prefix keys, None to query prefix keys but only match exactly,
            False for standard keys.
        :returns: A mapping of key to key metadata.
        '''
        if keys is None:
            raise ValueError('keys cannot be None')
        if prefix is False:
            return self._metaval.key_metadata(keys)
        else:
            return self._metaval.prefix_key_metadata(keys, exact_match=not bool(prefix))

    def create_data_link(
            self,
            user: UserID,
            duid: DataUnitID,
            sna: SampleNodeAddress,
            update: bool = False,
            as_admin: bool = False) -> DataLink:
        '''
        Create a link from a data unit to a sample. The user must have admin access to the sample,
        since linking data grants permissions: once linked, if a user
        has access to the data unit, the user also has access to the sample. The user must have
        write access to the data since adding a sample to the data effectively modifies the data,
        but doesn't grant any additional access.

        Each data unit can be linked to only one sample at a time. Expired links may exist to
        other samples.

        :param user: the user creating the link.
        :param duid: the data unit to link the the sample.
        :param sna: the sample node to link to the data unit.
        :param update: True to expire any extant link if it does not link to the provided sample.
            If False and a link from the data unit already exists, link creation will fail.
        :param as_admin: allow link creation to proceed if user does not have
            appropriate permissions.
        :returns: the new link.
        :raises UnauthorizedError: if the user does not have acceptable permissions.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises NoSuchSampleVersionError: if the sample version does not exist.
        :raises NoSuchSampleNodeError: if the sample node does not exist.
        :raises NoSuchWorkspaceDataError: if the workspace or UPA doesn't exist.
        :raises DataLinkExistsError: if a link already exists from the data unit.
        :raises TooManyDataLinksError: if there are too many links from the sample version or
            the workspace object version.
        :raises SampleStorageError: if the sample could not be retrieved.
        '''
        _not_falsy(user, 'user')
        _not_falsy(duid, 'duid')
        self._check_perms(
            _not_falsy(sna, 'sna').sampleid, user, _SampleAccessType.ADMIN, as_admin=as_admin)
        wsperm = _WorkspaceAccessType.NONE if as_admin else _WorkspaceAccessType.WRITE
        self._ws.has_permission(user, wsperm, upa=duid.upa)
        dl = DataLink(self._uuid_gen(), duid, sna, self._now(), user)
        expired_id = self._storage.create_data_link(dl, update=update)
        if self._kafka:
            self._kafka.notify_new_link(dl.id)
            if expired_id:  # maybe make the notifier accept both notifications & send both?
                self._kafka.notify_expired_link(expired_id)
        return dl

    def expire_data_link(self, user: UserID, duid: DataUnitID, as_admin: bool = False) -> None:
        '''
        Expire a data link, ensuring that it will not show up in link queries without an effective
        timestamp in the past.
        The user must have admin access to the sample and write access to the data. The data may
        be deleted.

        :param user: the user expiring the link.
        :param duid: the data unit ID for the extant link.
        :param as_admin: allow link expiration to proceed if user does not have
            appropriate permissions.
        :raises UnauthorizedError: if the user does not have acceptable permissions.
        :raises NoSuchWorkspaceDataError: if the workspace doesn't exist.
        :raises NoSuchLinkError: if there is no link from the data unit.
        '''
        _not_falsy(user, 'user')
        _not_falsy(duid, 'duid')
        # allow expiring links for deleted objects. It should be impossible to have a link
        # for an object that has never existed.
        wsperm = _WorkspaceAccessType.NONE if as_admin else _WorkspaceAccessType.WRITE
        self._ws.has_permission(user, wsperm, workspace_id=duid.upa.wsid)
        link = self._storage.get_data_link(duid=duid)
        # was concerned about exposing the sample ID, but if the user has write access to the
        # UPA then they can get the link with the sample ID, so don't worry about it.
        self._check_perms(
            link.sample_node_address.sampleid, user, _SampleAccessType.ADMIN, as_admin=as_admin)
        # Use the ID here to prevent a race condition expiring a new link to a different sample
        # since the user may not have perms
        # There's a chance the link could be expired between db fetch and update, but that
        # takes millisecond precision and just means a funky error message occurs, so don't
        # worry about it.
        self._storage.expire_data_link(self._now(), user, id_=link.id)
        if self._kafka:
            self._kafka.notify_expired_link(link.id)

    def get_links_from_sample(
            self,
            user: Optional[UserID],
            sample: SampleAddress,
            timestamp: datetime.datetime = None,
            as_admin: bool = False) -> Tuple[List[DataLink], datetime.datetime]:
        '''
        Get a set of data links originating from a sample at a particular time.

        :param user: the user requesting the links or None if the user is anonymous.
        :param sample: the sample from which the links originate.
        :param timestamp: the timestamp during which the links should be active, defaulting to
            the current time.
        :param as_admin: allow link retrieval to proceed if user does not have
            appropriate permissions.
        :returns: a tuple consisting of a list of links and the timestamp used to query the links.
        :raises UnauthorizedError: if the user does not have read permission for the sample.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises NoSuchSampleVersionError: if the sample version does not exist.
        :raises NoSuchUserError: if the user does not exist.
        '''
        _not_falsy(sample, 'sample')
        timestamp = self._resolve_timestamp(timestamp)
        self._check_perms(sample.sampleid, user, _SampleAccessType.READ, as_admin=as_admin)
        wsids = None if as_admin else self._ws.get_user_workspaces(user)
        # TODO DATALINK what about deleted objects? Currently not handled
        return self._storage.get_links_from_sample(sample, wsids, timestamp), timestamp

    def get_batch_links_from_sample_set(
            self,
            user: Optional[UserID],
            samples: List[SampleAddress],
            timestamp: datetime.datetime = None,
            as_admin: bool = False) -> Tuple[List[DataLink], datetime.datetime]:
        '''
        A batch version of get_links_from_sample. Gets a set of  data links originating
        from multiple samples in a given sampleset at a particular time.

        :param user: the user requesting the links or None if the user is anonymous.
        :param samples: the list of sample ids from which the links originate.
        :param timestamp: the timestamp during which the links should be active, defaulting to
            the current time.
        :param as_admin: allow link retrieval to proceed if user does not have
            appropriate permissions.
        :returns: a tuple consisting of a list of links and the timestamp used to query the links.
        :raises UnauthorizedError: if the user does not have read permission for the sample.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises NoSuchSampleVersionError: if the sample version does not exist.
        :raises NoSuchUserError: if the user does not exist.
        '''
        _not_falsy(samples, 'samples')
        timestamp = self._resolve_timestamp(timestamp)
        wsids = None if as_admin else self._ws.get_user_workspaces(user)
        # checks for all sample acls in one query
        sampleids = [s.sampleid for s in samples]
        self._check_batch_perms(sampleids, user, _SampleAccessType.READ, as_admin=as_admin)
        return_links = self._storage.get_batch_links_from_samples(samples, wsids, timestamp)
        return return_links, timestamp

    def _resolve_timestamp(self, timestamp: datetime.datetime = None) -> datetime.datetime:
        if timestamp:
            _check_timestamp(timestamp, 'timestamp')
        else:
            timestamp = self._now()
        return timestamp

    def get_links_from_data(
            self,
            user: Optional[UserID],
            upa: UPA,
            timestamp: datetime.datetime = None,
            as_admin: bool = False) -> Tuple[List[DataLink], datetime.datetime]:
        '''
        Get a set of data links originating from a workspace object at a particular time.

        :param user: the user requesting the links, or None for an anonymous user.
        :param upa: the data from which the links originate.
        :param timestamp: the timestamp during which the links should be active, defaulting to
            the current time.
        :param as_admin: allow link retrieval to proceed if user does not have
            appropriate permissions.
        :returns: a tuple consisting of a list of links and the timestamp used to query the links.
        :raises UnauthorizedError: if the user does not have read permission for the data.
        :raises NoSuchWorkspaceDataError: if the data does not exist.
        '''
        # may need to make this independent of the workspace. YAGNI.
        # handle ref path?
        _not_falsy(upa, 'upa')
        timestamp = self._resolve_timestamp(timestamp)
        # NONE still checks that WS/obj exists. If it's deleted this method should fail
        wsperm = _WorkspaceAccessType.NONE if as_admin else _WorkspaceAccessType.READ
        self._ws.has_permission(user, wsperm, upa=upa)
        return self._storage.get_links_from_data(upa, timestamp), timestamp

    def get_sample_via_data(
            self,
            user: Optional[UserID],
            upa: UPA,
            sample_address: SampleAddress) -> SavedSample:
        '''
        Given a workspace object, get a sample linked to that object. The user must have read
        permissions for the object, but not necessarily the sample. The link may be expired.

        :param user: The user requesting the sample or None if the user is anonymous.
        :param upa: the data from which the link to the sample originates.
        :param sample_address: the sample address.
        :returns: the linked sample.
        :raises UnauthorizedError: if the user cannot read the UPA.
        :raises NoSuchWorkspaceDataError: if the workspace object does not exist.
        :raises NoSuchLinkError: if there is no link from the object to the sample.
        :raises NoSuchSampleVersionError: if the sample version does not exist.
        '''
        # no admin mode needed - use get_links or get sample
        # may need to make this independent of the workspace. YAGNI.
        # handle ref path?
        _not_falsy(upa, 'upa')
        _not_falsy(sample_address, 'sample_address')
        # the order of these checks is important, check read first, then we know link & sample
        # access is ok
        self._ws.has_permission(user, _WorkspaceAccessType.READ, upa=upa)
        if not self._storage.has_data_link(upa, sample_address.sampleid):
            raise _NoSuchLinkError(
                f'There is no link from UPA {upa} to sample {sample_address.sampleid}')
        # can't raise no sample error since a link exists
        return self._storage.get_sample(sample_address.sampleid, sample_address.version)

    def get_data_link_admin(self, link_id: UUID) -> DataLink:
        '''
        This method is intended for admin use and should not be exposed in a public API.

        Get a link by its ID. The link may be expired.

        :param link_id: the link ID.
        :returns: the link.
        :raises NoSuchLinkError: if the link does not exist.
        '''
        # if we expose this to users need to add ACL checking. Don't see a use case ATM.
        return self._storage.get_data_link(_not_falsy(link_id, 'link_id'))

    def validate_sample(self, sample: Sample):
        '''
        This method performs only the validation steps on a sample

        :param sample: the sample to validate
        '''
        _not_falsy(sample, 'sample')
        error_detail = self._validate_metadata(sample, return_error_detail=True)
        for e in error_detail:
            e['sample_name'] = sample.name
        return error_detail
