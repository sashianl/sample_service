'''
Classes and methods for working with sample ACLs.
'''

import datetime

from enum import IntEnum

from typing import Sequence, cast as _cast, Optional, Tuple as _Tuple
from SampleService.core.arg_checkers import (
    not_falsy as _not_falsy,
    not_falsy_in_iterable as _not_falsy_in_iterable,
    check_timestamp as _check_timestamp
)
from SampleService.core.errors import (
    IllegalParameterError as _IllegalParameterError,
    UnauthorizedError as _UnauthorizedError,
)
from SampleService.core.user import UserID

# may need to add sane limits on ACL sizes if people act like jerks


class SampleAccessType(IntEnum):
    '''
    The different levels of sample access.
    '''
    NONE = 1
    READ = 2
    WRITE = 3
    ADMIN = 4
    OWNER = 5


class AdminPermission(IntEnum):
    '''
    The different levels of admin permissions.
    '''
    NONE = 1
    READ = 2
    FULL = 3


class SampleACLOwnerless:
    '''
    An Access Control List for a sample, consisting of user names for various privileges, but
    without an owner.

    :ivar admin: the list of admin usernames.
    :ivar write: the list of usernames with write privileges.
    :ivar read: the list of usernames with read privileges.
    :ivar public_read: a boolean designating whether the sample is publically readable.
    '''

    def __init__(
            self,
            admin: Sequence[UserID] = None,
            write: Sequence[UserID] = None,
            read: Sequence[UserID] = None,
            public_read: bool = False):
        '''
        Create the ACLs.

        :param admin: the list of admin usernames.
        :param write: the list of usernames with write privileges.
        :param read: the list of usernames with read privileges.
        :param public_read: a boolean designating whether the sample is publically readable.
            None is considered false.
        :raises IllegalParameterError: if a user appears in more than one ACL.
        '''
        self.admin = _to_tuple(admin, 'admin')
        self.write = _to_tuple(write, 'write')
        self.read = _to_tuple(read, 'read')
        self.public_read = bool(public_read)  # deal with None inputs
        _check_acl_duplicates(self.admin, self.write, self.read)

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.admin == self.admin
                    and other.write == self.write
                    and other.read == self.read
                    and other.public_read == self.public_read)
        return NotImplemented

    def __hash__(self):
        return hash((self.admin, self.write, self.read, self.public_read))


def _to_tuple(seq, name) -> _Tuple[UserID, ...]:
    # dict.fromkeys removes dupes
    return tuple(dict.fromkeys(
        sorted(  # sort to make equals and hash consistent
            _cast(Sequence[UserID], _not_falsy_in_iterable([] if seq is None else seq, name)),
            key=lambda u: u.id)))  # add comparison methods to user?


def _check_acl_duplicates(admin, write, read):
    for u in admin:
        if u in write or u in read:
            raise _IllegalParameterError(f'User {u} appears in two ACLs')
    for u in write:
        if u in read:
            raise _IllegalParameterError(f'User {u} appears in two ACLs')


class SampleACLDelta():
    '''
    An Access Control Sequence delta for a sample, consisting of user names that should be added
        for various privileges and and list of usernames that should be removed for all privileges.

    :ivar admin: the list of usernames to be granted admin privileges.
    :ivar write: the list of usernames to be granted write privileges.
    :ivar read: the list of usernames to be granted read privileges.
    :ivar remove: the list of usernames to have all privileges removed.
    :ivar public_read: a boolean designating whether the sample should be made publically readable.
        None signifies no change.
    :ivar at_least: True signifies that the provided user's permissions should not be downgraded
        if they are greater than the permission in the delta ACL. If False, the user's permission
        will be set to exactly the permission in the delta ACL.
    '''
    # hmm, this is pretty similar to SampleACLOwnerless... semantics are different though.

    def __init__(
            self,
            admin: Sequence[UserID] = None,
            write: Sequence[UserID] = None,
            read: Sequence[UserID] = None,
            remove: Sequence[UserID] = None,
            public_read: Optional[bool] = None,
            at_least: bool = False):
        '''
        Create the ACLs.

        :param admin: the list of usernames to be granted admin privileges.
        :param write: the list of usernames to be granted write privileges.
        :param read: the list of usernames to be granted read privileges.
        :param remove: the list of usernames to have all privileges removed.
        :param public_read: a boolean designating whether the sample is publically readable.
            None signifies no change.
        :ivar at_least: True signifies that the provided user's permissions should not be
            downgraded if they are greater than the permission in the delta ACL. If False, the
            user's permission will be set to exactly the permission in the delta ACL. None is
            treated as False.
        :raises IllegalParameterError: If a user appears in more than one ACL
        '''
        self.admin = _to_tuple(admin, 'admin')
        self.write = _to_tuple(write, 'write')
        self.read = _to_tuple(read, 'read')
        self.remove = _to_tuple(remove, 'remove')
        self.public_read = public_read
        self.at_least = bool(at_least)  # handle None
        _check_acl_duplicates(self.admin, self.write, self.read)
        all_ = set(self.admin + self.write + self.read)
        for r in self.remove:
            if r in all_:
                raise _IllegalParameterError('Users in the remove list cannot be in any other ACL')

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.admin == self.admin
                    and other.write == self.write
                    and other.read == self.read
                    and other.remove == self.remove
                    and other.public_read is self.public_read
                    and other.at_least is self.at_least)
        return NotImplemented

    def __hash__(self):
        return hash((self.admin, self.write, self.read, self.remove, self.public_read,
                     self.at_least))


class SampleACL(SampleACLOwnerless):
    '''
    An Access Control Sequence for a sample, consisting of user names for various privileges.

    :ivar owner: the owner username.
    :ivar admin: the list of admin usernames.
    :ivar write: the list of usernames with write privileges.
    :ivar read: the list of usernames with read privileges.
    :ivar public_read: a boolean designating whether the sample is publically readable.
    :ivar lastupdate: the date the last time the ACLs were updated.
    '''

    def __init__(
            self,
            owner: UserID,
            lastupdate: datetime.datetime,
            admin: Sequence[UserID] = None,
            write: Sequence[UserID] = None,
            read: Sequence[UserID] = None,
            public_read: bool = False):
        '''
        Create the ACLs.

        :param owner: the owner username.
        :param lastupdate: the last time the ACLs were updated.
        :param admin: the list of admin usernames.
        :param write: the list of usernames with write privileges.
        :param read: the list of usernames with read privileges.
        :param public_read: a boolean designating whether the sample is publically readable.
            None is considered false.
        :raises IllegalParameterError: If a user appears in more than one ACL
        '''
        self.owner = _not_falsy(owner, 'owner')
        self.lastupdate = _check_timestamp(lastupdate, 'lastupdate')
        super().__init__(admin, write, read, public_read)
        all_ = (self.admin, self.write, self.read)
        for i in range(len(all_)):
            if self.owner in all_[i]:
                raise _IllegalParameterError('The owner cannot be in any other ACL')

    def is_update(self, update: SampleACLDelta) -> bool:
        '''
        Check if an acl delta update is actually an update or a noop for the sample.

        :param update: the update.
        :returns: True if the update would change the ACLs, False if not. The timestamp is not
            considered.
        :raises UnauthorizedError: if the update would affect the owner and update.at_least is
            not true, or if the owner is in the remove list regardless of at_least.
        '''
        _not_falsy(update, 'update')
        o = self.owner
        ownerchange = o in update.admin or o in update.write or o in update.read
        if (ownerchange and not update.at_least) or o in update.remove:
            raise _UnauthorizedError(
                f'ACLs for the sample owner {o.id} may not be modified by a delta update.')

        rem = set(update.remove)
        admin = set(self.admin)
        write = set(self.write)
        read = set(self.read)

        # check if users are removed
        if not rem.isdisjoint(admin) or not rem.isdisjoint(write) or not rem.isdisjoint(read):
            return True

        # check if public read is changed
        if update.public_read is not None and update.public_read is not self.public_read:
            return True

        uadmin = set(update.admin)
        uwrite = set(update.write)
        uread = set(update.read)
        owner = set([o])

        # check if users' permission is changed
        if update.at_least:
            return (not uadmin.issubset(admin | owner) or
                    not uwrite.issubset(write | admin | owner) or
                    not uread.issubset(read | write | admin | owner))
        else:
            return (not uadmin.issubset(admin) or
                    not uwrite.issubset(write) or
                    not uread.issubset(read))

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.owner == self.owner
                    and other.lastupdate == self.lastupdate
                    and other.admin == self.admin
                    and other.write == self.write
                    and other.read == self.read
                    and other.public_read is self.public_read)
        return NotImplemented

    def __hash__(self):
        return hash((self.owner, self.lastupdate, self.admin, self.write, self.read,
                     self.public_read))

    # def __repr__(self):
    #     return (f'SampleACL[{self.owner}, {self.lastupdate}, {self.admin}, {self.write}, ' +
    #             f'{self.read}, {self.public_read}]')
