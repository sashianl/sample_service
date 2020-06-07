'''
Classes and methods for working with sample ACLs.
'''

import datetime

from enum import IntEnum

from typing import Sequence, cast as _cast
from SampleService.core.arg_checkers import (
    not_falsy as _not_falsy,
    not_falsy_in_iterable as _not_falsy_in_iterable,
    check_timestamp as _check_timestamp
)
from SampleService.core.errors import IllegalParameterError as _IllegalParameterError
from SampleService.core.user import UserID


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
    '''

    def __init__(
            self,
            admin: Sequence[UserID] = None,
            write: Sequence[UserID] = None,
            read: Sequence[UserID] = None):
        '''
        Create the ACLs.

        :param admin: the list of admin usernames.
        :param write: the list of usernames with write privileges.
        :param read: the list of usernames with read privileges.
        :raises IllegalParameterError: if a user appears in more than one ACL.
        '''
        self.admin = self._to_tuple(admin, 'admin')
        self.write = self._to_tuple(write, 'write')
        self.read = self._to_tuple(read, 'read')
        for u in self.admin:
            if u in self.write or u in self.read:
                raise _IllegalParameterError(f'User {u} appears in two ACLs')
        for u in self.write:
            if u in self.read:
                raise _IllegalParameterError(f'User {u} appears in two ACLs')

    def _to_tuple(self, seq, name):
        # dict.fromkeys removes dupes
        return tuple(dict.fromkeys(
            _cast(Sequence[UserID], _not_falsy_in_iterable([] if seq is None else seq, name))))

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.admin == self.admin
                    and other.write == self.write
                    and other.read == self.read)
        return NotImplemented

    def __hash__(self):
        return hash((self.admin, self.write, self.read))


class SampleACL(SampleACLOwnerless):
    '''
    An Access Control Sequence for a sample, consisting of user names for various privileges.

    :ivar owner: the owner username.
    :ivar admin: the list of admin usernames.
    :ivar write: the list of usernames with write privileges.
    :ivar read: the list of usernames with read privileges.
    :ivar lastupdate: the date the last time the ACLs were updated.
    '''

    def __init__(
            self,
            owner: UserID,
            lastupdate: datetime.datetime,
            admin: Sequence[UserID] = None,
            write: Sequence[UserID] = None,
            read: Sequence[UserID] = None):
        '''
        Create the ACLs.

        :param owner: the owner username.
        :param lastupdate: the last time the ACLs were updated.
        :param admin: the list of admin usernames.
        :param write: the list of usernames with write privileges.
        :param read: the list of usernames with read privileges.
        :raises IllegalParameterError: If a user appears in more than one ACL
        '''
        self.owner = _not_falsy(owner, 'owner')
        self.lastupdate = _check_timestamp(lastupdate, 'lastupdate')
        super().__init__(admin, write, read)
        all_ = (self.admin, self.write, self.read)
        for i in range(len(all_)):
            if self.owner in all_[i]:
                raise _IllegalParameterError('The owner cannot be in any other ACL')

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.owner == self.owner
                    and other.lastupdate == self.lastupdate
                    and other.admin == self.admin
                    and other.write == self.write
                    and other.read == self.read)
        return NotImplemented

    def __hash__(self):
        return hash((self.owner, self.lastupdate, self.admin, self.write, self.read))
