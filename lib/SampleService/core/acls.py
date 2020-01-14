'''
Classes and methods for working with sample ACLs.
'''

from enum import IntEnum

from typing import Sequence
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import not_falsy_in_iterable as _not_falsy_in_iterable
from SampleService.core.errors import IllegalParameterError as _IllegalParameterError


class SampleAccessType(IntEnum):
    '''
    The different levels of sample access.
    '''
    NONE = 1
    READ = 2,
    WRITE = 3,
    ADMIN = 4,
    OWNER = 5


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
            admin: Sequence[str] = None,
            write: Sequence[str] = None,
            read: Sequence[str] = None):
        '''
        Create the ACLs.

        :param admin: the list of admin usernames.
        :param write: the list of usernames with write privileges.
        :param read: the list of usernames with read privileges.
        :raises IllegalParameterError: if a user appears in more than one ACL.
        '''
        # dict.fromkeys removes dupes
        self.admin = tuple(dict.fromkeys(
            _not_falsy_in_iterable([] if admin is None else admin, 'admin')))
        self.write = tuple(dict.fromkeys(
            _not_falsy_in_iterable([] if write is None else write, 'write')))
        self.read = tuple(dict.fromkeys(
            _not_falsy_in_iterable([] if read is None else read, 'read')))
        # TODO test with integration test
        for u in self.admin:
            if u in self.write or u in self.read:
                raise _IllegalParameterError(f'User {u} appears in two ACLs')
        for u in self.write:
            if u in self.read:
                raise _IllegalParameterError(f'User {u} appears in two ACLs')

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
    '''

    def __init__(
            self,
            owner: str,
            admin: Sequence[str] = None,
            write: Sequence[str] = None,
            read: Sequence[str] = None):
        '''
        Create the ACLs.

        :param owner: the owner username.
        :param admin: the list of admin usernames.
        :param write: the list of usernames with write privileges.
        :param read: the list of usernames with read privileges.
        :raises IllegalParameterError: If a user appears in more than one ACL
        '''
        # TODO may want a class for user name rather than using raw strings
        self.owner = _not_falsy(owner, 'owner')
        super().__init__(admin, write, read)
        all_ = (self.admin, self.write, self.read)
        # TODO test with integration test
        for i in range(len(all_)):
            if self.owner in all_[i]:
                raise _IllegalParameterError('The owner cannot be in any other ACL')

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.owner == self.owner
                    and other.admin == self.admin
                    and other.write == self.write
                    and other.read == self.read)
        return NotImplemented

    def __hash__(self):
        return hash((self.owner, self.admin, self.write, self.read))
