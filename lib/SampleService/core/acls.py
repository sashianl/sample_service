'''
Classes and methods for working with sample ACLs.
'''

from typing import List
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import not_falsy_in_iterable as _not_falsy_in_iterable


class SampleACL:
    '''
    An Access Control List for a sample, consisting of user names for various privileges.

    :ivar owner: the owner username.
    :ivar admin: the list of admin usernames.
    :ivar write: the list of usernames with write privileges.
    :ivar read: the list of usernames with read privileges.
    '''

    def __init__(
            self,
            owner: str,
            admin: List[str] = None,
            write: List[str] = None,
            read: List[str] = None):
        '''
        Create the ACLs.

        :param owner: the owner username.
        :param admin: the list of admin usernames.
        :param write: the list of usernames with write privileges.
        :param read: the list of usernames with read privileges.
        '''
        # TODO may want a class for user name rather than using raw strings
        self.owner = _not_falsy(owner, 'owner')
        self.admin = tuple(_not_falsy_in_iterable([] if admin is None else admin, 'admin'))
        self.write = tuple(_not_falsy_in_iterable([] if write is None else write, 'write'))
        self.read = tuple(_not_falsy_in_iterable([] if read is None else read, 'read'))

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.owner == self.owner
                    and other.admin == self.admin
                    and other.write == self.write
                    and other.read == self.read)
        return NotImplemented

    def __hash__(self):
        return hash((self.owner, self.admin, self.write, self.read))
