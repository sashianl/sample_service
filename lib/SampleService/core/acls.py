'''
Classes and methods for working with sample ACLs.
'''

from typing import List
from SampleService.core.arg_checkers import not_falsy as _not_falsy


class SampleACL:

    def __init__(
            self,
            owner: str,
            admin: List[str] = None,
            write: List[str] = None,
            read: List[str] = None):
        # TODO may want a class for user name rather than using raw strings
        self.owner = _not_falsy(owner, 'owner')

