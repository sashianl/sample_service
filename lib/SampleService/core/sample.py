'''
Contains classes related to samples.
'''

from uuid import UUID
from typing import Optional
from SampleService.core.arg_checkers import not_falsy, check_string

# for now we'll assume people are nice and don't change attributes after init.
# if that doesn't hold true, override __setattr__.


_MAX_SAMPLE_NAME_LEN = 255


class Sample:
    '''
    A sample containing biological replicates, technical replicates, and sub samples.
    '''

    def __init__(self, name: Optional[str] = None):
        '''
        Create the the sample.
        :param name: The name of the sample. Cannot contain control characters or be longer than
            255 characters.
        '''
        self.name = check_string(name, 'name', max_len=_MAX_SAMPLE_NAME_LEN, optional=True)

    def __eq__(self, other):
        if type(other) is type(self):
            return other.name == self.name
        return NotImplemented

    def __hash__(self):
        return hash((self.name,))


class SampleWithID(Sample):
    '''
    A sample including an ID.
    '''

    def __init__(self, id_: UUID, name: Optional[str] = None):
        '''
        Create the sample.
        :param id': The ID of the sample.
        :param name: The name of the sample. Cannot contain control characters or be longer than
            255 characters.
        '''
        super().__init__(name)
        self.id = not_falsy(id_, 'id_')

    def __eq__(self, other):
        if type(other) is type(self):
            return other.id == self.id and other.name == self.name
        return NotImplemented

    def __hash__(self):
        return hash((self.id, self.name))
