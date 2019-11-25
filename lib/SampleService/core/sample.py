'''
Contains classes related to samples.
'''

from uuid import UUID
from SampleService.core.util import not_falsy

# for now we'll assume people are nice and don't change attributes after init.
# if that doesn't hold true, override __setattr__.


# TODO test

class Sample:
    '''
    A sample containing biological replicates, technical replicates, and sub samples.
    '''

    def __init__(self, name: str = None):
        '''
        Create the the sample.
        :param name: The name of the sample.
        '''
        # TODO restrictions on name
        self.name = name


class SampleWithID(Sample):
    '''
    A sample including an ID.
    '''

    def __init__(self, id_: UUID, name: str = None):
        '''
        Create the sample.
        :param id_': The ID of the sample.
        :param name: The name of the sample.
        '''
        super().__init__(name)
        self.id = not_falsy(id_, 'id_')
