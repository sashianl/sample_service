from uuid import UUID
from SampleService.core.util import not_falsy

# for now we'll assume people are nice and don't change attributes after init.
# if that doesn't hold true, override __setattr__.


# TODO docs
# TODO test

class Sample:

    def __init__(self, name: str = None):
        # TODO restrictions on name
        self.name = name


class SampleWithID(Sample):

    def __init__(self, id_: UUID, name: str = None):
        super().__init__(name)
        self.id = not_falsy(id_, 'id_')
