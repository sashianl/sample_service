# may need to extract an interface at some point, YAGNI for now.

import arango as _arango
from uuid import UUID
from arango.database import StandardDatabase
from SampleService.core.sample import SampleWithID
from SampleService.core.util import not_falsy as _not_falsy
from SampleService.core.errors import NoSuchSampleError as _NoSuchSampleError
from SampleService.core.storage.errors import SampleStorageError as _SampleStorageError


# TODO docs
# TODO test

_FLD_ARANGO_KEY = '_key'
_FLD_ID = 'id'
_FLD_NAME = 'name'

_FLD_ACLS = 'acls'
_FLD_OWNER = 'owner'
_FLD_READ = 'read'
_FLD_WRITE = 'write'
_FLD_ADMIN = 'admin'

_FLD_VERSIONS = 'vers'

# TODO transaction pt1 on startup, check for missing int vers on nodes & versions & fix (log).
# TODO transaction pt2 delete any docs > 1 hr old w/ ver uuids not in uuid list for sample doc
# TODO check indexes
# TODO check schema

# Notes for calling classes:
# a sample doc could still be added after this, so we need to save and expect a duplicate
# to occur, in which case we check perms and fail if no perms.


class ArangoSampleStorage:

    def __init__(
            self,
            db: StandardDatabase,
            sample_collection: str):
        # TODO create indexes for collections
        # TODO take workspace shadow object collection & check indexes exist, don't create
        self._col_sample = _not_falsy(db, 'db').collection(
            _not_falsy(sample_collection, 'sample_collection'))

    # True = saved, false = sample exists
    def save_sample(self, user_name: str, sample: SampleWithID) -> bool:
        _not_falsy(user_name, 'user_name')
        _not_falsy(sample, 'sample')

        # create version uuid
        # save nodes
        # save version
        # create sample document, adding uuid to version list
        tosave = {_FLD_ACLS: {_FLD_OWNER: user_name,
                              _FLD_ADMIN: [],
                              _FLD_WRITE: [],
                              _FLD_READ: []
                              },
                  _FLD_ARANGO_KEY: str(sample.id),
                  # yes, this is redundant. It'll match the ver & node collectons though
                  _FLD_ID: str(sample.id),
                  _FLD_VERSIONS: []  # TODO add version here
                  }

        try:
            self._col_sample.insert(tosave, silent=True)
        except _arango.exceptions.DocumentInsertError as e:
            if e.error_code == 1005:
                return False
            else:
                raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        return True
        # get int version from list index
        # update version & nodes with int version
        # start at root of nodes, progress to leaves, last is version doc

    def save_sample_version(self, sample: SampleWithID):

        # TODO opticoncur pt1 take a prior sample version, ensure that the new version is
        # TODO opticoncur pt2 ver + 1 for optimistic concurrency, so 0 for new object
        raise NotImplementedError

    def get_sample(self, id_: UUID, version: int = None) -> SampleWithID:
        doc = self._get_sample_doc(id_)
        return SampleWithID(UUID(doc[_FLD_ID]))

    def _get_sample_doc(self, id_: UUID):
        try:
            doc = self._col_sample.get(str(_not_falsy(id_, 'id_')))
        except _arango.exceptions.DocumentGetError as e:
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        if not doc:
            raise _NoSuchSampleError(str(id_))
        return doc

    def get_sample_acls(self, id_: UUID):
        # return no class for now, might need later
        doc = self._get_sample_doc(id_)
        acls = doc[_FLD_ACLS]
        # this is kind of redundant, but makes it easier to change the keys later
        # if we want to change the api
        return {
            _FLD_OWNER: acls[_FLD_OWNER],
            _FLD_ADMIN: acls[_FLD_ADMIN],
            _FLD_WRITE: acls[_FLD_WRITE],
            _FLD_READ: acls[_FLD_READ],
        }
