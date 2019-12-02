'''
An ArangoDB based storage system for the Sample service.
'''

# may need to extract an interface at some point, YAGNI for now.

import arango as _arango
import uuid as _uuid
from uuid import UUID
from arango.database import StandardDatabase
from SampleService.core.sample import SampleWithID
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.errors import NoSuchSampleError as _NoSuchSampleError
from SampleService.core.errors import NoSuchSampleVersionError as _NoSuchSampleVersionError
from SampleService.core.storage.errors import SampleStorageError as _SampleStorageError
from SampleService.core.storage.errors import StorageInitException as _StorageInitExecption


_FLD_ARANGO_KEY = '_key'
_FLD_ARANGO_FROM = '_from'
_FLD_ARANGO_TO = '_to'
_FLD_ID = 'id'
_FLD_UUID_VER = 'uuidver'
_FLD_VER = 'ver'
_VAL_NO_VER = -1
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

# TODO document that collections are never created so that admins can set sharding

# Notes for calling classes:
# a sample doc could still be added after this, so we need to save and expect a duplicate
# to occur, in which case we check perms and fail if no perms.


class ArangoSampleStorage:
    '''
    The ArangoDB storage wrapper.
    '''

    def __init__(
            self,
            db: StandardDatabase,
            sample_collection: str,
            version_collection: str,
            version_edge_collection: str,):
        '''
        Create the wrapper.
        :param db: the ArangoDB database in which data will be stored.
        :param sample_collection: the name of the collection in which to store sample documents.
        :param version_collection: the name of the collection in which to store sample version
            documents.
        :param version_edges_collection: the name of the collection in which edges from sample
            versions to samples will be stored.
        '''
        # Maybe make a configuration class...?
        # TODO create indexes for collections
        # TODO take workspace shadow object collection & check indexes exist, don't create
        _not_falsy(db, 'db')
        self._col_sample = _init_collection(db, sample_collection, 'sample_collection')
        self._col_version = _init_collection(db, version_collection, 'version_collection')
        self._col_ver_edge = _init_collection(
            db, version_edge_collection, 'version_edge_collection', edge=True)

    # True = saved, false = sample exists
    def save_sample(self, user_name: str, sample: SampleWithID) -> bool:
        '''
        Save a new sample.
        Sample nodes MUST have unique IDs or the save will fail.
        :param user_name: The user that is creating the sample.
        :param sample: The sample to save.
        :returns: True if the sample saved successfully, False if the same ID already exists.
        :raises SampleStorageError: if the sample fails to save.
        '''
        # TODO think about user name a bit. Make a class?
        _not_falsy(sample, 'sample')
        _not_falsy(user_name, 'user_name')
        if self._get_sample_doc(sample.id, exception=False):
            return False  # bail early
        return self._save_sample_pt2(user_name, sample)

    # this method is separated so we can test the race condition case where a sample with the
    # same ID is saved after the check above.
    def _save_sample_pt2(self, user_name: str, sample: SampleWithID) -> bool:
        verid = _uuid.uuid4()

        # TODO explain why save works as it does
        # TODO save nodes

        # save version document
        verdocid = self._get_version_id(sample.id, verid)
        verdoc = {_FLD_ARANGO_KEY: verdocid,
                  _FLD_ID: str(sample.id),
                  _FLD_VER: _VAL_NO_VER,
                  _FLD_UUID_VER: str(verid),
                  _FLD_NAME: sample.name
                  # TODO description
                  }
        self._insert(self._col_version, verdoc)

        # this actually isn't tested by anything since we're not doing traversals yet, but
        # it will be
        # save version edge
        veredgedoc = {_FLD_ARANGO_KEY: verdocid,
                      _FLD_ARANGO_FROM: f'{self._col_ver_edge.name}/{verdocid}',
                      _FLD_ARANGO_TO: f'{self._col_sample.name}/{sample.id}',
                      }
        self._insert(self._col_ver_edge, veredgedoc)

        # create sample document, adding uuid to version list
        tosave = {_FLD_ARANGO_KEY: str(sample.id),
                  # yes, this is redundant. It'll match the ver & node collectons though
                  _FLD_ID: str(sample.id),
                  _FLD_VERSIONS: [str(verid)],
                  _FLD_ACLS: {_FLD_OWNER: user_name,
                              _FLD_ADMIN: [],
                              _FLD_WRITE: [],
                              _FLD_READ: []
                              }
                  }
        try:
            self._col_sample.insert(tosave)
        except _arango.exceptions.DocumentInsertError as e:
            if e.error_code == 1210:  # unique constraint violation code
                # TODO clean up any other created docs
                return False
            else:  # this is a real pain to test.
                raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        ver = 1
        # update nodes with int version
        # start at root of nodes, progress to leaves, last is version doc
        self._update(self._col_version, {_FLD_ARANGO_KEY: verdocid, _FLD_VER: ver})

        # TODO DBFIX PT1 add thread to check for missing versions & fix
        # TODO DBFIX PT2 or del if no version in root doc & > 1hr old
        return True

    def _insert(self, col, doc):
        try:
            col.insert(doc, silent=True)
        except _arango.exceptions.DocumentInsertError as e:  # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _update(self, col, doc):
        try:
            col.update(doc, silent=True)
        except _arango.exceptions.DocumentUpdateError as e:  # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def save_sample_version(self, sample: SampleWithID):
        # TODO DOCS

        # TODO opticoncur pt1 take a prior sample version, ensure that the new version is
        # TODO opticoncur pt2 ver + 1 for optimistic concurrency, so 0 for new object
        raise NotImplementedError

    def get_sample(self, id_: UUID, version: int = None) -> SampleWithID:
        '''
        Get a sample from the database.
        :param id_: the ID of the sample.
        :param version: The version of the sample to retrieve. Defaults to the latest version.
        :returns: the sample.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises NoSuchSampleVersionError: if the sample version does not exist.
        :raises SampleStorageError: if the sample could not be retrieved.
        '''
        # TODO TEST version fail
        doc = self._get_sample_doc(id_)
        maxver_idx = len(doc[_FLD_VERSIONS])
        version = version if version else maxver_idx
        if version > maxver_idx:
            raise _NoSuchSampleVersionError(f'{id_} ver {version}')
        verdoc = self._get_version_doc(id_, doc[_FLD_VERSIONS][version - 1])
        # TODO if verdoc version = _NO_VERSION do what?
        return SampleWithID(UUID(doc[_FLD_ID]), verdoc[_FLD_NAME], version)

    def _get_version_id(self, id_: UUID, ver: UUID):
        return f'{id_}_{ver}'

    # assumes args are not None, and ver came from the sample doc in the db.
    def _get_version_doc(self, id_: UUID, ver: UUID):
        try:
            doc = self._col_version.get(self._get_version_id(id_, ver))
        except _arango.exceptions.DocumentGetError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        if not doc:
            raise _SampleStorageError(f'Corrupt DB: Missing version {ver} for sample {id_}')
        return doc

    def _get_sample_doc(self, id_: UUID, exception: bool = True):
        try:
            doc = self._col_sample.get(str(_not_falsy(id_, 'id_')))
        except _arango.exceptions.DocumentGetError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        if not doc:
            if exception:
                raise _NoSuchSampleError(str(id_))
            return None
        return doc

    def get_sample_acls(self, id_: UUID):
        '''
        Get a sample's acls from the database.
        :param id_: the ID of the sample.
        :returns: the sample acls as a dict.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises SampleStorageError: if the sample could not be retrieved.
        '''
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

    # TODO change acls


# if an edge is inserted into a non-edge collection _from and _to are silently dropped
def _init_collection(database, collection, collection_name, edge=False):
    c = database.collection(_check_string(collection, collection_name))
    if not c.properties()['edge'] is edge:  # this is a http call
        ctype = 'an edge' if edge else 'a vertex'
        raise _StorageInitExecption(f'{collection} is not {ctype} collection')
    return c
