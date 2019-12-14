'''
An ArangoDB based storage system for the Sample service.
'''


#                          READ BEFORE CHANGING STUFF
#
# There are a number of specialized steps that take place in the
# sample saving process and sample access process in order to ensure a consistent db state.
#
# The process is:
# 1) save all the node documents with the integer version = -1 and a UUID version.
# 2) save all the node edges.
# 3) save the version document with the integer version = -1 and the same UUID version.
# 4) save all the version edges.
# 5) if a new sample:
#        save the sample document with the UUID version in the ordered version list.
#    else:
#        add the UUID version to the end of the sample document's version list.
# 6) Update the integer version on the node documents.
# 7) Update the integer version on the version document.
#
# When accessing data, the data access methods should look for versions == -1 and correct the
# database appropriately:
#
# * If the UUID version exists in the sample document version list,
#   set the node and version documents for that UUID version to the index position + 1 of the UUID
#   version in the sample document version list.
#
# * If not or if the sample document does not exist at all *AND* an amount time has
#   passed such that it is reasonable that another process saving the sample has completed
#   (hardcoded to 1h at the time of this writing), delete all the nodes and the version document
#   with the corresponding UUID version.
#
# This process means that if a server or the database goes down in the middle of a write, it
# is always possible to correct the database as long as one server is running.
#
# The server runs correction code on startup and every minute if start_consistency_checker is
# called with default arguments.
#
# There are two choices for how to deal with node and version documents with an integer version
# of -1 in new code:
# 1) Fix it. This is what get_sample() does - take a look at that code for an example.
# 2) Ignore any documents (and their respective edges) with a -1 version. Effectively, they
#    currently don't exist in the db. In time, they'll be removed or updated to the correct
#    version but the current process doesn't care.
#
#  DO NOT expose documents containing a -1 version outside the db API.
#
# Alternatives considered (not exhaustive):
# * Just autoincrement the version in the sample document and then save the version and node docs.
#   This is simpler, but since the autoincrement has to happen first, if the save fails there
#   could be missing nodes or the version could be missing entirely, and so effectively a
#   dangling pointer is created.
# * Use transactions.
#   Unfortunately transactions aren't atomic in a sharded cluster as of arango 3.5:
#   https://www.arangodb.com/docs/stable/transactions-limitations.html#in-clusters
#

# Future programmers: This application is designed to be run in a sharded environment, so new
# unique indexes CANNOT be added unless the shard key is switched from _key to the new field.

# may need to extract an interface at some point, YAGNI for now.

import arango as _arango
import datetime
import hashlib as _hashlib
import uuid as _uuid
from typing import List as _List, cast as _cast, Optional as _Optional, Callable

from uuid import UUID
from apscheduler.schedulers.background import BackgroundScheduler as _BackgroundScheduler
from arango.database import StandardDatabase
from SampleService.core.acls import SampleACL
from SampleService.core.sample import SampleWithID
from SampleService.core.sample import SampleNode as _SampleNode, SubSampleType as _SubSampleType
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.errors import ConcurrencyError as _ConcurrencyError
from SampleService.core.errors import NoSuchSampleError as _NoSuchSampleError
from SampleService.core.errors import NoSuchSampleVersionError as _NoSuchSampleVersionError
from SampleService.core.storage.errors import SampleStorageError as _SampleStorageError
from SampleService.core.storage.errors import StorageInitException as _StorageInitException

_FLD_ARANGO_KEY = '_key'
_FLD_ARANGO_FROM = '_from'
_FLD_ARANGO_TO = '_to'
_FLD_ID = 'id'
_FLD_UUID_VER = 'uuidver'
_FLD_VER = 'ver'
_VAL_NO_VER = -1
_FLD_NAME = 'name'
_FLD_SAVE_TIME = 'saved'

_FLD_NODE_NAME = 'name'
_FLD_NODE_TYPE = 'type'
_FLD_NODE_PARENT = 'parent'
_FLD_NODE_SAMPLE_ID = 'id'
_FLD_NODE_VER = 'ver'
_FLD_NODE_UUID_VER = 'uuidver'
_FLD_NODE_INDEX = 'index'

_FLD_ACLS = 'acls'
_FLD_OWNER = 'owner'
_FLD_READ = 'read'
_FLD_WRITE = 'write'
_FLD_ADMIN = 'admin'

_FLD_VERSIONS = 'vers'

_JOB_ID = 'consistencyjob'

# schema version checking constants.

# the current version of the database schema.
_SCHEMA_VERSION = 1
# the value for the schema key.
_SCHEMA_VALUE = 'schema'
# whether the schema is in the process of an update. Value is a boolean.
_FLD_SCHEMA_UPDATE = 'inupdate'
# the version of the schema. Value is _SCHEMA_VERSION.
_FLD_SCHEMA_VERSION = 'schemaver'


class ArangoSampleStorage:
    '''
    The ArangoDB storage wrapper.
    '''

    def __init__(
            self,
            db: StandardDatabase,
            sample_collection: str,
            version_collection: str,
            version_edge_collection: str,
            node_collection: str,
            node_edge_collection: str,
            schema_collection: str,
            now: Callable[[], datetime.datetime] = lambda: datetime.datetime.now(
                tz=datetime.timezone.utc)):
        '''
        Create the wrapper.
        Note that the database consistency checker will not start until the
        start_consistency_checker() method is called.

        :param db: the ArangoDB database in which data will be stored.
        :param sample_collection: the name of the collection in which to store sample documents.
        :param version_collection: the name of the collection in which to store sample version
            documents.
        :param version_edges_collection: the name of the collection in which edges from sample
            versions to samples will be stored.
        :param node_collection: the name of the collection in which nodes for a sample version
            will be stored.
        :param version_edges_collection: the name of the collection in which edges from sample
            nodes to sample nodes (or versions in the case of root nodes) will be stored.
        '''
        # Don't publicize these params
        # :param now: A callable that returns the current time. Primarily used for testing.
        # Maybe make a configuration class...?
        # TODO take workspace shadow object collection & check indexes exist, don't create
        _not_falsy(db, 'db')
        _not_falsy(now, 'now')
        self._db = db
        self._now = now
        self._col_sample = _init_collection(
            db, sample_collection, 'sample collection', 'sample_collection')
        self._col_version = _init_collection(
            db, version_collection, 'version collection', 'version_collection')
        self._col_ver_edge = _init_collection(
            db, version_edge_collection, 'version edge collection', 'version_edge_collection',
            edge=True)
        self._col_nodes = _init_collection(
            db, node_collection, 'node collection', 'node_collection')
        self._col_node_edge = _init_collection(
            db, node_edge_collection, 'node edge collection', 'node_edge_collection', edge=True)
        self._col_schema = _init_collection(
            db, schema_collection, 'schema collection', 'schema_collection')
        self._ensure_indexes()
        self._check_schema()
        self._deletion_delay = datetime.timedelta(hours=1)  # make configurable?
        self._check_db_updated()
        self._scheduler = self._build_scheduler()

    def _ensure_indexes(self):
        try:
            self._col_node_edge.add_persistent_index([_FLD_UUID_VER])
            self._col_ver_edge.add_persistent_index([_FLD_UUID_VER])
            self._col_version.add_persistent_index([_FLD_UUID_VER])
            self._col_version.add_persistent_index([_FLD_VER])  # partial index would be useful
            self._col_nodes.add_persistent_index([_FLD_UUID_VER])
            self._col_nodes.add_persistent_index([_FLD_VER])  # partial index would be useful
        except _arango.exceptions.IndexCreateError as e:
            # this is a real pain to test.
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _check_schema(self):
        col = self._col_schema
        try:
            col.insert({_FLD_ARANGO_KEY: _SCHEMA_VALUE,
                        _FLD_SCHEMA_UPDATE: False,
                        _FLD_SCHEMA_VERSION: _SCHEMA_VERSION})
        except _arango.exceptions.DocumentInsertError as e:
            if e.error_code != 1210:  # unique constraint violation code
                # this is a real pain to test
                raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        # ok, the schema version document is already there, this isn't the first time this
        # database as been used. Now check the document is ok.
        try:
            if col.count() != 1:
                raise _StorageInitException(
                    'Multiple config objects found in the database. ' +
                    'This should not happen, something is very wrong.')
            cfgdoc = col.get(_SCHEMA_VALUE)
            if cfgdoc[_FLD_SCHEMA_VERSION] != _SCHEMA_VERSION:
                raise _StorageInitException(
                    f'Incompatible database schema. Server is v{_SCHEMA_VERSION}, ' +
                    f'DB is v{cfgdoc[_FLD_SCHEMA_VERSION]}')
            if cfgdoc[_FLD_SCHEMA_UPDATE]:
                raise _StorageInitException(
                    'The database is in the middle of an update from ' +
                    f'v{cfgdoc[_FLD_SCHEMA_VERSION]} of the schema. Aborting startup.')
        except _arango.exceptions.ArangoServerError as e:
            # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _check_db_updated(self):
        self._check_col_updated(self._col_version)
        self._check_col_updated(self._col_nodes)

    def _check_col_updated(self, col):
        # this should rarely find unupdated documents so don't worry too much about performance
        try:
            cur = col.find({_FLD_VER: _VAL_NO_VER})
            for doc in cur:
                id_ = UUID(doc[_FLD_ID])
                uver = UUID(doc[_FLD_UUID_VER])
                ts = self._timestamp_to_datetime(doc[_FLD_SAVE_TIME])
                sampledoc = self._get_sample_doc(id_, exception=False)
                if not sampledoc:
                    # the sample document was never saved for this version doc
                    self._delete_version_and_node_docs(uver, ts, self._deletion_delay)
                else:
                    version = self._get_int_version_from_sample_doc(sampledoc, str(uver))
                    if version:
                        self._update_version_and_node_docs_with_find(id_, uver, version)
                    else:
                        self._delete_version_and_node_docs(uver, ts, self._deletion_delay)
        except _arango.exceptions.DocumentGetError as e:
            # this is a real pain to test.
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _get_int_version_from_sample_doc(self, sampledoc, uuidverstr):
        for i, v in enumerate(sampledoc[_FLD_VERSIONS]):
            if v == uuidverstr:
                return i + 1
        return None

    def _delete_version_and_node_docs(self, uuidver, savedate, deletion_delay):
        if self._now() - savedate > self._deletion_delay:
            print('deleting docs', self._now(), savedate, self._deletion_delay)
            try:
                # TODO logging
                # delete edge docs first to ensure we don't orphan them
                self._col_ver_edge.delete_match({_FLD_UUID_VER: str(uuidver)})
                self._col_version.delete_match({_FLD_UUID_VER: str(uuidver)})
                self._col_node_edge.delete_match({_FLD_UUID_VER: str(uuidver)})
                self._col_nodes.delete_match({_FLD_UUID_VER: str(uuidver)})
            except _arango.exceptions.DocumentDeleteError as e:
                # this is a real pain to test.
                raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _build_scheduler(self):
        schd = _BackgroundScheduler()
        schd.add_job(self._check_db_updated, 'interval', seconds=1, id=_JOB_ID)
        schd.start(paused=True)
        return schd

    def start_consistency_checker(self, interval_sec=60):
        '''
        Start the database consistency checker. In production use the consistency checker
        should always be on.

        :param interval_ms: How frequently to run the scheduler in seconds. Defaults to one minute.
        '''
        if interval_sec < 1:
            raise ValueError('interval_sec must be > 0')
        self._scheduler.reschedule_job(_JOB_ID, trigger='interval', seconds=interval_sec)
        self._scheduler.resume()

    def stop_consistency_checker(self):
        '''
        Stop the consistency checker.
        '''
        self._scheduler.pause()

    def save_sample(self, user_name: str, sample: SampleWithID) -> bool:
        '''
        Save a new sample. The version in the sample object, if any, is ignored.

        The timestamp in the sample is expected to be accurate - the database may become corrupted
        if this is not the case.

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

        versionid = _uuid.uuid4()

        self._save_version_and_node_docs(sample, versionid)

        # create sample document, adding uuid to version list
        tosave = {_FLD_ARANGO_KEY: str(sample.id),
                  # yes, this is redundant. It'll match the ver & node collectons though
                  _FLD_ID: str(sample.id),  # TODO test this is saved
                  _FLD_VERSIONS: [str(versionid)],
                  _FLD_ACLS: {_FLD_OWNER: user_name,
                              _FLD_ADMIN: [],
                              _FLD_WRITE: [],
                              _FLD_READ: []
                              }
                  }
        try:
            self._col_sample.insert(tosave)
        except _arango.exceptions.DocumentInsertError as e:
            # we'll let the reaper clean up any left over docs
            if e.error_code == 1210:  # unique constraint violation code
                return False
            else:  # this is a real pain to test.
                raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        self._update_version_and_node_docs(sample, versionid, 1)
        return True

    def _update_version_and_node_docs(self, sample: SampleWithID, versionid: UUID, version: int):
        nodeupdates: _List[dict] = []
        for n in sample.nodes:
            ndoc = {_FLD_ARANGO_KEY: self._get_node_id(sample.id, versionid, n.name),
                    _FLD_NODE_VER: version,
                    }
            nodeupdates.append(ndoc)
        self._update_many(self._col_nodes, nodeupdates)

        verdocid = self._get_version_id(sample.id, versionid)
        self._update(self._col_version, {_FLD_ARANGO_KEY: verdocid, _FLD_VER: version})

    def _update_version_and_node_docs_with_find(self, id_: UUID, versionid: UUID, version: int):
        try:
            self._col_nodes.update_match({_FLD_UUID_VER: str(versionid)}, {_FLD_VER: version})
        except _arango.exceptions.DocumentUpdateError as e:
            # this is a real pain to test.
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

        verdocid = self._get_version_id(id_, versionid)
        self._update(self._col_version, {_FLD_ARANGO_KEY: verdocid, _FLD_VER: version})

    def _save_version_and_node_docs(self, sample: SampleWithID, versionid: UUID):
        verdocid = self._get_version_id(sample.id, versionid)

        nodedocs: _List[dict] = []
        nodeedgedocs: _List[dict] = []
        for index, n in enumerate(sample.nodes):
            key = self._get_node_id(sample.id, versionid, n.name)
            ndoc = {_FLD_ARANGO_KEY: key,
                    _FLD_NODE_SAMPLE_ID: str(sample.id),
                    _FLD_NODE_UUID_VER: str(versionid),
                    _FLD_NODE_VER: _VAL_NO_VER,
                    _FLD_SAVE_TIME: sample.savetime.timestamp(),
                    _FLD_NODE_NAME: n.name,
                    _FLD_NODE_TYPE: n.type.name,
                    _FLD_NODE_PARENT: n.parent,
                    _FLD_NODE_INDEX: index,
                    }
            if n.type == _SubSampleType.BIOLOGICAL_REPLICATE:
                to = f'{self._col_version.name}/{verdocid}'
            else:
                parentid = self._get_node_id(sample.id, versionid, _cast(str, n.parent))
                to = f'{self._col_nodes.name}/{parentid}'
            nedoc = {_FLD_ARANGO_KEY: key,
                     _FLD_UUID_VER: str(versionid),
                     _FLD_ARANGO_FROM: f'{self._col_nodes.name}/{key}',
                     _FLD_ARANGO_TO: to
                     }
            nodedocs.append(ndoc)
            nodeedgedocs.append(nedoc)
        self._insert_many(self._col_nodes, nodedocs)
        # TODO this actually isn't tested by anything since we're not doing traversals yet, but
        # it will be
        self._insert_many(self._col_node_edge, nodeedgedocs)

        # save version document
        verdoc = {_FLD_ARANGO_KEY: verdocid,
                  _FLD_ID: str(sample.id),
                  _FLD_VER: _VAL_NO_VER,
                  _FLD_UUID_VER: str(versionid),
                  _FLD_SAVE_TIME: sample.savetime.timestamp(),
                  _FLD_NAME: sample.name
                  # TODO description
                  }
        self._insert(self._col_version, verdoc)

        # TODO this actually isn't tested by anything since we're not doing traversals yet, but
        # it will be
        veredgedoc = {_FLD_ARANGO_KEY: verdocid,
                      _FLD_UUID_VER: str(versionid),
                      _FLD_ARANGO_FROM: f'{self._col_version.name}/{verdocid}',
                      _FLD_ARANGO_TO: f'{self._col_sample.name}/{sample.id}',
                      }
        self._insert(self._col_ver_edge, veredgedoc)

    def _insert(self, col, doc):
        try:
            col.insert(doc, silent=True)
        except _arango.exceptions.DocumentInsertError as e:  # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _insert_many(self, col, docs):
        try:
            col.insert_many(docs, silent=True)
        except _arango.exceptions.DocumentInsertError as e:  # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _update(self, col, doc):
        try:
            col.update(doc, silent=True)
        except _arango.exceptions.DocumentUpdateError as e:  # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _update_many(self, col, docs):
        try:
            col.update_many(docs, silent=True)
        except _arango.exceptions.DocumentUpdateError as e:  # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def save_sample_version(self, sample: SampleWithID, prior_version: int = None) -> int:
        '''
        Save a new version of a sample. The sample must already exist in the DB. Any version in
        the provided sample object is ignored.

        The timestamp in the sample is expected to be accurate - the database may become corrupted
        if this is not the case.

        No permissions checking is performed.
        :param sample: The new version of the sample.
        :param prior_version: If the sample version is not equal to this value, the save will fail.
        :return: the version of the saved sample.
        :raises SampleStorageError: if the sample fails to save.
        :raises ConcurrencyError: if the sample's version is not equal to prior_version.
        '''
        _not_falsy(sample, 'sample')
        if prior_version is not None and prior_version < 1:
            raise ValueError('prior_version must be > 0')
        sampledoc = self._get_sample_doc(sample.id, exception=False)
        if not sampledoc:
            raise _NoSuchSampleError(str(sample.id))  # bail early
        version = len(sampledoc[_FLD_VERSIONS])
        if prior_version and version != prior_version:
            raise _ConcurrencyError(f'Version required for sample {sample.id} is ' +
                                    f'{prior_version}, but current version is {version}')

        return self._save_sample_version_pt2(sample, prior_version)

    # this method is separated so we can test the race condition case where a sample version
    # is incremented after the check above.
    def _save_sample_version_pt2(self, sample, prior_version) -> int:

        versionid = _uuid.uuid4()

        self._save_version_and_node_docs(sample, versionid)

        aql = f'''
            FOR s IN @@col
                FILTER s.{_FLD_ARANGO_KEY} == @sampleid'''
        if prior_version:
            aql += f'''
                FILTER LENGTH(s.{_FLD_VERSIONS}) == @version_count'''
        aql += f'''
                UPDATE s WITH {{{_FLD_VERSIONS}: PUSH(s.{_FLD_VERSIONS}, @verid)}} IN @@col
                    RETURN NEW
            '''

        try:
            # we checked that the doc existed above, so it must exist now.
            # We assume here that you cannot delete samples from the DB. That's the plan as of now.
            bind_vars = {'@col': self._col_sample.name,
                         'sampleid': str(sample.id),
                         'verid': str(versionid),
                         }
            if prior_version:
                bind_vars['version_count'] = prior_version
            cur = self._db.aql.execute(aql, bind_vars=bind_vars)
            if not cur.empty():
                version = len(cur.next()[_FLD_VERSIONS])
            else:
                sampledoc = _cast(dict, self._get_sample_doc(sample.id))
                version = len(sampledoc[_FLD_VERSIONS])
                # so theoretically there could be a race condition within the race condition such
                # that the aql doesn't find the doc, then the version gets incremented, and the
                # version is ok here. That'll take millisecond timing though and the result is
                # one spurious error so we don't worry about it for now.
                raise _ConcurrencyError(f'Version required for sample {sample.id} is ' +
                                        f'{prior_version}, but current version is {version}')
        except _arango.exceptions.AQLQueryExecuteError as e:
            # let the reaper clean up any left over docs
            # this is a real pain to test.
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

        self._update_version_and_node_docs(sample, versionid, version)
        return version

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
        doc = _cast(dict, self._get_sample_doc(id_))
        maxver = len(doc[_FLD_VERSIONS])
        version = version if version else maxver
        if version > maxver:
            raise _NoSuchSampleVersionError(f'{id_} ver {version}')
        verdoc = self._get_version_doc(id_, doc[_FLD_VERSIONS][version - 1])
        if verdoc[_FLD_VER] == _VAL_NO_VER:
            # since the version id came from the sample doc, the implication
            # is that the db or server lost connection before the version could be updated
            # and the reaper hasn't caught it yet, so we go ahead and fix it.
            self._update_version_and_node_docs_with_find(id_, verdoc[_FLD_UUID_VER], version)

        nodes = self._get_nodes(id_, UUID(verdoc[_FLD_NODE_UUID_VER]), version)
        dt = self._timestamp_to_datetime(verdoc[_FLD_SAVE_TIME])

        return SampleWithID(UUID(doc[_FLD_ID]), nodes, dt, verdoc[_FLD_NAME], version)

    def _timestamp_to_datetime(self, ts: float) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)

    def _get_version_id(self, id_: UUID, ver: UUID):
        return f'{id_}_{ver}'

    def _get_node_id(self, id_: UUID, ver: UUID, node_id: str):
        # arango keys can be at most 254B and only a few characters are allowed, so we MD5
        # the node name for length and safe characters
        # https://www.arangodb.com/docs/stable/data-modeling-naming-conventions-document-keys.html
        return f'{id_}_{ver}_{_hashlib.md5(node_id.encode("utf-8")).hexdigest()}'

    # assumes args are not None, and ver came from the sample doc in the db.
    def _get_version_doc(self, id_: UUID, ver: UUID) -> dict:
        try:
            doc = self._col_version.get(self._get_version_id(id_, ver))
        except _arango.exceptions.DocumentGetError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        if not doc:
            raise _SampleStorageError(f'Corrupt DB: Missing version {ver} for sample {id_}')
        return doc

    # assumes ver came from the sample doc in the db.
    def _get_nodes(self, id_: UUID, ver: UUID, version: int) -> _List[_SampleNode]:
        # this class controls the version ID, and since it's a UUID we can assume it's unique
        # across all versions of all samples
        try:
            nodedocs = self._col_nodes.find({_FLD_NODE_UUID_VER: str(ver)})
            if not nodedocs:
                raise _SampleStorageError(
                    f'Corrupt DB: Missing nodes for version {ver} of sample {id_}')
            index_to_node = {}
            for n in nodedocs:
                if n[_FLD_VER] == _VAL_NO_VER:
                    # since it's assumed the version id came from the sample doc, the implication
                    # is that the db or server lost connection before the version could be updated
                    # and the reaper hasn't caught it yet, so we go ahead and fix it.
                    self._update_version_and_node_docs_with_find(id_, ver, version)
                index_to_node[n[_FLD_NODE_INDEX]] = _SampleNode(
                    n[_FLD_NODE_NAME],
                    _SubSampleType[n[_FLD_NODE_TYPE]],
                    n[_FLD_NODE_PARENT])
        except _arango.exceptions.DocumentGetError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        # could check for keyerror here if nodes were deleted, but db is corrupt either way
        # so YAGNI.
        # Could add a node count to the version... but how about we just assume the db works
        nodes = [index_to_node[i] for i in range(len(index_to_node))]
        return nodes

    def _get_sample_doc(self, id_: UUID, exception: bool = True) -> _Optional[dict]:
        try:
            doc = self._col_sample.get(str(_not_falsy(id_, 'id_')))
        except _arango.exceptions.DocumentGetError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        if not doc:
            if exception:
                raise _NoSuchSampleError(str(id_))
            return None
        return doc

    def get_sample_acls(self, id_: UUID) -> SampleACL:
        '''
        Get a sample's acls from the database.
        :param id_: the ID of the sample.
        :returns: the sample acls.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises SampleStorageError: if the sample could not be retrieved.
        '''
        # return no class for now, might need later
        doc = _cast(dict, self._get_sample_doc(id_))
        acls = doc[_FLD_ACLS]
        return SampleACL(acls[_FLD_OWNER], acls[_FLD_ADMIN], acls[_FLD_WRITE], acls[_FLD_READ])

    def replace_sample_acls(self, id_: UUID, acls: SampleACL):
        '''
        Completely replace a sample's ACLs.

        :param id_: the sample's ID.
        :param acls: the new ACLs.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises SampleStorageError: if the sample could not be retrieved.
        '''
        doc = {_FLD_ARANGO_KEY: str(_not_falsy(id_, 'id_')),
               _FLD_ACLS: {_FLD_OWNER: _not_falsy(acls, 'acls').owner,
                           _FLD_ADMIN: acls.admin,
                           _FLD_WRITE: acls.write,
                           _FLD_READ: acls.read}
               }
        try:
            self._col_sample.update(doc, silent=True)
        except _arango.exceptions.DocumentUpdateError as e:
            if e.error_code == 1202:  # doc not found code
                raise _NoSuchSampleError(str(id_))
            # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    # TODO change acls with more granularity


# if an edge is inserted into a non-edge collection _from and _to are silently dropped
def _init_collection(database, collection, collection_name, collection_variable_name, edge=False):
    c = database.collection(_check_string(collection, collection_variable_name))
    if not c.properties()['edge'] is edge:  # this is a http call
        ctype = 'an edge' if edge else 'a vertex'
        raise _StorageInitException(f'{collection_name} {collection} is not {ctype} collection')
    return c
