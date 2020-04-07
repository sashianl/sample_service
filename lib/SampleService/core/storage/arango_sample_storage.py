'''
An ArangoDB based storage system for the Sample service.
'''


#                          READ BEFORE CHANGING STUFF
#
# There are a number of specialized steps that take place in the
# sample saving process and sample access process in order to ensure a consistent db state.
#
# Note that the UUID version is internal to the database layer only and must not be exposed
# ouside of this layer
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
# * If not or if the sample document does not exist at all *AND* an amount of time has
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
from uuid import UUID
from collections import defaultdict
from typing import List as _List, cast as _cast, Optional as _Optional, Callable
from typing import Dict as _Dict, Any as _Any, Tuple as _Tuple

from apscheduler.schedulers.background import BackgroundScheduler as _BackgroundScheduler
from arango.database import StandardDatabase

from SampleService.core.acls import SampleACL
from SampleService.core.core_types import PrimitiveType as _PrimitiveType
from SampleService.core.data_link import DataLink
from SampleService.core.sample import SavedSample
from SampleService.core.sample import SampleNode as _SampleNode, SubSampleType as _SubSampleType
from SampleService.core.sample import SampleNodeAddress as _SampleNodeAddress
from SampleService.core.sample import SampleAddress as _SampleAddress
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.errors import ConcurrencyError as _ConcurrencyError
from SampleService.core.errors import DataLinkExistsError as _DataLinkExistsError
from SampleService.core.errors import NoSuchLinkError as _NoSuchLinkError
from SampleService.core.errors import NoSuchSampleError as _NoSuchSampleError
from SampleService.core.errors import NoSuchSampleVersionError as _NoSuchSampleVersionError
from SampleService.core.errors import TooManyDataLinksError as _TooManyDataLinksError
from SampleService.core.storage.errors import SampleStorageError as _SampleStorageError
from SampleService.core.storage.errors import StorageInitError as _StorageInitError
from SampleService.core.storage.errors import OwnerChangedError as _OwnerChangedError
from SampleService.core.workspace import DataUnitID, UPA as _UPA

_FLD_ARANGO_KEY = '_key'
_FLD_ARANGO_FROM = '_from'
_FLD_ARANGO_TO = '_to'
_FLD_ID = 'id'
_FLD_UUID_VER = 'uuidver'
_FLD_VER = 'ver'
_VAL_NO_VER = -1
_FLD_NAME = 'name'
_FLD_USER = 'user'
_FLD_SAVE_TIME = 'saved'

_FLD_NODE_NAME = 'name'
_FLD_NODE_TYPE = 'type'
_FLD_NODE_PARENT = 'parent'
_FLD_NODE_SAMPLE_ID = 'id'
_FLD_NODE_VER = 'ver'
_FLD_NODE_UUID_VER = 'uuidver'
_FLD_NODE_INDEX = 'index'
_FLD_NODE_CONTROLLED_METADATA = 'cmeta'
_FLD_NODE_UNCONTROLLED_METADATA = 'ucmeta'
_FLD_NODE_META_OUTER_KEY = 'ok'
_FLD_NODE_META_KEY = 'k'
_FLD_NODE_META_VALUE = 'v'


_FLD_ACLS = 'acls'
_FLD_OWNER = 'owner'
_FLD_READ = 'read'
_FLD_WRITE = 'write'
_FLD_ADMIN = 'admin'

_FLD_VERSIONS = 'vers'

_FLD_LINK_ID = 'id'
_FLD_LINK_WORKSPACE_ID = 'wsid'
_FLD_LINK_OBJECT_ID = 'objid'
_FLD_LINK_OBJECT_VERSION = 'objver'
_FLD_LINK_OBJECT_DATA_UNIT = 'dataid'
_FLD_LINK_SAMPLE_ID = 'sampleid'
_FLD_LINK_SAMPLE_UUID_VERSION = 'samuuidver'
_FLD_LINK_SAMPLE_INT_VERSION = 'samintver'
_FLD_LINK_SAMPLE_NODE = 'node'
_FLD_LINK_CREATED = 'created'
_FLD_LINK_EXPIRES = 'expired'

# see https://www.arangodb.com/2018/07/time-traveling-with-graph-databases/
_ARANGO_MAX_INTEGER = 2**53 - 1

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
            workspace_object_version_shadow_collection: str,
            data_link_collection: str,
            schema_collection: str,
            # See https://kbase.slack.com/archives/CNRT78G66/p1583967289053500 for justification
            max_links: int = 10000,
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
        :param version_edge_collection: the name of the collection in which edges from sample
            versions to samples will be stored.
        :param node_collection: the name of the collection in which nodes for a sample version
            will be stored.
        :param node_edge_collection: the name of the collection in which edges from sample
            nodes to sample nodes (or versions in the case of root nodes) will be stored.
        :param workspace_object_version_shadow_collection: The name of the collection where the
            KBase Relation Engine stores shadow object versions.
        :param data_link_collection: the name of the collection in which edges from workspace
            object version to sample nodes will be stored, indicating data links.
        :schema_collection: the name of the collection in which information about the database
            schema will be stored.
        '''
        # Don't publicize these params, for testing only
        # :param max_links: The maximum links any one sample version or workspace object version
        # can have.
        # :param now: A callable that returns the current time. Primarily used for testing.
        # Maybe make a configuration class...?
        self._db = _not_falsy(db, 'db')
        self._now = _not_falsy(now, 'now')
        self._max_links = max_links

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
        self._col_ws = _init_collection(
            db,
            workspace_object_version_shadow_collection,
            'workspace object version shadow collection',
            'workspace_object_version_shadow_collection')
        self._col_data_link = _init_collection(
            db, data_link_collection, 'data link collection', 'data_link_collection', edge=True)
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
            # find links by ID
            self._col_data_link.add_persistent_index([_FLD_LINK_ID])
            # find links from objects
            self._col_data_link.add_persistent_index(
                [_FLD_LINK_WORKSPACE_ID, _FLD_LINK_OBJECT_ID, _FLD_LINK_OBJECT_VERSION])
            # find links from samples
            self._col_data_link.add_persistent_index(
                [_FLD_LINK_SAMPLE_ID, _FLD_LINK_SAMPLE_UUID_VERSION])
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
                raise _StorageInitError(
                    'Multiple config objects found in the database. ' +
                    'This should not happen, something is very wrong.')
            cfgdoc = col.get(_SCHEMA_VALUE)
            if cfgdoc[_FLD_SCHEMA_VERSION] != _SCHEMA_VERSION:
                raise _StorageInitError(
                    f'Incompatible database schema. Server is v{_SCHEMA_VERSION}, ' +
                    f'DB is v{cfgdoc[_FLD_SCHEMA_VERSION]}')
            if cfgdoc[_FLD_SCHEMA_UPDATE]:
                raise _StorageInitError(
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
                    self._delete_version_and_node_docs(uver, ts)
                else:
                    version = self._get_int_version_from_sample_doc(sampledoc, str(uver))
                    if version:
                        self._update_version_and_node_docs_with_find(id_, uver, version)
                    else:
                        self._delete_version_and_node_docs(uver, ts)
        except _arango.exceptions.DocumentGetError as e:
            # this is a real pain to test.
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _get_int_version_from_sample_doc(self, sampledoc, uuidverstr):
        for i, v in enumerate(sampledoc[_FLD_VERSIONS]):
            if v == uuidverstr:
                return i + 1
        return None

    def _delete_version_and_node_docs(self, uuidver, savedate):
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

    def save_sample(self, sample: SavedSample) -> bool:
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
        if self._get_sample_doc(sample.id, exception=False):
            return False  # bail early
        return self._save_sample_pt2(sample)

    # this method is separated so we can test the race condition case where a sample with the
    # same ID is saved after the check above.
    def _save_sample_pt2(self, sample: SavedSample) -> bool:

        versionid = _uuid.uuid4()

        self._save_version_and_node_docs(sample, versionid)

        # create sample document, adding uuid to version list
        tosave = {_FLD_ARANGO_KEY: str(sample.id),
                  # yes, this is redundant. It'll match the ver & node collectons though
                  _FLD_ID: str(sample.id),  # TODO test this is saved
                  _FLD_VERSIONS: [str(versionid)],
                  _FLD_ACLS: {_FLD_OWNER: sample.user,
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

    def _update_version_and_node_docs(self, sample: SavedSample, versionid: UUID, version: int):
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

    def _save_version_and_node_docs(self, sample: SavedSample, versionid: UUID):
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
                    _FLD_NODE_CONTROLLED_METADATA: self._meta_to_list(n.controlled_metadata),
                    _FLD_NODE_UNCONTROLLED_METADATA: self._meta_to_list(n.user_metadata),
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
                  _FLD_USER: sample.user,
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

    # TODO may need to make a meta collection. See below.
    # Can only use equality comparisons on arrays:
    # https://www.arangodb.com/docs/stable/indexing-index-basics.html#indexing-array-values
    # Maybe need a doc for each metadata value, which implies going through the whole
    # save / version update routine the other docs go through
    # But that means you can't query for metadata on traversals
    def _meta_to_list(self, m: _Dict[str, _Dict[str, _PrimitiveType]]) -> _List[_Dict[str, _Any]]:
        ret = []
        for k in m:
            ret.extend([{_FLD_NODE_META_OUTER_KEY: k,
                         _FLD_NODE_META_KEY: ik,
                         _FLD_NODE_META_VALUE: m[k][ik]}
                        for ik in m[k]]
                       )
        return ret

    def _list_to_meta(self, l: _List[_Dict[str, _Any]]) -> _Dict[str, _Dict[str, _PrimitiveType]]:
        ret: _Dict[str, _Dict[str, _PrimitiveType]] = defaultdict(dict)
        for m in l:
            ret[m[_FLD_NODE_META_OUTER_KEY]][m[_FLD_NODE_META_KEY]] = m[_FLD_NODE_META_VALUE]
        return dict(ret)  # some libs don't play nice with default dict, in particular maps

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

    def save_sample_version(self, sample: SavedSample, prior_version: int = None) -> int:
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

    def get_sample(self, id_: UUID, version: int = None) -> SavedSample:
        '''
        Get a sample from the database.
        :param id_: the ID of the sample.
        :param version: The version of the sample to retrieve. Defaults to the latest version.
        :returns: the sample.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises NoSuchSampleVersionError: if the sample version does not exist.
        :raises SampleStorageError: if the sample could not be retrieved.
        '''
        doc, verdoc, version = self._get_sample_and_version_doc(id_, version)

        nodes = self._get_nodes(id_, UUID(verdoc[_FLD_NODE_UUID_VER]), version)
        dt = self._timestamp_to_datetime(verdoc[_FLD_SAVE_TIME])

        return SavedSample(
            UUID(doc[_FLD_ID]), verdoc[_FLD_USER], nodes, dt, verdoc[_FLD_NAME], version)

    def _get_sample_and_version_doc(
            self, id_: UUID, version: _Optional[int] = None) -> _Tuple[dict, dict, int]:
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
        return (doc, verdoc, version)

    def _timestamp_to_datetime(self, ts: float) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)

    def _get_version_id(self, id_: UUID, ver: UUID):
        return f'{id_}_{ver}'

    def _get_node_id(self, id_: UUID, ver: UUID, node_id: str):
        # arango keys can be at most 254B and only a few characters are allowed, so we MD5
        # the node name for length and safe characters
        # https://www.arangodb.com/docs/stable/data-modeling-naming-conventions-document-keys.html
        return f'{id_}_{ver}_{self._md5(node_id)}'

    def _md5(self, string: str):
        return _hashlib.md5(string.encode("utf-8")).hexdigest()

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
                    n[_FLD_NODE_PARENT],
                    self._list_to_meta(n[_FLD_NODE_CONTROLLED_METADATA]),
                    self._list_to_meta(n[_FLD_NODE_UNCONTROLLED_METADATA]),
                    )
        except _arango.exceptions.DocumentGetError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        # could check for keyerror here if nodes were deleted, but db is corrupt either way
        # so YAGNI.
        # Could add a node count to the version... but how about we just assume the db works
        nodes = [index_to_node[i] for i in range(len(index_to_node))]
        return nodes

    def _get_sample_doc(self, id_: UUID, exception: bool = True) -> _Optional[dict]:
        doc = self._get_doc(self._col_sample, str(_not_falsy(id_, 'id_')))
        if not doc:
            if exception:
                raise _NoSuchSampleError(str(id_))
            return None
        return doc

    def _get_doc(self, col, id_: str) -> _Optional[dict]:
        try:
            return col.get(id_)
        except _arango.exceptions.DocumentGetError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

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

        The owner may not be changed via this method, but is required to ensure the owner has
        not changed since the acls were retrieved from the database. If the current owner is not
        the same as the owner in the SampleACLs, the save will fail. This prevents race conditions
        from resulting in a user existing in both the owner acl and another acl.

        :param id_: the sample's ID.
        :param acls: the new ACLs.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises SampleStorageError: if the sample could not be retrieved.
        :raises OwnerChangedException: if the owner in the database is not the same as the owner
            in the provided ACLs.
        '''
        _not_falsy(id_, 'id_')
        _not_falsy(acls, 'acls')

        # could return a subset of s to save bandwith
        aql = f'''
            FOR s in @@col
                FILTER s.{_FLD_ARANGO_KEY} == @id
                FILTER s.{_FLD_ACLS}.{_FLD_OWNER} == @owner
                UPDATE s WITH {{{_FLD_ACLS}: MERGE(s.{_FLD_ACLS}, @acls)}} IN @@col
                RETURN s
            '''
        bind_vars = {'@col': self._col_sample.name,
                     'id': str(id_),
                     'owner': acls.owner,
                     'acls': {_FLD_ADMIN: acls.admin,
                              _FLD_WRITE: acls.write,
                              _FLD_READ: acls.read
                              }
                     }
        try:
            cur = self._db.aql.execute(aql, bind_vars=bind_vars, count=True)
            if not cur.count():
                # assume cur.count() is never > 1 as we're filtering on _key
                self._get_sample_doc(id_)  # will raise exception if document does not exist
                raise _OwnerChangedError()
        except _arango.exceptions.AQLQueryExecuteError as e:  # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    # TODO change acls with more granularity

    def create_data_link(self, link: DataLink):
        '''
        Link data in the workspace to a sample.
        Each data unit can be linked to only one sample at a time. Expired links may exist to
        other samples.

        Uniqueness of the link ID is required but not enforced. The caller is responsible for
        enforcement. If this contract is violated, the get_data_link method may behave
        unexpectedly.

        No checking is done on whether the user has permissions to link the data or whether the
        data or sample node exists.

        :param link: the link to save.

        :raises NoSuchSampleError: if the sample does not exist.
        :raises NoSuchSampleVersionError: if the sample version does not exist.
        :raises DataLinkExistsError: if a link already exists from the data unit.
        :raises TooManyDataLinksError: if there are too many links from the sample version or
            the workspace object version.
        '''
        # TODO DATALINK behavior for deleted objects?
        # TODO DATALINK notes re listing expired links - not scalable
        # TODO DATALINK update link - do last. can temporarily do with expire + create, just
        # need to put in transaction.
        # TODO DATALINK expire link
        # TODO DATALINK list samples linked to ws object
        # TODO DATALINK list ws objects linked to sample
        # TODO DATALINK may make sense to check for node existence here, make call after writing
        # next layer up

        # may want to link non-ws data at some point, would need a data source ID? YAGNI for now

        # Using the REST streaming api for the transaction. Might be faster with javascript
        # server side implementation, but this is easier to read, easier to understand, and easier
        # to implement. Switch to js if performance becomes an issue.

        # Might want a bulk method for peformance improvement, should take measurements at some
        # point.

        # For the current link from the DUID, the _key is the DUID. This ensures there's only 1
        # extant link per DUID. For expired links, the expiration time is added to the _key.
        # Since only one link should exist at any one time, this should make the _key unique.
        # Since _keys have a maxium length of 254 chars and the dataid of the DUID may be up to
        # 256 characters and may contain illegal characters, it is MD5'd. See
        # https://www.arangodb.com/docs/stable/data-modeling-naming-conventions-document-keys.html

        _not_falsy(link, 'link')
        sna = link.sample_node_address
        # need to get the version doc to ensure the documents have been updated appropriately,
        # see comments at beginning of file
        _, versiondoc, _ = self._get_sample_and_version_doc(sna.sampleid, sna.version)
        samplever = UUID(versiondoc[_FLD_UUID_VER])

        # makes a db specifically for this transaction
        tdb = self._db.begin_transaction(
            read=self._col_data_link.name,
            # Need exclusive as we're counting docs and making decisions based on that number
            # Write only checks for write collisions on specific docs, so count could change
            # during transaction
            exclusive=self._col_data_link.name)

        try:
            # makes a collection specifically for this transaction
            tdlc = tdb.collection(self._col_data_link.name)
            if self._has_doc(tdlc, self._create_link_key(link.duid)):
                raise _DataLinkExistsError(str(link.duid))

            if self._count_links_from_ws_object(
                    tdb, link.duid.upa, link.create, link.expire) >= self._max_links:
                raise _TooManyDataLinksError(
                    f'More than {self._max_links} links from workpace object {link.duid.upa}')
            if self._count_links_from_sample_ver(
                    tdb, sna.sampleid, samplever, link.create, link.expire) >= self._max_links:
                raise _TooManyDataLinksError(
                    f'More than {self._max_links} links from sample {sna.sampleid} ' +
                    f'version {sna.version}')

            ldoc = self._create_link_doc(link, samplever)
            self._insert(tdlc, ldoc)
            try:
                tdb.commit_transaction()
            except _arango.exceptions.TransactionCommitError as e:  # dunno how to test this
                # may want some retry logic here, YAGNI for now
                raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        finally:
            if tdb.transaction_status() != 'committed':
                try:
                    tdb.abort_transaction()
                except _arango.exceptions.TransactionAbortError as e:  # dunno how to test this
                    # this will mask the previous error, but if this fails probably the DB
                    # connection is hosed
                    raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _has_doc(self, col, id_):
        # may want exception thrown at some point?
        try:
            return col.has(id_)
        except _arango.exceptions.DocumentInError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _count_links_from_ws_object(
            self,
            db,
            upa: _UPA,
            create: datetime.datetime,
            expire: _Optional[datetime.datetime]):
        wsc = self._count_links(
            db,
            f'''
                    FILTER d.{_FLD_LINK_WORKSPACE_ID} == @wsid
                    FILTER d.{_FLD_LINK_OBJECT_ID} == @objid
                    FILTER d.{_FLD_LINK_OBJECT_VERSION} == @ver''',
            {'wsid': upa.wsid, 'objid': upa.objid, 'ver': upa.version},
            create,
            expire)
        return wsc

    def _count_links_from_sample_ver(
            self,
            db,
            sample: UUID,
            version: UUID,
            create: datetime.datetime,
            expire: _Optional[datetime.datetime]):
        sv = self._count_links(
            db,
            f'''
                    FILTER d.{_FLD_LINK_SAMPLE_ID} == @sid
                    FILTER d.{_FLD_LINK_SAMPLE_UUID_VERSION} == @sver''',
            {'sid': str(sample), 'sver': str(version)},
            create,
            expire)
        return sv

    def _count_links(self, db, filters: str, bind_vars, create, expire):
        bind_vars['@col'] = self._col_data_link.name
        bind_vars['create'] = create.timestamp()
        bind_vars['expire'] = expire = expire.timestamp() if expire else _ARANGO_MAX_INTEGER
        # might need to include create / expire in compound indexes if we get a ton of expired
        # links. Might not work in a NOT though. Alternate formulation is
        # (d.create >= @create AND d.create <= @expire) OR
        # (d.expire >= @create AND d.expire <= @expire)
        q = (f'''
                FOR d in @@col
             ''' +
             filters +
             f'''
                    FILTER NOT (d.{_FLD_LINK_EXPIRES} < @create OR d.{_FLD_LINK_CREATED} > @expire)
                    COLLECT WITH COUNT INTO linkcount
                    RETURN linkcount
             ''')
        try:
            cur = db.aql.execute(q, bind_vars=bind_vars)
            return cur.next()
        except _arango.exceptions.AQLQueryExecuteError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _create_link_key(self, duid: DataUnitID):
        dataid = f'_{self._md5(duid.dataid)}' if duid.dataid else ''
        return f'{duid.upa.wsid}_{duid.upa.objid}_{duid.upa.version}{dataid}'

    def _create_link_doc(self, link: DataLink, samplever: UUID):
        sna = link.sample_node_address
        upa = link.duid.upa
        nodeid = self._get_node_id(sna.sampleid, samplever, sna.node)
        # see https://github.com/kbase/relation_engine_spec/blob/4a9dc6df2088763a9df88f0b018fa5c64f2935aa/schemas/ws/ws_object_version.yaml#L17  # noqa
        from_ = f'{self._col_ws.name}/{upa.wsid}:{upa.objid}:{upa.version}'
        return {
            _FLD_ARANGO_KEY: self._create_link_key(link.duid),
            _FLD_ARANGO_FROM: from_,
            _FLD_ARANGO_TO: f'{self._col_nodes.name}/{nodeid}',
            _FLD_LINK_CREATED: link.create.timestamp(),
            _FLD_LINK_EXPIRES: link.expire.timestamp() if link.expire else _ARANGO_MAX_INTEGER,
            _FLD_LINK_ID: str(link.id),
            _FLD_LINK_WORKSPACE_ID: upa.wsid,
            _FLD_LINK_OBJECT_ID: upa.objid,
            _FLD_LINK_OBJECT_VERSION: upa.version,
            _FLD_LINK_OBJECT_DATA_UNIT: link.duid.dataid,
            _FLD_LINK_SAMPLE_ID: str(sna.sampleid),
            _FLD_LINK_SAMPLE_UUID_VERSION: str(samplever),
            # recording the integer version saves looking it up in the version doc and it's
            # immutable so denormalization is ok here
            _FLD_LINK_SAMPLE_INT_VERSION: sna.version,
            _FLD_LINK_SAMPLE_NODE: sna.node
        }

    def get_data_link(self, id_: UUID) -> DataLink:
        '''
        Get a link by its ID.

        :param id_: the link ID.
        :returns: the link.
        :raises NoSuchLinkError: if the link does not exist.
        '''
        # if delete/hid samples added may need some more logic here
        try:
            cur = self._col_data_link.find({_FLD_LINK_ID: str(_not_falsy(id_, 'id_'))}, limit=2)
            if cur.count() == 0:
                raise _NoSuchLinkError(str(id_))
            if cur.count() > 1:
                raise ValueError(f'More than one data link found for ID {id_}')
            doc = cur.next()
            cur.close(True)
        except _arango.exceptions.DocumentGetError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        return self._doc_to_link(doc)

    def _doc_to_link(self, doc) -> DataLink:
        ex = doc[_FLD_LINK_EXPIRES]
        return DataLink(
            UUID(doc[_FLD_LINK_ID]),
            DataUnitID(
                _UPA(wsid=doc[_FLD_LINK_WORKSPACE_ID],
                     objid=doc[_FLD_LINK_OBJECT_ID],
                     version=doc[_FLD_LINK_OBJECT_VERSION]),
                doc[_FLD_LINK_OBJECT_DATA_UNIT]),
            _SampleNodeAddress(
                _SampleAddress(
                    UUID(doc[_FLD_LINK_SAMPLE_ID]),
                    doc[_FLD_LINK_SAMPLE_INT_VERSION]),
                doc[_FLD_LINK_SAMPLE_NODE]),
            self._timestamp_to_datetime(doc[_FLD_LINK_CREATED]),
            None if ex == _ARANGO_MAX_INTEGER else self._timestamp_to_datetime(ex)
        )


# if an edge is inserted into a non-edge collection _from and _to are silently dropped
def _init_collection(database, collection, collection_name, collection_variable_name, edge=False):
    c = database.collection(_check_string(collection, collection_variable_name))
    if not c.properties()['edge'] is edge:  # this is a http call
        ctype = 'an edge' if edge else 'a vertex'
        raise _StorageInitError(f'{collection_name} {collection} is not {ctype} collection')
    return c
