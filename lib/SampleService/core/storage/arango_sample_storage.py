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
import uuid as _uuid  # lgtm [py/import-and-import-from]
from uuid import UUID
from collections import defaultdict
from typing import List, Tuple, Callable, cast as _cast, Optional, Sequence as _Sequence
from typing import Dict as _Dict, Any as _Any

from apscheduler.schedulers.background import BackgroundScheduler as _BackgroundScheduler
from arango.database import StandardDatabase

from SampleService.core.acls import SampleACL, SampleACLDelta
from SampleService.core.core_types import PrimitiveType as _PrimitiveType
from SampleService.core.data_link import DataLink
from SampleService.core.sample import (
    SavedSample,
    SampleAddress,
    SourceMetadata as _SourceMetadata,
    SampleNode as _SampleNode,
    SubSampleType as _SubSampleType,
    SampleNodeAddress as _SampleNodeAddress,
)
from SampleService.core.arg_checkers import (
    not_falsy as _not_falsy,
    not_falsy_in_iterable as _not_falsy_in_iterable,
    check_string as _check_string,
    check_timestamp as _check_timestamp,
)
from SampleService.core.errors import (
    ConcurrencyError as _ConcurrencyError,
    DataLinkExistsError as _DataLinkExistsError,
    NoSuchLinkError as _NoSuchLinkError,
    NoSuchSampleError as _NoSuchSampleError,
    NoSuchSampleVersionError as _NoSuchSampleVersionError,
    NoSuchSampleNodeError as _NoSuchSampleNodeError,
    TooManyDataLinksError as _TooManyDataLinksError,
)
from SampleService.core.storage.errors import SampleStorageError as _SampleStorageError
from SampleService.core.storage.errors import StorageInitError as _StorageInitError
from SampleService.core.storage.errors import OwnerChangedError as _OwnerChangedError
from SampleService.core.user import UserID
from SampleService.core.workspace import DataUnitID, UPA

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
_FLD_ACL_UPDATE_TIME = 'aclupdate'

_FLD_NODE_NAME = 'name'
_FLD_NODE_TYPE = 'type'
_FLD_NODE_PARENT = 'parent'
_FLD_NODE_SAMPLE_ID = 'id'
_FLD_NODE_VER = 'ver'
_FLD_NODE_UUID_VER = 'uuidver'
_FLD_NODE_INDEX = 'index'
_FLD_NODE_CONTROLLED_METADATA = 'cmeta'
_FLD_NODE_UNCONTROLLED_METADATA = 'ucmeta'
_FLD_NODE_SOURCE_METADATA = 'smeta'
_FLD_NODE_META_OUTER_KEY = 'ok'
_FLD_NODE_META_KEY = 'k'
_FLD_NODE_META_SOURCE_KEY = 'sk'
_FLD_NODE_META_VALUE = 'v'


_FLD_ACLS = 'acls'
_FLD_OWNER = 'owner'
_FLD_READ = 'read'
_FLD_WRITE = 'write'
_FLD_ADMIN = 'admin'
_FLD_PUBLIC_READ = 'pubread'

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
_FLD_LINK_CREATED_BY = 'createby'
_FLD_LINK_EXPIRED = 'expired'
_FLD_LINK_EXPIRED_BY = 'expireby'

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
        self._reaper_deletion_delay = datetime.timedelta(hours=1)  # make configurable?
        self._reaper_update_delay = datetime.timedelta(minutes=5)  # make configurable?
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
            # find links from sample versions
            self._col_data_link.add_persistent_index([_FLD_LINK_SAMPLE_UUID_VERSION])
            # find links from samples
            self._col_data_link.add_persistent_index([_FLD_LINK_SAMPLE_ID])
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
                ts = self._timestamp_to_datetime(self._timestamp_milliseconds_to_seconds(doc[_FLD_SAVE_TIME]))
                sampledoc = self._get_sample_doc(id_, exception=False)
                if not sampledoc:
                    # the sample document was never saved for this version doc
                    self._delete_version_and_node_docs(uver, ts)
                else:
                    # Do not update this document if it was last updated less than _reaper_update_delay ago
                    # this is to avoid writing to a document in the process of being created
                    if self._now() - ts < self._reaper_update_delay:
                        continue
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
        if self._now() - savedate > self._reaper_deletion_delay:
            print('deleting docs', self._now(), savedate, self._reaper_deletion_delay)
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
                  _FLD_ACL_UPDATE_TIME: sample.savetime.timestamp(),
                  _FLD_ACLS: {_FLD_OWNER: sample.user.id,
                              _FLD_ADMIN: [],
                              _FLD_WRITE: [],
                              _FLD_READ: [],
                              _FLD_PUBLIC_READ: False
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
        nodeupdates: List[dict] = []
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

        nodedocs: List[dict] = []
        nodeedgedocs: List[dict] = []
        for index, n in enumerate(sample.nodes):
            key = self._get_node_id(sample.id, versionid, n.name)
            ndoc = {_FLD_ARANGO_KEY: key,
                    _FLD_NODE_SAMPLE_ID: str(sample.id),
                    _FLD_NODE_UUID_VER: str(versionid),
                    _FLD_NODE_VER: _VAL_NO_VER,
                    _FLD_SAVE_TIME: self._timestamp_seconds_to_milliseconds(sample.savetime.timestamp()),
                    _FLD_NODE_NAME: n.name,
                    _FLD_NODE_TYPE: n.type.name,
                    _FLD_NODE_PARENT: n.parent,
                    _FLD_NODE_INDEX: index,
                    _FLD_NODE_CONTROLLED_METADATA: self._meta_to_list(n.controlled_metadata),
                    _FLD_NODE_UNCONTROLLED_METADATA: self._meta_to_list(n.user_metadata),
                    _FLD_NODE_SOURCE_METADATA: self._source_meta_to_list(n.source_metadata),
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
                  _FLD_USER: sample.user.id,
                  _FLD_VER: _VAL_NO_VER,
                  _FLD_UUID_VER: str(versionid),
                  _FLD_SAVE_TIME: self._timestamp_seconds_to_milliseconds(sample.savetime.timestamp()),
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
    def _meta_to_list(self, m: _Dict[str, _Dict[str, _PrimitiveType]]) -> List[_Dict[str, _Any]]:
        ret = []
        for k in m:
            ret.extend([{_FLD_NODE_META_OUTER_KEY: k,
                         _FLD_NODE_META_KEY: ik,
                         _FLD_NODE_META_VALUE: m[k][ik]}
                        for ik in m[k]]
                       )
        return ret

    def _list_to_meta(
            self, list_: List[_Dict[str, _Any]]) -> _Dict[str, _Dict[str, _PrimitiveType]]:
        ret: _Dict[str, _Dict[str, _PrimitiveType]] = defaultdict(dict)
        for m in list_:
            ret[m[_FLD_NODE_META_OUTER_KEY]][m[_FLD_NODE_META_KEY]] = m[_FLD_NODE_META_VALUE]
        return dict(ret)  # some libs don't play nice with default dict, in particular maps

    # source metadata is informational only and is not expected to be queryable
    def _source_meta_to_list(self, sm: _Sequence[_SourceMetadata]) -> List[_Dict[str, _Any]]:
        return [{_FLD_NODE_META_KEY: m.key,
                 _FLD_NODE_META_SOURCE_KEY: m.sourcekey,
                 _FLD_NODE_META_VALUE: dict(m.sourcevalue)} for m in sm]

    def _list_to_source_meta(self, list_: List[_Dict[str, _Any]]) -> List[_SourceMetadata]:
        # allow for compatibility with old samples without a source meta field
        if not list_:
            return []
        return [_SourceMetadata(
            sm[_FLD_NODE_META_KEY],
            sm[_FLD_NODE_META_SOURCE_KEY],
            sm[_FLD_NODE_META_VALUE]
        ) for sm in list_]

    def _insert(self, col, doc, upsert=False):
        try:
            col.insert(doc, silent=True, overwrite=upsert)
        except _arango.exceptions.DocumentInsertError as e:  # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _insert_many(self, col, docs):
        try:
            col.insert_many(docs, silent=True)
        except _arango.exceptions.DocumentInsertError as e:  # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _update(self, col, doc):
        try:
            col.update(doc, silent=True, keep_none=True)
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
        dt = self._timestamp_to_datetime(self._timestamp_milliseconds_to_seconds(verdoc[_FLD_SAVE_TIME]))

        return SavedSample(
            UUID(doc[_FLD_ID]), UserID(verdoc[_FLD_USER]), nodes, dt, verdoc[_FLD_NAME], version)

    def get_samples(self, ids_: List[_Dict[str, _Any]]) -> List[SavedSample]:
        '''
        ids_: list of dictionaries containing "id" and "version" field.
        '''
        aql = '''
            // Extract the ids from the input.
            LET ids = (
                FOR idver IN @ids
                RETURN idver.id
            )
            // Create a lookup table by id to select desired versions.
            LET verlookup = MERGE(
                FOR idver in @ids
                RETURN {[idver.id]: idver.version}
            )
            // Filter first to optimize aggregation.
            LET svs = (
                FOR sv IN @@version
                    FILTER sv.id IN ids
                    RETURN sv
            )
            // Select the specified version of each sample.
            LET partials = (
                FOR sv IN svs
                    FILTER sv.ver == verlookup[sv.id]
                    RETURN {
                        id: sv.id,
                        version: sv.ver,
                        verdoc: sv.uuidver,
                        version_record: sv
                    }
            )
            // For each sample, traverse and collect node trees.
            LET node_trees = (
                FOR startVertex IN @@nodes
                    FILTER startVertex.id IN ids
                    FOR v IN ANY startVertex @@nodes_edge
                        FILTER v.index >= 0
                        SORT v.index
                        COLLECT nid = v.id INTO ns
                        RETURN {[nid]: UNIQUE(ns[*].v)}
            )
            // Include node trees in samples.
            FOR partial in partials
                FOR node in @@nodes
                    FILTER node.uuidver == partial.verdoc
                        AND node.parent == NULL
                    RETURN MERGE(partial, {
                        "node_tree": MERGE(node_trees)[node.id] OR [node]
                    })
        '''
        # Convert UUID to strings.
        for id_ in ids_:
            id_['id'] = str(id_['id'])

        aql_bind = {
            'ids': ids_,
            '@nodes': self._col_nodes.name,
            '@nodes_edge': self._col_node_edge.name,
            '@version': self._col_version.name
        }
        # Convert arango doc structure into SavedSample.
        results = {}
        for doc in self._db.aql.execute(aql, bind_vars=aql_bind):
            node_tree = [
                _SampleNode(
                    node[_FLD_NODE_NAME],
                    _SubSampleType[node[_FLD_NODE_TYPE]],
                    node[_FLD_NODE_PARENT],
                    self._list_to_meta(node[_FLD_NODE_CONTROLLED_METADATA]),
                    self._list_to_meta(node[_FLD_NODE_UNCONTROLLED_METADATA]),
                    self._list_to_source_meta(node.get(_FLD_NODE_SOURCE_METADATA, [])),
                )
                for node in doc['node_tree']
            ]
            verdoc = doc['version_record']
            dt = self._timestamp_to_datetime(
                self._timestamp_milliseconds_to_seconds(verdoc[_FLD_SAVE_TIME])
            )
            docid = doc[_FLD_ID]
            results[docid] = SavedSample(
                UUID(docid),
                UserID(verdoc[_FLD_USER]),
                node_tree, dt, verdoc[_FLD_NAME], doc['version']
            )
        # Return samples in the order they were requested.
        return [results[id_['id']] for id_ in ids_]

    def _get_sample_and_version_doc(
            self, id_: UUID, version: Optional[int] = None) -> Tuple[dict, dict, int]:
        doc, uuidversion, version = self._get_sample_doc_and_versions(id_, version)
        verdoc = self._get_version_doc(id_, uuidversion)
        if verdoc[_FLD_VER] == _VAL_NO_VER:
            # since the version id came from the sample doc, the implication
            # is that the db or server lost connection before the version could be updated
            # and the reaper hasn't caught it yet, so we go ahead and fix it.
            self._update_version_and_node_docs_with_find(id_, verdoc[_FLD_UUID_VER], version)
        return (doc, verdoc, version)

    def _get_sample_doc_and_versions(
            self, id_: UUID, version: Optional[int] = None) -> Tuple[dict, UUID, int]:
        doc = _cast(dict, self._get_sample_doc(id_))
        maxver = len(doc[_FLD_VERSIONS])
        version = version if version else maxver
        if version > maxver:
            raise _NoSuchSampleVersionError(f'{id_} ver {version}')
        return doc, UUID(doc[_FLD_VERSIONS][version - 1]), version

    def _get_many_sample_and_version_doc(self, ids_: List[_Dict[str, _Any]]) -> Tuple[_Dict[str, dict], _Dict[str, dict], _Dict[str, int]]:
        docs, versions = self._get_many_sample_doc_and_versions(ids_)
        verdocs = self._get_many_version_docs([(id_, UUID(doc[_FLD_VERSIONS][versions[id_] - 1])) for id_, doc in docs.items()])  # sends id and version id
        return (docs, verdocs, versions)

    def _get_many_sample_doc_and_versions(self, ids_: List[_Dict[str, _Any]]) -> Tuple[_Dict[str, dict], _Dict[str, int]]:
        docs = [_cast(dict, doc) for doc in self._get_many_sample_doc(ids_)]
        ret_docs = {}
        ret_versions = {}
        for id_ver in ids_:
            id_ = id_ver['id']
            version = id_ver['version']
            # match to document

        for doc in docs:
            # get doc id
            id_ = doc['id']
            maxver = len(doc[_FLD_VERSIONS])
            version = version if version else maxver
            if version > maxver:
                raise _NoSuchSampleError(f"{id_} ver {version}")
            ret_versions[id_] = version
            ret_docs[id_] = doc
        return ret_docs, ret_versions

    def _timestamp_to_datetime(self, ts: float) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)

    def _timestamp_seconds_to_milliseconds(self, ts: float) -> int:
        return round(ts * 1000)

    def _timestamp_milliseconds_to_seconds(self, ts: int) -> float:
        return ts / 1000

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

    def _get_many_version_docs(self, ids_, exception: bool=True) -> _Dict[str, dict]:
        version_ids = [self._get_version_id(id_, ver) for id_, ver in ids_]
        ver_docs = self._get_many_docs(self._col_version, version_ids)
        if not ver_docs:
            if exception:
                raise _NoSuchSampleError(f"Could not complete search for samples: {[str(id_['id']) for id_ in ids_]}")
            # return None
        return {ver_doc['id']: ver_doc for ver_doc in ver_docs}

    # assumes ver came from the sample doc in the db.
    def _get_nodes(self, id_: UUID, ver: UUID, version: int) -> List[_SampleNode]:
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
                    # allow for compatatibility with old samples without a source meta field
                    self._list_to_source_meta(n.get(_FLD_NODE_SOURCE_METADATA)),
                    )
        except _arango.exceptions.DocumentGetError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        # could check for keyerror here if nodes were deleted, but db is corrupt either way
        # so YAGNI.
        # Could add a node count to the version... but how about we just assume the db works
        nodes = [index_to_node[i] for i in range(len(index_to_node))]
        return nodes

    def _get_sample_doc(self, id_: UUID, exception: bool = True) -> Optional[dict]:
        doc = self._get_doc(self._col_sample, str(_not_falsy(id_, 'id_')))
        if not doc:
            if exception:
                raise _NoSuchSampleError(str(id_))
            return None
        return doc

    def _get_many_sample_doc(self, ids_: List[_Dict[str, _Any]], exception: bool=True) -> List[dict]:
        docs = self._get_many_docs(self._col_sample, [str(_not_falsy(id_['id'], 'id_')) for id_ in ids_])
        if not docs:
            if exception:
                raise _NoSuchSampleError(f"Could not complete search for samples: {[str(id_['id']) for id_ in ids_]}")
            # return [{}]
        return docs

    def _get_doc(self, col, id_: str) -> Optional[dict]:
        try:
            return col.get(id_)
        except _arango.exceptions.DocumentGetError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _get_many_docs(self, col, ids_:List[str]) -> List[dict]:
        try:
            return col.get_many(ids_)
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
        return SampleACL(
            UserID(acls[_FLD_OWNER]),
            self._timestamp_to_datetime(doc[_FLD_ACL_UPDATE_TIME]),
            [UserID(u) for u in acls[_FLD_ADMIN]],
            [UserID(u) for u in acls[_FLD_WRITE]],
            [UserID(u) for u in acls[_FLD_READ]],
            # allow None for backwards compability with DB entries missing the key
            acls.get(_FLD_PUBLIC_READ))

    def get_sample_set_acls(self, ids_: List[UUID]) -> List[SampleACL]:
        # function to ensure docs are sorted correctly
        str_ids = [str(id_) for id_ in ids_]
        def _keyfunc(doc):
            return str_ids.index(doc[_FLD_ARANGO_KEY])
        # have to cast this way for compatibility with _get_many_sample_doc
        docs = self._get_many_sample_doc([{'id': str_id} for str_id in str_ids])
        # sort docs (ensure that the right id is raised for errors)
        sorted_docs = sorted(docs, key=_keyfunc)
        sample_acls = []
        for doc in docs:
            acls = doc[_FLD_ACLS]
            sample_acls.append(SampleACL(
                UserID(acls[_FLD_OWNER]),
                self._timestamp_to_datetime(doc[_FLD_ACL_UPDATE_TIME]),
                [UserID(u) for u in acls[_FLD_ADMIN]],
                [UserID(u) for u in acls[_FLD_WRITE]],
                [UserID(u) for u in acls[_FLD_READ]],
                acls.get(_FLD_PUBLIC_READ)
            ))

        return sample_acls

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
        # Could return a subset of s to save bandwith
        # This will update the timestamp even for a noop. Maybe that's ok?
        # Detecting a noop would make the query a lot more complicated. Don't worry about it for
        # now.
        aql = f'''
            FOR s in @@col
                FILTER s.{_FLD_ARANGO_KEY} == @id
                FILTER s.{_FLD_ACLS}.{_FLD_OWNER} == @owner
                UPDATE s WITH {{{_FLD_ACLS}: MERGE(s.{_FLD_ACLS}, @acls),
                                {_FLD_ACL_UPDATE_TIME}: @ts
                                }} IN @@col
                RETURN NEW
            '''
        bind_vars = {'@col': self._col_sample.name,
                     'id': str(id_),
                     'owner': acls.owner.id,
                     'ts': acls.lastupdate.timestamp(),
                     'acls': {_FLD_ADMIN: [u.id for u in acls.admin],
                              _FLD_WRITE: [u.id for u in acls.write],
                              _FLD_READ: [u.id for u in acls.read],
                              _FLD_PUBLIC_READ: acls.public_read
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

    def update_sample_acls(
            self, id_: UUID, update: SampleACLDelta, update_time: datetime.datetime) -> None:
        '''
        Update a sample's ACLs via a delta specification.

        :param id_: the sample ID.
        :param update: the update to apply to the ACLs.
        :param update_time: the update time to save in the database.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises SampleStorageError: if the sample could not be retrieved.
        :raises UnauthorizedError: if the update attempts to alter the sample owner.
        '''
        # Needs to ensure owner is not added to another ACL
        # could make an option to just ignore the update to the owner? YAGNI for now.
        _not_falsy(update, 'update')
        _check_timestamp(update_time, 'update_time')
        s = self.get_sample_acls(id_)
        if not s.is_update(update):
            # noop. Theoretically the values in the DB may have changed since we pulled the ACLs,
            # but now we're talking about millisecond ordering differences, so don't worry
            # about it.
            return
        self._update_sample_acls_pt2(id_, update, s.owner, update_time)

    _UPDATE_ACLS_AQL = f'''
        FOR s in @@col
            FILTER s.{_FLD_ARANGO_KEY} == @id
            FILTER s.{_FLD_ACLS}.{_FLD_OWNER} == @owner
            UPDATE s WITH {{
                {_FLD_ACL_UPDATE_TIME}: @ts,
                {_FLD_ACLS}: {{
                    {_FLD_ADMIN}: REMOVE_VALUES(
                        UNION_DISTINCT(s.{_FLD_ACLS}.{_FLD_ADMIN}, @admin),
                        @admin_remove),
                    {_FLD_WRITE}: REMOVE_VALUES(
                        UNION_DISTINCT(s.{_FLD_ACLS}.{_FLD_WRITE}, @write),
                        @write_remove),
                    {_FLD_READ}: REMOVE_VALUES(
                        UNION_DISTINCT(s.{_FLD_ACLS}.{_FLD_READ}, @read),
                        @read_remove)
        '''

    _UPDATE_ACLS_AT_LEAST_AQL = f'''
        FOR s in @@col
            FILTER s.{_FLD_ARANGO_KEY} == @id
            FILTER s.{_FLD_ACLS}.{_FLD_OWNER} == @owner
            UPDATE s WITH {{
                {_FLD_ACL_UPDATE_TIME}: @ts,
                {_FLD_ACLS}: {{
                    {_FLD_ADMIN}: UNION_DISTINCT(
                        REMOVE_VALUES(s.{_FLD_ACLS}.{_FLD_ADMIN}, @admin_remove),
                        @admin),
                    {_FLD_WRITE}: UNION_DISTINCT(
                        REMOVE_VALUES(s.{_FLD_ACLS}.{_FLD_WRITE}, @write_remove),
                        REMOVE_VALUES(@write, s.{_FLD_ACLS}.{_FLD_ADMIN})),
                    {_FLD_READ}: UNION_DISTINCT(
                        REMOVE_VALUES(s.{_FLD_ACLS}.{_FLD_READ}, @read_remove),
                        REMOVE_VALUES(@read, UNION_DISTINCT(
                            s.{_FLD_ACLS}.{_FLD_ADMIN}, s.{_FLD_ACLS}.{_FLD_WRITE})))
        '''

    def _update_sample_acls_pt2(self, id_, update, owner, update_time):
        # this method is split solely to allow testing the owner change case.

        # At this point we're committed to a DB update and therefore an ACL update time bump
        # (unless we make the query very complicated, which probably isn't worth the
        # complexity). Even with the noop checking code above, it's still possible for the DB
        # update to be a noop and yet bump the update time. What that means, though, is that
        # some other thread of operation changed the ACLs to the exact state that application of
        # our delta update would result in. The only issue here is that the update time stamp will
        # be a few milliseconds later than it should be, so don't worry about it.

        # we remove the owner from the update list for the case where update.at_least is
        # true so that we don't add the owner to another ACL. If at_least is false, the
        # update class would've thrown an error.
        a = [u.id for u in update.admin if u != owner]
        w = [u.id for u in update.write if u != owner]
        r = [u.id for u in update.read if u != owner]
        rem = [u.id for u in update.remove]

        bind_vars = {'@col': self._col_sample.name,
                     'id': str(id_),
                     'owner': owner.id,
                     'ts': update_time.timestamp(),
                     'admin': a,
                     'write': w,
                     'read': r,
                     'read_remove': a + w + rem,
                     }
        if update.at_least:
            bind_vars['admin_remove'] = rem
            bind_vars['write_remove'] = a + rem
        else:
            bind_vars['admin_remove'] = w + r + rem
            bind_vars['write_remove'] = a + r + rem
        # Could return a subset of s to save bandwith (see query text)
        # ensures the owner hasn't changed since we pulled the acls above (see query text).
        aql = self._UPDATE_ACLS_AT_LEAST_AQL if update.at_least else self._UPDATE_ACLS_AQL
        if update.public_read is not None:
            aql += f''',
                        {_FLD_PUBLIC_READ}: @pubread'''
            bind_vars['pubread'] = update.public_read
        aql += '''
                        }
                    } IN @@col
                RETURN NEW
            '''

        try:
            cur = self._db.aql.execute(aql, bind_vars=bind_vars, count=True)
            if not cur.count():
                # Assume cur.count() is never > 1 as we're filtering on _key.
                # We already know the sample exists, and samples at this point can't be
                # deleted, so just raise.
                raise _OwnerChangedError(  # if this happens a lot make a retry loop.
                    'The sample owner unexpectedly changed during the operation. Please retry. ' +
                    'If this error occurs frequently, code changes may be necessary.')
        except _arango.exceptions.AQLQueryExecuteError as e:  # this is a real pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def create_data_link(self, link: DataLink, update: bool = False) -> Optional[UUID]:
        '''
        Link data in the workspace to a sample.
        Each data unit can be linked to only one sample at a time. Expired links may exist to
        other samples.

        Uniqueness of the link ID is required but not enforced. The caller is responsible for
        enforcement. If this contract is violated, the get_data_link method may behave
        unexpectedly.

        No checking is done on whether the user has permissions to link the data or whether the
        data exists.

        It is expected that the creation time provided in the link is later than the expire time
        (and usually it's simply the current time) of any other links for the data unit ID.
        This is not enforced.

        :param link: the link to save, which cannot be expired.
        :param update: if the link from the object already exists and is linked to a different
            sample, update the link. If it is linked to the same sample take no action.
        :returns: The ID of the link that is expired as part of the update process, if any.

        :raises NoSuchSampleError: if the sample does not exist.
        :raises NoSuchSampleVersionError: if the sample version does not exist.
        :raises NoSuchSampleNodeError: if the sample node does not exist.
        :raises DataLinkExistsError: if a link already exists from the data unit.
        :raises TooManyDataLinksError: if there are too many links from the sample version or
            the workspace object version.
        '''
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

        # TODO CODE this method is too long, try to split up
        _not_falsy(link, 'link')
        if link.expired:
            raise ValueError('link cannot be expired')
        sna = link.sample_node_address
        # need to get the version doc to ensure the documents have been updated appropriately
        # as well as getting the uuid version, see comments at beginning of file
        _, versiondoc, _ = self._get_sample_and_version_doc(sna.sampleid, sna.version)
        samplever = UUID(versiondoc[_FLD_UUID_VER])
        nodeid = self._get_node_id(sna.sampleid, samplever, sna.node)
        if not self._get_doc(self._col_nodes, nodeid):
            raise _NoSuchSampleNodeError(f'{sna.sampleid} ver {sna.version} {sna.node}')

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
            oldlinkdoc = self._get_doc(tdlc, self._create_link_key(link))
            if oldlinkdoc:
                if not update:  # maybe want to move this after the noop check? or add noop option
                    raise _DataLinkExistsError(str(link.duid))
                oldlink = self._doc_to_link(oldlinkdoc)
                if link.is_equivalent(oldlink):
                    self._abort_transaction(tdb)
                    return None  # I don't like having a return in the middle of the method, but
                    # the alternative seems to be worse

                # See the notes in the expire method, many are relevant here.
                # However, since this is an exclusive lock and at this point we know the old
                # linkdoc exists, there's no need to check for key collisions on insert.
                # The entire transaction may fail, but not the individual inserts.
                oldlinkdoc[_FLD_LINK_EXPIRED_BY] = link.created_by.id
                # I'm not a fan of this, but a millisecond gap seems safe and most systems
                # should have millisecond resolution.
                # Consider rounding to millisecond resolution for consistency? Make a class?
                oldlinkdoc[_FLD_LINK_EXPIRED] = self._timestamp_seconds_to_milliseconds(link.created.timestamp() - 0.001)
                oldlinkdoc[_FLD_ARANGO_KEY] = self._create_link_key_from_link_doc(oldlinkdoc)

                # since we're replacing a link we don't need to worry about counting links from
                # ws object. Not true for the sample, which could be different.
                oldsna = oldlink.sample_node_address
                if sna.sampleid != oldsna.sampleid or sna.version != oldsna.version:
                    # it doesn't matter if only the node is different since the traversal from
                    # sample -> workspace objects starts at a version, so the count/version
                    # of extant links won't change
                    # Could support starting at a node later
                    self._check_link_count_from_sample_ver(tdb, samplever, link)
                self._insert(tdlc, oldlinkdoc)
            else:
                # might be able to get rid of these limits if it turns out the link queries
                # can be done without a traversal, which means the links can be looked up with
                # an index
                # but then paging is needed, so need to implement that
                self._check_link_count_from_ws_object(tdb, link)
                self._check_link_count_from_sample_ver(tdb, samplever, link)

            ldoc = self._create_link_doc(link, samplever)
            self._insert(tdlc, ldoc, upsert=bool(oldlinkdoc))
            # since transaction is exclusive write, conflicts can't happen
            # presumably any failures are unrecoverable...? conn / db down, etc
            self._commit_transaction(tdb)
        finally:
            self._abort_transaction(tdb)
        return UUID(oldlinkdoc[_FLD_LINK_ID]) if oldlinkdoc else None

    def _commit_transaction(self, transaction_db):
        try:
            transaction_db.commit_transaction()
        except _arango.exceptions.TransactionCommitError as e:  # dunno how to test this
            # TODO DATALINK if the transaction fails we may want to retry. pretty complicated
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _abort_transaction(self, transaction_db):
        if transaction_db.transaction_status() != 'committed':
            try:
                transaction_db.abort_transaction()
            except _arango.exceptions.TransactionAbortError as e:  # dunno how to test this
                # this will mask the previous error, but if this fails probably the DB
                # connection is hosed
                raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _check_link_count_from_ws_object(self, db, link: DataLink):
        if self._count_links_from_ws_object(
                db, link.duid.upa, link.created, link.expired) >= self._max_links:
            raise _TooManyDataLinksError(
                f'More than {self._max_links} links from workspace object {link.duid.upa}')

    def _count_links_from_ws_object(
            self,
            db,
            upa: UPA,
            created: datetime.datetime,
            expired: Optional[datetime.datetime]):
        wsc = self._count_links(
            db,
            f'''
                    FILTER d.{_FLD_LINK_WORKSPACE_ID} == @wsid
                    FILTER d.{_FLD_LINK_OBJECT_ID} == @objid
                    FILTER d.{_FLD_LINK_OBJECT_VERSION} == @ver''',
            {'wsid': upa.wsid, 'objid': upa.objid, 'ver': upa.version},
            created,
            expired)
        return wsc

    def _check_link_count_from_sample_ver(self, db, samplever: UUID, link: DataLink):
        if self._count_links_from_sample_ver(
                db, samplever, link.created, link.expired) >= self._max_links:
            sna = link.sample_node_address
            raise _TooManyDataLinksError(
                f'More than {self._max_links} links from sample {sna.sampleid} ' +
                f'version {sna.version}')

    def _count_links_from_sample_ver(
            self,
            db,
            version: UUID,
            created: datetime.datetime,
            expired: Optional[datetime.datetime]):
        sv = self._count_links(
            db,
            f'''
                    FILTER d.{_FLD_LINK_SAMPLE_UUID_VERSION} == @sver''',
            {'sver': str(version)},
            created,
            expired)
        return sv

    def _count_links(self, db, filters: str, bind_vars, created, expired):
        bind_vars['@col'] = self._col_data_link.name
        bind_vars['created'] = self._timestamp_seconds_to_milliseconds(created.timestamp())
        bind_vars['expired'] = self._timestamp_seconds_to_milliseconds(expired.timestamp()) if expired else _ARANGO_MAX_INTEGER
        # might need to include created / expired in compound indexes if we get a ton of expired
        # links. Might not work in a NOT though. Alternate formulation is
        # (d.creatd >= @created AND d.created <= @expired) OR
        # (d.expired >= @created AND d.expired <= @expired)
        q = ('''
                FOR d in @@col
             ''' +
             filters +
             f'''
                    FILTER NOT (d.{_FLD_LINK_EXPIRED} < @created OR
                        d.{_FLD_LINK_CREATED} > @expired)
                    COLLECT WITH COUNT INTO linkcount
                    RETURN linkcount
             ''')
        try:
            cur = db.aql.execute(q, bind_vars=bind_vars)
            return cur.next()
        except _arango.exceptions.AQLQueryExecuteError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _create_link_key(self, link: DataLink):
        cr = f'_{link.created.timestamp()}' if link.expired else ''
        upa = link.duid.upa
        dataid = f'_{self._md5(link.duid.dataid)}' if link.duid.dataid else ''
        return f'{upa.wsid}_{upa.objid}_{upa.version}{dataid}{cr}'

    # only creates keys for unexpired links.
    def _create_link_key_from_duid(self, duid: DataUnitID):
        dataid = f'_{self._md5(duid.dataid)}' if duid.dataid else ''
        return f'{duid.upa.wsid}_{duid.upa.objid}_{duid.upa.version}{dataid}'

    def _create_link_key_from_link_doc(self, link: dict):
        # arango sometimes removes trailing decimals and zeros from the number so we reformat
        # with datetime to ensure consistency
        created=self._timestamp_milliseconds_to_seconds(link[_FLD_LINK_CREATED])
        cr = (f'_{self._timestamp_to_datetime(created).timestamp()}'
              if link[_FLD_LINK_EXPIRED] != _ARANGO_MAX_INTEGER else '')
        dataid = (f'_{self._md5(link[_FLD_LINK_OBJECT_DATA_UNIT])}'
                  if link[_FLD_LINK_OBJECT_DATA_UNIT] else '')
        wsid = link[_FLD_LINK_WORKSPACE_ID]
        objid = link[_FLD_LINK_OBJECT_ID]
        version = link[_FLD_LINK_OBJECT_VERSION]
        return f'{wsid}_{objid}_{version}{dataid}{cr}'

    def _create_link_doc(self, link: DataLink, samplever: UUID):
        sna = link.sample_node_address
        upa = link.duid.upa
        nodeid = self._get_node_id(sna.sampleid, samplever, sna.node)
        # see https://github.com/kbase/relation_engine_spec/blob/4a9dc6df2088763a9df88f0b018fa5c64f2935aa/schemas/ws/ws_object_version.yaml#L17  # noqa
        from_ = f'{self._col_ws.name}/{upa.wsid}:{upa.objid}:{upa.version}'
        return {
            _FLD_ARANGO_KEY: self._create_link_key(link),
            _FLD_ARANGO_FROM: from_,
            _FLD_ARANGO_TO: f'{self._col_nodes.name}/{nodeid}',
            _FLD_LINK_CREATED: self._timestamp_seconds_to_milliseconds(link.created.timestamp()),
            _FLD_LINK_CREATED_BY: link.created_by.id,
            _FLD_LINK_EXPIRED: self._timestamp_seconds_to_milliseconds(link.expired.timestamp()) if link.expired else _ARANGO_MAX_INTEGER,
            _FLD_LINK_EXPIRED_BY: link.expired_by.id if link.expired_by else None,
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

    def _get_link_doc_from_link_id(self, id_):
        # if delete/hide samples added may need some more logic here
        try:
            cur = self._col_data_link.find({_FLD_LINK_ID: str(_not_falsy(id_, 'id_'))}, limit=2)
            if cur.count() == 0:
                raise _NoSuchLinkError(str(id_))
            if cur.count() > 1:
                raise _SampleStorageError(f'More than one data link found for ID {id_}')
            doc = cur.next()
            cur.close(True)
            return doc
        except _arango.exceptions.DocumentGetError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e

    def _get_link_doc_from_duid(self, duid):
        key = self._create_link_key_from_duid(duid)
        linkdoc = self._get_doc(self._col_data_link, key)
        if not linkdoc:
            raise _NoSuchLinkError(str(duid))
        return linkdoc

    def expire_data_link(
            self,
            expired: datetime.datetime,
            expired_by: UserID,
            id_: UUID = None,
            duid: DataUnitID = None) -> DataLink:
        '''
        Expire a data link. The link can be addressed by its ID or the DUID, since for a non-
        expired link the DUID is a unique identifier. Providing both IDs is an error.

        It is assumed, but not enforced, that the expired time is not in the future.

        :param expired: the expiration time.
        :param id_: the link ID.
        :param duid: the data unit ID from which the link originates.
        :returns: the updated link.
        :raises NoSuchLinkError: if the link does not exist or is already expired.
        '''
        # See notes for creating links re the transaction approach.
        _check_timestamp(expired, 'expired')
        _not_falsy(expired_by, 'expired_by')
        if not bool(id_) ^ bool(duid):  # xor
            raise ValueError('exactly one of id_ or duid must be provided')
        if id_:
            linkdoc = self._get_link_doc_from_link_id(id_)
            txtid = str(id_)
            if linkdoc[_FLD_LINK_EXPIRED] != _ARANGO_MAX_INTEGER:
                raise _NoSuchLinkError(txtid)
        else:
            linkdoc = self._get_link_doc_from_duid(duid)
            txtid = str(duid)

        if self._timestamp_seconds_to_milliseconds(expired.timestamp()) < linkdoc[_FLD_LINK_CREATED]:
            raise ValueError(f'expired is < link created time: {linkdoc[_FLD_LINK_CREATED]}')

        return self._expire_data_link_pt2(linkdoc, expired, expired_by, txtid)

    # this split is here in order to test the race condition where a link is expired after the
    # check above.
    def _expire_data_link_pt2(self, linkdoc, expired, expired_by, txtid) -> DataLink:

        oldkey = self._create_link_key_from_link_doc(linkdoc)

        linkdoc[_FLD_LINK_EXPIRED] = self._timestamp_seconds_to_milliseconds(expired.timestamp())
        linkdoc[_FLD_LINK_EXPIRED_BY] = expired_by.id
        linkdoc[_FLD_ARANGO_KEY] = self._create_link_key_from_link_doc(linkdoc)

        # There appears to be no way to transactionally update a document's _key.
        # Even using a transaction, as we do here, isn't guaranteed since in a cluster parts
        # of a transaction may fail, and so the DB may be left in an inconsistent state.
        # TODO DATALINK do we want to add failure checking / recovery code for this transaction?

        # makes a db specifically for this transaction
        tdb = self._db.begin_transaction(
            read=self._col_data_link.name,
            write=self._col_data_link.name)

        # TODO DATALINK Transactions in arango can allow some ops to succeed and others to fail.
        # What do?
        try:
            # makes a collection specifically for this transaction
            tdlc = tdb.collection(self._col_data_link.name)
            try:
                tdlc.insert(linkdoc, silent=True)
            except _arango.exceptions.DocumentInsertError as e:
                if e.error_code == 1210:  # unique constraint violation code
                    # ok, a race condition occurred and another thread expired the link.
                    # If an interleaving call made a new link after expiring the old one, we
                    # should *NOT* expire that.
                    raise _NoSuchLinkError(txtid)
                else:  # this is a real pain to test.
                    raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
            try:
                tdlc.delete(oldkey, silent=True)
            except _arango.exceptions.DocumentDeleteError as e:
                # In the context of the transaction, since the insert above succeeded the link
                # can't have been deleted yet, because the link hasn't been expired yet.
                # If the overall transaction fails at this point, it means either another call
                # expired and deleted the link, so we're done, or another call expired the link and
                # created a new link and the transaction collided. We should probably *NOT*
                # expire the brand new link - that was not the user's intent. Both of these
                # cases take millisecond timing and will be extremely rare, so just throw an
                # error and document potential failure modes for expire transaction

                # this is really hard to test - maybe impossible?
                raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
            self._commit_transaction(tdb)
            return self._doc_to_link(linkdoc)
        finally:
            self._abort_transaction(tdb)

    def get_data_link(self, id_: UUID = None, duid: DataUnitID = None) -> DataLink:
        '''
        Get a link by its ID or Data Unit ID. The latter can only retrieve non-expired links.
        Exactly one of the ID or DUID must be specified.

        :param id_: the link ID.
        :param duid: the link DUID.
        :returns: the link.
        :raises NoSuchLinkError: if the link does not exist.
        '''
        if not bool(id_) ^ bool(duid):  # xor
            raise ValueError('exactly one of id_ or duid must be provided')
        if id_:
            return self._doc_to_link(self._get_link_doc_from_link_id(id_))
        else:
            return self._doc_to_link(self._get_link_doc_from_duid(duid))

    def _doc_to_link(self, doc) -> DataLink:
        ex = doc[_FLD_LINK_EXPIRED]
        return DataLink(
            UUID(doc[_FLD_LINK_ID]),
            self._doc_to_dataunit_id(doc),
            _SampleNodeAddress(
                SampleAddress(
                    UUID(doc[_FLD_LINK_SAMPLE_ID]),
                    doc[_FLD_LINK_SAMPLE_INT_VERSION]),
                doc[_FLD_LINK_SAMPLE_NODE]),
            self._timestamp_to_datetime(self._timestamp_milliseconds_to_seconds(doc[_FLD_LINK_CREATED])),
            UserID(doc[_FLD_LINK_CREATED_BY]),
            None if ex == _ARANGO_MAX_INTEGER else self._timestamp_to_datetime(self._timestamp_milliseconds_to_seconds(ex)),
            UserID(doc[_FLD_LINK_EXPIRED_BY]) if doc[_FLD_LINK_EXPIRED_BY] else None
        )

    def _doc_to_dataunit_id(self, doc) -> DataUnitID:
        return DataUnitID(
            UPA(wsid=doc[_FLD_LINK_WORKSPACE_ID],
                objid=doc[_FLD_LINK_OBJECT_ID],
                version=doc[_FLD_LINK_OBJECT_VERSION]),
            doc[_FLD_LINK_OBJECT_DATA_UNIT])

    def get_links_from_sample(
            self,
            sample: SampleAddress,
            readable_wsids: Optional[List[int]],
            timestamp: datetime.datetime) -> List[DataLink]:
        '''
        Get the links from a sample at a particular time.

        :param sample: the sample of interest.
        :param readable_wsids: IDs of workspaces for which the user has read permissions.
            Pass None to return links to objects in all workspaces.
        :param timestamp: the time to use to determine which links are active.
        :returns: a list of links.
        :raises NoSuchSampleError: if the sample does not exist.
        :raises NoSuchSampleVersionError: if the sample version does not exist.
        '''
        # may want to make this work on non-ws objects at some point. YAGNI for now.
        _not_falsy(sample, 'sample')
        _check_timestamp(timestamp, 'timestamp')
        _not_falsy_in_iterable(readable_wsids, 'readable_wsids', allow_none=True)
        if readable_wsids is not None and not readable_wsids:
            return []
        # need to get the version doc to ensure the documents have been updated appropriately
        # as well as getting the uuid version, see comments at beginning of file
        # note that testing version updating has been done for at least 2 other methods
        # the tests are not repeated here
        _, versiondoc, _ = self._get_sample_and_version_doc(sample.sampleid, sample.version)
        bind_vars = {'@col': self._col_data_link.name,
                     'samplever': versiondoc[_FLD_UUID_VER],
                     'ts': self._timestamp_seconds_to_milliseconds(timestamp.timestamp())}
        wsidfilter = ''
        if readable_wsids:
            bind_vars['wsids'] = readable_wsids
            wsidfilter = f'FILTER d.{_FLD_LINK_WORKSPACE_ID} IN @wsids'
        q = f'''
            FOR d in @@col
                FILTER d.{_FLD_LINK_SAMPLE_UUID_VERSION} == @samplever
                {wsidfilter}
                FILTER d.{_FLD_LINK_CREATED} <= @ts
                FILTER d.{_FLD_LINK_EXPIRED} >= @ts
                RETURN d
            '''
        # may need an index on version + created and expired? Assume for now links aren't
        # expired very often.
        # may also want a sample ver / wsid index? Max 10k items per version though, and
        # probably much less. YAGNI for now.
        return self._find_links_via_aql(q, bind_vars)

    def _find_links_via_aql(self, query, bind_vars):
        duids = []
        try:
            for link in self._db.aql.execute(query, bind_vars=bind_vars):
                duids.append(self._doc_to_link(link))
        except _arango.exceptions.AQLQueryExecuteError as e:  # this is a pain to test
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        return duids  # a maxium of 10k can be returned based on the link creation function

    def get_batch_links_from_samples(self,
                                     samples: List[SampleAddress],
                                     readable_wsids: Optional[List[int]],
                                     timestamp: datetime.datetime) -> List[DataLink]:
        '''
        Get the links from a bulk list of samples at a particular time.

        :param samples: the samples of interest.
        :param readable_wsids: IDs of workspaces for which the user has read permissions.
            Pass None to return links to objects in all workspaces.
        :param timestamp: the time to use to determine which links are active.
        :returns: a list of links.
        :raises SampleStorageError: if a conection to the database fails.
        '''

        _not_falsy(samples, 'samples')
        _check_timestamp(timestamp, 'timestamp')

        aql_bind = {
            '@sample_col': self._col_sample.name,
            '@link_col': self._col_data_link.name,
            # cast SampleAddress to dict
            'sample_ids': [{'id': str(s.sampleid), 'version': s.version} for s in samples],
            'ts': self._timestamp_seconds_to_milliseconds(timestamp.timestamp())
        }

        wsidfilter = ''
        if readable_wsids:
            aql_bind['wsids'] = readable_wsids
            wsidfilter = f'FILTER d.{_FLD_LINK_WORKSPACE_ID} IN @wsids'

        q = f'''
            LET version_ids = (FOR sample_id IN @sample_ids
                LET doc = DOCUMENT(@@sample_col, sample_id.id)
                RETURN {{
                    'id': doc.id,
                    'version_id': doc.vers[sample_id.version - 1],
                    'version': sample_id.version
                }}
            )

            LET data_links = (FOR version_id IN version_ids
                FOR d in @@link_col
                    FILTER d.{_FLD_LINK_SAMPLE_UUID_VERSION} == version_id.version_id
                    {wsidfilter}
                    FILTER d.{_FLD_LINK_CREATED} <= @ts
                    FILTER d.{_FLD_LINK_EXPIRED} >= @ts
                    RETURN d
            )
            RETURN data_links
        '''

        duids = []
        try:
            # have to unwrap query result twice because its a nested array
            for link_set in self._db.aql.execute(q, bind_vars=aql_bind):
                for link in link_set:
                    duids.append(self._doc_to_link(link))
        except _arango.exceptions.AQLQueryExecuteError as e:
            raise _SampleStorageError('Connection to database failed: ' + str(e)) from e
        return duids

    def get_links_from_data(self, upa: UPA, timestamp: datetime.datetime) -> List[DataLink]:
        '''
        Get links originating from a data object. The data object is not checked for existence.

        :param upa: the address of the data object.
        :param timestamp: the time to use to determine which links are active.
        :returns: a list of links.
        '''
        # the UPA makes it workspace specific, may need to make it generic later. YAGNI for now.
        _not_falsy(upa, 'upa')
        _check_timestamp(timestamp, 'timestamp')
        q = f'''
            FOR d in @@col
                FILTER d.{_FLD_LINK_WORKSPACE_ID} == @wsid
                FILTER d.{_FLD_LINK_OBJECT_ID} == @objid
                FILTER d.{_FLD_LINK_OBJECT_VERSION} == @ver
                FILTER d.{_FLD_LINK_CREATED} <= @ts
                FILTER d.{_FLD_LINK_EXPIRED} >= @ts
                RETURN d
            '''
        bind_vars = {'@col': self._col_data_link.name,
                     'wsid': upa.wsid,
                     'objid': upa.objid,
                     'ver': upa.version,
                     'ts': self._timestamp_seconds_to_milliseconds(timestamp.timestamp())}
        # may need an index on upa + created and expired? Assume for now links aren't
        # expired very often.
        return self._find_links_via_aql(q, bind_vars)

    def has_data_link(self, upa: UPA, sample: UUID) -> bool:
        '''
        Check if a link exists or has ever existed between an object and a sample. The sample and
        object are not checked for existence.

        :param upa: the object to check.
        :param sample: the sample to check.
        :returns: True if a link has ever existed between the sample and the object, or False
            otherwise.
        '''
        # Again, this is fairly workspace specific. May want to generalize at some point.
        # YAGNI for now.
        _not_falsy(upa, 'upa')
        _not_falsy(sample, 'sample')
        q = f'''
            FOR d in @@col
                FILTER d.{_FLD_LINK_SAMPLE_ID} == @sampleid
                FILTER d.{_FLD_LINK_WORKSPACE_ID} == @wsid
                FILTER d.{_FLD_LINK_OBJECT_ID} == @objid
                FILTER d.{_FLD_LINK_OBJECT_VERSION} == @ver
                LIMIT 1
                RETURN d
            '''
        bind_vars = {'@col': self._col_data_link.name,
                     'sampleid': str(sample),
                     'wsid': upa.wsid,
                     'objid': upa.objid,
                     'ver': upa.version}
        # may want a sample / wsid index? Max 10k items per version though, and
        # probably much less. YAGNI for now.
        # not super efficient - could save some bandwidth by not returning the link.
        # likely trivial, though, so don't worry about it for now.
        return bool(self._find_links_via_aql(q, bind_vars))


# if an edge is inserted into a non-edge collection _from and _to are silently dropped
def _init_collection(database, collection, collection_name, collection_variable_name, edge=False):
    c = database.collection(_check_string(collection, collection_variable_name))
    if not c.properties()['edge'] is edge:  # this is a http call
        ctype = 'an edge' if edge else 'a vertex'
        raise _StorageInitError(f'{collection_name} {collection} is not {ctype} collection')
    return c
