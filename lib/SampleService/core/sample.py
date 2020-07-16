'''
Contains classes related to samples.
'''

import datetime
from enum import Enum as _Enum, unique as _unique
import maps as _maps
import json as _json
import unicodedata as _unicodedata
from uuid import UUID
from typing import Optional, List, Dict, TypeVar as _TypeVar
from typing import Set as _Set, cast as _cast
from SampleService.core.core_types import PrimitiveType
from SampleService.core.arg_checkers import (
    not_falsy as _not_falsy,
    not_falsy_in_iterable as _not_falsy_in_iterable,
)
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.arg_checkers import check_timestamp as _check_timestamp
from SampleService.core.errors import IllegalParameterError, MissingParameterError
from SampleService.core.user import UserID

# for now we'll assume people are nice and don't change attributes after init.
# if that doesn't hold true, override __setattr__.


_MAX_SAMPLE_NAME_LEN = 256
_MAX_SAMPLE_NODES = 10000

_META_MAX_SIZE_B = 100000  # based on serialized json
_META_MAX_KEY_SIZE = 256
_META_MAX_VALUE_SIZE = 1024


@_unique
class SubSampleType(_Enum):
    '''
    The type of a SampleNode.
    '''

    # do not change the enum constant variable names, they may be saved in DBs

    BIOLOGICAL_REPLICATE = 'BioReplicate'
    ''' A biological replicate. '''
    TECHNICAL_REPLICATE =  'TechReplicate'  # noqa: E222 @IgnorePep8
    ''' A technical replicate. '''
    SUB_SAMPLE =           'SubSample'      # noqa: E222 @IgnorePep8
    ''' A subsample that is not a biological or technical replicate.'''


_T = _TypeVar('_T')
_V = _TypeVar('_V')


def _fz(d: Dict[_T, _V]) -> Dict[_T, _V]:
    return _maps.FrozenMap.recurse(d)


class SourceMetadata:
    '''
    Sample metadata as it existed at the source.

    :ivar key: the controlled metadata key.
    :ivar sourcekey: the metadata key as it existed at the source.
    :ivar sourcevalue: the metadata value as it existed at the source.
    '''

    def __init__(self, key: str, sourcekey: str, sourcevalue: Dict[str, PrimitiveType]):
        '''
        Create the source metadata entry.

        :param key: the controlled metadata key.
        :param sourcekey: the metadata key as it existed at the source.
        :param sourcevalue: the metadata value as it existed at the source.
        :raises IllegalParameterError: if any of the various keys or values are missing, too
            long, or contain control characters.
        '''
        self.key = _check_metadata_key(key, 'Controlled')
        self.sourcekey = _check_metadata_key(sourcekey, 'Source')
        self.sourcevalue = _fz(_check_metadata_value(key, sourcevalue, 'Source'))

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.key == self.key
                    and other.sourcekey == self.sourcekey
                    and other.sourcevalue == self.sourcevalue
                    )
        return NotImplemented

    def __hash__(self):
        return hash((self.key, self.sourcekey, self.sourcevalue))

    # def __repr__(self):
    #     return f'{self.key}, {self.sourcekey}, {self.sourcevalue}'


class SampleNode:
    '''
    A node in the sample tree.
    :ivar name: The name of the sample node.
    :ivar type: The type of this sample nde.
    :ivar parent: The parent SampleNode of this node.
    :ivar controlled_metadata: Sample metadata that has been checked against a controlled
        vocabulary.
    :ivar user_metadata: Unrestricted sample metadata.
    :ivar source_metadata: Information about the controlled metadata as it existed at the data
        source, prior to any possible transformations for ingest.
    '''

    def __init__(
            self,
            name: str,
            type_: SubSampleType = SubSampleType.BIOLOGICAL_REPLICATE,
            parent: Optional[str] = None,
            controlled_metadata: Optional[Dict[str, Dict[str, PrimitiveType]]] = None,
            user_metadata: Optional[Dict[str, Dict[str, PrimitiveType]]] = None,
            source_metadata: Optional[List[SourceMetadata]] = None
            ):
        '''
        Create a sample node.
        :param name: The name of the sample node.
        :param type_: The type of this sample nde.
        :param parent: The parent SampleNode of this node. BIOLOGICAL_REPLICATEs, and only
            BIOLOGICAL_REPLICATEs, cannot have parents.
        :param controlled_metadata: Sample metadata that has been checked against a controlled
            vocabulary.
        :param user_metadata: Unrestricted sample metadata.
        :param source_metadata: Information about the controlled metadata as it existed at the data
            source, prior to any possible transformations for ingest.
        :raises MissingParameterError: if the name is None or whitespace only.
        :raises IllegalParameterError: if the name or parent is too long or contains illegal
            characters, the parent is missing and the node type is not BIOLOGICAL_REPLICATE,
            or basic metadata constraints are violated.
        '''
        # could make a bioreplicate class... meh for now
        self.name = _cast(str, _check_string(name, 'subsample name', max_len=_MAX_SAMPLE_NAME_LEN))
        self.type = _not_falsy(type_, 'type')
        self.parent = _check_string(parent, 'parent', max_len=_MAX_SAMPLE_NAME_LEN, optional=True)
        cm = controlled_metadata if controlled_metadata else {}
        _check_meta(cm, True)
        self.controlled_metadata = _fz(cm)
        um = user_metadata if user_metadata else {}
        _check_meta(um, False)
        self.user_metadata = _fz(um)
        sm = source_metadata if source_metadata else []
        _check_source_meta(sm, self.controlled_metadata)
        self.source_metadata = tuple(sm)
        isbiorep = type_ == SubSampleType.BIOLOGICAL_REPLICATE
        if not _xor(bool(parent), isbiorep):
            raise IllegalParameterError(
                f'Node {self.name} is of type {type_.value} and therefore ' +
                f'{"cannot" if isbiorep else "must"} have a parent')

        # TODO description

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.name == self.name
                    and other.type == self.type
                    and other.parent == self.parent
                    and other.controlled_metadata == self.controlled_metadata
                    and other.user_metadata == self.user_metadata
                    and other.source_metadata == self.source_metadata
                    )
        return NotImplemented

    def __hash__(self):
        return hash((self.name, self.type, self.parent, self.controlled_metadata,
                     self.user_metadata, self.source_metadata))

    # def __repr__(self):
    #     return (f'{self.name}, {self.type}, {self.parent}, {self.controlled_metadata}, ' +
    #             f'{self.user_metadata}, {self.source_metadata}')


def _check_meta(m: Dict[str, Dict[str, PrimitiveType]], controlled: bool):
    c = 'Controlled' if controlled else 'User'
    for k in m:
        _check_metadata_key(k, c)
        _check_metadata_value(k, m[k], c)
    if len(_json.dumps(m, ensure_ascii=False).encode('utf-8')) > _META_MAX_SIZE_B:
        # would be nice if that could be streamed so we don't make a new byte array
        raise IllegalParameterError(
            f'{c} metadata is larger than maximum of {_META_MAX_SIZE_B}B')


def _check_metadata_key(key: str, name: str) -> str:
    if not key or not key.strip():
        raise IllegalParameterError(f'{name} metadata keys may not be null or whitespace only')
    if len(key) > _META_MAX_KEY_SIZE:
        raise IllegalParameterError(
            f'{name} metadata has key starting with {key[:_META_MAX_KEY_SIZE]} that ' +
            f'exceeds maximum length of {_META_MAX_KEY_SIZE}')
    cc = _control_char_first_pos(key)
    if cc >= 0:
        raise IllegalParameterError(
            f"{name} metadata key {key}'s character at index {cc} is a control character.")
    return key


def _check_metadata_value(
        key: str, value: Dict[str, PrimitiveType], name: str) -> Dict[str, PrimitiveType]:
    if not value:
        raise IllegalParameterError(
            f'{name} metadata value associated with metadata key {key} is null or empty')
    for vk in value:
        cc = _control_char_first_pos(vk)
        if cc >= 0:
            raise IllegalParameterError(
                f"{name} metadata value key {vk} associated with metadata key {key} has a " +
                f'character at index {cc} that is a control character.')
        if len(vk) > _META_MAX_KEY_SIZE:
            raise IllegalParameterError(
                f'{name} metadata has a value key associated with metadata key {key} starting ' +
                f'with {vk[:_META_MAX_KEY_SIZE]} that exceeds maximum length of ' +
                f'{_META_MAX_KEY_SIZE}')
        val = value[vk]
        if type(val) == str:
            cc = _control_char_first_pos(_cast(str, val), allow_tabs_and_lf=True)
            if cc >= 0:
                raise IllegalParameterError(
                    f"{name} metadata value associated with metadata key {key} and " +
                    f'value key {vk} has a character at index {cc} that is a control character.')
            if len(_cast(str, val)) > _META_MAX_VALUE_SIZE:
                raise IllegalParameterError(
                    f'{name} metadata has a value associated with metadata key {key} ' +
                    f'and value key {vk} starting with {_cast(str, val)[:_META_MAX_KEY_SIZE]} ' +
                    f'that exceeds maximum length of {_META_MAX_VALUE_SIZE}')
    return value


def _control_char_first_pos(string: str, allow_tabs_and_lf: bool = False):
    for i, c in enumerate(string):
        if _unicodedata.category(c)[0] == "C":
            if not allow_tabs_and_lf or (c != '\n' and c != '\t'):
                return i
    return -1


def _check_source_meta(m: List[SourceMetadata], controlled_metadata):
    _not_falsy_in_iterable(m, 'source_metadata')
    # it doesn't make sense to turn the whole thing into json as a rough measure of size as the
    # user is not responsible for the field names.
    total_bytes = 0
    seen = set()
    for sm in m:
        if sm.key in seen:
            raise IllegalParameterError(f'Duplicate source metadata key: {sm.key}')
        seen.add(sm.key)
        # could check for duplicate sm.sourcekeys too, but possibly the source data is split into
        # two metadata keys?
        if sm.key not in controlled_metadata:
            raise IllegalParameterError(
                f'Source metadata key {sm.key} does not appear in the controlled metadata')
        total_bytes += len(sm.key.encode('utf-8')) + len(sm.sourcekey.encode('utf-8'))
        total_bytes += len(_json.dumps(dict(sm.sourcevalue), ensure_ascii=False).encode('utf-8'))
        # Would be nice if that could be streamed so we don't make a new byte array
        # This calculation is more convoluted than I would like and hard to reproduce for users
        # but it's really unlikely the limit is ever going to be hit so YAGNI re improvements,
        # at least for now.
    if total_bytes > _META_MAX_SIZE_B:
        raise IllegalParameterError(
            f'Source metadata is larger than maximum of {_META_MAX_SIZE_B}B')


class Sample:
    '''
    A sample containing biological replicates, technical replicates, and sub samples.
    Do NOT mutate the instance variables post creation.
    :ivar nodes: The nodes in this sample.
    :ivar name: The name of the sample.
    '''

    def __init__(
            self,
            nodes: List[SampleNode],
            name: Optional[str] = None,
            ):
        '''
        Create the the sample.
        :param nodes: The tree nodes in the sample. BIOLOGICAL_REPLICATES must come first in
            the list, and parents must come before children in the list.
        :param name: The name of the sample. Cannot contain control characters or be longer than
            255 characters.
        :raise MissingParameterError: if no nodes are provided.
        :raises IllegalParameterError: if the name is too long or contains illegal characters,
            the first node in the list is not a BIOLOGICAL_REPLICATE, all the BIOLOGICAL_REPLICATES
            are not at the start of this list, node names are not unique, or parent nodes
            do not appear in the list prior to their children.
        '''
        self.name = _check_string(name, 'name', max_len=_MAX_SAMPLE_NAME_LEN, optional=True)
        if not nodes:
            raise MissingParameterError('At least one node per sample is required')
        if len(nodes) > _MAX_SAMPLE_NODES:
            raise IllegalParameterError(
                f'At most {_MAX_SAMPLE_NODES} nodes are allowed per sample')
        if nodes[0].type != SubSampleType.BIOLOGICAL_REPLICATE:
            raise IllegalParameterError(
                f'The first node in a sample must be a {SubSampleType.BIOLOGICAL_REPLICATE.value}')
        no_more_bio = False
        seen_names: _Set[str] = set()
        for n in nodes:
            if no_more_bio and n.type == SubSampleType.BIOLOGICAL_REPLICATE:
                raise IllegalParameterError(
                    f'{SubSampleType.BIOLOGICAL_REPLICATE.value}s must be the first ' +
                    'nodes in the list of sample nodes.')
            if n.type != SubSampleType.BIOLOGICAL_REPLICATE:
                no_more_bio = True
            if n.name in seen_names:
                raise IllegalParameterError(f'Duplicate sample node name: {n.name}')
            if n.parent and n.parent not in seen_names:
                raise IllegalParameterError(f'Parent {n.parent} of node {n.name} does not ' +
                                            'appear in node list prior to node.')
            seen_names.add(n.name)
        self.nodes = tuple(nodes)  # make hashable

    def __eq__(self, other):
        if type(other) is type(self):
            return other.name == self.name and other.nodes == self.nodes
        return NotImplemented

    def __hash__(self):
        return hash((self.name, self.nodes))

    # def __repr__(self):
    #     return (f'{self.name}, {self.nodes}')


class SavedSample(Sample):
    '''
    A sample that has been, or is about to be, saved to persistent storage and therefore has
    a unique ID and a user associated with it. Do NOT mutate the instance variables post creation.
    :ivar id: The ID of the sample.
    :ivar user: The user who saved or is saving the sample.
    :ivar nodes: The nodes in this sample.
    :ivar savetime: The time the sample was saved.
    :ivar name: The name of the sample.
    :ivar version: The version of the sample. This may be None if the version has not yet been
        determined.
    '''

    def __init__(
            self,
            id_: UUID,
            user: UserID,
            nodes: List[SampleNode],
            savetime: datetime.datetime,
            name: Optional[str] = None,
            version: Optional[int] = None):
        '''
        Create the sample.
        :param id_: The ID of the sample.
        :param user: The user who saved the sample.
        :param nodes: The tree nodes in the sample. BIOLOGICAL_REPLICATES must come first in
            the list, and parents must come before children in the list.
        :param savetime: The time the sample was saved. Cannot be a naive datetime.
        :param name: The name of the sample. Cannot contain control characters or be longer than
            255 characters.
        :param version: The version of the sample, or None if unknown.
        :raise MissingParameterError: if no nodes are provided.
        :raises IllegalParameterError: if the name is too long or contains illegal characters,
            the first node in the list is not a BIOLOGICAL_REPLICATE, all the BIOLOGICAL_REPLICATES
            are not at the start of this list, node names are not unique, or parent nodes
            do not appear in the list prior to their children.
        '''
        # having None as a possible version doesn't sit well with me, but that means we need
        # yet another class, so...
        super().__init__(nodes, name)
        self.id = _not_falsy(id_, 'id_')
        self.user = _not_falsy(user, 'user')
        self.savetime = _check_timestamp(savetime, 'savetime')
        if version is not None and version < 1:
            raise ValueError('version must be > 0')
        self.version = version

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.id == self.id
                    and other.user == self.user
                    and other.name == self.name
                    and other.savetime == self.savetime
                    and other.version == self.version
                    and other.nodes == self.nodes)
        return NotImplemented

    def __hash__(self):
        return hash((self.id, self.user, self.name, self.savetime, self.version, self.nodes))

    # def __repr__(self):
    #     return (f'{self.id}, {self.user}, {self.name}, {self.savetime}, {self.version}, ' +
    #             f'{self.nodes}')


def _xor(bool1: bool, bool2: bool):
    return bool1 ^ bool2


class SampleAddress:
    '''
    A persistent address for a version of a sample.

    :ivar sampleid: The ID of the sample.
    :ivar version: The version of the sample.
    '''

    def __init__(self, sampleid: UUID, version: int):
        '''
        Create the address.

        :param sampleid: The ID of the sample.
        :param version: The version of the sample.
        '''
        self.sampleid = _not_falsy(sampleid, 'sampleid')
        if version is None or version < 1:
            raise IllegalParameterError('version must be > 0')
        self.version = version

    def __str__(self):
        return f'{self.sampleid}:{self.version}'

    def __eq__(self, other):
        if type(self) is type(other):
            return (self.sampleid, self.version) == (other.sampleid, other.version)
        return False

    def __hash__(self):
        return hash((self.sampleid, self.version))


class SampleNodeAddress:
    '''
    A persistent address for a node of a sample.

    :param sampleid: The ID of the sample.
    :param version: The version of the sample.
    :ivar node: The ID of the node.
    '''

    def __init__(self, sample: SampleAddress, node: str):
        '''
        Create the address.

        :param sample: The sample address.
        :param node: The ID of the sample node.
        '''

        self.sampleid = _not_falsy(sample, 'sample').sampleid
        self.version = sample.version
        self.node = _cast(str, _check_string(node, 'node', max_len=_MAX_SAMPLE_NAME_LEN))

    def __str__(self):
        return f'{self.sampleid}:{self.version}:{self.node}'

    def __eq__(self, other):
        if type(self) is type(other):
            return (self.sampleid, self.version, self.node) == (
                other.sampleid, other.version, other.node)
        return False

    def __hash__(self):
        return hash((self.sampleid, self.version, self.node))
