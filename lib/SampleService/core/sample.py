'''
Contains classes related to samples.
'''

from enum import Enum as _Enum, unique as _unique
from uuid import UUID
from typing import Optional, List
from typing import Set as _Set, cast as _cast
from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.errors import IllegalParameterError, MissingParameterError

# for now we'll assume people are nice and don't change attributes after init.
# if that doesn't hold true, override __setattr__.


_MAX_SAMPLE_NAME_LEN = 255


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


class SampleNode:
    '''
    A node in the sample tree.
    :ivar name: The name of the sample node.
    :ivar type: The type of this sample nde.
    :ivar parent: The parent SampleNode of this node.
    '''

    def __init__(
            self,
            name: str,
            type_: SubSampleType = SubSampleType.BIOLOGICAL_REPLICATE,
            parent: Optional[str] = None):
        '''
        Create a sample node.
        :param name: The name of the sample node.
        :param type_: The type of this sample nde.
        :param parent: The parent SampleNode of this node. BIOLOGICAL_REPLICATEs, and only
            BIOLOGICAL_REPLICATEs, cannot have parents.
        :raises MissingParameterError: if the name is None or whitespace only.
        :raises IllegalParameterError: if the name or parent is too long or contains illegal
            characters or the parent is missing and the node type is not BIOLOGICAL_REPLICATE.
        '''
        # could make a bioreplicate class... meh for now
        self.name = _cast(str, _check_string(name, 'subsample name', max_len=_MAX_SAMPLE_NAME_LEN))
        self.type = _not_falsy(type_, 'type')
        self.parent = _check_string(parent, 'parent', max_len=_MAX_SAMPLE_NAME_LEN, optional=True)
        isbiorep = type_ == SubSampleType.BIOLOGICAL_REPLICATE
        if not _xor(bool(parent), isbiorep):
            raise IllegalParameterError(
                f'Node {self.name} is of type {type_.value} and therefore ' +
                f'{"cannot" if isbiorep else "must"} have a parent')

        # TODO metadata, description

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.name == self.name
                    and other.type == self.type
                    and other.parent == self.parent)
        return NotImplemented

    def __hash__(self):
        return hash((self.name, self.type, self.parent))


class Sample:
    '''
    A sample containing biological replicates, technical replicates, and sub samples.
    Do NOT mutate the instance variables post creation.
    :ivar nodes: The nodes in this sample.
    :ivar name: The name of the sample.
    '''

    def __init__(self, nodes: List[SampleNode], name: Optional[str] = None):
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
                print(f'seen: {seen_names}')
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


class SampleWithID(Sample):
    '''
    A sample including an ID. Do NOT mutate the instance variables post creation.
    :ivar id: The ID of the sample.
    :ivar nodes: The nodes in this sample.
    :ivar name: The name of the sample.
    :ivar version: The version of the sample. This may be None if the version has not yet been
        determined.
    '''

    def __init__(
            self,
            id_: UUID,
            nodes: List[SampleNode],
            name: Optional[str] = None,
            version: Optional[int] = None):
        '''
        Create the sample.
        :param id_: The ID of the sample.
        :param nodes: The tree nodes in the sample. BIOLOGICAL_REPLICATES must come first in
            the list, and parents must come before children in the list.
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
        if version is not None and version < 1:
            raise ValueError('version must be > 0')
        self.version = version

    def __eq__(self, other):
        if type(other) is type(self):
            return (other.id == self.id
                    and other.name == self.name
                    and other.version == self.version
                    and other.nodes == self.nodes)
        return NotImplemented

    def __hash__(self):
        return hash((self.id, self.name, self.version, self.nodes))


def _xor(bool1: bool, bool2: bool):
    return bool1 ^ bool2
