'''
Contains classes relevant to linking data from outside sources (e.g. the KBase workspace
service) to samples.
'''

from __future__ import annotations

import datetime
import uuid

from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import check_timestamp as _check_timestamp
from SampleService.core.sample import SampleNodeAddress
from SampleService.core.user import UserID
from SampleService.core.workspace import DataUnitID


class DataLink:
    '''
    A link from a workspace object to a sample node.

    :ivar duid: the data ID.
    :ivar sample_node_address: the sample node address.
    :ivar created: the creation time.
    :ivar created_by: the user that created the link.
    :ivar expired: the expiration time or None if the link is not expired.
    :ivar expired_by: the user that expired the link or None if the link is not expired.
    '''

    def __init__(
            self,
            id_: uuid.UUID,
            duid: DataUnitID,
            sample_node_address: SampleNodeAddress,
            created: datetime.datetime,
            created_by: UserID,
            expired: datetime.datetime = None,
            expired_by: UserID = None):
        '''
        Create the link. If expired is provided expired_by must also be provided. If expired
        is falsy expired_by is ignored.

        :param id_: the link ID. This is generally expected to be unique per link.
        :param duid: the data ID.
        :param sample_node_address: the sample node address.
        :param created: the creation time for the link.
        :param created_by: the user that created the link.
        :param expired: the expiration time for the link or None if the link is not expired.
        :param expired_by: the user that expired the link or None if the link is not expired.
        '''
        # may need to make this non ws specific. YAGNI for now.
        self.id = _not_falsy(id_, 'id_')
        self.duid = _not_falsy(duid, 'duid')
        self.sample_node_address = _not_falsy(sample_node_address, 'sample_node_address')
        self.created = _check_timestamp(created, 'created')
        self.created_by = _not_falsy(created_by, 'created_by')
        self.expired = None
        self.expired_by = None
        if expired:
            self.expired = _check_timestamp(expired, 'expired')
            if expired < created:
                raise ValueError('link cannot expire before it is created')
            self.expired_by = _not_falsy(expired_by, 'expired_by')

    def is_equivalent(self, link: DataLink):
        '''
        Check whether this link is equivalent to another link. Equivalent means that the
        DUID and sample node address are identical.

        :param link: The link to check.
        :returns: True if the links are equivalent, False otherwise.
        '''
        _not_falsy(link, 'link')
        return (self.duid, self.sample_node_address) == (link.duid, link.sample_node_address)

    def __str__(self):
        return (f'id={self.id} ' +
                f'duid=[{self.duid}] ' +
                f'sample_node_address=[{self.sample_node_address}] ' +
                f'created={self.created.timestamp()} ' +
                f'created_by={self.created_by} ' +
                f'expired={self.expired.timestamp() if self.expired else None} ' +
                f'expired_by={self.expired_by}')

    def __eq__(self, other):
        if type(self) == type(other):
            return (self.id, self.duid, self.sample_node_address,
                    self.created, self.created_by, self.expired, self.expired_by) == (
                other.id, other.duid, other.sample_node_address,
                other.created, other.created_by, other.expired, other.expired_by)
        return False

    def __hash__(self):
        return hash((self.id, self.duid, self.sample_node_address,
                     self.created, self.created_by, self.expired, self.expired_by))
