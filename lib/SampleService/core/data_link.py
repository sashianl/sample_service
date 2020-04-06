'''
Contains classes relevant to linking data from outside sources (e.g. the KBase workspace
service) to samples.
'''

import datetime

from SampleService.core.arg_checkers import not_falsy as _not_falsy
from SampleService.core.arg_checkers import check_timestamp as _check_timestamp
from SampleService.core.workspace import DataUnitID
from SampleService.core.sample import SampleNodeAddress


class DataLink:
    '''
    A link from a workspace object to a sample node.

    :ivar duid: the data ID.
    :ivar sample_node_address: the sample node address.
    :ivar create: the creation time.
    :ivar expire: the expiration time or None if the link is not expired.
    '''

    def __init__(
            self,
            duid: DataUnitID,
            sample_node_address: SampleNodeAddress,
            create: datetime.datetime,
            expire: datetime.datetime = None):
        '''
        Create the link.

        :param duid: the data ID.
        :param sample_node_address: the sample node address.
        :param create: the creation time for the link.
        :param expire: the expiration time for the link, or None if the link is not expired.
        '''
        # may need to make this non ws specific. YAGNI for now.
        self.duid = _not_falsy(duid, 'duid')
        self.sample_node_address = _not_falsy(sample_node_address, 'sample_node_address')
        self.create = _check_timestamp(create, 'create')
        self.expire = None
        if expire:
            self.expire = _check_timestamp(expire, 'expire')

    def __str__(self):
        return (f'duid=[{self.duid}] ' +
                f'sample_node_address=[{self.sample_node_address}] ' +
                f'create={self.create.timestamp()} ' +
                f'expire={self.expire.timestamp() if self.expire else None}')

    def __eq__(self, other):
        if type(self) == type(other):
            return (self.duid, self.sample_node_address, self.create, self.expire) == (
                other.duid, other.sample_node_address, other.create, other.expire)
        return False

    def __hash__(self):
        return hash((self.duid, self.sample_node_address, self.create, self.expire))
