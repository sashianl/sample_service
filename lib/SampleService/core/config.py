'''
Configuration parsing and creation for the sample service.
'''

# Because creating the samples instance involves contacting arango and the auth service,
# this code is mostly tested in the integration tests.


from typing import Dict, Optional as _Optional, cast as _cast
import arango as _arango

from SampleService.core.samples import Samples as _Samples
from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage \
    as _ArangoSampleStorage
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.user_lookup import KBaseUserLookup as _KBaseUserLookup


def build_samples(config: Dict[str, str]):
    '''
    Build the sample service instance from the SDK server provided parameters.
    '''
    if not config:
        raise ValueError('config is empty, cannot start service')
    arango_url = _check_string_req(config.get('arango-url'), 'config param arango-url')
    arango_db = _check_string_req(config.get('arango-db'), 'config param arango-db')
    arango_user = _check_string_req(config.get('arango-user'), 'config param arango-user')
    arango_pwd = _check_string_req(config.get('arango-pwd'), 'config param arango-pwd')

    col_sample = _check_string_req(config.get('sample-collection'),
                                   'config param sample-collection')
    col_version = _check_string_req(
        config.get('version-collection'), 'config param version-collection')
    col_ver_edge = _check_string_req(
        config.get('version-edge-collection'), 'config param version-edge-collection')
    col_node = _check_string_req(config.get('node-collection'), 'config param node-collection')
    col_node_edge = _check_string_req(
        config.get('node-edge-collection'), 'config param node-edge-collection')
    col_schema = _check_string_req(config.get('schema-collection'),
                                   'config param schema-collection')

    auth_root_url = _check_string_req(config.get('auth-root-url'), 'config param auth-root-url')
    auth_token = _check_string_req(config.get('auth-token'), 'config param auth-token')

    print(f'''
        Starting server with config:
            arango-url: {arango_url}
            arango-db: {arango_db}
            arango-user: {arango_user}
            arango-pwd: [REDACTED FOR YOUR SAFETY AND COMFORT]
            sample-collection: {col_sample}
            version-collection: {col_version}
            version-edge-collection: {col_ver_edge}
            node-collection: {col_node}
            node-edge-collection: {col_node_edge}
            schema-collection: {col_schema}
            auth-root-url: {auth_root_url}
            auth-token: [REDACTED FOR YOUR CONVENIENCE AND ENJOYMENT]
    ''')

    arangoclient = _arango.ArangoClient(hosts=arango_url)
    arango_db = arangoclient.db(
        arango_db, username=arango_user, password=arango_pwd, verify=True)
    storage = _ArangoSampleStorage(
        arango_db,
        col_sample,
        col_version,
        col_ver_edge,
        col_node,
        col_node_edge,
        col_schema,
    )
    user_lookup = _KBaseUserLookup(auth_root_url, auth_token)
    # TODO VALIDATION pass in validators
    val = {'foo': lambda x: None}  # TODO REMOVE
    return _Samples(storage, user_lookup, val)


def _check_string_req(s: _Optional[str], name: str) -> str:
    return _cast(str, _check_string(s, name))
