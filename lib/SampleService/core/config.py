'''
Configuration parsing and creation for the sample service.
'''

# Because creating the samples instance involves contacting arango and the auth service,
# this code is mostly tested in the integration tests.

import importlib
from collections import defaultdict as _defaultdict
from typing import Dict, Callable, Optional as _Optional, cast as _cast
from typing import DefaultDict as _DefaultDict
import arango as _arango

from SampleService.core.core_types import PrimitiveType
from SampleService.core.samples import Samples
from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage \
    as _ArangoSampleStorage
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.user_lookup import KBaseUserLookup as _KBaseUserLookup


def build_samples(config: Dict[str, str]) -> Samples:
    '''
    Build the sample service instance from the SDK server provided parameters.

    :param cfg: The SDK generated configuration.
    :returns: A samples instance.
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
    # return _Samples(storage, user_lookup, get_validators(config))
    return Samples(storage, user_lookup, val)


def _check_string_req(s: _Optional[str], name: str) -> str:
    return _cast(str, _check_string(s, name))


def get_validators(cfg: Dict[str, str]) -> Dict[str, Callable[[Dict[str, PrimitiveType]], None]]:
    '''
    Given an SDK server generated config mapping, initialize any metadata validators present
    in the configuration.

    :param cfg: The SDK generated configuration.
    :returns: A mapping of metadata key to associated validator function.
    '''
    # https://github.com/python/mypy/issues/4226
    def lst() -> list:
        return [{}, {}]
    valparams: _DefaultDict[str, list] = _defaultdict(lst)
    for k, v in cfg.items():
        if k.startswith('metaval-'):
            p = k.split('-')
            if len(p) == 3:
                valparams[p[1]][0][p[2]] = v
            elif len(p) == 4:
                if p[2] != 'param':
                    raise ValueError(f'invalid configuration key: {k}')
                valparams[p[1]][1][p[3]] = v
            else:
                raise ValueError(f'invalid configuration key: {k}')

    ret = {}
    for k, v2 in valparams.items():
        if 'module' not in v2[0]:
            raise ValueError(f'Missing config param metaval-{k}-module')
        if 'callable_builder' not in v2[0]:
            raise ValueError(f'Missing config param metaval-{k}-callable_builder')
        m = importlib.import_module(v2[0]['module'])
        try:
            ret[k] = getattr(m, v2[0]['callable_builder'])(v2[1])
        except Exception as e:
            raise ValueError(
                f'Metadata validator callable build failed for key {k}: {e.args[0]}') from e
    return ret
