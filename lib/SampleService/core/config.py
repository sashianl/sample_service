'''
Configuration parsing and creation for the sample service.
'''

# Because creating the samples instance involves contacting arango and the auth service,
# this code is mostly tested in the integration tests.

import importlib
from typing import Dict, Callable, Optional, List, cast as _cast
import urllib as _urllib
from urllib.error import URLError as _URLError
import yaml as _yaml
from yaml.parser import ParserError as _ParserError
from jsonschema import validate as _validate
import arango as _arango

from SampleService.core.core_types import PrimitiveType
from SampleService.core.samples import Samples
from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage \
    as _ArangoSampleStorage
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.user_lookup import KBaseUserLookup as _KBaseUserLookup

# TODO NOW document config format


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

    metaval_url = _check_string(config.get('metadata-validator-config-url'),
                                'config param metadata-validator-config-url',
                                optional=True)

    # build the validators before trying to connect to arango
    metaval = get_validators(metaval_url) if metaval_url else {}

    # meta params may have info that shouldn't be logged so don't log any for now.
    # Add code to deal with this later if needed
    print(f'''
        Starting server with config (metadata validator params excluded):
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
    return Samples(storage, user_lookup, metaval)


def _check_string_req(s: Optional[str], name: str) -> str:
    return _cast(str, _check_string(s, name))


_META_VAL_JSONSCHEMA = {
    'type': 'object',
    # validate values only
    'additionalProperties': {
        'type': 'object',
        'properties': {
            'module': {'type': 'string'},
            'callable-builder': {'type': 'string'},
            'parameters': {'type': 'object'}
        },
        'additionalProperties': False,
        'required': ['module', 'callable-builder']
    }
}


def get_validators(url: str) -> Dict[
        str, List[Callable[[Dict[str, PrimitiveType]], Optional[str]]]]:
    '''
    Given a url pointing to a config file, initialize any metadata validators present
    in the configuration.

    :param url: The URL for a config file for the metadata validators.
    :returns: A mapping of metadata key to associated validator function.
    '''
    try:
        with _urllib.request.urlopen(url) as res:
            cfg = _yaml.safe_load(res)
    except _URLError as e:
        raise ValueError(
            f'Failed to open validator configuration file at {url}: {str(e.reason)}') from e
    except _ParserError as e:
        raise ValueError(
            f'Failed to open validator configuration file at {url}: {str(e)}') from e
    _validate(instance=cfg, schema=_META_VAL_JSONSCHEMA)

    ret = {}
    for k, v in cfg.items():
        m = importlib.import_module(v['module'])
        p = v.get('parameters')
        try:
            # TODO NOW handle lists of validators
            ret[k] = [getattr(m, v['callable-builder'])(p if p else {})]
        except Exception as e:
            raise ValueError(
                f'Metadata validator callable build failed for key {k}: {e.args[0]}') from e
    return ret
