'''
Configuration parsing and creation for the sample service.
'''

# Because creating the samples instance involves contacting arango and the auth service,
# this code is mostly tested in the integration tests.

from collections import defaultdict as _defaultdict
import importlib
from typing import Dict, Callable, Optional, List, Tuple
from typing import cast as _cast, DefaultDict as _DefaultDict
import urllib as _urllib
from urllib.error import URLError as _URLError
import yaml as _yaml
from yaml.parser import ParserError as _ParserError
from jsonschema import validate as _validate
import arango as _arango

from SampleService.core.core_types import PrimitiveType
from SampleService.core.validator.metadata_validator import MetadataValidator
from SampleService.core.samples import Samples
from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage \
    as _ArangoSampleStorage
from SampleService.core.arg_checkers import check_string as _check_string
from SampleService.core.user_lookup import KBaseUserLookup


def build_samples(config: Dict[str, str]) -> Tuple[Samples, KBaseUserLookup]:
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
    full_roles = split_value(config, 'auth-full-admin-roles')
    read_roles = split_value(config, 'auth-read-admin-roles')

    # build the validators before trying to connect to arango
    metaval = get_validators(metaval_url) if metaval_url else MetadataValidator()

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
            auth-full-admin-roles: {', '.join(full_roles)}
            auth-read-admin-roles: {', '.join(read_roles)}
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
    user_lookup = KBaseUserLookup(auth_root_url, auth_token, full_roles, read_roles)
    return Samples(storage, user_lookup, metaval), user_lookup


def split_value(d: Dict[str, str], key: str):
    '''
    Get a list of comma separated values given a string taken from a configuration dict.
    :param config: The configuration dict containing the string to be processed as a value.
    :param key: The key in the dict containing the value.
    :returns: a list of strings split from the source comma separated string, or an empty list
    if the key does not exist or contains only whitespace.
    :raises ValueError: if the value contains control characters.
    '''
    if d is None:
        raise ValueError('d cannot be None')
    rstr = _check_string(d.get(key), 'config param ' + key, optional=True)
    if not rstr:
        return []
    return [x.strip() for x in rstr.split(',')]


def _check_string_req(s: Optional[str], name: str) -> str:
    return _cast(str, _check_string(s, name))


_META_VAL_JSONSCHEMA = {
    'type': 'object',
    # validate values only
    'additionalProperties': {
        'type': 'array',
        'items': {
            'type': 'object',
            'properties': {
                'module': {'type': 'string'},
                'callable-builder': {'type': 'string'},
                'prefix': {'type': 'boolean'},
                'parameters': {'type': 'object'}
            },
            'additionalProperties': False,
            'required': ['module', 'callable-builder']
        }
    }
}


def get_validators(url: str) -> MetadataValidator:
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

    vals: _DefaultDict[
        str, List[Callable[[str, Dict[str, PrimitiveType]], Optional[str]]]] = _defaultdict(list)
    prefix_vals: _DefaultDict[
        str, List[Callable[[str, str, Dict[str, PrimitiveType]], Optional[str]]]
        ] = _defaultdict(list)
    for key, validator_list in cfg.items():
        for i, val in enumerate(validator_list):
            m = importlib.import_module(val['module'])
            p = val.get('parameters')
            try:
                build_func = getattr(m, val['callable-builder'])
                if val.get('prefix'):  # mypy gets unhappy if we condense this
                    prefix_vals[key].append(build_func(p if p else {}))
                else:
                    vals[key].append(build_func(p if p else {}))
            except Exception as e:
                raise ValueError(
                    f'Metadata validator callable build #{i} failed for key {key}: {e.args[0]}'
                    ) from e
    return MetadataValidator(vals, prefix_vals)
