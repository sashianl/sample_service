import datetime

from pytest import raises
from uuid import UUID
import json
from unittest.mock import create_autospec

from SampleService.core.api_translation import datetime_to_epochmilliseconds, get_id_from_object
from SampleService.core.api_translation import get_version_from_object, sample_to_dict
from SampleService.core.api_translation import acls_to_dict, acls_from_dict
from SampleService.core.api_translation import create_sample_params, get_sample_address_from_object
from SampleService.core.api_translation import (
    check_admin,
    get_static_key_metadata_params,
    create_data_link_params,
    get_datetime_from_epochmilliseconds_in_object,
    links_to_dicts,
    get_upa_from_object,
    get_data_unit_id_from_object,
    get_user_from_object,
    get_admin_request_from_object,
    acl_delta_from_dict
)
from SampleService.core.data_link import DataLink
from SampleService.core.sample import (
    Sample,
    SampleNode,
    SampleAddress,
    SampleNodeAddress,
    SubSampleType,
    SavedSample,
    SourceMetadata,
)
from SampleService.core.acls import SampleACL, SampleACLOwnerless, SampleACLDelta
from SampleService.core.errors import (
    IllegalParameterError,
    MissingParameterError,
    UnauthorizedError,
    NoSuchUserError
)
from SampleService.core.acls import AdminPermission
from SampleService.core.user_lookup import KBaseUserLookup
from SampleService.core.user import UserID
from SampleService.core.workspace import DataUnitID, UPA

from core.test_utils import assert_exception_correct


def test_get_user_from_object():
    assert get_user_from_object({}, 'user') is None
    assert get_user_from_object({'user': None}, 'user') is None
    assert get_user_from_object({'user': '   a   '}, 'user') == UserID('a')


def test_get_user_from_object_fail_bad_args():
    _get_user_from_object_fail(None, 'us', ValueError('params cannot be None'))
    _get_user_from_object_fail({'us': 'foo'}, None, MissingParameterError('key'))
    _get_user_from_object_fail({'us': []}, 'us', IllegalParameterError(
        'us must be a string if present'))
    _get_user_from_object_fail({'us': 'baz\tbaat'}, 'us', IllegalParameterError(
        # probably not worth the trouble to change the key name, we'll see
        'userid contains control characters'))


def _get_user_from_object_fail(params, key, expected):
    with raises(Exception) as got:
        get_user_from_object(params, key)
    assert_exception_correct(got.value, expected)


def test_get_admin_request_from_object():
    assert get_admin_request_from_object({'user': 'foo'}, 'as_ad', 'user') == (False, None)
    assert get_admin_request_from_object(
        {'as_ad': False, 'user': 'a'}, 'as_ad', 'user') == (False, None)
    assert get_admin_request_from_object(
        {'as_ad': [], 'user': 'a'}, 'as_ad', 'user') == (False, None)
    assert get_admin_request_from_object({'as_ad': True}, 'as_ad', 'user') == (True, None)
    assert get_admin_request_from_object(
        {'as_ad': True, 'user': None}, 'as_ad', 'user') == (True, None)
    assert get_admin_request_from_object(
        {'as_ad': 3, 'user': 'a'}, 'as_ad', 'user') == (True, UserID('a'))


def test_get_admin_request_from_object_fail_bad_args():
    _get_admin_request_from_object_fail(None, '1', '2', ValueError('params cannot be None'))
    _get_admin_request_from_object_fail({'a': 'b'}, None, '2', MissingParameterError('as_admin'))
    _get_admin_request_from_object_fail({'a': 'b'}, '1', None, MissingParameterError('as_user'))
    _get_admin_request_from_object_fail(
        {'asa': True, 'asu': ['foo']}, 'asa', 'asu', IllegalParameterError(
            'asu must be a string if present'))
    _get_admin_request_from_object_fail(
        {'asa': True, 'asu': 'whe\tee'}, 'asa', 'asu', IllegalParameterError(
            'userid contains control characters'))


def _get_admin_request_from_object_fail(params, akey, ukey, expected):
    with raises(Exception) as got:
        get_admin_request_from_object(params, akey, ukey)
    assert_exception_correct(got.value, expected)


def test_get_id_from_object():
    assert get_id_from_object(None, 'id', False) is None
    assert get_id_from_object({}, 'id', False) is None
    assert get_id_from_object({'id': None}, 'id', 'foo', False) is None
    assert get_id_from_object({'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41e'}, 'id', False) == UUID(
        'f5bd78c3-823e-40b2-9f93-20e78680e41e')
    assert get_id_from_object(
        {'lid': 'f5bd78c3-823e-40b2-9f93-20e78680e41e'},
        'lid',
        'foo',
        True) == UUID('f5bd78c3-823e-40b2-9f93-20e78680e41e')


def test_get_id_from_object_fail_bad_args():
    _get_id_from_object_fail(None, 'id', None, True, MissingParameterError('id'))
    _get_id_from_object_fail(None, 'id', 'thing', True, MissingParameterError('thing'))
    _get_id_from_object_fail({}, 'id', None, True, MissingParameterError('id'))
    _get_id_from_object_fail({'id': None}, 'id', None, True, MissingParameterError('id'))
    _get_id_from_object_fail({'id': None}, 'id', 'foo', True, MissingParameterError('foo'))
    _get_id_from_object_fail({'wid': 6}, 'wid', None, True, IllegalParameterError(
        'wid 6 must be a UUID string'))
    _get_id_from_object_fail({'id': 6}, 'id', 'whew', True, IllegalParameterError(
        'whew 6 must be a UUID string'))
    _get_id_from_object_fail(
        {'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41'}, 'id', None, False, IllegalParameterError(
            'id f5bd78c3-823e-40b2-9f93-20e78680e41 must be a UUID string'))
    _get_id_from_object_fail({'id': 'bar'}, 'id', 'whee', False, IllegalParameterError(
            'whee bar must be a UUID string'))

    goodid = 'f5bd78c3-823e-40b2-9f93-20e78680e41e'
    _get_id_from_object_fail({'id': goodid}, None, None, True, MissingParameterError('key'))


def _get_id_from_object_fail(d, key, name, required, expected):
    with raises(Exception) as got:
        get_id_from_object(d, key, name, required)
    assert_exception_correct(got.value, expected)


def dt(t):
    return datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)


def test_to_epochmilliseconds():
    assert datetime_to_epochmilliseconds(dt(54.97893)) == 54979
    assert datetime_to_epochmilliseconds(dt(-108196017.5496)) == -108196017550


def test_to_epochmilliseconds_fail_bad_args():
    with raises(Exception) as got:
        datetime_to_epochmilliseconds(None)
    assert_exception_correct(got.value, ValueError('d cannot be a value that evaluates to false'))


def test_create_sample_params_minimal():
    params = {'sample': {'version': 7,      # should be ignored
                         'save_date': 9,    # should be ignored
                         'node_tree': [{'id': 'foo',
                                        'type': 'BioReplicate'}]
                         }}
    expected = Sample([SampleNode('foo')])

    assert create_sample_params(params) == (expected, None, None)


def test_create_sample_params_maximal():
    params = {'sample': {'version': 7,      # should be ignored
                         'save_date': 9,    # should be ignored
                         'id': '706fe9e1-70ef-4feb-bbd9-32295104a119',
                         'name': 'myname',
                         'node_tree': [{'id': 'foo',
                                        'type': 'BioReplicate'},
                                       {'id': 'bar',
                                        'parent': 'foo',
                                        'type': 'TechReplicate',
                                        'meta_controlled':
                                            {'concentration/NO2':
                                                {'species': 'NO2',
                                                 'units': 'ppm',
                                                 'value': 78.91,
                                                 'protocol_id': 782,
                                                 'some_boolean_or_other': True
                                                 },
                                             'a': {'b': 'c'}
                                             },
                                        'meta_user': {'location_name': {'name': 'my_buttocks'}},
                                        'source_meta': [
                                            {'key': 'concentration/NO2',
                                             'skey': 'conc_nitrous_oxide',
                                             'svalue': {
                                                 'spec': 'nit ox',
                                                 'ppb': 0.07891,
                                                 'prot+2': 784,
                                                 'is this totally made up': False
                                                 }
                                             },
                                            {'key': 'a',
                                             'skey': 'vscode',
                                             'svalue': {'really': 'stinks'}
                                             }
                                            ]
                                        }
                                       ]
                         },
              'prior_version': 1}

    assert create_sample_params(params) == (
        Sample([
            SampleNode('foo'),
            SampleNode(
                'bar',
                SubSampleType.TECHNICAL_REPLICATE,
                'foo',
                {'concentration/NO2':
                    {'species': 'NO2',
                     'units': 'ppm',
                     'value': 78.91,
                     'protocol_id': 782,
                     'some_boolean_or_other': True
                     },
                 'a': {'b': 'c'}
                 },
                {'location_name': {'name': 'my_buttocks'}},
                [SourceMetadata(
                    'concentration/NO2',
                    'conc_nitrous_oxide',
                    {
                     'spec': 'nit ox',
                     'ppb': 0.07891,
                     'prot+2': 784,
                     'is this totally made up': False
                     }
                    ),
                 SourceMetadata('a', 'vscode', {'really': 'stinks'})
                 ]
                )
            ],
            'myname'
        ),
        UUID('706fe9e1-70ef-4feb-bbd9-32295104a119'),
        1)


def test_create_sample_params_fail_bad_input():
    create_sample_params_fail(
        None, ValueError('params cannot be None'))
    create_sample_params_fail(
        {}, IllegalParameterError('params must contain sample key that maps to a structure'))
    create_sample_params_fail(
        {'sample': {}},
        IllegalParameterError('sample node tree must be present and a list'))
    create_sample_params_fail(
        {'sample': {'node_tree': {'foo', 'bar'}}},
        IllegalParameterError('sample node tree must be present and a list'))
    create_sample_params_fail(
        {'sample': {'node_tree': [], 'name': 6}},
        IllegalParameterError('sample name must be omitted or a string'))
    create_sample_params_fail(
        {'sample': {'node_tree': [{'id': 'foo', 'type': 'BioReplicate'}, 'foo']}},
        IllegalParameterError('Node at index 1 is not a structure'))
    create_sample_params_fail(
        {'sample': {'node_tree': [{'type': 'BioReplicate'}, 'foo']}},
        IllegalParameterError('Node at index 0 must have an id key that maps to a string'))
    create_sample_params_fail(
        {'sample': {'node_tree': [{'id': 'foo', 'type': 'BioReplicate'},
                                  {'id': None, 'type': 'BioReplicate'}, 'foo']}},
        IllegalParameterError('Node at index 1 must have an id key that maps to a string'))
    create_sample_params_fail(
        {'sample': {'node_tree': [{'id': 'foo', 'type': 'BioReplicate'},
                                  {'id': 6, 'type': 'BioReplicate'}, 'foo']}},
        IllegalParameterError('Node at index 1 must have an id key that maps to a string'))
    create_sample_params_fail(
        {'sample': {'node_tree': [{'id': 'foo', 'type': 'BioReplicate'},
                                  {'id': 'foo'}, 'foo']}},
        IllegalParameterError('Node at index 1 has an invalid sample type: None'))
    create_sample_params_fail(
        {'sample': {'node_tree': [{'id': 'foo', 'type': 6},
                                  {'id': 'foo'}, 'foo']}},
        IllegalParameterError('Node at index 0 has an invalid sample type: 6'))
    create_sample_params_fail(
        {'sample': {'node_tree': [{'id': 'foo', 'type': 'BioReplicate2'},
                                  {'id': 'foo'}, 'foo']}},
        IllegalParameterError('Node at index 0 has an invalid sample type: BioReplicate2'))
    create_sample_params_fail(
        {'sample': {'node_tree': [{'id': 'foo', 'type': 'BioReplicate'},
                                  {'id': 'foo', 'type': 'TechReplicate', 'parent': 6}]}},
        IllegalParameterError('Node at index 1 has a parent entry that is not a string'))

    create_sample_params_meta_fail(6, "Node at index {}'s {} entry must be a mapping")
    create_sample_params_meta_fail(
        {'foo': {}, 'bar': 'yay'},
        "Node at index {}'s {} entry does not have a dict as a value at key bar")
    create_sample_params_meta_fail(
        {'foo': {}, 'bar': {'baz': 1, 'bat': ['yay']}},
        "Node at index {}'s {} entry does not have a primitive type as the value at bar/bat")
    create_sample_params_meta_fail(
        {'foo': {}, None: 'yay'},
        "Node at index {}'s {} entry contains a non-string key")
    create_sample_params_meta_fail(
        {'foo': {None: 'foo'}, 'bar': {'a': 'yay'}},
        "Node at index {}'s {} entry contains a non-string key under key foo")

    m = {'foo': {'b\nar': 'foo'}, 'bar': {'a': 'yay'}}
    create_sample_params_fail(
        {'sample': {'node_tree': [
            {'id': 'foo', 'type': 'BioReplicate', 'meta_controlled': m}]}},
        IllegalParameterError('Error for node at index 0: Controlled metadata value key b\nar ' +
                              'associated with metadata key foo has a character at index 1 that ' +
                              'is a control character.'))

    create_sample_params_fail(
        {'sample': {'node_tree': [{'id': 'foo', 'type': 'BioReplicate'},
                                  {'id': 'bar', 'type': 'TechReplicate', 'parent': 'yay'}]}},
        IllegalParameterError('Parent yay of node bar does not appear in node list prior to node.'))

    # the id getting function is tested above so we don't repeat here, just 1 failing test
    create_sample_params_fail(
        {'sample': {'node_tree': [{'id': 'foo', 'type': 'BioReplicate'},
                                  {'id': 'bar', 'type': 'TechReplicate', 'parent': 'bar'}],
                    'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41'}},
        IllegalParameterError(
            'sample.id f5bd78c3-823e-40b2-9f93-20e78680e41 must be a UUID string'))

    create_sample_params_fail(
        {'sample': {'node_tree': [{'id': 'foo', 'type': 'BioReplicate'},
                                  {'id': 'bar', 'type': 'TechReplicate', 'parent': 'foo'}]},
         'prior_version': 'six'},
        IllegalParameterError('prior_version must be an integer if supplied'))


def create_sample_params_meta_fail(m, expected):
    create_sample_params_fail(
        {'sample': {'node_tree': [
            {'id': 'foo', 'type': 'BioReplicate', 'meta_controlled': m}]}},
        IllegalParameterError(expected.format(0, 'controlled metadata')))
    create_sample_params_fail(
        {'sample': {'node_tree': [
            {'id': 'bar', 'type': 'BioReplicate'},
            {'id': 'foo', 'type': 'SubSample', 'parent': 'bar', 'meta_user': m}]}},
        IllegalParameterError(expected.format(1, 'user metadata')))


def test_create_sample_params_source_metadata_fail():
    _create_sample_params_source_metadata_fail(
        {'a': {'b': 'c'}}, {'a': {'b': 'c'}},
        IllegalParameterError("Node at index 1's source metadata must be a list")
    )
    _create_sample_params_source_metadata_fail(
        {'a': {'b': 'c'}}, [{'key': 'a', 'skey': 'b', 'svalue': {'b': 'c'}}, 'foo'],
        IllegalParameterError(
            "Node at index 1's source metadata has an entry at index 1 that is not a dict")
    )
    _create_sample_params_source_metadata_fail(
        {'a': {'b': 'c'}},
        [{'key': 'a', 'skey': 'b', 'svalue': {'b': 'c'}},
         {'key': 8, 'skey': 'b', 'svalue': {'b': 'c'}}],
        IllegalParameterError("Node at index 1's source metadata has an entry at index 1 " +
                              'where the required key field is not a string')
    )
    _create_sample_params_source_metadata_fail(
        {'a': {'b': 'c'}},
        [{'key': 'a', 'skey': [], 'svalue': {'b': 'c'}},
         {'key': 'b', 'skey': 'b', 'svalue': {'b': 'c'}}],
        IllegalParameterError("Node at index 1's source metadata has an entry at index 0 " +
                              'where the required skey field is not a string')
    )
    _create_sample_params_source_metadata_fail(
        {'a': {'b': 'c'}},
        [{'key': 'a', 'skey': 'a', 'svalue': {'b': 'c'}},
         {'key': 'b', 'skey': 'b', 'svalue': ['f']}],
        IllegalParameterError("Node at index 1's source metadata has an entry at index 1 " +
                              'where the required svalue field is not a mapping')
    )
    _create_sample_params_source_metadata_fail(
        {'a': {'b': 'c'}},
        [{'key': 'a', 'skey': 'a', 'svalue': {8: 'c'}},
         {'key': 'b', 'skey': 'b', 'svalue': {'b': 'c'}}],
        IllegalParameterError("Node at index 1's source metadata has an entry at index 0 " +
                              'with a value mapping key that is not a string')
    )
    _create_sample_params_source_metadata_fail(
        {'a': {'b': 'c'}},
        [{'key': 'a', 'skey': 'a', 'svalue': {'c': [43]}},
         {'key': 'b', 'skey': 'b', 'svalue': {'b': 'c'}}],
        IllegalParameterError(
            "Node at index 1's source metadata has an entry at index 0 with a value in the " +
            'value mapping under key c that is not a primitive type')
    )
    _create_sample_params_source_metadata_fail(
        {'a': {'b': 'c'}},
        [{'key': 'a', 'skey': 'a', 'svalue': {'c': 'x'}},
         {'key': 'b', 'skey': 'b', 'svalue': {'b\n': 'c'}}],
        IllegalParameterError(
            "Node at index 1's source metadata has an error at index 1: Source metadata value " +
            'key b\n associated with metadata key b has a character at index 1 that is a control ' +
            'character.')
    )
    _create_sample_params_source_metadata_fail(
        {'a': {'b': 'c'}},
        [{'key': 'a', 'skey': 'a', 'svalue': {'c': 'x'}},
         {'key': 'b', 'skey': 'b', 'svalue': {'b': 'c'}}],
        IllegalParameterError(
            'Error for node at index 1: Source metadata key b does not appear in the ' +
            'controlled metadata')
    )


def _create_sample_params_source_metadata_fail(m, s, expected):
    create_sample_params_fail(
        {'sample': {'node_tree': [
            {'id': 'bar', 'type': 'BioReplicate'},
            {'id': 'foo',
             'type': 'SubSample',
             'parent': 'bar',
             'meta_controlled': m,
             'source_meta': s}]}},
        expected)


def create_sample_params_fail(params, expected):
    with raises(Exception) as got:
        create_sample_params(params)
    assert_exception_correct(got.value, expected)


def test_get_version_from_object():
    assert get_version_from_object({}) is None
    assert get_version_from_object({'version': None}) is None
    assert get_version_from_object({'version': 3}, True) == 3
    assert get_version_from_object({'version': 1}) == 1


def test_get_version_from_object_fail_bad_args():
    get_version_from_object_fail(None, False, ValueError('params cannot be None'))
    get_version_from_object_fail({}, True, MissingParameterError('version'))
    get_version_from_object_fail(
        {'version': None}, True, MissingParameterError('version'))
    get_version_from_object_fail(
        {'version': 'whee'}, False, IllegalParameterError('Illegal version argument: whee'))
    get_version_from_object_fail(
        {'version': 0}, True, IllegalParameterError('Illegal version argument: 0'))
    get_version_from_object_fail(
        {'version': -3}, False, IllegalParameterError('Illegal version argument: -3'))


def get_version_from_object_fail(params, required, expected):
    with raises(Exception) as got:
        get_version_from_object(params, required)
    assert_exception_correct(got.value, expected)


def test_get_sample_address_from_object():
    assert get_sample_address_from_object({'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41e'}) == (
        UUID('f5bd78c3-823e-40b2-9f93-20e78680e41e'), None)
    assert get_sample_address_from_object({
        'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41e',
        'version': 1}, version_required=True) == (
        UUID('f5bd78c3-823e-40b2-9f93-20e78680e41e'), 1)


def test_get_sample_address_from_object_fail_bad_args():
    get_sample_address_from_object_fail(None, False, MissingParameterError('id'))
    get_sample_address_from_object_fail({}, False, MissingParameterError('id'))
    get_sample_address_from_object_fail({'id': None}, False, MissingParameterError('id'))
    get_sample_address_from_object_fail({'id': 6}, False, IllegalParameterError(
        'id 6 must be a UUID string'))
    get_sample_address_from_object_fail(
        {'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41'}, False,
        IllegalParameterError(
            'id f5bd78c3-823e-40b2-9f93-20e78680e41 must be a UUID string'))
    id_ = 'f5bd78c3-823e-40b2-9f93-20e78680e41e'
    get_sample_address_from_object_fail(
        {'id': id_}, True, MissingParameterError('version'))
    get_sample_address_from_object_fail(
        {'id': id_, 'version': [1]}, False, IllegalParameterError('Illegal version argument: [1]'))

    get_version_from_object_fail(
        {'id': id_, 'version': 'whee'},
        True,
        IllegalParameterError('Illegal version argument: whee'))
    get_version_from_object_fail(
        {'id': id_, 'version': 0}, True, IllegalParameterError('Illegal version argument: 0'))
    get_version_from_object_fail(
        {'id': id_, 'version': -3}, True, IllegalParameterError('Illegal version argument: -3'))


def get_sample_address_from_object_fail(params, required, expected):
    with raises(Exception) as got:
        get_sample_address_from_object(params, required)
    assert_exception_correct(got.value, expected)


def test_sample_to_dict_minimal():

    expected = {'node_tree': [{'id': 'foo',
                               'type': 'BioReplicate',
                               'meta_controlled': {},
                               'meta_user': {},
                               'source_meta': [],
                               'parent': None
                               }],
                'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41e',
                'user': 'user2',
                'save_date': 87897,
                'name': None,
                'version': None,
                }

    id_ = UUID('f5bd78c3-823e-40b2-9f93-20e78680e41e')

    s = sample_to_dict(SavedSample(id_, UserID('user2'), [SampleNode('foo')], dt(87.8971)))

    assert s == expected

    # ensure that the result is jsonable. The data structure includes frozen maps which are not
    json.dumps(s)


def test_sample_to_dict_maximal():
    expected = {'node_tree': [{'id': 'foo',
                               'type': 'BioReplicate',
                               'meta_controlled': {},
                               'meta_user': {},
                               'source_meta': [],
                               'parent': None
                               },
                              {'id': 'bar',
                               'type': 'TechReplicate',
                               'meta_controlled': {'a': {'b': 'c', 'm': 6.7}, 'b': {'c': 'd'}},
                               'meta_user': {'d': {'e': True}, 'g': {'h': 1}},
                               'source_meta': [
                                   {'key': 'a', 'skey': 'x', 'svalue': {'v': 2}},
                                   {'key': 'b', 'skey': 'y', 'svalue': {'z': 3}}
                                   ],
                               'parent': 'foo'
                               }],
                'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41e',
                'user': 'user3',
                'save_date': 87897,
                'name': 'myname',
                'version': 23,
                }

    id_ = UUID('f5bd78c3-823e-40b2-9f93-20e78680e41e')

    s = sample_to_dict(
        SavedSample(
            id_,
            UserID('user3'),
            [SampleNode('foo'),
             SampleNode(
                 'bar',
                 SubSampleType.TECHNICAL_REPLICATE,
                 'foo',
                 {'a': {'b': 'c', 'm': 6.7}, 'b': {'c': 'd'}},
                 {'d': {'e': True}, 'g': {'h': 1}},
                 [SourceMetadata('a', 'x', {'v': 2}), SourceMetadata('b', 'y', {'z': 3})]),
             ],
            dt(87.8971),
            'myname',
            23))

    assert s == expected

    # ensure that the result is jsonable. The data structure includes frozen maps which are not
    json.dumps(s)


def test_sample_to_dict_fail():
    with raises(Exception) as got:
        sample_to_dict(None)
    assert_exception_correct(
        got.value, ValueError('sample cannot be a value that evaluates to false'))


def test_acls_to_dict_minimal():
    assert acls_to_dict(SampleACL(UserID('user'), dt(1))) == {
        'owner': 'user',
        'admin': (),
        'write': (),
        'read': (),
        'public_read': 0
    }


def test_acls_to_dict_maximal():
    assert acls_to_dict(
        SampleACL(
            UserID('user'),
            dt(1),
            [UserID('foo'), UserID('bar')],
            [UserID('baz')],
            [UserID('hello'), UserID("I'm"), UserID('a'), UserID('robot')],
            True)) == {
        'owner': 'user',
        'admin': ('bar', 'foo'),
        'write': ('baz',),
        'read': ("I'm", 'a', 'hello', 'robot'),
        'public_read': 1
    }


def test_acls_to_dict_remove_service_token():
    assert acls_to_dict(
        SampleACL(
            UserID('user'),
            dt(1),
            [UserID('foo'), UserID('bar')],
            [UserID('baz')],
            [UserID('hello'), UserID("I'm"), UserID('a'), UserID('robot')],
            True), read_exempt_roles=["I'm", 'a']) == {
        'owner': 'user',
        'admin': ('bar', 'foo'),
        'write': ('baz',),
        'read': ('hello', 'robot'),
        'public_read': 1
    }


def test_acls_to_dict_fail():
    with raises(Exception) as got:
        acls_to_dict(None)
    assert_exception_correct(
        got.value, ValueError('acls cannot be a value that evaluates to false'))


def test_acls_from_dict():
    assert acls_from_dict({'acls': {}}) == SampleACLOwnerless()
    assert acls_from_dict({'acls': {'public_read': 0}}) == SampleACLOwnerless()
    assert acls_from_dict({'acls': {
        'read': [],
        'admin': ['whee', 'whoo'],
        'public_read': None}}) == SampleACLOwnerless([UserID('whee'), UserID('whoo')])
    assert acls_from_dict({'acls': {
        'read': ['a', 'b'],
        'write': ['x'],
        'admin': ['whee', 'whoo'],
        'public_read': 1}}) == SampleACLOwnerless(
            [UserID('whee'), UserID('whoo')], [UserID('x')], [UserID('a'), UserID('b')], True)


def test_acls_from_dict_fail_bad_args():
    _acls_from_dict_fail(None, ValueError('d cannot be a value that evaluates to false'))
    _acls_from_dict_fail({}, ValueError('d cannot be a value that evaluates to false'))
    m = 'ACLs must be supplied in the acls key and must be a mapping'
    _acls_from_dict_fail({'acls': None}, IllegalParameterError(m))
    _acls_from_dict_fail({'acls': 'foo'}, IllegalParameterError(m))
    _acls_from_dict_fail_acl_check('read')
    _acls_from_dict_fail_acl_check('write')
    _acls_from_dict_fail_acl_check('admin')


def _acls_from_dict_fail_acl_check(acltype):
    _acls_from_dict_fail({'acls': {acltype: {}}},
                         IllegalParameterError(f'{acltype} ACL must be a list'))
    _acls_from_dict_fail(
        {'acls': {acltype: ['one', 2, 'three']}},
        IllegalParameterError(f'Index 1 of {acltype} ACL does not contain a string'))


def _acls_from_dict_fail(d, expected):
    with raises(Exception) as got:
        acls_from_dict(d)
    assert_exception_correct(got.value, expected)


def test_acl_delta_from_dict():
    assert acl_delta_from_dict({}) == SampleACLDelta()
    assert acl_delta_from_dict({'public_read': 0, 'at_least': 0}) == SampleACLDelta()
    assert acl_delta_from_dict({'public_read': 1, 'at_least': None}) == SampleACLDelta(
        public_read=True)
    assert acl_delta_from_dict({'public_read': -1, 'at_least': 1}) == SampleACLDelta(
        public_read=False, at_least=True)
    assert acl_delta_from_dict({'public_read': -50, 'at_least': -50}) == SampleACLDelta(
        public_read=False, at_least=True)
    assert acl_delta_from_dict({
        'read': [],
        'admin': ['whee', 'whoo'],
        'public_read': None,
        'at_least': 100}) == SampleACLDelta([UserID('whee'), UserID('whoo')], at_least=True)
    assert acl_delta_from_dict({
        'read': ['a', 'b'],
        'write': ['c'],
        'admin': ['whee', 'whoo'],
        'remove': ['e', 'f'],
        'public_read': 100}) == SampleACLDelta(
            [UserID('whee'), UserID('whoo')], [UserID('c')], [UserID('a'), UserID('b')],
            [UserID('e'), UserID('f')], True)


def test_acl_delta_from_dict_fail_bad_args():
    _acl_delta_from_dict_fail({'public_read': '0'}, IllegalParameterError(
        'public_read must be an integer if present'))
    _acl_delta_from_dict_fail({'admin': {}}, IllegalParameterError(
        'admin ACL must be a list'))
    _acl_delta_from_dict_fail({'admin': ['foo', 1, '32']}, IllegalParameterError(
        'Index 1 of admin ACL does not contain a string'))
    _acl_delta_from_dict_fail({'write': 'foo'}, IllegalParameterError(
        'write ACL must be a list'))
    _acl_delta_from_dict_fail({'write': [[], 1, '32']}, IllegalParameterError(
        'Index 0 of write ACL does not contain a string'))
    _acl_delta_from_dict_fail({'read': 64.2}, IllegalParameterError(
        'read ACL must be a list'))
    _acl_delta_from_dict_fail({'read': ['f', 'z', {}]}, IllegalParameterError(
        'Index 2 of read ACL does not contain a string'))
    _acl_delta_from_dict_fail({'remove': (1,)}, IllegalParameterError(
        'remove ACL must be a list'))
    _acl_delta_from_dict_fail({'remove': ['f', id, {}]}, IllegalParameterError(
        'Index 1 of remove ACL does not contain a string'))


def _acl_delta_from_dict_fail(d, expected):
    with raises(Exception) as got:
        acl_delta_from_dict(d)
    assert_exception_correct(got.value, expected)


def test_check_admin():
    f = AdminPermission.FULL
    r = AdminPermission.READ
    _check_admin(f, f, 'user1', 'somemethod', None,
                 'User user1 is running method somemethod with administration permission FULL')
    _check_admin(f, f, 'user1', 'somemethod', UserID('otheruser'),
                 'User user1 is running method somemethod with administration permission FULL ' +
                 'as user otheruser')
    _check_admin(f, r, 'someuser', 'a_method', None,
                 'User someuser is running method a_method with administration permission FULL')
    _check_admin(r, r, 'user2', 'm', None,
                 'User user2 is running method m with administration permission READ')


def _check_admin(perm, permreq, user, method, as_user, expected_log):
    ul = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    logs = []

    ul.is_admin.return_value = (perm, user)
    ul.invalid_users.return_value = []

    assert check_admin(
        ul, 'thisisatoken', permreq, method, lambda x: logs.append(x), as_user) is True

    assert ul.is_admin.call_args_list == [(('thisisatoken',), {})]
    if as_user:
        ul.invalid_users.assert_called_once_with([as_user])
    assert logs == [expected_log]


def test_check_admin_skip():
    ul = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    logs = []

    ul.is_admin.return_value = (AdminPermission.FULL, 'u')

    assert check_admin(ul, 'thisisatoken', AdminPermission.FULL, 'm', lambda x: logs.append(x),
                       skip_check=True) is False

    assert ul.is_admin.call_args_list == []
    assert logs == []


def test_check_admin_fail_bad_args():
    ul = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    p = AdminPermission.FULL

    _check_admin_fail(None, 't', p, 'm', lambda _: None, None, ValueError(
        'user_lookup cannot be a value that evaluates to false'))
    _check_admin_fail(ul, '', p, 'm', lambda _: None, None, UnauthorizedError(
        'Anonymous users may not act as service administrators.'))
    _check_admin_fail(ul, 't', None, 'm', lambda _: None, None, ValueError(
        'perm cannot be a value that evaluates to false'))
    _check_admin_fail(ul, 't', p, None, lambda _: None, None, ValueError(
        'method cannot be a value that evaluates to false'))
    _check_admin_fail(ul, 't', p, 'm', None, None, ValueError(
        'log_fn cannot be a value that evaluates to false'))


def test_check_admin_fail_none_perm():
    ul = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    _check_admin_fail(ul, 't', AdminPermission.NONE, 'm', lambda _: None, None, ValueError(
        'what are you doing calling this method with no permission requirement? ' +
        'That totally makes no sense. Get a brain moran'))


def test_check_admin_fail_read_with_impersonate():
    ul = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    _check_admin_fail(ul, 't', AdminPermission.READ, 'm', lambda _: None, 'user', ValueError(
        'as_user is supplied, but permission is not FULL'))


def test_check_admin_fail_no_admin_perms():
    f = AdminPermission.FULL
    r = AdminPermission.READ
    n = AdminPermission.NONE
    _check_admin_fail_no_admin_perms(r, f)
    _check_admin_fail_no_admin_perms(n, f)
    _check_admin_fail_no_admin_perms(n, r)


def _check_admin_fail_no_admin_perms(permhas, permreq):
    ul = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    log = []

    ul.is_admin.return_value = (permhas, 'user1')
    err = 'User user1 does not have the necessary administration privileges to run method m'
    _check_admin_fail(ul, 't', permreq, 'm', lambda l: log.append(l), None, UnauthorizedError(err))

    assert log == [err]


def test_check_admin_fail_no_such_user():
    ul = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    log = []

    ul.is_admin.return_value = (AdminPermission.FULL, 'user1')
    ul.invalid_users.return_value = [UserID('bruh')]

    _check_admin_fail(
        ul, 'token', AdminPermission.FULL, 'a method', lambda x: log.append(x), UserID('bruh'),
        NoSuchUserError('bruh'))

    ul.is_admin.assert_called_once_with('token')
    ul.invalid_users.assert_called_once_with([UserID('bruh')])
    assert log == []


def _check_admin_fail(ul, token, perm, method, logfn, as_user, expected):
    with raises(Exception) as got:
        check_admin(ul, token, perm, method, logfn, as_user)
    assert_exception_correct(got.value, expected)


def test_get_static_key_metadata_params():
    assert get_static_key_metadata_params({'keys': []}) == ([], False)
    assert get_static_key_metadata_params({'keys': ['foo'], 'prefix': None}) == (['foo'], False)
    assert get_static_key_metadata_params({'keys': ['bar', 'foo'], 'prefix': 0}) == (
        ['bar', 'foo'], False)
    assert get_static_key_metadata_params({'keys': ['bar'], 'prefix': False}) == (['bar'], False)
    assert get_static_key_metadata_params({'keys': ['bar'], 'prefix': 1}) == (['bar'], None)
    assert get_static_key_metadata_params({'keys': ['bar'], 'prefix': 2}) == (['bar'], True)


def test_get_static_key_metadata_params_fail_bad_args():
    _get_static_key_metadata_params_fail(None, ValueError('params cannot be None'))
    _get_static_key_metadata_params_fail({}, IllegalParameterError('keys must be a list'))
    _get_static_key_metadata_params_fail({'keys': 0}, IllegalParameterError('keys must be a list'))
    _get_static_key_metadata_params_fail({'keys': ['foo', 0]}, IllegalParameterError(
        'index 1 of keys is not a string'))
    _get_static_key_metadata_params_fail({'keys': [], 'prefix': -1}, IllegalParameterError(
        'Unexpected value for prefix: -1'))
    _get_static_key_metadata_params_fail({'keys': [], 'prefix': 3}, IllegalParameterError(
        'Unexpected value for prefix: 3'))
    _get_static_key_metadata_params_fail({'keys': [], 'prefix': 'foo'}, IllegalParameterError(
        'Unexpected value for prefix: foo'))


def _get_static_key_metadata_params_fail(params, expected):
    with raises(Exception) as got:
        get_static_key_metadata_params(params)
    assert_exception_correct(got.value, expected)


def test_create_data_link_params_missing_update_key():
    params = {
        'id': '706fe9e1-70ef-4feb-bbd9-32295104a119',
        'version': 78,
        'node': 'mynode',
        'upa': '6/7/29',
        'dataid': 'mydata'
    }

    assert create_data_link_params(params) == (
        DataUnitID(UPA('6/7/29'), 'mydata'),
        SampleNodeAddress(
            SampleAddress(UUID('706fe9e1-70ef-4feb-bbd9-32295104a119'), 78), 'mynode'),
        False
    )


def test_create_data_link_params_with_update():
    _create_data_link_params_with_update(None, False)
    _create_data_link_params_with_update(False, False)  # doesn't work with SDK
    _create_data_link_params_with_update(0, False)
    _create_data_link_params_with_update([], False)  # illegal value theoretically
    _create_data_link_params_with_update('', False)  # illegal value theoretically
    _create_data_link_params_with_update(1, True)
    # rest of these are theoretically illegal values
    _create_data_link_params_with_update(100, True)
    _create_data_link_params_with_update(-1, True)
    _create_data_link_params_with_update('m', True)
    _create_data_link_params_with_update({'a': 'b'}, True)


def _create_data_link_params_with_update(update, expected):
    params = {
        'id': '706fe9e1-70ef-4feb-bbd9-32295104a119',
        'version': 1,
        'node': 'm',
        'upa': '1/1/1',
        'update': update
    }

    assert create_data_link_params(params) == (
        DataUnitID(UPA('1/1/1')),
        SampleNodeAddress(
            SampleAddress(UUID('706fe9e1-70ef-4feb-bbd9-32295104a119'), 1), 'm'),
        expected
    )


def test_create_data_link_params_fail_bad_args():
    id_ = '706fe9e1-70ef-4feb-bbd9-32295104a119'
    _create_data_link_params_fail(None, ValueError('params cannot be None'))
    _create_data_link_params_fail({}, MissingParameterError('id'))
    _create_data_link_params_fail({'id': 6}, IllegalParameterError(
        'id 6 must be a UUID string'))
    _create_data_link_params_fail({'id': id_[:-1]}, IllegalParameterError(
        'id 706fe9e1-70ef-4feb-bbd9-32295104a11 must be a UUID string'))
    _create_data_link_params_fail({'id': id_}, MissingParameterError('version'))
    _create_data_link_params_fail(
        {'id': id_, 'version': 'ver'},
        IllegalParameterError('Illegal version argument: ver'))
    _create_data_link_params_fail(
        {'id': id_, 'version': -1},
        IllegalParameterError('Illegal version argument: -1'))
    _create_data_link_params_fail(
        {'id': id_, 'version': 1},
        MissingParameterError('node'))
    _create_data_link_params_fail(
        {'id': id_, 'version': 1, 'node': {'a': 'b'}},
        IllegalParameterError('node key is not a string as required'))
    _create_data_link_params_fail(
        {'id': id_, 'version': 1, 'node': 'foo\tbar'},
        IllegalParameterError('node contains control characters'))
    _create_data_link_params_fail(
        {'id': id_, 'version': 1, 'node': 'm'},
        MissingParameterError('upa'))
    _create_data_link_params_fail(
        {'id': id_, 'version': 1, 'node': 'm', 'upa': 3.4},
        IllegalParameterError('upa key is not a string as required'))
    _create_data_link_params_fail(
        {'id': id_, 'version': 1, 'node': 'm', 'upa': '1/0/1'},
        IllegalParameterError('1/0/1 is not a valid UPA'))
    _create_data_link_params_fail(
        {'id': id_, 'version': 1, 'node': 'm', 'upa': '1/1/1', 'dataid': 6},
        IllegalParameterError('dataid key is not a string as required'))
    _create_data_link_params_fail(
        {'id': id_, 'version': 1, 'node': 'm', 'upa': '1/1/1', 'dataid': 'yay\nyo'},
        IllegalParameterError('dataid contains control characters'))


def _create_data_link_params_fail(params, expected):
    with raises(Exception) as got:
        create_data_link_params(params)
    assert_exception_correct(got.value, expected)


def test_get_data_unit_id_from_object():
    assert get_data_unit_id_from_object({'upa': '1/1/1'}) == DataUnitID(UPA('1/1/1'))
    assert get_data_unit_id_from_object({'upa': '8/3/2'}) == DataUnitID(UPA('8/3/2'))
    assert get_data_unit_id_from_object(
        {'upa': '8/3/2', 'dataid': 'a'}) == DataUnitID(UPA('8/3/2'), 'a')


def test_get_data_unit_id_from_object_fail_bad_args():
    _get_data_unit_id_from_object_fail(None, ValueError('params cannot be None'))
    _get_data_unit_id_from_object_fail({}, MissingParameterError('upa'))
    _get_data_unit_id_from_object_fail({'upa': '1/0/1'}, IllegalParameterError(
        '1/0/1 is not a valid UPA'))
    _get_data_unit_id_from_object_fail({'upa': 82}, IllegalParameterError(
        'upa key is not a string as required'))
    _get_data_unit_id_from_object_fail({'upa': '1/1/1', 'dataid': []}, IllegalParameterError(
        'dataid key is not a string as required'))
    _get_data_unit_id_from_object_fail({'upa': '1/1/1', 'dataid': 'f\t/b'}, IllegalParameterError(
        'dataid contains control characters'))


def _get_data_unit_id_from_object_fail(params, expected):
    with raises(Exception) as got:
        get_data_unit_id_from_object(params)
    assert_exception_correct(got.value, expected)


def test_get_upa_from_object():
    assert get_upa_from_object({'upa': '1/1/1'}) == UPA('1/1/1')
    assert get_upa_from_object({'upa': '8/3/2'}) == UPA('8/3/2')


def test_get_upa_from_object_fail_bad_args():
    _get_upa_from_object_fail(None, ValueError('params cannot be None'))
    _get_upa_from_object_fail({}, MissingParameterError('upa'))
    _get_upa_from_object_fail({'upa': '1/0/1'}, IllegalParameterError('1/0/1 is not a valid UPA'))
    _get_upa_from_object_fail({'upa': 82}, IllegalParameterError(
        'upa key is not a string as required'))


def _get_upa_from_object_fail(params, expected):
    with raises(Exception) as got:
        get_upa_from_object(params)
    assert_exception_correct(got.value, expected)


def test_get_datetime_from_epochmilliseconds_in_object():
    gt = get_datetime_from_epochmilliseconds_in_object
    assert gt({}, 'foo') is None
    assert gt({'bar': 1}, 'foo') is None
    assert gt({'foo': 0}, 'foo') == dt(0)
    assert gt({'foo': 1}, 'foo') == dt(0.001)
    assert gt({'foo': -1}, 'foo') == dt(-0.001)
    assert gt({'bar': 1234877807185}, 'bar') == dt(1234877807.185)
    assert gt({'bar': -1234877807185}, 'bar') == dt(-1234877807.185)
    # should really test overflow but that's system dependent, no reliable test


def test_get_datetime_from_epochmilliseconds_in_object_fail_bad_args():
    gt = _get_datetime_from_epochmilliseconds_in_object_fail

    gt(None, 'bar', ValueError('params cannot be None'))
    gt({'foo': 'a'}, 'foo', IllegalParameterError(
        "key 'foo' value of 'a' is not a valid epoch millisecond timestamp"))
    gt({'ts': 1.2}, 'ts', IllegalParameterError(
        "key 'ts' value of '1.2' is not a valid epoch millisecond timestamp"))


def _get_datetime_from_epochmilliseconds_in_object_fail(params, key, expected):
    with raises(Exception) as got:
        get_datetime_from_epochmilliseconds_in_object(params, key)
    assert_exception_correct(got.value, expected)


def test_links_to_dicts():
    links = [
        DataLink(
            UUID('f5bd78c3-823e-40b2-9f93-20e78680e41e'),
            DataUnitID(UPA('1/2/3'), 'foo'),
            SampleNodeAddress(
                SampleAddress(UUID('f5bd78c3-823e-40b2-9f93-20e78680e41f'), 6), 'foo'),
            dt(0.067),
            UserID('usera'),
            dt(89),
            UserID('userb')
        ),
        DataLink(
            UUID('f5bd78c3-823e-40b2-9f93-20e78680e41a'),
            DataUnitID(UPA('4/9/10')),
            SampleNodeAddress(
                SampleAddress(UUID('f5bd78c3-823e-40b2-9f93-20e78680e41b'), 4), 'bar'),
            dt(1),
            UserID('userc'),
        ),
    ]
    assert links_to_dicts(links) == [
        {
            'linkid': 'f5bd78c3-823e-40b2-9f93-20e78680e41e',
            'upa': '1/2/3',
            'dataid': 'foo',
            'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41f',
            'version': 6,
            'node': 'foo',
            'created': 67,
            'createdby': 'usera',
            'expired': 89000,
            'expiredby': 'userb'
            },
        {
            'linkid': 'f5bd78c3-823e-40b2-9f93-20e78680e41a',
            'upa': '4/9/10',
            'dataid': None,
            'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41b',
            'version': 4,
            'node': 'bar',
            'created': 1000,
            'createdby': 'userc',
            'expired': None,
            'expiredby': None
            }
    ]


def test_links_to_dicts_fail_bad_args():
    dl = DataLink(
            UUID('f5bd78c3-823e-40b2-9f93-20e78680e41e'),
            DataUnitID(UPA('1/2/3'), 'foo'),
            SampleNodeAddress(
                SampleAddress(UUID('f5bd78c3-823e-40b2-9f93-20e78680e41f'), 6), 'foo'),
            dt(0.067),
            UserID('usera'),
            dt(89),
            UserID('userb')
        )

    _links_to_dicts_fail(None, ValueError('links cannot be None'))
    _links_to_dicts_fail([dl, None], ValueError(
        'Index 1 of iterable links cannot be a value that evaluates to false'))


def _links_to_dicts_fail(links, expected):
    with raises(Exception) as got:
        links_to_dicts(links)
    assert_exception_correct(got.value, expected)
