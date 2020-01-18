import datetime

from pytest import raises
from uuid import UUID
import json

from SampleService.core.api_arguments import datetime_to_epochmilliseconds, get_id_from_object
from SampleService.core.api_arguments import get_version_from_object, sample_to_dict
from SampleService.core.api_arguments import acls_to_dict, acls_from_dict
from SampleService.core.api_arguments import create_sample_params, get_sample_address_from_object
from SampleService.core.sample import Sample, SampleNode, SubSampleType, SavedSample
from SampleService.core.acls import SampleACL, SampleACLOwnerless
from SampleService.core.errors import IllegalParameterError, MissingParameterError

from core.test_utils import assert_exception_correct


def test_get_id_from_object():
    assert get_id_from_object(None, False) is None
    assert get_id_from_object({}, False) is None
    assert get_id_from_object({'id': None}, False) is None
    assert get_id_from_object({'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41e'}, False) == UUID(
        'f5bd78c3-823e-40b2-9f93-20e78680e41e')
    assert get_id_from_object({'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41e'}, True) == UUID(
        'f5bd78c3-823e-40b2-9f93-20e78680e41e')


def test_get_id_from_object_fail_bad_args():
    get_id_from_object_fail({'id': 6}, True, IllegalParameterError(
        'Sample ID 6 must be a UUID string'))
    get_id_from_object_fail({
        'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41'},
        False,
        IllegalParameterError(
            'Sample ID f5bd78c3-823e-40b2-9f93-20e78680e41 must be a UUID string'))
    get_id_from_object_fail(None, True, MissingParameterError('Sample ID'))
    get_id_from_object_fail({}, True, MissingParameterError('Sample ID'))
    get_id_from_object_fail({'id': None}, True, MissingParameterError('Sample ID'))


def get_id_from_object_fail(d, required, expected):
    with raises(Exception) as got:
        get_id_from_object(d, required)
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
                                                 }
                                             },
                                        'meta_user': {'location_name': {'name': 'my_buttocks'}}
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
                     }
                 },
                {'location_name': {'name': 'my_buttocks'}}
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
        IllegalParameterError("Error for node at index 0: Controlled metadata value key b\nar " +
                              "under key foo's character at index 1 is a control character."))

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
            'Sample ID f5bd78c3-823e-40b2-9f93-20e78680e41 must be a UUID string'))

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


def create_sample_params_fail(params, expected):
    with raises(Exception) as got:
        create_sample_params(params)
    assert_exception_correct(got.value, expected)


def test_get_version_from_object():
    assert get_version_from_object({}) is None
    assert get_version_from_object({'version': None}) is None
    assert get_version_from_object({'version': 3}) == 3
    assert get_version_from_object({'version': 1}) == 1


def test_get_version_from_object_fail_bad_args():
    get_version_from_object_fail(None, ValueError('params cannot be None'))
    get_version_from_object_fail(
        {'version': 'whee'}, IllegalParameterError('Illegal version argument: whee'))
    get_version_from_object_fail(
        {'version': 0}, IllegalParameterError('Illegal version argument: 0'))
    get_version_from_object_fail(
        {'version': -3}, IllegalParameterError('Illegal version argument: -3'))


def get_version_from_object_fail(params, expected):
    with raises(Exception) as got:
        get_version_from_object(params)
    assert_exception_correct(got.value, expected)


def test_get_sample_address_from_object():
    assert get_sample_address_from_object({'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41e'}) == (
        UUID('f5bd78c3-823e-40b2-9f93-20e78680e41e'), None)
    assert get_sample_address_from_object({
        'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41e',
        'version': 1}) == (
        UUID('f5bd78c3-823e-40b2-9f93-20e78680e41e'), 1)


def test_get_sample_address_from_object_fail_bad_args():
    get_sample_address_from_object_fail(None, MissingParameterError('Sample ID'))
    get_sample_address_from_object_fail({}, MissingParameterError('Sample ID'))
    get_sample_address_from_object_fail({'id': None}, MissingParameterError('Sample ID'))
    get_sample_address_from_object_fail({'id': 6}, IllegalParameterError(
        'Sample ID 6 must be a UUID string'))
    get_sample_address_from_object_fail({
        'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41'},
        IllegalParameterError(
            'Sample ID f5bd78c3-823e-40b2-9f93-20e78680e41 must be a UUID string'))

    id_ = 'f5bd78c3-823e-40b2-9f93-20e78680e41e'
    get_version_from_object_fail(
        {'id': id_, 'version': 'whee'}, IllegalParameterError('Illegal version argument: whee'))
    get_version_from_object_fail(
        {'id': id_, 'version': 0}, IllegalParameterError('Illegal version argument: 0'))
    get_version_from_object_fail(
        {'id': id_, 'version': -3}, IllegalParameterError('Illegal version argument: -3'))


def get_sample_address_from_object_fail(params, expected):
    with raises(Exception) as got:
        get_sample_address_from_object(params)
    assert_exception_correct(got.value, expected)


def test_sample_to_dict_minimal():

    expected = {'node_tree': [{'id': 'foo',
                               'type': 'BioReplicate',
                               'meta_controlled': {},
                               'meta_user': {},
                               'parent': None
                               }],
                'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41e',
                'user': 'user2',
                'save_date': 87897,
                'name': None,
                'version': None,
                }

    id_ = UUID('f5bd78c3-823e-40b2-9f93-20e78680e41e')

    s = sample_to_dict(SavedSample(id_, 'user2', [SampleNode('foo')], dt(87.8971)))

    assert s == expected

    # ensure that the result is jsonable. The data structure includes frozen maps which are not
    json.dumps(s)


def test_sample_to_dict_maximal():
    expected = {'node_tree': [{'id': 'foo',
                               'type': 'BioReplicate',
                               'meta_controlled': {},
                               'meta_user': {},
                               'parent': None
                               },
                              {'id': 'bar',
                               'type': 'TechReplicate',
                               'meta_controlled': {'a': {'b': 'c', 'm': 6.7}},
                               'meta_user': {'d': {'e': True}, 'g': {'h': 1}},
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
            'user3',
            [SampleNode('foo'),
             SampleNode(
                 'bar',
                 SubSampleType.TECHNICAL_REPLICATE,
                 'foo',
                 {'a': {'b': 'c', 'm': 6.7}},
                 {'d': {'e': True}, 'g': {'h': 1}})
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
    assert acls_to_dict(SampleACL('user')) == {
        'owner': 'user',
        'admin': (),
        'write': (),
        'read': ()
    }


def test_acls_to_dict_maximal():
    assert acls_to_dict(
        SampleACL(
            'user',
            ['foo', 'bar'],
            ['baz'],
            ['hello', "I'm", 'a', 'robot'])) == {
        'owner': 'user',
        'admin': ('foo', 'bar'),
        'write': ('baz',),
        'read': ('hello', "I'm", 'a', 'robot')
    }


def test_acls_to_dict_fail():
    with raises(Exception) as got:
        acls_to_dict(None)
    assert_exception_correct(
        got.value, ValueError('acls cannot be a value that evaluates to false'))


def test_acls_from_dict():
    assert acls_from_dict({'acls': {}}) == SampleACLOwnerless()
    assert acls_from_dict({'acls': {
        'read': [],
        'admin': ['whee', 'whoo']}}) == SampleACLOwnerless(['whee', 'whoo'])
    assert acls_from_dict({'acls': {
        'read': ['a', 'b'],
        'write': ['x'],
        'admin': ['whee', 'whoo']}}) == SampleACLOwnerless(['whee', 'whoo'], ['x'], ['a', 'b'])


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
