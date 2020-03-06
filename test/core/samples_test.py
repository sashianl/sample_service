import datetime

from pytest import raises
from uuid import UUID
from unittest.mock import create_autospec

from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage
from SampleService.core.acls import SampleACL
from SampleService.core.errors import IllegalParameterError, UnauthorizedError, NoSuchUserError
from SampleService.core.errors import MetadataValidationError
from SampleService.core.sample import Sample, SampleNode, SavedSample
from SampleService.core.samples import Samples
from SampleService.core.storage.errors import OwnerChangedError
from SampleService.core.user_lookup import KBaseUserLookup
from SampleService.core.validator.metadata_validator import MetadataValidatorSet
from SampleService.core import user_lookup
from core.test_utils import assert_exception_correct


def nw():
    return datetime.datetime.fromtimestamp(6, tz=datetime.timezone.utc)


def test_init_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ug = lambda: UUID('1234567890abcdef1234567890abcdef')  # noqa E731

    _init_fail(None, lu, meta, nw, ug, ValueError(
        'storage cannot be a value that evaluates to false'))
    _init_fail(storage, None, meta, nw, ug, ValueError(
        'user_lookup cannot be a value that evaluates to false'))
    _init_fail(storage, lu, None, nw, ug, ValueError(
        'metadata_validator cannot be a value that evaluates to false'))
    _init_fail(storage, lu, meta, None, ug, ValueError(
        'now cannot be a value that evaluates to false'))
    _init_fail(storage, lu, meta, nw, None, ValueError(
        'uuid_gen cannot be a value that evaluates to false'))


def _init_fail(storage, lookup, meta, now, uuid_gen, expected):
    with raises(Exception) as got:
        Samples(storage, lookup, meta, now, uuid_gen)
    assert_exception_correct(got.value, expected)


def test_save_sample():
    _save_sample_with_name(None)
    _save_sample_with_name('bar')


def _save_sample_with_name(name):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    assert s.save_sample(
        Sample([
            SampleNode(
                'foo',
                controlled_metadata={'key1': {'val': 'foo'}, 'key2': {'val': 'bar'}},
                user_metadata={'key_not_in_validator_map': {'val': 'yay'},
                               'key1': {'val': 'wrong'}}
                ),
            SampleNode(
                'foo2',
                controlled_metadata={'key3': {'val': 'foo'}, 'key4': {'val': 'bar'}},
                )
            ],
            name),
        'auser') == (UUID('1234567890abcdef1234567890abcdef'), 1)

    assert storage.save_sample.call_args_list == [
        ((SavedSample(UUID('1234567890abcdef1234567890abcdef'),
                      'auser',
                      [SampleNode(
                          'foo',
                          controlled_metadata={'key1': {'val': 'foo'}, 'key2': {'val': 'bar'}},
                          user_metadata={'key_not_in_validator_map': {'val': 'yay'},
                                         'key1': {'val': 'wrong'}}
                          ),
                       SampleNode(
                           'foo2',
                           controlled_metadata={'key3': {'val': 'foo'}, 'key4': {'val': 'bar'}},
                           )
                       ],
                      datetime.datetime.fromtimestamp(6, tz=datetime.timezone.utc),
                      name
                      ),  # make a tuple
          ), {})]

    assert meta.validate_metadata.call_args_list == [
        (({'key1': {'val': 'foo'}, 'key2': {'val': 'bar'}},), {}),
        (({'key3': {'val': 'foo'}, 'key4': {'val': 'bar'}},), {})
    ]


def test_save_sample_version():
    _save_sample_version_per_user('someuser', None, None)
    _save_sample_version_per_user('otheruser', 'sample name', 2)
    # this one should really fail based on the mock output... but it's a mock so it won't
    _save_sample_version_per_user('anotheruser', 'ur dad yeah', 1)


def _save_sample_version_per_user(user, name, prior_version):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.'])

    storage.save_sample_version.return_value = 3

    assert s.save_sample(
        Sample([SampleNode('foo')], name),
        user,
        UUID('1234567890abcdef1234567890abcdea'),
        prior_version) == (UUID('1234567890abcdef1234567890abcdea'), 3)

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcdea'),), {})]

    assert meta.validate_metadata.call_args_list == [(({},), {})]

    assert storage.save_sample_version.call_args_list == [
        ((SavedSample(UUID('1234567890abcdef1234567890abcdea'),
                      user,
                      [SampleNode('foo')],
                      datetime.datetime.fromtimestamp(6, tz=datetime.timezone.utc),
                      name
                      ),
          prior_version), {})]


def test_save_sample_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    s = Sample([SampleNode('foo')])
    id_ = UUID('1234567890abcdef1234567890abcdef')

    _save_sample_fail(
        samples, None, 'a', id_, 1, ValueError('sample cannot be a value that evaluates to false'))
    _save_sample_fail(
        samples, s, '', id_, 1, ValueError('user cannot be a value that evaluates to false'))
    _save_sample_fail(
        samples, s, 'a', id_, 0, IllegalParameterError('Prior version must be > 0'))


def test_save_sample_fail_no_metadata_validator():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)

    meta.validate_metadata.side_effect = MetadataValidationError('No validator for key3')
    s = Samples(storage, lu, meta, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    with raises(Exception) as got:
        s.save_sample(
            Sample([
                SampleNode(
                    'foo',
                    controlled_metadata={'key1': {'val': 'foo'}, 'key3': {'val': 'bar'}},
                    user_metadata={'key_not_in_validator_map': {'val': 'yay'}}
                    )
                ],
                'foo'),
            'auser')
    assert_exception_correct(got.value, MetadataValidationError(
        'Node at index 0: No validator for key3'))


def test_save_sample_fail_metadata_validator_exception():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    meta.validate_metadata.side_effect = [None, MetadataValidationError('key2: u suk lol')]
    s = Samples(storage, lu, meta, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    with raises(Exception) as got:
        s.save_sample(
            Sample([
                SampleNode(
                    'foo',
                    controlled_metadata={'key1': {'val': 'foo'}, 'key2': {'val': 'bar'}},
                    user_metadata={'key_not_in_validator_map': {'val': 'yay'}}
                    ),
                SampleNode(
                    'foo2',
                    controlled_metadata={'key1': {'val': 'foo'}, 'key2': {'val': 'bar'}},
                    )
                ],
                'foo'),
            'auser')
    assert_exception_correct(got.value, MetadataValidationError(
        'Node at index 1: key2: u suk lol'))


def test_save_sample_fail_unauthorized():
    _save_sample_fail_unauthorized('x')
    _save_sample_fail_unauthorized('nouserhere')


def _save_sample_fail_unauthorized(user):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x'])

    with raises(Exception) as got:
        samples.save_sample(
            Sample([SampleNode('foo')]),
            user,
            UUID('1234567890abcdef1234567890abcdea'))
    assert_exception_correct(got.value, UnauthorizedError(
        f'User {user} cannot write to sample 12345678-90ab-cdef-1234-567890abcdea'))

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcdea'),), {})]


def _save_sample_fail(samples, sample, user, id_, prior_version, expected):
    with raises(Exception) as got:
        samples.save_sample(sample, user, id_, prior_version)
    assert_exception_correct(got.value, expected)


def test_get_sample():
    # sample versions other than 4 don't really make sense but the mock doesn't care
    _get_sample('someuser', None, False)
    _get_sample('otheruser', None, False)
    _get_sample('anotheruser', None, False)
    _get_sample('x', None, False)
    _get_sample('notinacl', None, True)


def _get_sample(user, version, as_admin):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x'])

    storage.get_sample.return_value = SavedSample(
        UUID('1234567890abcdef1234567890abcdea'),
        'anotheruser',
        [SampleNode('foo')],
        datetime.datetime.fromtimestamp(42, tz=datetime.timezone.utc),
        'bar',
        4)

    assert samples.get_sample(
        UUID('1234567890abcdef1234567890abcdea'), user, version, as_admin) == SavedSample(
            UUID('1234567890abcdef1234567890abcdea'),
            'anotheruser',
            [SampleNode('foo')],
            datetime.datetime.fromtimestamp(42, tz=datetime.timezone.utc),
            'bar',
            4)
    if not as_admin:
        assert storage.get_sample_acls.call_args_list == [
            ((UUID('1234567890abcdef1234567890abcdea'),), {})]
    else:
        assert storage.get_sample_acls.call_args_list == []

    assert storage.get_sample.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcdea'), version), {})]


def test_get_sample_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcdef')

    _get_sample_fail(samples, None, 'foo', 1, ValueError(
        'id_ cannot be a value that evaluates to false'))
    _get_sample_fail(samples, id_, '', 1, ValueError(
        'user cannot be a value that evaluates to false'))
    _get_sample_fail(samples, id_, 'a', 0, IllegalParameterError('Version must be > 0'))


def test_get_sample_fail_unauthorized():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x'])

    _get_sample_fail(
        samples, UUID('1234567890abcdef1234567890abcdef'), 'y', 3,
        UnauthorizedError('User y cannot read sample 12345678-90ab-cdef-1234-567890abcdef'))

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcdef'),), {})]


def _get_sample_fail(samples, id_, user, version, expected):
    with raises(Exception) as got:
        samples.get_sample(id_, user, version)
    assert_exception_correct(got.value, expected)


def test_get_sample_acls():
    _get_sample_acls('someuser', False)
    _get_sample_acls('otheruser', False)
    _get_sample_acls('anotheruser', False)
    _get_sample_acls('x', False)
    _get_sample_acls('no_rights_here', True)


def _get_sample_acls(user, as_admin):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    storage.get_sample_acls.return_value = SampleACL(
        'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x'])

    assert samples.get_sample_acls(id_, user, as_admin) == SampleACL(
        'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x'])

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {})]


def test_get_sample_acls_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcdef')

    _get_sample_acls_fail(samples, None, 'foo', ValueError(
        'id_ cannot be a value that evaluates to false'))
    _get_sample_acls_fail(samples, id_, '', ValueError(
        'user cannot be a value that evaluates to false'))


def test_get_sample_acls_fail_unauthorized():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x'])

    _get_sample_acls_fail(
        samples, UUID('1234567890abcdef1234567890abcdea'), 'y',
        UnauthorizedError('User y cannot read sample 12345678-90ab-cdef-1234-567890abcdea'))

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcdea'),), {})]


def _get_sample_acls_fail(samples, id_, user, expected):
    with raises(Exception) as got:
        samples.get_sample_acls(id_, user)
    assert_exception_correct(got.value, expected)


def test_replace_sample_acls():
    _replace_sample_acls('someuser', False)
    _replace_sample_acls('otheruser', False)
    _replace_sample_acls('super_admin_man', True)


def _replace_sample_acls(user, as_admin):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = []

    storage.get_sample_acls.return_value = SampleACL(
        'someuser', ['otheruser', 'y'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x'])

    samples.replace_sample_acls(id_, user, SampleACL(
        'someuser', ['x', 'y'], ['z', 'a'], ['b', 'c']),
        as_admin=as_admin)

    assert lu.are_valid_users.call_args_list == [((['x', 'y', 'z', 'a', 'b', 'c'],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {})]

    assert storage.replace_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),
          SampleACL('someuser', ['x', 'y'], ['z', 'a'], ['b', 'c'])), {})]


def test_replace_sample_acls_with_owner_change():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = []

    storage.get_sample_acls.side_effect = [
        SampleACL(
            'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x']),
        SampleACL(
            'someuser2', ['otheruser', 'y'],)
        ]

    storage.replace_sample_acls.side_effect = [OwnerChangedError, None]

    samples.replace_sample_acls(id_, 'otheruser', SampleACL('someuser', ['a']))

    assert lu.are_valid_users.call_args_list == [((['a'],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {}),
        ((UUID('1234567890abcdef1234567890abcde0'),), {})
        ]

    assert storage.replace_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'), SampleACL('someuser', ['a'])), {}),
        ((UUID('1234567890abcdef1234567890abcde0'), SampleACL('someuser2', ['a'])), {})
        ]


def test_replace_sample_acls_with_owner_change_fail_lost_perms():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = []

    storage.get_sample_acls.side_effect = [
        SampleACL(
            'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x']),
        SampleACL(
            'someuser2', ['otheruser2', 'y'],)
        ]

    storage.replace_sample_acls.side_effect = [OwnerChangedError, None]

    _replace_sample_acls_fail(
        samples, id_, 'otheruser', SampleACL('someuser', write=['b']),
        UnauthorizedError(f'User otheruser cannot administrate sample {id_}'))

    assert lu.are_valid_users.call_args_list == [((['b'],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {}),
        ((UUID('1234567890abcdef1234567890abcde0'),), {})
        ]

    assert storage.replace_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'), SampleACL('someuser', write=['b'])), {})
        ]


def test_replace_sample_acls_with_owner_change_fail_5_times():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = []

    storage.get_sample_acls.side_effect = [
        SampleACL(f'someuser{x}', ['otheruser']) for x in range(5)
    ]

    storage.replace_sample_acls.side_effect = OwnerChangedError

    _replace_sample_acls_fail(
        samples, id_, 'otheruser', SampleACL('someuser', read=['c']),
        ValueError(f'Failed setting ACLs after 5 attempts for sample {id_}'))

    assert lu.are_valid_users.call_args_list == [((['c'],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {}) for _ in range(5)
        ]

    assert storage.replace_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),
            SampleACL(f'someuser{x}', read=['c']),), {}) for x in range(5)
        ]


def test_replace_sample_acls_fail_bad_input():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    _replace_sample_acls_fail(samples, None, 'y', SampleACL('foo'), ValueError(
        'id_ cannot be a value that evaluates to false'))
    _replace_sample_acls_fail(samples, id_, '', SampleACL('foo'), ValueError(
        'user cannot be a value that evaluates to false'))
    _replace_sample_acls_fail(samples, id_, 'y', None, ValueError(
        'new_acls cannot be a value that evaluates to false'))


def test_replace_sample_acls_fail_nonexistent_user_4_users():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = ['whoo', 'yay', 'bugga', 'w']

    acls = SampleACL('foo', ['x', 'whoo'], ['yay', 'fwew'], ['y', 'bugga', 'z', 'w'])

    _replace_sample_acls_fail(samples, id_, 'foo', acls, NoSuchUserError('whoo, yay, bugga, w'))

    assert lu.are_valid_users.call_args_list == [
        ((['x', 'whoo', 'yay', 'fwew', 'y', 'bugga', 'z', 'w'],), {})]


def test_replace_sample_acls_fail_nonexistent_user_5_users():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = ['whoo', 'yay', 'bugga', 'w', 'c']

    acls = SampleACL('foo', ['x', 'whoo'], ['yay', 'fwew'], ['y', 'bugga', 'z', 'w', 'c'])

    _replace_sample_acls_fail(samples, id_, 'foo', acls, NoSuchUserError('whoo, yay, bugga, w, c'))

    assert lu.are_valid_users.call_args_list == [
        ((['x', 'whoo', 'yay', 'fwew', 'y', 'bugga', 'z', 'w', 'c'],), {})]


def test_replace_sample_acls_fail_nonexistent_user_6_users():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = ['whoo', 'yay', 'bugga', 'w', 'c', 'whee']

    acls = SampleACL('foo', ['x', 'whoo'], ['yay', 'fwew'], ['y', 'bugga', 'z', 'w', 'c', 'whee'])

    _replace_sample_acls_fail(samples, id_, 'foo', acls, NoSuchUserError('whoo, yay, bugga, w, c'))

    assert lu.are_valid_users.call_args_list == [
        ((['x', 'whoo', 'yay', 'fwew', 'y', 'bugga', 'z', 'w', 'c', 'whee'],), {})]


def test_replace_sample_acls_fail_invalid_user():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.side_effect = user_lookup.InvalidUserError('o shit waddup')

    acls = SampleACL('foo', ['o shit waddup', 'whoo'], ['yay', 'fwew'], ['y', 'bugga', 'z'])

    _replace_sample_acls_fail(samples, id_, 'foo', acls, NoSuchUserError('o shit waddup'))

    assert lu.are_valid_users.call_args_list == [
        ((['o shit waddup', 'whoo', 'yay', 'fwew', 'y', 'bugga', 'z'],), {})]


def test_replace_sample_acls_fail_invalid_token():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.side_effect = user_lookup.InvalidTokenError('you big dummy')

    acls = SampleACL('foo', ['x', 'whoo'], ['yay', 'fwew'], ['y', 'bugga', 'z'])

    _replace_sample_acls_fail(samples, id_, 'foo', acls, ValueError(
        'user lookup token for KBase auth server is invalid, cannot continue'))

    assert lu.are_valid_users.call_args_list == [
        ((['x', 'whoo', 'yay', 'fwew', 'y', 'bugga', 'z'],), {})]


def test_replace_sample_acls_fail_unauthorized():
    _replace_sample_acls_fail_unauthorized('anotheruser')
    _replace_sample_acls_fail_unauthorized('x')
    _replace_sample_acls_fail_unauthorized('MrsEntity')


def _replace_sample_acls_fail_unauthorized(user):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = []

    storage.get_sample_acls.return_value = SampleACL(
        'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x'])

    _replace_sample_acls_fail(samples, id_, user, SampleACL('foo'), UnauthorizedError(
        f'User {user} cannot administrate sample 12345678-90ab-cdef-1234-567890abcde0'))

    assert lu.are_valid_users.call_args_list == [(([],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {})]


def _replace_sample_acls_fail(samples, id_, user, acls, expected):
    with raises(Exception) as got:
        samples.replace_sample_acls(id_, user, acls)
    assert_exception_correct(got.value, expected)
