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
from SampleService.core.user import UserID
from SampleService.core.user_lookup import KBaseUserLookup
from SampleService.core.validator.metadata_validator import MetadataValidatorSet
from SampleService.core import user_lookup
from core.test_utils import assert_exception_correct


def u(user):
    return UserID(user)


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
    _save_sample_version_per_user(UserID('someuser'), None, None)
    _save_sample_version_per_user(UserID('otheruser'), 'sample name', 2)
    # this one should really fail based on the mock output... but it's a mock so it won't
    _save_sample_version_per_user(UserID('anotheruser'), 'ur dad yeah', 1)


def _save_sample_version_per_user(user: UserID, name, prior_version):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.')])

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
    u = UserID('u')

    _save_sample_fail(
        samples, None, u, id_, 1, ValueError('sample cannot be a value that evaluates to false'))
    _save_sample_fail(
        samples, s, None, id_, 1, ValueError('user cannot be a value that evaluates to false'))
    _save_sample_fail(
        samples, s, u, id_, 0, IllegalParameterError('Prior version must be > 0'))


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
            UserID('auser'))
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
            UserID('auser'))
    assert_exception_correct(got.value, MetadataValidationError(
        'Node at index 1: key2: u suk lol'))


def test_save_sample_fail_unauthorized():
    _save_sample_fail_unauthorized(UserID('x'))
    _save_sample_fail_unauthorized(UserID('nouserhere'))


def _save_sample_fail_unauthorized(user: UserID):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

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
    _get_sample(UserID('someuser'), None, False)
    _get_sample(UserID('otheruser'), None, False)
    _get_sample(UserID('anotheruser'), None, False)
    _get_sample(UserID('x'), None, False)
    _get_sample(UserID('notinacl'), None, True)


def _get_sample(user, version, as_admin):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    storage.get_sample.return_value = SavedSample(
        UUID('1234567890abcdef1234567890abcdea'),
        UserID('anotheruser'),
        [SampleNode('foo')],
        datetime.datetime.fromtimestamp(42, tz=datetime.timezone.utc),
        'bar',
        4)

    assert samples.get_sample(
        UUID('1234567890abcdef1234567890abcdea'), user, version, as_admin) == SavedSample(
            UUID('1234567890abcdef1234567890abcdea'),
            UserID('anotheruser'),
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

    _get_sample_fail(samples, None, UserID('foo'), 1, ValueError(
        'id_ cannot be a value that evaluates to false'))
    _get_sample_fail(samples, id_, None, 1, ValueError(
        'user cannot be a value that evaluates to false'))
    _get_sample_fail(samples, id_, UserID('a'), 0, IllegalParameterError('Version must be > 0'))


def test_get_sample_fail_unauthorized():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    _get_sample_fail(
        samples, UUID('1234567890abcdef1234567890abcdef'), UserID('y'), 3,
        UnauthorizedError('User y cannot read sample 12345678-90ab-cdef-1234-567890abcdef'))

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcdef'),), {})]


def _get_sample_fail(samples, id_, user, version, expected):
    with raises(Exception) as got:
        samples.get_sample(id_, user, version)
    assert_exception_correct(got.value, expected)


def test_get_sample_acls():
    _get_sample_acls(UserID('someuser'), False)
    _get_sample_acls(UserID('otheruser'), False)
    _get_sample_acls(UserID('anotheruser'), False)
    _get_sample_acls(UserID('x'), False)
    _get_sample_acls(UserID('no_rights_here'), True)


def _get_sample_acls(user, as_admin):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    assert samples.get_sample_acls(id_, user, as_admin) == SampleACL(
        u('someuser'),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {})]


def test_get_sample_acls_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcdef')

    _get_sample_acls_fail(samples, None, UserID('foo'), ValueError(
        'id_ cannot be a value that evaluates to false'))
    _get_sample_acls_fail(samples, id_, None, ValueError(
        'user cannot be a value that evaluates to false'))


def test_get_sample_acls_fail_unauthorized():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    _get_sample_acls_fail(
        samples, UUID('1234567890abcdef1234567890abcdea'), UserID('y'),
        UnauthorizedError('User y cannot read sample 12345678-90ab-cdef-1234-567890abcdea'))

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcdea'),), {})]


def _get_sample_acls_fail(samples, id_, user, expected):
    with raises(Exception) as got:
        samples.get_sample_acls(id_, user)
    assert_exception_correct(got.value, expected)


def test_replace_sample_acls():
    _replace_sample_acls(UserID('someuser'), False)
    _replace_sample_acls(UserID('otheruser'), False)
    _replace_sample_acls(UserID('super_admin_man'), True)


def _replace_sample_acls(user: UserID, as_admin):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = []

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        [u('otheruser'), u('y')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    samples.replace_sample_acls(id_, user, SampleACL(
        u('someuser'), [u('x'), u('y')], [u('z'), u('a')], [u('b'), u('c')]),
        as_admin=as_admin)

    assert lu.are_valid_users.call_args_list == [
        (([u(x) for x in ['x', 'y', 'z', 'a', 'b', 'c']],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {})]

    assert storage.replace_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),
          SampleACL(u('someuser'), [u('x'), u('y')], [u('z'), u('a')], [u('b'), u('c')])), {})]


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
            u('someuser'),
            [u('otheruser')],
            [u('anotheruser'), u('ur mum')],
            [u('Fungus J. Pustule Jr.'), u('x')]),
        SampleACL(
            u('someuser2'), [u('otheruser'), u('y')],)
        ]

    storage.replace_sample_acls.side_effect = [OwnerChangedError, None]

    samples.replace_sample_acls(id_, UserID('otheruser'), SampleACL(u('someuser'), [u('a')]))

    assert lu.are_valid_users.call_args_list == [(([u('a')],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {}),
        ((UUID('1234567890abcdef1234567890abcde0'),), {})
        ]

    assert storage.replace_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'), SampleACL(u('someuser'), [u('a')])), {}),
        ((UUID('1234567890abcdef1234567890abcde0'), SampleACL(u('someuser2'), [u('a')])), {})
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
            u('someuser'),
            [u('otheruser')],
            [u('anotheruser'), u('ur mum')],
            [u('Fungus J. Pustule Jr.'), u('x')]),
        SampleACL(
            u('someuser2'), [u('otheruser2'), u('y')],)
        ]

    storage.replace_sample_acls.side_effect = [OwnerChangedError, None]

    _replace_sample_acls_fail(
        samples, id_, UserID('otheruser'), SampleACL(u('someuser'), write=[u('b')]),
        UnauthorizedError(f'User otheruser cannot administrate sample {id_}'))

    assert lu.are_valid_users.call_args_list == [(([u('b')],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {}),
        ((UUID('1234567890abcdef1234567890abcde0'),), {})
        ]

    assert storage.replace_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'), SampleACL(u('someuser'), write=[u('b')])), {})
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
        SampleACL(u(f'someuser{x}'), [u('otheruser')]) for x in range(5)
    ]

    storage.replace_sample_acls.side_effect = OwnerChangedError

    _replace_sample_acls_fail(
        samples, id_, UserID('otheruser'), SampleACL(u('someuser'), read=[u('c')]),
        ValueError(f'Failed setting ACLs after 5 attempts for sample {id_}'))

    assert lu.are_valid_users.call_args_list == [(([u('c')],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {}) for _ in range(5)
        ]

    assert storage.replace_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),
            SampleACL(u(f'someuser{x}'), read=[u('c')]),), {}) for x in range(5)
        ]


def test_replace_sample_acls_fail_bad_input():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')
    u = UserID('u')

    _replace_sample_acls_fail(samples, None, u, SampleACL(UserID('foo')), ValueError(
        'id_ cannot be a value that evaluates to false'))
    _replace_sample_acls_fail(samples, id_, None, SampleACL(UserID('foo')), ValueError(
        'user cannot be a value that evaluates to false'))
    _replace_sample_acls_fail(samples, id_, u, None, ValueError(
        'new_acls cannot be a value that evaluates to false'))


def test_replace_sample_acls_fail_nonexistent_user_4_users():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = [u('whoo'), u('yay'), u('bugga'), u('w')]

    acls = SampleACL(
        u('foo'),
        [u('x'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z'), u('w')])

    _replace_sample_acls_fail(
        samples, id_, UserID('foo'), acls, NoSuchUserError('whoo, yay, bugga, w'))

    assert lu.are_valid_users.call_args_list == [
        (([u('x'), u('whoo'), u('yay'), u('fwew'), u('y'), u('bugga'), u('z'), u('w')],), {})]


def test_replace_sample_acls_fail_nonexistent_user_5_users():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = [u('whoo'), u('yay'), u('bugga'), u('w'), u('c')]

    acls = SampleACL(
        u('foo'),
        [u('x'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z'), u('w'), u('c')])

    _replace_sample_acls_fail(
        samples, id_, UserID('foo'), acls, NoSuchUserError('whoo, yay, bugga, w, c'))

    assert lu.are_valid_users.call_args_list == [
        (([u('x'), u('whoo'), u('yay'), u('fwew'), u('y'), u('bugga'), u('z'), u('w'),
           u('c')],), {})]


def test_replace_sample_acls_fail_nonexistent_user_6_users():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = [u('whoo'), u('yay'), u('bugga'), u('w'), u('c'), u('whee')]

    acls = SampleACL(
        u('foo'),
        [u('x'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z'), u('w'), u('c'), u('whee')])

    _replace_sample_acls_fail(
        samples, id_, UserID('foo'), acls, NoSuchUserError('whoo, yay, bugga, w, c'))

    assert lu.are_valid_users.call_args_list == [
        (([u('x'), u('whoo'), u('yay'), u('fwew'), u('y'), u('bugga'), u('z'), u('w'), u('c'),
           u('whee')],), {})]


def test_replace_sample_acls_fail_invalid_user():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.side_effect = user_lookup.InvalidUserError('o shit waddup')

    acls = SampleACL(
        u('foo'),
        [u('o shit waddup'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z')])

    _replace_sample_acls_fail(samples, id_, UserID('foo'), acls, NoSuchUserError('o shit waddup'))

    assert lu.are_valid_users.call_args_list == [
        (([u('o shit waddup'), u('whoo'), u('yay'), u('fwew'), u('y'), u('bugga'), u('z')],), {})]


def test_replace_sample_acls_fail_invalid_token():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.side_effect = user_lookup.InvalidTokenError('you big dummy')

    acls = SampleACL(
        u('foo'),
        [u('x'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z')])

    _replace_sample_acls_fail(samples, id_, UserID('foo'), acls, ValueError(
        'user lookup token for KBase auth server is invalid, cannot continue'))

    assert lu.are_valid_users.call_args_list == [
        (([u('x'), u('whoo'), u('yay'), u('fwew'), u('y'), u('bugga'), u('z')],), {})]


def test_replace_sample_acls_fail_unauthorized():
    _replace_sample_acls_fail_unauthorized(UserID('anotheruser'))
    _replace_sample_acls_fail_unauthorized(UserID('x'))
    _replace_sample_acls_fail_unauthorized(UserID('MrsEntity'))


def _replace_sample_acls_fail_unauthorized(user: UserID):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.are_valid_users.return_value = []

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    _replace_sample_acls_fail(samples, id_, user, SampleACL(u('foo')), UnauthorizedError(
        f'User {user} cannot administrate sample 12345678-90ab-cdef-1234-567890abcde0'))

    assert lu.are_valid_users.call_args_list == [(([],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {})]


def _replace_sample_acls_fail(samples, id_, user, acls, expected):
    with raises(Exception) as got:
        samples.replace_sample_acls(id_, user, acls)
    assert_exception_correct(got.value, expected)


def test_get_key_metadata():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    meta.key_metadata.side_effect = [
        {'a': {'c': 'd'}, 'b': {'e': 3}},
        {'a': {'c': 'e'}, 'b': {'e': 4}},
        ]

    assert s.get_key_static_metadata(['a', 'b']) == {'a': {'c': 'd'}, 'b': {'e': 3}}

    meta.key_metadata.assert_called_once_with(['a', 'b'])

    assert s.get_key_static_metadata(['a', 'b'], prefix=False) == {'a': {'c': 'e'}, 'b': {'e': 4}}

    meta.key_metadata.assert_called_with(['a', 'b'])

    assert meta.key_metadata.call_count == 2


def test_get_prefix_key_metadata():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    meta.prefix_key_metadata.side_effect = [
        {'a': {'c': 'd'}, 'b': {'e': 3}},
        {'a': {'c': 'f'}, 'b': {'e': 5}}
        ]

    assert s.get_key_static_metadata(['a', 'b'], prefix=None) == {
        'a': {'c': 'd'}, 'b': {'e': 3}}

    meta.prefix_key_metadata.assert_called_once_with(['a', 'b'], exact_match=True)

    assert s.get_key_static_metadata(['a', 'b'], prefix=True) == {
        'a': {'c': 'f'}, 'b': {'e': 5}}

    meta.prefix_key_metadata.assert_called_with(['a', 'b'], exact_match=False)

    assert meta.prefix_key_metadata.call_count == 2


def test_get_prefix_key_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    with raises(Exception) as got:
        s.get_key_static_metadata(None)
    assert_exception_correct(got.value, ValueError('keys cannot be None'))
