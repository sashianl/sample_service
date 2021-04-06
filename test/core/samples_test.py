import datetime
import uuid

from pytest import raises
from uuid import UUID
from unittest.mock import (create_autospec, call)

from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage
from SampleService.core.acls import SampleACL, SampleACLOwnerless, SampleACLDelta
from SampleService.core.data_link import DataLink
from SampleService.core.errors import (
    IllegalParameterError,
    UnauthorizedError,
    NoSuchUserError,
    MetadataValidationError,
    NoSuchLinkError
)
from SampleService.core.notification import KafkaNotifier
from SampleService.core.sample import Sample, SampleNode, SavedSample, SampleAddress
from SampleService.core.sample import SampleNodeAddress
from SampleService.core.samples import Samples
from SampleService.core.storage.errors import OwnerChangedError
from SampleService.core.user import UserID
from SampleService.core.user_lookup import KBaseUserLookup
from SampleService.core.validator.metadata_validator import MetadataValidatorSet
from SampleService.core import user_lookup
from SampleService.core.workspace import WS, UPA, DataUnitID, WorkspaceAccessType
from core.test_utils import assert_exception_correct


def u(user):
    return UserID(user)


def nw():
    return datetime.datetime.fromtimestamp(6, tz=datetime.timezone.utc)


def dt(timestamp):
    return datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)


def test_init_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    ug = lambda: UUID('1234567890abcdef1234567890abcdef')  # noqa E731

    _init_fail(None, lu, meta, ws, nw, ug, ValueError(
        'storage cannot be a value that evaluates to false'))
    _init_fail(storage, None, meta, ws, nw, ug, ValueError(
        'user_lookup cannot be a value that evaluates to false'))
    _init_fail(storage, lu, None, ws, nw, ug, ValueError(
        'metadata_validator cannot be a value that evaluates to false'))
    _init_fail(storage, lu, meta, None, nw, ug, ValueError(
        'workspace cannot be a value that evaluates to false'))
    _init_fail(storage, lu, meta, ws, None, ug, ValueError(
        'now cannot be a value that evaluates to false'))
    _init_fail(storage, lu, meta, ws, nw, None, ValueError(
        'uuid_gen cannot be a value that evaluates to false'))


def _init_fail(storage, lookup, meta, ws, now, uuid_gen, expected):
    with raises(Exception) as got:
        # no errors can occur based on the notifier
        Samples(storage, lookup, meta, ws, None, now, uuid_gen)
    assert_exception_correct(got.value, expected)


def test_validate_sample_with_name():
    _validate_sample(None)
    _validate_sample('foo')


def _validate_sample(name):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, kafka, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    s.validate_sample(Sample([
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
    ], name))


def test_save_sample():
    _save_sample_with_name(None, True)
    _save_sample_with_name('bar', False)


def _save_sample_with_name(name, as_admin):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, kafka, now=nw,
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
        UserID('auser'),
        as_admin=as_admin) == (UUID('1234567890abcdef1234567890abcdef'), 1)

    assert storage.save_sample.call_args_list == [
        ((SavedSample(UUID('1234567890abcdef1234567890abcdef'),
                      UserID('auser'),
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

    call_arg_list = [
        call({'key1': {'val': 'foo'}, 'key2': {'val': 'bar'}}, False),
        call({'key3': {'val': 'foo'}, 'key4': {'val': 'bar'}}, False)
    ]

    meta.validate_metadata.assert_has_calls(call_arg_list)

    kafka.notify_new_sample_version.assert_called_once_with(
        UUID('1234567890abcdef1234567890abcdef'), 1)


def test_save_sample_version():
    _save_sample_version_per_user(UserID('someuser'), None, None)
    _save_sample_version_per_user(UserID('otheruser'), 'sample name', 2)
    # this one should really fail based on the mock output... but it's a mock so it won't
    _save_sample_version_per_user(UserID('anotheruser'), 'ur dad yeah', 1)


def _save_sample_version_per_user(user: UserID, name, prior_version):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, kafka, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
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

    meta.validate_metadata.assert_has_calls([call({}, False)])

    assert storage.save_sample_version.call_args_list == [
        ((SavedSample(UUID('1234567890abcdef1234567890abcdea'),
                      user,
                      [SampleNode('foo')],
                      datetime.datetime.fromtimestamp(6, tz=datetime.timezone.utc),
                      name
                      ),
          prior_version), {})]

    kafka.notify_new_sample_version.assert_called_once_with(
        UUID('1234567890abcdef1234567890abcdea'), 3)


def test_save_sample_version_as_admin():
    '''
    Also test that not providing a notifier causes no issues.
    '''
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.save_sample_version.return_value = 3

    assert s.save_sample(
        Sample([SampleNode('foo')], 'some sample'),
        UserID('usera'),
        UUID('1234567890abcdef1234567890abcdea'),
        as_admin=True) == (UUID('1234567890abcdef1234567890abcdea'), 3)

    meta.validate_metadata.assert_has_calls([call({}, False)])

    storage.save_sample_version.assert_called_once_with(
        SavedSample(UUID('1234567890abcdef1234567890abcdea'),
                    UserID('usera'),
                    [SampleNode('foo')],
                    datetime.datetime.fromtimestamp(6, tz=datetime.timezone.utc),
                    'some sample'
                    ),
        None)


def test_save_sample_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

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
    ws = create_autospec(WS, spec_set=True, instance=True)

    meta.validate_metadata.side_effect = MetadataValidationError('No validator for key3')
    s = Samples(storage, lu, meta, ws, now=nw,
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
    ws = create_autospec(WS, spec_set=True, instance=True)

    meta.validate_metadata.side_effect = [None, MetadataValidationError('key2: u suk lol')]
    s = Samples(storage, lu, meta, ws, now=nw,
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
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')],
        True)  # public read should not allow saving a new version of a sample

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
    _get_sample(UserID('otheruser'), 4, False)
    _get_sample(UserID('anotheruser'), 2, False)
    _get_sample(UserID('x'), None, False)
    _get_sample(UserID('notinacl'), None, True)
    _get_sample(UserID('notinacl'), None, False, True)  # public read
    _get_sample(None, None, False, True)  # public read & anon


def test_get_samples():
    # sample versions other than 4 don't really make sense but the mock doesn't care
    _get_samples(UserID('someuser'), None, False)
    _get_samples(UserID('otheruser'), 4, False)
    _get_samples(UserID('anotheruser'), 2, False)
    _get_samples(UserID('x'), None, False)
    _get_samples(UserID('notinacl'), None, True)
    _get_samples(UserID('notinacl'), None, False, True)  # public read
    _get_samples(None, None, False, True)  # public read & anon


def _get_sample(user, version, as_admin, public_read=False):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')],
        public_read=public_read)

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


def _get_samples(user, version, as_admin, public_read=False):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, uuid_gen=lambda: UUID('1234567890abcdef1234567890fbcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')],
        public_read=public_read)

    storage.get_samples.return_value = [
        SavedSample(
            UUID('1234567890abcdef1234567890fbcdef'),
            UserID('anotheruser'),
            [SampleNode('foo')],
            datetime.datetime.fromtimestamp(42, tz=datetime.timezone.utc),
            'bar',
            4
        ),
        SavedSample(
            UUID('1234567890abcdef1234567890fbcdeb'),
            UserID('anotheruser'),
            [SampleNode('fid')],
            datetime.datetime.fromtimestamp(42, tz=datetime.timezone.utc),
            'baz',
            4
        )
    ]

    assert samples.get_samples([
        {'id': UUID('1234567890abcdef1234567890fbcdef'), 'version': version},
        {'id': UUID('1234567890abcdef1234567890fbcdeb'), 'version': version}
    ], user, as_admin) == [
        SavedSample(
            UUID('1234567890abcdef1234567890fbcdef'),
            UserID('anotheruser'), [SampleNode('foo')],
            datetime.datetime.fromtimestamp(42, tz=datetime.timezone.utc),
            'bar', 4),
        SavedSample(
            UUID('1234567890abcdef1234567890fbcdeb'),
            UserID('anotheruser'), [SampleNode('fid')],
            datetime.datetime.fromtimestamp(42, tz=datetime.timezone.utc),
            'baz', 4)
    ]
    if not as_admin:
        assert storage.get_sample_acls.call_args_list == [
            call(UUID('12345678-90ab-cdef-1234-567890fbcdef')),
            call(UUID('12345678-90ab-cdef-1234-567890fbcdeb'))
        ]
    else:
        assert storage.get_sample_acls.call_args_list == []

    # print('-'*80)
    # print(storage.get_samples.call_args_list)
    # print('-'*80)

    assert storage.get_samples.call_args_list == [
        call([
            {'id': UUID('12345678-90ab-cdef-1234-567890fbcdef'), 'version': version},
            {'id': UUID('12345678-90ab-cdef-1234-567890fbcdeb'), 'version': version}
        ])
    ]

    # assert storage.get_samples.call_args_list == [
    #     ([{'id': UUID('1234567890abcdef1234567890fbcdef'),'version': version},
    #     {'id': UUID('1234567890abcdef1234567890fbcdeb'), 'version': version}], {})]


def test_get_sample_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcdef')

    _get_sample_fail(samples, None, UserID('foo'), 1, ValueError(
        'id_ cannot be a value that evaluates to false'))
    _get_sample_fail(samples, id_, UserID('a'), 0, IllegalParameterError('Version must be > 0'))


def test_get_sample_fail_unauthorized():
    _get_sample_fail_unauthorized(UserID('y'), UnauthorizedError(
        'User y cannot read sample 12345678-90ab-cdef-1234-567890abcdef'))
    _get_sample_fail_unauthorized(None, UnauthorizedError(
        'Anonymous users cannot read sample 12345678-90ab-cdef-1234-567890abcdef'))


def _get_sample_fail_unauthorized(user, expected):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    _get_sample_fail(
        samples, UUID('1234567890abcdef1234567890abcdef'), user, 3, expected)

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
    _get_sample_acls(UserID('no_rights_here'), False, True)
    _get_sample_acls(None, False, True)


def _get_sample_acls(user, as_admin, public_read=False):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(78),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')],
        public_read=public_read)

    assert samples.get_sample_acls(id_, user, as_admin) == SampleACL(
        u('someuser'),
        dt(78),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')],
        public_read=public_read)

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {})]


def test_get_sample_acls_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    _get_sample_acls_fail(samples, None, UserID('foo'), ValueError(
        'id_ cannot be a value that evaluates to false'))


def test_get_sample_acls_fail_unauthorized():
    _get_sample_acls_fail_unauthorized(UserID('y'), UnauthorizedError(
        'User y cannot read sample 12345678-90ab-cdef-1234-567890abcdea'))
    _get_sample_acls_fail_unauthorized(None, UnauthorizedError(
        'Anonymous users cannot read sample 12345678-90ab-cdef-1234-567890abcdea'))


def _get_sample_acls_fail_unauthorized(user, expected):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    _get_sample_acls_fail(
        samples, UUID('1234567890abcdef1234567890abcdea'), user, expected)

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcdea'),), {})]


def _get_sample_acls_fail(samples, id_, user, expected):
    with raises(Exception) as got:
        samples.get_sample_acls(id_, user)
    assert_exception_correct(got.value, expected)


def test_replace_sample_acls():
    _replace_sample_acls(UserID('someuser'), True, False)
    _replace_sample_acls(UserID('otheruser'), False, False)
    _replace_sample_acls(UserID('super_admin_man'), False, True)


def _replace_sample_acls(user: UserID, public_read, as_admin):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)
    samples = Samples(storage, lu, meta, ws, kafka, now=nw,
                      uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = []

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser'), u('y')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    samples.replace_sample_acls(id_, user, SampleACLOwnerless(
        [u('x'), u('y')], [u('z'), u('a')], [u('b'), u('c')], public_read),
        as_admin=as_admin)

    lu.invalid_users.assert_called_once_with([u(x) for x in ['x', 'y', 'a', 'z', 'b', 'c']])

    storage.get_sample_acls.assert_called_once_with(UUID('1234567890abcdef1234567890abcde0'))

    storage.replace_sample_acls.assert_called_once_with(
        UUID('1234567890abcdef1234567890abcde0'),
        SampleACL(
            u('someuser'),
            dt(6),
            [u('x'), u('y')],
            [u('z'), u('a')],
            [u('b'), u('c')],
            public_read))

    kafka.notify_sample_acl_change.assert_called_once_with(id_)


def test_replace_sample_acls_with_owner_change():
    """
    Also tests replacing sample acls without a notifier.
    """
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = []

    storage.get_sample_acls.side_effect = [
        SampleACL(
            u('someuser'),
            dt(1),
            [u('otheruser')],
            [u('anotheruser'), u('ur mum')],
            [u('Fungus J. Pustule Jr.'), u('x')]),
        SampleACL(
            u('someuser2'), dt(1), [u('otheruser'), u('y')],)
        ]

    storage.replace_sample_acls.side_effect = [OwnerChangedError, None]

    samples.replace_sample_acls(id_, UserID('otheruser'), SampleACLOwnerless([u('a')]))

    assert lu.invalid_users.call_args_list == [(([u('a')],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {}),
        ((UUID('1234567890abcdef1234567890abcde0'),), {})
        ]

    assert storage.replace_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'), SampleACL(u('someuser'), dt(6), [u('a')])), {}),
        ((UUID('1234567890abcdef1234567890abcde0'), SampleACL(u('someuser2'), dt(6), [u('a')])), {})
        ]


def test_replace_sample_acls_with_owner_change_fail_lost_perms():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = []

    storage.get_sample_acls.side_effect = [
        SampleACL(
            u('someuser'),
            dt(1),
            [u('otheruser')],
            [u('anotheruser'), u('ur mum')],
            [u('Fungus J. Pustule Jr.'), u('x')]),
        SampleACL(
            u('someuser2'), dt(1), [u('otheruser2'), u('y')],)
        ]

    storage.replace_sample_acls.side_effect = [OwnerChangedError, None]

    _replace_sample_acls_fail(
        samples, id_, UserID('otheruser'), SampleACLOwnerless(write=[u('b')]),
        UnauthorizedError(f'User otheruser cannot administrate sample {id_}'))

    assert lu.invalid_users.call_args_list == [(([u('b')],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {}),
        ((UUID('1234567890abcdef1234567890abcde0'),), {})
        ]

    assert storage.replace_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),
          SampleACL(u('someuser'), dt(6), write=[u('b')])), {})
        ]


def test_replace_sample_acls_with_owner_change_fail_5_times():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = []

    storage.get_sample_acls.side_effect = [
        SampleACL(u(f'someuser{x}'), dt(1), [u('otheruser')]) for x in range(5)
    ]

    storage.replace_sample_acls.side_effect = OwnerChangedError

    _replace_sample_acls_fail(
        samples, id_, UserID('otheruser'), SampleACLOwnerless(read=[u('c')]),
        ValueError(f'Failed setting ACLs after 5 attempts for sample {id_}'))

    assert lu.invalid_users.call_args_list == [(([u('c')],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {}) for _ in range(5)
        ]

    assert storage.replace_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),
            SampleACL(u(f'someuser{x}'), dt(6), read=[u('c')]),), {}) for x in range(5)
        ]


def test_replace_sample_acls_fail_bad_input():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')
    u = UserID('u')

    _replace_sample_acls_fail(samples, None, u, SampleACLOwnerless(), ValueError(
        'id_ cannot be a value that evaluates to false'))
    _replace_sample_acls_fail(samples, id_, None, SampleACLOwnerless(), ValueError(
        'user cannot be a value that evaluates to false'))
    _replace_sample_acls_fail(samples, id_, u, None, ValueError(
        'new_acls cannot be a value that evaluates to false'))


def test_replace_sample_acls_fail_nonexistent_user_4_users():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = [u('whoo'), u('yay'), u('bugga'), u('w')]

    acls = SampleACLOwnerless(
        [u('x'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z'), u('w')])

    _replace_sample_acls_fail(
        samples, id_, UserID('foo'), acls, NoSuchUserError('whoo, yay, bugga, w'))

    lu.invalid_users.assert_called_once_with(
        [u('whoo'), u('x'), u('fwew'), u('yay'), u('bugga'), u('w'), u('y'), u('z')])


def test_replace_sample_acls_fail_nonexistent_user_5_users():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = [u('whoo'), u('yay'), u('bugga'), u('w'), u('c')]

    acls = SampleACLOwnerless(
        [u('x'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z'), u('w'), u('c')])

    _replace_sample_acls_fail(
        samples, id_, UserID('foo'), acls, NoSuchUserError('whoo, yay, bugga, w, c'))

    lu.invalid_users.assert_called_once_with(
        [u('whoo'), u('x'), u('fwew'), u('yay'), u('bugga'), u('c'), u('w'), u('y'), u('z')])


def test_replace_sample_acls_fail_nonexistent_user_6_users():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = [u('whoo'), u('yay'), u('bugga'), u('w'), u('c'), u('whee')]

    acls = SampleACLOwnerless(
        [u('x'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z'), u('w'), u('c'), u('whee')])

    _replace_sample_acls_fail(
        samples, id_, UserID('foo'), acls, NoSuchUserError('whoo, yay, bugga, w, c'))

    lu.invalid_users.assert_called_once_with(
        [u('whoo'), u('x'), u('fwew'), u('yay'), u('bugga'), u('c'), u('w'), u('whee'), u('y'),
         u('z')])


def test_replace_sample_acls_fail_invalid_user():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.side_effect = user_lookup.InvalidUserError('o shit waddup')

    acls = SampleACLOwnerless(
        [u('o shit waddup'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z')])

    _replace_sample_acls_fail(samples, id_, UserID('foo'), acls, NoSuchUserError('o shit waddup'))

    assert lu.invalid_users.call_args_list == [
        (([u('o shit waddup'), u('whoo'), u('fwew'), u('yay'), u('bugga'), u('y'), u('z')],), {})]


def test_replace_sample_acls_fail_invalid_token():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.side_effect = user_lookup.InvalidTokenError('you big dummy')

    acls = SampleACLOwnerless(
        [u('x'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z')])

    _replace_sample_acls_fail(samples, id_, UserID('foo'), acls, ValueError(
        'user lookup token for KBase auth server is invalid, cannot continue'))

    assert lu.invalid_users.call_args_list == [
        (([u('whoo'), u('x'), u('fwew'), u('yay'), u('bugga'), u('y'), u('z')],), {})]


def test_replace_sample_acls_fail_unauthorized():
    _replace_sample_acls_fail_unauthorized(UserID('anotheruser'))
    _replace_sample_acls_fail_unauthorized(UserID('x'))
    _replace_sample_acls_fail_unauthorized(UserID('MrsEntity'))


def _replace_sample_acls_fail_unauthorized(user: UserID):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = []

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')],
        public_read=True)  # public read shouldn't grant privs.

    _replace_sample_acls_fail(samples, id_, user, SampleACLOwnerless(), UnauthorizedError(
        f'User {user} cannot administrate sample 12345678-90ab-cdef-1234-567890abcde0'))

    assert lu.invalid_users.call_args_list == [(([],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {})]


def _replace_sample_acls_fail(samples, id_, user, acls: SampleACLOwnerless, expected):
    with raises(Exception) as got:
        samples.replace_sample_acls(id_, user, acls)
    assert_exception_correct(got.value, expected)


def test_update_sample_acls():
    _update_sample_acls(UserID('someuser'), True)
    _update_sample_acls(UserID('otheruser'), False)


def _update_sample_acls(user: UserID, public_read):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)
    samples = Samples(storage, lu, meta, ws, kafka, now=nw,
                      uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = []

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser'), u('y')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    samples.update_sample_acls(id_, user, SampleACLDelta(
        [u('x'), u('y')], [u('z'), u('a')], [u('b'), u('c')], [u('r'), u('q')],
        public_read))

    lu.invalid_users.assert_called_once_with(
        [u(x) for x in ['x', 'y', 'a', 'z', 'b', 'c', 'q', 'r']])

    storage.get_sample_acls.assert_called_once_with(UUID('1234567890abcdef1234567890abcde0'))

    storage.update_sample_acls.assert_called_once_with(
        UUID('1234567890abcdef1234567890abcde0'),
        SampleACLDelta(
            [u('x'), u('y')],
            [u('z'), u('a')],
            [u('b'), u('c')],
            [u('r'), u('q')],
            public_read),
        dt(6))

    kafka.notify_sample_acl_change.assert_called_once_with(
        UUID('1234567890abcdef1234567890abcde0'))


def test_update_sample_acls_as_admin_without_notifier():
    '''
    Also use None for public read.
    '''
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(storage, lu, meta, ws, now=nw,
                      uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = []

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser'), u('y')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    samples.update_sample_acls(id_, UserID('someguy'), SampleACLDelta(
        [u('x'), u('y')], [u('z'), u('a')], [u('b'), u('c')], [u('r'), u('q')],
        None),
        as_admin=True)

    lu.invalid_users.assert_called_once_with(
        [u(x) for x in ['x', 'y', 'a', 'z', 'b', 'c', 'q', 'r']])

    storage.update_sample_acls.assert_called_once_with(
        UUID('1234567890abcdef1234567890abcde0'),
        SampleACLDelta(
            [u('x'), u('y')],
            [u('z'), u('a')],
            [u('b'), u('c')],
            [u('r'), u('q')],
            None),
        dt(6))


def test_update_sample_acls_fail_bad_input():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')
    u = UserID('u')

    _update_sample_acls_fail(samples, None, u, SampleACLDelta(), ValueError(
        'id_ cannot be a value that evaluates to false'))
    _update_sample_acls_fail(samples, id_, None, SampleACLDelta(), ValueError(
        'user cannot be a value that evaluates to false'))
    _update_sample_acls_fail(samples, id_, u, None, ValueError(
        'update cannot be a value that evaluates to false'))


def test_update_sample_acls_fail_nonexistent_user_5_users():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = [u('whoo'), u('yay'), u('bugga'), u('w'), u('c')]

    acls = SampleACLDelta(
        [u('x'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z'), u('w'), u('c')],
        [u('rem')])

    _update_sample_acls_fail(
        samples, id_, UserID('foo'), acls, NoSuchUserError('whoo, yay, bugga, w, c'))

    lu.invalid_users.assert_called_once_with(
        [u('whoo'), u('x'), u('fwew'), u('yay'), u('bugga'), u('c'), u('w'), u('y'), u('z'),
         u('rem')])


def test_update_sample_acls_fail_nonexistent_user_6_users():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = [u('whoo'), u('yay'), u('bugga'), u('w'), u('c'), u('whee')]

    acls = SampleACLDelta(
        [u('x'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z'), u('w'), u('c'), u('whee')],
        [u('rem')])

    _update_sample_acls_fail(
        samples, id_, UserID('foo'), acls, NoSuchUserError('whoo, yay, bugga, w, c'))

    lu.invalid_users.assert_called_once_with(
        [u('whoo'), u('x'), u('fwew'), u('yay'), u('bugga'), u('c'), u('w'), u('whee'), u('y'),
         u('z'), u('rem')])


def test_update_sample_acls_fail_invalid_user():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.side_effect = user_lookup.InvalidUserError('o shit waddup')

    acls = SampleACLDelta(
        [u('o shit waddup'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z')])

    _update_sample_acls_fail(samples, id_, UserID('foo'), acls, NoSuchUserError('o shit waddup'))

    assert lu.invalid_users.call_args_list == [
        (([u('o shit waddup'), u('whoo'), u('fwew'), u('yay'), u('bugga'), u('y'), u('z')],), {})]


def test_update_sample_acls_fail_invalid_token():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.side_effect = user_lookup.InvalidTokenError('you big dummy')

    acls = SampleACLDelta(
        [u('x'), u('whoo')],
        [u('yay'), u('fwew')],
        [u('y'), u('bugga'), u('z')])

    _update_sample_acls_fail(samples, id_, UserID('foo'), acls, ValueError(
        'user lookup token for KBase auth server is invalid, cannot continue'))

    assert lu.invalid_users.call_args_list == [
        (([u('whoo'), u('x'), u('fwew'), u('yay'), u('bugga'), u('y'), u('z')],), {})]


def test_update_sample_acls_fail_unauthorized():
    _update_sample_acls_fail_unauthorized(UserID('anotheruser'))
    _update_sample_acls_fail_unauthorized(UserID('x'))
    _update_sample_acls_fail_unauthorized(UserID('MrsEntity'))


def _update_sample_acls_fail_unauthorized(user: UserID):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    samples = Samples(
        storage, lu, meta, ws, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    lu.invalid_users.return_value = []

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')],
        public_read=True)  # public read shouldn't grant privs.

    _update_sample_acls_fail(samples, id_, user, SampleACLDelta(), UnauthorizedError(
        f'User {user} cannot administrate sample 12345678-90ab-cdef-1234-567890abcde0'))

    assert lu.invalid_users.call_args_list == [(([],), {})]

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {})]


def _update_sample_acls_fail(samples, id_, user, update, expected):
    with raises(Exception) as got:
        samples.update_sample_acls(id_, user, update)
    assert_exception_correct(got.value, expected)


def test_get_key_metadata():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw,
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
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw,
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
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    with raises(Exception) as got:
        s.get_key_static_metadata(None)
    assert_exception_correct(got.value, ValueError('keys cannot be None'))


def test_create_data_link():
    _create_data_link(UserID('someuser'))
    _create_data_link(UserID('otheruser'))
    _create_data_link(UserID('y'))


def _create_data_link(user):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, kafka, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser'), u('y')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    assert s.create_data_link(
        user,
        DataUnitID(UPA('1/1/1')),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode')
        ) == DataLink(
            UUID('1234567890abcdef1234567890abcdef'),
            DataUnitID(UPA('1/1/1')),
            SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
            dt(6),
            user
        )

    storage.get_sample_acls.assert_called_once_with(UUID('1234567890abcdef1234567890abcdee'))

    ws.has_permission.assert_called_once_with(user, WorkspaceAccessType.WRITE, upa=UPA('1/1/1'))

    dl = DataLink(
        UUID('1234567890abcdef1234567890abcdef'),
        DataUnitID(UPA('1/1/1')),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
        dt(6),
        user
    )

    storage.create_data_link.assert_called_once_with(dl, update=False)

    kafka.notify_new_link.assert_called_once_with(UUID('1234567890abcdef1234567890abcdef'))


def test_create_data_link_with_data_id_and_update():
    '''
    Test with a data id in the DUID and update=True.
    '''
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, kafka, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(u('someuser'), dt(1))

    storage.create_data_link.return_value = UUID('1234567890abcdef1234567890abcde1')

    assert s.create_data_link(
        UserID('someuser'),
        DataUnitID(UPA('1/1/1'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
        update=True
        ) == DataLink(
            UUID('1234567890abcdef1234567890abcdef'),
            DataUnitID(UPA('1/1/1'), 'foo'),
            SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
            dt(6),
            UserID('someuser')
        )

    storage.get_sample_acls.assert_called_once_with(UUID('1234567890abcdef1234567890abcdee'))

    ws.has_permission.assert_called_once_with(
        UserID('someuser'), WorkspaceAccessType.WRITE, upa=UPA('1/1/1'))

    dl = DataLink(
        UUID('1234567890abcdef1234567890abcdef'),
        DataUnitID(UPA('1/1/1'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
        dt(6),
        UserID('someuser')
    )

    storage.create_data_link.assert_called_once_with(dl, update=True)

    kafka.notify_new_link.assert_called_once_with(UUID('1234567890abcdef1234567890abcdef'))
    kafka.notify_expired_link.assert_called_once_with(UUID('1234567890abcdef1234567890abcde1'))


def test_create_data_link_as_admin():
    """
    Also tests creating a link without a notifier.
    """
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    assert s.create_data_link(
        UserID('someuser'),
        DataUnitID(UPA('1/1/1'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
        as_admin=True
        ) == DataLink(
            UUID('1234567890abcdef1234567890abcdef'),
            DataUnitID(UPA('1/1/1'), 'foo'),
            SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
            dt(6),
            UserID('someuser')
        )

    ws.has_permission.assert_called_once_with(
        UserID('someuser'), WorkspaceAccessType.NONE, upa=UPA('1/1/1'))

    dl = DataLink(
        UUID('1234567890abcdef1234567890abcdef'),
        DataUnitID(UPA('1/1/1'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
        dt(6),
        UserID('someuser')
    )

    storage.create_data_link.assert_called_once_with(dl, update=False)


def test_create_data_link_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    u = UserID('u')
    d = DataUnitID(UPA('1/1/1'))
    sna = SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'node')

    _create_data_link_fail(s, None, d, sna, ValueError(
        'user cannot be a value that evaluates to false'))
    _create_data_link_fail(s, u, None, sna, ValueError(
        'duid cannot be a value that evaluates to false'))
    _create_data_link_fail(s, u, d, None, ValueError(
        'sna cannot be a value that evaluates to false'))


def test_create_data_link_fail_no_sample_access():
    _create_data_link_fail_no_sample_access(UserID('writeonly'))
    _create_data_link_fail_no_sample_access(UserID('readonly'))
    _create_data_link_fail_no_sample_access(UserID('noaccess'))


def _create_data_link_fail_no_sample_access(user):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser'), u('y')],
        [u('anotheruser'), u('writeonly')],
        [u('readonly'), u('x')],
        public_read=True)  # public read shouldn't grant privs

    _create_data_link_fail(
        s,
        user,
        DataUnitID(UPA('1/1/1'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
        UnauthorizedError(
            f'User {user} cannot administrate sample 12345678-90ab-cdef-1234-567890abcdee'))

    storage.get_sample_acls.assert_called_once_with(UUID('1234567890abcdef1234567890abcdee'))


def test_create_data_link_fail_no_ws_access():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw,
                uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(u('someuser'), dt(1))

    ws.has_permission.side_effect = UnauthorizedError('nope. uh uh')

    _create_data_link_fail(
        s,
        UserID('someuser'),
        DataUnitID(UPA('7/3/2'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
        UnauthorizedError('nope. uh uh'))

    storage.get_sample_acls.assert_called_once_with(UUID('1234567890abcdef1234567890abcdee'))

    ws.has_permission.assert_called_once_with(
        UserID('someuser'), WorkspaceAccessType.WRITE, upa=UPA('7/3/2'))


def _create_data_link_fail(samples, user, duid, sna, expected):
    with raises(Exception) as got:
        samples.create_data_link(user, duid, sna)
    assert_exception_correct(got.value, expected)


def test_get_links_from_sample():
    _get_links_from_sample(UserID('someuser'))
    _get_links_from_sample(UserID('otheruser'))
    _get_links_from_sample(UserID('ur mum'))
    _get_links_from_sample(UserID('x'))
    _get_links_from_sample(UserID('noaccess'), True)
    _get_links_from_sample(None, True)


def _get_links_from_sample(user, public_read=False):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser'), u('y')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')],
        public_read=public_read)

    ws.get_user_workspaces.return_value = [7, 90, 106]

    dl1 = DataLink(
        UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('1/1/1'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
        dt(5),
        UserID('userb')
    )

    dl2 = DataLink(
        UUID('1234567890abcdef1234567890abcdec'),
        DataUnitID(UPA('1/2/1'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode3'),
        dt(5),
        UserID('usera')
    )

    storage.get_links_from_sample.return_value = [dl1, dl2]

    assert s.get_links_from_sample(
        user, SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3)) == ([dl1, dl2], dt(6))

    storage.get_sample_acls.assert_called_once_with(UUID('1234567890abcdef1234567890abcdee'))

    ws.get_user_workspaces.assert_called_once_with(user)

    storage.get_links_from_sample.assert_called_once_with(
        SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3),
        [7, 90, 106],
        dt(6)
    )


def test_get_links_from_sample_as_admin():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    dl1 = DataLink(
        UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('1/1/1'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
        dt(5),
        UserID('userb')
    )

    dl2 = DataLink(
        UUID('1234567890abcdef1234567890abcdec'),
        DataUnitID(UPA('1/2/1'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode3'),
        dt(5),
        UserID('usera')
    )

    storage.get_links_from_sample.return_value = [dl1, dl2]

    assert s.get_links_from_sample(
        UserID('whateva'),
        SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3),
        as_admin=True) == ([dl1, dl2], dt(6))

    storage.get_links_from_sample.assert_called_once_with(
        SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3),
        None,
        dt(6)
    )


def test_get_links_from_sample_with_timestamp():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    storage.get_sample_acls.return_value = SampleACL(u('someuser'), dt(1))

    ws.get_user_workspaces.return_value = [3]

    dl1 = DataLink(
        UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('1/1/1'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3), 'mynode'),
        dt(5),
        UserID('userb')
    )

    storage.get_links_from_sample.return_value = [dl1]

    assert s.get_links_from_sample(
        UserID('someuser'),
        SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3),
        dt(40)) == ([dl1], dt(40))

    storage.get_sample_acls.assert_called_once_with(UUID('1234567890abcdef1234567890abcdee'))

    ws.get_user_workspaces.assert_called_once_with(UserID('someuser'))

    storage.get_links_from_sample.assert_called_once_with(
        SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3),
        [3],
        dt(40)
    )


def test_get_links_from_sample_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    u = UserID('u')
    sa = SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3)
    bt = datetime.datetime.fromtimestamp(1)

    _get_links_from_sample_fail(s, u, None, None, ValueError(
        'sample cannot be a value that evaluates to false'))
    _get_links_from_sample_fail(s, u, sa, bt, ValueError(
        'timestamp cannot be a naive datetime'))


def test_get_links_from_sample_fail_unauthorized():
    _get_links_from_sample_fail_unauthorized(UserID('z'), UnauthorizedError(
        'User z cannot read sample 12345678-90ab-cdef-1234-567890abcdee'))
    _get_links_from_sample_fail_unauthorized(None, UnauthorizedError(
        'Anonymous users cannot read sample 12345678-90ab-cdef-1234-567890abcdee'))


def _get_links_from_sample_fail_unauthorized(user, expected):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser'), u('y')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    _get_links_from_sample_fail(
        s,
        user,
        SampleAddress(UUID('1234567890abcdef1234567890abcdee'), 3),
        None,
        expected)

    storage.get_sample_acls.assert_called_once_with(UUID('1234567890abcdef1234567890abcdee'))


def _get_links_from_sample_fail(samples, user, sample, ts, expected):
    with raises(Exception) as got:
        samples.get_links_from_sample(user, sample, ts)
    assert_exception_correct(got.value, expected)


def test_get_links_from_data():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    dl1 = DataLink(
        UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('2/4/6'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdea'), 3), 'mynode'),
        dt(5),
        UserID('userb')
    )

    dl2 = DataLink(
        UUID('1234567890abcdef1234567890abcdec'),
        DataUnitID(UPA('2/4/6')),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdeb'), 1), 'mynode3'),
        dt(4),
        UserID('usera')
    )

    storage.get_links_from_data.return_value = [dl1, dl2]

    assert s.get_links_from_data(UserID('u1'), UPA('2/4/6')) == ([dl1, dl2], dt(6))

    ws.has_permission.assert_called_once_with(
        UserID('u1'), WorkspaceAccessType.READ, upa=UPA('2/4/6'))

    storage.get_links_from_data.assert_called_once_with(UPA('2/4/6'), dt(6))


def test_get_links_from_data_with_timestamp_and_anon_user():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    dl1 = DataLink(
        UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('2/4/6'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdea'), 3), 'mynode'),
        dt(5),
        UserID('userb')
    )

    storage.get_links_from_data.return_value = [dl1]

    assert s.get_links_from_data(None, UPA('2/4/6'), timestamp=dt(700)) == ([dl1], dt(700))

    ws.has_permission.assert_called_once_with(
        None, WorkspaceAccessType.READ, upa=UPA('2/4/6'))

    storage.get_links_from_data.assert_called_once_with(UPA('2/4/6'), dt(700))


def test_get_links_from_data_as_admin():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    dl1 = DataLink(
        UUID('1234567890abcdef1234567890abcdee'),
        DataUnitID(UPA('2/4/6'), 'foo'),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdea'), 3), 'mynode'),
        dt(5),
        UserID('userb')
    )

    dl2 = DataLink(
        UUID('1234567890abcdef1234567890abcdec'),
        DataUnitID(UPA('2/4/6')),
        SampleNodeAddress(SampleAddress(UUID('1234567890abcdef1234567890abcdeb'), 1), 'mynode3'),
        dt(4),
        UserID('usera')
    )

    storage.get_links_from_data.return_value = [dl1, dl2]

    assert s.get_links_from_data(UserID('u1'), UPA('2/4/6'), as_admin=True) == ([dl1, dl2], dt(6))

    ws.has_permission.assert_called_once_with(
        UserID('u1'), WorkspaceAccessType.NONE, upa=UPA('2/4/6'))

    storage.get_links_from_data.assert_called_once_with(UPA('2/4/6'), dt(6))


def test_get_links_from_data_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    u = UserID('u')
    up = UPA('1/1/1')
    bt = datetime.datetime.fromtimestamp(1)

    _get_links_from_from_data_fail(s, u, None, None, ValueError(
        'upa cannot be a value that evaluates to false'))
    _get_links_from_from_data_fail(s, u, up, bt, ValueError(
        'timestamp cannot be a naive datetime'))


def test_get_links_from_data_fail_no_ws_access():
    _get_links_from_data_fail_no_ws_access(UserID('u'))
    _get_links_from_data_fail_no_ws_access(None)


def _get_links_from_data_fail_no_ws_access(user):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    ws.has_permission.side_effect = UnauthorizedError('oh honey')

    _get_links_from_from_data_fail(s, user, UPA('1/1/1'), None,
                                   UnauthorizedError('oh honey'))

    ws.has_permission.assert_called_once_with(
        user, WorkspaceAccessType.READ, upa=UPA('1/1/1'))


def _get_links_from_from_data_fail(samples, user, upa, ts, expected):
    with raises(Exception) as got:
        samples.get_links_from_data(user, upa, ts)
    assert_exception_correct(got.value, expected)


def test_get_sample_via_data():
    _get_sample_via_data(None)
    _get_sample_via_data(UserID('someguy'))


def _get_sample_via_data(user):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    id_ = UUID('1234567890abcdef1234567890abcdee')

    storage.has_data_link.return_value = True

    storage.get_sample.return_value = SavedSample(
        id_,
        UserID('yay'),
        [SampleNode('myname')],
        dt(84),
        version=4
    )

    assert s.get_sample_via_data(
        user, UPA('4/5/7'), SampleAddress(id_, 4)) == SavedSample(
            id_,
            UserID('yay'),
            [SampleNode('myname')],
            dt(84),
            version=4
        )

    ws.has_permission.assert_called_once_with(
        user, WorkspaceAccessType.READ, upa=UPA('4/5/7'))

    storage.has_data_link.assert_called_once_with(UPA('4/5/7'), id_)
    storage.get_sample.assert_called_once_with(id_, 4)


def test_get_sample_via_data_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    u = UserID('u')
    up = UPA('1/1/1')
    sa = SampleAddress(uuid.uuid4(), 1)

    _get_sample_via_data_fail(s, u, None, sa, ValueError(
        'upa cannot be a value that evaluates to false'))
    _get_sample_via_data_fail(s, u, up, None, ValueError(
        'sample_address cannot be a value that evaluates to false'))


def test_get_sample_via_data_fail_no_ws_access():
    _get_sample_via_data_fail_no_ws_access(None)
    _get_sample_via_data_fail_no_ws_access(UserID('u'))


def _get_sample_via_data_fail_no_ws_access(user):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    ws.has_permission.side_effect = UnauthorizedError('oh honey boo boo')

    _get_sample_via_data_fail(s, user, UPA('1/1/1'), SampleAddress(uuid.uuid4(), 5),
                              UnauthorizedError('oh honey boo boo'))

    ws.has_permission.assert_called_once_with(
        user, WorkspaceAccessType.READ, upa=UPA('1/1/1'))


def test_get_sample_via_data_fail_no_link():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    id_ = UUID('1234567890abcdef1234567890abcdee')

    storage.has_data_link.return_value = False

    _get_sample_via_data_fail(s, UserID('u'), UPA('4/5/6'), SampleAddress(id_, 3),
                              NoSuchLinkError(f'There is no link from UPA 4/5/6 to sample {id_}'))

    ws.has_permission.assert_called_once_with(
        UserID('u'), WorkspaceAccessType.READ, upa=UPA('4/5/6'))

    storage.has_data_link.assert_called_once_with(UPA('4/5/6'), id_)


def _get_sample_via_data_fail(samples, user, upa, sa, expected):
    with raises(Exception) as got:
        samples.get_sample_via_data(user, upa, sa)
    assert_exception_correct(got.value, expected)


def test_expire_data_link():
    _expire_data_link(UserID('someuser'))
    _expire_data_link(UserID('otheruser'))
    _expire_data_link(UserID('y'))


def _expire_data_link(user):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, kafka, now=nw)

    sid = UUID('1234567890abcdef1234567890abcdee')
    lid = UUID('1234567890abcdef1234567890abcde2')
    storage.get_data_link.return_value = DataLink(
        lid,
        DataUnitID(UPA('6/1/2'), 'foo'),
        SampleNodeAddress(SampleAddress(sid, 3), 'node'),
        dt(34),
        UserID('userc')
    )

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser'), u('y')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    s.expire_data_link(user, DataUnitID(UPA('6/1/2'), 'foo'))

    ws.has_permission.assert_called_once_with(
        user, WorkspaceAccessType.WRITE, workspace_id=6)

    storage.get_data_link.assert_called_once_with(duid=DataUnitID(UPA('6/1/2'), 'foo'))
    storage.get_sample_acls.assert_called_once_with(sid)
    storage.expire_data_link.assert_called_once_with(dt(6), user, id_=lid)

    kafka.notify_expired_link.assert_called_once_with(lid)


def test_expire_data_link_as_admin():
    """
    Also tests expiring links without a notifier.
    """
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    sid = UUID('1234567890abcdef1234567890abcdee')
    lid = UUID('1234567890abcdef1234567890abcde1')
    storage.get_data_link.return_value = DataLink(
        lid,
        DataUnitID(UPA('6/1/2'), 'foo'),
        SampleNodeAddress(SampleAddress(sid, 3), 'node'),
        dt(34),
        UserID('userc')
    )

    s.expire_data_link(UserID('userf'), DataUnitID(UPA('6/1/2'), 'foo'), as_admin=True)

    ws.has_permission.assert_called_once_with(
        UserID('userf'), WorkspaceAccessType.NONE, workspace_id=6)

    storage.get_data_link.assert_called_once_with(duid=DataUnitID(UPA('6/1/2'), 'foo'))
    storage.expire_data_link.assert_called_once_with(dt(6), UserID('userf'), id_=lid)


def test_expire_data_link_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    _expire_data_link_fail(s, None, DataUnitID(UPA('1/1/1'), 'foo'), ValueError(
        'user cannot be a value that evaluates to false'))
    _expire_data_link_fail(s, UserID('u'), None, ValueError(
        'duid cannot be a value that evaluates to false'))


def test_expire_data_link_fail_no_ws_access():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    ws.has_permission.side_effect = UnauthorizedError('oh honey boo boo foofy foo')

    _expire_data_link_fail(s, UserID('u'), DataUnitID(UPA('1/1/1')),
                           UnauthorizedError('oh honey boo boo foofy foo'))

    ws.has_permission.assert_called_once_with(
        UserID('u'), WorkspaceAccessType.WRITE, workspace_id=1)


def test_expire_data_link_fail_no_link():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    storage.get_data_link.side_effect = NoSuchLinkError('oh lordy')

    _expire_data_link_fail(s, UserID('a'), DataUnitID(UPA('6/1/2'), 'foo'),
                           NoSuchLinkError('oh lordy'))

    ws.has_permission.assert_called_once_with(
        UserID('a'), WorkspaceAccessType.WRITE, workspace_id=6)

    storage.get_data_link.assert_called_once_with(duid=DataUnitID(UPA('6/1/2'), 'foo'))


def test_expire_data_link_fail_no_sample_access():
    _expire_data_link_fail_no_sample_access(UserID('anotheruser'))
    _expire_data_link_fail_no_sample_access(UserID('x'))
    _expire_data_link_fail_no_sample_access(UserID('z'))


def _expire_data_link_fail_no_sample_access(user):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    sid = UUID('1234567890abcdef1234567890abcdee')
    storage.get_data_link.return_value = DataLink(
        uuid.uuid4(),  # unused
        DataUnitID(UPA('9/1/2'), 'foo'),
        SampleNodeAddress(SampleAddress(sid, 3), 'node'),
        dt(34),
        UserID('userc')
    )

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser'), u('y')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')],
        public_read=True)  # public read shouldn't grant perms

    _expire_data_link_fail(s, user, DataUnitID(UPA('9/1/2'), 'foo'),
                           UnauthorizedError(f'User {user} cannot administrate sample {sid}'))

    ws.has_permission.assert_called_once_with(
        user, WorkspaceAccessType.WRITE, workspace_id=9)

    storage.get_data_link.assert_called_once_with(duid=DataUnitID(UPA('9/1/2'), 'foo'))
    storage.get_sample_acls.assert_called_once_with(sid)


def test_expire_data_link_fail_no_link_at_storage():
    '''
    Tests the improbable case where the link is expired after fetching it from storage
    but before the expire command is sent from storage.
    '''
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    sid = UUID('1234567890abcdef1234567890abcdee')
    lid = UUID('1234567890abcdef1234567890abcde1')
    storage.get_data_link.return_value = DataLink(
        lid,
        DataUnitID(UPA('6/1/2')),
        SampleNodeAddress(SampleAddress(sid, 3), 'node'),
        dt(34),
        UserID('userc')
    )

    storage.get_sample_acls.return_value = SampleACL(
        u('someuser'),
        dt(1),
        [u('otheruser'), u('y')],
        [u('anotheruser'), u('ur mum')],
        [u('Fungus J. Pustule Jr.'), u('x')])

    storage.expire_data_link.side_effect = NoSuchLinkError("dang y'all")

    _expire_data_link_fail(s, UserID('y'), DataUnitID(UPA('6/1/2')),
                           NoSuchLinkError("dang y'all"))

    ws.has_permission.assert_called_once_with(
        UserID('y'), WorkspaceAccessType.WRITE, workspace_id=6)

    storage.get_data_link.assert_called_once_with(duid=DataUnitID(UPA('6/1/2')))
    storage.get_sample_acls.assert_called_once_with(sid)
    storage.expire_data_link.assert_called_once_with(dt(6), UserID('y'), id_=lid)


def _expire_data_link_fail(samples, user, duid, expected):
    with raises(Exception) as got:
        samples.expire_data_link(user, duid)
    assert_exception_correct(got.value, expected)


def test_get_data_link_admin():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    sid = UUID('1234567890abcdef1234567890abcdee')
    storage.get_data_link.return_value = DataLink(
        UUID('1234567890abcdef1234567890abcde1'),
        DataUnitID(UPA('6/1/2')),
        SampleNodeAddress(SampleAddress(sid, 3), 'node'),
        dt(34),
        UserID('userc')
    )

    assert s.get_data_link_admin(UUID('1234567890abcdef1234567890abcde1')) == DataLink(
        UUID('1234567890abcdef1234567890abcde1'),
        DataUnitID(UPA('6/1/2')),
        SampleNodeAddress(SampleAddress(sid, 3), 'node'),
        dt(34),
        UserID('userc'))

    storage.get_data_link.assert_called_once_with(UUID('1234567890abcdef1234567890abcde1'))


def test_get_data_link_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    lu = create_autospec(KBaseUserLookup, spec_set=True, instance=True)
    meta = create_autospec(MetadataValidatorSet, spec_set=True, instance=True)
    ws = create_autospec(WS, spec_set=True, instance=True)
    s = Samples(storage, lu, meta, ws, now=nw)

    _get_data_link_fail(s, None, ValueError('link_id cannot be a value that evaluates to false'))


def _get_data_link_fail(samples, linkid, expected):
    with raises(Exception) as got:
        samples.get_data_link_admin(linkid)
    assert_exception_correct(got.value, expected)
