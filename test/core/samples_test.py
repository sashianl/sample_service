import datetime

from pytest import raises
from uuid import UUID
from unittest.mock import create_autospec

from SampleService.core.storage.arango_sample_storage import ArangoSampleStorage
from SampleService.core.acls import SampleACL
from SampleService.core.errors import IllegalParameterError, UnauthorizedError
from SampleService.core.sample import Sample, SampleNode, SampleWithID
from SampleService.core.samples import Samples
from core.test_utils import assert_exception_correct


def nw():
    return datetime.datetime.fromtimestamp(6, tz=datetime.timezone.utc)


def test_init_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    ug = lambda: UUID('1234567890abcdef1234567890abcdef')  # noqa E731

    _init_fail(None, nw, ug, ValueError('storage cannot be a value that evaluates to false'))
    _init_fail(storage, None, ug, ValueError('now cannot be a value that evaluates to false'))
    _init_fail(storage, nw, None, ValueError('uuid_gen cannot be a value that evaluates to false'))


def _init_fail(storage, now, uuid_gen, expected):
    with raises(Exception) as got:
        Samples(storage, now, uuid_gen)
    assert_exception_correct(got.value, expected)


def test_save_sample():
    _save_sample_with_name(None)
    _save_sample_with_name('bar')


def _save_sample_with_name(name):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    s = Samples(storage, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    assert s.save_sample(Sample([SampleNode('foo')], name), 'auser') == (UUID(
        '1234567890abcdef1234567890abcdef'), 1)

    assert storage.save_sample.call_args_list == [
        (('auser',
          SampleWithID(UUID('1234567890abcdef1234567890abcdef'),
                       [SampleNode('foo')],
                       datetime.datetime.fromtimestamp(6, tz=datetime.timezone.utc),
                       name
                       )
          ), {})]


def test_save_sample_version():
    _save_sample_version_per_user('someuser', None, None)
    _save_sample_version_per_user('otheruser', 'sample name', 2)
    # this one should really fail based on the mock output... but it's a mock so it won't
    _save_sample_version_per_user('anotheruser', 'ur dad yeah', 1)


def _save_sample_version_per_user(user, name, prior_version):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    s = Samples(storage, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

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

    assert storage.save_sample_version.call_args_list == [
        ((SampleWithID(UUID('1234567890abcdef1234567890abcdea'),
                       [SampleNode('foo')],
                       datetime.datetime.fromtimestamp(6, tz=datetime.timezone.utc),
                       name
                       ),
          prior_version), {})]


def test_save_sample_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    samples = Samples(storage, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    s = Sample([SampleNode('foo')])
    id_ = UUID('1234567890abcdef1234567890abcdef')

    _save_sample_fail(
        samples, None, 'a', id_, 1, ValueError('sample cannot be a value that evaluates to false'))
    _save_sample_fail(
        samples, s, '', id_, 1, ValueError('user cannot be a value that evaluates to false'))
    _save_sample_fail(
        samples, s, 'a', id_, 0, IllegalParameterError('Prior version must be > 0'))


def test_save_sample_fail_unauthorized():
    _save_sample_fail_unauthorized('x')
    _save_sample_fail_unauthorized('nouserhere')


def _save_sample_fail_unauthorized(user):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    samples = Samples(storage, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

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
    _get_sample('someuser', None)
    _get_sample('otheruser', None)
    _get_sample('anotheruser', None)
    _get_sample('x', None)


def _get_sample(user, version):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    samples = Samples(storage, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

    storage.get_sample_acls.return_value = SampleACL(
        'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x'])

    storage.get_sample.return_value = SampleWithID(
        UUID('1234567890abcdef1234567890abcdea'),
        [SampleNode('foo')],
        datetime.datetime.fromtimestamp(42, tz=datetime.timezone.utc),
        'bar',
        4)

    assert samples.get_sample(
        UUID('1234567890abcdef1234567890abcdea'), user, version) == SampleWithID(
            UUID('1234567890abcdef1234567890abcdea'),
            [SampleNode('foo')],
            datetime.datetime.fromtimestamp(42, tz=datetime.timezone.utc),
            'bar',
            4)

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcdea'),), {})]

    assert storage.get_sample.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcdea'), version), {})]


def test_get_sample_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    samples = Samples(storage, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcdef')

    _get_sample_fail(samples, None, 'foo', 1, ValueError(
        'id_ cannot be a value that evaluates to false'))
    _get_sample_fail(samples, id_, '', 1, ValueError(
        'user cannot be a value that evaluates to false'))
    _get_sample_fail(samples, id_, 'a', 0, IllegalParameterError('Version must be > 0'))


def test_get_sample_fail_unauthorized():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    samples = Samples(storage, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

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
    _get_sample_acls('someuser')
    _get_sample_acls('otheruser')
    _get_sample_acls('anotheruser')
    _get_sample_acls('x')


def _get_sample_acls(user):
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    samples = Samples(storage, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcde0')

    storage.get_sample_acls.return_value = SampleACL(
        'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x'])

    assert samples.get_sample_acls(id_, user) == SampleACL(
        'someuser', ['otheruser'], ['anotheruser', 'ur mum'], ['Fungus J. Pustule Jr.', 'x'])\

    assert storage.get_sample_acls.call_args_list == [
        ((UUID('1234567890abcdef1234567890abcde0'),), {})]


def test_get_sample_acls_fail_bad_args():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    samples = Samples(storage, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))
    id_ = UUID('1234567890abcdef1234567890abcdef')

    _get_sample_acls_fail(samples, None, 'foo', ValueError(
        'id_ cannot be a value that evaluates to false'))
    _get_sample_acls_fail(samples, id_, '', ValueError(
        'user cannot be a value that evaluates to false'))


def test_get_sample_acls_fail_unauthorized():
    storage = create_autospec(ArangoSampleStorage, spec_set=True, instance=True)
    samples = Samples(storage, now=nw, uuid_gen=lambda: UUID('1234567890abcdef1234567890abcdef'))

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
