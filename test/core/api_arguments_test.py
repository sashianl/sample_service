import datetime

from pytest import raises
from uuid import UUID
from SampleService.core.api_arguments import datetime_to_epochmilliseconds, get_id_from_object
from SampleService.core.errors import IllegalParameterError

from core.test_utils import assert_exception_correct


def test_get_id_from_object():
    assert get_id_from_object(None) is None
    assert get_id_from_object({}) is None
    assert get_id_from_object({'id': None}) is None
    assert get_id_from_object({'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41e'}) == UUID(
        'f5bd78c3-823e-40b2-9f93-20e78680e41e')


def test_get_id_from_object_fail_bad_args():
    get_id_from_object_fail({'id': 6}, IllegalParameterError('Sample ID 6 must be a UUID string'))
    get_id_from_object_fail({'id': 'f5bd78c3-823e-40b2-9f93-20e78680e41'}, IllegalParameterError(
        'Sample ID f5bd78c3-823e-40b2-9f93-20e78680e41 must be a UUID string'))


def get_id_from_object_fail(d, expected):
    with raises(Exception) as got:
        get_id_from_object(d)
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
