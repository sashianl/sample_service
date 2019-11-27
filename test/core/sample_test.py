import uuid
from pytest import raises
from core.test_utils import assert_exception_correct
from SampleService.core.sample import Sample, SampleWithID
from SampleService.core.errors import IllegalParameterError


def test_sample_build():
    s = Sample()

    assert s.name is None

    s = Sample('   \t   foo    ')

    assert s.name == 'foo'

    s = Sample('a' * 255)

    assert s.name == 'a' * 255

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    s = SampleWithID(id_)

    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.name is None

    s = SampleWithID(id_, 'foo')

    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.name == 'foo'


def test_sample_build_fail():
    # not testing every permutation of failing check_string here, just one test to make sure
    # it's there

    with raises(Exception) as got:
        Sample('a' * 256)
    assert_exception_correct(
        got.value, IllegalParameterError('name exceeds maximum length of 255'))

    with raises(Exception) as got:
        SampleWithID(None)
    assert_exception_correct(
        got.value, ValueError('id_ cannot be a value that evaluates to false'))
