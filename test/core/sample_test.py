import uuid
from pytest import raises
from core.test_utils import assert_exception_correct
from SampleService.core.sample import Sample, SampleWithID


def test_sample_build():
    s = Sample('foo')

    assert s.name == 'foo'

    id_ = uuid.UUID('1234567890abcdef1234567890abcdef')
    s = SampleWithID(id_)

    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.name is None

    s = SampleWithID(id_, 'foo')

    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.name == 'foo'


def test_sample_build_fail():
    with raises(Exception) as got:
        SampleWithID(None)
    assert_exception_correct(
        got.value, ValueError('id_ cannot be a value that evaluates to false'))
