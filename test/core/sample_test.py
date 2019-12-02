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
    assert s.version is None

    s = SampleWithID(id_, 'foo')
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.name == 'foo'
    assert s.version is None

    s = SampleWithID(id_, 'foo', 1)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.name == 'foo'
    assert s.version == 1

    s = SampleWithID(id_, 'foo', 8)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.name == 'foo'
    assert s.version == 8

    s = SampleWithID(id_, version=8)
    assert s.id == uuid.UUID('1234567890abcdef1234567890abcdef')
    assert s.name is None
    assert s.version == 8


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

    with raises(Exception) as got:
        SampleWithID('a', version=0)
    assert_exception_correct(
        got.value, ValueError('version must be > 0'))


def test_sample_eq():
    assert Sample('yay') == Sample('yay')
    assert Sample('yay') != Sample('yooo')

    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdea')

    assert SampleWithID(id1) == SampleWithID(id1)
    assert SampleWithID(id1) != SampleWithID(id2)

    assert SampleWithID(id1, 'yay') == SampleWithID(id1, 'yay')
    assert SampleWithID(id1, 'yay') != SampleWithID(id1, 'yooo')

    assert SampleWithID(id1, 'yay', 6) == SampleWithID(id1, 'yay', 6)
    assert SampleWithID(id1, 'yay', 6) != SampleWithID(id1, 'yooo', 7)

    assert SampleWithID(id1, 'yay') != Sample('yay')
    assert Sample('yay') != SampleWithID(id1, 'yay')


def test_sample_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__
    id1 = uuid.UUID('1234567890abcdef1234567890abcdef')
    id2 = uuid.UUID('1234567890abcdef1234567890abcdea')

    assert hash(Sample('yay')) == hash(Sample('yay'))
    assert hash(Sample('foo')) == hash(Sample('foo'))
    assert hash(Sample('yay')) != hash(Sample('yo'))

    assert hash(SampleWithID(id1, 'yay')) == hash(SampleWithID(id1, 'yay'))
    assert hash(SampleWithID(id2, 'foo')) == hash(SampleWithID(id2, 'foo'))
    assert hash(SampleWithID(id2, 'foo')) != hash(SampleWithID(id2, 'bar'))
    assert hash(SampleWithID(id1, 'foo')) != hash(SampleWithID(id2, 'foo'))
    assert hash(SampleWithID(id1, 'foo', 6)) == hash(SampleWithID(id1, 'foo', 6))
    assert hash(SampleWithID(id1, 'foo', 6)) != hash(SampleWithID(id1, 'foo', 7))
