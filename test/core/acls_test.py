from pytest import raises

from SampleService.core.acls import SampleACL
from core.test_utils import assert_exception_correct


def test_build():
    a = SampleACL('foo')
    assert a.owner == 'foo'
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()

    a = SampleACL('foo', ['baz', 'bat'], read=['wheee'], write=['wugga', 'a', 'b'])
    assert a.owner == 'foo'
    assert a.admin == ('baz', 'bat')
    assert a.write == ('wugga', 'a', 'b')
    assert a.read == ('wheee',)


def test_build_fail():
    _build_fail('', None, None, None, ValueError(
        'owner cannot be a value that evaluates to false'))
    _build_fail('foo', ['a', ''], None, None, ValueError(
        'Index 1 of iterable admin cannot be a value that evaluates to false'))
    _build_fail('foo', None, ['', ''], None, ValueError(
        'Index 0 of iterable write cannot be a value that evaluates to false'))
    _build_fail('foo', None, None, ['a', 'b', ''], ValueError(
        'Index 2 of iterable read cannot be a value that evaluates to false'))


def _build_fail(owner, admin, write, read, expected):
    with raises(Exception) as got:
        SampleACL(owner, admin, write, read)
    assert_exception_correct(got.value, expected)


def test_eq():
    assert SampleACL('foo') == SampleACL('foo')
    assert SampleACL('foo') != SampleACL('bar')

    assert SampleACL('foo', ['bar']) == SampleACL('foo', ['bar'])
    assert SampleACL('foo', ['bar']) != SampleACL('foo', ['baz'])

    assert SampleACL('foo', write=['bar']) == SampleACL('foo', write=['bar'])
    assert SampleACL('foo', write=['bar']) != SampleACL('foo', write=['baz'])

    assert SampleACL('foo', read=['bar']) == SampleACL('foo', read=['bar'])
    assert SampleACL('foo', read=['bar']) != SampleACL('foo', read=['baz'])

    assert SampleACL('foo') != 1
    assert 'foo' != SampleACL('foo')


def test_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__

    assert hash(SampleACL('foo')) == hash(SampleACL('foo'))
    assert hash(SampleACL('bar')) == hash(SampleACL('bar'))
    assert hash(SampleACL('foo')) != hash(SampleACL('bar'))

    assert hash(SampleACL('foo', ['bar'])) == hash(SampleACL('foo', ['bar']))
    assert hash(SampleACL('foo', ['bar'])) != hash(SampleACL('foo', ['baz']))

    assert hash(SampleACL('foo', write=['bar'])) == hash(SampleACL('foo', write=['bar']))
    assert hash(SampleACL('foo', write=['bar'])) != hash(SampleACL('foo', write=['baz']))

    assert hash(SampleACL('foo', read=['bar'])) == hash(SampleACL('foo', read=['bar']))
    assert hash(SampleACL('foo', read=['bar'])) != hash(SampleACL('foo', read=['baz']))
