from pytest import raises

from SampleService.core.errors import MissingParameterError, IllegalParameterError
from SampleService.core.user import UserID

from core.test_utils import assert_exception_correct


def test_init():
    u = UserID('foo')

    assert u.id == 'foo'
    assert str(u) == 'foo'
    assert repr(u) == 'UserID("foo")'

    u = UserID('u' * 256)

    assert u.id == 'u' * 256
    assert str(u) == 'u' * 256
    assert repr(u) == f'UserID("{"u" * 256}")'

    u = UserID('u⎇a')

    assert u.id == 'u⎇a'
    assert str(u) == 'u⎇a'
    assert repr(u) == 'UserID("u⎇a")'


def test_init_fail():
    _init_fail(None, MissingParameterError('userid'))
    _init_fail('   \t    ', MissingParameterError('userid'))
    _init_fail('foo \t bar', IllegalParameterError('userid contains control characters'))
    _init_fail('u' * 257, IllegalParameterError('userid exceeds maximum length of 256'))


def _init_fail(u, expected):
    with raises(Exception) as got:
        UserID(u)
    assert_exception_correct(got.value, expected)


def test_equals():
    assert UserID('u') == UserID('u')

    assert UserID('u') != 'u'

    assert UserID('u') != UserID('v')


def test_hash():
    # string hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__
    assert hash(UserID('u')) == hash(UserID('u'))

    assert hash(UserID('u')) != hash(UserID('v'))
