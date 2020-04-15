from pytest import raises

from SampleService.core.acls import SampleACL, SampleACLOwnerless
from core.test_utils import assert_exception_correct
from SampleService.core.errors import IllegalParameterError
from SampleService.core.user import UserID


def u(user):
    return UserID(user)


def test_build_ownerless():
    a = SampleACLOwnerless()
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()

    # test duplicates are removed and order maintained
    a = SampleACLOwnerless(
        [u('baz'), u('bat'), u('baz')],
        read=[u('wheee'), u('wheee')],
        write=[u('wugga'), u('a'), u('b'), u('a')])
    assert a.admin == (u('baz'), u('bat'))
    assert a.write == (u('wugga'), u('a'), u('b'))
    assert a.read == (u('wheee'),)


def test_build_fail_ownerless():
    _build_fail_ownerless([u('a'), None], None, None, ValueError(
        'Index 1 of iterable admin cannot be a value that evaluates to false'))
    _build_fail_ownerless(None, [None, None], None, ValueError(
        'Index 0 of iterable write cannot be a value that evaluates to false'))
    _build_fail_ownerless(None, None, [u('a'), u('b'), None], ValueError(
        'Index 2 of iterable read cannot be a value that evaluates to false'))

    # test that you cannot have a user in 2 acls
    _build_fail_ownerless(
        [u('a'), u('z')], [u('a'), u('c')], [u('w'), u('b')],
        IllegalParameterError('User a appears in two ACLs'))
    _build_fail_ownerless(
        [u('a'), u('z')], [u('b'), u('c')], [u('w'), u('a')],
        IllegalParameterError('User a appears in two ACLs'))
    _build_fail_ownerless(
        [u('x'), u('z')], [u('b'), u('c'), u('w')], [u('w'), u('a')],
        IllegalParameterError('User w appears in two ACLs'))


def _build_fail_ownerless(admin, write, read, expected):
    with raises(Exception) as got:
        SampleACLOwnerless(admin, write, read)
    assert_exception_correct(got.value, expected)


def test_eq_ownerless():
    assert SampleACLOwnerless([u('bar')]) == SampleACLOwnerless([u('bar')])
    assert SampleACLOwnerless([u('bar')]) != SampleACLOwnerless([u('baz')])

    assert SampleACLOwnerless(write=[u('bar')]) == SampleACLOwnerless(write=[u('bar')])
    assert SampleACLOwnerless(write=[u('bar')]) != SampleACLOwnerless(write=[u('baz')])

    assert SampleACLOwnerless(read=[u('bar')]) == SampleACLOwnerless(read=[u('bar')])
    assert SampleACLOwnerless(read=[u('bar')]) != SampleACLOwnerless(read=[u('baz')])

    assert SampleACLOwnerless([u('foo')]) != 1
    assert u('foo') != SampleACLOwnerless([u('foo')])


def test_hash_ownerless():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__

    assert hash(SampleACLOwnerless([u('bar')])) == hash(SampleACLOwnerless([u('bar')]))
    assert hash(SampleACLOwnerless([u('bar')])) != hash(SampleACLOwnerless([u('baz')]))

    assert hash(SampleACLOwnerless(write=[u('bar')])) == hash(SampleACLOwnerless(write=[u('bar')]))
    assert hash(SampleACLOwnerless(write=[u('bar')])) != hash(SampleACLOwnerless(write=[u('baz')]))

    assert hash(SampleACLOwnerless(read=[u('bar')])) == hash(SampleACLOwnerless(read=[u('bar')]))
    assert hash(SampleACLOwnerless(read=[u('bar')])) != hash(SampleACLOwnerless(read=[u('baz')]))


def test_build():
    a = SampleACL(u('foo'))
    assert a.owner == u('foo')
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()

    a = SampleACL(
        u('foo'),
        [u('baz'), u('bat'), u('baz')],
        read=[u('wheee'), u('wheee')],
        write=[u('wugga'), u('a'), u('b'), u('wugga')])
    assert a.owner == u('foo')
    assert a.admin == (u('baz'), u('bat'))
    assert a.write == (u('wugga'), u('a'), u('b'))
    assert a.read == (u('wheee'),)


def test_build_fail():
    _build_fail(None, None, None, None, ValueError(
        'owner cannot be a value that evaluates to false'))
    _build_fail(u('foo'), [u('a'), None], None, None, ValueError(
        'Index 1 of iterable admin cannot be a value that evaluates to false'))
    _build_fail(u('foo'), None, [None, None], None, ValueError(
        'Index 0 of iterable write cannot be a value that evaluates to false'))
    _build_fail(u('foo'), None, None, [u('a'), u('b'), None], ValueError(
        'Index 2 of iterable read cannot be a value that evaluates to false'))

    # test that you cannot have an owner in another ACL
    _build_fail(
        u('foo'), [u('c'), u('d'), u('foo')], None, [u('a'), u('b'), u('x')],
        IllegalParameterError('The owner cannot be in any other ACL'))
    _build_fail(
        u('foo'), None, [u('a'), u('b'), u('foo')], [u('x')],
        IllegalParameterError('The owner cannot be in any other ACL'))
    _build_fail(
        u('foo'), [u('y')], None, [u('a'), u('b'), u('foo')],
        IllegalParameterError('The owner cannot be in any other ACL'))

    # test that you cannot have a user in 2 acls
    _build_fail(
        u('foo'), [u('a'), u('z')], [u('a'), u('c')], [u('w'), u('b')],
        IllegalParameterError('User a appears in two ACLs'))
    _build_fail(
        u('foo'), [u('a'), u('z')], [u('b'), u('c')], [u('w'), u('a')],
        IllegalParameterError('User a appears in two ACLs'))
    _build_fail(
        u('foo'), [u('x'), u('z')], [u('b'), u('c'), u('w')], [u('w'), u('a')],
        IllegalParameterError('User w appears in two ACLs'))


def _build_fail(owner, admin, write, read, expected):
    with raises(Exception) as got:
        SampleACL(owner, admin, write, read)
    assert_exception_correct(got.value, expected)


def test_eq():
    assert SampleACL(u('foo')) == SampleACL(u('foo'))
    assert SampleACL(u('foo')) != SampleACL(u('bar'))

    assert SampleACL(u('foo'), [u('bar')]) == SampleACL(u('foo'), [u('bar')])
    assert SampleACL(u('foo'), [u('bar')]) != SampleACL(u('foo'), [u('baz')])

    assert SampleACL(u('foo'), write=[u('bar')]) == SampleACL(u('foo'), write=[u('bar')])
    assert SampleACL(u('foo'), write=[u('bar')]) != SampleACL(u('foo'), write=[u('baz')])

    assert SampleACL(u('foo'), read=[u('bar')]) == SampleACL(u('foo'), read=[u('bar')])
    assert SampleACL(u('foo'), read=[u('bar')]) != SampleACL(u('foo'), read=[u('baz')])

    assert SampleACL(u('foo')) != 1
    assert u('foo') != SampleACL(u('foo'))


def test_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__

    assert hash(SampleACL(u('foo'))) == hash(SampleACL(u('foo')))
    assert hash(SampleACL(u('bar'))) == hash(SampleACL(u('bar')))
    assert hash(SampleACL(u('foo'))) != hash(SampleACL(u('bar')))

    assert hash(SampleACL(u('foo'), [u('bar')])) == hash(SampleACL(u('foo'), [u('bar')]))
    assert hash(SampleACL(u('foo'), [u('bar')])) != hash(SampleACL(u('foo'), [u('baz')]))

    assert hash(SampleACL(u('foo'), write=[u('bar')])) == hash(
        SampleACL(u('foo'), write=[u('bar')]))
    assert hash(SampleACL(u('foo'), write=[u('bar')])) != hash(
        SampleACL(u('foo'), write=[u('baz')]))

    assert hash(SampleACL(u('foo'), read=[u('bar')])) == hash(SampleACL(u('foo'), read=[u('bar')]))
    assert hash(SampleACL(u('foo'), read=[u('bar')])) != hash(SampleACL(u('foo'), read=[u('baz')]))
