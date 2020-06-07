import datetime

from pytest import raises

from SampleService.core.acls import SampleACL, SampleACLOwnerless
from core.test_utils import assert_exception_correct
from SampleService.core.errors import IllegalParameterError
from SampleService.core.user import UserID


def u(user):
    return UserID(user)


def dt(t):
    return datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)


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
    a = SampleACL(u('foo'), dt(30))
    assert a.owner == u('foo')
    assert a.lastupdate == dt(30)
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()

    a = SampleACL(
        u('foo'),
        dt(-56),
        [u('baz'), u('bat'), u('baz')],
        read=[u('wheee'), u('wheee')],
        write=[u('wugga'), u('a'), u('b'), u('wugga')])
    assert a.owner == u('foo')
    assert a.lastupdate == dt(-56)
    assert a.admin == (u('baz'), u('bat'))
    assert a.write == (u('wugga'), u('a'), u('b'))
    assert a.read == (u('wheee'),)


def test_build_fail():
    t = dt(1)
    _build_fail(None, t, None, None, None, ValueError(
        'owner cannot be a value that evaluates to false'))
    _build_fail(u('f'), None, None, None, None, ValueError(
        'lastupdate cannot be a value that evaluates to false'))
    _build_fail(u('f'), datetime.datetime.fromtimestamp(1), None, None, None, ValueError(
        'lastupdate cannot be a naive datetime'))
    _build_fail(u('foo'), t, [u('a'), None], None, None, ValueError(
        'Index 1 of iterable admin cannot be a value that evaluates to false'))
    _build_fail(u('foo'), t, None, [None, None], None, ValueError(
        'Index 0 of iterable write cannot be a value that evaluates to false'))
    _build_fail(u('foo'), t, None, None, [u('a'), u('b'), None], ValueError(
        'Index 2 of iterable read cannot be a value that evaluates to false'))

    # test that you cannot have an owner in another ACL
    _build_fail(
        u('foo'), t, [u('c'), u('d'), u('foo')], None, [u('a'), u('b'), u('x')],
        IllegalParameterError('The owner cannot be in any other ACL'))
    _build_fail(
        u('foo'), t, None, [u('a'), u('b'), u('foo')], [u('x')],
        IllegalParameterError('The owner cannot be in any other ACL'))
    _build_fail(
        u('foo'), t, [u('y')], None, [u('a'), u('b'), u('foo')],
        IllegalParameterError('The owner cannot be in any other ACL'))

    # test that you cannot have a user in 2 acls
    _build_fail(
        u('foo'), t, [u('a'), u('z')], [u('a'), u('c')], [u('w'), u('b')],
        IllegalParameterError('User a appears in two ACLs'))
    _build_fail(
        u('foo'), t, [u('a'), u('z')], [u('b'), u('c')], [u('w'), u('a')],
        IllegalParameterError('User a appears in two ACLs'))
    _build_fail(
        u('foo'), t, [u('x'), u('z')], [u('b'), u('c'), u('w')], [u('w'), u('a')],
        IllegalParameterError('User w appears in two ACLs'))


def _build_fail(owner, lastchanged, admin, write, read, expected):
    with raises(Exception) as got:
        SampleACL(owner, lastchanged, admin, write, read)
    assert_exception_correct(got.value, expected)


def test_eq():
    t = dt(3)
    assert SampleACL(u('foo'), t) == SampleACL(u('foo'), t)
    assert SampleACL(u('foo'), t) != SampleACL(u('bar'), t)
    assert SampleACL(u('foo'), t) != SampleACL(u('foo'), dt(7))

    assert SampleACL(u('foo'), t, [u('bar')]) == SampleACL(u('foo'), t, [u('bar')])
    assert SampleACL(u('foo'), t, [u('bar')]) != SampleACL(u('foo'), t, [u('baz')])

    assert SampleACL(u('foo'), t, write=[u('bar')]) == SampleACL(u('foo'), t, write=[u('bar')])
    assert SampleACL(u('foo'), t, write=[u('bar')]) != SampleACL(u('foo'), t, write=[u('baz')])

    assert SampleACL(u('foo'), t, read=[u('bar')]) == SampleACL(u('foo'), t, read=[u('bar')])
    assert SampleACL(u('foo'), t, read=[u('bar')]) != SampleACL(u('foo'), t, read=[u('baz')])

    assert SampleACL(u('foo'), t) != 1
    assert u('foo') != SampleACL(u('foo'), t)


def test_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__

    t = dt(56)

    assert hash(SampleACL(u('foo'), t)) == hash(SampleACL(u('foo'), t))
    assert hash(SampleACL(u('bar'), dt(5))) == hash(SampleACL(u('bar'), dt(5)))
    assert hash(SampleACL(u('foo'), t)) != hash(SampleACL(u('bar'), t))
    assert hash(SampleACL(u('foo'), t)) != hash(SampleACL(u('foo'), dt(55)))

    assert hash(SampleACL(u('foo'), t, [u('bar')])) == hash(SampleACL(u('foo'), t, [u('bar')]))
    assert hash(SampleACL(u('foo'), t, [u('bar')])) != hash(SampleACL(u('foo'), t, [u('baz')]))

    assert hash(SampleACL(u('foo'), t, write=[u('bar')])) == hash(
        SampleACL(u('foo'), t, write=[u('bar')]))
    assert hash(SampleACL(u('foo'), t, write=[u('bar')])) != hash(
        SampleACL(u('foo'), t, write=[u('baz')]))

    assert hash(SampleACL(u('foo'), t, read=[u('bar')])) == hash(
        SampleACL(u('foo'), t, read=[u('bar')]))
    assert hash(SampleACL(u('foo'), t, read=[u('bar')])) != hash(
        SampleACL(u('foo'), t, read=[u('baz')]))
