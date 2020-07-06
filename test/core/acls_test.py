import datetime

from pytest import raises

from SampleService.core.acls import SampleACL, SampleACLOwnerless, SampleACLDelta
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
    assert a.public_read is False

    # test duplicates are removed and order maintained
    a = SampleACLOwnerless(
        [u('baz'), u('baz')],
        read=[u('wheee'), u('wheee'), u('c')],
        write=[u('wugga'), u('a'), u('b'), u('a')],
        public_read=True)
    assert a.admin == (u('baz'),)
    assert a.write == (u('a'), u('b'), u('wugga'))
    assert a.read == (u('c'), u('wheee'))
    assert a.public_read is True

    # test None input for public read
    a = SampleACLOwnerless(public_read=None)
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()
    assert a.public_read is False


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

    assert SampleACLOwnerless(public_read=True) == SampleACLOwnerless(public_read=True)
    assert SampleACLOwnerless(public_read=True) != SampleACLOwnerless(public_read=False)

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

    assert hash(SampleACLOwnerless(public_read=True)) == hash(SampleACLOwnerless(public_read=True))
    assert hash(SampleACLOwnerless(public_read=True)) != hash(SampleACLOwnerless(public_read=False))


def test_build():
    a = SampleACL(u('foo'), dt(30))
    assert a.owner == u('foo')
    assert a.lastupdate == dt(30)
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()
    assert a.public_read is False

    a = SampleACL(
        u('foo'),
        dt(-56),
        [u('baz'), u('bat'), u('baz')],
        read=[u('wheee'), u('wheee')],
        write=[u('wugga'), u('a'), u('b'), u('wugga')],
        public_read=True)
    assert a.owner == u('foo')
    assert a.lastupdate == dt(-56)
    assert a.admin == (u('bat'), u('baz'))
    assert a.write == (u('a'), u('b'), u('wugga'))
    assert a.read == (u('wheee'),)
    assert a.public_read is True

    a = SampleACL(u('foo'), dt(30), public_read=None)
    assert a.owner == u('foo')
    assert a.lastupdate == dt(30)
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()
    assert a.public_read is False


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

    assert SampleACL(u('foo'), t, public_read=True) == SampleACL(u('foo'), t, public_read=True)
    assert SampleACL(u('foo'), t, public_read=True) != SampleACL(u('foo'), t, public_read=False)

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

    assert hash(SampleACL(u('foo'), t, public_read=True)) == hash(
        SampleACL(u('foo'), t, public_read=True))
    assert hash(SampleACL(u('foo'), t, public_read=True)) != hash(
        SampleACL(u('foo'), t, public_read=False))


def test_delta_build():
    a = SampleACLDelta(dt(30))
    assert a.lastupdate == dt(30)
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()
    assert a.remove == ()
    assert a.public_read is None

    a = SampleACLDelta(
        dt(-56),
        [u('baz'), u('bat'), u('baz')],
        read=[u('wheee'), u('wheee')],
        write=[u('wugga'), u('a'), u('b'), u('wugga')],
        remove=[u('bleah'), u('ffs'), u('c')],
        public_read=True)
    assert a.lastupdate == dt(-56)
    assert a.admin == (u('bat'), u('baz'))
    assert a.write == (u('a'), u('b'), u('wugga'))
    assert a.read == (u('wheee'),)
    assert a.remove == (u('bleah'), u('c'), u('ffs'))
    assert a.public_read is True

    a = SampleACLDelta(dt(30), public_read=False)
    assert a.lastupdate == dt(30)
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()
    assert a.remove == ()
    assert a.public_read is False

    a = SampleACLDelta(dt(30), public_read=None)
    assert a.lastupdate == dt(30)
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()
    assert a.remove == ()
    assert a.public_read is None


def test_delta_build_fail():
    t = dt(1)
    _build_delta_fail(None, None, None, None, None, ValueError(
        'lastupdate cannot be a value that evaluates to false'))
    _build_delta_fail(datetime.datetime.fromtimestamp(1), None, None, None, None, ValueError(
        'lastupdate cannot be a naive datetime'))
    _build_delta_fail(t, [u('a'), None], None, None, None, ValueError(
        'Index 1 of iterable admin cannot be a value that evaluates to false'))
    _build_delta_fail(t, None, [None, None], None, None, ValueError(
        'Index 0 of iterable write cannot be a value that evaluates to false'))
    _build_delta_fail(t, None, None, [u('a'), u('b'), None], None, ValueError(
        'Index 2 of iterable read cannot be a value that evaluates to false'))
    _build_delta_fail(t, None, None, None, [None], ValueError(
        'Index 0 of iterable remove cannot be a value that evaluates to false'))

    # test that you cannot have a user in 2 acls
    _build_delta_fail(
        t, [u('a'), u('z')], [u('a'), u('c')], [u('w'), u('b')], None,
        IllegalParameterError('User a appears in two ACLs'))
    _build_delta_fail(
        t, [u('a'), u('z')], [u('b'), u('c')], [u('w'), u('a')], None,
        IllegalParameterError('User a appears in two ACLs'))
    _build_delta_fail(
        t, [u('x'), u('z')], [u('b'), u('c'), u('w')], [u('w'), u('a')], None,
        IllegalParameterError('User w appears in two ACLs'))

    # test that you cannot have a user in the remove list and an acl
    _build_delta_fail(
        t, [u('f'), u('z')], [u('b'), u('c'), u('g')], [u('w'), u('a')], [u('m'), u('f')],
        IllegalParameterError('Users in the remove list cannot be in any other ACL'))
    _build_delta_fail(
        t, [u('a'), u('z')], [u('x'), u('c')], [u('w'), u('b')], [u('m'), u('x')],
        IllegalParameterError('Users in the remove list cannot be in any other ACL'))
    _build_delta_fail(
        t, [u('a'), u('z')], [u('b'), u('c')], [u('w'), u('y')], [u('y')],
        IllegalParameterError('Users in the remove list cannot be in any other ACL'))


def _build_delta_fail(lastchanged, admin, write, read, remove, expected):
    with raises(Exception) as got:
        SampleACLDelta(lastchanged, admin, write, read, remove)
    assert_exception_correct(got.value, expected)


def test_delta_eq():
    t = dt(3)
    assert SampleACLDelta(t) == SampleACLDelta(t)
    assert SampleACLDelta(t) != SampleACLDelta(dt(7))

    assert SampleACLDelta(t, [u('bar')]) == SampleACLDelta(t, [u('bar')])
    assert SampleACLDelta(t, [u('bar')]) != SampleACLDelta(t, [u('baz')])

    assert SampleACLDelta(t, write=[u('bar')]) == SampleACLDelta(t, write=[u('bar')])
    assert SampleACLDelta(t, write=[u('bar')]) != SampleACLDelta(t, write=[u('baz')])

    assert SampleACLDelta(t, read=[u('bar')]) == SampleACLDelta(t, read=[u('bar')])
    assert SampleACLDelta(t, read=[u('bar')]) != SampleACLDelta(t, read=[u('baz')])

    assert SampleACLDelta(t, remove=[u('bar')]) == SampleACLDelta(t, remove=[u('bar')])
    assert SampleACLDelta(t, remove=[u('bar')]) != SampleACLDelta(t, remove=[u('baz')])

    assert SampleACLDelta(t, public_read=True) == SampleACLDelta(t, public_read=True)
    assert SampleACLDelta(t, public_read=True) != SampleACLDelta(t, public_read=False)

    assert SampleACLDelta(t) != 1
    assert t != SampleACLDelta(t)


def test_delta_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__

    t = dt(56)

    assert hash(SampleACLDelta(t)) == hash(SampleACLDelta(t))
    assert hash(SampleACLDelta(t)) != hash(SampleACLDelta(dt(55)))

    assert hash(SampleACLDelta(t, [u('bar')])) == hash(SampleACLDelta(t, [u('bar')]))
    assert hash(SampleACLDelta(t, [u('bar')])) != hash(SampleACLDelta(t, [u('baz')]))

    assert hash(SampleACLDelta(t, write=[u('bar')])) == hash(
        SampleACLDelta(t, write=[u('bar')]))
    assert hash(SampleACLDelta(t, write=[u('bar')])) != hash(
        SampleACLDelta(t, write=[u('baz')]))

    assert hash(SampleACLDelta(t, read=[u('bar')])) == hash(
        SampleACLDelta(t, read=[u('bar')]))
    assert hash(SampleACLDelta(t, read=[u('bar')])) != hash(
        SampleACLDelta(t, read=[u('baz')]))

    assert hash(SampleACLDelta(t, remove=[u('bar')])) == hash(
        SampleACLDelta(t, remove=[u('bar')]))
    assert hash(SampleACLDelta(t, remove=[u('bar')])) != hash(
        SampleACLDelta(t, remove=[u('baz')]))

    assert hash(SampleACLDelta(t, public_read=True)) == hash(
        SampleACLDelta(t, public_read=True))
    assert hash(SampleACLDelta(t, public_read=True)) != hash(
        SampleACLDelta(t, public_read=False))
