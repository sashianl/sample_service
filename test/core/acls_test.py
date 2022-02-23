import datetime

from pytest import raises

from SampleService.core.acls import SampleACL, SampleACLOwnerless, SampleACLDelta
from core.test_utils import assert_exception_correct
from SampleService.core.errors import IllegalParameterError, UnauthorizedError
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


def test_is_update():
    s = SampleACL(
        u('o'),
        dt(1),
        [u('a1'), u('a2')],
        [u('w1'), u('w2')],
        [u('r1'), u('r2')],
        True)

    assert s.is_update(SampleACLDelta()) is False

    assert s.is_update(SampleACLDelta([u('a1')])) is False
    assert s.is_update(SampleACLDelta([u('a1')], at_least=True)) is False
    assert s.is_update(SampleACLDelta([u('o')], at_least=True)) is False
    assert s.is_update(SampleACLDelta([u('a3')])) is True

    assert s.is_update(SampleACLDelta(write=[u('w2')])) is False
    assert s.is_update(SampleACLDelta(write=[u('w2')], at_least=True)) is False
    assert s.is_update(SampleACLDelta(write=[u('o')], at_least=True)) is False
    assert s.is_update(SampleACLDelta(write=[u('a1')])) is True
    assert s.is_update(SampleACLDelta(write=[u('a1')], at_least=True)) is False
    assert s.is_update(SampleACLDelta(write=[u('w4')])) is True

    assert s.is_update(SampleACLDelta(read=[u('r1')])) is False
    assert s.is_update(SampleACLDelta(read=[u('r1')], at_least=True)) is False
    assert s.is_update(SampleACLDelta(read=[u('o')], at_least=True)) is False
    assert s.is_update(SampleACLDelta(read=[u('a1')])) is True
    assert s.is_update(SampleACLDelta(read=[u('a1')], at_least=True)) is False
    assert s.is_update(SampleACLDelta(read=[u('w1')])) is True
    assert s.is_update(SampleACLDelta(read=[u('w1')], at_least=True)) is False
    assert s.is_update(SampleACLDelta(read=[u('r3')])) is True

    assert s.is_update(SampleACLDelta(remove=[u('a1')])) is True
    assert s.is_update(SampleACLDelta(remove=[u('a1')], at_least=True)) is True
    assert s.is_update(SampleACLDelta(remove=[u('a3')])) is False

    assert s.is_update(SampleACLDelta(remove=[u('w2')])) is True
    assert s.is_update(SampleACLDelta(remove=[u('w2')], at_least=True)) is True
    assert s.is_update(SampleACLDelta(remove=[u('w4')])) is False

    assert s.is_update(SampleACLDelta(remove=[u('r1')])) is True
    assert s.is_update(SampleACLDelta(remove=[u('r1')], at_least=True)) is True
    assert s.is_update(SampleACLDelta(remove=[u('r3')])) is False

    assert s.is_update(SampleACLDelta(public_read=False)) is True
    assert s.is_update(SampleACLDelta(public_read=None)) is False
    assert s.is_update(SampleACLDelta(public_read=True)) is False


def test_is_update_fail():
    s = SampleACL(u('u'), dt(1))

    _is_update_fail(s, None, ValueError('update cannot be a value that evaluates to false'))
    _is_update_fail(
        s, SampleACLDelta([u('a'), u('u')], [u('v')]),
        UnauthorizedError('ACLs for the sample owner u may not be modified by a delta update.'))
    _is_update_fail(
        s, SampleACLDelta([u('a')], write=[u('v'), u('u')]),
        UnauthorizedError('ACLs for the sample owner u may not be modified by a delta update.'))
    _is_update_fail(
        s, SampleACLDelta([u('a')], read=[u('v'), u('u')]),
        UnauthorizedError('ACLs for the sample owner u may not be modified by a delta update.'))
    _is_update_fail(
        s, SampleACLDelta([u('a')], remove=[u('v'), u('u')]),
        UnauthorizedError('ACLs for the sample owner u may not be modified by a delta update.'))
    _is_update_fail(
        s, SampleACLDelta([u('a')], remove=[u('v'), u('u')], at_least=True),
        UnauthorizedError('ACLs for the sample owner u may not be modified by a delta update.'))


def _is_update_fail(sample, delta, expected):
    with raises(Exception) as got:
        sample.is_update(delta)
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
    a = SampleACLDelta()
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()
    assert a.remove == ()
    assert a.public_read is None
    assert a.at_least is False

    a = SampleACLDelta(
        [u('baz'), u('bat'), u('baz')],
        read=[u('wheee'), u('wheee')],
        write=[u('wugga'), u('a'), u('b'), u('wugga')],
        remove=[u('bleah'), u('ffs'), u('c')],
        public_read=True,
        at_least=True)
    assert a.admin == (u('bat'), u('baz'))
    assert a.write == (u('a'), u('b'), u('wugga'))
    assert a.read == (u('wheee'),)
    assert a.remove == (u('bleah'), u('c'), u('ffs'))
    assert a.public_read is True
    assert a.at_least is True

    a = SampleACLDelta(public_read=False, at_least=None)
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()
    assert a.remove == ()
    assert a.public_read is False
    assert a.at_least is False

    a = SampleACLDelta(public_read=None)
    assert a.admin == ()
    assert a.write == ()
    assert a.read == ()
    assert a.remove == ()
    assert a.public_read is None
    assert a.at_least is False


def test_delta_build_fail():
    _build_delta_fail([u('a'), None], None, None, None, ValueError(
        'Index 1 of iterable admin cannot be a value that evaluates to false'))
    _build_delta_fail(None, [None, None], None, None, ValueError(
        'Index 0 of iterable write cannot be a value that evaluates to false'))
    _build_delta_fail(None, None, [u('a'), u('b'), None], None, ValueError(
        'Index 2 of iterable read cannot be a value that evaluates to false'))
    _build_delta_fail(None, None, None, [None], ValueError(
        'Index 0 of iterable remove cannot be a value that evaluates to false'))

    # test that you cannot have a user in 2 acls
    _build_delta_fail(
        [u('a'), u('z')], [u('a'), u('c')], [u('w'), u('b')], None,
        IllegalParameterError('User a appears in two ACLs'))
    _build_delta_fail(
        [u('a'), u('z')], [u('b'), u('c')], [u('w'), u('a')], None,
        IllegalParameterError('User a appears in two ACLs'))
    _build_delta_fail(
        [u('x'), u('z')], [u('b'), u('c'), u('w')], [u('w'), u('a')], None,
        IllegalParameterError('User w appears in two ACLs'))

    # test that you cannot have a user in the remove list and an acl
    _build_delta_fail(
        [u('f'), u('z')], [u('b'), u('c'), u('g')], [u('w'), u('a')], [u('m'), u('f')],
        IllegalParameterError('Users in the remove list cannot be in any other ACL'))
    _build_delta_fail(
        [u('a'), u('z')], [u('x'), u('c')], [u('w'), u('b')], [u('m'), u('x')],
        IllegalParameterError('Users in the remove list cannot be in any other ACL'))
    _build_delta_fail(
        [u('a'), u('z')], [u('b'), u('c')], [u('w'), u('y')], [u('y')],
        IllegalParameterError('Users in the remove list cannot be in any other ACL'))


def _build_delta_fail(admin, write, read, remove, expected):
    with raises(Exception) as got:
        SampleACLDelta(admin, write, read, remove)
    assert_exception_correct(got.value, expected)


def test_delta_eq():
    assert SampleACLDelta() == SampleACLDelta()

    assert SampleACLDelta([u('bar')]) == SampleACLDelta([u('bar')])
    assert SampleACLDelta([u('bar')]) != SampleACLDelta([u('baz')])

    assert SampleACLDelta(write=[u('bar')]) == SampleACLDelta(write=[u('bar')])
    assert SampleACLDelta(write=[u('bar')]) != SampleACLDelta(write=[u('baz')])

    assert SampleACLDelta(read=[u('bar')]) == SampleACLDelta(read=[u('bar')])
    assert SampleACLDelta(read=[u('bar')]) != SampleACLDelta(read=[u('baz')])

    assert SampleACLDelta(remove=[u('bar')]) == SampleACLDelta(remove=[u('bar')])
    assert SampleACLDelta(remove=[u('bar')]) != SampleACLDelta(remove=[u('baz')])

    assert SampleACLDelta(public_read=True) == SampleACLDelta(public_read=True)
    assert SampleACLDelta(public_read=True) != SampleACLDelta(public_read=False)

    assert SampleACLDelta(at_least=True) == SampleACLDelta(at_least=True)
    assert SampleACLDelta(at_least=True) != SampleACLDelta(at_least=False)

    assert SampleACLDelta() != 1
    assert [] != SampleACLDelta()


def test_delta_hash():
    # hashes will change from instance to instance of the python interpreter, and therefore
    # tests can't be written that directly test the hash value. See
    # https://docs.python.org/3/reference/datamodel.html#object.__hash__

    assert hash(SampleACLDelta()) == hash(SampleACLDelta())

    assert hash(SampleACLDelta([u('bar')])) == hash(SampleACLDelta([u('bar')]))
    assert hash(SampleACLDelta([u('bar')])) != hash(SampleACLDelta([u('baz')]))

    assert hash(SampleACLDelta(write=[u('bar')])) == hash(SampleACLDelta(write=[u('bar')]))
    assert hash(SampleACLDelta(write=[u('bar')])) != hash(SampleACLDelta(write=[u('baz')]))

    assert hash(SampleACLDelta(read=[u('bar')])) == hash(SampleACLDelta(read=[u('bar')]))
    assert hash(SampleACLDelta(read=[u('bar')])) != hash(SampleACLDelta(read=[u('baz')]))

    assert hash(SampleACLDelta(remove=[u('bar')])) == hash(SampleACLDelta(remove=[u('bar')]))
    assert hash(SampleACLDelta(remove=[u('bar')])) != hash(SampleACLDelta(remove=[u('baz')]))

    assert hash(SampleACLDelta(public_read=True)) == hash(SampleACLDelta(public_read=True))
    assert hash(SampleACLDelta(public_read=True)) != hash(SampleACLDelta(public_read=False))

    assert hash(SampleACLDelta(at_least=True)) == hash(SampleACLDelta(at_least=True))
    assert hash(SampleACLDelta(at_least=True)) != hash(SampleACLDelta(at_least=False))
