from pytest import raises

from core.test_utils import assert_exception_correct

from SampleService.core.util import not_falsy


def test_falsy_true():
    for t in ['a', 1, True, [1], {'a': 1}, {1}]:
        assert not_falsy(t, 'foo') is t


def test_falsy_fail():
    for f in ['', 0, False, [], dict(), {}]:
        with raises(Exception) as got:
            not_falsy(f, 'my name')
        assert_exception_correct(
            got.value, ValueError('my name cannot be a value that evaluates to false'))
