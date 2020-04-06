# Not testing the simple errors here, they should get tested as part of the code they get
# thrown in
# Just testing the main error class

from pytest import raises
from SampleService.core.errors import SampleError, ErrorType
from core.test_utils import assert_exception_correct


def test_error_root_no_message():
    e = SampleError(ErrorType.UNSUPPORTED_OP)
    errstr = 'Sample service error code 100000 Unsupported operation'
    assert e.error_type == ErrorType.UNSUPPORTED_OP
    assert e.args == (errstr,)
    assert e.message is None

    for msg in [None, '    \t     ', '']:
        e = SampleError(ErrorType.UNSUPPORTED_OP, msg)
        errstr = 'Sample service error code 100000 Unsupported operation'
        assert e.error_type == ErrorType.UNSUPPORTED_OP
        assert e.args == (errstr,)
        assert e.message is None


def test_error_root_with_message():
    e = SampleError(ErrorType.UNSUPPORTED_OP, '  really important message  \t  ')
    errstr = 'Sample service error code 100000 Unsupported operation: really important message'
    assert e.error_type == ErrorType.UNSUPPORTED_OP
    assert e.args == (errstr,)
    assert e.message == 'really important message'


def test_error_root_no_error_type():
    with raises(Exception) as got:
        SampleError(None)
    assert_exception_correct(
        got.value, TypeError('error_type cannot be None'))
