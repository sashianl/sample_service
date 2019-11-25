"""
Exceptions thrown by the Sample system.
"""
from enum import Enum


# TODO test

class ErrorType(Enum):
    """
    The type of an error, consisting of an error code and a brief string describing the type.

    :ivar error_code: an integer error code.
    :ivar error_type: a brief string describing the error type.
    """

# These should be handled by the SDK code but keeping them around for future use if we
# add a rest-ish endpoint.
#    AUTHENTICATION_FAILED =  (10000, "Authentication failed")  # noqa: E222 @IgnorePep8
#    """ A general authentication error. """

#    NO_TOKEN =               (10010, "No authentication token")  # noqa: E222 @IgnorePep8
#    """ No token was provided when required. """

#    INVALID_TOKEN =          (10020, "Invalid token")  # noqa: E222 @IgnorePep8
#    """ The token provided is not valid. """

    UNAUTHORIZED =           (20000, "Unauthorized")  # noqa: E222 @IgnorePep8
    """ The user is not authorized to perform the requested action. """

    MISSING_PARAMETER =      (30000, "Missing input parameter")  # noqa: E222 @IgnorePep8
    """ A required input parameter was not provided. """

    ILLEGAL_PARAMETER =      (30001, "Illegal input parameter")  # noqa: E222 @IgnorePep8
    """ An input parameter had an illegal value. """

    NO_SUCH_SAMPLE =         (50000, "No such sample")  # noqa: E222 @IgnorePep8
    """ The requested sample does not exist. """

    UNSUPPORTED_OP =         (60000, "Unsupported operation")  # noqa: E222 @IgnorePep8
    """ The requested operation is not supported. """

    def __init__(self, error_code, error_type):
        self.error_code = error_code
        self.error_type = error_type


class SampleError(Exception):
    """
    The super class of all ID mapping related errors.

    :ivar error_type: the error type of this error.
    :ivar message: the message for this error.
    """

    def __init__(self, error_type: ErrorType, message: str = None) -> None:
        '''
        Create an ID mapping error.

        :param error_type: the error type of this error.
        :param message: an error message.
        :raises TypeError: if error_type is None
        '''
        if not error_type:  # don't use not_falsy here, causes circular import
            raise TypeError('error_type cannot be None')
        et = error_type
        msg = f'Sample service error code {et.error_code} {et.error_type}'
        message = message.strip() if message and message.strip() else None
        if message:
            msg += ': ' + message
        super().__init__(msg)
        self.error_type = error_type
        self.message = message


class NoDataException(SampleError):
    """
    An error thrown when expected data does not exist.
    """

    def __init__(self, error_type: ErrorType, message: str) -> None:
        super().__init__(error_type, message)


class UnauthorizedError(SampleError):
    """
    An error thrown when a user attempts a disallowed action.
    """

    def __init__(self, message: str = None) -> None:
        super().__init__(ErrorType.UNAUTHORIZED, message)


class MissingParameterError(SampleError):
    """
    An error thrown when a required parameter is missing.
    """

    def __init__(self, message: str = None) -> None:
        super().__init__(ErrorType.MISSING_PARAMETER, message)


class IllegalParameterError(SampleError):
    """
    An error thrown when a provided parameter is illegal.
    """

    def __init__(self, message: str = None) -> None:
        super().__init__(ErrorType.ILLEGAL_PARAMETER, message)


class NoSuchSampleError(NoDataException):
    """
    An error thrown when a sample does not exist.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.NO_SUCH_SAMPLE, message)
