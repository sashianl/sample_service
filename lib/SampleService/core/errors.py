"""
Exceptions thrown by the Sample system.
"""
from enum import Enum
from typing import Optional


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

    METADATA_VALIDATION =    (30010, "Metadata validation failed")  # noqa: E222 @IgnorePep8
    """ Metadata failed validation. """

    SAMPLE_CONCURRENCY =     (40000, "Concurrency violation")  # noqa: E222 @IgnorePep8
    """ A concurrency check failed and the operation could not continue. """

    NO_SUCH_USER =           (50000, "No such user")  # noqa: E222 @IgnorePep8
    """ The requested user does not exist. """

    NO_SUCH_SAMPLE =         (50010, "No such sample")  # noqa: E222 @IgnorePep8
    """ The requested sample does not exist. """

    NO_SUCH_SAMPLE_VERSION = (50020, "No such sample version")  # noqa: E222 @IgnorePep8
    """ The requested sample version does not exist. """

    NO_SUCH_SAMPLE_NODE =    (50030, "No such sample node")  # noqa: E222 @IgnorePep8
    """ The requested sample node does not exist. """

    NO_SUCH_WORKSPACE_DATA = (50040, "No such workspace data")  # noqa: E222 @IgnorePep8
    """ The requested workspace or workspace data does not exist. """

    NO_SUCH_DATA_LINK =      (50050, "No such data link")  # noqa: E222 @IgnorePep8
    """ The requested data link does not exist. """

    DATA_LINK_EXISTS =       (60000, "Data link exists for data ID")  # noqa: E222 @IgnorePep8
    """ A link from the provided data ID already exists. """

    TOO_MANY_DATA_LINKS =    (60010, "Too many data links")  # noqa: E222 @IgnorePep8
    """ Too many links from the sample version or workspace object version already exist. """

    UNSUPPORTED_OP =         (100000, "Unsupported operation")  # noqa: E222 @IgnorePep8
    """ The requested operation is not supported. """

    def __init__(self, error_code, error_type):
        self.error_code = error_code
        self.error_type = error_type


class SampleError(Exception):
    """
    The super class of all Sample related errors.

    :ivar error_type: the error type of this error.
    :ivar message: the message for this error.
    """

    def __init__(self, error_type: ErrorType, message: Optional[str] = None) -> None:
        '''
        Create a Sample error.

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
        self.message: Optional[str] = message


class NoDataException(SampleError):
    """
    An error thrown when expected data does not exist.
    """

    def __init__(self, error_type: ErrorType, message: str) -> None:
        super().__init__(error_type, message)


class NoSuchUserError(NoDataException):
    """
    An error thrown when a user does not exist.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.NO_SUCH_USER, message)


class NoSuchWorkspaceDataError(NoDataException):
    """
    An error thrown when a workspace or workspace data does not exist.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.NO_SUCH_WORKSPACE_DATA, message)


class NoSuchLinkError(NoDataException):
    """
    An error thrown when a data link does not exist.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.NO_SUCH_DATA_LINK, message)


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


class MetadataValidationError(SampleError):
    """
    An error thrown when metadata fails to validate.
    """

    def __init__(self, message: str = None) -> None:
        super().__init__(ErrorType.METADATA_VALIDATION, message)


class NoSuchSampleError(NoDataException):
    """
    An error thrown when a sample does not exist.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.NO_SUCH_SAMPLE, message)


class NoSuchSampleVersionError(NoDataException):
    """
    An error thrown when a sample version does not exist.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.NO_SUCH_SAMPLE_VERSION, message)


class NoSuchSampleNodeError(NoDataException):
    """
    An error thrown when a sample node does not exist.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.NO_SUCH_SAMPLE_NODE, message)


class ConcurrencyError(SampleError):
    """
    An error thrown when a concurrency check fails.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.SAMPLE_CONCURRENCY, message)


class DataLinkExistsError(SampleError):
    """
    An error thrown when a data link for a given data ID already exists.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.DATA_LINK_EXISTS, message)


class TooManyDataLinksError(SampleError):
    """
    An error thrown when too many data links exists for a sample version or workspace object.
    """

    def __init__(self, message: str) -> None:
        super().__init__(ErrorType.TOO_MANY_DATA_LINKS, message)
