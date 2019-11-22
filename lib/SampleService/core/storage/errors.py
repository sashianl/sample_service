"""
Exceptions for storage events.
"""


class SampleStorageError(Exception):
    """
    Superclass of all storage related exceptions. Denotes a general storage error.
    """


class StorageInitException(SampleStorageError):
    """
    Denotes an error during initialization of the storage system.
    """


class SampleExistsException(SampleStorageError):
    """
    Thrown when a sample already exists and saving new sample with the same ID is attempted.
    """
