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
