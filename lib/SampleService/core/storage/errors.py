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


class OwnerChangedException(SampleStorageError):
    """
    Denotes that the owner designated by a save operation is not the same as the owner in the
    database - e.g. the owner has changed since the ACLs were last pulled from the database.
    This error generally denotes a race condition has occurred.
    """
