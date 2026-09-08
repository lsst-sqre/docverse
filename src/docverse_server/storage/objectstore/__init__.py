"""Object store abstractions and implementations."""

from ._exceptions import MAX_REPORTED_OBJECT_FAILURES, ObjectStoreError
from ._factory import create_objectstore
from ._mock import MockObjectStore
from ._protocol import ObjectStore
from ._s3 import S3ObjectStore

__all__ = [
    "MAX_REPORTED_OBJECT_FAILURES",
    "MockObjectStore",
    "ObjectStore",
    "ObjectStoreError",
    "S3ObjectStore",
    "create_objectstore",
]
