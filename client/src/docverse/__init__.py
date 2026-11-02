"""Docverse client library."""

from importlib.metadata import PackageNotFoundError, version

from ._client import DEFAULT_UPDATED_SINCE_OVERLAP, DocverseClient, ProjectList
from ._exceptions import BuildProcessingError, DocverseClientError
from ._tar import Tarball, create_tarball
from .excluded_paths import EXCLUDED_PATH_NAMES, is_excluded_path

__all__ = [
    "DEFAULT_UPDATED_SINCE_OVERLAP",
    "EXCLUDED_PATH_NAMES",
    "BuildProcessingError",
    "DocverseClient",
    "DocverseClientError",
    "ProjectList",
    "Tarball",
    "__version__",
    "create_tarball",
    "is_excluded_path",
]

try:
    __version__ = version("docverse")
except PackageNotFoundError:
    __version__ = "0.0.0"
