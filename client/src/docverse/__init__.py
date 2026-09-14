"""Docverse client library."""

from importlib.metadata import PackageNotFoundError, version

from ._client import DocverseClient, ProjectList
from ._exceptions import BuildProcessingError, DocverseClientError
from ._tar import create_tarball

__all__ = [
    "BuildProcessingError",
    "DocverseClient",
    "DocverseClientError",
    "ProjectList",
    "__version__",
    "create_tarball",
]

try:
    __version__ = version("docverse")
except PackageNotFoundError:
    __version__ = "0.0.0"
