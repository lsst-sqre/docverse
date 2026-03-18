"""Docverse client library."""

from importlib.metadata import PackageNotFoundError, version

from ._client import DocverseClient
from ._exceptions import BuildProcessingError, DocverseClientError
from ._tar import create_tarball

__all__ = [
    "BuildProcessingError",
    "DocverseClient",
    "DocverseClientError",
    "__version__",
    "create_tarball",
]

try:
    __version__ = version("docverse-client")
except PackageNotFoundError:
    __version__ = "0.0.0"
