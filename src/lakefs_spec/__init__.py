"""lakefs-spec is an fsspec file system integration for the lakeFS data lake."""

from importlib.metadata import PackageNotFoundError, version

from .spec import LakeFSFile, LakeFSFileSystem
from .transaction import LakeFSTransaction

try:
    __version__ = version("lakefs-spec")
except PackageNotFoundError:
    # package is not installed
    pass
