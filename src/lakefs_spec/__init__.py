"""lakefs-spec is an fsspec file system integration for the lakeFS data lake."""

from importlib.metadata import PackageNotFoundError, version

from lakefs_sdk import __version__ as __lakefs_sdk_version__

from .spec import LakeFSFile, LakeFSFileSystem

try:
    __version__ = version("lakefs-spec")
except PackageNotFoundError:
    # package is not installed
    pass

lakefs_sdk_version = tuple(int(v) for v in __lakefs_sdk_version__.split("."))
del __lakefs_sdk_version__
