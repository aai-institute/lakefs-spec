from importlib.metadata import PackageNotFoundError, version

from lakefs_client import __version__ as __lakefs_client_version__

from .spec import LakeFSFile, LakeFSFileSystem

try:
    __version__ = version("lakefs-spec")
except PackageNotFoundError:
    # package is not installed
    pass

lakefs_client_version = tuple(int(v) for v in __lakefs_client_version__.split("."))
del __lakefs_client_version__
