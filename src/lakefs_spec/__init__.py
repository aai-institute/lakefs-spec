from importlib.metadata import PackageNotFoundError, version

from .spec import LakeFSFile, LakeFSFileSystem

try:
    __version__ = version("lakefs-spec")
except PackageNotFoundError:
    # package is not installed
    pass
