import pytest

from lakefs_spec import LakeFSFileSystem


def test_info_on_directory(fs: LakeFSFileSystem, repository: str) -> None:
    """
    Check that an `fs.info` call on a trailing-slash resource yields a directory.
    """
    resource = f"{repository}/main/"
    res = fs.info(resource)

    assert res["type"] == "directory"


def test_info_on_nonexistent_directory(fs: LakeFSFileSystem, repository: str) -> None:
    """
    Check that a nonexistent directory raises a FileNotFoundError in `fs.info`.
    """
    resource = f"{repository}/main/blabla/"

    with pytest.raises(FileNotFoundError):
        fs.info(resource)
