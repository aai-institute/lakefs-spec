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


def test_info_on_directory_no_trailing_slash(fs: LakeFSFileSystem, repository: str) -> None:
    """
    Regression test to check that calling `fs.info()` on existing non-slash-terminated
    directories yields the same results as if terminated with a slash.
    """
    resource = f"{repository}/main/data/"
    res = fs.info(resource)

    assert res["type"] == "directory"

    non_slash_resource = resource.rstrip("/")
    assert fs.info(non_slash_resource) == res
