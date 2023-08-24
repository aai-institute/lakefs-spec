from pathlib import Path

import pytest

from lakefs_spec import LakeFSFileSystem
from tests.conftest import LakeFSOptions


def test_get_nonexistent_file(lakefs_options: LakeFSOptions, repository: str) -> None:
    """
    Regression test against error on file closing in fs.get_file() after a
    lakeFS API exception.
    """
    fs = LakeFSFileSystem(
        host=lakefs_options.host,
        username=lakefs_options.username,
        password=lakefs_options.password,
    )
    rpath = f"{repository}/main/hello-i-no-exist1234.txt"

    with pytest.raises(FileNotFoundError):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()


def test_get_from_nonexistent_repo(lakefs_options: LakeFSOptions) -> None:
    """
    Tests that a FileNotFoundError and not a lakeFS API exception is raised
    when attempting to access a nonexistent repository.
    """
    fs = LakeFSFileSystem(
        host=lakefs_options.host,
        username=lakefs_options.username,
        password=lakefs_options.password,
    )
    rpath = "nonexistent-repo/main/a.txt"

    with pytest.raises(FileNotFoundError):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()


def test_get_from_nonexistent_branch(lakefs_options: LakeFSOptions, repository: str) -> None:
    """
    Tests that a FileNotFoundError and not a lakeFS API exception is raised
    when attempting to access a nonexistent branch in an existing repository.
    """
    fs = LakeFSFileSystem(
        host=lakefs_options.host,
        username=lakefs_options.username,
        password=lakefs_options.password,
    )
    rpath = f"{repository}/nonexistentbranch/a.txt"

    with pytest.raises(FileNotFoundError):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()
