from pathlib import Path

import pytest

from lakefs_spec import LakeFSFileSystem


def test_get_nonexistent_file(fs: LakeFSFileSystem, repository: str) -> None:
    """
    Regression test against error on file closing in fs.get_file() after a
    lakeFS API exception.
    """
    rpath = f"{repository}/main/hello-i-no-exist1234.txt"

    with pytest.raises(FileNotFoundError):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()


def test_get_from_nonexistent_repo(fs: LakeFSFileSystem) -> None:
    """
    Tests that a FileNotFoundError and not a lakeFS API exception is raised
    when attempting to access a nonexistent repository.
    """
    rpath = "nonexistent-repo/main/a.txt"

    with pytest.raises(FileNotFoundError):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()


def test_get_from_nonexistent_branch(fs: LakeFSFileSystem, repository: str) -> None:
    """
    Tests that a FileNotFoundError and not a lakeFS API exception is raised
    when attempting to access a nonexistent branch in an existing repository.
    """
    rpath = f"{repository}/nonexistentbranch/a.txt"

    with pytest.raises(FileNotFoundError):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()
