from pathlib import Path

import pytest
from lakefs_client.client import LakeFSClient

from lakefs_spec.spec import LakeFSFileSystem


def test_get_nonexistent_file(lakefs_client: LakeFSClient, repository: str) -> None:
    """
    Regression test against error on file closing in fs.get_file() after a
    lakeFS API exception.
    """
    fs = LakeFSFileSystem(client=lakefs_client)
    rpath = f"{repository}/main/hello-i-no-exist1234.txt"

    with pytest.raises(FileNotFoundError):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()


def test_get_from_nonexistent_repo(lakefs_client: LakeFSClient) -> None:
    """
    Tests that a FileNotFoundError and not a lakeFS API exception is raised
    when attempting to access a nonexistent repository.
    """
    fs = LakeFSFileSystem(client=lakefs_client)
    rpath = "nonexistent-repo/main/a.txt"

    with pytest.raises(FileNotFoundError):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()


def test_get_from_nonexistent_branch(lakefs_client: LakeFSClient, repository: str) -> None:
    """
    Tests that a FileNotFoundError and not a lakeFS API exception is raised
    when attempting to access a nonexistent branch in an existing repository.
    """
    fs = LakeFSFileSystem(client=lakefs_client)
    rpath = f"{repository}/nonexistentbranch/a.txt"

    with pytest.raises(FileNotFoundError):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()
