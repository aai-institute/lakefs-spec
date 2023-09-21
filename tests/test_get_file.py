from pathlib import Path

import pytest

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory, with_counter


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


def test_get_client_caching(
    random_file_factory: RandomFileFactory, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    """
    Tests that `precheck=True` prevents the download of a previously uploaded identical file.
    """
    fs.client, counter = with_counter(fs.client)

    random_file = random_file_factory.make()
    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"
    fs.put(lpath, rpath)
    assert fs.exists(rpath)

    # try to get file, should not initiate a download due to checksum matching.
    fs.get(rpath, lpath)
    assert counter.count("objects_api.upload_object") == 1
    assert counter.count("objects_api.get_object") == 0
