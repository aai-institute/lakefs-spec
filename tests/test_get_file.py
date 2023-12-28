from pathlib import Path

import pytest
from lakefs.branch import Branch
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory, put_random_file_on_branch, with_counter


def test_get_nonexistent_file(fs: LakeFSFileSystem, repository: Repository) -> None:
    """
    Regression test against error on file closing in fs.get_file() after a
    lakeFS API exception.
    """
    rpath = f"{repository.id}/main/hello-i-no-exist1234.txt"

    with pytest.raises(FileNotFoundError, match=rpath):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()


def test_get_from_nonexistent_repo(fs: LakeFSFileSystem) -> None:
    """
    Tests that a FileNotFoundError and not a lakeFS API exception is raised
    when attempting to access a nonexistent repository.
    """
    rpath = "nonexistent-repo/main/a.txt"

    with pytest.raises(FileNotFoundError, match=rpath):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()


def test_get_from_nonexistent_branch(fs: LakeFSFileSystem, repository: Repository) -> None:
    """
    Tests that a FileNotFoundError and not a lakeFS API exception is raised
    when attempting to access a nonexistent branch in an existing repository.
    """
    rpath = f"{repository.id}/nonexistentbranch/a.txt"

    with pytest.raises(FileNotFoundError, match=rpath):
        fs.get(rpath, "out.txt")

    assert not Path("out.txt").exists()


def test_get_client_caching(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """
    Tests that `precheck=True` prevents the download of a previously uploaded identical file.
    """
    fs.client, counter = with_counter(fs.client)

    rpath = put_random_file_on_branch(
        random_file_factory, fs, repository, temp_branch, commit=False
    )
    assert fs.exists(rpath)

    # try to get file, should not initiate a download due to checksum matching.
    lpath = str(random_file_factory.path / Path(rpath).name)
    fs.get(rpath, lpath)
    assert counter.count("objects_api.get_object") == 0
