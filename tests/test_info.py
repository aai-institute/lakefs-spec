import lakefs
import pytest
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem


def test_info_on_directory(fs: LakeFSFileSystem, repository: Repository) -> None:
    """
    Check that an `fs.info` call on a trailing-slash resource yields a directory.
    """
    resource = f"{repository.id}/main/"
    res = fs.info(resource)

    assert res["type"] == "directory"


def test_info_on_nonexistent_directory(fs: LakeFSFileSystem, repository: Repository) -> None:
    """
    Check that a nonexistent directory raises a FileNotFoundError in `fs.info`.
    """
    resource = f"{repository.id}/main/blabla/"

    with pytest.raises(FileNotFoundError):
        fs.info(resource)


def test_info_on_directory_no_trailing_slash(fs: LakeFSFileSystem, repository: Repository) -> None:
    """
    Regression test to check that calling `fs.info()` on existing non-slash-terminated
    directories yields the same results as if terminated with a slash.
    """
    resource = f"{repository.id}/main/data/"
    res = fs.info(resource)

    assert res["type"] == "directory"

    non_slash_resource = resource.rstrip("/")
    assert fs.info(non_slash_resource) == res


def test_info_on_commit(
    fs: LakeFSFileSystem,
    repository: Repository,
) -> None:
    prefix = f"lakefs://{repository.id}"

    head = lakefs.Branch(repository.id, "main", client=fs.client).head

    binfo = fs.info(f"{prefix}/main/README.md")
    assert binfo["type"] == "file"

    branch_metadata = (binfo["checksum"], binfo["mtime"], binfo["size"])

    # fetching directly from commit should yield the same result.
    cinfo = fs.info(f"{prefix}/{head.id}/README.md")
    assert cinfo["type"] == "file"

    commit_metadata = (cinfo["checksum"], cinfo["mtime"], cinfo["size"])

    assert branch_metadata == commit_metadata
