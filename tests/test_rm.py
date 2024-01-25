from lakefs.branch import Branch
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem


def test_rm(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    path = f"{repository.id}/{temp_branch.id}/README.md"

    fs.rm(path)
    assert not fs.exists(path)


def test_rm_with_transaction(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    path = f"{repository.id}/{temp_branch.id}/README.md"
    message = "Remove file README.md"

    with fs.transaction(repository, temp_branch, automerge=True) as tx:
        fs.rm(f"{repository.id}/{tx.branch.id}/README.md")
        tx.commit(message=message)

    commits = list(temp_branch.log(max_amount=2))
    assert not fs.exists(path)
    assert commits[-1].message == message
    assert commits[0].message.startswith("Merge")


def test_rm_recursive(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """Validate that recursive ``rm`` removes subdirectories as well."""
    prefix = f"lakefs://{repository.id}/{temp_branch.id}"

    fs.pipe(f"{prefix}/dir1/b.txt", b"b")
    fs.pipe(f"{prefix}/dir1/dir2/c.txt", b"c")

    fs.rm(f"{prefix}/dir1", recursive=False)
    assert fs.exists(f"{prefix}/dir1/dir2/c.txt")
    fs.rm(f"{prefix}/dir1", recursive=True)
    assert not fs.exists(f"{prefix}/dir1/dir2/c.txt")


def test_rm_recursive_with_maxdepth(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """
    Check that recursive ``rm`` with maxdepth leaves directories beyond maxdepth untouched.
    """
    prefix = f"lakefs://{repository.id}/{temp_branch.id}"

    fs.pipe(f"{prefix}/dir1/b.txt", b"b")
    fs.pipe(f"{prefix}/dir1/dir2/c.txt", b"c")

    fs.rm(f"{prefix}/dir1", recursive=True, maxdepth=1)
    # maxdepth is 1-indexed, level 1 being the directory to be removed.
    assert fs.exists(f"{prefix}/dir1/dir2/c.txt")
