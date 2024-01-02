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


def test_rm_with_postcommit(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    path = f"{repository.id}/{temp_branch.id}/README.md"
    msg = "Remove file README.md"

    with fs.transaction as tx:
        fs.rm(path)
        tx.commit(repository, temp_branch, message=msg)
    assert not fs.exists(path)

    commits = list(temp_branch.log())
    latest_commit = commits[0]
    assert latest_commit.message == msg


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
