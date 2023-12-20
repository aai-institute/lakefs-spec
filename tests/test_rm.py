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
