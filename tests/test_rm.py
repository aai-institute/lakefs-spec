from lakefs_spec import LakeFSFileSystem


def test_rm(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    path = f"{repository}/{temp_branch}/README.md"

    fs.rm(path)
    assert not fs.exists(path)


def test_rm_with_postcommit(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    path = f"{repository}/{temp_branch}/README.md"
    msg = "Remove file README.md"

    with fs.transaction as tx:
        fs.rm(path)
        tx.commit(repository=repository, branch=temp_branch, message=msg)
    assert not fs.exists(path)

    commits = fs.client.refs_api.log_commits(
        repository=repository,
        ref=temp_branch,
    )
    latest_commit = commits.results[0]
    assert latest_commit.message == msg
