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
    with fs.scope(postcommit=True):
        path = f"{repository}/{temp_branch}/README.md"

        fs.rm(path)
        assert not fs.exists(path)

    commits = fs.client.refs_api.log_commits(
        repository=repository,
        ref=temp_branch,
    )
    latest_commit = commits.results[0]
    assert latest_commit.message == "Remove file README.md"
