from lakefs_client.client import LakeFSClient

from lakefs_spec.spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_put_with_default_commit_hook(
    random_file_factory: RandomFileFactory,
    lakefs_client: LakeFSClient,
    repository: str,
    temp_branch: str,
) -> None:
    fs = LakeFSFileSystem(client=lakefs_client, postcommit=True)

    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"
    fs.put(lpath, rpath)

    commits = fs.client.commits_api.log_branch_commits(
        repository=repository,
        branch=temp_branch,
    )
    latest_commit = commits.results[0]  # commit log is ordered branch-tip-first
    assert latest_commit.message == f"Add file {random_file.name}"
