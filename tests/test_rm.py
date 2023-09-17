from lakefs_client.client import LakeFSClient

from lakefs_spec import LakeFSFileSystem
from lakefs_spec.client_helpers import commit
from lakefs_spec.hooks import FSEvent, HookContext


def commit_after_rm(client: LakeFSClient, ctx: HookContext) -> None:
    message = f"Remove file {ctx.resource}"
    commit(client, repository=ctx.repository, branch=ctx.ref, message=message)


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
    fs.register_hook(FSEvent.RM, commit_after_rm)

    path = f"{repository}/{temp_branch}/README.md"

    fs.rm(path)
    assert not fs.exists(path)

    commits = fs.client.refs_api.log_commits(
        repository=repository,
        ref=temp_branch,
    )
    latest_commit = commits.results[0]
    assert latest_commit.message == "Remove file README.md"
