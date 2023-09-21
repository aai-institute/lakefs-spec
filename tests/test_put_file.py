import random
import string

import pytest
from lakefs_client.client import LakeFSClient

from lakefs_spec import LakeFSFileSystem
from lakefs_spec.client_helpers import commit
from lakefs_spec.hooks import FSEvent, HookContext
from tests.util import RandomFileFactory, with_counter


def commit_after_put(client: LakeFSClient, ctx: HookContext) -> None:
    message = f"Add file {ctx.resource}"
    commit(client, repository=ctx.repository, branch=ctx.ref, message=message)


def test_put_with_commit_helper(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    fs.register_hook(FSEvent.PUT, commit_after_put)
    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"
    fs.put(lpath, rpath)

    commits = fs.client.refs_api.log_commits(
        repository=repository,
        ref=temp_branch,
    )
    latest_commit = commits.results[0]  # commit log is ordered branch-tip-first
    assert latest_commit.message == f"Add file {random_file.name}"


def test_no_change_postcommit(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    fs.register_hook(FSEvent.PUT, commit_after_put)

    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"
    fs.put(lpath, rpath, precheck=False)

    commits = fs.client.refs_api.log_commits(
        repository=repository,
        ref=temp_branch,
    )
    latest_commit = commits.results[0]  # commit log is ordered branch-tip-first
    assert latest_commit.message == f"Add file {random_file.name}"

    # put the same file again, this time the diff is empty
    fs.put(lpath, rpath, precheck=False)
    # check that no other commit has happened.
    commits = fs.client.refs_api.log_commits(
        repository=repository,
        ref=temp_branch,
    )
    assert commits.results[0] == latest_commit


def test_implicit_branch_creation(
    random_file_factory: RandomFileFactory, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    fs.register_hook(FSEvent.PUT, commit_after_put)

    random_file = random_file_factory.make()
    lpath = str(random_file)

    with fs.scope(create_branch_ok=True):
        temp_branch_commits = fs.client.refs_api.log_commits(
            repository=repository,
            ref=temp_branch,
        )
        n_commits = len(temp_branch_commits.results)

        non_existing_branch = "non-existing-" + "".join(random.choices(string.digits, k=8))
        rpath = f"{repository}/{non_existing_branch}/{random_file.name}"
        try:
            fs.put(lpath, rpath)
            commits = fs.client.refs_api.log_commits(
                repository=repository,
                ref=non_existing_branch,
            )
            latest_commit = commits.results[0]
            assert latest_commit.message == f"Add file {random_file.name}"
            assert len(commits.results) == n_commits + 1
        finally:
            fs.client.branches_api.delete_branch(
                repository=repository,
                branch=non_existing_branch,
            )

    with fs.scope(create_branch_ok=False):
        another_non_existing_branch = "non-existing-" + "".join(random.choices(string.digits, k=8))
        rpath = f"{repository}/{another_non_existing_branch}/{random_file.name}"
        with pytest.raises(FileNotFoundError):
            fs.put(lpath, rpath)


def test_put_client_caching(
    random_file_factory: RandomFileFactory, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    """
    Tests that `precheck=True` prevents a second upload of an identical file by matching checksums.
    """
    fs.client, counter = with_counter(fs.client)

    random_file = random_file_factory.make()
    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"
    fs.put(lpath, rpath)
    assert fs.exists(rpath)

    # second put, should not happen.
    fs.put(lpath, rpath)
    print(list(counter.named_counts()))
    assert counter.count("objects_api.upload_object") == 1
