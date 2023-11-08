from typing import Any

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_transaction_commit(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"

    message = f"Add file {random_file.name}"

    with fs.transaction as tx:
        fs.put_file(lpath, rpath)
        tx.commit(repository, temp_branch, message=message)

    commits = fs.client.refs_api.log_commits(
        repository=repository,
        ref=temp_branch,
    )
    latest_commit = commits.results[0]
    assert latest_commit.message == message


def test_transaction_tag(fs: LakeFSFileSystem, repository: str) -> None:
    try:
        # tag gets created on exit of the context.
        with fs.transaction as tx:
            tag = tx.tag(repository=repository, ref="main", tag="v2")

        assert any(commit.id == tag for commit in fs.client.tags_api.list_tags(repository).results)
    finally:
        fs.client.tags_api.delete_tag(repository=repository, tag=tag)


def test_transaction_merge(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
    temporary_branch_context: Any,
) -> None:
    random_file = random_file_factory.make()

    with temporary_branch_context("transaction-merge-test") as new_branch:
        resource = f"{repository}/{new_branch}/{random_file.name}"
        message = "Commit new file"

        with fs.transaction as tx:
            # stage a file on new_branch...
            fs.put(str(random_file), resource)
            # ... commit it with the above message
            tx.commit(
                repository=repository,
                branch=new_branch,
                message=message,
            )
            # ... and merge it into temp_branch.
            tx.merge(repository=repository, source_ref=new_branch, into=temp_branch)

        # at last, verify temp_branch@HEAD is the merge commit.
        commits = fs.client.refs_api.log_commits(
            repository=repository,
            ref=temp_branch,
        )
        latest_commit = commits.results[0]
        assert latest_commit.message == f"Merge {new_branch!r} into {temp_branch!r}"
        second_latest_commit = commits.results[1]
        assert second_latest_commit.message == message


def test_transaction_revert(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"

    message = f"Add file {random_file.name}"

    with fs.transaction as tx:
        fs.put_file(lpath, rpath)
        tx.commit(repository, temp_branch, message=message)
        tx.revert(repository=repository, branch=temp_branch)

    commits = fs.client.refs_api.log_commits(
        repository=repository,
        ref=temp_branch,
    )
    latest_commit = commits.results[0]
    assert latest_commit.message == f"Revert {temp_branch}"
