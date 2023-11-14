from typing import Any

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory, with_counter


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
        fs.put_file(lpath, rpath, autocommit=False)
        tx.commit(repository, temp_branch, message=message)
        # stack contains the file to upload, and the commit op.
        assert len(tx.files) == 2

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
            fs.put_file(str(random_file), resource, autocommit=False)
            # ... commit it with the above message
            tx.commit(
                repository=repository,
                branch=new_branch,
                message=message,
            )
            # ... and merge it into temp_branch.
            tx.merge(repository=repository, source_ref=new_branch, into=temp_branch)

            # stack contents: file upload, commit, merge to temp.
            assert len(tx.files) == 3

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
        fs.put_file(lpath, rpath, autocommit=False)
        tx.commit(repository, temp_branch, message=message)
        tx.revert(repository=repository, branch=temp_branch)

    commits = fs.client.refs_api.log_commits(
        repository=repository,
        ref=temp_branch,
    )
    latest_commit = commits.results[0]
    assert latest_commit.message == f"Revert {temp_branch}"


def test_transaction_branch(fs: LakeFSFileSystem, repository: str) -> None:
    branch = "new-hello"

    try:
        with fs.transaction as tx:
            tx.create_branch(repository=repository, branch=branch, source_branch="main")

        branches = [
            b.id for b in fs.client.branches_api.list_branches(repository=repository).results
        ]

        # existence check for a newly created branch.
        assert branch in branches

    finally:
        fs.client.branches_api.delete_branch(
            repository=repository,
            branch=branch,
        )


def test_transaction_entry(fs: LakeFSFileSystem) -> None:
    fs.start_transaction()
    assert fs._intrans
    assert fs._transaction is not None


def test_transaction_failure(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"

    message = f"Add file {random_file.name}"

    fs.client, counter = with_counter(fs.client)
    try:
        with fs.transaction as tx:
            fs.put_file(lpath, rpath, autocommit=False)
            tx.commit(repository, temp_branch, message=message)
            raise RuntimeError("something went wrong")
    except RuntimeError:
        pass

    # assert that no commit was attempted because of the exception.
    assert counter.count("commits_api.commit") == 0
