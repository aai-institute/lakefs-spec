from pathlib import Path
from typing import Any

import pytest
from lakefs.branch import Branch
from lakefs.reference import Commit, Reference
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem
from lakefs_spec.transaction import Placeholder
from tests.util import RandomFileFactory, put_random_file_on_branch, with_counter


def test_transaction_commit(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository.id}/{temp_branch.id}/{random_file.name}"

    message = f"Add file {random_file.name}"

    with fs.transaction as tx:
        fs.put_file(lpath, rpath)
        assert len(tx.files) == 1
        # sha is a placeholder for the actual SHA created on transaction completion.
        sha = tx.commit(repository, temp_branch, message=message)
        # stack contains the file to upload, and the commit op.
        assert len(tx.files) == 2
        assert not sha.available

    assert sha.available

    commits = list(temp_branch.log())
    latest_commit = commits[0]
    assert latest_commit.message == message
    assert latest_commit.id == sha.id


def test_transaction_tag(fs: LakeFSFileSystem, repository: Repository) -> None:
    try:
        # tag gets created on exit of the context.
        with fs.transaction as tx:
            sha = tx.rev_parse(repository, "main")
            tag = tx.tag(repository, ref=sha, tag="v2")

        assert sha.available

        tags = list(repository.tags())
        assert len(tags) > 0
        assert tags[0].id == tag
        assert tags[0].get_commit().id == sha.id
    finally:
        repository.tag(tag).delete()


def test_transaction_merge(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
    temporary_branch_context: Any,
) -> None:
    random_file = random_file_factory.make()

    with temporary_branch_context("transaction-merge-test") as new_branch:
        resource = f"{repository.id}/{new_branch.id}/{random_file.name}"
        message = "Commit new file"

        with fs.transaction as tx:
            # stage a file on new_branch...
            fs.put_file(str(random_file), resource)
            # ... commit it with the above message
            tx.commit(repository, new_branch, message)
            # ... and merge it into temp_branch.
            tx.merge(repository, new_branch, into=temp_branch)

        # at last, verify temp_branch@HEAD is the merge commit.
        commits = list(temp_branch.log())
        assert len(commits) > 2
        latest_commit = commits[0]
        assert latest_commit.message == f"Merge {new_branch.id!r} into {temp_branch.id!r}"
        second_latest_commit = commits[1]
        assert second_latest_commit.message == message


def test_transaction_revert(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository.id}/{temp_branch.id}/{random_file.name}"

    message = f"Add file {random_file.name}"

    with fs.transaction as tx:
        fs.put_file(lpath, rpath, autocommit=False)
        tx.commit(repository, temp_branch, message=message)
        tx.revert(repository, temp_branch)

    commits = list(temp_branch.log())
    assert len(commits) > 1
    latest_commit = commits[0]
    assert latest_commit.message == f"Revert {temp_branch.id}"


def test_transaction_branch(fs: LakeFSFileSystem, repository: Repository) -> None:
    branch = "new-hello"

    try:
        with fs.transaction as tx:
            tx.create_branch(repository, branch, source="main")

        assert branch in [b.id for b in list(repository.branches())]
    finally:
        repository.branch(branch).delete()


def test_transaction_entry(fs: LakeFSFileSystem) -> None:
    fs.start_transaction()
    assert fs._intrans
    assert fs._transaction is not None


def test_transaction_failure(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository.id}/{temp_branch.id}/{random_file.name}"

    message = f"Add file {random_file.name}"

    fs.client, counter = with_counter(fs.client)
    try:
        with fs.transaction as tx:
            fs.put_file(lpath, rpath)
            tx.commit(repository, temp_branch, message=message)
            raise RuntimeError("something went wrong")
    except RuntimeError:
        pass

    # assert that no commit happens because of the exception.
    assert counter.count("commits_api.commit") == 0


def test_placeholder_representations(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    with fs.transaction as tx:
        rpath = put_random_file_on_branch(random_file_factory, fs, repository, temp_branch)
        message = f"Add file {Path(rpath).name}"
        sha = tx.commit(repository, temp_branch, message=message)

    assert isinstance(sha, Reference)
    commits = list(temp_branch.log())
    latest_commit = commits[0]
    assert sha.id == latest_commit.id
    assert repr(sha.id) == repr(latest_commit.id)


def test_unfilled_placeholder_error() -> None:
    p: Placeholder[Commit] = Placeholder()

    with pytest.raises(RuntimeError):
        _ = p.value
