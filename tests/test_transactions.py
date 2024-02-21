from typing import Any

import pytest
from lakefs.branch import Branch
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory


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

    with fs.transaction(repository, temp_branch) as tx:
        fs.put_file(lpath, f"{repository.id}/{tx.branch.id}/{random_file.name}")
        assert len(tx.files) == 1
        # sha is a placeholder for the actual SHA created on transaction completion.
        sha = tx.commit(message=message)

    # HEAD should be the merge commit.
    head_tilde = list(temp_branch.log(max_amount=2))[-1]
    assert head_tilde.message == message
    assert head_tilde.id == sha.id


def test_transaction_tag(fs: LakeFSFileSystem, repository: Repository) -> None:
    try:
        # tag gets created on exit of the context.
        # in this test, initialize with the repo name.
        with fs.transaction(repository.id) as tx:
            sha = tx.rev_parse("main")
            tag = tx.tag(sha, "v2")

        tags = list(repository.tags())
        assert len(tags) > 0
        assert tags[0].id == tag.id
        assert tags[0].get_commit().id == sha.id
    finally:
        tag.delete()


def test_transaction_merge(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
    temporary_branch_context: Any,
) -> None:
    random_file = random_file_factory.make()

    with temporary_branch_context("transaction-merge-test") as new_branch:
        message = "Commit new file"

        with fs.transaction(repository, new_branch) as tx:
            tbname = tx.branch.id
            lpath = str(random_file)
            # stage a file on the transaction branch...
            fs.put_file(lpath, f"{repository.id}/{tx.branch.id}/{random_file.name}")
            # ... commit it with the above message
            tx.commit(message=message)
            # ... and merge it into temp_branch.
            tx.merge(tx.branch, into=temp_branch)

        head, head_tilde = list(temp_branch.log(max_amount=2))
        # HEAD should be the merge commit of the transaction branch.
        assert head.message.startswith(f"Merge {tbname!r}")
        # HEAD~ should be the commit message.
        assert head_tilde.message == message


def test_transaction_revert(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    message = f"Add file {random_file.name}"

    with fs.transaction(repository, temp_branch, automerge=True) as tx:
        fs.put_file(lpath, f"{repository.id}/{tx.branch.id}/{random_file.name}")
        tx.commit(message=message)
        revert_commit = tx.revert(temp_branch, temp_branch.head)

    # first commit should be the merge commit
    assert temp_branch.head.get_commit().message.startswith("Merge")
    assert revert_commit.message.startswith("Revert")


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

    try:
        with fs.transaction(repository, temp_branch) as tx:
            fs.put_file(lpath, f"{repository.id}/{tx.branch.id}/{random_file.name}")
            tx.commit(message=message)
            raise RuntimeError("something went wrong")
    except RuntimeError:
        pass

    # assert that no commit happens because of the exception.
    assert not fs.exists(rpath)


def test_transaction_no_automerge(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    currhead = temp_branch.head.get_commit()

    with fs.transaction(repository, temp_branch, automerge=False, delete="never") as tx:
        transaction_branch = tx.branch

    try:
        # assert no merge commit is created on temp_branch.
        assert currhead == next(temp_branch.log())
        # assert the transaction branch still exists.
        assert transaction_branch.id in [b.id for b in repository.branches()]
    finally:
        transaction_branch.delete()


def test_transaction_bad_repo(fs: LakeFSFileSystem) -> None:
    with pytest.raises(ValueError, match="repository .* does not exist"):
        with fs.transaction(repository="REEEE"):
            pass


def test_warn_uncommitted_changes(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)

    with pytest.warns(match="uncommitted changes.*lost"):
        with fs.transaction(repository, temp_branch) as tx:
            fs.put_file(lpath, f"{repository.id}/{tx.branch.id}/{random_file.name}")


def test_warn_uncommitted_changes_on_persisted_branch(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)

    with pytest.warns(match="uncommitted changes(?:(?!lost).)*$"):
        with fs.transaction(repository, temp_branch, delete="never") as tx:
            fs.put_file(lpath, f"{repository.id}/{tx.branch.id}/{random_file.name}")
