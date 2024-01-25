from typing import Any

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
        with fs.transaction(repository) as tx:
            sha = tx.rev_parse("main")
            tag = tx.tag(sha, tag="v2")

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
        resource = f"{repository.id}/{new_branch.id}/{random_file.name}"
        message = "Commit new file"

        with fs.transaction(repository, new_branch) as tx:
            lpath = str(random_file)
            # stage a file on new_branch...
            fs.put_file(lpath, f"{repository.id}/{tx.branch.id}/{random_file.name}")
            # ... commit it with the above message
            tx.commit(message=message)
            # ... and merge it into temp_branch.
            tx.merge(new_branch, into=temp_branch)

        # at last, verify temp_branch~ is the merge commit.
        commits = list(temp_branch.log(max_amount=3))
        head_tilde = commits[1]
        assert head_tilde.message == f"Merge {new_branch.id!r} into {temp_branch.id!r}"
        second_latest_commit = commits[2]
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

    with fs.transaction(repository, temp_branch) as tx:
        fs.put_file(lpath, f"{repository.id}/{tx.branch.id}/{random_file.name}")
        tx.commit(message=message)
        tx.revert(temp_branch, temp_branch.head)

    head, head_tilde = list(temp_branch.log(max_amount=2))
    assert head.message.startswith("Merge")
    assert head_tilde.message.startswith("Revert")


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
