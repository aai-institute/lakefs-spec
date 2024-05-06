import random
import string

import pytest
from lakefs.branch import Branch
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory, list_branchnames, put_random_file_on_branch, with_counter


def test_no_change_postcommit(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    message = f"Add file {random_file.name}"

    with fs.transaction(repository, temp_branch) as tx:
        fs.put(lpath, f"{repository.id}/{tx.branch.id}/{random_file.name}")
        tx.commit(message=message)

    commits = list(temp_branch.log(max_amount=2))
    current_head = temp_branch.head.get_commit()
    assert commits[0].message.startswith("Merge")
    assert commits[1].message == message

    # put the same file again, this time the diff is empty
    with fs.transaction(repository, temp_branch) as tx:
        fs.put(lpath, f"{repository.id}/{tx.branch.id}/{random_file.name}", precheck=False)
        tx.commit(message=f"Add file {random_file.name}")

    # check that no other commit has happened.
    assert temp_branch.head.get_commit() == current_head


def test_implicit_branch_creation(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    non_existing_branch = "non-existing-" + "".join(random.choices(string.digits, k=8))
    put_random_file_on_branch(random_file_factory, fs, repository, non_existing_branch)

    assert non_existing_branch in list_branchnames(repository)
    # branch has been created at this point
    repository.branch(non_existing_branch).delete()

    fs.create_branch_ok = False
    another_non_existing_branch = "non-existing-" + "".join(random.choices(string.digits, k=8))
    with pytest.raises(FileNotFoundError):
        put_random_file_on_branch(random_file_factory, fs, repository, another_non_existing_branch)


def test_put_client_caching(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """
    Tests that ``precheck=True`` prevents a second upload of an identical file by matching checksums.
    """
    fs.client, counter = with_counter(fs.client)

    rpath = put_random_file_on_branch(random_file_factory, fs, repository, temp_branch)
    assert fs.exists(rpath)
