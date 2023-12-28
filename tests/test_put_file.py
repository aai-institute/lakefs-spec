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
    rpath = f"{repository.id}/{temp_branch.id}/{random_file.name}"

    with fs.transaction as tx:
        fs.put(lpath, rpath, precheck=False, autocommit=False)
        tx.commit(repository, temp_branch, message=f"Add file {random_file.name}")

    commits = list(temp_branch.log())
    latest_commit = commits[0]  # commit log is ordered branch-tip-first
    assert latest_commit.message == f"Add file {random_file.name}"

    # put the same file again, this time the diff is empty
    with fs.transaction as tx:
        fs.put(lpath, rpath, precheck=False, autocommit=False)
        tx.commit(repository, temp_branch, message=f"Add file {random_file.name}")

    # check that no other commit has happened.
    commits = list(temp_branch.log())
    assert commits[0] == latest_commit


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
    with pytest.raises(FileNotFoundError, match="Not Found: .*"):
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
