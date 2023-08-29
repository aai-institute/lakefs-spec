import random
import string

import pytest
from lakefs_client.exceptions import NotFoundException

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_put_with_default_commit_hook(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    fs.postcommit = True

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
    # we just push without pre-checks, otherwise the no-diff scenario does not happen
    fs.postcommit = True
    fs.precheck_files = False

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

    # put the same file again, this time the diff is empty
    fs.put(lpath, rpath)
    # check that no other commit has happened.
    commits = fs.client.refs_api.log_commits(
        repository=repository,
        ref=temp_branch,
    )
    assert commits.results[0] == latest_commit
    # in particular, this test asserts that no API exception happens in postcommit.


def test_implicit_branch_creation(
    random_file_factory: RandomFileFactory, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
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
        with pytest.raises(NotFoundException):
            fs.put(lpath, rpath)
