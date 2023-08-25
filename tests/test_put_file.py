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

    commits = fs.client.commits_api.log_branch_commits(
        repository=repository,
        branch=temp_branch,
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

    commits = fs.client.commits_api.log_branch_commits(
        repository=repository,
        branch=temp_branch,
    )
    latest_commit = commits.results[0]  # commit log is ordered branch-tip-first
    assert latest_commit.message == f"Add file {random_file.name}"

    # put the same file again, this time the diff is empty
    fs.put(lpath, rpath)
    # check that no other commit has happened.
    commits = fs.client.commits_api.log_branch_commits(
        repository=repository,
        branch=temp_branch,
    )
    assert commits.results[0] == latest_commit
    # in particular, this test asserts that no API exception happens in postcommit.
