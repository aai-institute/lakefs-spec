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
    latest_commit = commits.results[0]  # commit log is ordered branch-tip-first
    assert latest_commit.message == message
