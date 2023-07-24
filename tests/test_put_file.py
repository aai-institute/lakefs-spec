import random
import string
from pathlib import Path

from lakefs_spec.client import LakeFSClient
from lakefs_spec.spec import LakeFSFileSystem


def test_put_file_with_default_commit_hook(
    tmp_path: Path, lakefs_client: LakeFSClient, repository: str, temp_branch: str
) -> None:
    fs = LakeFSFileSystem(client=lakefs_client, autocommit=True)

    # TODO: Abstract this into a tempfile factory fixture
    # generate 4KiB random string
    random_file = tmp_path / "test.txt"
    random_str = "".join(
        random.choices(string.ascii_letters + string.digits, k=2**12)
    )
    random_file.write_text(random_str, encoding="utf-8")
    lpath = str(random_file)

    fname = (
        "test-"
        + "".join(random.choices(string.ascii_letters + string.digits, k=8))
        + ".txt"
    )
    rpath = f"{repository}/{temp_branch}/{fname}"
    fs.put_file(lpath, rpath)

    commits = fs.client.commits.log_branch_commits(
        repository=repository,
        branch=temp_branch,
    )
    latest_commit = commits.results[0]  # commit log is ordered branch-tip-first
    assert latest_commit.message == f"Add file {fname}"
