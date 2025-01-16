from lakefs import Branch, Repository

from lakefs_spec.spec import LakeFSFileSystem


def test_gh_314(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """
    Regression test for GitHub issue 314: Enable `@` and `~N` syntax
    https://github.com/aai-institute/lakefs-spec/issues/314
    """

    prefix = f"lakefs://{repository.id}/{temp_branch.id}"
    datapath = f"{prefix}/data.txt"

    # add new file, and immediately commit.
    fs.pipe(datapath, b"data1")
    temp_branch.commit(message="Add data.txt")

    fs.pipe(datapath, b"data2")
    # Reading the committed version of the file should yield the correct data.
    committed_head_path = f"{prefix}@/data.txt"
    assert fs.read_text(committed_head_path) == "data1"

    # Reading a relative commit should yield the correct data.
    temp_branch.commit(message="Update data.txt")
    relative_commit_path = f"{prefix}~1/data.txt"
    assert fs.read_text(relative_commit_path) == "data1"
