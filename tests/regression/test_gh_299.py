from lakefs import Branch, Repository

from lakefs_spec.spec import LakeFSFileSystem


def test_gh_299(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """
    Regression test for GitHub issue 299: Extending ref expression validity.
    https://github.com/aai-institute/lakefs-spec/issues/299
    """

    prefix = f"lakefs://{repository.id}/{temp_branch.id}"
    datapath = f"{prefix}/data.txt"

    # add new file, and immediately commit.
    fs.pipe(datapath, b"data1")
    temp_branch.commit(message="Add data.txt")

    # update the file with new data, commit again.
    fs.pipe(datapath, b"data2")
    temp_branch.commit(message="Update data.txt")

    assert fs.exists(datapath)
    assert fs.read_text(datapath) == "data2"

    # caret at the end of the ref should point to the first descendant...
    previous_datapath = prefix + "^/data.txt"
    assert fs.read_text(previous_datapath) == "data1"

    # ...and, since we have a linear history, equal "~".
    previous_with_tilde = previous_datapath.replace("^", "~")
    assert fs.read_text(previous_with_tilde) == "data1"
