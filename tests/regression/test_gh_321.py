from lakefs import Branch, Repository

from lakefs_spec.spec import LakeFSFileSystem


def test_gh_321(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """
    Regression test for GitHub issue 321: ls() method doesn't return the expected type in info
    https://github.com/aai-institute/lakefs-spec/issues/321
    """

    from pyarrow.fs import FileSelector, FileType, FSSpecHandler, PyFileSystem

    rpath = f"lakefs://{repository.id}/{temp_branch.id}"

    # Use the fs object to list files in the directory
    pa_lakefs = PyFileSystem(FSSpecHandler(fs))
    info = pa_lakefs.get_file_info(FileSelector(f"{rpath}/data", recursive=True))

    for item in info:
        assert item.type != FileType.Unknown
