from lakefs import Branch, Repository

from lakefs_spec.spec import LakeFSFileSystem


def test_gh_319(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """
    Regression test for GitHub issue 319: lakefs_spec does not display custom metadata
    https://github.com/aai-institute/lakefs-spec/issues/319
    """

    rpath = f"lakefs://{repository.id}/{temp_branch.id}/test.txt"

    # Create a file with custom metadata
    fs.pipe(
        rpath,
        b"Hello, world!",
        metadata={"custom_key": "custom_value"},
    )

    # Read the file and check the custom metadata
    info = fs.info(rpath)

    metadata = info.get("metadata")
    assert metadata is not None, "Metadata should not be None"
    assert metadata.get("custom_key") == "custom_value"
