import os

from lakefs import Branch, Repository

from lakefs_spec.spec import LakeFSFileSystem


def test_gh_297(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """
    Regression test for GitHub issue 297: Strange behavior with files containing `__`

    https://github.com/aai-institute/lakefs-spec/issues/297
    """

    root_dir = "data/foo"
    files = [
        f"{root_dir}/bar/axe.just",
        f"{root_dir}/bar/quux/widg.txt",
        f"{root_dir}/bar_baz.txt",
    ]
    prefix = f"lakefs://{repository.id}/{temp_branch.id}"

    for file in files:
        fs.pipe(f"{prefix}/{file}", b"data")

    # -- fs.find() should list all files
    # In #297, the `__` in the filename caused the `find` method to return
    # just the `foo/bar/` directory and the `foo/bar__baz.txt` file.
    found_files = fs.find(f"{prefix}/{root_dir}")
    assert set(found_files) == {f"{repository.id}/{temp_branch.id}/{f}" for f in files}

    # -- fs.walk() should list all files exactly once
    result = list(fs.walk(f"{prefix}/{root_dir}"))
    found_files = set()
    for entry in result:
        rootdir = entry[0]
        for filename in entry[2]:
            found_files.add(os.path.join(rootdir, filename))

    assert found_files == {f"{repository.id}/{temp_branch.id}/{f}" for f in files}

    # Check that the files are present
    for file in files:
        assert fs.isfile(f"{prefix}/{file}")
