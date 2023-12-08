from lakefs_spec.spec import LakeFSFileSystem


def test_walk_single_dir(fs: LakeFSFileSystem, repository: str) -> None:
    """`walk` in a single directory should find all files contained therein"""
    branch = "main"
    resource = "images"
    path = f"{repository}/{branch}/{resource}/"

    dirname, dirs, files = next(fs.walk(path))
    assert dirname == path.rstrip("/")
    assert dirs == []
    assert len(files) == 37  # NOTE: hardcoded for quickstart repo


def test_walk_repo_root(fs: LakeFSFileSystem, repository: str) -> None:
    """`walk` should be able to be called on the root directory of a repository"""
    branch = "main"
    path = f"{repository}/{branch}/"

    dirname, dirs, files = next(fs.walk(path))
    assert dirname == path
    assert len(dirs) == 2
    assert len(files) == 2
