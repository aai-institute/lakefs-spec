from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_exists(fs: LakeFSFileSystem, repository: str) -> None:
    """Test `fs.exists` on an existing file. Requires a populated repository."""

    example_file = f"{repository}/main/README.md"
    assert fs.exists(example_file)

    nonexistent_file = f"{repository}/main/nonexistent.parquet"
    assert not fs.exists(nonexistent_file)


def test_exists_on_staged_file(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"

    # upload, verify existence.
    fs.put(lpath, rpath)
    assert fs.exists(rpath)
