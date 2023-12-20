from lakefs.branch import Branch
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_exists(fs: LakeFSFileSystem, repository: Repository) -> None:
    """Test `fs.exists` on an existing file. Requires a populated repository."""

    example_file = f"{repository.id}/main/README.md"
    assert fs.exists(example_file)

    nonexistent_file = f"{repository.id}/main/nonexistent.parquet"
    assert not fs.exists(nonexistent_file)


def test_exists_on_staged_file(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository.id}/{temp_branch.id}/{random_file.name}"

    # upload, verify existence.
    fs.put(lpath, rpath)
    assert fs.exists(rpath)
