import lakefs
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


def test_exists_on_commit(fs: LakeFSFileSystem, repository: Repository) -> None:
    """Test `fs.exists` works on commit SHAs to query existence of files in revisions."""
    example_file = f"{repository.id}/main/README.md"
    assert fs.exists(example_file)
    head = lakefs.Branch(repository.id, "main", client=fs.client).head
    assert fs.exists(f"{repository.id}/{head.id}/README.md")


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


def test_exists_repo_root(
    fs: LakeFSFileSystem,
    repository: Repository,
) -> None:
    """Test `fs.exists` on the repository root."""

    # Existing repo and branch should return true
    assert fs.exists(f"lakefs://{repository.id}/main/")

    # Existing repo and commit should return true
    head_ref = lakefs.Branch(repository.id, "main", client=fs.client).head
    assert fs.exists(f"lakefs://{repository.id}/{head_ref.id}/")

    # Nonexistent branch should return false
    assert not fs.exists(f"lakefs://{repository.id}/nonexistent/")

    # Nonexistent repo should return false
    assert not fs.exists("lakefs://nonexistent/main/")
