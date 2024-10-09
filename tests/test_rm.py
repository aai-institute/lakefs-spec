import asyncio

import pytest
from lakefs.branch import Branch
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_rm(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    path = f"{repository.id}/{temp_branch.id}/README.md"

    fs.rm(path)
    assert not fs.exists(path)


def test_rm_with_transaction(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    path = f"{repository.id}/{temp_branch.id}/README.md"
    message = "Remove file README.md"

    with fs.transaction(repository, temp_branch, automerge=True) as tx:
        fs.rm(f"{repository.id}/{tx.branch.id}/README.md")
        tx.commit(message=message)

    commits = list(temp_branch.log(max_amount=2))
    assert not fs.exists(path)
    assert commits[-1].message == message
    assert commits[0].message.startswith("Merge")


def test_rm_recursive(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """Validate that recursive ``rm`` removes subdirectories as well."""
    prefix = f"lakefs://{repository.id}/{temp_branch.id}"

    fs.pipe(f"{prefix}/dir1/b.txt", b"b")
    fs.pipe(f"{prefix}/dir1/dir2/c.txt", b"c")

    fs.rm(f"{prefix}/dir1", recursive=False)
    assert fs.exists(f"{prefix}/dir1/dir2/c.txt")
    fs.rm(f"{prefix}/dir1", recursive=True)
    assert not fs.exists(f"{prefix}/dir1/dir2/c.txt")


def test_rm_recursive_with_maxdepth(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """
    Check that recursive ``rm`` with maxdepth leaves directories beyond maxdepth untouched.
    """
    prefix = f"lakefs://{repository.id}/{temp_branch.id}"

    fs.pipe(f"{prefix}/dir1/b.txt", b"b")
    fs.pipe(f"{prefix}/dir1/dir2/c.txt", b"c")

    fs.rm(f"{prefix}/dir1", recursive=True, maxdepth=1)
    # maxdepth is 1-indexed, level 1 being the directory to be removed.
    assert fs.exists(f"{prefix}/dir1/dir2/c.txt")


@pytest.mark.asyncio
async def test_rm_with_1k_objects_or_more(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
    random_file_factory: RandomFileFactory,
) -> None:
    """
    Confirm that lakeFS does not error when attempting to delete more than 1k objects.
    """
    testdir = f"{repository.id}/{temp_branch.id}/subfolder"

    # Create and put 1001 objects into the above lakeFS directory (to exceed the 1k API batch limit)
    # Doing this async since we are I/O bound and get a significant speedup.
    # Unfortunately, we cannot use a TaskGroup, since it was only introduced in Python 3.11.
    tasks = []
    for i in range(1002):
        f = random_file_factory.make()
        lpath = str(f)
        rpath = testdir + f"/test_{i}.txt"
        task = asyncio.create_task(asyncio.to_thread(fs.put_file, lpath, rpath))
        tasks.append(task)
    await asyncio.gather(*tasks)

    assert len(fs.ls(testdir, detail=False)) > 1000

    # should not error, because we chunk the file deletion requests to size 1000.
    fs.rm(testdir, recursive=True, maxdepth=1)
