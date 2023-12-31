from typing import Any

import pytest
from lakefs.branch import Branch
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory, with_counter


def test_copy(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
    temporary_branch_context: Any,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository.id}/{temp_branch.id}/{random_file.name}"

    fs.put(lpath, rpath)
    assert fs.exists(rpath)

    with temporary_branch_context("new-copy-test") as b:
        new_rpath = f"{repository.id}/{b.id}/{random_file.name}"
        fs.cp_file(rpath, new_rpath)
        assert fs.exists(new_rpath)


def test_copy_edge_cases(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    fs.client, counter = with_counter(fs.client)

    path = f"{repository.id}/main/lakes.parquet"

    fs.cp_file(path, path)

    assert counter.count("objects_api.copy_object") == 0

    with pytest.raises(ValueError, match="can only copy objects within a repository.*"):
        fs.cp_file(path, "my-repo/main/lakes.parquet")
