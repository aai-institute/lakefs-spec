from typing import Any

import pytest

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory, with_counter


def test_copy(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
    temporary_branch_context: Any,
) -> None:
    random_file = random_file_factory.make()

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"

    fs.put(lpath, rpath)
    assert fs.exists(rpath)

    with temporary_branch_context("new-copy-test") as b:
        new_rpath = f"{repository}/{b}/{random_file.name}"
        fs.cp_file(rpath, new_rpath)
        assert fs.exists(new_rpath)


def test_copy_edge_cases(
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    fs.client, counter = with_counter(fs.client)

    path = f"{repository}/main/lakes.parquet"

    fs.cp_file(path, path)

    assert counter.count("objects_api.copy_object") == 0

    with pytest.raises(ValueError, match="can only copy objects within a repository.*"):
        fs.cp_file(path, "my-repo/main/lakes.parquet")
