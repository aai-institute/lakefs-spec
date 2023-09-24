from lakefs_spec import LakeFSFileSystem
from tests.util import with_counter


def test_paginated_ls(fs: LakeFSFileSystem, repository: str) -> None:
    """
    Check that all results of an ``ls`` call are returned independently of page size.
    """
    resource = f"{repository}/main/"

    # default amount of 100 objects per page
    all_results = fs.ls(resource)

    for pagesize in [2, 5, 10, 50]:
        paged_results = fs.ls(resource, amount=pagesize)
        assert paged_results == all_results


def test_ls_caching(fs: LakeFSFileSystem, repository: str) -> None:
    """
    Check that ls calls are properly cached.
    """
    fs.client, counter = with_counter(fs.client)

    testdir = "data"
    resource = f"{repository}/main/{testdir}/"

    for _ in range(2):
        fs.ls(resource)
        assert len(fs.dircache) == 1
        assert tuple(fs.dircache.keys()) == (testdir,)

    # assert the second `ls` call hits the cache
    assert counter.count("objects_api.list_objects") == 1
