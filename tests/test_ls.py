from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory, with_counter


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


def test_ls_stale_cache_entry(
    fs: LakeFSFileSystem,
    repository: str,
    random_file_factory: RandomFileFactory,
    temp_branch: str,
) -> None:
    fs.client, counter = with_counter(fs.client)

    random_file = random_file_factory.make()

    resource = f"{repository}/main/data/"

    fs.ls(resource)
    assert counter.count("objects_api.list_objects") == 1
    assert tuple(fs.dircache.keys()) == ("data",)

    cache_entry = fs.dircache["data"]

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/data/{random_file.name}"

    fs.put_file(lpath, rpath)

    res = fs.ls(rpath)
    assert counter.count("objects_api.list_objects") == 2
    # is the file now added to the cache entry?
    assert res[0] in cache_entry


def test_ls_no_detail(fs: LakeFSFileSystem, repository: str) -> None:
    fs.client, counter = with_counter(fs.client)

    resource = f"{repository}/main/data"

    expected = ["data/lakes.source.md"]
    # first, verify the API fetch does the expected...
    assert fs.ls(resource, detail=False) == expected
    assert list(fs.dircache.keys()) == ["data"]

    # ...as well as the cache fetch.
    assert fs.ls(resource, detail=False) == expected
    assert counter.count("objects_api.list_objects") == 1

    # test the same thing with a subfolder + file prefix
    resource = f"{repository}/main/images/duckdb"
    fs.ls(resource, detail=False)

    assert list(fs.dircache.keys()) == ["data", "images"]
