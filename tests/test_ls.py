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
        assert set(fs.dircache.keys()) == {resource.removesuffix("/")}

    # assert the second `ls` call hits the cache
    assert counter.count("objects_api.list_objects") == 1


def test_ls_cache_refresh(fs: LakeFSFileSystem, repository: str) -> None:
    """
    Check that ls calls bypass the dircache if requested through ``refresh=False``
    """
    fs.client, counter = with_counter(fs.client)

    testdir = "data"
    resource = f"{repository}/main/{testdir}/"

    for _ in range(2):
        fs.ls(resource, refresh=True)
        assert len(fs.dircache) == 1
        assert set(fs.dircache.keys()) == {resource.removesuffix("/")}

    # assert the second `ls` call bypasses the cache
    assert counter.count("objects_api.list_objects") == 2


def test_ls_stale_cache_entry(
    fs: LakeFSFileSystem,
    repository: str,
    random_file_factory: RandomFileFactory,
    temp_branch: str,
) -> None:
    fs.client, counter = with_counter(fs.client)

    random_file = random_file_factory.make()

    resource = f"{repository}/main/data/"

    res = fs.ls(resource)
    assert counter.count("objects_api.list_objects") == 1
    assert set(fs.dircache.keys()) == {resource.removesuffix("/")}

    cache_entry = fs.dircache[resource.removesuffix("/")]
    assert len(cache_entry) == 1
    assert cache_entry[0] == res[0]

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/data/{random_file.name}"

    fs.put_file(lpath, rpath, precheck=False)

    res = fs.ls(rpath)
    assert counter.count("objects_api.list_objects") == 2

    # Should not be added to the cache for the other branch...
    assert len(cache_entry) == 1

    # ... but instead to the cache entry for the new branch
    cache_entry = fs.dircache[f"{repository}/{temp_branch}/data"]
    assert len(cache_entry) == 1
    assert cache_entry[0] == res[0]


def test_ls_no_detail(fs: LakeFSFileSystem, repository: str) -> None:
    fs.client, counter = with_counter(fs.client)

    branch = "main"
    prefix = f"{repository}/{branch}"
    resource = f"{prefix}/data"

    expected = [f"{resource}/lakes.source.md"]
    # first, verify the API fetch does the expected...
    assert fs.ls(resource, detail=False) == expected
    assert list(fs.dircache.keys()) == [resource]

    # ...as well as the cache fetch.
    assert fs.ls(resource, detail=False) == expected
    assert counter.count("objects_api.list_objects") == 1

    # test the same thing with a subfolder + file prefix
    resource = f"{prefix}/images/duckdb"
    fs.ls(resource, detail=False)

    assert set(fs.dircache.keys()) == {f"{prefix}/data", f"{prefix}/images"}
