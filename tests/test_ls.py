import lakefs
import pytest
from lakefs.branch import Branch
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory, with_counter


def test_ls_basic(fs: LakeFSFileSystem, repository: Repository) -> None:
    """
    Check basic ``ls`` behavior and return types.
    """
    resource = f"{repository.id}/main/"
    expected_files = ["README.md", "data", "images", "lakes.source.md"]

    all_results = fs.ls(resource, detail=True)
    assert len(all_results) == len(expected_files)
    assert all([o["type"] in ["file", "directory"] for o in all_results])
    assert all([o["name"].startswith(resource) for o in all_results])

    all_results = fs.ls(resource, detail=False)
    assert len(all_results) == len(expected_files)
    assert all([o.startswith(resource) for o in all_results])
    assert isinstance(all_results, list)


@pytest.mark.parametrize("pagesize", [1, 2, 5, 10, 50])
def test_paginated_ls(fs: LakeFSFileSystem, repository: Repository, pagesize: int) -> None:
    """
    Check that all results of an ``ls`` call are returned independently of page size.
    """
    resource = f"{repository.id}/main/"

    # default amount of 100 objects per page
    all_results = fs.ls(resource)

    paged_results = fs.ls(resource, amount=pagesize, refresh=True)
    assert paged_results == all_results


def test_ls_caching(fs: LakeFSFileSystem, repository: Repository) -> None:
    """
    Check that ls calls are properly cached.
    """
    fs.client, counter = with_counter(fs.client)

    testdir = "data"
    resource = f"{repository.id}/main/{testdir}/"

    for _ in range(2):
        fs.ls(resource)
        assert len(fs.dircache) == 1
        assert set(fs.dircache.keys()) == {resource.removesuffix("/")}

    # assert the second `ls` call hits the cache
    assert counter.count("objects_api.list_objects") == 1


def test_ls_with_one_dir(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """
    Check that ls calls are properly cached.
    """
    root = f"lakefs://{repository.id}/{temp_branch.id}"

    fs.mkdir(f"{root}/test")
    fs.cp(f"{root}/lakes.parquet", f"{root}/test/lakes.parquet")
    fs.cp(f"{root}/lakes.parquet", f"{root}/test/lakes2.parquet")
    fs.rm(f"{root}/README.md")
    fs.rm(f"{root}/data/", recursive=True)
    fs.rm(f"{root}/images/", recursive=True)
    fs.rm(f"{root}/lakes.parquet")

    # Check root
    root_resource = f"{root}/"
    list_of_files = fs.ls(root_resource)
    assert [fs.unstrip_protocol(x["name"]) for x in list_of_files] == [f"{root_resource}test/"]
    assert len(fs.dircache) == 1
    assert {fs.unstrip_protocol(x) for x in fs.dircache.keys()} == {root_resource.removesuffix("/")}

    # Check testdir
    testdir = "test"
    resource = f"{root}/{testdir}"
    list_of_files = fs.ls(resource)
    assert [fs.unstrip_protocol(x["name"]) for x in list_of_files] == [
        f"{resource}/lakes.parquet",
        f"{resource}/lakes2.parquet",
    ]
    assert len(fs.dircache) == 2
    assert {fs.unstrip_protocol(x) for x in fs.dircache.keys()} == {
        resource.removesuffix("/"),
        root_resource.removesuffix("/"),
    }


def test_ls_cache_refresh(fs: LakeFSFileSystem, repository: Repository) -> None:
    """
    Check that ls calls bypass the dircache if requested through ``refresh=False``
    """
    fs.client, counter = with_counter(fs.client)

    resource = f"{repository.id}/main/data/"

    for _ in range(2):
        fs.ls(resource, refresh=True)
        assert len(fs.dircache) == 1
        assert set(fs.dircache.keys()) == {resource.removesuffix("/")}

    # assert the second `ls` call bypasses the cache
    assert counter.count("objects_api.list_objects") == 2


def test_ls_stale_cache_entry(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
    random_file_factory: RandomFileFactory,
) -> None:
    fs.client, counter = with_counter(fs.client)

    resource = f"{repository.id}/main/data/"

    res = fs.ls(resource)
    assert counter.count("objects_api.list_objects") == 1
    assert set(fs.dircache.keys()) == {resource.removesuffix("/")}

    cache_entry = fs.dircache[resource.removesuffix("/")]
    assert len(cache_entry) == 1
    assert cache_entry[0] == res[0]

    random_file = random_file_factory.make()
    lpath = str(random_file)
    rpath = f"{repository.id}/{temp_branch.id}/data/{random_file.name}"

    fs.put_file(lpath, rpath, precheck=False)

    res = fs.ls(rpath)
    assert counter.count("objects_api.list_objects") == 2

    # Should not be added to the cache for the other branch...
    assert len(cache_entry) == 1

    # ... but instead to the cache entry for the new branch
    cache_entry = fs.dircache[f"{repository.id}/{temp_branch.id}/data"]
    assert len(cache_entry) == 1
    assert cache_entry[0] == res[0]


def test_ls_no_detail(fs: LakeFSFileSystem, repository: Repository) -> None:
    fs.client, counter = with_counter(fs.client)

    branch = "main"
    prefix = f"{repository.id}/{branch}"
    resource = f"{prefix}/data"

    expected = [f"{resource}/lakes.source.md"]
    # first, verify the API fetch does the expected...
    assert fs.ls(resource, detail=False) == expected
    assert set(fs.dircache.keys()) == {resource}

    # ...as well as the cache fetch.
    assert fs.ls(resource, detail=False) == expected

    # One API call for the directory object, and one for listing its contents
    assert counter.count("objects_api.list_objects") == 2


def test_ls_dircache_remove_uncached(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    resource = f"{repository.id}/{temp_branch.id}/"
    listing_pre = fs.ls(resource)
    fs.rm(listing_pre[0]["name"])

    # List again, bypassing the cache...
    listing_post = fs.ls(resource, refresh=True)
    assert len(listing_post) == len(listing_pre) - 1

    # ... and through the cache (which should have been updated above)
    listing_post = fs.ls(resource)
    assert len(listing_post) == len(listing_pre) - 1


def test_ls_dircache_remove_cached(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    resource = f"{repository.id}/{temp_branch.id}/"
    listing_pre = fs.ls(resource)
    fs.rm(listing_pre[0]["name"])

    # List again, cache should have been invalidated by rm
    listing_post = fs.ls(resource)
    assert len(listing_post) == len(listing_pre) - 1


def test_ls_dircache_recursive(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    prefix = f"{repository.id}/{temp_branch.id}"

    # (1) Basic recursive dircache filling
    listing = fs.ls(prefix + "/", recursive=True)
    assert len(fs.dircache) > 1  # Should contain entries for all sub-folders

    # Dircache invariant: all files in an entry must be direct descendants of its parent
    for cache_dir, files in fs.dircache.items():
        assert all([fs._parent(v["name"].rstrip("/")) == cache_dir for v in files])

    # (2) Dircache correctness, recursive
    cached_listing_recursive = fs.ls(prefix + "/", recursive=True)
    # Recursive listing from cache must contain all items, ordering need not be preserved
    assert {o["name"] for o in cached_listing_recursive} == {o["name"] for o in listing}

    # (3) Dircache correctness, non-recursive
    cached_listing_nonrecursive = fs.ls(prefix + "/", recursive=False)
    # Non-recursive listing from cache must only contain direct descendants of the listed directory
    # (and the subfolders directly contained therein)
    assert all([fs._parent(o["name"].rstrip("/")) == prefix for o in cached_listing_nonrecursive])

    # (4) Adding a file should only modify a single dircache entry
    directory = "data"
    filename = "new-file.txt"
    rpath = f"{prefix}/{directory}/{filename}"

    old_cache_len = len(fs.dircache.get(f"{prefix}/{directory}"))

    fs.pipe(rpath, b"data")
    _ = fs.ls(prefix + "/", refresh=True, recursive=True)

    cache_entry = fs.dircache.get(f"{prefix}/{directory}")

    # Added file appears in the cache entry for its parent dir
    assert len(cache_entry) == old_cache_len + 1
    assert rpath in {f["name"] for f in cache_entry}

    # Dircache invariant is maintained
    for cache_dir, files in fs.dircache.items():
        assert all([fs._parent(v["name"].rstrip("/")) == cache_dir for v in files])


def test_ls_directories(
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    """Validate that recursive and non-recursive ``ls`` handle directory entries identically."""
    prefix = f"lakefs://{repository.id}/{temp_branch.id}"

    fs.rm(f"{prefix}/images/", recursive=True)
    fs.rm(f"{prefix}/data/", recursive=True)

    fs.pipe(f"{prefix}/a.txt", b"a")
    fs.pipe(f"{prefix}/dir1/b.txt", b"b")
    fs.pipe(f"{prefix}/dir1/dir2/c.txt", b"c")

    # (1) - recursive ls includes virtual directory entries for all levels except the root
    ls_recursive = fs.ls(prefix + "/", recursive=True)

    dirs = [o for o in ls_recursive if o["type"] == "directory"]
    assert len(dirs) == 2  # includes `dir1/` and `dir1/dir2`, but not the root.

    # (2) - non-recursive ls only includes virtual dir entries on the same level
    ls_nonrecursive = fs.ls(prefix + "/", recursive=False)

    dirs = [o for o in ls_nonrecursive if o["type"] == "directory"]
    assert len(dirs) == 1  # only `dir1/`


def test_ls_on_commit(
    fs: LakeFSFileSystem,
    repository: Repository,
) -> None:
    prefix = f"lakefs://{repository.id}"

    head = lakefs.Branch(repository.id, "main", client=fs.client).head

    from_branch = fs.ls(f"{prefix}/main/images")
    # we cannot directly compare the objects since the names will be different -
    # they are prefixed with the repository and requested reference.
    branch_metadata = [(o["checksum"], o["mtime"], o["size"]) for o in from_branch]
    # fetching directly from commit should yield the same result.
    from_commit = fs.ls(f"{prefix}/{head.id}/images")
    commit_metadata = [(o["checksum"], o["mtime"], o["size"]) for o in from_commit]

    assert branch_metadata == commit_metadata
