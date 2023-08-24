from lakefs_spec import LakeFSFileSystem
from tests.conftest import LakeFSOptions


def test_paginated_ls(lakefs_options: LakeFSOptions, repository: str) -> None:
    """
    Check that all results of an ``ls`` call are returned independently of page size.
    """
    fs = LakeFSFileSystem(
        host=lakefs_options.host,
        username=lakefs_options.username,
        password=lakefs_options.password,
    )
    resource = f"{repository}/main/"

    # default amount of 100 objects per page
    all_results = fs.ls(resource)

    for pagesize in [2, 5, 10, 50]:
        paged_results = fs.ls(resource, amount=pagesize)
        assert paged_results == all_results
