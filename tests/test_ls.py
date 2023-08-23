from lakefs_client.client import LakeFSClient

from lakefs_spec.spec import LakeFSFileSystem


def test_paginated_ls(lakefs_client: LakeFSClient, repository: str) -> None:
    """
    Check that all results of an ``ls`` call are returned independently of page size.
    """
    fs = LakeFSFileSystem(client=lakefs_client)
    resource = f"{repository}/main/"

    # default amount of 100 objects per page
    all_results = fs.ls(resource)

    for pagesize in [2, 5, 10, 50]:
        paged_results = fs.ls(resource, amount=pagesize)
        assert paged_results == all_results
