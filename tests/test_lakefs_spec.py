import collections
import random
import string
from pathlib import Path

import pytest

from lakefs_spec.client import LakeFSClient
from lakefs_spec.spec import LakeFSFileSystem, md5_checksum


class APICounter:
    def __init__(self):
        self._counts: dict[str, int] = collections.defaultdict(int)

    def clear(self):
        self._counts.clear()

    def count(self, name: str) -> int:
        return self._counts[name]

    def counts(self):
        """Gives an iterator over the API counts."""
        return self._counts.values()

    def named_counts(self):
        """Gives an iterator over the API names and counts."""
        return self._counts.items()

    def increment(self, name: str) -> None:
        self._counts[name] += 1


def with_counter(client: LakeFSClient) -> tuple[LakeFSClient, APICounter]:
    """Instruments a lakeFS API client with an API counter."""
    counter = APICounter()

    def patch(fn, name):
        """Patches an API instance method ``fn`` on an API ``name``."""

        def wrapped_fn(*args, **kwargs):
            counter.increment(name)
            return fn(*args, **kwargs)

        return wrapped_fn

    api_names = [
        name
        for name in dir(client)
        if not name.startswith("_") and not name.endswith("_api")
    ]
    for api_name in api_names:
        api_object = getattr(client, api_name)
        endpoint_names = [
            name
            for name in dir(api_object)
            if not name.startswith("_") and not name.endswith("_endpoint")
        ]
        for endpoint_name in endpoint_names:
            if endpoint_name == "api_client":
                continue
            endpoint = getattr(api_object, endpoint_name)
            setattr(
                api_object,
                endpoint_name,
                patch(endpoint, f"{api_name}.{endpoint_name}"),
            )

    return client, counter


def test_checksum_matching(
    tmp_path: Path, lakefs_client: LakeFSClient, repository: str
) -> None:
    # generate 4KiB random string
    random_file = tmp_path / "test.txt"
    random_str = "".join(
        random.choices(string.ascii_letters + string.digits, k=2**12)
    )
    random_file.write_text(random_str, encoding="utf-8")
    random_fp = str(random_file)

    fs = LakeFSFileSystem(client=lakefs_client, repository=repository)
    fs.client, counter = with_counter(fs.client)

    remote_path = "test.txt"
    fs.put_file(random_fp, remote_path, branch="main")

    # assert that MD5 hash is insensitive to the block size
    blocksizes = [2**5, 2**8, 2**10, 2**12, 2**22]
    for blocksize in blocksizes:
        local_checksum = md5_checksum(random_fp, blocksize)
        assert local_checksum == fs.checksum("test.txt", ref="main")

    # we expect to get one `ls` call per upload attempt,
    # but only one actual upload!
    assert counter.count("objects.list_objects") == len(blocksizes) + 1
    assert counter.count("objects.upload_object") == 1

    # force overwrite this time, assert the `upload` API was called again
    fs.put_file(random_fp, remote_path, branch="main", force=True)
    assert counter.count("objects.upload_object") == 2


def test_get_nonexistent_file(lakefs_client: LakeFSClient, repository: str) -> None:
    """
    Regression test against error on file closing in fs.get_file() after a
     lakeFS API exception.
    """
    fs = LakeFSFileSystem(client=lakefs_client, repository=repository)

    with pytest.raises(FileNotFoundError):
        fs.get_file("hello-i-no-exist1234.txt", "out.txt", ref="main")

    Path("out.txt").unlink(missing_ok=True)


def test_paginated_ls(lakefs_client: LakeFSClient, repository: str) -> None:
    """
    Check that all results of an ``ls`` call are returned independently of page size.
    """
    fs = LakeFSFileSystem(client=lakefs_client, repository=repository)

    # default amount of 100 objects per page
    all_results = fs.ls("/", ref="main")

    for pagesize in [2, 5, 10, 50]:
        paged_results = fs.ls("/", ref="main", amount=pagesize)
        assert paged_results == all_results
