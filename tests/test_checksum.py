import random
import string
import time
from pathlib import Path

from lakefs_spec.client import LakeFSClient
from lakefs_spec.spec import LakeFSFileSystem, md5_checksum
from tests.util import with_counter


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

    fs = LakeFSFileSystem(client=lakefs_client)
    fs.client, counter = with_counter(fs.client)

    remote_path = f"{repository}/main/test.txt"
    fs.put_file(random_fp, remote_path)

    # assert that MD5 hash is insensitive to the block size
    blocksizes = [2**5, 2**8, 2**10, 2**12, 2**22]
    for blocksize in blocksizes:
        local_checksum = md5_checksum(random_fp, blocksize)
        assert local_checksum == fs.checksum(remote_path)
        # this test sometimes fails because of a race condition in the client
        time.sleep(0.1)

    # we expect to get one `ls` call per upload attempt,
    # but only one actual upload.
    assert counter.count("objects.list_objects") == len(blocksizes) + 1
    assert counter.count("objects.upload_object") == 1

    # force overwrite this time, assert the `upload` API was called again
    fs.put_file(random_fp, remote_path, branch="main", force=True)
    assert counter.count("objects.upload_object") == 2
