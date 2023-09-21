import time

from lakefs_spec import LakeFSFileSystem
from lakefs_spec.spec import md5_checksum
from tests.util import RandomFileFactory, with_counter


def test_checksum_matching(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: str,
    temp_branch: str,
) -> None:
    random_file = random_file_factory.make()

    fs.client, counter = with_counter(fs.client)

    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"
    fs.put_file(lpath, rpath)

    # assert that MD5 hash is insensitive to the block size
    blocksizes = [2**5, 2**8, 2**10, 2**12, 2**22]
    for blocksize in blocksizes:
        local_checksum = md5_checksum(lpath, blocksize)
        assert local_checksum == fs.checksum(rpath)
        # this test sometimes fails because of a race condition in the client
        time.sleep(0.1)

    # we expect to get one `info` call per upload attempt, but only one actual upload.
    assert counter.count("objects_api.stat_object") == len(blocksizes) + 1
    assert counter.count("objects_api.upload_object") == 1

    # force overwrite this time, assert the `upload` API was called again
    fs.put_file(lpath, rpath, precheck=False)
    assert counter.count("objects_api.upload_object") == 2
