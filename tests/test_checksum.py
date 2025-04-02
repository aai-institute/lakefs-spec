import time

from lakefs.branch import Branch
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem
from lakefs_spec.spec import md5_checksum
from tests.util import RandomFileFactory, with_counter


def test_checksum_matching(
    random_file_factory: RandomFileFactory,
    fs: LakeFSFileSystem,
    repository: Repository,
    temp_branch: Branch,
) -> None:
    random_file = random_file_factory.make()

    fs.client, counter = with_counter(fs.client)

    lpath = str(random_file)
    rpath = f"{repository.id}/{temp_branch.id}/{random_file.name}"
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


def test_checksum_directory(
    fs: LakeFSFileSystem,
    repository: Repository,
) -> None:
    rpath = f"lakefs://{repository.id}/main/data/"
    assert fs.isdir(rpath)
    assert fs.checksum(rpath) is None, "Checksum of a directory should be None"
