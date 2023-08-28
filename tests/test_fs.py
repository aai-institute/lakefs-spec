from lakefs_spec import LakeFSFileSystem


def test_instance_caching(fs: LakeFSFileSystem) -> None:
    assert len(LakeFSFileSystem._cache) == 1
    # same as the fixture, so we should get a cache hit.
    fs_new = LakeFSFileSystem(
        host="localhost:8000",
        username="AKIAIOSFOLQUICKSTART",
        password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )

    assert fs_new._fs_token in LakeFSFileSystem._cache
    assert fs_new._fs_token == fs._fs_token

    # this, however, should not be cached.
    fs2 = LakeFSFileSystem(
        host="localhost:80000",
    )

    assert fs2._fs_token != fs._fs_token
    assert len(LakeFSFileSystem._cache) == 2
