from pytest import MonkeyPatch

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


def test_initialization(monkeypatch: MonkeyPatch, temporary_lakectl_config: str) -> None:
    """
    Verify the priority order of initialization in lakeFS file system:
    1. direct arguments
    2. environment variables
    3. `lakectl` config file

    NOTE: The configuration appends "/api/v1" to all hostnames by default.
    """

    # Verify behaviors in reverse priority order.
    # Case 1: Instantiation from ~/.lakectl.yaml.
    cfg_fs = LakeFSFileSystem()
    config = cfg_fs.client._api.configuration
    assert config.host == "http://hello-world-xyz/api/v1"
    assert config.username == "hello"
    assert config.password == "world"

    monkeypatch.setenv("LAKEFS_HOST", "http://localhost:1000")
    monkeypatch.setenv("LAKEFS_USERNAME", "my-user")
    monkeypatch.setenv("LAKEFS_PASSWORD", "my-password-12345")

    # Case 2: Instantiation from envvars.
    # Clear the instance cache first, since we otherwise would get a cache hit
    # due to the instantiation being the same as from `.lakectl.yaml` above.
    LakeFSFileSystem._cache.clear()
    envvar_fs = LakeFSFileSystem()
    config = envvar_fs.client._api.configuration
    assert config.host == "http://localhost:1000/api/v1"
    assert config.username == "my-user"
    assert config.password == "my-password-12345"

    # Case 3: Explicit initialization.
    fs = LakeFSFileSystem(host="http://lakefs.hello", username="my-user", password="my-password")
    config = fs.client._api.configuration
    assert config.host == "http://lakefs.hello/api/v1"
    assert config.username == "my-user"
    assert config.password == "my-password"
