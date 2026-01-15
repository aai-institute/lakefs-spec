from importlib.metadata import version
from unittest.mock import patch

import pytest
from lakefs.repository import Repository
from packaging.version import Version
from pytest import MonkeyPatch

from lakefs_spec import LakeFSFileSystem
from lakefs_spec.types import RequestConfig

lakefs_version = Version(version("lakefs"))


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
    config = cfg_fs.client.config
    assert config.host == "http://hello-world-xyz/api/v1"
    assert config.username == "hello"
    assert config.password == "world"

    monkeypatch.setenv("LAKECTL_SERVER_ENDPOINT_URL", "http://localhost:1000")
    monkeypatch.setenv("LAKECTL_CREDENTIALS_ACCESS_KEY_ID", "my-user")
    monkeypatch.setenv("LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY", "my-password-12345")

    # Case 2: Instantiation from envvars.
    # Clear the instance cache first, since we otherwise would get a cache hit
    # due to the instantiation being the same as from `.lakectl.yaml` above.
    LakeFSFileSystem.clear_instance_cache()
    envvar_fs = LakeFSFileSystem()
    config = envvar_fs.client.config
    assert config.host == "http://localhost:1000/api/v1"
    assert config.username == "my-user"
    assert config.password == "my-password-12345"

    # Case 3: Explicit initialization.
    fs = LakeFSFileSystem(host="http://lakefs.hello", username="my-user", password="my-password")
    config = fs.client.config
    assert config.host == "http://lakefs.hello/api/v1"
    assert config.username == "my-user"
    assert config.password == "my-password"


@pytest.mark.skipif(lakefs_version < Version("0.14"), reason="requires lakefs>=0.14.0")
def test_request_config(repository: Repository) -> None:
    # Set a shorter timeout value for the filesystem (= lakeFS client)
    request_config: RequestConfig = {"request_timeout": 2}
    fs = LakeFSFileSystem(
        host="localhost:8000",
        username="AKIAIOSFOLQUICKSTART",
        password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        request_config=request_config,
    )

    # Mock `get_object_with_http_info` to intercept the API call made on read() below,
    # and assert it contains the custom timeout.
    api = fs.client.sdk_client.objects_api  # pyright: ignore[reportOptionalMemberAccess]
    with patch.object(
        api,
        "get_object_with_http_info",
        wraps=api.get_object_with_http_info,
    ) as get_object:
        with fs.open(f"lakefs://{repository.id}/main/lakes.parquet") as fp:
            fp.read(1)

    # Timeout should show up in the API call kwargs
    assert get_object.call_count == 1
    _, kwargs = get_object.call_args
    for k, v in request_config.items():
        assert kwargs.get(f"_{k}") == v, f"{k} request kwarg not passed correctly"
