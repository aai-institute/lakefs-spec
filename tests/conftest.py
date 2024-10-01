import contextlib
import logging
import random
import string
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any, TypeVar
from unittest.mock import MagicMock

import lakefs
import pytest
import yaml
from lakefs.client import Client
from lakefs.repository import Repository

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))

T = TypeVar("T")

YieldFixture = Generator[T, None, None]


def pytest_report_header(config):
    from importlib.metadata import version

    lakefs_version = version("lakefs")
    from lakefs_sdk import __version__ as __lakefs_sdk_version__

    from lakefs_spec import __version__ as __lakefs_spec_version__

    return [
        f"lakefs version: {lakefs_version}",
        f"lakeFS SDK version: {__lakefs_sdk_version__}",
        f"lakeFS-spec version: {__lakefs_spec_version__}",
    ]


@pytest.fixture
def fs() -> LakeFSFileSystem:
    LakeFSFileSystem.clear_instance_cache()
    return LakeFSFileSystem(
        host="localhost:8000",
        username="AKIAIOSFOLQUICKSTART",
        password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )


@pytest.fixture(scope="session")
def _client() -> Client:
    """A lakeFS client for API operations outside of the file system."""
    return Client(
        host="localhost:8000",
        username="AKIAIOSFOLQUICKSTART",
        password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )


@pytest.fixture(scope="session")
def repository(_client: Client) -> Repository:
    name = "lakefs-spec-tests"
    storage_namespace = f"{_client.storage_config.default_namespace_prefix}/{name}"
    repo = lakefs.Repository(name, client=_client).create(
        storage_namespace=storage_namespace, include_samples=True, exist_ok=True
    )
    return repo


@pytest.fixture
def temporary_branch_context(repository: Repository) -> Any:
    @contextlib.contextmanager
    def _wrapper(name: str) -> YieldFixture[str]:
        branch = repository.branch(name)
        try:
            yield branch.create("main", exist_ok=False)
        finally:
            branch.delete()

    return _wrapper


@pytest.fixture
def temp_branch(repository: str, temporary_branch_context: Any) -> YieldFixture[str]:
    """Create a temporary branch for a test."""
    name = "test-" + "".join(random.choices(string.digits, k=8))
    with temporary_branch_context(name) as tb:
        yield tb


@pytest.fixture
def random_file_factory(tmp_path: Path) -> RandomFileFactory:
    return RandomFileFactory(path=tmp_path)


@pytest.fixture
def temporary_lakectl_config() -> YieldFixture[str]:
    d = {
        "credentials": {"access_key_id": "hello", "secret_access_key": "world"},
        "server": {"endpoint_url": "http://hello-world-xyz"},
    }

    loc = "~/.lakectl.yaml"
    path = Path(loc).expanduser()
    backup_path = path.with_stem(path.stem + "_BAK")

    try:
        if path.exists():
            path.rename(backup_path)
        with open(path, "w") as f:
            yaml.dump(d, f)
        yield loc
    finally:
        path.unlink()
        if backup_path.exists():
            backup_path.rename(path)


@pytest.fixture()
def mock_urlopen(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mock = MagicMock()
    monkeypatch.setattr("urllib.request.urlopen", mock)
    return mock
