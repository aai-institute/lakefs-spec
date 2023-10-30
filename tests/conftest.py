import contextlib
import logging
import random
import string
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Generator, TypeVar
from unittest.mock import MagicMock

import pytest
import yaml
from lakefs_sdk import Configuration
from lakefs_sdk.client import LakeFSClient
from lakefs_sdk.models import BranchCreation, RepositoryCreation

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory

_TEST_REPO = "lakefs-spec-tests"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))

T = TypeVar("T")

YieldFixture = Generator[T, None, None]


@dataclass
class LakeFSOptions:
    host: str
    username: str
    password: str


@pytest.fixture(scope="session")
def lakefs_options() -> LakeFSOptions:
    """Raw configuration options for a lakeFS test instance."""
    return LakeFSOptions(
        host="localhost:8000",
        username="AKIAIOSFOLQUICKSTART",
        password="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )


@pytest.fixture
def fs(lakefs_options: LakeFSOptions) -> LakeFSFileSystem:
    LakeFSFileSystem.clear_instance_cache()
    return LakeFSFileSystem(**asdict(lakefs_options))


@pytest.fixture(scope="session")
def lakefs_client(lakefs_options: LakeFSOptions) -> LakeFSClient:
    """A lakeFS client for API operations outside of the file system."""

    configuration = Configuration(
        host=lakefs_options.host,
        username=lakefs_options.username,
        password=lakefs_options.password,
    )
    return LakeFSClient(configuration=configuration)


@pytest.fixture(scope="session")
def ensurerepo(lakefs_client: LakeFSClient) -> str:
    # no loop, assumes there exist fewer than 100 repos.
    reponames = [r.id for r in lakefs_client.repositories_api.list_repositories().results]

    if _TEST_REPO in reponames:
        logger.info(f"Test repository {_TEST_REPO!r} already exists.")
    else:
        storage_config = lakefs_client.config_api.get_config().storage_config
        storage_namespace = f"{storage_config.default_namespace_prefix}/{_TEST_REPO}"
        logger.info(
            f"Creating test repository {_TEST_REPO!r} "
            f"with associated storage namespace {storage_namespace!r}."
        )
        lakefs_client.repositories_api.create_repository(
            RepositoryCreation(
                name=_TEST_REPO,
                storage_namespace=storage_namespace,
                sample_data=True,
            )
        )
    return _TEST_REPO


@pytest.fixture(scope="session")
def repository(ensurerepo: str) -> str:
    return ensurerepo


@pytest.fixture
def temporary_branch_context(lakefs_client: LakeFSClient, repository: str) -> Any:
    @contextlib.contextmanager
    def _wrapper(name: str) -> YieldFixture[str]:
        try:
            lakefs_client.branches_api.create_branch(
                repository=repository,
                branch_creation=BranchCreation(name=name, source="main"),
            )
            yield name
        finally:
            lakefs_client.branches_api.delete_branch(
                repository=repository,
                branch=name,
            )

    return _wrapper


@pytest.fixture
def temp_branch(lakefs_client: LakeFSClient, repository: str) -> YieldFixture[str]:
    """Create a temporary branch for a test."""
    name = "test-" + "".join(random.choices(string.digits, k=8))
    try:
        lakefs_client.branches_api.create_branch(
            repository=repository,
            branch_creation=BranchCreation(name=name, source="main"),
        )
        yield name
    finally:
        lakefs_client.branches_api.delete_branch(
            repository=repository,
            branch=name,
        )


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
