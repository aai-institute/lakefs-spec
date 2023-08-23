import logging
import random
import string
import sys
from pathlib import Path
from typing import Generator, TypeVar

import pytest
from lakefs_client import Configuration
from lakefs_client.client import LakeFSClient
from lakefs_client.models import BranchCreation, RepositoryCreation

from tests.util import RandomFileFactory

_TEST_REPO = "lakefs-spec-tests"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))

T = TypeVar("T")

YieldFixture = Generator[T, None, None]


@pytest.fixture(scope="session")
def lakefs_client() -> LakeFSClient:
    """A lakeFS client with communication preferences set."""
    host = "localhost:8000"
    access_key_id = "AKIAIOSFOLQUICKSTART"
    secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    configuration = Configuration(
        host=host,
        username=access_key_id,
        password=secret_access_key,
    )
    return LakeFSClient(configuration=configuration)


@pytest.fixture(scope="session")
def ensurerepo(lakefs_client: LakeFSClient) -> str:
    # no loop, assumes there exist fewer than 100 repos.
    reponames = [r.id for r in lakefs_client.repositories_api.list_repositories().results]

    if _TEST_REPO in reponames:
        logger.info(f"Test repository {_TEST_REPO!r} already exists.")
    else:
        storage_config = lakefs_client.config_api.get_storage_config()
        storage_namespace = f"{storage_config.default_namespace_prefix}/{_TEST_REPO}"
        logger.info(
            f"Creating test repository {_TEST_REPO!r} "
            f"with associated storage namespace {storage_namespace!r}."
        )
        lakefs_client.repositories_api.create_repository(
            RepositoryCreation(
                name=_TEST_REPO,
                storage_namespace=storage_namespace,
            )
        )
    return _TEST_REPO


@pytest.fixture(scope="session")
def repository(ensurerepo: str) -> str:
    return ensurerepo


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
