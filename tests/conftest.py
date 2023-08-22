import logging
import random
import string
import sys
import time
from pathlib import Path
from typing import Generator, TypeVar

import pytest
from lakefs_client import Configuration
from lakefs_client import __version__ as lakefs_version
from lakefs_client.models import BranchCreation, CommPrefsInput, RepositoryCreation
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready

from lakefs_spec.client import LakeFSClient
from tests.util import RandomFileFactory

_TEST_REPO = "lakefs-spec-tests"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))

T = TypeVar("T")

YieldFixture = Generator[T, None, None]


@pytest.fixture(scope="session")
def lakefs_client(_lakefs_client: LakeFSClient) -> YieldFixture[LakeFSClient]:
    """A lakeFS client for a sidecar testcontainer with quickstart settings and communication preferences set."""

    # Note: Quickstart is only available in lakeFS>=0.105.0
    with DockerContainer(f"treeverse/lakefs:{lakefs_version}").with_command(
        ["run", "--quickstart"]
    ).with_bind_ports(8000, 8000) as container:
        wait_container_is_ready()(container)
        time.sleep(1)

        # Set up comms preferences
        comms_prefs = CommPrefsInput(
            email="lakefs@example.org",
            feature_updates=False,
            security_updates=False,
        )
        _lakefs_client.config.setup_comm_prefs(comms_prefs)

        yield _lakefs_client


@pytest.fixture(scope="session")
def _lakefs_client() -> LakeFSClient:
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
    reponames = [r.id for r in lakefs_client.repositories.list_repositories().results]

    if _TEST_REPO in reponames:
        logger.info(f"Test repository {_TEST_REPO!r} already exists.")
    else:
        storage_config = lakefs_client.config.get_storage_config()
        storage_namespace = f"{storage_config.default_namespace_prefix}/{_TEST_REPO}"
        logger.info(
            f"Creating test repository {_TEST_REPO!r} "
            f"with associated storage namespace {storage_namespace!r}."
        )
        lakefs_client.repositories.create_repository(
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
        lakefs_client.branches.create_branch(
            repository=repository,
            branch_creation=BranchCreation(name=name, source="main"),
        )
        yield name
    finally:
        lakefs_client.branches.delete_branch(
            repository=repository,
            branch=name,
        )


@pytest.fixture
def random_file_factory(tmp_path: Path) -> RandomFileFactory:
    return RandomFileFactory(path=tmp_path)
