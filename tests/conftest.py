import logging
import os
import random
import string
import sys
from pathlib import Path
from typing import Any, Generator, TypeVar

import pytest
from lakefs_client import Configuration
from lakefs_client.models import BranchCreation, RepositoryCreation

from lakefs_spec.client import LakeFSClient
from tests.util import RandomFileFactory

_DEFAULT_LAKEFS_INSTANCE = "http://lakefs.10.32.16.101.nip.io"
_DEFAULT_LAKEFS_USERNAME = "mlopskit"
_DEFAULT_LAKEFS_PASSWORD = "mlopskit"
_TEST_REPO = "lakefs-spec-tests"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))

T = TypeVar("T")

YieldFixture = Generator[T, None, None]


def pytest_addoption(parser):
    parser.addoption(
        "--lakefs-host",
        action="store",
        type=str,
        default=_DEFAULT_LAKEFS_INSTANCE,
        help="lakeFS endpoint to run tests with.",
    )


@pytest.fixture(scope="session")
def lakefs_host(request: Any) -> str:
    return request.config.getoption("--lakefs-host")


@pytest.fixture(scope="session", autouse=True)
def lakefs_client(lakefs_host: str) -> LakeFSClient:
    host = os.getenv("LAKEFS_HOST", lakefs_host)
    access_key_id = os.getenv("LAKEFS_ACCESS_KEY_ID", _DEFAULT_LAKEFS_USERNAME)
    secret_access_key = os.getenv("LAKEFS_SECRET_ACCESS_KEY", _DEFAULT_LAKEFS_PASSWORD)
    configuration = Configuration(
        host=host,
        username=access_key_id,
        password=secret_access_key,
    )
    return LakeFSClient(configuration=configuration)


@pytest.fixture(scope="session")
def ensurerepo(lakefs_client: LakeFSClient) -> str:
    repos = lakefs_client.repositories.list_repositories()
    reponames = [r.id for r in repos.results]

    if _TEST_REPO in reponames:
        logger.info(f"Test repository {_TEST_REPO!r} already exists.")
    else:
        # Storage prefix is s3://lakefs/ in lakeFS via Helm deployment,
        # but local:// e.g. in the local Docker container. This seems to
        # be a solid way of extracting the prefix under the assumption that
        # it does not change inside a deployment.
        (sn_prefix,) = set(r.storage_namespace.replace(r.id, "") for r in repos)
        storage_namespace = sn_prefix + _TEST_REPO
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


@pytest.fixture(scope="session", autouse=True)
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
