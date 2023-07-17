import logging
import os
import sys
from typing import Any

import pytest
from lakefs_client import Configuration
from lakefs_client.models import RepositoryCreation

from lakefs_spec.client import LakeFSClient

_DEFAULT_LAKEFS_INSTANCE = "http://lakefs.10.32.16.101.nip.io"
_DEFAULT_LAKEFS_USERNAME = "mlopskit"
_DEFAULT_LAKEFS_PASSWORD = "mlopskit"
_TEST_REPO = "lakefs-spec-tests"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))


def pytest_addoption(parser):
    parser.addoption(
        "--lakefs-instance",
        action="store",
        type=str,
        default=_DEFAULT_LAKEFS_INSTANCE,
        help="lakeFS endpoint to run tests with.",
    )


@pytest.fixture(scope="session")
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
def lakefs_host(request: Any) -> str:
    return request.config.getoption("--lakefs-instance")


@pytest.fixture(scope="session")
def ensurerepo(lakefs_client: LakeFSClient) -> str:
    repos = lakefs_client.repositories.list_repositories()
    reponames = [r.id for r in repos.results]
    if _TEST_REPO in reponames:
        pass
    else:
        logger.info(f"Creating test repository {_TEST_REPO!r}.")
        lakefs_client.repositories.create_repository(
            RepositoryCreation(
                name=_TEST_REPO,
                # TODO (n.junge): Local Docker storage namespaces start with
                #  the local:// prefix instead of s3://lakefs/, is this a problem?
                storage_namespace=f"s3://lakefs/{_TEST_REPO}",
            )
        )
    return _TEST_REPO


@pytest.fixture(scope="session")
def repository(ensurerepo: str) -> str:
    return ensurerepo
