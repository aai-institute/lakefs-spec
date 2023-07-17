import os

import pytest
from lakefs_client import Configuration

from lakefs_spec.client import LakeFSClient


@pytest.fixture
def lakefs_client() -> LakeFSClient:
    host = os.getenv("LAKEFS_HOST")
    access_key_id = os.getenv("LAKEFS_ACCESS_KEY_ID")
    secret_access_key = os.getenv("LAKEFS_SECRET_ACCESS_KEY")
    configuration = Configuration(
        host=host,
        username=access_key_id,
        password=secret_access_key,
    )
    return LakeFSClient(configuration=configuration)
