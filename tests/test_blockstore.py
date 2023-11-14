import collections
from unittest.mock import MagicMock, patch

import pytest
from lakefs_sdk.models.staging_location import StagingLocation

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory

# Mock a lakeFS config return object.
# we only query the block store type for now, we can add more attributes later if needed.
# we define these mock types because it is way easier than initializing the proper
# pydantic models with mock data.
StorageConfig = collections.namedtuple("StorageConfig", ["blockstore_type"])
ConfigMock = collections.namedtuple("ConfigMock", ["storage_config"])


def test_local_blockstore_type(
    random_file_factory: RandomFileFactory, repository: str, temp_branch: str, fs: LakeFSFileSystem
) -> None:
    random_file = random_file_factory.make()
    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"

    with patch(
        "lakefs_sdk.api.ConfigApi.get_config", new_callable=MagicMock
    ) as mock_get_client_blockstore_type:
        mock_get_client_blockstore_type.return_value = ConfigMock(
            storage_config=StorageConfig("local")
        )
        with pytest.raises(ValueError, match="not implemented for blockstore type 'local'"):
            fs.put_file_to_blockstore(lpath, rpath)


def test_presigned_url(
    mock_urlopen: MagicMock,
    random_file_factory: RandomFileFactory,
    repository: str,
    temp_branch: str,
    fs: LakeFSFileSystem,
) -> None:
    random_file = random_file_factory.make()
    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"

    with patch(
        "lakefs_sdk.api.ConfigApi.get_config", new_callable=MagicMock
    ) as mock_get_client_blockstore_type:
        mock_get_client_blockstore_type.return_value = ConfigMock(
            storage_config=StorageConfig("s3")
        )
        get_physical_address_mock = fs.client.staging_api.get_physical_address = MagicMock(
            return_value=StagingLocation(presigned_url="http://mock.address", token="123")
        )
        link_physical_address_mock = fs.client.staging_api.link_physical_address = MagicMock()

        fs.put_file_to_blockstore(lpath, rpath, presign=True)
    mock_urlopen.assert_called_once()
    get_physical_address_mock.assert_called_once()
    link_physical_address_mock.assert_called_once()
