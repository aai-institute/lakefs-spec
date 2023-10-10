from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from lakefs_client.model.staging_location import StagingLocation

from lakefs_spec import LakeFSFileSystem, lakefs_client_version
from tests.util import RandomFileFactory


def test_local_blockstore_type(
    random_file_factory: RandomFileFactory, repository: str, temp_branch: str, fs: LakeFSFileSystem
) -> None:
    random_file = random_file_factory.make()
    lpath = str(random_file)

    mock_storage_config = MagicMock()
    mock_storage_config.return_value.blockstore_type = "local"

    mock_storage_config = MagicMock()
    type(mock_storage_config).blockstore_type = PropertyMock(return_value="local")

    def run_put_file_to_blockstore_catching_value_error() -> pytest.ExceptionInfo[ValueError]:
        with pytest.raises(ValueError) as exc_info:
            fs.put_file_to_blockstore(lpath, repository, temp_branch, random_file.name)
        return exc_info

    if lakefs_client_version < (0, 111, 0):
        with patch.object(
            fs.client.config_api,
            "get_storage_config",
            return_value=mock_storage_config(),
        ):
            exc_info = run_put_file_to_blockstore_catching_value_error()
    else:
        mock_config = MagicMock()
        mock_config.return_value.storage_config = mock_storage_config
        with patch.object(
            fs.client.config_api,
            "get_config",
            return_value=mock_config(),
        ):
            exc_info = run_put_file_to_blockstore_catching_value_error()
    assert (
        str(exc_info.value)
        == "Cannot write to blockstore of type 'local'. Disable use_blockstore or configure remote blockstore."
    )


def test_presigned_url(
    mock_urlopen: MagicMock,
    random_file_factory: RandomFileFactory,
    repository: str,
    temp_branch: str,
    fs: LakeFSFileSystem,
) -> None:
    random_file = random_file_factory.make()
    lpath = str(random_file)
    if lakefs_client_version < (0, 111, 0):
        fs.client.config_api.get_storage_config = MagicMock(blockstore_type="s3")
    else:
        fs.client.config_api.get_config = MagicMock(blockstore_type="s3")
    get_physical_address_mock = fs.client.staging_api.get_physical_address = MagicMock(
        return_value=StagingLocation(presigned_url="http://mock.address", token="123")
    )
    link_physical_address_mock = fs.client.staging_api.link_physical_address = MagicMock()

    fs.put_file_to_blockstore(lpath, repository, temp_branch, random_file.name, presign=True)
    mock_urlopen.assert_called_once()
    get_physical_address_mock.assert_called_once()
    link_physical_address_mock.assert_called_once()
