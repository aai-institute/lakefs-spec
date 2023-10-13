from unittest.mock import MagicMock, patch

import pytest
from lakefs_client.model.staging_location import StagingLocation

from lakefs_spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_local_blockstore_type(
    random_file_factory: RandomFileFactory, repository: str, temp_branch: str, fs: LakeFSFileSystem
) -> None:
    random_file = random_file_factory.make()
    lpath = str(random_file)

    with patch(
        "lakefs_spec.spec.get_blockstore_type", new_callable=MagicMock
    ) as mock_get_client_blockstore_type:
        mock_get_client_blockstore_type.return_value = "local"
        with pytest.raises(ValueError, match="not implemented for blockstore type 'local'"):
            fs.put_file_to_blockstore(lpath, repository, temp_branch, random_file.name)


def test_presigned_url(
    mock_urlopen: MagicMock,
    random_file_factory: RandomFileFactory,
    repository: str,
    temp_branch: str,
    fs: LakeFSFileSystem,
) -> None:
    random_file = random_file_factory.make()
    lpath = str(random_file)

    with patch(
        "lakefs_spec.spec.get_blockstore_type", new_callable=MagicMock
    ) as mock_get_client_blockstore_type:
        mock_get_client_blockstore_type.return_value = "s3"
        get_physical_address_mock = fs.client.staging_api.get_physical_address = MagicMock(
            return_value=StagingLocation(presigned_url="http://mock.address", token="123")
        )
        link_physical_address_mock = fs.client.staging_api.link_physical_address = MagicMock()

        fs.put_file_to_blockstore(lpath, repository, temp_branch, random_file.name, presign=True)
    mock_urlopen.assert_called_once()
    get_physical_address_mock.assert_called_once()
    link_physical_address_mock.assert_called_once()
