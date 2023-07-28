from lakefs_spec.client import LakeFSClient
from lakefs_spec.spec import LakeFSFileSystem
from tests.util import RandomFileFactory


def test_lakefs_file_open(
    lakefs_client: LakeFSClient,
    repository: str,
    temp_branch: str,
    random_file_factory: RandomFileFactory,
) -> None:
    random_file = random_file_factory.make()
    with open(random_file, "rb") as f:
        orig_text = f.read()

    fs = LakeFSFileSystem(client=lakefs_client)
    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"
    fs.put_file(lpath, rpath)

    # try opening the remote file
    with fs.open(rpath) as fp:
        text = fp.read()

    assert text == orig_text
