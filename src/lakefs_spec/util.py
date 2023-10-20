import re

from lakefs_client import __version__ as __lakefs_client_version__
from lakefs_client.client import LakeFSClient


def parse(path: str) -> tuple[str, str, str]:
    """
    Parses a lakeFS URI in the form ``<repo>/<ref>/<resource>``.

    Parameters
    ----------
    path: str
        String path, needs to conform to the lakeFS URI format described above.
        The ``<resource>`` part can be the empty string.

    Returns
    -------
    str
        A 3-tuple of repository name, reference, and resource name.
    """

    # First regex reflects the lakeFS repository naming rules:
    # only lowercase letters, digits and dash, no leading dash,
    # minimum 3, maximum 63 characters
    # https://docs.lakefs.io/understand/model.html#repository
    # Second regex is the branch: Only letters, digits, underscores
    # and dash, no leading dash
    path_regex = re.compile(r"([a-z0-9][a-z0-9\-]{2,62})/(\w[\w\-]*)/(.*)")
    results = path_regex.fullmatch(path)
    if results is None:
        raise ValueError(f"expected path with structure <repo>/<ref>/<resource>, got {path!r}")

    repo, ref, resource = results.groups()
    return repo, ref, resource


lakefs_client_version = tuple(int(v) for v in __lakefs_client_version__.split("."))
del __lakefs_client_version__


def get_blockstore_type(client: LakeFSClient) -> str:
    """
    Get the blockstore of the lakeFS server.
    Backwards compatible to breaking config_api change in lakeFSClient version 0.110.1.

    Args:
        client (LakeFSClient): The lakefs client.

    Returns:
        str: The lakeFS server's blockstore type.
    """
    if lakefs_client_version < (0, 111, 0):
        blockstore_type = client.config_api.get_storage_config().blockstore_type
    else:
        blockstore_type = client.config_api.get_config().storage_config.blockstore_type
    return blockstore_type
