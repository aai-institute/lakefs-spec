import hashlib
import re
from typing import Any, Callable, Generator, Protocol

from lakefs_sdk import Pagination
from lakefs_sdk import __version__ as __lakefs_sdk_version__

lakefs_sdk_version = tuple(int(v) for v in __lakefs_sdk_version__.split("."))
del __lakefs_sdk_version__


class PaginatedApiResponse(Protocol):
    pagination: Pagination
    results: list


def depaginate(
    api: Callable[..., PaginatedApiResponse], *args: Any, **kwargs: Any
) -> Generator[Any, None, None]:
    """Send a number of lakeFS API response documents to a generator."""
    while True:
        resp = api(*args, **kwargs)
        yield from resp.results
        if not resp.pagination.has_more:
            break
        kwargs["after"] = resp.pagination.next_offset


def md5_checksum(lpath: str, blocksize: int = 2**22) -> str:
    with open(lpath, "rb") as f:
        file_hash = hashlib.md5(usedforsecurity=False)
        chunk = f.read(blocksize)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(blocksize)
    return file_hash.hexdigest()


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
    path_regex = re.compile(r"(?:lakefs://)?([a-z0-9][a-z0-9\-]{2,62})/(\w[\w\-]*)/(.*)")
    results = path_regex.fullmatch(path)
    if results is None:
        raise ValueError(
            f"expected path with structure lakefs://<repo>/<ref>/<resource>, got {path!r}"
        )

    repo, ref, resource = results.groups()
    return repo, ref, resource
