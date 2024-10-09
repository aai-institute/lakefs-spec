"""
Useful utilities for handling lakeFS URIs and results of lakeFS API calls.
"""

import hashlib
import itertools
import os
import re
import sys
from collections.abc import Callable, Generator, Iterable, Iterator
from typing import Any, Protocol

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
    """
    Unwrap the responses from a paginated lakeFS API method into a generator.

    Parameters
    ----------
    api: Callable[..., PaginatedApiResponse]
        The lakeFS client API to call. Must return a paginated response with the ``pagination`` and ``results`` fields set.
    *args: Any
        Positional arguments to pass to the API call.
    **kwargs: Any
        Keyword arguments to pass to the API call.

    Yields
    ------
    Any
        The obtained API result objects.
    """
    while True:
        resp = api(*args, **kwargs)
        yield from resp.results
        if not resp.pagination.has_more:
            break
        kwargs["after"] = resp.pagination.next_offset


def _batched(iterable: Iterable, n: int) -> Iterator[tuple]:
    # "roughly equivalent" block from
    # https://docs.python.org/3/library/itertools.html#itertools.batched
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, n)):
        yield batch


def batched(iterable: Iterable, n: int) -> Iterator[tuple]:
    # itertools.batched was added in Python 3.12.
    if sys.version_info >= (3, 12):
        # TODO(nicholasjng): Remove once target Python version is 3.12
        yield from itertools.batched(iterable, n)
    else:
        yield from _batched(iterable, n)


def md5_checksum(lpath: str | os.PathLike[str], blocksize: int = 2**22) -> str:
    """
    Calculate a local file's MD5 hash.

    Parameters
    ----------
    lpath: str | os.PathLike[str]
        The local path whose MD5 hash to calculate. Must be a file.
    blocksize: int
        Block size (in bytes) to use while reading in the file.

    Returns
    -------
    str
        The file's MD5 hash value, as a string.
    """
    with open(lpath, "rb") as f:
        file_hash = hashlib.md5(usedforsecurity=False)
        chunk = f.read(blocksize)
        while chunk:
            file_hash.update(chunk)
            chunk = f.read(blocksize)
    return file_hash.hexdigest()


def parse(path: str) -> tuple[str, str, str]:
    """
    Parses a lakeFS URI in the form ``lakefs://<repo>/<ref>/<resource>``.

    Parameters
    ----------
    path: str
        String path, needs to conform to the lakeFS URI format described above.
        The ``<resource>`` part can be the empty string; the leading ``lakefs://`` scheme may be omitted.

    Returns
    -------
    tuple[str, str, str]
        A 3-tuple of repository name, reference, and resource name.

    Raises
    ------
    ValueError
        If the path does not conform to the lakeFS URI format.
    """

    # First regex reflects the lakeFS repository naming rules:
    # only lowercase letters, digits and dash, no leading dash, minimum 3, maximum 63 characters
    # https://docs.lakefs.io/understand/model.html#repository
    # Second regex is the branch: Only letters, digits, underscores and dash, no leading dash.
    path_regex = re.compile(r"(?:lakefs://)?([a-z0-9][a-z0-9\-]{2,62})/(\w[\w\-]*)/(.*)")
    results = path_regex.fullmatch(path)
    if results is None:
        raise ValueError(
            f"expected path with structure lakefs://<repo>/<ref>/<resource>, got {path!r}"
        )

    repo, ref, resource = results.groups()
    return repo, ref, resource
