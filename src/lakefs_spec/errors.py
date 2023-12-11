"""
Error translation facilities to map lakeFS API errors to Python-native OS errors in the lakeFS file system.

This is important to honor the fsspec API contract, where users only need to expect builtin Python exceptions to
avoid complicated error handling setups.
"""

from __future__ import annotations

import errno
import json
from functools import partial
from urllib.error import HTTPError

from lakefs_sdk import ApiException

HTTP_CODE_TO_ERROR: dict[int, type[OSError] | partial[OSError]] = {
    400: partial(IOError, errno.EINVAL),
    401: PermissionError,
    403: PermissionError,
    404: FileNotFoundError,
    410: FileNotFoundError,  # Gone (temporarily / permanently unavailable)
    416: partial(IOError, errno.EINVAL),  # invalid range
    420: partial(IOError, errno.EBUSY),  # too many requests
}


def translate_lakefs_error(
    error: ApiException | HTTPError,
    rpath: str | None = None,
    message: str | None = None,
    set_cause: bool = True,
) -> OSError:
    """
    Convert a lakeFS API exception or urllib HTTP error to a Python builtin exception.

    For some subclasses of ``lakefs_sdk.ApiException``, a direct Python builtin equivalent exists.
    In these cases, the suitable equivalent is returned. All other classes are converted to a standard ``IOError``.

    Parameters
    ----------
    error: ApiException | HTTPError
        The exception returned by the lakeFS API.
    rpath: str | None
        The remote resource path involved in the error.
    message: str | None
        An error message to use for the returned exception.
         If not given, the error message returned by the lakeFS server is used instead.
    set_cause: bool
        Whether to set the ``__cause__`` attribute to the previous exception if the exception is translated.

    Returns
    -------
    OSError
        A builtin Python exception ready to be thrown.
    """
    if isinstance(error, ApiException):
        status = error.status
        reason = json.loads(error.body or "{}").get("message", "")
    else:
        status, reason = error.code, error.reason

    emsg = f"{status} {reason}"
    if rpath:
        emsg += f": {rpath!r}"

    constructor = HTTP_CODE_TO_ERROR.get(status, partial(IOError, errno.EIO))
    custom_exc = constructor(message or emsg)

    if set_cause:
        custom_exc.__cause__ = error
    return custom_exc
