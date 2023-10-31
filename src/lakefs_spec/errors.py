from __future__ import annotations

import errno
import functools
import json
from typing import Any

from lakefs_sdk import ApiException

HTTP_CODE_TO_ERROR: dict[int, type[OSError]] = {
    401: PermissionError,
    403: PermissionError,
    404: FileNotFoundError,
}


def translate_lakefs_error(
    error: ApiException,
    message: str | None = None,
    set_cause: bool = True,
    *args: Any,
) -> OSError:
    """Convert a lakeFS client ApiException into a Python exception.

    Parameters
    ----------

    error : lakefs_client.ApiException
        The exception returned by the lakeFS API.
    message : str
        An error message to use for the returned exception. If not given, the
        error message returned by the lakeFS server is used instead.
    set_cause : bool
        Whether to set the ``__cause__`` attribute to the previous exception if the
        exception is translated.
    *args:
        Additional arguments to pass to the exception constructor, after the
        error message. Useful for passing the filename arguments to ``IOError``.

    Returns
    -------
    OSError
        An instantiated exception ready to be thrown. If the error code isn't
        recognized, an ``IOError`` with the original error message is returned.
    """
    status, reason, body = error.status, error.reason, error.body

    emsg = f"HTTP{status} ({reason})"
    try:
        lakefs_msg = json.loads(body)["message"]
        emsg += f": {lakefs_msg}"
    except json.JSONDecodeError:
        pass

    constructor = HTTP_CODE_TO_ERROR.get(status, functools.partial(IOError, errno.EIO))
    custom_exc = constructor(message or emsg, *args)
    if set_cause:
        custom_exc.__cause__ = error
    return custom_exc
