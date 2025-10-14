# Functional syntax to allow for the attribute name containing a dash
from typing import Any, Literal, TypedDict

from typing_extensions import Required

ObjectType = Literal["file", "directory"]

ObjectInfoData = TypedDict(
    "ObjectInfoData",
    {
        "type": Required[ObjectType],
        "name": Required[str],
        "size": int | None,
        "checksum": str | None,
        # TODO: the dash was an unfortunate choice, but is kept for backwards compatibility.
        "content-type": str | None,
        "mtime": int | None,
        "metadata": dict[str, str] | None,
    },
    total=False,
)


class RequestConfig(TypedDict, total=False):
    """A custom dict type for keyword arguments configuring OpenAPI requests
    made with the lakeFS SDK."""

    headers: dict[str, Any]
    content_type: str
    request_auth: str
    request_timeout: int | tuple[int, int]
    preload_content: bool
    return_http_data_only: bool


class MergeKwargs(TypedDict, total=False):
    """Options to control the merge of a transaction branch into the base branch.

    This is essentially the `lakefs_sdk.Merge` model, without the optionals.
    """

    message: str
    metadata: dict[str, str]
    strategy: Literal["dest-wins", "source-wins"]
    force: bool
    allow_empty: bool
    squash_merge: bool
