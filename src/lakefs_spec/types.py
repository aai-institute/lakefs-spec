# Functional syntax to allow for the attribute name containing a dash
from typing import Literal, TypedDict

from typing_extensions import Required

ObjectInfoData = TypedDict(
    "ObjectInfoData",
    {
        "type": Required[Literal["file", "directory"]],
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
