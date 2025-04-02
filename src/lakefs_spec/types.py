# Functional syntax to allow for the attribute name containing a dash
from typing import Literal, TypedDict

FileInfoData = TypedDict(
    "FileInfoData",
    {
        "type": Literal["file"],
        "name": str,
        "size": int | None,
        "checksum": str,
        # TODO: the dash was an unfortunate choice, but is kept for backwards compatibility.
        "content-type": str | None,
        "mtime": int,
        "metadata": dict[str, str] | None,
    },
)


class DirectoryInfoData(TypedDict):
    type: Literal["directory"]
    name: str
    size: int
