from enum import StrEnum, auto
from typing import Callable, NamedTuple

from lakefs_client.client import LakeFSClient

from lakefs_spec.util import parse


class FSEvent(StrEnum):
    CHECKSUM = auto()
    EXISTS = auto()
    GET_FILE = auto()
    GET = auto()
    INFO = auto()
    LS = auto()
    PUT_FILE = auto()
    PUT = auto()
    RM_FILE = auto()
    RM = auto()

    @classmethod
    def canonicalize(cls, s: str) -> "FSEvent":
        if isinstance(s, FSEvent):
            return s

        try:
            return cls[s.upper()]
        except KeyError:
            raise ValueError(f"unknown file system event {s!r}")


class HookContext(NamedTuple):
    repository: str
    ref: str
    resource: str

    @classmethod
    def new(cls, path: str) -> "HookContext":
        repository, ref, resource = parse(path)
        return cls(repository=repository, ref=ref, resource=resource)


LakeFSHook = Callable[[LakeFSClient, HookContext], None]
"""
A hook to execute after a completed file operation.
Input arguments are the lakeFS file system's client, and a context object containing repository, branch name, and (remote) resource name.
"""


def noop(client: LakeFSClient, ctx: HookContext) -> None:
    pass
