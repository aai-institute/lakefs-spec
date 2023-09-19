import pytest
from lakefs_client.client import LakeFSClient

from lakefs_spec import LakeFSFileSystem
from lakefs_spec.hooks import FSEvent, HookContext


def print_hello(client: LakeFSClient, ctx: HookContext) -> None:
    print("hello")


def test_double_registration_error(fs: LakeFSFileSystem) -> None:
    fs.register_hook(FSEvent.LS, print_hello)

    with pytest.raises(RuntimeError, match="hook already registered for file system event .*"):
        fs.register_hook(FSEvent.LS, print_hello)


def test_deregister_hook(fs: LakeFSFileSystem) -> None:
    fs.register_hook(FSEvent.LS, print_hello)

    assert FSEvent.LS in fs._hooks

    fs.deregister_hook(FSEvent.LS)
    assert FSEvent.LS not in fs._hooks


def test_scoped_disable_hooks(fs: LakeFSFileSystem) -> None:
    fs.register_hook(FSEvent.LS, print_hello)

    assert FSEvent.LS in fs._hooks

    with fs.scope(disable_hooks=True):
        assert FSEvent.LS not in fs._hooks

    assert FSEvent.LS in fs._hooks


def test_string_hookinit(fs: LakeFSFileSystem) -> None:
    fs.register_hook("ls", print_hello)

    assert FSEvent.LS in fs._hooks

    fs.deregister_hook("ls")

    assert FSEvent.LS not in fs._hooks

    with pytest.raises(ValueError, match="unknown file system event .*"):
        fs.register_hook("blabla", print_hello)
