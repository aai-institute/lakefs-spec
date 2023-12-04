import collections
import random
import string
import uuid
from inspect import ismethod
from pathlib import Path
from typing import Optional, Union

from lakefs_sdk.client import LakeFSClient

import lakefs_spec.client_helpers as client_helpers
from lakefs_spec import LakeFSFileSystem


class APICounter:
    def __init__(self):
        self._counts: dict[str, int] = collections.defaultdict(int)

    def clear(self):
        self._counts.clear()

    def count(self, name: str) -> int:
        return self._counts[name]

    def counts(self):
        """Gives an iterator over the API counts."""
        return self._counts.values()

    def named_counts(self):
        """Gives an iterator over the API names and counts."""
        return self._counts.items()

    def increment(self, name: str) -> None:
        self._counts[name] += 1


def with_counter(client: LakeFSClient) -> tuple[LakeFSClient, APICounter]:
    """Instruments a lakeFS API client with an API counter."""
    counter = APICounter()

    def patch(fn, name):
        """Patches an API instance method ``fn`` on an API ``apiname``."""

        def wrapped_fn(*args, **kwargs):
            counter.increment(name)
            return fn(*args, **kwargs)

        return wrapped_fn

    for api_name, api in client.__dict__.items():
        if api_name == "_api":
            continue

        for ep_name in filter(
            lambda op: not op.startswith("_")
            and not op.endswith("with_http_info")
            and ismethod(getattr(api, op)),
            dir(api),
        ):
            endpoint = getattr(api, ep_name)
            setattr(api, ep_name, patch(endpoint, f"{api_name}.{ep_name}"))

    return client, counter


class RandomFileFactory:
    def __init__(self, path: Union[str, Path]):
        path = Path(path)
        if not path.is_dir():
            raise ValueError(f"input path needs to be a directory, got {path}")

        self.path = path

    def list(self) -> list[Path]:
        return list(self.path.iterdir())

    def make(self, fname: Optional[str] = None, size: int = 2**10) -> Path:
        """
        Generate a random file named ``fname`` with a random string of size
        ``size`` (in bytes) as content.
        """
        if fname is None:
            fname = "test-" + str(uuid.uuid4()) + ".txt"
        random_file = self.path / fname
        random_str = "".join(random.choices(string.ascii_letters + string.digits, k=size))
        random_file.write_text(random_str, encoding="utf-8")
        return random_file


def commit_random_file_on_branch(
    random_file_factory: RandomFileFactory, fs: LakeFSFileSystem, repository: str, temp_branch: str
) -> None:
    random_file = random_file_factory.make()
    lpath = str(random_file)
    rpath = f"{repository}/{temp_branch}/{random_file.name}"

    fs.put(lpath, rpath, precheck=False)

    commit = client_helpers.commit(
        client=fs.client,
        repository=repository,
        branch=temp_branch,
        message=f"Add file {random_file.name!r}",
    )


def list_branch_names(fs: LakeFSFileSystem, repository: str) -> list[str]:
    return [branch.id for branch in client_helpers.list_branches(fs.client, repository=repository)]
