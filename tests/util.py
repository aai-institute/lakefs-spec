import collections

from lakefs_spec.client import LakeFSClient


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
        if "_api" not in api_name:
            for ep_name in dir(api):
                if not (ep_name.endswith("_endpoint") or ep_name.startswith("_")):
                    endpoint = getattr(api, ep_name)
                    setattr(api, ep_name, patch(endpoint, f"{api_name}.{ep_name}"))

    return client, counter
