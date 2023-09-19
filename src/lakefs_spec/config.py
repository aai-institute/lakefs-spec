import logging
import sys
from pathlib import Path
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


class LakectlConfig(NamedTuple):
    host: str | None = None
    username: str | None = None
    password: str | None = None

    @classmethod
    def read(cls, path: str | Path) -> "LakectlConfig":
        try:
            import yaml
        except ModuleNotFoundError:
            logger.warning(
                f"Configuration '{path}' exists, but cannot be read because the `pyyaml` package "
                f"is not installed. To fix, run `python -m pip install --upgrade pyyaml`.",
            )
            return cls()

        with open(path, "r") as f:
            obj: dict[str, Any] = yaml.safe_load(f)

        # config struct schema (Golang backend code):
        # https://github.com/treeverse/lakeFS/blob/master/cmd/lakectl/cmd/root.go
        creds: dict[str, str] = obj.get("credentials", {})
        server: dict[str, str] = obj.get("server", {})
        username = creds.get("access_key_id")
        password = creds.get("secret_access_key")
        host = server.get("endpoint_url")
        return cls(host=host, username=username, password=password)
