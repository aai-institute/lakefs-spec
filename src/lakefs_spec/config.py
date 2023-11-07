from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any, NamedTuple


class LakectlConfig(NamedTuple):
    host: str | None = None
    username: str | None = None
    password: str | None = None

    @classmethod
    def read(cls, path: str | Path) -> "LakectlConfig":
        if not Path(path).exists():
            raise FileNotFoundError(path)

        try:
            import yaml
        except ModuleNotFoundError:
            warnings.warn(
                f"Configuration '{path}' cannot be read because `pyyaml` is not installed. "
                f"To fix, run `python -m pip install --upgrade pyyaml`.",
                UserWarning,
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
