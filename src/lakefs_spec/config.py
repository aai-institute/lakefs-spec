from __future__ import annotations

from pathlib import Path
from typing import Any, NamedTuple

import yaml


class LakectlConfig(NamedTuple):
    host: str | None = None
    username: str | None = None
    password: str | None = None

    @classmethod
    def read(cls, path: str | Path) -> "LakectlConfig":
        if not Path(path).exists():
            raise FileNotFoundError(path)

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
