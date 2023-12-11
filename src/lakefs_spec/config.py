"""
Functionality for working with ``lakectl`` configuration files useable for authentication in the lakeFS file system.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, NamedTuple

import yaml


class LakectlConfig(NamedTuple):
    """
    Holds configuration values necessary for authentication with a lakeFS server from Python.
    """

    host: str | None = None
    """URL of the lakeFS host, http(s) prefix is optional."""
    username: str | None = None
    """The access key ID to use in authentication with lakeFS."""
    password: str | None = None
    """The secret access key to use in authentication with lakeFS."""

    @classmethod
    def read(cls, path: str | Path) -> "LakectlConfig":
        """
        Read in a lakectl YAML configuration file and parse out relevant authentication parameters.

        Parameters
        ----------
        path: str | Path
            Path to the YAML configuration file.

        Returns
        -------
        LakectlConfig
            The immutable loaded configuration. Missing values are filled with ``None`` placeholders.

        Raises
        ------
        FileNotFoundError
            If the configuration file does not exist.
        """
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
