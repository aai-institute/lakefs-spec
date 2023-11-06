import sys
from pathlib import Path

import pytest
from pytest import LogCaptureFixture, MonkeyPatch

from lakefs_spec.config import LakectlConfig


def test_config_read_nonexistent_file():
    """
    Tests that a FileNotFoundError is raised when a lakectl config is read from
    a nonexistent file.
    """
    with pytest.raises(FileNotFoundError):
        LakectlConfig.read("aaaaaaaaaaaaaaaa.yaml")


def test_lakectl_config_parsing_without_yaml(
    monkeypatch: MonkeyPatch, caplog: LogCaptureFixture, temporary_lakectl_config: str
) -> None:
    # unset YAML module from sys.modules.
    monkeypatch.setitem(sys.modules, "yaml", None)

    LakectlConfig.read(Path("~/.lakectl.yaml").expanduser())

    # look for the log record, assert that it has the `pyyaml` install instruction.
    assert len(caplog.records) > 0
    assert "`python -m pip install --upgrade pyyaml`." in caplog.records[0].message
