import pytest

from lakefs_spec.config import LakectlConfig


def test_config_read_nonexistent_file():
    """
    Tests that a FileNotFoundError is raised when a lakectl config is read from
    a nonexistent file.
    """
    with pytest.raises(FileNotFoundError):
        LakectlConfig.read("aaaaaaaaaaaaaaaa.yaml")
