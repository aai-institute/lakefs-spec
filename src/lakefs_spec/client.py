# The lakeFS import has no typing information and stubs available for the
# LakeFSClient class. Hence, we create a dummy package here with just the
# client (the only public symbol from ``lakefs_client.client`), but with
# typing information from the ``client.pyi`` file.
# pylint: disable=unused-import
from lakefs_client.client import LakeFSClient
