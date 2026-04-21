# Passing configuration to the file system

There are multiple ways to configure the `LakeFSFileSystem` for use with a deployed lakeFS instance.
This guide introduces them in the order of least to most in-Python configuration - the preferred way to use the file system is with as little Python code as possible.

!!! Info
    
    The configuration methods are introduced in reverse order of precedence - config file arguments have the lowest priority and are overwritten by environment variables (if specified).

## The `.lakectl.yaml` configuration file

The easiest way of configuring the lakeFS file system is with a `lakectl` YAML configuration file. To address a lakeFS server, the following minimum configuration is required:

```yaml title="~/.lakectl.yaml"
credentials:
  access_key_id: <ID>
  secret_access_key: <KEY>
server:
  endpoint_url: <LAKEFS-HOST>
```

For a local instance produced by the [quickstart](../quickstart.md), the following values will work:

```yaml title="~/.lakectl.yaml"
credentials:
  access_key_id: AKIAIOSFOLQUICKSTART
  secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
server:
  endpoint_url: http://127.0.0.1:8000
```

To work without any more arguments "out of the box", the configuration file has to be placed in your home directory with the name `.lakectl.yaml` (this is where lakeFS expects it).
If you set all values correctly, you can instantiate the lakeFS file system without any arguments:

```python
from lakefs_spec import LakeFSFileSystem

# zero config necessary.
fs = LakeFSFileSystem()
```

If you cannot use the default location (`$HOME/.lakectl.yaml`), set the `LAKECTL_CONFIG_FILE` environment variable to the desired path before instantiating the file system:

```python
import os
from lakefs_spec import LakeFSFileSystem

os.environ["LAKECTL_CONFIG_FILE"] = "/path/to/my/configfile.yaml"

fs = LakeFSFileSystem()
```

## Setting environment variables

It is also possible to specify configuration values used for authentication with the lakeFS server with environment variables.
These follow the same naming scheme as the `lakectl` CLI and override the corresponding values from a `.lakectl.yaml` config file:

| Environment variable | Config file field |
| --- | --- |
| `LAKECTL_SERVER_ENDPOINT_URL` | `server.endpoint_url` |
| `LAKECTL_CREDENTIALS_ACCESS_KEY_ID` | `credentials.access_key_id` |
| `LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY` | `credentials.secret_access_key` |

```python
import os
from lakefs_spec import LakeFSFileSystem

os.environ["LAKECTL_SERVER_ENDPOINT_URL"] = "http://my-lakefs.host"
os.environ["LAKECTL_CREDENTIALS_ACCESS_KEY_ID"] = "my-access-key-id"
os.environ["LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY"] = "my-secret-access-key"

# also zero-config.
fs = LakeFSFileSystem()
```

!!! Info
    
    Environment variable discovery is handled by the underlying `lakefs` Python client. The `proxy`, `create_branch_ok`, and `source_branch` arguments of `LakeFSFileSystem` have no environment variable counterpart and can only be supplied in Python.

## Appendix: Mixing zero-config methods

Two of the introduced methods allow for "zero-config" (i.e. no arguments given to the constructor) initialization of the file system.
However, care must be taken when working with different file systems configured by the same means (for example, file systems configured with separate environment variables).

The reason for this is the [instance caching mechanism](https://filesystem-spec.readthedocs.io/en/latest/features.html#instance-caching) built into fsspec.
While this allows for efficient reuse of file systems e.g. by third-party libraries (pandas, DuckDB, ...), it can lead to silent misconfigurations. Consider this example, with an existent `.lakectl.yaml` file:

```yaml title="~/.lakectl.yaml"
credentials:
  access_key_id: AKIAIOSFOLQUICKSTART
  secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
server:
  endpoint_url: http://127.0.0.1:8000
```

Now, mixing config file and environment variable initializations leads to the wrong result:

```python
import os
from lakefs_spec import LakeFSFileSystem

# first file system, initialized from the config file
config_fs = LakeFSFileSystem()

os.environ["LAKECTL_SERVER_ENDPOINT_URL"] = "http://my-other-lakefs.host"
os.environ["LAKECTL_CREDENTIALS_ACCESS_KEY_ID"] = "my-access-key-id"
os.environ["LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY"] = "my-secret-access-key"

envvar_fs = LakeFSFileSystem()

print(config_fs is envvar_fs) # <- prints True! 
```

The reason why the above code does not work as desired is that the cached config-file-initialized file system is simply reused on the second assignment.
To clear the file system instance cache, you can run the following:

```python
from lakefs_spec import LakeFSFileSystem

LakeFSFileSystem.clear_instance_cache()
```
