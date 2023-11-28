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

If you cannot use the default location (`$HOME/.lakectl.yaml`), you can read a file from any other location by passing the `configfile` argument:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem(configfile="/path/to/my/configfile.yaml")
```

## Setting environment variables

It is also possible to specify certain configuration values used for authentication with the lakeFS server with environment variables.
For these values, the variable name is exactly the constructor argument name prefaced with `LAKEFS_`, e.g. the `host` argument can be set via the `LAKEFS_HOST` environment variable.

```python
import os
from lakefs_spec import LakeFSFileSystem

os.environ["LAKEFS_HOST"] = "http://my-lakefs.host"
os.environ["LAKEFS_USERNAME"] = "my-username"
os.environ["LAKEFS_PASSWORD"] = "my-password"

# also zero-config.
fs = LakeFSFileSystem()
```

!!! Info
    
    Not all initialization values can be set via environment variables - the `proxy`, `create_branch_ok`, and `source_branch` arguments can only be supplied in Python.

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

os.environ["LAKEFS_HOST"] = "http://my-other-lakefs.host"
os.environ["LAKEFS_USERNAME"] = "my-username"
os.environ["LAKEFS_PASSWORD"] = "my-password"

envvar_fs = LakeFSFileSystem()

print(config_fs is envvar_fs) # <- prints True! 
```

The reason why the above code does not work as desired is that the cached config-file-initialized file system is simply reused on the second assignment.
To clear the file system instance cache, you can run the following:

```python
from lakefs_spec import LakeFSFileSystem

LakeFSFileSystem.clear_instance_cache()
```
