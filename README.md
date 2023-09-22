# lakefs-spec: An `fsspec` implementation for lakeFS

This repository contains a [filesystem-spec](https://github.com/fsspec/filesystem_spec) implementation for the [lakeFS](https://lakefs.io/) project.
Its main goal is to facilitate versioned data operations in lakeFS directly from Python code, for example using `pandas`. See the [examples](#usage) below for inspiration.

## Installation

To install the package directly from PyPI via `pip`, run

```shell
pip install --upgrade pip
pip install lakefs-spec
```

or, for the bleeding edge version,

```shell
pip install git+https://github.com/appliedAI-Initiative/lakefs-spec.git
```

To add the project as a dependency using `poetry`, use

```shell
poetry add lakefs-spec
```

or, for the development version,

```shell
poetry add git+https://github.com/appliedAI-Initiative/lakefs-spec.git
```

## Usage

As an example showcase, we use the lakeFS file system to read a Pandas `DataFrame` directly from a branch. To follow
this small tutorial, you should first complete Step 1 in the [lakeFS quickstart](https://docs.lakefs.io/quickstart/launch.html) by
launching an instance, and then creating a pre-populated repository by clicking the green button on the login page.

Then, run the following code to download the sample dataframe directly from the `main` branch:

```python
import pandas as pd

# change these settings to match your instance's address and credentials
storage_options={
    "host": "localhost:8000",
    "username": "username",
    "password": "password",
}

df = pd.read_parquet('lakefs://quickstart/main/lakes.parquet', storage_options=storage_options)
```

You can then update data in LakeFS like so:

```python
df.to_csv('lakefs://quickstart/main/lakes.parquet', storage_options=storage_options)
```

If the target file does not exist, it is created, otherwise, the existing file is updated.

If the specified branch does not exist, it is created by default. This behaviour can be set in the filesystem constructor via the `create_branch_ok` flag.

```python
from lakefs_spec import LakeFSFileSystem

# create_branch_ok=True (the default setting) enables implicit branch creation
fs = LakeFSFileSystem(host="localhost:8000", create_branch_ok=False)
```

If set to `create_branch_ok = False`, adressing non-existing branches causes an error.
The flag can also be set in scoped filesystem behaviour changes. Like so 

```python
with fs.scope(create_branch_ok=False)
    fs.put('lakefs://quickstart/test/lakes.parquet')
```
This code throws an error should the `test` branch not exist. 

### Paths and URIs

The lakeFS filesystem expects URIs that follow the [lakeFS protocol](https://docs.lakefs.io/understand/model.html#lakefs-protocol-uris).
URIs need to have the form `lakefs://<repo>/<ref>/<resource>`, with the repository name, the ref name (either a branch name or a commit SHA, depending on the operation), and resource name.
The resource can be a single file name, or a directory name for recursive operations.

### Client-side caching

In order to reduce the number of IO operations, you can enable client-side caching of both uploaded and downloaded files.
Caching works by calculating the MD5 checksum of the local file, and comparing it to that of the lakeFS remote file.
If they match, the operations are cancelled, and no file up- or downloads happen.

Client-side caching can be controlled through the boolean `precheck` argument in the `fs.get` and `fs_put` methods and
their more granular single-file counterparts `fs.get_file` and `fs.put_file`.

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem(host="localhost:8000")

# The default is precheck=True, you can force the operation by setting precheck=False.
fs.get_file("my-repo/my-ref/file.txt", "file.txt", precheck=True)
```

### Creating lakeFS automations in Python with `LakeFSFileSystem` hooks

LakeFS has a variety of administrative APIs available through its Python client library.
Within `lakefs-spec`, you can register hooks to your `LakeFSFileSystem` to run code after file system operations.
A hook needs to have the signature `(client, context) -> None`, where the `client` argument holds the 
file system's lakeFS API client, and the `context` object contains information about the requested resource (repository, ref/branch, name).

As an example, the following snippet installs a lakeFS hook that creates a commit on the lakeFS branch after a file upload:

```python
from lakefs_client.client import LakeFSClient

from lakefs_spec import LakeFSFileSystem
from lakefs_spec.client_helpers import commit
from lakefs_spec.hooks import FSEvent, HookContext

def create_commit_on_put(client: LakeFSClient, ctx: HookContext) -> None:
    message = f"Add file {ctx.resource}"
    commit(client, repository=ctx.repository, branch=ctx.ref, message=message)

fs = LakeFSFileSystem()

fs.register_hook(FSEvent.PUT_FILE, create_commit_on_put)

# creates a commit with the message "Add file my-file.txt" after the file put.
fs.put_file("my-file.txt", "my-repo/my-branch/my-file.txt")
```

### Implicit initialization and instance caching

Aside from explicit initialization, you can also use environment variables and a configuration file (by default `~/.lakectl.yaml`) to initialize a lakeFS file system.
The environment variables for the lakeFS client arguments are the names of the constructor arguments prefixed with `LAKEFS_`:

```python
import os
from lakefs_spec import LakeFSFileSystem

os.environ["LAKEFS_HOST"] = "localhost:8000"
os.environ["LAKEFS_USERNAME"] = "username"
os.environ["LAKEFS_PASSWORD"] = "password"

fs = LakeFSFileSystem()
```

To initialize the lakeFS file system from a `lakectl` YAML configuration file, you can specify the `configfile` argument.

```python
from lakefs_spec import LakeFSFileSystem

# No argument means the default config (~/.lakectl.yaml) will be used.
fs = LakeFSFileSystem(configfile="path/to/my/lakectl.yaml")
```

⚠️ To be able to read settings from a YAML configuration file, `pyyaml` has to be installed. You can do this by installing `lakefs-spec` together with the `yaml` extra:

```shell
pip install --upgrade lakefs-spec[yaml]
```

### A note on mixing environment variables and `lakectl` configuration files

lakeFS file system instances are cached, and existing lakeFS instances are reused from an instance cache when requested.

For implicit initialization from environment variables and configuration files as described above, this means that whichever initialization method is used first populates the cache -
thus, when using the other method, a cache hit happens and no new instance is created. This can lead to surprising misconfigurations:

```python
import os
from lakefs_spec import LakeFSFileSystem

# set envvars
os.environ["LAKEFS_HOST"] = "localhost:8000"
os.environ["LAKEFS_USERNAME"] = "username"
os.environ["LAKEFS_PASSWORD"] = "password"

# creates a cache entry for the bare instance
fs = LakeFSFileSystem()

# ~/.lakectl.yaml
#  server:
#    endpoint_url: http://example-host

# this time, try to read in the default lakectl config, with http://example-host set as host.
fs = LakeFSFileSystem()
print(fs.client._api.configuration.host) # <- prints localhost:8000!
```

The best way to avoid this is to commit to only using either environment variables or `lakectl` configuration files.
If you do have to mix both methods, you can clear the instance cache like so:

```python
from lakefs_spec import LakeFSFileSystem

LakeFSFileSystem.clear_instance_cache()
```

## Developing and contributing to `lakefs-spec`

We welcome contributions to the project! For information on the general development workflow, head over to the [contribution guide](CONTRIBUTING.md).
