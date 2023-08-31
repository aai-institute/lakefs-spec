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
The flag can also be set in [scoped filesystem behaviour changes](#scoped-filesystem-behavior-changes) .

### Paths and URIs

The lakeFS filesystem expects URIs that follow the [lakeFS protocol](https://docs.lakefs.io/understand/model.html#lakefs-protocol-uris).
URIs need to have the form `lakefs://<repo>/<ref>/<resource>`, with the repository name, the ref name (either a branch name or a commit SHA, depending on the operation), and resource name.
The resource can be a single file name, or a directory name for recursive operations.

### Client-side caching

In order to reduce the number of IO operations, you can enable client-side caching of both uploaded and downloaded files.
Caching works by calculating the MD5 checksum of the local file, and comparing it to that of the lakeFS remote file.
If they match, the operations are cancelled, and no additional client-server communication (including up- and downloads) happens.

Client-side caching is enabled by default in the lakeFS file system, and can be controlled through the `precheck_files` argument in the constructor:

```python
from lakefs_spec import LakeFSFileSystem

# The default setting, precheck_files=False disables client-side caching.
fs = LakeFSFileSystem(host="localhost:8000", precheck_files=True)
```

### Automatic commit creation with a commit hook

Some operations, like `fs.put()` or `fs.rm()`, change the state of a lakeFS repository by changing files. According to
the lakeFS working model, these changes are tracked as _uncommitted changes_, similarly to the git version control system.

With `lakefs-spec`, you can optionally commit changes caused by file system operations directly after they are made,
by using a **commit hook**. A commit hook is a Python function taking the `fsspec` event that caused the changes
(e.g. `put` or `rm`), as well as a context object containing useful information like the repository, branch name,
changed resource, and the lakeFS diff, and returning a `CommitCreation` object that is then used by
lakeFS to create a commit on the chosen branch.

An example of a commit hook:

```python
from lakefs_client.models import CommitCreation
from lakefs_spec.commithook import FSEvent, HookContext

def my_commit_hook(event: FSEvent, ctx: HookContext) -> CommitCreation:
    if event == FSEvent.RM:
        message = f"❌ Remove file {ctx.resource}"
    else:
        message = f"✅ Add file {ctx.resource}"

    return CommitCreation(message=message)
```

To enable automatic commits after stateful filesystem operations, set `postcommit = True` in the filesystem constructor. If you
would like to use your own commit hook, supply a Python callable with the aforementioned signature as the `commithook` argument:

```python
from lakefs_spec import LakeFSFileSystem

# use the example commit hook from above
fs = LakeFSFileSystem(host="localhost:8000", postcommit=True, commithook=my_commit_hook)
```

### Scoped filesystem behavior changes

To selectively enable or disable automatic commits, client-side caching, or automatic branch creation, you can use a `scope` context manager:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem(host="localhost:8000")

with fs.scope(precheck_files=False):
    # get a fresh version of the file by disabling caching checks
    fs.get("lakefs://my-repo/my-branch/my-file.txt", "my-file.txt")

# do something with the text file...
...

# create a commit on upload by enabling automatic commits in a scoped section
with fs.scope(postcommit=True):
    fs.put("my-file.txt", "lakefs://my-repo/my-branch/my-new-file.txt")
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

LakeFSFileSystem._cache.clear()
```

## Developing and contributing to `lakefs-spec`

We welcome contributions to the project! For information on the general development workflow, head over to the [contribution guide](CONTRIBUTING.md).
