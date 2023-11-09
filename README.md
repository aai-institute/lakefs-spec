[![](https://img.shields.io/pypi/v/lakefs-spec)](https://pypi.org/project/lakefs-spec) ![GitHub](https://img.shields.io/github/license/appliedAI-Initiative/lakefs-spec) [![docs](https://img.shields.io/badge/docs-latest-blue)](https://appliedai-initiative.github.io/lakefs-spec/)
 ![GitHub](https://img.shields.io/github/stars/appliedAI-Initiative/lakefs-spec)

# lakefs-spec: An `fsspec` implementation for lakeFS

This repository contains a [filesystem-spec](https://github.com/fsspec/filesystem_spec) implementation for the [lakeFS](https://lakefs.io/) project.
Its main goal is to facilitate versioned data operations in lakeFS directly from Python code, for example using `pandas`. Data versioning enables reproducibility of experiments - a best practice in machine learning.

See the examples below ([features](#usage), [versioning best-practices](#reproducibility-through-data-versioning-with-lakefs-and-lakefs-spec)) below for inspiration.

A more detailed example is in the notebook in the [`/demos` directory](/demos/demo_data_science_project.ipynb).

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
df.to_parquet('lakefs://quickstart/main/lakes.parquet', storage_options=storage_options)
```

If the target file does not exist, it is created, otherwise, the existing file is updated.

If the specified branch does not exist, it is created by default. This behaviour can be set in the filesystem constructor via the `create_branch_ok` flag.

```python
from lakefs_spec import LakeFSFileSystem

# create_branch_ok=True (the default setting) enables implicit branch creation.
fs = LakeFSFileSystem(create_branch_ok=False)
```

If set to `create_branch_ok = False`, adressing non-existing branches causes an error.
The flag can also be set in scoped filesystem behaviour changes:

```python
with fs.scope(create_branch_ok=False):
    fs.put("lakes.parquet", 'quickstart/test/lakes.parquet')
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

fs = LakeFSFileSystem()

# The default is precheck=True, you can force the operation by setting precheck=False.
fs.get_file("my-repo/my-ref/file.txt", "file.txt", precheck=True)
```

### Creating lakeFS automations in Python with `LakeFSFileSystem` transactions

LakeFS has a variety of administrative APIs available through its Python client library.
Within `lakefs-spec`, you can use transactions to define a versioning workflow in your file uploads.

As an example, the following snippet creates a transaction that creates a commit on the given lakeFS branch after a file upload:

```python
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

with fs.transaction as tx:
    # creates a commit with the message "Add file my-file.txt" after the file put.
    fs.put_file("my-file.txt", "my-repo/my-branch/my-file.txt", autocommit=False)
    tx.commit("my-repo", "my-branch", message="Add file my-file.txt")
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

⚠️ To be able to read settings from a YAML configuration file, `pyyaml` has to be installed, for example using `pip`:

```shell
pip install --upgrade pyyaml
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

## Reproducibility through data versioning with lakeFS and lakeFS-spec

Here we briefly show an example how data versioning for the reproducibility of machine learning experiments can be achieved using lakeFS-spec. We do this via python-like pseudocode.

First, we ingest the data into our versioning system.

```python

# Data ingestion
import pandas as pd
from lakefs_spec.client_helpers import commit

raw = pd.read_csv('local-path-to.csv')
raw.to_csv('lakefs://<lakeFS-uri')
commit('raw data ingestion')
```

This commit function creates a commit with a unique SHA which you can get from the lakeFS user interface or withthe `lakefs_spec.client_helpers.get_tags` function.

The SHA points to a specific state of the dataset.

```python
raw_df = pd.read_csv('lakefs://<lakeFS-commit-SHA>')
prep_df = preprocess(raw_df)
trained_model = model.fit(prep_df)
acc = trained_model.eval()

experiment_tracking.log('Data version','lakefs://<lakeFS-commit-SHA>')
experiment_tracking.log('Code version','<git commit SHA of this code>')
experiment_tracking.log('Accuracy', acc)
```

Now, with the data and code version identified by a specific commit sha you will always reproduce the same experiment outcomes, e.g. `acc`.

## Developing and contributing to `lakefs-spec`

We welcome contributions to the project! For information on the general development workflow, head over to the [contribution guide](CONTRIBUTING.md).
