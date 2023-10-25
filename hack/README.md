# Useful resources for development on `lakefs-spec`

This document contains information on the resources in this directory, and how they can be used in development and testing.

## `docker-compose.yml` - local lakeFS quickstart instance

This Docker Compose file bootstraps a local [lakeFS quickstart instance](https://docs.lakefs.io/quickstart/launch.html).
It does not come with an associated volume, so you can spin it up and down without creating dangling resources.

Requirements:
* A Docker runtime & CLI with `docker compose`.

To bootstrap, run the following command:

```shell
docker compose -f hack/docker-compose.yml up
```

To stop the container again, exit with `Ctrl-c`.

## `lakefs-s3-local.yml` - lakeFS with a local, SeaweedFS-backed S3 blockstore

For simulating a lakeFS deployment with a remote blockstore, the `lakefs-s3-local.yml` Docker Compose file contains a
recipe with a local S3 blockstore implementation using [SeaweedFS](https://github.com/seaweedfs/seaweedfs/wiki).

To bootstrap this setup, run the command

```shell
docker compose -f hack/lakefs-s3-local.yml up
```

To stop the containers again, exit with `Ctrl-C`.

To clean the created volume, e.g., for when you want to remove created storage namespaces after repository deletions,
you can remove the container and attached volume like so:

```shell
docker compose -f hack/lakefs-s3-local.yml rm -v
```

In order to write to the local S3 blockstore using `LakeFSFileSystem.put_file_to_blockstore`, you can use the following
environment variables and values:

```shell
export AWS_ENDPOINT_URL="http://localhost:9001"
export AWS_ACCESS_KEY_ID=sandbox
export AWS_SECRET_ACCESS_KEY=sandbox
```

or, in Python directly,

```python
import os

os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9001"
os.environ["AWS_ACCESS_KEY_ID"] = "sandbox"
os.environ["AWS_SECRET_ACCESS_KEY"] = "sandbox"
```

If you prefer working with AWS credential files, you can run the following setup command:

```shell
cat > $HOME/.aws/credentials <<EOL
[default]
endpoint_url = http://localhost:9001
aws_access_key_id = sandbox
aws_secret_access_key = sandbox
```

Beware that this will overwrite an existing AWS credential file, so it is recommended to back it up first.
