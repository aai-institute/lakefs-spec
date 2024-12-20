# Quickstart

Welcome! This quickstart guide will get you up and running with lakeFS-spec by showing you how to

1. [install the `lakefs-spec` package](#installing-lakefs-spec),
1. [spin up a local lakeFS server](#spinning-up-a-local-lakefs-instance),
1. [create a lakeFS repository for experimentation](#create-a-lakefs-repository), and
1. [perform basic file system operations](#using-the-lakefs-file-system)
in a lakeFS repository using lakeFS-spec.

??? info "Prerequisites"

    To follow along with this guide, you will need a few prerequisites ready on your machine:

    - lakeFS-spec supports Windows, macOS, or Linux
    - [Docker](https://www.docker.com/get-started/), with Docker Compose
    - [Python 3.10](https://python.org) or later
    - optionally, [`lakectl`](https://docs.lakefs.io/reference/cli.html), the lakeFS command line tool

    Please take a moment to make sure you have these tools available before proceeding with the next steps.

## Installing lakeFS-spec

??? tip "A note on virtual environments"

    We generally recommend installing the library in a virtual environment to ensure proper isolation, especially when following this quickstart guide.

    If you are using uv, virtual environments can automatically be created by the tool.

    If you prefer the `venv` functionality built into Python, see the [official docs](https://docs.python.org/3/library/venv.html) (tl;dr: `python -m venv venv; source venv/bin/activate`).

To install the package directly from PyPI, run:

=== "pip"

    ```
    pip install lakefs-spec
    ```

=== "uv"

    ```
    uv add lakefs-spec
    ```

Or, if you want to try the latest pre-release version directly from GitHub:

=== "pip"

    ```
    pip install git+https://github.com/aai-institute/lakefs-spec.git
    ```

=== "uv"

    ```
    uv add git+https://github.com/aai-institute/lakefs-spec.git
    ```

## First Steps

### Spinning up a local lakeFS instance

!!! warning
    This setup is not recommended for production uses, since it does not store the data persistently.

    Please check out the [lakeFS docs](https://docs.lakefs.io/howto/deploy/) for production-ready deployment options.

If you don't already have access to a lakeFS server, you can quickly start a local instance using Docker Compose. Before continuing, please make sure Docker is installed and running on your machine.

The lakeFS quickstart deployment can be launched directly with a [configuration file](https://github.com/aai-institute/lakefs-spec/blob/main/hack/compose.yml) provided in the lakeFS-spec repository:

```shell
$ curl https://raw.githubusercontent.com/aai-institute/lakefs-spec/main/hack/compose.yml | docker-compose -f - up
```

If you do not have `curl` installed on your machine or would like to examine and/or customize the container configuration, you can also create a `compose.yml` file locally and use it with `docker-compose up`:

```yaml title="compose.yml"
--8<-- "https://raw.githubusercontent.com/aai-institute/lakefs-spec/main/hack/compose.yml:3:"
```

In order to allow lakeFS-spec to automatically discover credentials to access this lakeFS instance, create a `.lakectl.yaml` in your home directory containing the credentials for the quickstart environment (you can also use `lakectl config` to create this file interactively if you have the `lakectl` tool installed on your machine):

```yaml title="~/.lakectl.yaml"
credentials: # (1)!
  access_key_id: AKIAIOSFOLQUICKSTART
  secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
server:
  endpoint_url: http://127.0.0.1:8000
```

1. These must match the credentials set in the `environment` section of the Docker Compose file above

After the container has finished initializing, you can access the [web UI](http://localhost:8000) of your local lakeFS deployment in your browser. Fill out the setup form, where you can optionally share your email address with the developers of lakeFS to receive updates on their product. Next, you can log into your fresh lakeFS instance with the credentials listed above.

!!! success
    Your fresh local lakeFS instance is a playground for you to explore lakeFS functionality. 
    
    In the next step, we will create your first repository on this server.

### Create a lakeFS repository

Once you have logged into the web UI of the lakeFS server for the first time, you can create an empty repository on the next page.
Click the small _Click here_ link at the bottom of the page to proceed and create a repository named `repo` (we don't want to add the sample data for this guide):

![](_images/quickstart-lakefs-sample-repo.png)

??? tip "Tip: Creating a repository later"

    If you have inadvertently skipped over the quickstart repository creation page, you can always create a new repository on the [_Repositories_ tab](http://localhost:8000/repositories) in the lakeFS web UI (and optionally choose to add the sample data):

    ![](_images/quickstart-lakefs-repositories.png)

!!! success
    You have successfully created a lakeFS repository named `repo`, ready to be used with lakeFS-spec.

### Using the lakeFS file system

We will now use the lakeFS-spec file system interface to perform some basic operations on the repository created in the previous step:

* Upload a local file to the repository
* Read data from a file in the repository
* Make a commit
* Fetch metadata about repository contents
* Delete a file from the repository

To get started, create a file called `quickstart.py` with the following contents:

```python title="quickstart.py"
--8<-- "docs/_code/quickstart.py::9"
```

!!! tip
    We will keep adding more code to this file as we progress through the next steps.
    Feel free to execute the script after each step and observe the effects as noted in the guide.

This code snippet prepares a file `demo.txt` on your machine, ready to be added to the lakeFS repository, so let's do just that:

```python
--8<-- "docs/_code/quickstart.py:12:17"
```

If you execute the `quickstart.py` script at this point, you can already see the [committed file](http://localhost:8000/repositories/repo/object?ref=main&path=demo.txt) in the lakeFS web UI:

![](_images/quickstart-lakefs-ui.png)

While examining the file contents in the browser is nice, we want to access the committed file programmatically. Add the following lines at the end of your script and observe the output:

```python
--8<-- "docs/_code/quickstart.py:20:21"
```

Note that executing the same code multiple times will only result in a single commit in the repository since the contents of the file on disk and in the repository are identical.

In addition to simple read and write operations, the fsspec file system interface also allows us to list the files in a repository folder using `ls`, and query the metadata of objects in the repository through `info` (akin to the POSIX `stat` system call).
Let's add the following code to our script and observe the output:

```python
--8<-- "docs/_code/quickstart.py:23:30"
```

As the last order of business, let's clean up the repository to its original state by removing the file using the `rm` operation and creating another commit (also, the local file is deleted, since we don't need it anymore):

```python
--8<-- "docs/_code/quickstart.py:33:36"
```

!!! success

    You now have all the basic tools available to version data from your Python code using the file system interface provided by lakeFS-spec.

??? tip "Full example code"

    ```python title="quickstart.py"
    --8<-- "docs/_code/quickstart.py"
    ```

## Next Steps

After this walkthrough of the installation and an introduction to basic file system operations using lakeFS-spec, you might want to consider more advanced topics:

- [API Reference](reference/lakefs_spec/spec.md)
- [User Guide](guides/index.md), in particular
    - [How to use the lakeFS file system](guides/filesystem-usage.md)
    - [How to use lakeFS-spec with third-party data science libraries](guides/integrations.md)
- [Tutorial: Using lakeFS-spec in a data science project](tutorials/demo_data_science_project.ipynb)
