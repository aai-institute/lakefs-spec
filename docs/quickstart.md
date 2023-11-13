# Quickstart

This quickstart guide will show you how to

1. [Install the `lakefs-spec` package](#installing),
1. [spin up a local lakeFS server](#spinning-up-a-local-lakefs-instance),
1. [create a lakeFS repository for experimentation](#create-a-lakefs-repository), and
1. [perform basic file system operations](#using-the-lakefs-fsspec-file-system)
in a lakeFS repository using `lakefs-spec`.

## Installing `lakefs-spec`

`lakefs-spec` can be used on any platform and requires at least Python 3.9.

To install the package directly from PyPI, run:

=== "pip"

    ```
    pip install lakefs-spec
    ```

=== "poetry"

    ```
    poetry add lakefs-spec
    ```

Or, if you want to try the latest pre-release version directly from GitHub:

=== "pip"

    ```
    pip install git+https://github.com/appliedAI-Initiative/lakefs-spec.git
    ```

=== "poetry"

    ```
    poetry add git+https://github.com/appliedAI-Initiative/lakefs-spec.git
    ```

??? tip "Virtual Environments"

    Consider installing the library in a separate virtual environment.

    If you are using Poetry, virtual environments can automatically be created by the tool.

    If you prefer the `venv` functionality built into Python, see the [official docs](https://docs.python.org/3/library/venv.html).

## First Steps

### Spinning up a local lakeFS instance

!!! warning
    This setup is not recommended for production uses, since it does not store the data persistently.
    Please check out the [lakeFS docs](https://docs.lakefs.io/howto/deploy/) for production-ready deployment options.

If you don't already have access to a lakeFS server, you can quickly start a local instance using Docker Compose with a [configuration file](https://github.com/appliedAI-Initiative/lakefs-spec/blob/main/hack/docker-compose.yml) provided in the `lakefs-spec` repository:

```shell
$ curl https://raw.githubusercontent.com/appliedAI-Initiative/lakefs-spec/main/hack/docker-compose.yml | docker-compose -f - up
```

If you do not have `curl` installed on your machine or would like to examine and/or customize the container configuration, you can also create a `docker-compose.yml` file locally and use it with `docker-compose up`:

```yaml
--8<-- "https://raw.githubusercontent.com/appliedAI-Initiative/lakefs-spec/main/hack/docker-compose.yml:3:"
```

In order to allow `lakefs-spec` to automatically discover credentials to access this lakeFS instance, create a `.lakectl.yaml` in your home directory containing the following:

```yaml
credentials:
  access_key_id: AKIAIOSFOLQUICKSTART
  secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
server:
  endpoint_url: http://127.0.0.1:8000
```

After the container has finished initializing, you can access the [web UI](http://localhost:8000) of your local lakeFS deployment in your browser. Fill out the setup form, where you can optionally share your email address with the developers of lakeFS to receive updates on their product. Next, you can log into your fresh lakeFS instance with the credentials listed above.

!!! success
    Your fresh local lakeFS instance is a playground for you to explore lakeFS functionality. 
    
    In the next step, we will create your first repository on this server.

### Create a lakeFS repository

Once you have logged into the web UI of the lakeFS server for the first time, you can create a quickstart repository containing sample data on the next page. Click the _Create Sample Repository_ button to proceed:

![](_images/quickstart-lakefs-sample-repo.png)

??? tip "Tip: Creating a repository later"

    If you have inadvertently skipped over the quickstart repository creation page, you can always create a new repository on the [_Repositories_ tab](http://localhost:8000/repositories) in the lakeFS web UI (and optionally choose to add the same sample data):

    ![](_images/quickstart-lakefs-repositories.png)

!!! success
    You have successfully created a lakeFS repository named `quickstart`, ready to be used with `lakefs-spec`.

### Using the lakeFS `fsspec` file system

While the `quickstart` repository already contains some files, we won't be using them for the remainder of this guide. Instead, we will now use the `lakefs-spec` file system interface to upload a file to the repository we just created, make a commit, and read back the committed data.

To get started, create a file called `quickstart.py` with the following contents:

```python
--8<-- "docs/_code/quickstart.py::13"
```

This code snippet prepares a file `demo.txt` on your machine, ready to be added to the lakeFS repository, so let's do just that:

```python
--8<-- "docs/_code/quickstart.py:14:16"
```

If you execute the `quickstart.py` script at this point, you can already see the [committed file](http://localhost:8000/repositories/repo/object?ref=main&path=demo.txt) in the lakeFS web UI:

![](_images/quickstart-lakefs-ui.png)

While examining the file contents in the browser is nice, we want to access the committed file programmatically. Add the following lines at the end of your script and observe the output:

```python
--8<-- "docs/_code/quickstart.py:18:19"
```

Note that executing the same code multiple times will only result in a single commit in the repository since the contents of the file on disk and in the repository are identical.

!!! success

    You now have all the basic tools available to version data from your Python code using the file system interface provided by `lakefs-spec`.

??? tip "Full example code"

    ```python
    --8<-- "docs/_code/quickstart.py"
    ```

## Next Steps

After this walkthrough of the installation and an introduction to basic file system operations using `lakefs-spec`, you might want to consider more advanced topics:

- [Use Cases for `lakefs-spec`](/use-cases)
- [API Reference](/reference/lakefs_spec)
- [TODO: User Guide](/guides/overview/)
