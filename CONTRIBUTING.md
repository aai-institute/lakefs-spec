# Developing on `lakefs-spec`

Thank you for your interest in contributing to this project!

We appreciate issue reports, pull requests for code and documentation,
as well as any project-related communication through [GitHub Discussions](https://github.com/appliedAI-Initiative/lakefs-spec/discussions).

## Quickstart

To get started with development, you can follow these steps:

1. Clone this repository:

    ```shell
    git clone https://github.com/appliedAI-Initiative/lakefs-spec.git
    ```

2. Navigate to the directory and install the development dependencies into a virtual environment:

    ```shell
    cd lakefs-spec
    python3 -m venv venv --system-site-packages
    source venv/bin/activate
    python -m pip install -r requirements.txt -r requirements-dev.txt
    python -m pip install -e . --no-deps
    ```

3. After making your changes, verify they adhere to our Python code style by running `pre-commit`:
    
    ```shell
    pre-commit run --all-files
    ```

    You can also set up Git hooks through `pre-commit` to perform these checks automatically:
    
    ```shell
    pre-commit install
    ```

4. To run the tests against an ephemeral lakeFS instance, you just run `pytest`:
    ```shell
    pytest
    ```

    To spin up a local lakeFS instance quickly for testing, you can use the Docker Compose file bundled with this repository:

    ```shell
    docker-compose -f hack/docker-compose.yml up
    ```

## Updating dependencies

Dependencies should stay locked for as long as possible, ideally for a whole release.
If you have to update a dependency during development, you should do the following:

1. If it is a core dependency needed for the package, add it to the `dependencies` section in the `pyproject.toml`.
2. In case of a development dependency, add it to the `dev` section of the `project.optional-dependencies` table instead.

After adding the dependency in either of these sections, run `pip-compile` to pin all dependencies again:

```shell
python -m pip install --upgrade pip-tools
pip-compile --strip-extras pyproject.toml
pip-compile --extra=dev --output-file=requirements-dev.txt pyproject.toml
```

⚠️ Since the official development version is Python 3.11, please run the above `pip-compile` command in a virtual environment with Python 3.11.

## A note on local development with poetry

Since `lakefs-spec` relies on a `setuptools` entry point for registration, it will not be registered by `poetry` upon installation, since that uses a different build backend.

To register the lakeFS filesystem with poetry in local development, add the following to your `pyproject.toml` file:

```toml
[tool.poetry.plugins."fsspec.specs"]
"lakefs" = "lakefs_spec.LakeFSFileSystem"
```
