# Contributing to lakeFS-spec

Thank you for your interest in contributing to this project!

We appreciate issue reports, pull requests for code and documentation,
as well as any project-related communication through [GitHub Discussions](https://github.com/aai-institute/lakefs-spec/discussions).

## Getting Started

To get started with development, you can follow these steps (requires an installation of `uv`):

1. Clone this repository:

    ```shell
    git clone https://github.com/aai-institute/lakefs-spec.git
    ```

2. Navigate to the directory and install the development dependencies into a virtual environment:

    ```shell
    cd lakefs-spec
    uv sync --all-groups
    ```

3. After making your changes, verify they adhere to our Python code style by running `pre-commit`:
    
    ```shell
    uv run pre-commit run --all-files
    ```

    You can also set up Git hooks through `pre-commit` to perform these checks automatically:
    
    ```shell
    uv run pre-commit install
    ```

4. To run the tests against an ephemeral lakeFS instance, you just run `pytest`:
    ```shell
    uv run pytest
    ```

    To spin up a local lakeFS instance quickly for testing, you can use the Docker Compose file bundled with this repository:

    ```shell
    docker-compose -f hack/compose.yml up
    ```

## Updating dependencies

Dependencies should stay locked for as long as possible, ideally for a whole release.
If you have to update a dependency during development, you should do the following:

1. If it is a core dependency needed for the package, add it to the `dependencies` section in the `pyproject.toml` via `uv add <dep>`.
2. In case of a development dependency, add it to the `dev` section of the `project.dependency-groups` table instead (`uv add --group dev <dep>`).
3. Dependencies needed for documentation generation are found in the `docs` sections of `project.dependency-groups` (`uv add --group docs <dep>`).

After adding the dependency in either of these sections, lock all dependencies again:

```shell
uv lock
```

## Working on Documentation

Improvements or additions to the project's documentation are highly appreciated.

The documentation is based on the [MkDocs](http://mkdocs.org) and [Material for MkDocs (`mkdocs-material`)](https://squidfunk.github.io/mkdocs-material/) projects, see their homepages for in-depth guides on their features and usage. We use the [Numpy documentation style](https://numpydoc.readthedocs.io/en/latest/format.html) for Python docstrings.

To build the documentation locally, you need to first install the optional `docs` dependencies from `pyproject.toml`, e.g., with `uv sync --group docs`.
You can then start a local documentation server with `uv run mkdocs serve`, or build the documentation into its output folder in `public/`.

In order to maintain documentation for multiple versions of this library, we use the [mike](https://github.com/jimporter/mike) tool, which automatically maintains individual documentation builds per version and publishes them to the `gh-pages` branch.

The GitHub CI pipeline automatically invokes `mike` as part of the release process with the correct version and updates the GitHub pages branch for the project.
