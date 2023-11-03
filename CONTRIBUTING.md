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
3. Dependencies needed for documentation generation are found in the `docs` sections of `project.optional-dependencies`.

After adding the dependency in either of these sections, run the helper script `hack/lock-deps.sh` (which in turn uses `pip-compile`) to pin all dependencies again:

```shell
python -m pip install --upgrade pip-tools
hack/lock-deps.sh
```

In addition to these manual steps, we also provide `pre-commit` hooks that automatically lock the dependencies whenever `pyproject.toml` is changed.

Selective package upgrade for existing dependencies are also handled by the helper script above.
If you want to update the `lakefs-sdk` dependency, for example, simply run:

```shell
hack/lock-deps.sh lakefs-sdk
```

⚠️ Since the official development version is Python 3.11, please run the above commands in a virtual environment with Python 3.11.

## Working on Documentation

Improvements or additions to the project's documentation are highly appreciated.

The documentation is based on the [`mkdocs`](https://mkdocs.org) and [`mkdocs-material`](https://squidfunk.github.io/mkdocs-material/) projects, see their homepages for in-depth guides on their features and usage. We use the [Numpy documentation style](https://numpydoc.readthedocs.io/en/latest/format.html) for Python docstrings.

To build the documentation locally, you need to first install the optional `docs` dependencies from `requirements-docs.txt`,
e.g., with `pip install -r requirements-docs.txt`. You can then start a local documentation server with `mkdocs serve`, or
build the documentation into its output folder in `public/`.

In order to maintain documentation for multiple versions of this library, we use the [`mike`](https://github.com/jimporter/mike) tool, which automatically maintains individual documentation builds per version and publishes them to the `gh-pages` branch.

The GitHub CI pipeline automatically invokes `mike` as part of the release process with the correct version and updates the GitHub pages branch for the project.
