Thank you for your interest in contributing to this project!

In order to get started with development, you can follow these steps:

1. Clone this repository:
```shell
git clone https://github.com/appliedAI-Initiative/lakefs-spec.git
```
2. Navigate to the directory and install the development dependencies into a virtual environment:
```shell
cd lakefs-spec
python3 -m venv venv --system-site-packages
source venv/bin/activate
python -m pip install -r dev-deps.lock
python -m pip install -e . --no-deps
```
3. After making your changes, verify they adhere to our Python code style by running `pre-commit`:
```shell
pre-commit run --all-files
```
4. To run the tests against a lakeFS instance, you can do the following:
```shell
# assumes that your instance is available on port 8000 on localhost
LAKEFS_HOST=localhost:8000 LAKEFS_ACCESS_KEY_ID="MY-KEY" LAKEFS_SECRET_ACCESS_KEY="MY-SECRET" pytest
```
For information on how to quickly spin up lakeFS instances for testing, you can refer to the [lakeFS quickstart](https://docs.lakefs.io/quickstart/).
