# ---
# jupyter:
#   jupytext:
#     cell_markers: '"""'
#     cell_metadata_filter: -all
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
"""
# Data Science with lakeFS-spec

In this notebook, we will complete a small end-to-end data science tutorial that employs lakeFS-spec for data versioning.
We will use versioned weather data to train a decision tree classifier to predict whether it is raining tomorrow given the current weather.

We will do the following:

* Environment setup
* LakeFS setup
* Authenticating with the lakeFS server
* Data ingestion via transactions
* Model training
* Updating data and retraining a model
* Accessing data versions and reproducing experiments
* Using tags for semantic versioning

!!! tip "Local Execution"
    If you want to execute the code in this tutorial as a Jupyter notebook yourself, download the `demo_data_science_project.py` file from the lakeFS-spec repository.

    You can then convert the Python file to a notebook using [Jupytext](https://jupytext.readthedocs.io/en/latest/using-cli.html) using the following command: `jupytext --to notebook demo_data_science_project.py`.

This tutorial assumes that you have installed lakeFS-spec in a virtual environment, and that you have followed the [quickstart guide](../quickstart.md) to set up a local lakeFS instance.

## Environment setup

Install the necessary libraries for this notebook on the environment you have just created:
"""

# %% tags=["Remove_single_output"]
# %pip install numpy pandas scikit-learn

# %% [markdown]
"""
Also install an appropriate lakeFS-spec version, which can be either the latest release from PyPI via `pip install --upgrade lakefs-spec`, or the development version from GitHub via `pip install git+https://github.com/aai-institute/lakefs-spec.git`.
"""

# %% [markdown]
"""
## lakeFS Setup

With Docker Desktop or a similar runtime running set up lakeFS by executing the following `docker run` command (from the [lakeFS quickstart](https://docs.lakefs.io/quickstart/launch.html)) in your console:

```shell
docker run --name lakefs --pull always --rm --publish 8000:8000 treeverse/lakefs:latest run --quickstart
```

You find the authentication credentials in the terminal output. The default address for the local lakeFS GUI is http://localhost:8000/.

## Authenticating with the lakeFS server

There are multiple ways to authenticate with lakeFS from Python code. In this tutorial, we choose the YAML file configuration.
By executing the cell below, you will download a YAML file containing the default lakeFS quickstart credentials and server URL to your user directory.
"""

# %% tags=["Remove_all_output"]
import os
import tempfile
import urllib.request
from pathlib import Path

urllib.request.urlretrieve(
    "https://raw.githubusercontent.com/aai-institute/lakefs-spec/main/docs/tutorials/.lakectl.yaml",
    os.path.expanduser("~/.lakectl.yaml"),
)

# %% [markdown]
"""
We can now instantiate the `LakeFSFileSystem` with the credentials we just downloaded.
Alternatively, we could have passed the credentials directly in the code.
It is important that the credentials are available at the time of filesystem instantiation.
"""

# %%
from lakefs_spec import LakeFSFileSystem

fs = LakeFSFileSystem()

REPO_NAME = "weather"

# %% [markdown]
"""
We will create a repository using a helper function provided by lakeFS-spec.
If you have already created one in the UI, make sure to set the `REPO_NAME` variable accordingly in the cell directly above.
"""

# %%
import lakefs

repo = lakefs.Repository(REPO_NAME, fs.client).create(storage_namespace=f"local://{REPO_NAME}")

# %% [markdown]
"""
## Data Ingestion

Now it's time to get some data. We will use the [Open-Meteo API](https://open-meteo.com/), where we can pull weather data from an API for free (as long as we are non-commercial) and without an API token.
In order to prevent hitting the rate limits when repeatedly querying the API (and out of courtesy towards the operators of the API), the `_maybe_urlretrieve` function provides a simple local cache for the downloaded data.

For training our toy model, we download the full weather data of Munich for the year 2010:
"""


# %%
def _maybe_urlretrieve(url: str, filename: str) -> str:
    # Avoid API rate limit errors by downloading to a fixed local location
    destination = Path(tempfile.gettempdir()) / "lakefs-spec-tutorials" / filename
    destination.parent.mkdir(exist_ok=True, parents=True)
    if destination.exists():
        return str(destination)

    outfile, _ = urllib.request.urlretrieve(url, str(destination))
    return outfile


outfile = _maybe_urlretrieve(
    "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2010-01-01&end_date=2010-12-31&hourly=temperature_2m,relativehumidity_2m,rain,pressure_msl,surface_pressure,cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,windspeed_10m,windspeed_100m,winddirection_10m,winddirection_100m",
    "weather-2010.json",
)

# %% [markdown]
"""
The data is in JSON format.
Therefore, we need to wrangle the data a bit to make it usable.
But first, we will upload it to our lakeFS instance.
"""

# %% [markdown]
"""
## Upload a file using transactions

lakeFS works similar to `git` as a versioning system.
You can create *commits* that contain specific changes to the data.
You can also work with *branches* to create your own isolated view of the data independently of your colleagues.
Every commit (on any branch) is identified by a commit SHA.
This SHA can be used to programmatically interact with specific states of your data and enables logging of the specific data versions used to create a certain model.

To easily carry out versioning operations while uploading files, you can use **transactions**.
A transaction is a context manager that keeps track of all files that were uploaded in its scope, as well as all versioning operations happening between file uploads.
All operations are deferred to the end of the transaction, and are executed sequentially on completion.

To create a commit after a file upload, you can run the following transaction:
"""

# %%
NEW_BRANCH = lakefs.Branch(REPO_NAME, "transform-raw-data", client=fs.client)
NEW_BRANCH.create("main")

with fs.transaction(REPO_NAME, NEW_BRANCH) as tx:
    fs.put(outfile, f"{REPO_NAME}/{tx.branch.id}/weather-2010.json")
    tx.commit(message="Add 2010 weather data")

# %% [markdown]
"""
You can inspect this commit by selecting the `transform-raw-data` branch, and navigating to the **Commits** tab.
"""

# %% [markdown]
"""
## Data Transformation

Now let's transform the data for our use case.
We put the transformation into a function to be able to reuse it later.

In this notebook, we use a simple toy model to predict whether it is raining at the same time tomorrow given weather data from right now.

We will skip a lot of possible feature engineering and other data science aspects in order to focus more on the application of the `LakeFSFileSystem`.
"""

# %%
import json

import pandas as pd


def transform_json_weather_data(filepath):
    if hasattr(filepath, "close") and hasattr(filepath, "tell"):
        data = json.load(filepath)
    else:
        with open(filepath) as f:
            data = json.load(f)

    df = pd.DataFrame.from_dict(data["hourly"])
    df.time = pd.to_datetime(df.time)
    df["is_raining"] = df.rain > 0
    df["is_raining_in_1_day"] = df.is_raining.shift(24).astype(bool)
    df = df.dropna()
    return df


df = transform_json_weather_data(outfile)
df.head(5)

# %% [markdown]
"""
Next, we save this data as a CSV file into the main branch.
When the transaction commit helper is called, the newly put CSV file is committed.
You can verify the saving worked in the lakeFS UI in your browser by switching to the commits tab of the `main` branch.
"""

# %%
with fs.transaction(REPO_NAME, "main") as tx:
    df.to_csv(f"lakefs://{REPO_NAME}/{tx.branch.id}/weather_2010.csv")
    tx.commit(message="Update weather data")

# %% [markdown]
"""
## Training the initial weather model

First we will do a train-test split:
"""

# %%
import sklearn.model_selection

model_data = df.drop("time", axis=1)

train, test = sklearn.model_selection.train_test_split(model_data, random_state=7)

# %% [markdown]
"""
We save these train and test datasets into a new `training` branch.
If the branch does not exist yet, as in this case, it is implicitly created by default.
You can control this behaviour with the `create_branch_ok` flag when initializing the `LakeFSFileSystem`.
By default, `create_branch_ok` is set to `True`, so we need to only set `fs = LakeFSFileSystem()` to enable implicit branch creation.
"""

# %%
TRAINING_BRANCH = lakefs.Branch(REPO_NAME, "training", client=fs.client)
TRAINING_BRANCH.create("main")

with fs.transaction(REPO_NAME, TRAINING_BRANCH) as tx:
    train.to_csv(f"lakefs://{REPO_NAME}/{tx.branch.id}/train_weather.csv")
    test.to_csv(f"lakefs://{REPO_NAME}/{tx.branch.id}/test_weather.csv")
    tx.commit(message="Add train-test split of 2010 weather data")

# %% [markdown]
"""
Let's check the shape of train and test data.
Later on, we will get back to this data version and reproduce the results of the experiment.
"""

# %%
print(f"Initial train data shape: {train.shape}")
print(f"Initial test data shape: {test.shape}")

# %% [markdown]
"""
We now proceed to train a decision tree classifier and evaluate it on the test set:
"""

# %%
from sklearn.tree import DecisionTreeClassifier

dependent_variable = "is_raining_in_1_day"

model = DecisionTreeClassifier(random_state=7)

x_train, y_train = train.drop(dependent_variable, axis=1), train[dependent_variable].astype(bool)
x_test, y_test = test.drop(dependent_variable, axis=1), test[dependent_variable].astype(bool)

model.fit(x_train, y_train)

test_acc = model.score(x_test, y_test)
print(f"Test accuracy: {test_acc:.2%}")

# %% [markdown]
"""
## Updating data and retraining the model

Until now, we only have used data from 2010.
Let's download additional 2020 data, transform it, and save it to lakeFS.
"""

# %%
outfile = _maybe_urlretrieve(
    "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2020-01-01&end_date=2020-12-31&hourly=temperature_2m,relativehumidity_2m,rain,pressure_msl,surface_pressure,cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,windspeed_10m,windspeed_100m,winddirection_10m,winddirection_100m",
    "weather-2020.json",
)

new_data = transform_json_weather_data(outfile)

with fs.transaction(REPO_NAME, "main") as tx:
    new_data.to_csv(f"lakefs://{REPO_NAME}/{tx.branch.id}/weather_2020.csv")
    tx.commit(message="Add 2020 weather data")

# Remove leftover temporary files from previous `urlretrieve` calls
urllib.request.urlcleanup()


# %% [markdown]
"""
Let's concatenate the old data and the new data, create a new train-test split, and push the updated files to lakeFS:
"""

# %%
new_data = new_data.drop("time", axis=1)
full_data = pd.concat([new_data, train, test])

train_df, test_df = sklearn.model_selection.train_test_split(full_data, random_state=7)

print(f"Updated train data shape: {train_df.shape}")
print(f"Updated test data shape: {test_df.shape}")

with fs.transaction(REPO_NAME, TRAINING_BRANCH) as tx:
    train_df.to_csv(f"lakefs://{REPO_NAME}/{tx.branch.id}/train_weather.csv")
    test_df.to_csv(f"lakefs://{REPO_NAME}/{tx.branch.id}/test_weather.csv")
    tx.commit(message="Add train-test split of 2010 and 2020 data")

# %% [markdown]
"""
Now, we train the model on the new data and validate on the new test data.
"""

# %%
x_train, y_train = (
    train_df.drop(dependent_variable, axis=1),
    train_df[dependent_variable].astype(bool),
)
x_test, y_test = test_df.drop(dependent_variable, axis=1), test_df[dependent_variable].astype(bool)

model.fit(x_train, y_train)

test_acc = model.score(x_test, y_test)

print(f"Test accuracy: {test_acc:.2%}")

# %% [markdown]
"""
## Accessing data versions through commits and reproducing experiments

If we need to go to our initial data and reproduce the first experiment (the model trained on the 2010 data with its initial accuracy), we can go back in the commit history of the `training` branch and select the appropriate commit data snapshot.
Since we have created multiple commits on the same branch already, we will address different data versions by their commit SHAs.

To obtain the actual commit SHA from a branch, we have multiple options.
Manually, we could go into the lakeFS UI, select the training branch, and navigate to the **Commits** tab.
There, we take the parent of the previous commit, titled `Add train-test split of 2010 weather data`, and copy its revision SHA (also called `ID`).

In code, we can obtain commit SHAs for different revisions on the `training` branch by using `lakefs.Reference` objects.
"""

# %%

# access the data of the previous commit with a lakefs ref expression, in this case the same as in git.
previous_commit = repo.ref(f"{TRAINING_BRANCH.id}~").get_commit()
fixed_commit_id = previous_commit.id
print(fixed_commit_id)

# %% [markdown]
"""
Let's check whether we managed to get the initial train and test data with this commit SHA, checking equality to the initial data:
"""

# %%
orig_train = pd.read_csv(f"lakefs://{REPO_NAME}/{fixed_commit_id}/train_weather.csv", index_col=0)
orig_test = pd.read_csv(f"lakefs://{REPO_NAME}/{fixed_commit_id}/test_weather.csv", index_col=0)

print(f"Is the pulled training data equal to the local training data? {train.equals(orig_train)}")
print(f"Is the pulled test data equal to the local test data? {test.equals(orig_test)}")

# %% [markdown]
"""
Let's train and validate the model again based on the redownloaded data and see if we manage to reproduce the initial accuracy.
"""
# %%
x_train, y_train = train.drop(dependent_variable, axis=1), train[dependent_variable].astype(bool)
x_test, y_test = test.drop(dependent_variable, axis=1), test[dependent_variable].astype(bool)

model.fit(x_train, y_train)

test_acc = model.score(x_test, y_test)

print(f"Test accuracy: {test_acc:.2%}")

# %% [markdown]
"""
## Using tags instead of commit SHAs for semantic versioning

The above method for data versioning works great when you have experiment tracking tools to store and retrieve the commit SHA in automated pipelines.
But it can be tedious to retrieve in manual prototyping.
We can make selected versions of the dataset more accessible with semantic versioning by attaching a human-interpretable tag to a specific commit SHA.

Creating a tag is easiest when done inside a transaction, just like the files we already uploaded.
To do this, simply call `tx.tag` on the transaction and supply the repository name, the commit SHA to tag, and the intended tag name.
Tags are immutable once created, so attempting to tag two different commits with the same name will result in an error.
"""


# %%
with fs.transaction(REPO_NAME, "main") as tx:
    # returns the tag as a lakeFS object.
    tag = tx.tag(fixed_commit_id, name="train-test-split-2010")


# %% [markdown]
"""
Now we can access the specific files with the semantic tag.
Both the `fixed_commit_id` and `tag` reference the same version `ref` in lakeFS, whereas a branch name always points to the latest version on that respective branch.
"""

# %%
train_from_commit = pd.read_csv(
    f"lakefs://{REPO_NAME}/{fixed_commit_id}/train_weather.csv", index_col=0
)
train_from_tag = pd.read_csv(f"lakefs://{REPO_NAME}/{tag.id}/train_weather.csv", index_col=0)

# %% [markdown]
"""
We can verify this by comparing the `DataFrame`s. We see that the `train_from_commit` and `train_from_tag` are equal.
"""

# %%
print(
    f"Is the data tagged {tag!r} equal to the data in commit {fixed_commit_id[:8]}? {train_from_commit.equals(train_from_tag)}"
)

# %% tags=["Remove_input", "Remove_all_output"]
# Clean-up cell removing artifacts created in notebook execution to ensure idempotency.
# Cell hidden in docs

for t in repo.tags():
    t.delete()
for b in repo.branches():
    if b.id != "main":
        b.delete()
