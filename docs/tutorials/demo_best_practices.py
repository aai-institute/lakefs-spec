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
# Data Versioning Best Practices

This notebook will guide you through the best practices for versioning data in a data science project.
We assume you have lakeFS and lakeFS-spec setup. If you need help with it, look into the other parts of the docs or the other tutorial which is more focused on the setup.

We will explain the following best practices for data versioning in this example:
- Define a data repository
- Follow a branching strategy that ensures data integrity on the `main` branch, e.g. by running tests on feature branch datasets.
- Use commits to save checkpoints and merge branches for atomic changes.
- Keep naming (of branches and commits) consistent, concise, and unique.
- Use descriptive naming (where it matters).

For this demo project, we aim to build a weather predictor using data from a public API. 
This simulates the dynamics within a real world scenario where we continuously collect more data.
"""
# %%
import json
import pandas as pd
import tempfile
import urllib.request
from pathlib import Path
import lakefs_spec
import sklearn
import sklearn.model_selection

# %% [markdown]
"""
The cell below contains a helper function to obtain the data. It is otherwise not relevant to this demonstration.
"""
# %%
def _maybe_urlretrieve(url: str, filename: str) -> str:
    # Avoid API rate limit errors by downloading to a fixed local location
    destination = Path(tempfile.gettempdir()) / \
        "lakefs-spec-tutorials" / filename
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
## Define a data repository

We got the data for the year 2010. That should be enough for initial prototyping.
Later, however, we want to use more data. Since our dataset will be evolving, we implement data version control.
This ensure the reproducibility of our experiments, enables collaboration with colleagues, and ensures our dynamic dataset to stay a valuable asset.

To set up our versioning, we need to decide on a versioning tool, set up a repository, and define which data is considered in scope and should be versioned and which is not.
In this case, we want to version the weather data using lakeFS.
Let us now set up a lakeFS repository using lakeFS-spec, our filesystem implementation for lakeFS.
"""

# %%
fs = lakefs_spec.LakeFSFileSystem()

REPO_NAME = "weatherpred"

repo = lakefs_spec.client_helpers.create_repository(client=fs.client, name=REPO_NAME,
                                                    storage_namespace=f"local://{REPO_NAME}")

# %% [markdown]
"""
lakeFS works similarly to `git` as a versioning system.
You can create *commits* that contain specific changes to the data.
You can also work with *branches* to create an isolated view of the data.
Every commit (on any branch) is identified by a commit SHA, a unique identifier obtained via a hashing function.

## Branching Strategy

We recommend following a branching strategy that ensures the data integrity on the main branch.
Since we are about to do some data wrangling, we will fork off a branch and later merge it to `main` once we are sure everything works as expected.
"""

# %%
NEW_BRANCH_NAME = "transform-raw-data"

with fs.transaction as tx:
    fs.put(outfile, f"{REPO_NAME}/{NEW_BRANCH_NAME}/weather-2010.json")
    tx.commit(repository=REPO_NAME, branch=NEW_BRANCH_NAME,
              message="Add 2010 weather data")

# %% [markdown]
"""
Now that we have the data on the `transform-raw-data` branch, we can start with the transformation.

It is good practice to encapsulate common transformations in functions.
This way we can save some work by reusing our code. We can also add unit tests for the transformation functions.
This increases our confidence in the data quality and serves as additional context to infer the purpose of the function should we or someone else come back at a later time. 
Since the focus of this demo is data version control, we wont write tests now.
"""


# %%
def transform_json_weather_data(filepath):
    if hasattr(filepath, "close") and hasattr(filepath, "tell"):
        data = json.load(filepath)
    else:
        with open(filepath, "r") as f:
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
## Commits and Branch Merges for Atomic Changes

Now we can commit the updated data to the transform-raw-data branch in lakeFS repository.
We write a descriptive commit message.
"""
# %%
with fs.transaction as tx:
    df.to_csv(f"lakefs://{REPO_NAME}/main/weather_2010.csv")
    commit = tx.commit(repository=REPO_NAME, branch="main",
                       message="Preprocess 2010 data")
print(commit)

# %% [markdown]
"""
We see that the commit now has a unique id, the commit SHA which we can log to identify this particular state of the data.

As the data looks good, we can also merge the branch back into `main`.
"""

# %%
with fs.transaction as tx:
    tx.merge(repository=REPO_NAME, source_ref=NEW_BRANCH_NAME, into="main")

# %% [markdown]
"""
We will now start to develop our ML model. We recommend checking out a new branch for every set of experiments.
Here, we will conduct the train test split and further experiment specific modifications, if applicable.
"""

# %%
TRAINING_BRANCH = "experiment-1"
with fs.transaction as tx:
    tx.create_branch(repository=REPO_NAME,
                     name=TRAINING_BRANCH, source_branch="main")

df = pd.read_csv(
    f"lakefs://{REPO_NAME}/{TRAINING_BRANCH}/weather_2010.csv")
model_data = df.drop("time", axis=1)
train, test = sklearn.model_selection.train_test_split(
    model_data, random_state=7)

# %% [markdown]
"""
## Descriptive Tags for Human-Readability

Since the train test split of the new branch is something we expect to address quite often in development, we will also add a human-readable tag to it.
This makes it easy to refer back to it and to communicate this specific state of the data to colleagues. Tags are immutable which ensures consistency.
You and your colleagues can then also work with (the same state (i.e. train test split, etc.) of the data.
"""

# %%
TAG_NAME = "exp1_2010_data"
with fs.transaction as tx:
    train.to_csv(f"lakefs://{REPO_NAME}/{TRAINING_BRANCH}/train_weather.csv")
    test.to_csv(f"lakefs://{REPO_NAME}/{TRAINING_BRANCH}/test_weather.csv")
    commit = tx.commit(
        repository=REPO_NAME,
        branch=TRAINING_BRANCH,
        message="Perform train-test split of 2010 weather data",
    )
    tx.tag(repository=REPO_NAME, ref=commit, tag=TAG_NAME)

# %% [markdown]
"""
Now we have the data on different branches. If new data comes in, we can perform necessary preprocessing on a separate branch and merge it to `main` once we are sure about its compatibility and we have run all the necessary tests.
Should the new data be important for the experimentation as well, then we can merge the new main branch into the experimentation branch.
You should then create a new tag for the dataset.
You cannot directly reassign tags. If you want to do this anyways, for example to prevent namespace clutter, you have to delete the tag and create a new one. 
However, beware as this might break reproducibility in other places (i.e. colleagues might expect unchanged data).
To ensure failsafe versioning use the SHA's of the commits in tracking tools.
"""
