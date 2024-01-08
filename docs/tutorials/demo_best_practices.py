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
We assume you have lakeFS and lakeFS-spec set up. For guidance on setup and configuration, check the lakeFS-spec documentation.

We will explain the following best practices for data versioning in this example:
- Define a data repository
- Follow a branching strategy that ensures data integrity on the `main` branch, e.g. by running tests on feature branch datasets.
- Utilize reusable and tested functions for data transformation.
- Use commits to save checkpoints and merge branches for atomic changes.
- Keep naming (of branches and commits) consistent, concise, and unique.
- Use descriptive naming where it matters.

In this demo project, we build a weather predictor using data from a public API.
This simulates the dynamics within a real world scenario where we continuously collect more data.
"""

# %% tags=["Remove_single_output"]
# %pip install numpy pandas scikit-learn

# %%
import json
import tempfile
import urllib.request
from pathlib import Path

import lakefs
import pandas as pd
import sklearn
import sklearn.model_selection

import lakefs_spec

# %% [markdown]
"""
The cell below contains a helper function to obtain the data. It is otherwise not relevant to this demonstration.
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

repo = lakefs.Repository(REPO_NAME, fs.client).create(storage_namespace=f"local://{REPO_NAME}")


# %% [markdown]
"""
lakeFS works similarly to `git` as a versioning system.
You can create *commits* that contain specific changes to the data.
You can also work with *branches* to create an isolated view of the data.
Every commit (on any branch) is identified by a commit SHA, a unique identifier obtained via a hashing function.

## Branching Strategy

We recommend following a branching strategy that ensures the data integrity on the main branch.
Since we are about to do some data wrangling, we will branch off `main` and later merge back into it, once we are sure everything works as expected.
"""

# %%
NEW_BRANCH_NAME = "transform-raw-data"

with fs.transaction as tx:
    fs.put(outfile, f"{REPO_NAME}/{NEW_BRANCH_NAME}/weather-2010.json")
    tx.commit(repository=REPO_NAME, branch=NEW_BRANCH_NAME, message="Add 2010 weather data")

# %% [markdown]
"""
## Utilize reusable and tested functions for data transformation

Now that we have the data on the `transform-raw-data` branch, we can start with the transformation.

It is good practice to encapsulate common transformations in functions.
"""


# %%
def load_json_data(filepath):
    if hasattr(filepath, "close") and hasattr(filepath, "tell"):
        return json.load(filepath)
    else:
        with open(filepath, "r") as f:
            return json.load(f)


# %%
def create_dataframe_from_json(json_data):
    df = pd.DataFrame.from_dict(json_data["hourly"])
    return df


sample_json_data = {
    "hourly": [{"time": "2023-01-01 00:00", "rain": 0}, {"time": "2023-01-01 01:00", "rain": 2}]
}

# Tests
df_test = create_dataframe_from_json(sample_json_data)
assert isinstance(df_test, pd.DataFrame), "Output should be a pandas DataFrame"
assert list(df_test.columns) == ["time", "rain"], "DataFrame should have time and rain columns"
assert len(df_test) == 2, "DataFrame should have two rows"


# %%
def convert_time_column_to_datetime(df):
    df["time"] = pd.to_datetime(df["time"])
    return df


# Tests
df_test = pd.DataFrame({"time": ["2023-01-01 00:00", "2023-01-01 01:00"], "rain": [0, 2]})

df_test = convert_time_column_to_datetime(df_test)
assert pd.api.types.is_datetime64_any_dtype(
    df_test["time"]
), "Time column should be of datetime type"


# %%
def add_rain_indicators(df):
    df["is_raining"] = df.rain > 0
    df["is_raining_in_1_day"] = df.is_raining.shift(24).astype(bool)
    return df.dropna()


# Tests
df_test = pd.DataFrame(
    {"time": pd.date_range(start="2023-01-01", periods=48, freq="H"), "rain": [0] * 24 + [2] * 24}
)

# Test the function
df_test = add_rain_indicators(df_test)
assert (
    "is_raining" in df_test.columns and "is_raining_in_1_day" in df_test.columns
), "Both indicator columns should be present"
assert all(
    df_test.loc[24:, "is_raining"]
), "All values should be True in 'is_raining' for the second day"
assert all(
    df_test.loc[:23, "is_raining_in_1_day"]
), "All values should be True in 'is_raining_in_1_day' for the first day"
assert df_test.isna().sum().sum() == 0, "There should be no NaN values in the DataFrame"

# %% [markdown]
"""
Encapsulating data processing steps in unit tested functions (which are also made available to the whole team or organisation) saves some work by reusing our code.
Additionally, the tests increase our confidence in the data quality and serve as additional context to infer the purpose of the function should we or someone else come back at a later time.

We can now apply the functions to process the data.
"""

# %%
json_data = load_json_data(outfile)
df = create_dataframe_from_json(json_data)
df = convert_time_column_to_datetime(df)
df = add_rain_indicators(df)

df.head(5)

# %% [markdown]
"""
## Use Commits to Save Checkpoints and Merge Branches for Atomic Changes

Now we can commit the updated data to the transform-raw-data branch in lakeFS repository.
We write a descriptive commit message.
"""
# %%
with fs.transaction as tx:
    df.to_csv(f"lakefs://{REPO_NAME}/main/weather_2010.csv")
    commit = tx.commit(repository=REPO_NAME, branch="main", message="Preprocess 2010 data")
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
    tx.create_branch(repository=REPO_NAME, name=TRAINING_BRANCH, source="main")

df = pd.read_csv(f"lakefs://{REPO_NAME}/{TRAINING_BRANCH}/weather_2010.csv")
model_data = df.drop("time", axis=1)
train, test = sklearn.model_selection.train_test_split(model_data, random_state=7)

# %% [markdown]
"""
## Descriptive Tags for Human-Readability and Uniqe SHAs for unique Identification

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
