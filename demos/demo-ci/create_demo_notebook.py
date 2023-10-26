# %% [markdown]
"""
# Introduction

In this notebook, we will complete a small end-to-end data science tutorial that employs lakeFS-spec for data versioning. We will use weather data to train a random forest classifier to predict whether it is raining a day from now given the current weather.

We will do the following:
* Environment Setup
* lakeFS Setup
* Data Ingestion
    * Event Hooks
    * PUT a File
* Model Training
* Updating Data and Retraining Model
* Accessing Data Version and Reproducing Experiment
* Using a Tag instead of a commit SHA for semantic versioning

To execute the code in this tutorial as a jupyter notebook, download this `.ipynb` file to a convenient location on your machine. You can also clone the whole `lakefs-spec` repository. During the execution of this tutorial, in the same directory, a folder, 'data', will be created. We will also download a file `.lakectl.yaml`.

Prerequisites before we start:
* Python 3.9 or higher
* Docker desktop installed - [see guidance](https://www.docker.com/get-started/)
* git installed

# Environment Setup
To set up the environment, run the following commands in your console:

Create a virtual environment:

    `python -m venv .venv`

Activate environment 
- macOS and Linux:

        `source .venv/bin/activate`

- activate environment - Windows:

        `.venv\Scripts\activate`

Install relevant libriaries on the environment you have just created:

    `pip install -r https://raw.githubusercontent.com/appliedAI-Initiative/lakefs-spec/main/demos/requirements.txt`

From a terminal activate this jupyter notebook (if not running already).

In the notebook "Kernel" menu select the environment you just created as a Jupyter kernel.

# lakeFS Setup

Ensure you have docker desktop is running.

Set up LakeFS. You can do this by executing the docker run command given here lakeFS quickstart in your console:

`docker run --name lakefs --pull always --rm --publish 8000:8000 treeverse/lakefs:latest run --quickstart`

Open a browser and navigate to the lakeFS instance - by default: http://localhost:8000/. 

Authenticate with the credentials provided https://docs.lakefs.io/quickstart/launch.html :

    Access Key ID    : AKIAIOSFOLQUICKSTART
    Secret Access Key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

As an email, you can enter anything, we won't need it in this example. 

Proceed to create an empty repository and call it 'weather'.

In your jupyter notebook create variable REPO_NAME and set its value to the name of the repo you have just created in lakeFS web  interface:
"""

# %%
REPO_NAME = 'weather'

# %% [markdown]
"""
There many ways to authenticate to lakeFS while executing Python code - in this tutorial we choose convinient yaml file configuration. Execute the code below to dowload yaml file including lakeFS quickstart credentials and server url. This file will be downloaded to the same location as your notebook. 
"""

# %%

!curl -o .lakectl.yaml "https://raw.githubusercontent.com/appliedAI-Initiative/lakefs-spec/main/demos/.lakectl.yaml"

## %% [markdown]
"""
# Data Ingestion

Now it's time to get some data. We will use the [Open Meteo api](https://open-meteo.com/), where we can pull weather data from an API for free (as long as we are non-commercial) and without an API-token.

First create folder 'data' inside a directory when your notebook is located:
"""

# %% 
!mkdir -p data

# %% [markdown]
"""
Then, for the purpose of training, get the full data of the 2010s from Munich:
"""
# %%
!curl -o data/weather-2010s.json "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2010-01-01&end_date=2019-12-31&hourly=temperature_2m,relativehumidity_2m,rain,pressure_msl,surface_pressure,cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,windspeed_10m,windspeed_100m,winddirection_10m,winddirection_100m"


# %% [markdown]
"""
# PUT a file

The data is in JSON format. Therefore, we need to wrangle the data a bit to make it usable. But first we will save it into our lakeFS instance.

lakeFS works similar to `git` as a versioning system. You can create `commits` that encapsulate specific changes to the data. You can also work with `branches` to fork of your own copy of the data such that you don't interfere with your colleagues. Every commit (on any branch) is identified by a `commit-SHA`. This can be used to programmatically interact with specific states of your data and enables logging of the specific versions used to create a certain model. We will cover all this later in this demo.

For now, we will `put` the file we have. Therefore, we will create a new branch, `transform-raw-data` for our data.

"""

# %% 
from lakefs_spec import LakeFSFileSystem

NEW_BRANCH_NAME = 'transform-raw-data'


fs = LakeFSFileSystem()
fs.put('./data/weather-2010s.json',  f'{REPO_NAME}/{NEW_BRANCH_NAME}/weather-2010.json')


# %% [markdown]
"""
Now, on LakeFS in your browser, can change the branch to transform-raw-data and see the saved file. However, the change is not yet committed. While you can do that manually via the uncommitted changes tab in the lakeFS UI, we will commit the file in a different way.
"""
# %% [markdown]
"""
# Event Hooks

To commit changes programmatically, we can register a hook. This hook needs to have the signature `(client, context) -> None`, where the `client` is the file system's LakeFS client. The context object contains information about the requested resource. Within this hook, we can automatically create a commit. We will register the hook for the `PUT_FILE` and `FILEUPLOAD` events. Pandas uses the latter in 'DataFrame.to_csv()' and hence we commit when using 'DataFrame.to_csv()' as well.

"""

# %% 
from lakefs_sdk.client import LakeFSClient

from lakefs_spec.client_helpers import commit
from lakefs_spec.hooks import FSEvent, HookContext


# Define the commit hook
def commit_on_put(client: LakeFSClient, ctx:HookContext) -> None:
    commit_message = f"Add file {ctx.resource}"
    print(f"Attempting Commit: {commit_message}")
    commit(client, repository=ctx.repository, branch=ctx.ref, message=commit_message)
    

# Register the commit hook to be executed after the PUT_FILE and FILEUPLOAD events
fs.register_hook(FSEvent.PUT_FILE, commit_on_put)
fs.register_hook(FSEvent.FILEUPLOAD, commit_on_put)

# %% [markdown]
"""
Now uploading the file will create a commit. Since we already uploaded the file, lakeFS will skip the upload as the checksums of the local and remote file match. The hook will be executed regardless.

If we want to execute the upload even for an unchanged file, we can do so by passing `precheck=False` to the `fs.put()` operation.
"""

# %% 

fs.put('data/weather-2010s.json',  f'{REPO_NAME}/{NEW_BRANCH_NAME}/weather-2010.json', precheck=True)

# %% [markdown]
"""
# Data Transformation
Now let's transform the data for our use case. We put the transformation into a function such that we can reuse it later

In this notebook, we follow a simple toy example to predict whether it is raining at the same time tomorrow given weather data from right now.

We will skip a lot of possible feature engineering etc. in order to focus on the application of lakeFS and the `LakeFSFileSystem`.
"""

# %% 
import pandas as pd
import json
import numpy as np

def transform_json_weather_data(filepath):
    with open(filepath,"r") as f:
        data = json.load(f)

    df = pd.DataFrame.from_dict(data["hourly"])
    df.time = pd.to_datetime(df.time)
    df['is_raining'] = df.rain > 0
    df['is_raining_in_1_day'] = df.is_raining.shift(24)
    df = df.dropna()
    return df
    
df = transform_json_weather_data('data/weather-2010s.json')
df.head(5)

# %% [markdown]

"""
Now we save this data as a CSV file into the main branch. The `DataFrame.to_csv` method calls an `open` operation behind the scenes, our commit hook is called and the file is committed. You can verify the saving worked in the LakeFS UI in your browser by switching to the commits tab of the main branch.
"""

# %% 
df.to_csv(f'lakefs://{REPO_NAME}/main/weather_2010s.csv')

# %% [markdown]
"""
# Model Training
First we will do a train-test split:
"""

# %% 
import sklearn.model_selection

model_data=df.drop('time', axis=1)

train, test = sklearn.model_selection.train_test_split(model_data, random_state=7)

# %% [markdown]
"""
We save these train and test datasets into a new `training` branch. If the branch does not yet exist, as in this case, it is implicitly created by default. You can control this behaviour with the `create_branch_ok` flag when initializing the 'LakeFSFileSystem'. By default `create_branch_ok=True`, so that we needed only `fs = LakeFSFileSystem()` to enable implicit branch creation.
"""

# %%
TRAINING_BRANCH = 'training'
train.to_csv(f'lakefs://{REPO_NAME}/{TRAINING_BRANCH}/train_weather.csv')
test.to_csv(f'lakefs://{REPO_NAME}/{TRAINING_BRANCH}/test_weather.csv')

# %% [markdown]
"""
Implicit branch creation is a convenient way to create new branches programmatically. However, one drawback is that typos in your code might result in new accidental branch creations. If you want to avoid this implicit behavior and raise errors instead, you can disable implicit branch creation by setting `fs.create_branch_ok=False`.

We can now read train and test files directly from the remote LakeFS instance. (You can verify that neither the train nor the test file are saved in the `/data` directory).
"""

# %% 
train = pd.read_csv(f'lakefs://{REPO_NAME}/{TRAINING_BRANCH}/train_weather.csv', index_col=0)
test = pd.read_csv(f'lakefs://{REPO_NAME}/{TRAINING_BRANCH}/test_weather.csv', index_col=0)

train.head()

# %% [markdown]
"""
Let's check the shape of train and test data. Later on we will train to get back to this data version and reproduce the results of the experiment.
"""

# %% 

print(f'Train initial data shape: {train.shape}')
print(f'Test initial data shape: {test.shape}')

# %% [markdown]
"""
We now proceed to train a random forest classifier and evaluate it on the test set:
"""

# %%
from sklearn.ensemble import RandomForestClassifier

dependent_variable = 'is_raining_in_1_day'

model = RandomForestClassifier(random_state=7)
x_train, y_train = train.drop(dependent_variable, axis=1), train[dependent_variable].astype(bool)
x_test, y_test = test.drop(dependent_variable, axis=1), test[dependent_variable].astype(bool)

model.fit(x_train, y_train)

test_acc = model.score(x_test, y_test)

print(f"Test accuracy: {round(test_acc, 4) * 100 } %")

# %% [markdown]
"""
# Updating Data and Model
Until now, we only have used data from the 2010s. Let's download additional 2020s data, transform it, and save it to LakeFS.
"""

# %% 
!curl -o data/weather-2020s.json "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2020-01-01&end_date=2023-08-31&hourly=temperature_2m,relativehumidity_2m,rain,pressure_msl,surface_pressure,cloudcover,cloudcover_low,cloudcover_mid,cloudcover_high,windspeed_10m,windspeed_100m,winddirection_10m,winddirection_100m"

new_data = transform_json_weather_data('./data/weather-2020s.json')
new_data.to_csv(f'lakefs://{REPO_NAME}/main/weather_2020s.csv')
new_data = new_data.drop('time', axis=1)


# %% [markdown]
"""
Let's concatenate the old data and the new data, create a new train-test split, and overwrite the files on lakeFS:
"""

# %% 
df_train = pd.read_csv(f'lakefs://{REPO_NAME}/{TRAINING_BRANCH}/train_weather.csv', index_col=0)
df_test = pd.read_csv(f'lakefs://{REPO_NAME}/{TRAINING_BRANCH}/test_weather.csv', index_col=0)

full_data = pd.concat([new_data, df_train, df_test])

train_df, test_df = sklearn.model_selection.train_test_split(full_data, random_state=7)

train_df.to_csv(f'lakefs://{REPO_NAME}/{TRAINING_BRANCH}/train_weather.csv')
test_df.to_csv(f'lakefs://{REPO_NAME}/{TRAINING_BRANCH}/test_weather.csv')

# %% [markdown]
"""
We may now read the updated data directly from lakeFS and check their shape to insure that initial files `train_weather.csv` and `test_weather.csv` have been overwritten successfully (number of rows should be significantly higher as 2020 data were added):
"""
# %% 

train = pd.read_csv(f'lakefs://{REPO_NAME}/{TRAINING_BRANCH}/train_weather.csv', index_col=0)
test = pd.read_csv(f'lakefs://{REPO_NAME}/{TRAINING_BRANCH}/test_weather.csv', index_col=0)
print(f'train updated data shape: {train.shape}')
print(f'test updated data shape: {test.shape}')

# %% [markdown]
"""
Now we may train the model based on the new train data and validate based on the new test data:
"""

# %% 
x_train, y_train = train.drop(dependent_variable, axis=1), train[dependent_variable].astype(bool)
x_test, y_test = test.drop(dependent_variable, axis=1), test[dependent_variable].astype(bool)

model.fit(x_train, y_train)

test_acc = model.score(x_test, y_test)

print(f"Test accuracy: {round(test_acc, 4) * 100 } %")

# %% [markdown]
"""
# Accessing Data Version and Reproducing Experiment

Let's assume we need to go to our initial data and reproduce initial experiment (initial model with its initial accuracy). This might be tricky as we have overwritten initial train and test data on lakeFS.

To enable data versioning we should save the `ref` of the specific datasets. `ref` can be a branch we are pulling a file from LakeFS. `ref` can be also a commit id - then you can access different data versions within the same branch and not only the version from the latest commit. Therefore, we will use explicit versioning and get the actual commit SHA. We have multiple ways to do this. Manually, we could go into the lakeFS UI, select the training branch, and navigate to the "Commits" tab. There, we could see the latest two commits, titled `Add file test_weather.csv` and `Add file train_weather.csv`, and copy their IDs.

However, we want to automate as much as possible and therefore use a helper function. You find pre-written helper functions in the `lakefs_spec.client_helpers` module:
"""

# %% 
from lakefs_spec.client_helpers import rev_parse

fixed_commit_id  = rev_parse(fs.client, REPO_NAME, TRAINING_BRANCH, parent=2) # parent is a relative number of a commit when 0 is the latest
print(fixed_commit_id)

# %% [markdown]
"""
With our commit hook setup, both DataFrame.to_csv() operations create an individual commit. lakeFS saves the state of every file at every commit. To get other commits with the rev_parse function, you can change the repo, branch parameters. To go back in the chosen branch's commit history, you can increase the `parent` parameter. In our case the initial data was commited 3 commits ago - we count the latest commit on a branch as 0, thus `parent` = 2.
Let's check whether we manage to get the initial train and test data with this commit SHA - let's compare the shape to the initial data shape:
"""


# %% 
train = pd.read_csv(f"lakefs://{REPO_NAME}/{fixed_commit_id}/train_weather.csv", index_col=0)
test = pd.read_csv(f"lakefs://{REPO_NAME}/{fixed_commit_id}/test_weather.csv", index_col=0)

print(f'train data shape: {train.shape}')
print(f'test data shape: {test.shape}')

# %% [markdown]
"""
Let's train and validate the model based on re-fetched data and see whether we manage to reproduce the initial accuracy ratio:  
"""
# %% 
x_train, y_train = train.drop(dependent_variable, axis=1), train[dependent_variable].astype(bool)
x_test, y_test = test.drop(dependent_variable, axis=1), test[dependent_variable].astype(bool)

model.fit(x_train, y_train)

test_acc = model.score(x_test, y_test)

print(f"Test accuracy: {round(test_acc, 4) * 100 } %")

# %% [markdown]
"""
# Using a Tag instead of a commit SHA for semantic versioning
The above method for data versioning works great when you have experiment tracking tools to store and retrieve the commit SHA in automated pipelines. But it is hard to remember and tedious to retrieve in manual prototyping. We can make selected versions of the dataset more accessible with semantic versioning. We attach a human-interpretable tag that points to a specific commit SHA.

The `client_helpers` module of the `lakefs-spec` library provides the helper function `create_tag` to achieve this. We make a semantic tag point to the `fixed_commit_id`.
"""

# %% 

from lakefs_spec.client_helpers import create_tag

TAG='train-test-split'

create_tag(client=fs.client, repository=REPO_NAME,ref=fixed_commit_id, tag=TAG)


# %% [markdown]
"""
Now we can access the specific files with the semantic tag. Now the `fixed_commit_id` and `TAG` reference the same version `ref` in lakeFS whereas specifying a branch points to the latest version on the respective branch.
"""

# %% 
train_from_branch_head = pd.read_csv(f'lakefs://{REPO_NAME}/{TRAINING_BRANCH}/train_weather.csv', index_col=0)
train_from_commit_sha = pd.read_csv(f'lakefs://{REPO_NAME}/{fixed_commit_id}/train_weather.csv', index_col=0)
train_from_semantic_tag = pd.read_csv(f'lakefs://{REPO_NAME}/{TAG}/train_weather.csv', index_col=0)

# %% [markdown]
"""
We can verify this by looking at the lengths of the `DataFrame`s. We see that the `train_from_commit_sha` and `train_from_semantic_tag` are equal. 
"""

# %% 
print(len(train_from_branch_head))
print(len(train_from_commit_sha))
print(len(train_from_semantic_tag))
