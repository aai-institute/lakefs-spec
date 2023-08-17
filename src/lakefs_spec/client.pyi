from typing import Any

import lakefs_client
from lakefs_client import Configuration, apis

class _WrappedApiClient(lakefs_client.ApiClient):
    def files_parameters(self, files: str | None = ...) -> list[Any]: ...

class LakeFSClient:
    _api: _WrappedApiClient
    actions: apis.ActionsApi
    auth: apis.AuthApi
    branches: apis.BranchesApi
    commits: apis.CommitsApi
    config: apis.ConfigApi
    experimental: apis.ExperimentalApi
    health_check: apis.HealthCheckApi
    import_api: apis.ImportApi  # import is a reserved keyword
    metadata: apis.MetadataApi
    objects: apis.ObjectsApi
    otfdiff: apis.OtfDiffApi
    refs: apis.RefsApi
    repositories: apis.RepositoriesApi
    retention: apis.RetentionApi
    staging: apis.StagingApi
    statistics: apis.StatisticsApi
    tags: apis.TagsApi
    templates: apis.TemplatesApi

    def __init__(
        self,
        configuration: Configuration | None = ...,
        header_name: str | None = ...,
        header_value: str | None = ...,
        cookie: str | None = ...,
        pool_threads: int = ...,
    ) -> None: ...
