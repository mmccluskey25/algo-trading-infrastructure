import dagster as dg
from dagster_dbt import DbtCliResource

from orchestrator.defs.dbt_assets import dbt_project


class DataPathsResource(dg.ConfigurableResource):
    data_root: str = dg.EnvVar("DATA_ROOT")
    delete_after_compaction: bool = True

    @property
    def landing_dir(self) -> str:
        return f"{self.data_root}/landing"

    @property
    def bronze_dir(self) -> str:
        return f"{self.data_root}/bronze"


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "data_paths": DataPathsResource(),
            "dbt": DbtCliResource(project_dir=dbt_project),
        }
    )
