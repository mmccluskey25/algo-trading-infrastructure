import dagster as dg
from dagster_dbt import DbtCliResource

from orchestrator.defs.dbt_assets import dbt_project


class DataPathsResource(dg.ConfigurableResource):
    landing_dir: str = dg.EnvVar("LANDING_DIR")
    bronze_dir: str = dg.EnvVar("BRONZE_DIR")
    delete_after_compaction: bool = True


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "data_paths": DataPathsResource(),
            "dbt": DbtCliResource(project_dir=dbt_project),
        }
    )
