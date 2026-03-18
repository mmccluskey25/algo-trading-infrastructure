import dagster as dg


class DataPathsResource(dg.ConfigurableResource):
    landing_dir: str = dg.EnvVar("LANDING_DIR")
    bronze_dir: str = dg.EnvVar("BRONZE_DIR")
    delete_after_compaction: bool = True
    compactor_script: str = dg.EnvVar("COMPACTOR_SCRIPT_PATH")
    compactor_python: str = dg.EnvVar("COMPACTOR_PYTHON")


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "data_paths": DataPathsResource(),
            "pipes_client": dg.PipesSubprocessClient(),
        }
    )
