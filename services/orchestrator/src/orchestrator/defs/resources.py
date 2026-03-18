import dagster as dg


class DataPathsResource(dg.ConfigurableResource):
    landing_dir: str = "/data/landing/ticks"
    bronze_dir: str = "/data/bronze/ticks"
    delete_after_compaction: bool = True


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "data_paths": DataPathsResource(),
            "pipes_client": dg.PipesSubprocessClient(),
        }
    )
