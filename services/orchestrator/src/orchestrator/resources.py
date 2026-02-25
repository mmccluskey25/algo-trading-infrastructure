from dagster import ConfigurableResource


class DataPathsResource(ConfigurableResource):
    landing_dir: str = "/data/landing/ticks"
    bronze_dir: str = "/data/bronze/ticks"
    delete_after_compaction: bool = True
