from dagster import Definitions

from orchestrator.assets.compaction import bronze_ticks, landing_ticks
from orchestrator.resources import DataPathsResource

defs = Definitions(
    assets=[landing_ticks, bronze_ticks], resources={"data_paths": DataPathsResource()}
)
