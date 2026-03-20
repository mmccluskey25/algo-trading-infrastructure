from pathlib import Path

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

dbt_project = DbtProject(
    project_dir=Path(__file__).parent.parent.parent / "dbt_project",
)
dbt_project.prepare_if_dev()


class CustomDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        """
        Maps dbt models to the medallion architecture used in dagster while allowing
        the dbt model files to exist in their conventional folders.
        Automatic mapping can be overwritten using {{ config(meta={'dagster_group': 'bronze'}) }}
        in the model file.
        """
        meta = dbt_resource_props.get("meta", {})
        if "dagster_group" in meta:
            return meta["dagster_group"]

        fqn = dbt_resource_props.get("fqn", [])
        folder = fqn[1] if len(fqn) >= 2 else "default"
        folder_to_group = {
            "staging": "silver",
            "intermediate": "silver",
            "marts": "gold",
        }
        return folder_to_group.get(folder, "default")


@dbt_assets(
    manifest=dbt_project.manifest_path, dagster_dbt_translator=CustomDbtTranslator()
)
def dbt_project_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
