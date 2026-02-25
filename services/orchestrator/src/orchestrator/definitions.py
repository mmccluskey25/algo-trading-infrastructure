from dagster import AssetExecutionContext, Definitions, asset


@asset
def hello_dagster(context: AssetExecutionContext):
    context.log.info("Hello from Dagster")


defs = Definitions(assets=[hello_dagster])
